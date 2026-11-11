package core

import (
	"context"
	"errors"
	"fmt"
	"io"
)

// CopyOptions keeps the actual source read and target publication independent.
type CopyOptions struct {
	Source ReadSelection `json:"source"`
	Target WriteOptions  `json:"target"`
}

// Copy synchronously writes target from the complete source body. The parent
// must exist; replacement is explicit. It does not remove source.
// The returned Change describes the target even when error is non-nil.
func (c *Core) Copy(ctx context.Context, principal Principal, source, target Address, options ...CopyOptions) (change Change, err error) {
	change.Effect = Unchanged
	ctx, turn, err := enterOrder(ctx, nil)
	if err != nil {
		return change, err
	}
	defer func() { turn.finish(change.Effect == Unknown) }()
	if len(options) > 1 {
		return Change{Effect: Unchanged}, Fail(InvalidRequest, nil)
	}
	var policy CopyOptions
	if len(options) == 1 {
		policy = options[0]
	}
	write, err := writeOptions([]WriteOptions{policy.Target})
	if err != nil {
		return Change{Effect: Unchanged}, err
	}
	if c == nil {
		return Change{Effect: Unchanged}, Fail(InvalidRequest, nil)
	}
	if write.Continuation != nil {
		return Change{Effect: Unchanged}, Fail(UnsupportedOperation, nil)
	}
	if err = c.validateInput(ctx, source); err != nil {
		return Change{Effect: Unchanged}, err
	}
	if err = c.validateInput(ctx, target); err != nil {
		return Change{Effect: Unchanged}, err
	}
	if source == target {
		return Change{Effect: Unchanged}, Fail(InvalidRequest, nil)
	}
	from, to := c.captureChecked(ctx, source), c.captureChecked(ctx, target)
	if from != nil && to != nil {
		return c.copyAt(ctx, principal, from, to, policy)
	}
	defer func() { err = observedCause(ctx, err) }()
	var opened Content
	if from != nil {
		opened, err = c.openAt(ctx, principal, from, OpenOptions{Selection: policy.Source})
	} else {
		u, _, admissionErr := c.admitChecked(ctx, principal, ReadAction, source)
		opened, err = c.openUse(ctx, principal, u, OpenOptions{Selection: policy.Source}, admissionErr, nil)
	}
	if err != nil {
		return Change{Effect: Unchanged}, err
	}
	if to != nil {
		return c.copyContent(ctx, principal, to, opened, write)
	}
	return c.copyContentRequest(ctx, principal, target, opened, write)
}

func (c *Core) copyAt(ctx context.Context, p Principal, from, to *location, options CopyOptions) (change Change, err error) {
	write := options.Target
	if write.Continuation != nil {
		return Change{Effect: Unchanged}, Fail(UnsupportedOperation, nil)
	}
	if sameName(from, to) {
		return Change{Effect: Unchanged}, Fail(InvalidRequest, nil)
	}
	defer func() { err = observedCause(ctx, err) }()
	opened, err := c.openAt(ctx, p, from, OpenOptions{Selection: options.Source})
	if err != nil {
		return Change{Effect: Unchanged}, fmt.Errorf("open source: %w", err)
	}
	return c.copyContent(ctx, p, to, opened, write)
}

func (c *Core) copyContent(ctx context.Context, principal Principal, at *location, opened Content, write WriteOptions) (Change, error) {
	return copyOpened(ctx, opened, func(input io.Reader, length *int64) (Change, error) {
		return c.writeAt(ctx, principal, at, input, length, write)
	})
}

func copyOpened(ctx context.Context, opened Content, publish func(io.Reader, *int64) (Change, error)) (change Change, err error) {
	change.Effect = Unchanged
	defer func() {
		if cause := ctx.Err(); cause != nil && !errors.Is(err, cause) {
			err = errors.Join(err, cause)
		}
	}()
	defer func() {
		if closeErr := opened.Body.Close(); closeErr != nil {
			err = errors.Join(err, &OperationError{Side: SourceSide, Phase: ReadPhase, Cause: closeErr})
		}
	}()

	change, err = publish(opened.Body, opened.Length)
	err = copyError(opened.Body, err)
	return change, err
}

// The request fallback preserves authorization/error ordering for absent or
// unavailable targets; prepared composite work uses its captured location.
func (c *Core) copyContentRequest(ctx context.Context, p Principal, target Address, opened Content, write WriteOptions) (change Change, err error) {
	defer func() { err = errors.Join(err, opened.Body.Close()) }()
	u, _, err := c.admitChecked(ctx, p, write.action(), target)
	u, err = changeUse(u, err)
	return c.writeUse(u, opened.Body, opened.Length, write, err)
}

func copyError(body io.ReadCloser, err error) error {
	if err == nil {
		return nil
	}
	side := TargetSide
	if owned, ok := body.(*ownedBody); ok {
		owned.mu.Lock()
		if owned.terminal != nil && !errors.Is(owned.terminal, io.EOF) {
			side = SourceSide
		}
		owned.mu.Unlock()
	}
	return &OperationError{Side: side, Phase: WritePhase, Cause: err}
}
