package core

import (
	"context"
	"errors"
	"fmt"
)

// Copy synchronously writes target from the complete source body. The parent
// must exist; replacement is explicit. It does not remove source.
// The returned Change describes the target even when error is non-nil.
func (c *Core) Copy(ctx context.Context, principal Principal, source, target string, options ...WriteOptions) (change Change, err error) {
	write, err := writeOptions(options)
	if err != nil {
		return Change{Unchanged}, err
	}
	if c == nil {
		return Change{Unchanged}, Fail(InvalidRequest, nil)
	}
	if write.Continuation != nil {
		return Change{Unchanged}, Fail(UnsupportedOperation, nil)
	}
	if err = ValidatePath(source); err != nil {
		return Change{Unchanged}, err
	}
	if err = ValidatePath(target); err != nil {
		return Change{Unchanged}, err
	}
	if source == target {
		return Change{Unchanged}, Fail(InvalidRequest, nil)
	}
	from, to := c.captureChecked(ctx, source), c.captureChecked(ctx, target)
	if from != nil && to != nil {
		return c.copyAt(ctx, principal, from, to, write)
	}
	defer func() { err = observedCause(ctx, err) }()
	var opened Content
	if from != nil {
		opened, err = c.openAt(ctx, principal, from, nil)
	} else {
		opened, err = c.Open(ctx, principal, source, nil)
	}
	if err != nil {
		return Change{Unchanged}, err
	}
	if to != nil {
		return c.copyContent(ctx, principal, to, opened, write)
	}
	return c.copyContentRequest(ctx, principal, target, opened, write)
}
func (c *Core) copyAt(ctx context.Context, p Principal, from, to *location, write WriteOptions) (change Change, err error) {
	if write.Continuation != nil {
		return Change{Unchanged}, Fail(UnsupportedOperation, nil)
	}
	if from.mount == to.mount && from.path == to.path {
		return Change{Unchanged}, Fail(InvalidRequest, nil)
	}
	defer func() { err = observedCause(ctx, err) }()
	opened, err := c.openAt(ctx, p, from, nil)
	if err != nil {
		return Change{Unchanged}, fmt.Errorf("open source: %w", err)
	}
	return c.copyContent(ctx, p, to, opened, write)
}

func (c *Core) copyContent(ctx context.Context, principal Principal, at *location, opened Content, write WriteOptions) (change Change, err error) {
	change.Effect = Unchanged
	defer func() {
		if cause := ctx.Err(); cause != nil && !errors.Is(err, cause) {
			err = errors.Join(err, cause)
		}
	}()
	defer func() {
		if closeErr := opened.Body.Close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("close source: %w", closeErr))
		}
	}()

	change, err = c.writeAt(ctx, principal, at, opened.Body, opened.Length, write)
	if err != nil {
		err = fmt.Errorf("write target: %w", err)
	}
	return change, err
}

// The request fallback preserves authorization/error ordering for absent or
// unavailable targets; prepared composite work uses its captured location.
func (c *Core) copyContentRequest(ctx context.Context, p Principal, target string, opened Content, write WriteOptions) (change Change, err error) {
	defer func() { err = errors.Join(err, opened.Body.Close()) }()
	return c.Write(ctx, p, target, opened.Body, opened.Length, write)
}
