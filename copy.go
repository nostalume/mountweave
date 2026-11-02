package core

import (
	"context"
	"errors"
	"fmt"
)

// Copy synchronously writes target from the complete source body. The parent
// must exist; replacement is explicit. It does not remove source.
// The returned Change describes the target even when error is non-nil.
func (c *Core) Copy(ctx context.Context, principal Principal, source, target string, options ...WriteOptions) (Change, error) {
	write, err := writeOptions(options)
	if err != nil {
		return Change{Unchanged}, err
	}
	return c.copy(ctx, principal, source, target, write, nil, nil)
}
func (c *Core) copy(ctx context.Context, principal Principal, source, target string, write WriteOptions, from, to *location) (change Change, err error) {
	change.Effect = Unchanged
	if c == nil {
		return change, Fail(InvalidRequest, errors.New("core is required"))
	}
	if err = ValidatePath(source); err != nil {
		return change, fmt.Errorf("source path: %w", err)
	}
	if err = ValidatePath(target); err != nil {
		return change, fmt.Errorf("target path: %w", err)
	}
	if from == nil {
		from = c.capture(source)
	}
	if to == nil {
		to = c.capture(target)
	}
	if source == target || (from != nil && to != nil && from.binding.mount == to.binding.mount && from.path == to.path) {
		return change, Fail(InvalidRequest, errors.New("source and target must differ"))
	}
	defer func() {
		if cause := ctx.Err(); cause != nil && !errors.Is(err, cause) {
			err = errors.Join(err, cause)
		}
	}()
	opened, err := c.open(ctx, principal, source, nil, from, nil)
	if err != nil {
		return change, fmt.Errorf("open source: %w", err)
	}
	return c.copyContent(ctx, principal, target, opened, write, to)
}

func (c *Core) copyContent(ctx context.Context, principal Principal, target string, opened Content, write WriteOptions, expected *location) (change Change, err error) {
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

	change, err = c.write(ctx, principal, target, opened.Body, opened.Length, write, expected)
	if err != nil {
		err = fmt.Errorf("write target: %w", err)
	}
	return change, err
}
