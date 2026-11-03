package core

import (
	"context"
	"errors"
	"fmt"
)

// Relocator atomically moves an admitted regular file or directory without
// replacement. Unsupported entry kinds must be refused before effects. Paths use
// the Driver's native execution-time binding; metadata checks do not bind an
// object version or type against external replacement.
// Source and target belong to the same installed Driver and an existing parent.
type Relocator interface {
	Relocate(context.Context, string, string) (Change, error)
}

// MoveResult reports this operation's source removal and target creation.
// Receipts do not describe changes made by other callers after either effect.
type MoveResult struct {
	Source Change
	Target Change
}

// Move renames or moves a file or directory without replacing target. Directory
// admission authorizes the whole native relocation, without descendant checks.
// Both receipts must be Applied and error must be nil for completion without
// errors. Currently only
// native relocation within one mount is supported; copying followed by removal
// through an unbound source path is never used.
func (c *Core) Move(ctx context.Context, p Principal, source, target string) (result MoveResult, err error) {
	result = MoveResult{Source: Change{Unchanged}, Target: Change{Unchanged}}
	defer func() { err = observedCause(ctx, err) }()
	if c == nil {
		return result, Fail(InvalidRequest, nil)
	}
	if err = ValidatePath(source); err != nil {
		return result, err
	}
	if err = ValidatePath(target); err != nil {
		return result, err
	}
	if source == target {
		return result, Fail(InvalidRequest, nil)
	}
	from, err := c.beginChange(ctx, p, RemoveAction, source)
	if err != nil {
		return result, fmt.Errorf("source: %w", err)
	}
	to, err := c.beginChange(ctx, p, CreateAction, target)
	if err != nil {
		from.release()
		return result, fmt.Errorf("target: %w", err)
	}
	return c.relocate(from, to)
}
func (c *Core) moveAt(ctx context.Context, p Principal, source, target *location) (result MoveResult, err error) {
	result = MoveResult{Source: Change{Unchanged}, Target: Change{Unchanged}}
	defer func() { err = observedCause(ctx, err) }()
	from, err := c.beginChangeAt(ctx, p, RemoveAction, source)
	if err != nil {
		return result, err
	}
	to, err := c.beginChangeAt(ctx, p, CreateAction, target)
	if err != nil {
		from.release()
		return result, err
	}
	return c.relocate(from, to)
}
func (c *Core) relocate(from, to *use) (result MoveResult, err error) {
	defer from.release()
	defer to.release()
	result = MoveResult{Source: Change{Unchanged}, Target: Change{Unchanged}}
	oldPath, newPath := from.path, to.path
	if from.mount == to.mount && Within(newPath, oldPath) {
		return MoveResult{Source: Change{Unchanged}, Target: Change{Unchanged}}, Fail(InvalidRequest, nil)
	}
	if from.mount != to.mount {
		return MoveResult{Source: Change{Unchanged}, Target: Change{Unchanged}}, Fail(UnsupportedOperation, nil)
	}
	mover, ok := from.mount.driver.(Relocator)
	if !ok {
		return MoveResult{Source: Change{Unchanged}, Target: Change{Unchanged}}, Fail(UnsupportedOperation, nil)
	}
	if err = from.ctx.Err(); err != nil {
		return MoveResult{Source: Change{Unchanged}, Target: Change{Unchanged}}, err
	}
	change, err := mover.Relocate(from.ctx, oldPath, newPath)
	if change.Effect == Incomplete {
		change.Effect = Unknown
		err = errors.Join(err, Fail(InvalidUpstreamResponse, errors.New("atomic relocation reported partial effects")))
	}
	change, err = c.finishChange(from, change, err)
	result.Source, result.Target = change, change
	if err != nil {
		err = fmt.Errorf("relocate: %w", err)
	}
	return result, err
}
