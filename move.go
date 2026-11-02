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
func (c *Core) Move(ctx context.Context, p Principal, source, target string) (MoveResult, error) {
	return c.move(ctx, p, source, target, nil, nil)
}

func (c *Core) move(ctx context.Context, p Principal, source, target string, expectedSource, expectedTarget *location) (result MoveResult, err error) {
	result = MoveResult{Source: Change{Unchanged}, Target: Change{Unchanged}}
	defer func() {
		if cause := ctx.Err(); cause != nil && !errors.Is(err, cause) {
			err = errors.Join(err, cause)
		}
	}()
	if c == nil {
		return result, Fail(InvalidRequest, errors.New("core is required"))
	}
	if err = ValidatePath(source); err != nil {
		return result, fmt.Errorf("source path: %w", err)
	}
	if err = ValidatePath(target); err != nil {
		return result, fmt.Errorf("target path: %w", err)
	}
	if source == target {
		return result, Fail(InvalidRequest, errors.New("source and target must differ"))
	}
	from, oldPath, err := c.beginChangeAt(ctx, p, RemoveAction, source, expectedSource)
	if err != nil {
		return result, fmt.Errorf("source: %w", err)
	}
	defer from.release()
	to, newPath, err := c.beginChangeAt(ctx, p, CreateAction, target, expectedTarget)
	if err != nil {
		return result, fmt.Errorf("target: %w", err)
	}
	defer to.release()
	if from.mount == to.mount && Within(newPath, oldPath) {
		return result, Fail(InvalidRequest, nil)
	}
	if from.mount != to.mount {
		return result, Fail(UnsupportedOperation, nil)
	}
	mover, ok := from.mount.driver.(Relocator)
	if !ok {
		return result, Fail(UnsupportedOperation, nil)
	}
	if err = from.ctx.Err(); err != nil {
		return result, err
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
