package core

import (
	"context"
	"errors"
	"fmt"
)

// Relocator atomically moves an admitted regular file or directory without
// replacement. Unsupported entry kinds must be refused before effects. Paths use
// the Driver's native execution-time binding. Unsupported options and entry
// kinds must be refused before reporting an existing-target conflict, so Core
// can safely distinguish an unavailable mode from explicit replacement policy.
// Metadata checks do not bind an
// object version or type against external replacement.
// Source and target belong to the same installed Driver and an existing parent.
type Relocator interface {
	Relocate(context.Context, NativeAddress, NativeAddress, ...MoveOptions) (Change, error)
}

type MoveOptions struct {
	Source   Condition    `json:"source"`
	Target   Condition    `json:"target"`
	Existing TargetPolicy `json:"existing,omitempty"`
	Tree     TreeOptions  `json:"tree,omitzero"`
	NoCopy   bool         `json:"-"` // Require the existing relocation path.
}

func (o MoveOptions) validate() error {
	if o.Existing != "" && o.Existing != CreateNew && o.Existing != Replace {
		return Fail(InvalidRequest, nil)
	}
	if o.Tree.MaxEntries < 0 || o.Tree.MaxPlanBytes < 0 || o.Tree.Concurrency < 0 {
		return Fail(InvalidRequest, nil)
	}
	if err := o.Source.Validate(); err != nil {
		return err
	}
	return o.Target.Validate()
}

// MoveResult reports this operation's source removal and target creation.
// Receipts do not describe changes made by other callers after either effect.
type MoveResult struct {
	Source Change
	Target Change
	// Removed records a separately confirmed target-removal phase. It never
	// claims that the later source relocation or target creation succeeded.
	Removed *TreeResult
	Tree    *TreeResult // Attempted phases of a cross-mount directory move.
}

// Move renames or moves a file or directory. The zero policy never replaces.
// Explicit Replace may first remove a target, recorded separately in Removed.
// Tree budgets are required if that phase encounters a directory. Directory
// admission authorizes the whole native relocation, without descendant checks.
// Both receipts must be Applied and error must be nil for completion without
// errors. A failed later relocation does not undo Removed. A cross-mount move
// publishes exclusively before removing the exact source version. Directories
// require positive Tree budgets. NoCopy retains the native relocation path.
func (c *Core) Move(ctx context.Context, p Principal, source, target Address, policies ...MoveOptions) (result MoveResult, err error) {
	result = MoveResult{Source: Change{Effect: Unchanged}, Target: Change{Effect: Unchanged}}
	ctx, turn, err := enterOrder(ctx, nil)
	if err != nil {
		return result, err
	}
	defer func() {
		turn.finish(result.Source.Effect == Unknown || result.Target.Effect == Unknown || result.Removed != nil && result.Removed.Change.Effect == Unknown)
	}()
	if len(policies) > 1 {
		return result, Fail(InvalidRequest, nil)
	}
	var policy MoveOptions
	if len(policies) == 1 {
		policy = policies[0]
	}
	if err := policy.validate(); err != nil {
		return result, err
	}
	defer func() { err = observedCause(ctx, err) }()
	if c == nil {
		return result, Fail(InvalidRequest, nil)
	}
	if err = c.validateInput(ctx, source); err != nil {
		return result, err
	}
	if err = c.validateInput(ctx, target); err != nil {
		return result, err
	}
	if source == target {
		return result, Fail(InvalidRequest, nil)
	}
	if policy.Existing == Replace {
		ctx, err = c.selectOperation(ctx, AddressAccess{Address: source, Write: true}, AddressAccess{Address: target, Write: true})
		if err != nil {
			return result, err
		}
	}
	u, _, err := c.admitChecked(ctx, p, RemoveAction, source)
	from, err := changeUse(u, err)
	if err != nil {
		return result, fmt.Errorf("source: %w", err)
	}
	u, _, err = c.admitChecked(ctx, p, CreateAction, target)
	to, err := changeUse(u, err)
	if err != nil {
		from.release()
		return result, fmt.Errorf("target: %w", err)
	}
	return c.moveUses(ctx, p, from, to, policy)
}

func (c *Core) moveAt(ctx context.Context, p Principal, source, target *location, options MoveOptions) (result MoveResult, err error) {
	result = MoveResult{Source: Change{Effect: Unchanged}, Target: Change{Effect: Unchanged}}
	if err := options.validate(); err != nil {
		return result, err
	}
	defer func() { err = observedCause(ctx, err) }()
	if options.Existing == Replace {
		ctx, err = c.selectOperation(ctx, AddressAccess{Address: source.address(), Write: true}, AddressAccess{Address: target.address(), Write: true})
		if err != nil {
			return result, err
		}
	}
	from, err := c.beginChangeAt(ctx, p, RemoveAction, source)
	if err != nil {
		return result, err
	}
	to, err := c.beginChangeAt(ctx, p, CreateAction, target)
	if err != nil {
		from.release()
		return result, err
	}
	return c.moveUses(ctx, p, from, to, options)
}

// The first no-overwrite attempt validates the native relocation mode, and is
// also the fast path when the target is absent. Unsupported options must be
// refused before a Relocator reports an existing-target conflict.
func (c *Core) moveUses(ctx context.Context, p Principal, from, to *use, options MoveOptions) (MoveResult, error) {
	if from.mount != to.mount {
		return c.moveAcross(p, from, to, options)
	}
	if options.Existing == Replace && from.mount == to.mount && from.namespace == TreeNamespace && to.namespace == TreeNamespace && Within(from.path, to.path) {
		from.release()
		to.release()
		return MoveResult{Source: Change{Effect: Unchanged}, Target: Change{Effect: Unchanged}}, Fail(InvalidRequest, nil)
	}
	native := MoveOptions{Source: options.Source, Target: options.Target}
	result, err := c.relocate(from, to, native)
	if options.Existing != Replace || result.Source.Effect != Unchanged || result.Target.Effect != Unchanged || !directoryConflict(err) {
		return result, err
	}
	result.Target.Before = TargetPresent
	removed, removeErr := c.removeTarget(ctx, p, to.address(), options.Tree, options.Target, nil)
	result.Removed = &removed
	if removeErr != nil {
		return result, removeErr
	}
	left, err := c.beginChangeAt(ctx, p, RemoveAction, from.location)
	if err != nil {
		return result, err
	}
	right, err := c.beginChangeAt(ctx, p, CreateAction, to.location)
	if err != nil {
		left.release()
		return result, err
	}
	// The old target predicate was consumed by its removal. The final native
	// step remains exclusive, protecting a peer created after that removal.
	moved, err := c.relocate(left, right, MoveOptions{Source: options.Source})
	moved.Removed = &removed
	moved.Target.Before = TargetPresent
	return moved, err
}

func (c *Core) relocate(from, to *use, options MoveOptions) (result MoveResult, err error) {
	defer from.release()
	defer to.release()
	result = MoveResult{Source: Change{Effect: Unchanged}, Target: Change{Effect: Unchanged}}
	oldPath, newPath := from.path, to.path
	if sameName(from.location, to.location) || from.mount == to.mount && from.namespace == TreeNamespace && to.namespace == TreeNamespace && Within(newPath, oldPath) {
		return MoveResult{Source: Change{Effect: Unchanged}, Target: Change{Effect: Unchanged}}, Fail(InvalidRequest, nil)
	}
	if from.mount != to.mount {
		return MoveResult{Source: Change{Effect: Unchanged}, Target: Change{Effect: Unchanged}}, Fail(UnsupportedOperation, nil)
	}
	mover, ok := from.mount.driver.(Relocator)
	if !ok {
		return MoveResult{Source: Change{Effect: Unchanged}, Target: Change{Effect: Unchanged}}, Fail(UnsupportedOperation, nil)
	}
	if err = from.ctx.Err(); err != nil {
		return MoveResult{Source: Change{Effect: Unchanged}, Target: Change{Effect: Unchanged}}, err
	}
	change, err := mover.Relocate(from.ctx, from.native(), to.native(), options)
	if change.Effect == Incomplete {
		change.Effect = Unknown
		err = errors.Join(err, Fail(InvalidUpstreamResponse, errors.New("atomic relocation reported partial effects")))
	}
	change, err = c.finishChange(from, change, err)
	result.Source, result.Target = change, change
	result.Source.Evidence = Evidence{}
	result.Source.Before = TargetUnknown
	if result.Target.Effect == Applied {
		result.Target.Before = TargetMissing
	}
	if err != nil {
		err = fmt.Errorf("relocate: %w", err)
	}
	return result, err
}
