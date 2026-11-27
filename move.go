package core

import (
	"context"
	"errors"
	"fmt"
	"path"
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

// DirectoryRelocator atomically moves one item into an existing directory.
// The driver binds the source and parent at commit, prefers the source name,
// preserves existing children, and returns the confirmed direct-child name
// with Applied. It reports unsupported modes before changing either side.
type DirectoryRelocator interface {
	RelocateInto(context.Context, NativeAddress, NativeAddress) (string, Change, error)
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
	// Destination is the confirmed path chosen for a directory-target move.
	Destination Address
	// Removed records a separately confirmed target-removal phase. It never
	// claims that the later source relocation or target creation succeeded.
	Removed *TreeResult
	Tree    *TreeResult // Attempted phases of a cross-mount directory move.
}

func (r MoveResult) uncertain() bool {
	return r.Source.Effect == Unknown || r.Target.Effect == Unknown || r.Removed != nil && r.Removed.Change.Effect == Unknown
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
	defer func() { turn.finish(result.uncertain()) }()
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

// MoveInto moves one item into a mounted directory. The requested directory
// stays fixed; the resulting leaf name is returned in Destination. Cross-mount
// directory moves use the optional tree budgets, as in Move.
func (c *Core) MoveInto(ctx context.Context, p Principal, source, directory Address, trees ...TreeOptions) (result MoveResult, err error) {
	result = MoveResult{Source: Change{Effect: Unchanged}, Target: Change{Effect: Unchanged}}
	ctx, turn, err := enterOrder(ctx, nil)
	if err != nil {
		return result, err
	}
	defer func() { turn.finish(result.uncertain()); err = observedCause(ctx, err) }()
	var options MoveOptions
	if len(trees) > 1 {
		return result, Fail(InvalidRequest, nil)
	}
	if len(trees) == 1 {
		options.Tree = trees[0]
		if err := options.validate(); err != nil {
			return result, err
		}
	}
	if c == nil {
		return result, Fail(InvalidRequest, nil)
	}
	if source.Namespace() != TreeNamespace || directory.Namespace() != TreeNamespace {
		return result, Fail(UnsupportedOperation, nil)
	}
	ctx, err = c.selectOperation(ctx, AddressAccess{Address: source, Write: true}, AddressAccess{Address: directory, Write: true})
	if err != nil {
		return result, err
	}
	if err := c.validateInput(ctx, directory); err != nil {
		return result, err
	}
	from, err := c.beginChange(ctx, p, RemoveAction, source)
	if err != nil {
		return result, fmt.Errorf("source: %w", err)
	}
	defer from.release()
	c.mu.Lock()
	parent, _, err := c.resolveRequestLocked(ctx, directory)
	c.mu.Unlock()
	if err != nil {
		return result, err
	}
	if parent == nil {
		return result, Fail(InvalidRequest, errors.New("a real mounted directory is required"))
	}
	sameMount := from.mount == parent.mount
	if sameMount && (Within(parent.path, from.path) || path.Dir(from.path) == parent.path) {
		return result, Fail(InvalidRequest, errors.New("the source cannot move into itself or its current directory"))
	}
	relocator, ok := from.mount.driver.(DirectoryRelocator)
	if !sameMount || !ok {
		return c.moveIntoExact(ctx, p, from, directory, options, result)
	}
	to, err := c.admitAt(from.ctx, p, CreateChildAction, parent)
	if err != nil {
		return result, fmt.Errorf("target directory: %w", err)
	}
	defer to.release()
	visible := to.visible()
	room := min(childRoom(visible), childRoom(to.primary()))
	if room <= 0 {
		return result, Fail(InvalidPath, nil)
	}
	if err := to.ctx.Err(); err != nil {
		return result, err
	}
	name, change, err := relocator.RelocateInto(to.ctx, from.native(), to.native())
	if change.Effect == Applied {
		if ValidName(name) && len(name) <= room {
			result.Destination = Path(childPath(visible, name))
		} else {
			change = Change{Effect: Unknown}
			err = errors.Join(err, Fail(InvalidUpstreamResponse, errors.New("relocation returned an invalid child name")))
		}
	}
	change, err = c.finishRelocation(to, change, err)
	result.Source, result.Target = relocationEffects(change)
	return result, err
}

func (c *Core) moveIntoExact(ctx context.Context, p Principal, from *use, directory Address, options MoveOptions, result MoveResult) (MoveResult, error) {
	child, err := c.availableChild(from.ctx, p, directory, path.Base(from.path))
	if err != nil {
		return result, err
	}
	to, err := c.beginChangeAt(from.ctx, p, CreateAction, child)
	if err != nil {
		return result, err
	}
	result, err = c.moveUses(ctx, p, from, to, options)
	if result.Target.Effect == Applied || result.Target.Effect == Incomplete {
		result.Destination = child.address()
	}
	return result, err
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
	change, err = c.finishRelocation(from, change, err)
	result.Source, result.Target = relocationEffects(change)
	return result, err
}

func (c *Core) finishRelocation(u *use, change Change, err error) (Change, error) {
	if change.Effect == Incomplete {
		change.Effect = Unknown
		err = errors.Join(err, Fail(InvalidUpstreamResponse, errors.New("atomic relocation reported partial effects")))
	}
	change, err = c.finishChange(u, change, err)
	if err != nil {
		err = fmt.Errorf("relocate: %w", err)
	}
	return change, err
}

func relocationEffects(change Change) (source, target Change) {
	source, target = change, change
	source.Evidence = Evidence{}
	source.Before = TargetUnknown
	if target.Effect == Applied {
		target.Before = TargetMissing
	}
	return source, target
}
