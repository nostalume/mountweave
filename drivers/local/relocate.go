package local

import (
	"context"
	"errors"
	"os"

	core "github.com/nostalume/mountweave"
)

func (d *Driver) Relocate(ctx context.Context, sourceAddress, targetAddress core.NativeAddress, policies ...core.MoveOptions) (change core.Change, err error) {
	if len(policies) > 1 {
		return core.Change{Effect: core.Unchanged}, core.Fail(core.InvalidRequest, nil)
	}
	if len(policies) == 1 && (!policies[0].Source.Empty() || !policies[0].Target.Empty()) {
		return core.Change{Effect: core.Unchanged}, core.Fail(core.UnsupportedOperation, nil)
	}
	if sourceAddress.Namespace() != core.TreeNamespace {
		return core.Change{Effect: core.Unchanged}, core.Fail(core.UnsupportedOperation, nil)
	}
	source := sourceAddress.Value()
	if targetAddress.Namespace() != core.TreeNamespace {
		return core.Change{Effect: core.Unchanged}, core.Fail(core.UnsupportedOperation, nil)
	}
	target := targetAddress.Value()

	change.Effect = core.Unchanged
	from, oldName, err := d.openParent(ctx, source)
	if err != nil {
		err = &core.OperationError{Side: core.SourceSide, Phase: core.RemovePhase, Cause: err}
		return
	}
	defer func() { err = errors.Join(err, d.closeFile(from)) }()
	if core.Within(target, source) {
		return change, core.Fail(core.InvalidRequest, nil)
	}
	to, newName, err := d.openParent(ctx, target)
	if err != nil {
		return change, &core.OperationError{Side: core.TargetSide, Phase: core.CreatePhase, Cause: err}
	}
	defer func() { err = errors.Join(err, d.closeFile(to)) }()
	return d.relocateNative(ctx, from, to, oldName, newName)
}

// Check after the source is admitted. Core may act on an existing-target
// conflict, and the final native rename still excludes concurrent creation.
func (d *Driver) checkRelocateTarget(ctx context.Context, parent *os.File, name string) error {
	_, err := d.statChild(ctx, parent, name, leafChild)
	if err == nil {
		return core.Fail(core.AlreadyExists, nil)
	}
	if core.CodeOf(err) == core.NotFound {
		return ctx.Err()
	}
	return &core.OperationError{Side: core.TargetSide, Phase: core.CreatePhase, Cause: err}
}

func relocationResult(err error) (core.Change, error) {
	if errors.Is(err, errors.ErrUnsupported) {
		return core.Change{Effect: core.Unchanged}, core.Fail(core.UnsupportedOperation, err)
	}
	return nativeChange(err, core.AlreadyExists)
}

var _ core.Relocator = (*Driver)(nil)
