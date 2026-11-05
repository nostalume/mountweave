package local

import (
	"context"
	"errors"
	"path"

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
	if err = d.changeTarget(ctx, source, true); err != nil {
		err = &core.OperationError{Side: core.SourceSide, Phase: core.RemovePhase, Cause: err}
		return
	}
	if err = d.changeTarget(ctx, target, false); err != nil {
		err = &core.OperationError{Side: core.TargetSide, Phase: core.CreatePhase, Cause: err}
		return
	}
	if core.Within(target, source) {
		return change, core.Fail(core.InvalidRequest, nil)
	}
	info, err := d.root.Lstat(native(source))
	if err != nil {
		return change, localError(err)
	}
	if !info.Mode().IsRegular() && !info.IsDir() {
		return change, core.Fail(core.UnsupportedOperation, nil)
	}
	// Normalize the exclusive conflict across file/directory combinations.
	// The final rename still excludes peers created after this observation.
	if _, err := d.root.Lstat(native(target)); err == nil {
		return change, core.Fail(core.AlreadyExists, nil)
	} else if core.CodeOf(localError(err)) != core.NotFound {
		return change, &core.OperationError{Side: core.TargetSide, Phase: core.CreatePhase, Cause: localError(err)}
	}
	from, err := d.root.Open(native(path.Dir(source)))
	if err != nil {
		return change, localError(err)
	}
	defer func() { err = errors.Join(err, d.closeFile(from)) }()
	to, err := d.root.Open(native(path.Dir(target)))
	if err != nil {
		return change, localError(err)
	}
	defer func() { err = errors.Join(err, d.closeFile(to)) }()
	if err = ctx.Err(); err != nil {
		return
	}
	return d.relocateNative(ctx, from, to, path.Base(source), path.Base(target))
}

func relocationResult(err error) (core.Change, error) {
	if errors.Is(err, errors.ErrUnsupported) {
		return core.Change{Effect: core.Unchanged}, core.Fail(core.UnsupportedOperation, err)
	}
	return nativeChange(err, core.AlreadyExists)
}

var _ core.Relocator = (*Driver)(nil)
