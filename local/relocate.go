package local

import (
	"context"
	"errors"
	"path"

	core "github.com/nostalume/mountweave"
)

func (d *Driver) Relocate(ctx context.Context, source, target string) (change core.Change, err error) {
	change.Effect = core.Unchanged
	if err = d.changeTarget(ctx, source, true); err != nil {
		return
	}
	if err = d.changeTarget(ctx, target, false); err != nil {
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
