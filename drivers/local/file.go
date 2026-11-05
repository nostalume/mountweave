package local

import (
	"context"
	"errors"
	"io"
	"os"
	"path"

	core "github.com/nostalume/mountweave"
)

func (d *Driver) edit(ctx context.Context, rel string, options core.OpenOptions) (opened core.Opened, err error) {
	opened.Change.Effect = core.Unchanged
	when := options.Selection.When
	if !when.Empty() && !when.IsPresent() && !when.IsAbsent() {
		return opened, core.Fail(core.UnsupportedOperation, nil)
	}
	creation := options.Creation
	if when.IsAbsent() {
		if creation == core.OpenExisting {
			return opened, core.Fail(core.InvalidRequest, nil)
		}
		creation = core.OpenNew
	}
	if when.IsPresent() {
		if creation == core.OpenNew {
			return opened, core.Fail(core.InvalidRequest, nil)
		}
		creation = core.OpenExisting
	}
	if err = core.ValidatePath(rel); err != nil {
		return opened, err
	}
	if rel == "/" {
		return opened, core.Fail(core.InvalidRequest, nil)
	}
	parent, err := d.OpenDirectory(ctx, core.NativePath(path.Dir(rel)))
	if err != nil {
		return opened, err
	}
	defer func() { err = errors.Join(err, parent.Close()) }()
	if err = ctx.Err(); err != nil {
		return opened, err
	}
	flags := os.O_WRONLY
	if options.Read {
		flags = os.O_RDWR
	}
	if options.AppendOnly {
		flags |= os.O_APPEND
	}
	var file *os.File
	var info os.FileInfo
	if creation != core.OpenExisting {
		file, info, err = openChild(parent.(*directoryScope).file, path.Base(rel), editChild, flags|os.O_CREATE|os.O_EXCL)
		if file != nil {
			opened.Change = core.Change{Effect: core.Applied, Before: core.TargetMissing}
		}
		conflict := errors.Is(err, os.ErrExist) || core.CodeOf(err) == core.AlreadyExists
		if file == nil && conflict && creation == core.OpenOrCreate {
			err = nil
		} else if file == nil && err != nil {
			if when.IsAbsent() && conflict {
				return opened, core.Fail(core.PreconditionFailed, err)
			}
			change, nativeErr := nativeChange(err, core.AlreadyExists)
			opened.Change = change
			return opened, nativeErr
		}
	}
	if file == nil && err == nil {
		if err = findChild(ctx, parent.(*directoryScope).file, path.Base(rel)); err == nil {
			file, info, err = openChild(parent.(*directoryScope).file, path.Base(rel), editChild, flags)
		}
		if when.IsPresent() && core.CodeOf(localError(err)) == core.NotFound {
			err = core.Fail(core.PreconditionFailed, err)
		}
		if file != nil {
			opened.Change.Before = core.TargetPresent
		}
	}
	if file != nil {
		opened.File = &boundFile{file: file, driver: d, name: path.Base(rel), readable: options.Read, appendOnly: options.AppendOnly}
		opened.Binding = core.BoundObject
	}
	if err != nil {
		return opened, localError(err)
	}
	if !info.Mode().IsRegular() {
		return opened, core.Fail(core.NotFile, nil)
	}
	if options.Truncate {
		change, truncateErr := opened.File.(core.EditFile).Truncate(ctx, 0)
		if change.Effect != core.Unchanged {
			opened.Change.Effect = change.Effect
		}
		if truncateErr != nil {
			return opened, truncateErr
		}
		info, err = file.Stat()
		if err != nil {
			return opened, localError(err)
		}
	}
	opened.Metadata, err = metadata(path.Base(rel), info)
	return opened, errors.Join(err, ctx.Err())
}

func editBytes(n, wanted int, err error) (int, core.Change, error) {
	change := core.Change{Effect: core.Unchanged}
	if n > 0 {
		change.Effect = core.Incomplete
		if n == wanted {
			change.Effect = core.Applied
		}
	}
	if n < wanted && err == nil {
		err = io.ErrShortWrite
	}
	return n, change, err
}

func (f *boundFile) WriteAt(ctx context.Context, p []byte, offset int64) (int, core.Change, error) {
	if err := ctx.Err(); err != nil {
		return 0, core.Change{Effect: core.Unchanged}, err
	}
	if f.appendOnly {
		return 0, core.Change{Effect: core.Unchanged}, core.Fail(core.UnsupportedOperation, nil)
	}
	n, err := f.file.WriteAt(p, offset)
	return editBytes(n, len(p), errors.Join(localError(err), ctx.Err()))
}

func (f *boundFile) Append(ctx context.Context, p []byte) (int, core.Change, error) {
	if err := ctx.Err(); err != nil {
		return 0, core.Change{Effect: core.Unchanged}, err
	}
	if !f.appendOnly {
		return 0, core.Change{Effect: core.Unchanged}, core.Fail(core.UnsupportedOperation, nil)
	}
	n, err := appendFile(f.file, p)
	return editBytes(n, len(p), errors.Join(localError(err), ctx.Err()))
}

func (f *boundFile) Truncate(ctx context.Context, size int64) (core.Change, error) {
	if err := ctx.Err(); err != nil {
		return core.Change{Effect: core.Unchanged}, err
	}
	err := f.file.Truncate(size)
	change, err := nativeChange(err, core.AlreadyExists)
	return change, errors.Join(err, ctx.Err())
}

func (f *boundFile) Sync(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return errors.Join(f.file.Sync(), ctx.Err())
}
