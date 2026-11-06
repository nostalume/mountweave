package local

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
	"sync"
	"syscall"

	core "github.com/nostalume/mountweave"
)

// closeFile keeps failed cleanup owned by this Driver. A later Driver.Close
// must not acknowledge retirement while these failures remain unresolved.
func (d *Driver) closeFile(file *os.File) error {
	err := file.Close()
	if err != nil {
		d.mu.Lock()
		d.failedFiles = append(d.failedFiles, file)
		d.cleanupErr = errors.Join(d.cleanupErr, err)
		d.mu.Unlock()
	}
	return err
}

func nativeChange(err error, conflict core.Code) (core.Change, error) {
	if err == nil {
		return core.Change{Effect: core.Applied}, nil
	}
	if errors.Is(err, fs.ErrExist) {
		return core.Change{Effect: core.Unchanged}, core.Fail(conflict, err)
	}
	var checked *core.Error
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || errors.As(err, &checked) {
		return core.Change{Effect: core.Unchanged}, err
	}
	if errors.Is(err, fs.ErrNotExist) || errors.Is(err, fs.ErrPermission) {
		return core.Change{Effect: core.Unchanged}, localError(err)
	}
	return core.Change{Effect: core.Unknown}, localError(err)
}

func (d *Driver) Mkdir(ctx context.Context, relAddress core.NativeAddress) (change core.Change, err error) {
	change.Effect = core.Unchanged
	if relAddress.Namespace() != core.TreeNamespace {
		return change, core.Fail(core.UnsupportedOperation, nil)
	}
	parent, name, err := d.openParent(ctx, relAddress.Value())
	if err != nil {
		return change, err
	}
	defer func() { err = errors.Join(err, d.closeFile(parent)) }()
	if err := ctx.Err(); err != nil {
		return change, err
	}
	return d.mkdirAt(ctx, parent, name)
}

func (d *Driver) Remove(ctx context.Context, relAddress core.NativeAddress, policies ...core.RemoveOptions) (change core.Change, err error) {
	change.Effect = core.Unchanged
	if len(policies) > 1 {
		return change, core.Fail(core.InvalidRequest, nil)
	}
	var options core.RemoveOptions
	if len(policies) == 1 {
		options = policies[0]
	}
	if relAddress.Namespace() != core.TreeNamespace || !options.When.Empty() {
		return change, core.Fail(core.UnsupportedOperation, nil)
	}
	parent, name, err := d.openParent(ctx, relAddress.Value())
	if err != nil {
		return change, err
	}
	defer func() { err = errors.Join(err, d.closeFile(parent)) }()
	return d.removeWith(ctx, parent, name, options)
}

func (d *Driver) Create(ctx context.Context, relAddress core.NativeAddress, input io.Reader, length *int64) (core.Change, error) {
	if relAddress.Namespace() != core.TreeNamespace {
		return core.Change{Effect: core.Unchanged}, core.Fail(core.UnsupportedOperation, nil)
	}
	if input == nil || (length != nil && *length < 0) {
		return core.Change{Effect: core.Unchanged}, core.Fail(core.InvalidRequest, nil)
	}
	parent, name, err := d.openParent(ctx, relAddress.Value())
	if err != nil {
		return core.Change{Effect: core.Unchanged}, err
	}
	if err := ctx.Err(); err != nil {
		return core.Change{Effect: core.Unchanged}, errors.Join(err, d.closeFile(parent))
	}
	file, _, err := d.openEdit(ctx, parent, name, os.O_WRONLY|os.O_CREATE|os.O_EXCL)
	parentErr := d.closeFile(parent)
	if file == nil {
		// Some platforms report an existing directory as EISDIR rather than
		// EEXIST. Exclusive creation has not changed that target.
		if errors.Is(err, syscall.EISDIR) {
			return core.Change{Effect: core.Unchanged}, errors.Join(core.Fail(core.AlreadyExists, err), parentErr)
		}
		change, err := nativeChange(err, core.AlreadyExists)
		return change, errors.Join(err, parentErr)
	}
	if err != nil || parentErr != nil {
		return core.Change{Effect: core.Incomplete}, errors.Join(localError(err), parentErr, d.closeFile(file))
	}
	var once sync.Once
	var closeErr error
	closeFile := func() error {
		once.Do(func() { closeErr = d.closeFile(file) })
		return closeErr
	}
	stop := context.AfterFunc(ctx, func() { _ = closeFile() })
	copyErr := copyInput(ctx, file, input, length)
	stop()
	fileCloseErr := closeFile()
	if copyErr != nil || fileCloseErr != nil {
		return core.Change{Effect: core.Incomplete}, errors.Join(localError(copyErr), localError(fileCloseErr))
	}
	return core.Change{Effect: core.Applied}, nil
}

// copyInput validates exact lengths and normal EOF in one streaming pass.
func copyInput(ctx context.Context, output io.Writer, input io.Reader, length *int64) error {
	source := &inputReader{ctx: ctx, source: input}
	var reader io.Reader = source
	if length != nil {
		reader = io.LimitReader(source, *length)
	}
	copied, err := io.Copy(output, reader)
	if err != nil || length == nil {
		return err
	}
	if copied != *length {
		return core.Fail(core.InvalidRequest, io.ErrUnexpectedEOF)
	}
	var extra [1]byte
	n, endErr := io.ReadFull(source, extra[:])
	if n != 0 {
		return core.Fail(core.InvalidRequest, errors.Join(errors.New("input exceeds declared length"), endErr))
	}
	if endErr != io.EOF {
		return endErr
	}
	return nil
}

// inputReader checks cancellation without taking ownership of the borrowed
// input. A blocked underlying Read still has to acknowledge its completion.
type inputReader struct {
	ctx    context.Context
	source io.Reader
}

func (r *inputReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	n, err := r.source.Read(p)
	if cause := r.ctx.Err(); cause != nil {
		return n, errors.Join(err, cause)
	}
	return n, err
}

var _ core.DirectoryMaker = (*Driver)(nil)
var _ core.Remover = (*Driver)(nil)
var _ core.FileCreator = (*Driver)(nil)
