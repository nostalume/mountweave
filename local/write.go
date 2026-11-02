package local

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
	"path"
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

func (d *Driver) changeTarget(ctx context.Context, rel string, existing bool) error {
	if err := core.ValidatePath(rel); err != nil {
		return err
	}
	if rel == "/" {
		return core.Fail(core.InvalidRequest, nil)
	}
	if existing {
		return d.exact(ctx, rel)
	}
	return d.exact(ctx, path.Dir(rel))
}

func nativeChange(err error, conflict core.Code) (core.Change, error) {
	if err == nil {
		return core.Change{Effect: core.Applied}, nil
	}
	if errors.Is(err, fs.ErrExist) {
		return core.Change{Effect: core.Unchanged}, core.Fail(conflict, err)
	}
	if errors.Is(err, fs.ErrNotExist) || errors.Is(err, fs.ErrPermission) {
		return core.Change{Effect: core.Unchanged}, localError(err)
	}
	return core.Change{Effect: core.Unknown}, localError(err)
}

func (d *Driver) Mkdir(ctx context.Context, rel string) (core.Change, error) {
	if err := d.changeTarget(ctx, rel, false); err != nil {
		return core.Change{Effect: core.Unchanged}, err
	}
	if err := ctx.Err(); err != nil {
		return core.Change{Effect: core.Unchanged}, err
	}
	return nativeChange(d.root.Mkdir(native(rel), 0700), core.AlreadyExists)
}

func (d *Driver) Remove(ctx context.Context, rel string) (core.Change, error) {
	if err := d.changeTarget(ctx, rel, true); err != nil {
		return core.Change{Effect: core.Unchanged}, err
	}
	if err := ctx.Err(); err != nil {
		return core.Change{Effect: core.Unchanged}, err
	}
	return nativeChange(d.root.Remove(native(rel)), core.DirectoryNotEmpty)
}

func (d *Driver) Create(ctx context.Context, rel string, input io.Reader, length *int64) (core.Change, error) {
	if input == nil || (length != nil && *length < 0) {
		return core.Change{Effect: core.Unchanged}, core.Fail(core.InvalidRequest, nil)
	}
	if err := d.changeTarget(ctx, rel, false); err != nil {
		return core.Change{Effect: core.Unchanged}, err
	}
	if err := ctx.Err(); err != nil {
		return core.Change{Effect: core.Unchanged}, err
	}
	file, err := d.root.OpenFile(native(rel), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		// Some platforms report an existing directory as EISDIR rather than
		// EEXIST. Exclusive creation has not changed that target.
		if errors.Is(err, syscall.EISDIR) {
			return core.Change{Effect: core.Unchanged}, core.Fail(core.AlreadyExists, err)
		}
		return nativeChange(err, core.AlreadyExists)
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
