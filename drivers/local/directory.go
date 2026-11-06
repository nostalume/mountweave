package local

import (
	"context"
	"errors"
	"io"
	"os"
	"strings"

	core "github.com/nostalume/mountweave"
)

type childMode uint8

const (
	inspectChild childMode = iota
	leafChild
	directoryChild
	readChild
	relocateChild
	deleteChild
	mkdirChild
	stageChild
	editChild
)

type directoryScope struct {
	driver       *Driver
	file         *os.File
	items        map[string]core.Kind
	done, closed bool
	closeErr     error
}

func (d *Driver) OpenDirectory(ctx context.Context, relAddress core.NativeAddress) (core.DirectoryScope, error) {
	if relAddress.Namespace() != core.TreeNamespace {
		return nil, core.Fail(core.UnsupportedOperation, nil)
	}
	file, err := d.openDirectory(ctx, relAddress.Value())
	if err != nil {
		return nil, err
	}
	return &directoryScope{driver: d, file: file, items: make(map[string]core.Kind)}, nil
}

func (d *Driver) openDirectory(ctx context.Context, rel string) (*os.File, error) {
	if !directoryScopesSupported {
		return nil, core.Fail(core.UnsupportedOperation, nil)
	}
	if err := core.ValidatePath(rel); err != nil {
		return nil, err
	}
	file, err := d.root.OpenFile(".", readFlags(), 0)
	if err != nil {
		return nil, localError(err)
	}
	fail := func(err error) (*os.File, error) { return nil, errors.Join(err, d.closeFile(file)) }
	if err = exactDirectory(file); err != nil {
		return fail(err)
	}
	if rel != "/" {
		for _, name := range strings.Split(rel[1:], "/") {
			if err = ctx.Err(); err != nil {
				return fail(err)
			}
			next, _, openErr := d.openChild(file, name, directoryChild)
			if openErr != nil {
				return fail(localError(openErr))
			}
			if closeErr := d.closeFile(file); closeErr != nil {
				return nil, errors.Join(closeErr, d.closeFile(next))
			}
			file = next
			if err = exactDirectory(file); err != nil {
				return fail(err)
			}
		}
	}
	if err = ctx.Err(); err != nil {
		return fail(err)
	}
	return file, nil
}

func (d *Driver) openParent(ctx context.Context, rel string) (*os.File, string, error) {
	if err := core.ValidatePath(rel); err != nil {
		return nil, "", err
	}
	if rel == "/" {
		return nil, "", core.Fail(core.InvalidRequest, nil)
	}
	cut := strings.LastIndexByte(rel, '/')
	parent, err := d.openDirectory(ctx, rel[:max(cut, 1)])
	return parent, rel[cut+1:], err
}

func (s *directoryScope) Next(ctx context.Context, n int) ([]core.Metadata, error) {
	if s.closed {
		return nil, io.ErrClosedPipe
	}
	if s.done {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if n < 1 {
			return nil, core.Fail(core.InvalidRequest, nil)
		}
		return nil, io.EOF
	}
	items, err := s.driver.readDirectory(ctx, s.file, n)
	for _, item := range items {
		if _, exists := s.items[item.Name]; exists {
			return nil, core.Fail(core.PathAmbiguous, nil)
		}
		s.items[item.Name] = item.Kind
	}
	s.done = errors.Is(err, io.EOF)
	return items, err
}

func (d *Driver) openEdit(ctx context.Context, parent *os.File, name string, flags int) (*os.File, os.FileInfo, error) {
	if flags&os.O_EXCL != 0 {
		unlock, err := d.names.lock(ctx, parent, nil)
		if err != nil {
			return nil, nil, err
		}
		defer unlock()
	}
	return openChild(parent, name, editChild, flags)
}

func (d *Driver) readDirectory(ctx context.Context, parent *os.File, n int) ([]core.Metadata, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if n < 1 {
		return nil, core.Fail(core.InvalidRequest, nil)
	}
	names, err := parent.Readdirnames(n)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, localError(err)
	}
	items := make([]core.Metadata, 0, len(names))
	for _, name := range names {
		if cause := ctx.Err(); cause != nil {
			return nil, cause
		}
		if !core.ValidName(name) {
			return nil, core.Fail(core.UnrepresentableName, nil)
		}
		meta, metaErr := d.statChild(ctx, parent, name, inspectChild)
		if metaErr != nil {
			return nil, metaErr
		}
		items = append(items, meta)
	}
	return items, err
}

func (s *directoryScope) OpenFile(ctx context.Context, name string) (core.Opened, error) {
	if s.closed || !s.done || !core.ValidName(name) {
		return core.Opened{}, core.Fail(core.InvalidRequest, nil)
	}
	kind, exists := s.items[name]
	if !exists {
		return core.Opened{}, core.Fail(core.NotFound, nil)
	}
	if kind != core.File {
		return core.Opened{}, core.Fail(core.NotFile, nil)
	}
	if err := ctx.Err(); err != nil {
		return core.Opened{}, err
	}
	file, info, err := s.driver.openChild(s.file, name, readChild)
	if err != nil {
		return core.Opened{}, localError(err)
	}
	meta, err := metadata(name, info)
	if err != nil || meta.Kind != core.File {
		return core.Opened{}, errors.Join(core.Fail(core.NotFile, err), s.driver.closeFile(file))
	}
	size := info.Size()
	return core.Opened{Metadata: meta, Body: file, Length: &size}, ctx.Err()
}

// RemoveChild operates on the bound parent, without requiring enumeration.
func (s *directoryScope) RemoveChild(ctx context.Context, name string, options core.RemoveOptions) (change core.Change, err error) {
	change.Effect = core.Unchanged
	if s.closed || !core.ValidName(name) {
		return change, core.Fail(core.InvalidRequest, nil)
	}
	change, err = s.driver.removeWith(ctx, s.file, name, options)
	if cause := ctx.Err(); cause != nil && !errors.Is(err, cause) {
		err = errors.Join(err, cause)
	}
	return change, err
}

func (d *Driver) removeWith(ctx context.Context, parent *os.File, name string, options core.RemoveOptions) (core.Change, error) {
	unchanged := core.Change{Effect: core.Unchanged}
	if !options.When.Empty() {
		return unchanged, core.Fail(core.UnsupportedOperation, nil)
	}
	if options.Target != "" && options.Target != core.RemoveNonDirectory && options.Target != core.RemoveEmptyDirectory {
		return unchanged, core.Fail(core.InvalidRequest, nil)
	}
	if err := ctx.Err(); err != nil {
		return unchanged, err
	}
	return d.removeChild(ctx, parent, name, options.Target)
}

var _ core.ChildRemover = (*directoryScope)(nil)

func (s *directoryScope) Close() error {
	if s.closed {
		return s.closeErr
	}
	s.closed = true
	s.items = nil
	s.closeErr = s.driver.closeFile(s.file)
	return s.closeErr
}

var _ core.DirectoryReader = (*Driver)(nil)

func (d *Driver) openChild(parent *os.File, name string, mode childMode) (*os.File, os.FileInfo, error) {
	file, info, err := openChild(parent, name, mode)
	if err != nil && file != nil {
		return nil, nil, errors.Join(err, d.closeFile(file))
	}
	return file, info, err
}
