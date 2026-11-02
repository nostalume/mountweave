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
	directoryChild
	readChild
	relocateChild
	unlinkChild
	removeDirectoryChild
	stageChild
)

type directoryScope struct {
	driver       *Driver
	file         *os.File
	items        map[string]core.Kind
	done, closed bool
	closeErr     error
}

func (d *Driver) OpenDirectory(ctx context.Context, rel string) (core.DirectoryScope, error) {
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
	fail := func(err error) (core.DirectoryScope, error) { return nil, errors.Join(err, d.closeFile(file)) }
	if rel != "/" {
		for _, name := range strings.Split(rel[1:], "/") {
			if err = ctx.Err(); err != nil {
				return fail(err)
			}
			if err = findChild(ctx, file, name); err != nil {
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
		}
	}
	if err = ctx.Err(); err != nil {
		return fail(err)
	}
	return &directoryScope{driver: d, file: file, items: make(map[string]core.Kind)}, nil
}

// findChild checks stored spelling in an already bound parent, including on
// native filesystems whose lookup accepts case variants.
func findChild(ctx context.Context, parent *os.File, name string) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		names, err := parent.Readdirnames(core.MaxPage)
		for _, stored := range names {
			if stored == name {
				return nil
			}
		}
		if errors.Is(err, io.EOF) {
			return core.Fail(core.NotFound, nil)
		}
		if err != nil {
			return localError(err)
		}
	}
}

func (s *directoryScope) Next(ctx context.Context, n int) ([]core.Metadata, error) {
	if s.closed {
		return nil, io.ErrClosedPipe
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if n < 1 {
		return nil, core.Fail(core.InvalidRequest, nil)
	}
	if s.done {
		return nil, io.EOF
	}
	names, err := s.file.Readdirnames(n)
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
		if _, exists := s.items[name]; exists {
			return nil, core.Fail(core.PathAmbiguous, nil)
		}
		file, info, openErr := s.driver.openChild(s.file, name, inspectChild)
		if openErr != nil {
			return nil, localError(openErr)
		}
		if err := s.driver.closeFile(file); err != nil {
			return nil, localError(err)
		}
		meta, metaErr := metadata(name, info)
		if metaErr != nil {
			return nil, metaErr
		}
		s.items[name] = meta.Kind
		items = append(items, meta)
	}
	s.done = errors.Is(err, io.EOF)
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
func (s *directoryScope) RemoveChild(ctx context.Context, name string, kind core.Kind) (change core.Change, err error) {
	change.Effect = core.Unchanged
	if s.closed || !core.ValidName(name) || (kind != core.File && kind != core.Directory) {
		return change, core.Fail(core.InvalidRequest, nil)
	}
	if err = ctx.Err(); err != nil {
		return change, err
	}
	change, err = s.driver.removeChild(ctx, s.file, name, kind)
	if cause := ctx.Err(); cause != nil && !errors.Is(err, cause) {
		err = errors.Join(err, cause)
	}
	return change, err
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
