package alipan

import (
	"context"

	core "github.com/nostalume/mountweave"
)

type directoryScope struct{ *iterator }

func (d *Driver) OpenDirectory(ctx context.Context, pAddress core.NativeAddress) (core.DirectoryScope, error) {
	if pAddress.Namespace() != core.TreeNamespace {
		return nil, core.Fail(core.UnsupportedOperation, nil)
	}
	p := pAddress.Value()

	f, err := d.resolve(ctx, p)
	if err != nil {
		return nil, err
	}
	if f.Type != "folder" {
		return nil, core.Fail(core.NotDirectory, nil)
	}
	return &directoryScope{iterator: &iterator{driver: d, parent: f.ID, files: make(map[string]nativeFile)}}, nil
}

func (s *directoryScope) OpenFile(ctx context.Context, name string) (core.Opened, error) {
	if s.closed || !s.done || !core.ValidName(name) {
		return core.Opened{}, core.Fail(core.InvalidRequest, nil)
	}
	f, exists := s.files[name]
	if !exists {
		return core.Opened{}, core.Fail(core.NotFound, nil)
	}
	if f.Type != "file" {
		return core.Opened{}, core.Fail(core.NotFile, nil)
	}
	return s.driver.openFile(ctx, f, nil)
}

var _ core.DirectoryReader = (*Driver)(nil)
