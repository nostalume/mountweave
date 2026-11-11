package s3

import (
	"context"

	core "github.com/nostalume/mountweave"
)

type directoryScope struct{ *iterator }

func (d *Driver) OpenDirectory(ctx context.Context, p core.NativeAddress) (core.DirectoryScope, error) {
	if p.Namespace() != core.TreeNamespace {
		return nil, core.Fail(core.UnsupportedOperation, nil)
	}
	iter, err := d.directoryIterator(ctx, p)
	if err != nil {
		return nil, err
	}
	iter.files = make(map[string]core.Metadata)
	return &directoryScope{iterator: iter}, nil
}

func (s *directoryScope) OpenFile(ctx context.Context, name string) (core.Opened, error) {
	if s.closed || !s.done || !core.ValidName(name) {
		return core.Opened{}, core.Fail(core.InvalidRequest, nil)
	}
	meta, exists := s.files[name]
	if !exists {
		return core.Opened{}, core.Fail(core.NotFound, nil)
	}
	if meta.Kind != core.File {
		return core.Opened{}, core.Fail(core.NotFile, nil)
	}
	if err := s.driver.admit(ctx, core.NativePath("/"+name)); err != nil {
		return core.Opened{}, err
	}
	return s.driver.get(ctx, s.prefix+name, name, nil)
}

var _ core.DirectoryReader = (*Driver)(nil)
