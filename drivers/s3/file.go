package s3

import (
	"context"
	"errors"
	"io"
	"sync"

	core "github.com/nostalume/mountweave"
)

func (d *Driver) bindFile(ctx context.Context, p core.NativeAddress, selection core.ReadSelection) (core.Opened, error) {
	meta, err := d.Stat(ctx, p, selection)
	if err != nil {
		return core.Opened{}, err
	}
	if meta.Kind != core.File {
		return core.Opened{}, core.Fail(core.NotFile, nil)
	}
	if meta.Evidence.Version == "" || meta.Evidence.Version == "null" {
		return core.Opened{}, core.Fail(core.UnsupportedOperation, nil)
	}
	if meta.Size == nil || *meta.Size < 0 {
		return core.Opened{}, core.Fail(core.InvalidUpstreamResponse, nil)
	}
	life, cancel := context.WithCancel(context.WithoutCancel(ctx))
	f := &boundFile{driver: d, key: d.key(p), name: meta.Name, version: meta.Evidence.Version, length: *meta.Size, life: life, cancel: cancel}
	return core.Opened{Metadata: meta, File: f, Binding: core.PinnedVersion}, nil
}

// The literal key and immutable VersionId form the bound address. No later
// request resolves the tree projection or falls back to the current version.
type boundFile struct {
	driver     *Driver
	key, name  string
	version    core.Version
	length     int64
	life       context.Context
	cancel     context.CancelFunc
	mu         sync.Mutex
	cleanupErr error
	failed     io.Closer
}

func (f *boundFile) begin(ctx context.Context) (context.Context, func(), error) {
	f.mu.Lock()
	if f.life.Err() != nil || f.driver.closed.Load() || f.cleanupErr != nil {
		err := errors.Join(io.ErrClosedPipe, f.cleanupErr)
		f.mu.Unlock()
		return nil, nil, err
	}
	call, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(f.life, cancel)
	return call, func() { stop(); cancel(); f.mu.Unlock() }, nil
}

func (f *boundFile) Stat(ctx context.Context) (core.Metadata, error) {
	call, end, err := f.begin(ctx)
	if err != nil {
		return core.Metadata{}, err
	}
	defer end()
	meta, err := f.driver.head(call, f.key, f.name, f.version)
	if err == nil && meta.Evidence.Version != f.version {
		err = core.Fail(core.InvalidUpstreamResponse, nil)
	}
	return meta, err
}

func (f *boundFile) ReadAt(ctx context.Context, p []byte, offset int64) (n int, err error) {
	if offset < 0 {
		return 0, core.Fail(core.InvalidRange, nil)
	}
	call, end, err := f.begin(ctx)
	if err != nil {
		return 0, err
	}
	defer end()
	if len(p) == 0 {
		return 0, call.Err()
	}
	if offset >= f.length {
		return 0, io.EOF
	}
	want := min(int64(len(p)), f.length-offset)
	r := &core.Range{Start: offset, End: offset + want - 1}
	opened, err := f.driver.get(call, f.key, f.name, r, core.ReadSelection{Version: f.version})
	if opened.Body != nil {
		defer func() {
			if closeErr := opened.Body.Close(); closeErr != nil {
				f.cleanupErr = closeErr
				f.failed = opened.Body
				err = errors.Join(err, closeErr)
			}
		}()
	}
	if err != nil {
		return 0, err
	}
	if opened.Body == nil || opened.Range == nil || opened.Range.Start != offset || opened.Range.End != r.End || opened.Range.Total != f.length {
		return 0, core.Fail(core.InvalidUpstreamResponse, nil)
	}
	n, err = io.ReadFull(opened.Body, p[:int(want)])
	if err == nil {
		var extra [1]byte
		count, tail := opened.Body.Read(extra[:])
		if count != 0 || tail != io.EOF {
			err = core.Fail(core.InvalidUpstreamResponse, tail)
		}
	}
	if err == nil && n < len(p) {
		err = io.EOF
	}
	return n, err
}

func (f *boundFile) Close() error {
	f.cancel()
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.cleanupErr
}
