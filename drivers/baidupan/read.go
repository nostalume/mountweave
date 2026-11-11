package baidupan

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"math"
	"net/url"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	core "github.com/nostalume/mountweave"
)

type nativeItem struct {
	Errno     *int    `json:"errno"`
	ID        *uint64 `json:"fs_id"`
	Path      string  `json:"path"`
	Name      string  `json:"server_filename"`
	Filename  string  `json:"filename"`
	Directory *int    `json:"isdir"`
	Size      *int64  `json:"size"`
	Modified  *int64  `json:"server_mtime"`
	Deleted   *int    `json:"isdelete"`
	Dlink     string  `json:"dlink"`
}

type item struct {
	meta core.Metadata
	link string
}

func accept(native nativeItem, expected string) (item, error) {
	if native.ID == nil || native.Directory == nil || (*native.Directory != 0 && *native.Directory != 1) {
		return item{}, core.Fail(core.InvalidUpstreamResponse, errors.New("native item identity or type is invalid"))
	}
	if native.Path != expected {
		return item{}, core.Fail(core.InvalidUpstreamResponse, errors.New("native item changed the exact path"))
	}
	name := path.Base(expected)
	if expected == "/" {
		name = ""
	}
	if native.Name == "" && native.Filename == "" && expected != "/" ||
		native.Name != "" && native.Name != name && !(expected == "/" && native.Name == "/") ||
		native.Filename != "" && native.Filename != name && !(expected == "/" && native.Filename == "/") {
		return item{}, core.Fail(core.InvalidUpstreamResponse, errors.New("native filename and path disagree"))
	}
	if native.Deleted != nil && *native.Deleted != 0 {
		return item{}, core.Fail(core.NotFound, nil)
	}
	f := item{meta: core.Metadata{Name: name, Kind: core.Directory}, link: native.Dlink}
	if *native.Directory == 0 {
		if native.Size == nil || *native.Size < 0 {
			return item{}, core.Fail(core.InvalidUpstreamResponse, errors.New("native file size is missing or invalid"))
		}
		f.meta.Kind, f.meta.Size = core.File, native.Size
	}
	if native.Modified != nil {
		if *native.Modified < 0 {
			return item{}, core.Fail(core.InvalidUpstreamResponse, errors.New("native modification time is invalid"))
		}
		modified := time.Unix(*native.Modified, 0)
		f.meta.ModifiedAt = &modified
	}
	return f, nil
}

func (d *Driver) address(p core.NativeAddress) (string, error) {
	if p.Namespace() != core.TreeNamespace {
		return "", core.Fail(core.UnsupportedOperation, nil)
	}
	if err := core.ValidatePath(p.Value()); err != nil {
		return "", err
	}
	if p.Value() == "/" {
		return d.root, nil
	}
	if d.root == "/" {
		return p.Value(), nil
	}
	return d.root + p.Value(), nil
}

func (d *Driver) metadata(ctx context.Context, p, token string, body bool) (item, error) {
	target, _ := json.Marshal([]string{p})
	args := url.Values{"method": {"filemetas"}, "target": {string(target)}, "blocks": {"0"}, "media": {"0"}}
	if body {
		args.Set("dlink", "1")
	}
	result, err := d.call(ctx, args, token, true)
	if err != nil {
		return item{}, err
	}
	if result.Info == nil || len(*result.Info) != 1 {
		return item{}, core.Fail(core.InvalidUpstreamResponse, errors.New("native path query did not return one result"))
	}
	native := (*result.Info)[0]
	if native.Errno == nil {
		return item{}, core.Fail(core.InvalidUpstreamResponse, errors.New("native path result omitted errno"))
	}
	if *native.Errno != 0 {
		return item{}, nativeFailure(*native.Errno, result, token)
	}
	f, err := accept(native, p)
	if err == nil && p == d.root && f.meta.Kind != core.Directory {
		return item{}, core.Fail(core.NotDirectory, nil)
	}
	return f, err
}

func (d *Driver) Stat(ctx context.Context, p core.NativeAddress, selections ...core.ReadSelection) (core.Metadata, error) {
	if len(selections) > 1 {
		return core.Metadata{}, core.Fail(core.InvalidRequest, nil)
	}
	if len(selections) == 1 && (selections[0].Version != "" || !selections[0].When.Empty()) {
		return core.Metadata{}, core.Fail(core.UnsupportedOperation, nil)
	}
	full, err := d.address(p)
	if err != nil {
		return core.Metadata{}, err
	}
	token, err := d.credential(ctx)
	if err != nil {
		return core.Metadata{}, err
	}
	f, err := d.metadata(ctx, full, token, false)
	if p.Value() == "/" {
		f.meta.Name = ""
	}
	return f.meta, err
}

func (d *Driver) Enumerate(ctx context.Context, q core.NativeQuery) (core.Iterator, error) {
	if q.Delimiter() != "" || q.StartAfter() != "" {
		return nil, core.Fail(core.UnsupportedOperation, nil)
	}
	full, err := d.address(q.Address())
	if err != nil {
		return nil, err
	}
	token, err := d.credential(ctx)
	if err != nil {
		return nil, err
	}
	parent, err := d.metadata(ctx, full, token, false)
	if err != nil {
		return nil, err
	}
	if parent.meta.Kind != core.Directory {
		return nil, core.Fail(core.NotDirectory, nil)
	}
	life, cancel := context.WithCancel(context.WithoutCancel(ctx))
	return &iterator{d: d, parent: full, life: life, cancel: cancel}, nil
}

type iterator struct {
	d            *Driver
	parent       string
	offset       int64
	life         context.Context
	cancel       context.CancelFunc
	mu           sync.Mutex
	closed, done bool
	failed       error
}

func (i *iterator) Close() error {
	i.cancel()
	i.mu.Lock()
	defer i.mu.Unlock()
	i.closed = true
	return nil
}

func (i *iterator) Next(ctx context.Context, n int) ([]core.Metadata, error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.closed {
		return nil, io.ErrClosedPipe
	}
	if n < 1 {
		return nil, core.Fail(core.InvalidRequest, nil)
	}
	if i.failed != nil {
		return nil, i.failed
	}
	if i.done {
		return nil, io.EOF
	}
	call, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(i.life, cancel)
	defer func() { stop(); cancel() }()
	items, err := i.page(call)
	if err == nil {
		err = call.Err()
	}
	if err != nil {
		i.failed = err
		return nil, err
	}
	if len(items) == 0 {
		i.done = true
		return nil, io.EOF
	}
	return items, nil
}

func (i *iterator) page(ctx context.Context) ([]core.Metadata, error) {
	token, err := i.d.credential(ctx)
	if err != nil {
		return nil, err
	}
	args := url.Values{"method": {"list"}, "dir": {i.parent}, "start": {strconv.FormatInt(i.offset, 10)},
		"limit": {"1000"}, "order": {"name"}, "desc": {"0"}, "folder": {"0"}}
	result, err := i.d.call(ctx, args, token, false)
	if err != nil {
		return nil, err
	}
	if result.List == nil || i.offset > math.MaxInt64-1000 {
		return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("native directory window is invalid"))
	}
	native := *result.List
	items := make([]core.Metadata, len(native))
	for k, entry := range native {
		if entry.Errno != nil && *entry.Errno != 0 {
			return nil, nativeFailure(*entry.Errno, result, token)
		}
		name := path.Base(entry.Path)
		if !core.ValidName(name) {
			return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("listing returned an invalid child name"))
		}
		expected := strings.TrimSuffix(i.parent, "/") + "/" + name
		f, err := accept(entry, expected)
		if err != nil {
			return nil, err
		}
		items[k] = f.meta
	}
	i.offset += 1000
	return items, nil
}

var _ core.Driver = (*Driver)(nil)
