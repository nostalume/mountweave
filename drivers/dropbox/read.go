package dropbox

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	core "github.com/nostalume/mountweave"
)

type nativeMetadata struct {
	Tag          string     `json:".tag"`
	ID           string     `json:"id"`
	Name         string     `json:"name"`
	Size         *int64     `json:"size"`
	Modified     *time.Time `json:"server_modified"`
	Downloadable *bool      `json:"is_downloadable"`
}

type item struct {
	ID   string
	meta core.Metadata
}

// FileMetadata is not a Metadata tagged union. Only typed file responses
// supply file=true; RPC metadata must retain its tag.
func accept(m nativeMetadata, file bool) (item, error) {
	tag := m.Tag
	if file && tag == "" {
		tag = "file"
	}
	if (file && tag != "file") || (tag != "file" && tag != "folder") {
		return item{}, core.Fail(core.InvalidUpstreamResponse, errors.New("unexpected native metadata variant"))
	}
	if !strings.HasPrefix(m.ID, "id:") || len(m.ID) <= 3 || !utf8.ValidString(m.ID) ||
		strings.ContainsAny(m.ID, "/\\\r\n") || !core.ValidName(m.Name) {
		return item{}, core.Fail(core.InvalidUpstreamResponse, errors.New("invalid native item identity or name"))
	}
	f := item{ID: m.ID, meta: core.Metadata{Name: m.Name, Kind: core.Directory}}
	if tag == "file" {
		if m.Size == nil || *m.Size < 0 {
			return item{}, core.Fail(core.InvalidUpstreamResponse, errors.New("invalid native file size"))
		}
		f.meta.Kind, f.meta.ModifiedAt = core.File, m.Modified
		if m.Downloadable == nil || *m.Downloadable {
			f.meta.Size = m.Size
		}
	}
	return f, nil
}

func childPath(parent item, name string) string {
	return parent.ID + "/" + name
}

func (d *Driver) admit(ctx context.Context, p string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if d.closed.Load() {
		return core.Fail(core.MountUnavailable, nil)
	}
	return core.ValidatePath(p)
}

func (d *Driver) resolve(ctx context.Context, p string) (item, error) {
	if err := d.admit(ctx, p); err != nil {
		return item{}, err
	}
	return d.walk(ctx, p)
}

// walk consumes a path already validated at the public operation's boundary.
func (d *Driver) walk(ctx context.Context, p string) (item, error) {
	current := d.root
	if p == "/" {
		return current, nil
	}
	for _, name := range strings.Split(strings.TrimPrefix(p, "/"), "/") {
		if current.meta.Kind != core.Directory {
			return item{}, core.Fail(core.NotDirectory, nil)
		}
		found, err := d.lookup(ctx, current, name)
		if err != nil {
			return item{}, err
		}
		current = found
	}
	return current, nil
}

func (d *Driver) lookup(ctx context.Context, parent item, name string) (item, error) {
	var native nativeMetadata
	if err := d.rpc(ctx, "/files/get_metadata", map[string]any{"path": childPath(parent, name), "include_deleted": false}, &native); err != nil {
		return item{}, err
	}
	found, err := accept(native, false)
	if err != nil {
		return item{}, err
	}
	if found.meta.Name != name {
		return item{}, core.Fail(core.NotFound, nil)
	}
	return found, nil
}
func (d *Driver) Stat(ctx context.Context, p core.NativeAddress, selections ...core.ReadSelection) (core.Metadata, error) {
	if len(selections) > 1 {
		return core.Metadata{}, core.Fail(core.InvalidRequest, nil)
	}
	if p.Namespace() != core.TreeNamespace || len(selections) == 1 && (selections[0].Version != "" || !selections[0].When.Empty()) {
		return core.Metadata{}, core.Fail(core.UnsupportedOperation, nil)
	}
	f, err := d.resolve(ctx, p.Value())
	if err != nil {
		return core.Metadata{}, err
	}
	meta := f.meta
	if p.Value() == "/" {
		meta.Name = ""
	}
	return meta, nil
}

func (d *Driver) page(ctx context.Context, parent item, cursor string, n int) ([]core.Metadata, string, bool, error) {
	route := "/files/list_folder"
	argument := map[string]any{
		"path": parent.ID, "recursive": false, "include_deleted": false,
		"include_mounted_folders": true, "include_non_downloadable_files": true, "limit": min(n, 2000),
	}
	if cursor != "" {
		route = "/files/list_folder/continue"
		argument = map[string]any{"cursor": cursor}
	}
	var result struct {
		Entries *[]nativeMetadata `json:"entries"`
		Cursor  string            `json:"cursor"`
		More    *bool             `json:"has_more"`
	}
	if err := d.rpc(ctx, route, argument, &result); err != nil {
		return nil, "", false, err
	}
	if result.Entries == nil || result.Cursor == "" || result.More == nil {
		return nil, "", false, core.Fail(core.InvalidUpstreamResponse, errors.New("incomplete native directory page"))
	}
	items := make([]core.Metadata, len(*result.Entries))
	for n, native := range *result.Entries {
		f, err := accept(native, false)
		if err != nil {
			return nil, "", false, err
		}
		items[n] = f.meta
	}
	return items, result.Cursor, *result.More, nil
}

func (d *Driver) Enumerate(ctx context.Context, q core.NativeQuery) (core.Iterator, error) {
	if q.Address().Namespace() != core.TreeNamespace || q.Delimiter() != "" || q.StartAfter() != "" {
		return nil, core.Fail(core.UnsupportedOperation, nil)
	}
	parent, err := d.resolve(ctx, q.Address().Value())
	if err != nil {
		return nil, err
	}
	if parent.meta.Kind != core.Directory {
		return nil, core.Fail(core.NotDirectory, nil)
	}
	life, cancel := context.WithCancel(context.WithoutCancel(ctx))
	return &iterator{d: d, parent: parent, life: life, cancel: cancel}, nil
}

type iterator struct {
	d            *Driver
	parent       item
	life         context.Context
	cancel       context.CancelFunc
	mu           sync.Mutex
	cursor       string
	done, closed bool
	failed       error
}

func (i *iterator) Close() error {
	i.cancel()
	i.mu.Lock()
	defer i.mu.Unlock()
	i.closed = true
	i.cursor = ""
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
	items, cursor, more, err := i.d.page(call, i.parent, i.cursor, n)
	if err == nil {
		err = call.Err()
	}
	if err != nil {
		i.failed = err
		return nil, err
	}
	i.cursor, i.done = cursor, !more
	if i.done {
		i.cursor = ""
		return items, io.EOF
	}
	return items, nil
}

var _ core.Driver = (*Driver)(nil)
