package pan115

import (
	"context"
	"errors"
	"io"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/internal/childscan"
)

type item struct {
	meta      core.Metadata
	ID        nativeID `json:"fid"`
	Parent    nativeID `json:"pid"`
	Name      string   `json:"fn"`
	Category  string   `json:"fc"`
	Pick      string   `json:"pc"`
	Size      *int64   `json:"fs"`
	Encrypted *int     `json:"isp"`
	State     string   `json:"aid"`
	Uploaded  string   `json:"fta"`
	Modified  *int64   `json:"upt"`
}

func (f *item) prepare() error {
	m := core.Metadata{Name: f.Name, Kind: core.Directory}
	if f.ID == "" || !core.ValidName(f.Name) && f.ID != "0" {
		return core.Fail(core.InvalidUpstreamResponse, errors.New("invalid native file identity or name"))
	}
	if f.State != "" && f.State != "1" {
		return core.Fail(core.UpstreamAccessDenied, errors.New("native item is not active"))
	}
	if f.Encrypted != nil && *f.Encrypted != 0 && *f.Encrypted != 1 {
		return core.Fail(core.UnsupportedOperation, errors.New("encrypted or unknown directory state"))
	}
	switch f.Category {
	case "0":
	case "1":
		if f.Size == nil || *f.Size < 0 {
			return core.Fail(core.InvalidUpstreamResponse, nil)
		}
		if f.Uploaded != "" && f.Uploaded != "1" {
			return core.Fail(core.UnsupportedOperation, errors.New("native file upload is not complete"))
		}
		m.Kind, m.Size = core.File, f.Size
	default:
		return core.Fail(core.InvalidUpstreamResponse, errors.New("unknown native file category"))
	}
	if f.Modified != nil {
		if *f.Modified < 0 {
			return core.Fail(core.InvalidUpstreamResponse, nil)
		}
		t := time.Unix(*f.Modified, 0).UTC()
		m.ModifiedAt = &t
	}
	f.meta = m
	return nil
}
func (d *Driver) info(ctx context.Context, id string) (item, error) {
	var out struct {
		Data struct {
			ID       nativeID `json:"file_id"`
			Name     string   `json:"file_name"`
			Category string   `json:"file_category"`
			Pick     string   `json:"pick_code"`
			Size     *int64   `json:"size_byte"`
		} `json:"data"`
	}
	err := d.call(ctx, "GET", "/open/folder/get_info", url.Values{"file_id": {id}}, nil, &out)
	f := item{ID: out.Data.ID, Name: out.Data.Name, Category: out.Data.Category, Pick: out.Data.Pick, Size: out.Data.Size}
	if err != nil {
		return f, err
	}
	if string(f.ID) != id {
		return f, core.Fail(core.InvalidUpstreamResponse, errors.New("details changed file ID"))
	}
	err = f.prepare()
	return f, err
}

type page struct {
	Data   []item   `json:"data"`
	Count  *int64   `json:"count"`
	Offset *int64   `json:"offset"`
	Limit  *int64   `json:"limit"`
	ID     nativeID `json:"cid"`
	State  string   `json:"aid"`
	Path   []struct {
		ID        nativeID `json:"cid"`
		Encrypted *int     `json:"isp"`
	} `json:"path"`
	readable bool
}

func (d *Driver) readPage(ctx context.Context, parent string, offset int64, n int) (page, error) {
	var p page
	q := url.Values{"cid": {parent}, "offset": {strconv.FormatInt(offset, 10)}, "limit": {strconv.Itoa(min(n, 1150))}, "cur": {"1"}, "show_dir": {"1"}, "o": {"file_name"}, "asc": {"1"}, "custom_order": {"1"}}
	if err := d.call(ctx, "GET", "/open/ufile/files", q, nil, &p); err != nil {
		return p, err
	}
	if string(p.ID) != parent || p.Data == nil || p.Count == nil || p.Offset == nil || p.Limit == nil || *p.Offset != offset || *p.Limit < 1 || *p.Limit > 1150 || *p.Count < offset || int64(len(p.Data)) > *p.Limit || int64(len(p.Data)) > *p.Count-offset {
		return p, core.Fail(core.InvalidUpstreamResponse, errors.New("invalid native directory page"))
	}
	if p.State != "" && p.State != "1" {
		return p, core.Fail(core.UpstreamAccessDenied, errors.New("directory is not active"))
	}
	for _, a := range p.Path {
		if a.Encrypted != nil && *a.Encrypted != 0 {
			return p, core.Fail(core.UnsupportedOperation, errors.New("encrypted directory contents are not available"))
		}
		if string(a.ID) == parent && a.Encrypted != nil {
			p.readable = true
		}
	}
	if parent == "0" || len(p.Data) != 0 {
		p.readable = true
	}
	if len(p.Data) == 0 && offset < *p.Count {
		return p, core.Fail(core.InvalidUpstreamResponse, errors.New("native page cannot advance"))
	}
	for k := range p.Data {
		f := &p.Data[k]
		if string(f.Parent) != parent || f.ID == "0" || f.ID == f.Parent {
			return p, core.Fail(core.InvalidUpstreamResponse, errors.New("listing changed parent identity"))
		}
		if err := f.prepare(); err != nil {
			return p, err
		}
	}
	return p, nil
}
func (d *Driver) resolve(ctx context.Context, p string) (item, error) {
	current := d.root
	if p == "/" {
		return current, nil
	}
	for _, name := range strings.Split(strings.TrimPrefix(p, "/"), "/") {
		remaining := d.resolvePages
		if current.Encrypted != nil && *current.Encrypted != 0 {
			return item{}, core.Fail(core.UnsupportedOperation, errors.New("encrypted directory contents are not available"))
		}
		if current.Category != "0" {
			return item{}, core.Fail(core.NotDirectory, nil)
		}
		parent := string(current.ID)
		offset := int64(0)
		next := func(call context.Context) ([]item, error) {
			if d.resolvePages > 0 {
				if remaining == 0 {
					return nil, core.Fail(core.ResolutionBudgetExceeded, nil)
				}
				remaining--
			}
			page, err := d.readPage(call, parent, offset, 1150)
			if err != nil {
				return nil, err
			}
			if len(page.Data) == 0 && !page.readable && (current.Encrypted == nil || *current.Encrypted != 0) {
				return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("empty directory omitted encryption state"))
			}
			offset += int64(len(page.Data))
			if offset == *page.Count {
				return page.Data, io.EOF
			}
			return page.Data, nil
		}
		found, exists, err := childscan.Unique(ctx, name, next, func(f item) (string, string) { return string(f.ID), f.Name })
		if err != nil {
			return item{}, err
		}
		if !exists {
			return item{}, core.Fail(core.NotFound, nil)
		}
		current = found
	}
	return current, nil
}
func (d *Driver) Enumerate(ctx context.Context, q core.NativeQuery) (core.Iterator, error) {
	if q.Delimiter() != "" || q.StartAfter() != "" {
		return nil, core.Fail(core.UnsupportedOperation, nil)
	}
	if err := d.check(ctx, q.Address()); err != nil {
		return nil, err
	}
	parent, err := d.resolve(ctx, q.Address().Value())
	if err != nil {
		return nil, err
	}
	if parent.Encrypted != nil && *parent.Encrypted != 0 {
		return nil, core.Fail(core.UnsupportedOperation, errors.New("encrypted directory contents are not available"))
	}
	if parent.Category != "0" {
		return nil, core.Fail(core.NotDirectory, nil)
	}
	life, cancel := context.WithCancel(context.WithoutCancel(ctx))
	return &iterator{d: d, parent: string(parent.ID), readable: parent.Encrypted != nil && *parent.Encrypted == 0, life: life, cancel: cancel}, nil
}

type iterator struct {
	d            *Driver
	parent       string
	readable     bool
	offset       int64
	life         context.Context
	cancel       context.CancelFunc
	mu           sync.Mutex
	done, closed bool
	failed       error
}

func (i *iterator) Close() error {
	i.cancel()
	i.mu.Lock()
	defer i.mu.Unlock()
	i.closed = true
	return nil
}
func (i *iterator) Next(ctx context.Context, n int) (items []core.Metadata, err error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.closed {
		return nil, io.ErrClosedPipe
	}
	if n < 1 {
		return nil, core.Fail(core.InvalidRequest, nil)
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	if i.failed != nil {
		return nil, i.failed
	}
	if i.done {
		return nil, io.EOF
	}
	call, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(i.life, cancel)
	defer func() {
		stop()
		cancel()
		if err != nil && err != io.EOF {
			i.failed = err
		}
	}()
	page, err := i.d.readPage(call, i.parent, i.offset, n)
	if err != nil {
		return nil, err
	}
	if !page.readable && !i.readable {
		return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("directory omitted encryption state"))
	}
	items = make([]core.Metadata, 0, len(page.Data))
	for _, f := range page.Data {
		items = append(items, f.meta)
	}
	i.offset += int64(len(page.Data))
	i.done = i.offset == *page.Count
	if i.done {
		return items, io.EOF
	}
	return items, nil
}
