package teldrive

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/internal/childscan"
	"github.com/nostalume/mountweave/drivers/internal/httpcontent"
)

type item struct {
	ID, ParentID, Name, Kind, Status string
	Size                             *int64
	Modified                         *time.Time `json:"modTime"`
	meta                             core.Metadata
}

func (f *item) admit() error {
	if !validID(f.ID) || f.Status != "active" || f.Size != nil && *f.Size < 0 {
		return core.Fail(core.InvalidUpstreamResponse, errors.New("invalid Teldrive item"))
	}
	if !core.ValidName(f.Name) {
		return core.Fail(core.UnrepresentableName, nil)
	}
	f.meta = core.Metadata{Name: f.Name, ModifiedAt: f.Modified}
	switch f.Kind {
	case "file":
		f.meta.Kind, f.meta.Size = core.File, f.Size
	case "folder":
		f.meta.Kind = core.Directory
	default:
		return core.Fail(core.InvalidUpstreamResponse, errors.New("unknown Teldrive item kind"))
	}
	return nil
}

type iterator struct {
	driver             *Driver
	parent, nextCursor string
	seen               map[string]struct{}
	done, closed       bool
}

func (i *iterator) next(ctx context.Context, n int) ([]item, error) {
	if err := i.driver.check(ctx); err != nil {
		return nil, err
	}
	if i.closed {
		return nil, io.ErrClosedPipe
	}
	if i.done {
		return nil, io.EOF
	}
	if n < 1 {
		return nil, core.Fail(core.InvalidRequest, nil)
	}
	limit := min(n, 500)
	q := url.Values{"scope": {"folder"}, "status": {"active"}, "limit": {strconv.Itoa(limit)}}
	if i.parent != "" {
		q.Set("parentId", i.parent)
	}
	if i.nextCursor != "" {
		q.Set("cursor", i.nextCursor)
	}
	var page struct {
		Items      []item
		NextCursor string
	}
	if err := i.driver.json(ctx, "?"+q.Encode(), &page); err != nil {
		return nil, err
	}
	if _, repeated := i.seen[page.NextCursor]; repeated || page.Items == nil || len(page.Items) > limit ||
		page.NextCursor != "" && len(page.Items) == 0 {
		return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("invalid Teldrive cursor page"))
	}
	for n := range page.Items {
		f := &page.Items[n]
		if f.ParentID != i.parent {
			return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("Teldrive returned an item outside its direct parent"))
		}
		if err := f.admit(); err != nil {
			return nil, err
		}
	}
	i.nextCursor, i.done = page.NextCursor, page.NextCursor == ""
	if i.done {
		i.seen = nil
		return page.Items, io.EOF
	}
	if i.seen == nil {
		i.seen = make(map[string]struct{})
	}
	i.seen[i.nextCursor] = struct{}{}
	return page.Items, nil
}

func (i *iterator) Next(ctx context.Context, n int) ([]core.Metadata, error) {
	items, err := i.next(ctx, n)
	if err != nil && err != io.EOF {
		return nil, err
	}
	result := make([]core.Metadata, len(items))
	for k, f := range items {
		result[k] = f.meta
	}
	return result, err
}

func (i *iterator) Close() error {
	i.closed, i.nextCursor, i.seen = true, "", nil
	return nil
}

func (d *Driver) resolve(ctx context.Context, a core.NativeAddress) (item, error) {
	if err := d.check(ctx); err != nil {
		return item{}, err
	}
	if a.Namespace() != core.TreeNamespace {
		return item{}, core.Fail(core.UnsupportedOperation, nil)
	}
	if err := core.ValidatePath(a.Value()); err != nil {
		return item{}, err
	}
	current := item{ID: d.root, meta: core.Metadata{Kind: core.Directory}}
	if a.Value() == "/" {
		return current, nil
	}
	for _, name := range strings.Split(a.Value()[1:], "/") {
		if current.meta.Kind != core.Directory {
			return item{}, core.Fail(core.NotDirectory, nil)
		}
		children := iterator{driver: d, parent: current.ID}
		f, found, err := childscan.Unique(ctx, name, func(ctx context.Context) ([]item, error) {
			return children.next(ctx, core.DefaultPage)
		}, func(f item) (string, string) { return f.ID, f.Name })
		if err != nil {
			return item{}, err
		}
		if !found {
			return item{}, core.Fail(core.NotFound, nil)
		}
		current = f
	}
	return current, nil
}

func (d *Driver) Stat(ctx context.Context, a core.NativeAddress, selections ...core.ReadSelection) (core.Metadata, error) {
	if len(selections) > 1 {
		return core.Metadata{}, core.Fail(core.InvalidRequest, nil)
	}
	if len(selections) == 1 && (selections[0].Version != "" || !selections[0].When.Empty()) {
		return core.Metadata{}, core.Fail(core.UnsupportedOperation, nil)
	}
	f, err := d.resolve(ctx, a)
	return f.meta, err
}

func (d *Driver) Enumerate(ctx context.Context, q core.NativeQuery) (core.Iterator, error) {
	if q.Delimiter() != "" || q.StartAfter() != "" {
		return nil, core.Fail(core.UnsupportedOperation, nil)
	}
	f, err := d.resolve(ctx, q.Address())
	if err != nil {
		return nil, err
	}
	if f.meta.Kind != core.Directory {
		return nil, core.Fail(core.NotDirectory, nil)
	}
	return &iterator{driver: d, parent: f.ID}, nil
}

func (d *Driver) Open(ctx context.Context, a core.NativeAddress, o core.OpenOptions) (core.Opened, error) {
	if err := o.Validate(); err != nil {
		return core.Opened{}, err
	}
	if o.Mode != core.StreamRead || o.Selection.Version != "" || !o.Selection.When.Empty() {
		return core.Opened{}, core.Fail(core.UnsupportedOperation, nil)
	}
	f, err := d.resolve(ctx, a)
	if err != nil {
		return core.Opened{}, err
	}
	if f.meta.Kind != core.File {
		return core.Opened{}, core.Fail(core.NotFile, nil)
	}
	route := "/" + f.ID + "/content/" + url.PathEscape(f.Name)
	resp, err := d.get(ctx, route, "", httpcontent.Header(o.Range))
	if err != nil {
		return core.Opened{}, err
	}
	switch resp.StatusCode {
	case http.StatusOK, http.StatusPartialContent, http.StatusRequestedRangeNotSatisfiable:
		return d.cleanup.OpenBody(resp, o.Range, f.Name)
	default:
		return core.Opened{}, errors.Join(responseError(resp.StatusCode), d.cleanup.CloseBody(resp.Body))
	}
}
