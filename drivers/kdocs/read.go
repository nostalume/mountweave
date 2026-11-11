package kdocs

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

type identity struct {
	OpenID string `json:"open_id"`
}

type item struct {
	ID, Parent identity
	Name       string `json:"fname"`
	Kind       string `json:"ftype"`
	Size       *int64 `json:"fsize"`
	Modified   *int64 `json:"mtime"`
	meta       core.Metadata
}

func (f *item) admit(parent string) error {
	if !core.ValidName(f.ID.OpenID) || parent != "" && f.Parent.OpenID != parent || f.Size != nil && *f.Size < 0 {
		return core.Fail(core.InvalidUpstreamResponse, errors.New("invalid KDocs file or parent identity"))
	}
	if !core.ValidName(f.Name) {
		return core.Fail(core.UnrepresentableName, nil)
	}
	f.meta.Name = f.Name
	switch f.Kind {
	case "file":
		f.meta.Kind, f.meta.Size = core.File, f.Size
	case "folder":
		f.meta.Kind = core.Directory
	default:
		return core.Fail(core.UnsupportedOperation, errors.New("unsupported KDocs item type"))
	}
	if f.Modified != nil {
		t := time.Unix(*f.Modified, 0).UTC()
		f.meta.ModifiedAt = &t
	}
	return nil
}

type position struct {
	offset int64
	filter string
}

type iterator struct {
	driver       *Driver
	parent       string
	nextPosition position
	seen         map[position]struct{}
	done, closed bool
}

type page struct {
	Files      []item
	NextOffset *int64 `json:"next_offset"`
	NextFilter string `json:"next_filter"`
}

func (i *iterator) next(ctx context.Context, n int) ([]item, error) {
	if i.closed {
		return nil, io.ErrClosedPipe
	}
	if i.done {
		return nil, io.EOF
	}
	if n < 1 {
		return nil, core.Fail(core.InvalidRequest, nil)
	}
	query := url.Values{"count": {strconv.Itoa(n)}}
	if i.parent != "" {
		query.Set("parent_id", i.parent)
	}
	if i.nextPosition.filter != "" {
		query.Set("offset", strconv.FormatInt(i.nextPosition.offset, 10))
		query.Set("filter", i.nextPosition.filter)
	}
	p, err := fetch[page](ctx, i.driver, "", query)
	if err != nil {
		return nil, err
	}
	if p.Files == nil || p.NextOffset == nil || *p.NextOffset < -1 {
		return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("invalid KDocs list response"))
	}
	for n := range p.Files {
		if err := p.Files[n].admit(i.parent); err != nil {
			return nil, err
		}
	}
	if *p.NextOffset == -1 {
		i.done, i.seen = true, nil
		return p.Files, io.EOF
	}
	next := position{offset: *p.NextOffset, filter: p.NextFilter}
	if _, repeated := i.seen[next]; repeated || next.filter != "folder" && next.filter != "file" {
		return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("invalid KDocs list continuation"))
	}
	if i.seen == nil {
		i.seen = make(map[position]struct{})
	}
	i.seen[next] = struct{}{}
	i.nextPosition = next
	return p.Files, nil
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
	i.closed, i.seen = true, nil
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
	current := item{ID: identity{OpenID: d.root}, meta: core.Metadata{Kind: core.Directory}}
	if a.Value() == "/" {
		return current, nil
	}
	for _, name := range strings.Split(a.Value()[1:], "/") {
		if current.meta.Kind != core.Directory {
			return item{}, core.Fail(core.NotDirectory, nil)
		}
		children := iterator{driver: d, parent: current.ID.OpenID}
		f, found, err := childscan.Unique(ctx, name, func(ctx context.Context) ([]item, error) {
			return children.next(ctx, core.DefaultPage)
		}, func(f item) (string, string) { return f.ID.OpenID, f.Name })
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
	return &iterator{driver: d, parent: f.ID.OpenID}, nil
}

type download struct {
	URL  string
	Size *int64 `json:"fsize"`
}

func (d *Driver) Open(ctx context.Context, a core.NativeAddress, o core.OpenOptions) (core.Opened, error) {
	if err := o.Validate(); err != nil {
		return core.Opened{}, err
	}
	if o.Mode != core.StreamRead || o.NoFollow || o.Selection.Version != "" || !o.Selection.When.Empty() {
		return core.Opened{}, core.Fail(core.UnsupportedOperation, nil)
	}
	f, err := d.resolve(ctx, a)
	if err != nil {
		return core.Opened{}, err
	}
	if f.meta.Kind != core.File {
		return core.Opened{}, core.Fail(core.NotFile, nil)
	}
	source, err := fetch[download](ctx, d, "/"+url.PathEscape(f.ID.OpenID)+"/download", url.Values{})
	if err != nil {
		return core.Opened{}, err
	}
	if source.Size == nil || *source.Size < 0 {
		return core.Opened{}, core.Fail(core.InvalidUpstreamResponse, errors.New("invalid KDocs download size"))
	}
	resp, err := d.get(ctx, &d.content, source.URL, httpcontent.Header(o.Range))
	if err != nil {
		return core.Opened{}, err
	}
	switch resp.StatusCode {
	case http.StatusOK, http.StatusPartialContent, http.StatusRequestedRangeNotSatisfiable:
		return d.cleanup.OpenSizedBody(resp, o.Range, f.Name, *source.Size)
	default:
		return core.Opened{}, errors.Join(statusError(resp.StatusCode), d.cleanup.CloseBody(resp.Body))
	}
}
