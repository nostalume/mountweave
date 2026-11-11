package feishudrive

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/internal/childscan"
	"github.com/nostalume/mountweave/drivers/internal/httpcontent"
)

const pageSize = 100

type item struct {
	Token  string    `json:"token"`
	Parent string    `json:"parent_token"`
	Name   string    `json:"name"`
	Kind   core.Kind `json:"type"`

	exportType string
}

func (f item) metadata() core.Metadata {
	return core.Metadata{Name: f.Name, Kind: f.Kind}
}

type page struct {
	Files []item `json:"files"`
	More  *bool  `json:"has_more"`
	Next  string `json:"next_page_token"`
}

func (d *Driver) page(ctx context.Context, parent, after string, n int) (page, error) {
	query := url.Values{"folder_token": {parent}, "page_size": {strconv.Itoa(n)}}
	if after != "" {
		query.Set("page_token", after)
	}
	var p page
	resp, _, err := d.request(ctx, http.MethodGet, "/files?"+query.Encode(), "", nil)
	if err != nil {
		return p, err
	}
	if err := d.envelope(resp, &p); err != nil {
		return p, err
	}
	if p.More == nil || *p.More && (p.Next == "" || p.Next == after) {
		return p, core.Fail(core.InvalidUpstreamResponse, errors.New("missing or stalled Feishu page continuation"))
	}
	for idx := range p.Files {
		f := &p.Files[idx]
		if !validToken(f.Token) || f.Token == parent || f.Parent != parent || f.Kind == "" {
			return p, core.Fail(core.InvalidUpstreamResponse, errors.New("invalid Feishu child identity or parent"))
		}
		if !core.ValidName(f.Name) {
			return p, core.Fail(core.UnrepresentableName, nil)
		}
		switch f.Kind {
		case "folder":
			f.Kind = core.Directory
		case "file":
			f.Kind = core.File
		default:
			if d.exports[string(f.Kind)] != "" {
				f.exportType, f.Kind = string(f.Kind), core.File
			} else {
				// Keep the native identity for exact-name checks.
				f.Kind = ""
			}
		}
	}
	return p, ctx.Err()
}

type listing struct {
	d      *Driver
	parent string
	after  string
	done   bool
}

func (l *listing) next(ctx context.Context) ([]item, error) {
	if l.done {
		return nil, io.EOF
	}
	p, err := l.d.page(ctx, l.parent, l.after, pageSize)
	if err != nil {
		return nil, err
	}
	if !*p.More {
		l.done = true
		return p.Files, io.EOF
	}
	l.after = p.Next
	return p.Files, nil
}

func (d *Driver) resolve(ctx context.Context, value string) (item, error) {
	current := item{Token: d.root, Kind: core.Directory}
	if value == "/" {
		return current, nil
	}
	for _, name := range strings.Split(strings.TrimPrefix(value, "/"), "/") {
		if current.Kind != core.Directory {
			return item{}, core.Fail(core.NotDirectory, nil)
		}
		l := listing{d: d, parent: current.Token}
		count := 0
		next := func(ctx context.Context) ([]item, error) {
			if d.resolvePages > 0 && count == d.resolvePages {
				return nil, core.Fail(core.ResolutionBudgetExceeded, nil)
			}
			count++
			return l.next(ctx)
		}
		child, found, err := childscan.Unique(ctx, name, next, func(f item) (string, string) { return f.Token, f.Name })
		if err != nil {
			return item{}, err
		}
		if !found {
			return item{}, core.Fail(core.NotFound, nil)
		}
		if child.Kind == "" {
			return item{}, core.Fail(core.UnsupportedOperation, errors.New("Feishu item does not have an original file body"))
		}
		current = child
	}
	return current, nil
}

func (d *Driver) Stat(ctx context.Context, address core.NativeAddress, selections ...core.ReadSelection) (core.Metadata, error) {
	if len(selections) > 1 {
		return core.Metadata{}, core.Fail(core.InvalidRequest, nil)
	}
	var selection core.ReadSelection
	if len(selections) == 1 {
		selection = selections[0]
	}
	if selection.Version != "" {
		return core.Metadata{}, core.Fail(core.UnsupportedOperation, nil)
	}
	if err := selection.When.ForDomain(""); err != nil {
		return core.Metadata{}, err
	}
	if err := d.check(ctx, address); err != nil {
		return core.Metadata{}, err
	}
	f, err := d.resolve(ctx, address.Value())
	if err != nil {
		return core.Metadata{}, err
	}
	if address.Value() == "/" {
		if _, err := d.page(ctx, d.root, "", 1); err != nil {
			return core.Metadata{}, err
		}
	}
	meta := f.metadata()
	return meta, selection.Check(meta.Evidence)
}

func (d *Driver) Open(ctx context.Context, address core.NativeAddress, options core.OpenOptions) (core.Opened, error) {
	if err := options.Validate(); err != nil {
		return core.Opened{}, err
	}
	if options.Mode != core.StreamRead || options.NoFollow || options.Selection.Version != "" {
		return core.Opened{}, core.Fail(core.UnsupportedOperation, nil)
	}
	selectionErr := options.Selection.Check(core.Evidence{})
	if selectionErr != nil && core.CodeOf(selectionErr) != core.PreconditionFailed {
		return core.Opened{}, selectionErr
	}
	if err := d.check(ctx, address); err != nil {
		return core.Opened{}, err
	}
	f, err := d.resolve(ctx, address.Value())
	if err != nil {
		return core.Opened{}, err
	}
	if f.Kind != core.File {
		return core.Opened{}, core.Fail(core.NotFile, nil)
	}
	if selectionErr != nil {
		return core.Opened{}, selectionErr
	}
	wanted := options.Range
	route := "/files/" + url.PathEscape(f.Token) + "/download"
	var exportedSize *int64
	if f.exportType != "" {
		result, err := d.export(ctx, f)
		if err != nil {
			return core.Opened{}, err
		}
		route = "/export_tasks/file/" + url.PathEscape(result.Token) + "/download"
		wanted, exportedSize = nil, result.Size
	}
	resp, _, err := d.request(ctx, http.MethodGet, route, httpcontent.Header(wanted), nil)
	if err != nil {
		return core.Opened{}, err
	}
	switch resp.StatusCode {
	case http.StatusOK, http.StatusPartialContent, http.StatusRequestedRangeNotSatisfiable:
		if f.exportType != "" && resp.StatusCode != http.StatusOK {
			return core.Opened{Body: resp.Body}, core.Fail(core.InvalidUpstreamResponse, errors.New("Feishu export returned a partial or unsatisfied body"))
		}
		var opened core.Opened
		if exportedSize == nil {
			opened, err = httpcontent.Open(resp, wanted)
		} else {
			opened, err = httpcontent.OpenSized(resp, wanted, *exportedSize)
			if err == nil {
				opened.Metadata.Size = exportedSize
			}
		}
		opened.Metadata.Name = f.Name
		return opened, err
	default:
		return core.Opened{}, d.envelope(resp, nil)
	}
}

func (d *Driver) Enumerate(ctx context.Context, query core.NativeQuery) (core.Iterator, error) {
	if query.Delimiter() != "" || query.StartAfter() != "" {
		return nil, core.Fail(core.UnsupportedOperation, nil)
	}
	if err := d.check(ctx, query.Address()); err != nil {
		return nil, err
	}
	f, err := d.resolve(ctx, query.Address().Value())
	if err != nil {
		return nil, err
	}
	if f.Kind != core.Directory {
		return nil, core.Fail(core.NotDirectory, nil)
	}
	life, cancel := context.WithCancel(context.WithoutCancel(ctx))
	return &iterator{listing: listing{d: d, parent: f.Token}, life: life, cancel: cancel}, nil
}

type iterator struct {
	listing
	life   context.Context
	cancel context.CancelFunc
	mu     sync.Mutex
	failed error
}

func (i *iterator) Next(ctx context.Context, n int) (items []core.Metadata, err error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.life.Err() != nil {
		return nil, io.ErrClosedPipe
	}
	if n < 1 {
		return nil, core.Fail(core.InvalidRequest, nil)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if i.failed != nil {
		return nil, i.failed
	}
	call, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(i.life, cancel)
	defer func() {
		stop()
		cancel()
		if cause := ctx.Err(); cause != nil {
			if err == io.EOF {
				err = nil
			}
			err = errors.Join(err, cause)
		}
		if err != nil && err != io.EOF {
			i.failed = err
		}
	}()
	files, err := i.listing.next(call)
	if err != nil && err != io.EOF {
		return nil, err
	}
	items = make([]core.Metadata, 0, len(files))
	for _, f := range files {
		if f.Kind != "" {
			items = append(items, f.metadata())
		}
	}
	return items, err
}

func (i *iterator) Close() error {
	i.cancel()
	i.mu.Lock()
	i.mu.Unlock()
	return nil
}
