package box

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/internal/childscan"
	"github.com/nostalume/mountweave/drivers/internal/httpcontent"
)

type item struct {
	ID, Name, Type string
	Size           *int64
	Modified       *time.Time `json:"modified_at"`
	Parent         *struct{ ID, Type string }
	meta           core.Metadata
}

type listing struct {
	d      *Driver
	parent string
	marker string
	seen   map[string]struct{}
	done   bool
}

func (s *listing) next(ctx context.Context) ([]item, error) {
	if s.done {
		return nil, io.EOF
	}
	q := url.Values{
		"usemarker": {"true"}, "limit": {"100"},
		"fields": {"id,type,name,parent,size,modified_at"},
	}
	if s.marker != "" {
		q.Set("marker", s.marker)
	}
	var page struct {
		Entries []item
		Next    string `json:"next_marker"`
	}
	if err := s.d.getJSON(ctx, folderURL(s.parent)+"/items?"+q.Encode(), &page); err != nil {
		return nil, err
	}
	if page.Entries == nil {
		return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("Box page omitted its entries"))
	}
	for k := range page.Entries {
		f := &page.Entries[k]
		if !core.ValidName(f.ID) || f.Type == "folder" && (f.ID == "0" || f.ID == s.parent) ||
			f.Parent != nil && (f.Parent.ID != s.parent || f.Parent.Type != "folder") {
			return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("Box page contains an invalid child identity"))
		}
		if !core.ValidName(f.Name) {
			return nil, core.Fail(core.UnrepresentableName, nil)
		}
		f.meta = core.Metadata{Name: f.Name, Kind: core.File, ModifiedAt: f.Modified}
		switch f.Type {
		case "folder":
			f.meta.Kind = core.Directory
		case "file":
			if f.Size != nil && *f.Size < 0 {
				return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("Box returned a negative file size"))
			}
			f.meta.Size = f.Size
		case "web_link":
		default:
			return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("Box page contains an unknown item type"))
		}
	}
	if page.Next == "" {
		s.done, s.seen = true, nil
		return page.Entries, io.EOF
	}
	if _, repeated := s.seen[page.Next]; repeated {
		return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("Box repeated a page marker"))
	}
	if s.seen == nil {
		s.seen = make(map[string]struct{})
	}
	s.seen[page.Next] = struct{}{}
	s.marker = page.Next
	return page.Entries, nil
}

func (d *Driver) resolve(ctx context.Context, address core.NativeAddress) (item, error) {
	if err := d.check(ctx, address); err != nil {
		return item{}, err
	}
	current := item{ID: d.root, meta: core.Metadata{Kind: core.Directory}}
	if address.Value() == "/" {
		return current, nil
	}
	for _, name := range strings.Split(address.Value()[1:], "/") {
		if current.meta.Kind != core.Directory {
			return item{}, core.Fail(core.NotDirectory, nil)
		}
		scan := listing{d: d, parent: current.ID}
		found, exists, err := childscan.Unique(ctx, name, scan.next, func(f item) (string, string) {
			return f.Type + ":" + f.ID, f.Name
		})
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

func (d *Driver) Stat(ctx context.Context, a core.NativeAddress, selections ...core.ReadSelection) (core.Metadata, error) {
	if len(selections) > 1 {
		return core.Metadata{}, core.Fail(core.InvalidRequest, nil)
	}
	if len(selections) == 1 && (selections[0].Version != "" || !selections[0].When.Empty()) {
		return core.Metadata{}, core.Fail(core.UnsupportedOperation, nil)
	}
	f, err := d.resolve(ctx, a)
	if err == nil && a.Value() == "/" {
		err = d.confirmRoot(ctx)
	}
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
	life, cancel := context.WithCancel(context.WithoutCancel(ctx))
	return &iterator{scan: listing{d: d, parent: f.ID}, life: life, cancel: cancel}, nil
}

type iterator struct {
	mu     sync.Mutex
	scan   listing
	life   context.Context
	cancel context.CancelFunc
}

func (i *iterator) Next(ctx context.Context, n int) ([]core.Metadata, error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.life.Err() != nil {
		return nil, io.ErrClosedPipe
	}
	if n < 1 {
		return nil, core.Fail(core.InvalidRequest, nil)
	}
	call, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(i.life, cancel)
	defer cancel()
	defer stop()
	items, err := i.scan.next(call)
	if cause := call.Err(); cause != nil {
		if err == io.EOF {
			err = nil
		}
		return nil, errors.Join(cause, err)
	}
	if err != nil && err != io.EOF {
		return nil, err
	}
	result := make([]core.Metadata, len(items))
	for k, item := range items {
		result[k] = item.meta
	}
	return result, err
}

func (i *iterator) Close() error {
	i.cancel()
	i.mu.Lock()
	i.scan.seen = nil
	i.mu.Unlock()
	return nil
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
	if f.Type == "web_link" {
		return core.Opened{}, core.Fail(core.UnsupportedOperation, errors.New("Box web links have no file body"))
	}
	if f.meta.Kind != core.File {
		return core.Opened{}, core.Fail(core.NotFile, nil)
	}
	target := endpoint + "/files/" + url.PathEscape(f.ID) + "/content"
	byteRange := httpcontent.Header(o.Range)
	response, err := httpcontent.FollowDownload(target, func(target string, delegated bool) (*http.Response, error) {
		client := &d.api
		if delegated {
			client = &d.content
		}
		return d.get(ctx, client, target, "", byteRange)
	}, d.cleanup.CloseBody)
	if err != nil {
		return core.Opened{}, err
	}
	switch response.StatusCode {
	case http.StatusOK, http.StatusPartialContent, http.StatusRequestedRangeNotSatisfiable:
		return d.cleanup.OpenBody(response, o.Range, f.Name)
	default:
		return core.Opened{}, errors.Join(responseError(response), d.cleanup.CloseBody(response.Body))
	}
}
