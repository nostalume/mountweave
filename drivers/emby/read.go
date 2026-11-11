package emby

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
	ID, ParentID, Name, LocationType string
	IsFolder                         *bool
	meta                             core.Metadata
}

type listing struct {
	d      *Driver
	parent string
	offset int64
	done   bool
}

func (s *listing) next(ctx context.Context) ([]item, error) {
	if s.done {
		return nil, io.EOF
	}
	query := url.Values{
		"ParentId": {s.parent}, "Recursive": {"false"}, "Fields": {"ParentId"},
		"SortBy": {"SortName"}, "SortOrder": {"Ascending"}, "EnableTotalRecordCount": {"true"},
		"StartIndex": {strconv.FormatInt(s.offset, 10)}, "Limit": {strconv.Itoa(pageSize)},
	}
	var page struct {
		Items            []item
		TotalRecordCount *int32
	}
	if err := s.d.getJSON(ctx, s.d.userRoute+"/Items?"+query.Encode(), &page); err != nil {
		return nil, err
	}
	count := len(page.Items)
	if page.Items == nil || page.TotalRecordCount == nil || *page.TotalRecordCount < 0 || count > pageSize ||
		count == 0 && s.offset < int64(*page.TotalRecordCount) {
		return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("invalid Emby library page"))
	}
	for i := range page.Items {
		f := &page.Items[i]
		// Some servers fall back to the user root if the selected parent has
		// disappeared. Require the native display parent before using any ID.
		if !core.ValidName(f.ID) || f.ID == s.parent || f.ParentID != s.parent {
			return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("Emby item is outside the selected parent"))
		}
		if !core.ValidName(f.Name) {
			return nil, core.Fail(core.UnrepresentableName, nil)
		}
		f.meta = core.Metadata{Name: f.Name, Kind: core.File}
		if f.IsFolder != nil && *f.IsFolder {
			f.meta.Kind = core.Directory
		}
	}
	s.offset += int64(count)
	s.done = s.offset >= int64(*page.TotalRecordCount)
	if s.done {
		return page.Items, io.EOF
	}
	return page.Items, nil
}

func (d *Driver) lookup(ctx context.Context, a core.NativeAddress) (item, error) {
	if err := ctx.Err(); err != nil {
		return item{}, err
	}
	if d.closed.Load() {
		return item{}, core.Fail(core.MountUnavailable, nil)
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
		scan := listing{d: d, parent: current.ID}
		found, exists, err := childscan.Unique(ctx, name, scan.next, func(f item) (string, string) { return f.ID, f.Name })
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
	f, err := d.lookup(ctx, a)
	if err == nil && a.Value() == "/" {
		err = d.confirmRoot(ctx)
	}
	return f.meta, err
}

func (d *Driver) Enumerate(ctx context.Context, q core.NativeQuery) (core.Iterator, error) {
	if q.Delimiter() != "" || q.StartAfter() != "" {
		return nil, core.Fail(core.UnsupportedOperation, nil)
	}
	f, err := d.lookup(ctx, q.Address())
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
	f, err := d.lookup(ctx, a)
	if err != nil {
		return core.Opened{}, err
	}
	if f.meta.Kind != core.File {
		return core.Opened{}, core.Fail(core.NotFile, nil)
	}
	if f.IsFolder == nil || f.LocationType != "FileSystem" {
		return core.Opened{}, core.Fail(core.UnsupportedOperation, errors.New("Emby item has no verified local original file"))
	}
	resp, err := d.get(ctx, "/Items/"+url.PathEscape(f.ID)+"/Download", "", httpcontent.Header(o.Range))
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
