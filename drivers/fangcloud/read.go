package fangcloud

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/internal/childscan"
	"github.com/nostalume/mountweave/drivers/internal/httpcontent"
)

const nativePageSize = 20

type folderRef struct {
	ID *int64 `json:"id"`
}

type item struct {
	ID       int64      `json:"id"`
	Name     string     `json:"name"`
	Type     string     `json:"type"`
	Size     *int64     `json:"size"`
	Modified *int64     `json:"modified_at"`
	Parent   *folderRef `json:"parent"`
	InTrash  bool       `json:"in_trash"`
	Deleted  bool       `json:"is_deleted"`
	meta     core.Metadata
}

func (f *item) prepare(kind core.Kind) error {
	if f.ID <= 0 || f.InTrash || f.Deleted {
		return core.Fail(core.InvalidUpstreamResponse, errors.New("invalid Fangcloud item identity or state"))
	}
	if !core.ValidName(f.Name) {
		return core.Fail(core.UnrepresentableName, nil)
	}
	f.meta = core.Metadata{Name: f.Name, Kind: kind}
	if kind == core.File {
		if f.Size == nil || *f.Size < 0 {
			return core.Fail(core.InvalidUpstreamResponse, errors.New("invalid Fangcloud file size"))
		}
		f.meta.Size = f.Size
	}
	if f.Modified != nil {
		t := time.Unix(*f.Modified, 0).UTC()
		f.meta.ModifiedAt = &t
	}
	return nil
}

func folderRoute(id int64) string { return "/api/v2/folder/" + strconv.FormatInt(id, 10) }

func (d *Driver) folder(ctx context.Context, id int64) (item, error) {
	var receipt struct {
		item
		Space struct {
			Type string `json:"type"`
		} `json:"space"`
	}
	if err := d.call(ctx, folderRoute(id)+"/info", nil, &receipt, false); err != nil {
		return item{}, err
	}
	if receipt.ID != id || receipt.Type != "folder" {
		return item{}, core.Fail(core.InvalidUpstreamResponse, errors.New("folder details differ from the selected ID"))
	}
	if receipt.Space.Type != "personal" {
		return item{}, core.Fail(core.UnsupportedOperation, errors.New("the selected folder is outside personal space"))
	}
	err := receipt.item.prepare(core.Directory)
	return receipt.item, err
}

func (d *Driver) readPage(ctx context.Context, parent, index int64) ([]item, bool, error) {
	query := url.Values{
		"page_id": {strconv.FormatInt(index, 10)}, "page_capacity": {strconv.Itoa(nativePageSize)},
		"type": {"all"}, "sort_by": {"name"}, "sort_direction": {"asc"},
	}
	route := "/api/v2/folder/personal_items"
	if parent != 0 {
		route = folderRoute(parent) + "/children"
		query.Set("folder_id", strconv.FormatInt(parent, 10))
	}
	var reply struct {
		Files    []item `json:"files"`
		Folders  []item `json:"folders"`
		Page     *int64 `json:"page_id"`
		Pages    *int64 `json:"page_count"`
		Capacity *int64 `json:"page_capacity"`
		Total    *int64 `json:"total_count"`
	}
	if err := d.call(ctx, route, query, &reply, false); err != nil {
		return nil, false, err
	}
	count := len(reply.Files) + len(reply.Folders)
	if reply.Page == nil || *reply.Page != index || reply.Pages == nil || *reply.Pages < 0 ||
		reply.Capacity == nil || *reply.Capacity != nativePageSize || reply.Total == nil || *reply.Total < int64(count) ||
		count > nativePageSize || index >= *reply.Pages && count != 0 {
		return nil, false, core.Fail(core.InvalidUpstreamResponse, errors.New("invalid Fangcloud page"))
	}
	done := index >= *reply.Pages-1
	if count == 0 && !done {
		return nil, false, core.Fail(core.InvalidUpstreamResponse, errors.New("empty Fangcloud page before the final page"))
	}
	for _, group := range []struct {
		items []item
		kind  core.Kind
		typ   string
	}{{reply.Folders, core.Directory, "folder"}, {reply.Files, core.File, "file"}} {
		for i := range group.items {
			f := &group.items[i]
			ancestor := f.Parent
			if ancestor != nil && (ancestor.ID == nil || *ancestor.ID != parent) ||
				f.Type != group.typ || group.kind == core.Directory && f.ID == parent {
				return nil, false, core.Fail(core.InvalidUpstreamResponse, errors.New("Fangcloud page contains an unrelated item"))
			}
			if err := f.prepare(group.kind); err != nil {
				return nil, false, err
			}
		}
	}
	return append(reply.Folders, reply.Files...), done, nil
}

type listing struct {
	d      *Driver
	parent int64
	page   int64
	done   bool
}

func (s *listing) next(ctx context.Context) ([]item, error) {
	if s.done {
		return nil, io.EOF
	}
	files, done, err := s.d.readPage(ctx, s.parent, s.page)
	if err != nil {
		return nil, err
	}
	s.page++
	s.done = done
	if done {
		return files, io.EOF
	}
	return files, nil
}

func (d *Driver) resolve(ctx context.Context, path string) (item, error) {
	current := d.root
	if path == "/" {
		return current, nil
	}
	for _, name := range strings.Split(strings.TrimPrefix(path, "/"), "/") {
		if current.meta.Kind != core.Directory {
			return item{}, core.Fail(core.NotDirectory, nil)
		}
		scan := listing{d: d, parent: current.ID}
		next := func(call context.Context) ([]item, error) {
			if d.resolvePages > 0 && scan.page >= int64(d.resolvePages) {
				return nil, core.Fail(core.ResolutionBudgetExceeded, nil)
			}
			return scan.next(call)
		}
		found, exists, err := childscan.Unique(ctx, name, next, func(f item) (string, string) {
			return f.Type + ":" + strconv.FormatInt(f.ID, 10), f.Name
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

func (d *Driver) Stat(ctx context.Context, address core.NativeAddress, selections ...core.ReadSelection) (core.Metadata, error) {
	var selection core.ReadSelection
	if len(selections) > 1 {
		return core.Metadata{}, core.Fail(core.InvalidRequest, nil)
	}
	if len(selections) == 1 {
		selection = selections[0]
	}
	if selection.Version != "" || !selection.When.Empty() {
		return core.Metadata{}, core.Fail(core.UnsupportedOperation, nil)
	}
	if err := d.check(ctx, address); err != nil {
		return core.Metadata{}, err
	}
	f, err := d.resolve(ctx, address.Value())
	if err != nil {
		return core.Metadata{}, err
	}
	if address.Value() == "/" {
		if f.ID == 0 {
			_, _, err = d.readPage(ctx, 0, 0)
		} else {
			f, err = d.folder(ctx, f.ID)
		}
		f.meta.Name = ""
	}
	return f.meta, err
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
	if parent.meta.Kind != core.Directory {
		return nil, core.Fail(core.NotDirectory, nil)
	}
	life, cancel := context.WithCancel(context.WithoutCancel(ctx))
	return &iterator{listing: listing{d: d, parent: parent.ID}, life: life, cancel: cancel}, nil
}

type iterator struct {
	listing
	life   context.Context
	cancel context.CancelFunc
	mu     sync.Mutex
	failed error
}

func (i *iterator) Close() error {
	i.cancel()
	i.mu.Lock()
	i.mu.Unlock()
	return nil
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
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if i.failed != nil {
		return nil, i.failed
	}
	call, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(i.life, cancel)
	defer cancel()
	defer stop()
	files, err := i.listing.next(call)
	if cause := call.Err(); cause != nil {
		if err == io.EOF {
			err = nil
		}
		err = errors.Join(err, cause)
	}
	if err != nil && err != io.EOF {
		i.failed = err
		return nil, err
	}
	items := make([]core.Metadata, len(files))
	for k, f := range files {
		items[k] = f.meta
	}
	return items, err
}

func (d *Driver) Open(ctx context.Context, address core.NativeAddress, o core.OpenOptions) (core.Opened, error) {
	if err := o.Validate(); err != nil {
		return core.Opened{}, err
	}
	if o.Mode != core.StreamRead || o.NoFollow || o.Selection.Version != "" || !o.Selection.When.Empty() {
		return core.Opened{}, core.Fail(core.UnsupportedOperation, nil)
	}
	if err := d.check(ctx, address); err != nil {
		return core.Opened{}, err
	}
	f, err := d.resolve(ctx, address.Value())
	if err != nil {
		return core.Opened{}, err
	}
	if f.meta.Kind != core.File {
		return core.Opened{}, core.Fail(core.NotFile, nil)
	}
	var receipt struct {
		URL string `json:"download_url"`
	}
	route := "/api/v2/file/" + strconv.FormatInt(f.ID, 10) + "/download_v2"
	if err := d.call(ctx, route, nil, &receipt, true); err != nil {
		return core.Opened{}, err
	}
	target, err := url.Parse(receipt.URL)
	if err != nil || target.Scheme != "https" || target.Hostname() == "" ||
		target.User != nil || target.Opaque != "" || target.Fragment != "" {
		return core.Opened{}, core.Fail(core.InvalidUpstreamResponse, errors.New("invalid Fangcloud content address"))
	}
	byteRange := httpcontent.Header(o.Range)
	response, err := httpcontent.FollowDownload(target.String(), func(address string, _ bool) (*http.Response, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, address, nil)
		if err != nil {
			return nil, core.Fail(core.InvalidUpstreamResponse, nil)
		}
		if byteRange != "" {
			req.Header.Set("Range", byteRange)
		}
		return d.exchange(&d.content, req, true)
	}, d.protected.CloseBody)
	if err != nil {
		return core.Opened{}, err
	}
	switch response.StatusCode {
	case http.StatusOK, http.StatusPartialContent, http.StatusRequestedRangeNotSatisfiable:
		return d.protected.OpenBody(response, o.Range, f.Name)
	default:
		return core.Opened{}, errors.Join(core.Fail(core.UpstreamFailure,
			fmt.Errorf("Fangcloud content returned HTTP %d", response.StatusCode)), d.protected.CloseBody(response.Body))
	}
}
