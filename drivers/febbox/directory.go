package febbox

import (
	"context"
	"errors"
	"io"
	"math"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/internal/childscan"
	"github.com/nostalume/mountweave/drivers/internal/httpcontent"
)

const nativePageSize = 100

type nativeID = httpcontent.DecimalID

type item struct {
	ID       nativeID  `json:"fid"`
	Parent   nativeID  `json:"parent_id"`
	Name     string    `json:"file_name"`
	Dir      *int      `json:"is_dir"`
	Size     *int64    `json:"file_size"`
	Updated  int64     `json:"update_time"`
	Share    int       `json:"is_share"`
	ShareFID *nativeID `json:"share_fid"`
	FromUID  *nativeID `json:"from_uid"`
	meta     core.Metadata
}

func (f item) shared() bool { return f.Share == 1 || nonzero(f.ShareFID) || nonzero(f.FromUID) }

func nonzero(id *nativeID) bool { return id != nil && *id != "0" }

func (f *item) prepare() error {
	if f.ID == "" || f.ID == "0" || f.Parent == "" || f.ID == f.Parent ||
		f.Dir == nil || (*f.Dir != 0 && *f.Dir != 1) || (f.Share != 0 && f.Share != 1) ||
		(f.Size != nil && *f.Size < 0) {
		return core.Fail(core.InvalidUpstreamResponse, errors.New("invalid FEB Box file metadata"))
	}
	if !core.ValidName(f.Name) {
		return core.Fail(core.UnrepresentableName, nil)
	}
	f.meta = core.Metadata{Name: f.Name, Kind: core.Directory}
	if *f.Dir == 0 {
		f.meta.Kind, f.meta.Size = core.File, f.Size
	}
	if f.Updated > 0 {
		modified := time.Unix(f.Updated, 0).UTC()
		f.meta.ModifiedAt = &modified
	}
	return nil
}

func (d *Driver) readRoot(ctx context.Context, id string) (item, error) {
	if id == "0" {
		_, _, err := d.readPage(ctx, "0", 1, 1)
		return item{ID: "0", meta: core.Metadata{Kind: core.Directory}}, err
	}
	var result struct {
		File item `json:"file"`
	}
	if err := d.call(ctx, "file_info", url.Values{"fid": {id}}, &result); err != nil {
		return item{}, err
	}
	if string(result.File.ID) != id {
		return item{}, core.Fail(core.InvalidUpstreamResponse, errors.New("file details changed the selected root"))
	}
	if result.File.shared() {
		return item{}, core.Fail(core.UnsupportedOperation, errors.New("subscribed shares have a separate directory scope"))
	}
	if err := result.File.prepare(); err != nil {
		return item{}, err
	}
	return result.File, nil
}

func (d *Driver) readPage(ctx context.Context, parent nativeID, number, size int) ([]item, bool, error) {
	var page struct {
		Items    []item    `json:"file_list"`
		Number   *int      `json:"page"`
		Size     *int      `json:"pagelimit"`
		More     *int      `json:"has_more"`
		ShareFID *nativeID `json:"share_fid"`
		FromUID  *nativeID `json:"from_uid"`
	}
	if err := d.call(ctx, "file_list", url.Values{
		"parent_id": {string(parent)}, "page": {strconv.Itoa(number)},
		"pagelimit": {strconv.Itoa(size)}, "order": {"name_asc"},
	}, &page); err != nil {
		return nil, false, err
	}
	if nonzero(page.ShareFID) || nonzero(page.FromUID) {
		return nil, false, core.Fail(core.UnsupportedOperation, errors.New("listing switched to a subscribed share"))
	}
	if page.Items == nil || len(page.Items) > size || page.More == nil || (*page.More != 0 && *page.More != 1) ||
		(page.Number != nil && *page.Number != number) || (page.Size != nil && *page.Size != size) {
		return nil, false, core.Fail(core.InvalidUpstreamResponse, errors.New("invalid FEB Box page receipt"))
	}
	owned := page.Items[:0]
	for _, f := range page.Items {
		if f.shared() {
			continue
		}
		if f.Parent != parent {
			return nil, false, core.Fail(core.InvalidUpstreamResponse, errors.New("listing returned a different parent"))
		}
		if err := f.prepare(); err != nil {
			return nil, false, err
		}
		owned = append(owned, f)
	}
	return owned, *page.More == 1, nil
}

type listing struct {
	driver *Driver
	parent nativeID
	page   int // Zero after the terminal page.
}

func (l *listing) next(ctx context.Context) ([]item, error) {
	if l.page == 0 {
		return nil, io.EOF
	}
	items, more, err := l.driver.readPage(ctx, l.parent, l.page, nativePageSize)
	if err != nil {
		return nil, err
	}
	if !more {
		l.page = 0
		return items, io.EOF
	}
	if l.page == math.MaxInt {
		return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("page number overflow"))
	}
	l.page++
	return items, nil
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
		pages := listing{driver: d, parent: current.ID, page: 1}
		remaining := d.resolvePages
		found, exists, err := childscan.Unique(ctx, name, func(call context.Context) ([]item, error) {
			if d.resolvePages > 0 {
				if remaining == 0 {
					return nil, core.Fail(core.ResolutionBudgetExceeded, nil)
				}
				remaining--
			}
			return pages.next(call)
		}, func(f item) (string, string) { return string(f.ID), f.Name })
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
	if len(selections) > 1 {
		return core.Metadata{}, core.Fail(core.InvalidRequest, nil)
	}
	var selected core.ReadSelection
	if len(selections) == 1 {
		selected = selections[0]
	}
	if selected.Version != "" {
		return core.Metadata{}, core.Fail(core.UnsupportedOperation, nil)
	}
	if err := selected.When.ForDomain(""); err != nil {
		return core.Metadata{}, err
	}
	if err := d.check(ctx, address); err != nil {
		return core.Metadata{}, err
	}
	var meta core.Metadata
	if address.Value() == "/" {
		root, err := d.readRoot(ctx, string(d.root.ID))
		if err != nil {
			return core.Metadata{}, err
		}
		if root.meta.Kind != core.Directory {
			return core.Metadata{}, core.Fail(core.NotDirectory, nil)
		}
		meta = root.meta
		meta.Name = ""
	} else {
		file, err := d.resolve(ctx, address.Value())
		if err != nil {
			return core.Metadata{}, err
		}
		meta = file.meta
	}
	return meta, selected.Check(meta.Evidence)
}

func (d *Driver) Enumerate(ctx context.Context, query core.NativeQuery) (core.Iterator, error) {
	if query.Delimiter() != "" || query.StartAfter() != "" {
		return nil, core.Fail(core.UnsupportedOperation, nil)
	}
	if err := d.check(ctx, query.Address()); err != nil {
		return nil, err
	}
	parent, err := d.resolve(ctx, query.Address().Value())
	if err != nil {
		return nil, err
	}
	if parent.meta.Kind != core.Directory {
		return nil, core.Fail(core.NotDirectory, nil)
	}
	life, cancel := context.WithCancel(context.WithoutCancel(ctx))
	return &iterator{listing: listing{driver: d, parent: parent.ID, page: 1}, life: life, cancel: cancel}, nil
}

type iterator struct {
	listing
	life   context.Context
	cancel context.CancelFunc
	mu     sync.Mutex
}

func (i *iterator) Next(ctx context.Context, size int) ([]core.Metadata, error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.life.Err() != nil {
		return nil, io.ErrClosedPipe
	}
	if size < 1 {
		return nil, core.Fail(core.InvalidRequest, nil)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	call, cancel := context.WithCancel(ctx)
	defer cancel()
	stop := context.AfterFunc(i.life, cancel)
	defer stop()
	files, err := i.listing.next(call)
	if err != nil && err != io.EOF {
		return nil, err
	}
	out := make([]core.Metadata, 0, len(files))
	for _, f := range files {
		out = append(out, f.meta)
	}
	return out, err
}

func (i *iterator) Close() error {
	i.cancel()
	i.mu.Lock()
	defer i.mu.Unlock()
	return nil
}
