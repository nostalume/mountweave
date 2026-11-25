package pan123

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"math/bits"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/internal/childscan"
	"github.com/nostalume/mountweave/drivers/internal/httpcontent"
)

// reader shares path resolution, page ownership, and body handling across the
// Open API and website. Each source validates its own native receipts.
type reader struct {
	responseReader
	root         item
	content      http.Client
	resolvePages int
	closed       atomic.Bool
	page         func(context.Context, string, string, int) ([]item, string, error)
	verifyRoot   func(context.Context) (item, error)
	grant        func(context.Context, item) (string, *int64, error)
}

func newReader(client *http.Client, maxResolvePages int) *reader {
	d := &reader{root: item{ID: "0", meta: core.Metadata{Kind: core.Directory}},
		content: httpcontent.IsolatedClient(client), resolvePages: maxResolvePages}
	d.content.CheckRedirect = d.cleanup.RedirectDownload
	return d
}

func (d *reader) active(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if d.closed.Load() {
		return core.Fail(core.MountUnavailable, nil)
	}
	return nil
}

func (d *reader) check(ctx context.Context, p core.NativeAddress) error {
	if err := d.active(ctx); err != nil {
		return err
	}
	if p.Namespace() != core.TreeNamespace {
		return core.Fail(core.UnsupportedOperation, nil)
	}
	return core.ValidatePath(p.Value())
}

type item struct {
	meta    core.Metadata
	ID      nativeID `json:"fileId"`
	Parent  nativeID `json:"parentFileId"`
	Name    string   `json:"filename"`
	Type    *int     `json:"type"`
	Size    *int64   `json:"size"`
	Trashed *int     `json:"trashed"`
}

// prepare admits native item facts once; consumers use the admitted metadata.
func (f *item) prepare() error {
	if f.ID == "" || f.ID == "0" || f.Parent == "" || f.ID == f.Parent ||
		f.Type == nil || f.Trashed == nil || (*f.Trashed != 0 && *f.Trashed != 1) {
		return core.Fail(core.InvalidUpstreamResponse, errors.New("invalid native file identity or state"))
	}
	if *f.Trashed == 1 {
		return core.Fail(core.NotFound, nil)
	}
	if !core.ValidName(f.Name) {
		return core.Fail(core.UnrepresentableName, nil)
	}
	meta := core.Metadata{Name: f.Name, Kind: core.Directory}
	switch *f.Type {
	case 0:
		if f.Size == nil || *f.Size < 0 {
			return core.Fail(core.InvalidUpstreamResponse, nil)
		}
		meta.Kind, meta.Size = core.File, f.Size
	case 1:
	default:
		return core.Fail(core.InvalidUpstreamResponse, errors.New("unknown native file type"))
	}
	f.meta = meta
	return nil
}

func (d *Driver) info(ctx context.Context, id string) (item, error) {
	var f item
	if err := d.call(ctx, "/api/v1/file/detail", url.Values{"fileID": {id}}, &f); err != nil {
		return f, err
	}
	if string(f.ID) != id {
		return f, core.Fail(core.InvalidUpstreamResponse, errors.New("details changed file ID"))
	}
	err := f.prepare()
	return f, err
}

func (d *Driver) statRoot(ctx context.Context) (item, error) {
	if d.root.ID != "0" {
		return d.info(ctx, string(d.root.ID))
	}
	_, _, err := d.readPage(ctx, "0", "", 1)
	return d.root, err
}

func (d *Driver) download(ctx context.Context, file item) (string, *int64, error) {
	route, idParam := downloadRoute, "fileId"
	if d.directLinks {
		route, idParam = directLinkRoute, "fileID"
	}
	var capability struct {
		DownloadURL string `json:"downloadUrl"`
		DirectURL   string `json:"url"`
	}
	if err := d.call(ctx, route, url.Values{idParam: {string(file.ID)}}, &capability); err != nil {
		return "", nil, err
	}
	if d.directLinks {
		return capability.DirectURL, nil, nil
	}
	return capability.DownloadURL, nil, nil
}

func (d *Driver) readPage(ctx context.Context, parent, after string, n int) ([]item, string, error) {
	query := url.Values{"parentFileId": {parent}, "limit": {strconv.Itoa(min(n, 100))}}
	if after != "" {
		query.Set("lastFileId", after)
	}
	var page struct {
		Last  *json.Number `json:"lastFileId"`
		Items []item       `json:"fileList"`
	}
	if err := d.call(ctx, "/api/v2/file/list", query, &page); err != nil {
		return nil, "", err
	}
	if page.Last == nil || page.Items == nil || len(page.Items) > 100 {
		return nil, "", core.Fail(core.InvalidUpstreamResponse, errors.New("invalid native page"))
	}
	next := page.Last.String()
	if next != "-1" && (!httpcontent.ValidDecimalID(next) || next == after) {
		return nil, "", core.Fail(core.InvalidUpstreamResponse, errors.New("invalid native continuation"))
	}
	active := page.Items[:0]
	for k := range page.Items {
		f := &page.Items[k]
		if string(f.Parent) != parent || f.Trashed == nil || (*f.Trashed != 0 && *f.Trashed != 1) {
			return nil, "", core.Fail(core.InvalidUpstreamResponse, errors.New("listing changed parent or omitted recycle state"))
		}
		if *f.Trashed == 1 {
			continue
		}
		if err := f.prepare(); err != nil {
			return nil, "", err
		}
		active = append(active, *f)
	}
	return active, next, nil
}

// listing owns only the provider's continuation, shared by lookup and Iterator.
// Filtering recycle-bin entries never changes this progress or its terminal mark.
type listing struct {
	d             *reader
	parent, after string
	checkpoint    string
	pages         uint64
	done          bool
}

func (s *listing) next(ctx context.Context, n int) ([]item, error) {
	if s.done {
		return nil, io.EOF
	}
	if err := s.d.active(ctx); err != nil {
		return nil, err
	}
	files, next, err := s.d.page(ctx, s.parent, s.after, n)
	if err != nil {
		return nil, err
	}
	if next == "-1" {
		s.done = true
		return files, io.EOF
	}
	if next == s.checkpoint {
		return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("native continuation cycle"))
	}
	// Power-of-two checkpoints detect a repeating cycle without saving every cursor.
	s.pages++
	if bits.OnesCount64(s.pages) == 1 {
		s.checkpoint = next
	}
	s.after = next
	return files, nil
}

func (d *reader) resolve(ctx context.Context, p string) (item, error) {
	current := d.root
	if p == "/" {
		return current, nil
	}
	for _, name := range strings.Split(strings.TrimPrefix(p, "/"), "/") {
		if current.meta.Kind != core.Directory {
			return item{}, core.Fail(core.NotDirectory, nil)
		}
		stream := listing{d: d, parent: string(current.ID)}
		remaining := d.resolvePages
		next := func(call context.Context) ([]item, error) {
			if d.resolvePages > 0 {
				if remaining == 0 {
					return nil, core.Fail(core.ResolutionBudgetExceeded, nil)
				}
				remaining--
			}
			return stream.next(call, 100)
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

func (d *reader) Enumerate(ctx context.Context, q core.NativeQuery) (core.Iterator, error) {
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
	return &iterator{listing: listing{d: d, parent: string(parent.ID)}, life: life, cancel: cancel}, nil
}

type iterator struct {
	listing
	life   context.Context
	cancel context.CancelFunc
	mu     sync.Mutex
	closed bool
	failed error
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
	files, err := i.listing.next(call, n)
	if err != nil && err != io.EOF {
		return nil, err
	}
	items = make([]core.Metadata, 0, len(files))
	for _, f := range files {
		items = append(items, f.meta)
	}
	return items, err
}
