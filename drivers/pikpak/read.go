package pikpak

import (
	"context"
	"encoding/json"
	"errors"
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

type nativeItem struct {
	ID, Kind, Name string
	Parent         string          `json:"parent_id"`
	Owner          string          `json:"user_id"`
	Size           json.RawMessage `json:"size"`
	Modified       *time.Time      `json:"modified_time"`
	meta           core.Metadata
}

func fileSize(raw json.RawMessage) (int64, error) {
	size, err := strconv.ParseInt(strings.Trim(string(raw), `"`), 10, 64)
	if err != nil || size < 0 {
		return 0, core.Fail(core.InvalidUpstreamResponse, errors.New("invalid PikPak file size"))
	}
	return size, nil
}

type listing struct {
	driver        *Driver
	parent, token string
	seen          map[string]struct{}
	done          bool
}

func (s *listing) next(ctx context.Context) ([]nativeItem, error) {
	if s.done {
		return nil, io.EOF
	}
	query := url.Values{"parent_id": {s.parent}, "limit": {"100"},
		"filters": {`{"trashed":{"eq":false},"phase":{"eq":"PHASE_TYPE_COMPLETE"}}`}}
	if s.token != "" {
		query.Set("page_token", s.token)
	}
	var page struct {
		Files []nativeItem
		Next  string `json:"next_page_token"`
	}
	if err := s.driver.getJSON(ctx, filesURL+"?"+query.Encode(), &page); err != nil {
		return nil, err
	}
	if page.Files == nil {
		return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("PikPak page omitted its files"))
	}
	for i := range page.Files {
		f := &page.Files[i]
		if !validID(f.ID) || f.ID == s.parent || f.Parent != s.parent {
			return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("invalid PikPak child identity"))
		}
		if !core.ValidName(f.Name) {
			return nil, core.Fail(core.UnrepresentableName, nil)
		}
		f.meta = core.Metadata{Name: f.Name, ModifiedAt: f.Modified}
		switch f.Kind {
		case "drive#folder":
			f.meta.Kind = core.Directory
		case "drive#file":
			size, err := fileSize(f.Size)
			if err != nil {
				return nil, err
			}
			f.meta.Kind, f.meta.Size = core.File, &size
		default:
			return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("unknown PikPak item kind"))
		}
	}
	if page.Next == "" {
		s.done, s.seen = true, nil
		return page.Files, io.EOF
	}
	if _, repeated := s.seen[page.Next]; repeated {
		return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("PikPak repeated a page token"))
	}
	if s.seen == nil {
		s.seen = make(map[string]struct{})
	}
	s.seen[page.Next] = struct{}{}
	s.token = page.Next
	return page.Files, nil
}

func (d *Driver) resolve(ctx context.Context, address core.NativeAddress) (nativeItem, error) {
	if err := d.check(ctx, address); err != nil {
		return nativeItem{}, err
	}
	current := nativeItem{ID: d.root, meta: core.Metadata{Kind: core.Directory}}
	if address.Value() == "/" {
		return current, nil
	}
	for _, name := range strings.Split(address.Value()[1:], "/") {
		if current.meta.Kind != core.Directory {
			return nativeItem{}, core.Fail(core.NotDirectory, nil)
		}
		scan := listing{driver: d, parent: current.ID}
		child, found, err := childscan.Unique(ctx, name, scan.next, func(f nativeItem) (string, string) {
			return f.ID, f.Name
		})
		if err != nil {
			return nativeItem{}, err
		}
		if !found {
			return nativeItem{}, core.Fail(core.NotFound, nil)
		}
		current = child
	}
	return current, nil
}

func (d *Driver) Stat(ctx context.Context, address core.NativeAddress, selections ...core.ReadSelection) (core.Metadata, error) {
	if len(selections) > 1 {
		return core.Metadata{}, core.Fail(core.InvalidRequest, nil)
	}
	if len(selections) == 1 && (selections[0].Version != "" || !selections[0].When.Empty()) {
		return core.Metadata{}, core.Fail(core.UnsupportedOperation, nil)
	}
	f, err := d.resolve(ctx, address)
	return f.meta, err
}

func (d *Driver) Enumerate(ctx context.Context, query core.NativeQuery) (core.Iterator, error) {
	if query.Delimiter() != "" || query.StartAfter() != "" {
		return nil, core.Fail(core.UnsupportedOperation, nil)
	}
	f, err := d.resolve(ctx, query.Address())
	if err != nil {
		return nil, err
	}
	if f.meta.Kind != core.Directory {
		return nil, core.Fail(core.NotDirectory, nil)
	}
	life, cancel := context.WithCancel(context.WithoutCancel(ctx))
	return &iterator{scan: listing{driver: d, parent: f.ID}, life: life, cancel: cancel}, nil
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

func (d *Driver) Open(ctx context.Context, address core.NativeAddress, options core.OpenOptions) (core.Opened, error) {
	if err := options.Validate(); err != nil {
		return core.Opened{}, err
	}
	if options.Mode != core.StreamRead || options.NoFollow || options.Selection.Version != "" || !options.Selection.When.Empty() {
		return core.Opened{}, core.Fail(core.UnsupportedOperation, nil)
	}
	f, err := d.resolve(ctx, address)
	if err != nil {
		return core.Opened{}, err
	}
	if f.meta.Kind != core.File {
		return core.Opened{}, core.Fail(core.NotFile, nil)
	}
	var grant struct {
		ID, Kind string
		Owner    string          `json:"user_id"`
		Size     json.RawMessage `json:"size"`
		URL      string          `json:"web_content_link"`
	}
	if err := d.getJSON(ctx, fileURL(f.ID, "FETCH"), &grant); err != nil {
		return core.Opened{}, err
	}
	if grant.ID != "" && grant.ID != f.ID || grant.Kind != "" && grant.Kind != f.Kind ||
		grant.Owner != "" && f.Owner != "" && grant.Owner != f.Owner {
		return core.Opened{}, core.Fail(core.InvalidUpstreamResponse, errors.New("PikPak grant refers to a different file"))
	}
	if grant.URL == "" {
		return core.Opened{}, core.Fail(core.UpstreamAccessDenied, errors.New("PikPak did not issue an original download"))
	}
	size := *f.meta.Size
	if len(grant.Size) > 0 && string(grant.Size) != "null" {
		size, err = fileSize(grant.Size)
		if err != nil {
			return core.Opened{}, err
		}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, grant.URL, nil)
	if err != nil || req.URL.Hostname() == "" || req.URL.User != nil || req.URL.Opaque != "" || strings.Contains(grant.URL, "#") ||
		(req.URL.Scheme != "https" && req.URL.Scheme != "http") {
		return core.Opened{}, core.Fail(core.InvalidUpstreamResponse, errors.New("invalid PikPak download address"))
	}
	req.Header.Set("Accept-Encoding", "identity")
	if options.Range != nil {
		req.Header.Set("Range", httpcontent.Header(options.Range))
	}
	res, err := d.content.Do(req)
	if err != nil {
		if errors.Is(err, httpcontent.ErrContentRedirect) {
			return core.Opened{}, core.Fail(core.InvalidUpstreamResponse, httpcontent.ErrContentRedirect)
		}
		return core.Opened{}, errors.Join(httpcontent.UpstreamIOError(err), ctx.Err())
	}
	switch res.StatusCode {
	case http.StatusOK, http.StatusPartialContent, http.StatusRequestedRangeNotSatisfiable:
		return d.cleanup.OpenSizedBody(res, options.Range, f.Name, size)
	default:
		return core.Opened{}, errors.Join(responseError(res.StatusCode), d.cleanup.CloseBody(res.Body))
	}
}
