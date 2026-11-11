package gokuai

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
	"github.com/nostalume/mountweave/drivers/internal/httpcontent"
)

type item struct {
	Hash, FullPath, Filename, FileHash string
	Dir                                *int
	FileSize                           *int64
	Modified                           int64 `json:"last_dateline"`
}

func (f item) validate(expected string) error {
	if f.FullPath != expected || f.Filename != expected[strings.LastIndexByte(expected, '/')+1:] ||
		f.Hash == "" || f.Dir == nil || *f.Dir != 0 && *f.Dir != 1 ||
		*f.Dir == 0 && (f.FileSize == nil || *f.FileSize < 0) {
		return core.Fail(core.InvalidUpstreamResponse, errors.New("Gokuai did not confirm the exact file or directory"))
	}
	return nil
}

func (f item) metadata() core.Metadata {
	meta := core.Metadata{Kind: core.File, Name: f.Filename, Size: f.FileSize}
	if *f.Dir == 1 {
		meta.Kind, meta.Size = core.Directory, nil
	}
	if f.Modified != 0 {
		t := time.Unix(f.Modified, 0)
		meta.ModifiedAt = &t
	}
	return meta
}

func (d *Driver) entry(ctx context.Context, path string) (item, error) {
	var f item
	fields := url.Values{"fullpath": {path}, "attribute": {"10"}}
	if err := d.control(ctx, "info", fields, &f); err != nil {
		return item{}, err
	}
	return f, f.validate(path)
}

func (d *Driver) Stat(ctx context.Context, a core.NativeAddress, selections ...core.ReadSelection) (core.Metadata, error) {
	if len(selections) > 1 {
		return core.Metadata{}, core.Fail(core.InvalidRequest, nil)
	}
	if len(selections) == 1 && (selections[0].Version != "" || !selections[0].When.Empty()) {
		return core.Metadata{}, core.Fail(core.UnsupportedOperation, nil)
	}
	path, err := d.path(a)
	if err != nil {
		return core.Metadata{}, err
	}
	if path == "" {
		return core.Metadata{Kind: core.Directory}, d.check(ctx)
	}
	f, err := d.entry(ctx, path)
	if err != nil {
		return core.Metadata{}, err
	}
	meta := f.metadata()
	if a.Value() == "/" {
		if meta.Kind != core.Directory {
			return core.Metadata{}, core.Fail(core.NotDirectory, nil)
		}
		meta.Name = ""
	}
	return meta, nil
}

func (d *Driver) Enumerate(ctx context.Context, q core.NativeQuery) (core.Iterator, error) {
	if q.Delimiter() != "" || q.StartAfter() != "" {
		return nil, core.Fail(core.UnsupportedOperation, nil)
	}
	path, err := d.path(q.Address())
	if err != nil {
		return nil, err
	}
	if err := d.check(ctx); err != nil {
		return nil, err
	}
	return &listing{driver: d, path: path}, nil
}

type listing struct {
	driver       *Driver
	path         string
	offset       int64
	done, closed bool
}

func (l *listing) Next(ctx context.Context, n int) ([]core.Metadata, error) {
	if l.closed {
		return nil, io.ErrClosedPipe
	}
	if l.done {
		return nil, io.EOF
	}
	if n < 1 {
		return nil, core.Fail(core.InvalidRequest, nil)
	}
	n = min(n, 100)
	var page struct {
		Count *int64
		List  []item
	}
	q := url.Values{"fullpath": {l.path}, "start": {strconv.FormatInt(l.offset, 10)}, "size": {strconv.Itoa(n)}}
	if err := l.driver.control(ctx, "ls", q, &page); err != nil {
		return nil, err
	}
	if page.Count == nil || *page.Count < 0 || page.List == nil || len(page.List) > n ||
		len(page.List) > 0 && int64(len(page.List)) > *page.Count-l.offset ||
		len(page.List) == 0 && l.offset < *page.Count {
		return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("invalid Gokuai directory page or offset"))
	}
	items := make([]core.Metadata, len(page.List))
	for i, f := range page.List {
		if !core.ValidName(f.Filename) {
			return nil, core.Fail(core.UnrepresentableName, nil)
		}
		if err := f.validate(childPath(l.path, f.Filename)); err != nil {
			return nil, err
		}
		items[i] = f.metadata()
	}
	l.offset += int64(len(items))
	if l.offset >= *page.Count {
		l.done = true
		return items, io.EOF
	}
	return items, nil
}

func (l *listing) Close() error {
	l.closed = true
	return nil
}

func (d *Driver) Open(ctx context.Context, a core.NativeAddress, o core.OpenOptions) (core.Opened, error) {
	if err := o.Validate(); err != nil {
		return core.Opened{}, err
	}
	if o.Mode != core.StreamRead || o.NoFollow || o.Selection.Version != "" || !o.Selection.When.Empty() {
		return core.Opened{}, core.Fail(core.UnsupportedOperation, nil)
	}
	path, err := d.path(a)
	if err != nil {
		return core.Opened{}, err
	}
	if a.Value() == "/" {
		if err := d.check(ctx); err != nil {
			return core.Opened{}, err
		}
		return core.Opened{}, core.Fail(core.NotFile, nil)
	}
	f, err := d.entry(ctx, path)
	if err != nil {
		return core.Opened{}, err
	}
	if *f.Dir != 0 {
		return core.Opened{}, core.Fail(core.NotFile, nil)
	}
	if f.FileHash == "" {
		return core.Opened{}, core.Fail(core.InvalidUpstreamResponse, errors.New("Gokuai omitted the original file's content hash"))
	}
	// Bind the grant to the authorized path and the observed content version.
	fields := url.Values{"fullpath": {path}, "filehash": {f.FileHash}, "open": {"0"}}
	var grant struct{ URLs []string }
	if err := d.control(ctx, "download_url", fields, &grant); err != nil {
		return core.Opened{}, err
	}
	target := ""
	for _, raw := range grant.URLs {
		u, err := url.Parse(raw)
		if err == nil && u.Scheme == "https" && u.Hostname() != "" && u.User == nil && u.Opaque == "" &&
			u.Fragment == "" && !strings.ContainsRune(raw, '#') {
			target = raw
			break
		}
	}
	if target == "" {
		return core.Opened{}, core.Fail(core.UnsupportedOperation, errors.New("Gokuai did not grant an HTTPS original download"))
	}
	wanted := httpcontent.Header(o.Range)
	response, err := httpcontent.FollowDownload(target, func(target string, _ bool) (*http.Response, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
		if err != nil {
			return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("invalid Gokuai content URL"))
		}
		if wanted != "" {
			req.Header.Set("Range", wanted)
		}
		return d.request(req, &d.content)
	}, d.cleanup.CloseBody)
	if err != nil {
		return core.Opened{}, err
	}
	switch response.StatusCode {
	case http.StatusOK, http.StatusPartialContent, http.StatusRequestedRangeNotSatisfiable:
		return d.cleanup.OpenSizedBody(response, o.Range, f.Filename, *f.FileSize)
	default:
		return core.Opened{}, errors.Join(statusError(response.StatusCode, 0), d.cleanup.CloseBody(response.Body))
	}
}
