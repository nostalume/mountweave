package wuyingdrive

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
	FileID, ParentID, Name, FileType, ModifiedTime string
	Size                                           *int64
	meta                                           core.Metadata
}

type listing struct {
	driver        *Driver
	parent, token string
	seen          map[string]struct{}
	done, closed  bool
}

func (l *listing) page(ctx context.Context, size int) ([]item, error) {
	if l.done {
		return nil, io.EOF
	}
	var response struct {
		result
		Count, NextToken string
		FileModels       []item
	}
	q := url.Values{"ParentFileId": {l.parent}, "Status": {"available"}, "MaxResults": {strconv.Itoa(size)}}
	if l.token != "" {
		q.Set("NextToken", l.token)
	}
	if err := l.driver.control(ctx, "ListCdsFiles", q, &response); err != nil {
		return nil, err
	}
	if err := response.result.check(); err != nil {
		return nil, err
	}
	if len(response.FileModels) > size || response.FileModels == nil && (response.Count != "0" || response.NextToken != "") {
		return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("invalid WUYING directory page"))
	}
	for i := range response.FileModels {
		f := &response.FileModels[i]
		if !core.ValidName(f.FileID) || f.FileID == rootID || f.FileID == l.parent || f.ParentID != l.parent {
			return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("WUYING returned a child outside the requested parent"))
		}
		if !core.ValidName(f.Name) {
			return nil, core.Fail(core.UnrepresentableName, nil)
		}
		f.meta.Name = f.Name
		switch f.FileType {
		case "folder":
			f.meta.Kind = core.Directory
		case "file":
			if f.Size != nil && *f.Size < 0 {
				return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("invalid WUYING file size"))
			}
			f.meta.Kind, f.meta.Size = core.File, f.Size
		default:
			return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("unrecognized WUYING item type"))
		}
		if f.ModifiedTime != "" {
			modified, err := time.Parse(time.RFC3339Nano, f.ModifiedTime)
			if err != nil {
				return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("invalid WUYING modification time"))
			}
			f.meta.ModifiedAt = &modified
		}
	}
	if response.NextToken == "" {
		l.done, l.seen = true, nil
		return response.FileModels, io.EOF
	}
	if _, repeated := l.seen[response.NextToken]; repeated {
		return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("WUYING repeated a directory token"))
	}
	if l.seen == nil {
		l.seen = make(map[string]struct{})
	}
	l.token = response.NextToken
	l.seen[l.token] = struct{}{}
	return response.FileModels, nil
}

func (l *listing) Next(ctx context.Context, n int) ([]core.Metadata, error) {
	if l.closed {
		return nil, io.ErrClosedPipe
	}
	if n < 1 {
		return nil, core.Fail(core.InvalidRequest, nil)
	}
	files, err := l.page(ctx, min(n, 100))
	if err != nil && err != io.EOF {
		return nil, err
	}
	items := make([]core.Metadata, len(files))
	for i, f := range files {
		items[i] = f.meta
	}
	return items, err
}

func (l *listing) Close() error {
	l.closed, l.seen = true, nil
	return nil
}

func (d *Driver) resolve(ctx context.Context, a core.NativeAddress) (item, error) {
	if err := d.check(ctx); err != nil {
		return item{}, err
	}
	if a.Namespace() != core.TreeNamespace {
		return item{}, core.Fail(core.UnsupportedOperation, nil)
	}
	path := a.Value()
	if err := core.ValidatePath(path); err != nil {
		return item{}, err
	}
	current := item{FileID: rootID, meta: core.Metadata{Kind: core.Directory}}
	if path == "/" {
		return current, nil
	}
	for _, name := range strings.Split(path[1:], "/") {
		if current.meta.Kind != core.Directory {
			return item{}, core.Fail(core.NotDirectory, nil)
		}
		scan := listing{driver: d, parent: current.FileID}
		found, exists, err := childscan.Unique(ctx, name,
			func(ctx context.Context) ([]item, error) { return scan.page(ctx, 100) },
			func(f item) (string, string) { return f.FileID, f.Name })
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
	return &listing{driver: d, parent: f.FileID}, nil
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
	var response struct {
		result
		DownloadFileModel struct {
			FileID, DownloadURL string
			Size                *int64
		}
	}
	if err := d.control(ctx, "DownloadCdsFile", url.Values{"FileId": {f.FileID}}, &response); err != nil {
		return core.Opened{}, err
	}
	if err := response.result.check(); err != nil {
		return core.Opened{}, err
	}
	grant := response.DownloadFileModel
	u, err := url.Parse(grant.DownloadURL)
	if grant.FileID != f.FileID || grant.Size == nil || *grant.Size < 0 ||
		err != nil || u.Scheme != "https" || u.Hostname() == "" || u.User != nil || u.Opaque != "" ||
		strings.ContainsRune(grant.DownloadURL, '#') {
		return core.Opened{}, core.Fail(core.InvalidUpstreamResponse, errors.New("invalid WUYING original download receipt"))
	}
	wanted := httpcontent.Header(o.Range)
	body, err := httpcontent.FollowDownload(grant.DownloadURL, func(target string, _ bool) (*http.Response, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
		if err != nil {
			return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("invalid WUYING content URL"))
		}
		if wanted != "" {
			req.Header.Set("Range", wanted)
		}
		return d.request(req, &d.content)
	}, d.cleanup.CloseBody)
	if err != nil {
		return core.Opened{}, err
	}
	switch body.StatusCode {
	case http.StatusOK, http.StatusPartialContent, http.StatusRequestedRangeNotSatisfiable:
		opened, err := d.cleanup.OpenSizedBody(body, o.Range, f.Name, *grant.Size)
		if err == nil {
			opened.Metadata.Size = grant.Size
		}
		return opened, err
	default:
		return core.Opened{}, errors.Join(statusError(body.StatusCode, ""), d.cleanup.CloseBody(body.Body))
	}
}
