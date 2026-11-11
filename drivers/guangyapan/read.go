package guangyapan

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

const nativePageSize = 20

type item struct {
	ID       string `json:"fileId"`
	Name     string `json:"fileName"`
	ParentID string `json:"parentId"`
	Size     *int64 `json:"fileSize"`
	Type     int    `json:"resType"`
	Updated  int64  `json:"utime"`
}

func (f item) metadata() core.Metadata {
	m := core.Metadata{Name: f.Name, Kind: core.File, Size: f.Size}
	if f.Type == 2 {
		m.Kind, m.Size = core.Directory, nil
	}
	if f.Updated > 0 {
		t := time.Unix(f.Updated, 0).UTC()
		m.ModifiedAt = &t
	}
	return m
}

func (d *Driver) page(ctx context.Context, parent string, index int64) ([]item, int64, error) {
	q := url.Values{"page": {strconv.FormatInt(index, 10)}, "pageSize": {strconv.Itoa(nativePageSize)},
		"orderBy": {"0"}, "sortType": {"0"}}
	if parent != "" {
		q.Set("parentId", parent)
	}
	var result struct {
		Total *int64
		List  []item
	}
	if err := d.control(ctx, "get_file_list", q, &result); err != nil {
		return nil, 0, err
	}
	if result.Total == nil || result.List == nil || *result.Total < 0 || len(result.List) > nativePageSize {
		return nil, 0, core.Fail(core.InvalidUpstreamResponse, errors.New("invalid Guangya Pan page"))
	}
	for _, f := range result.List {
		if f.ID == "" || f.ParentID != parent || f.Type != 1 && f.Type != 2 || f.Size != nil && *f.Size < 0 {
			return nil, 0, core.Fail(core.InvalidUpstreamResponse, errors.New("invalid Guangya Pan file identity or parent"))
		}
		if !core.ValidName(f.Name) {
			return nil, 0, core.Fail(core.UnrepresentableName, nil)
		}
	}
	return result.List, *result.Total, nil
}

type iterator struct {
	d            *Driver
	parent       string
	index        int64
	done, closed bool
}

func (i *iterator) next(ctx context.Context) ([]item, error) {
	if i.done {
		return nil, io.EOF
	}
	entries, total, err := i.d.page(ctx, i.parent, i.index)
	if err != nil {
		return nil, err
	}
	remaining := max(0, total-i.index*nativePageSize)
	if int64(len(entries)) != min(remaining, nativePageSize) {
		return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("Guangya Pan page did not match its total"))
	}
	i.index++
	i.done = remaining <= nativePageSize
	if i.done {
		return entries, io.EOF
	}
	return entries, nil
}

func (i *iterator) Next(ctx context.Context, n int) ([]core.Metadata, error) {
	if i.closed {
		return nil, io.ErrClosedPipe
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if n < 1 {
		return nil, core.Fail(core.InvalidRequest, nil)
	}
	entries, err := i.next(ctx)
	if err != nil && err != io.EOF {
		return nil, err
	}
	out := make([]core.Metadata, len(entries))
	for k, f := range entries {
		out[k] = f.metadata()
	}
	return out, err
}

func (i *iterator) Close() error {
	i.closed = true
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
	current := item{ID: d.root, Type: 2}
	if a.Value() == "/" {
		return current, nil
	}
	for _, name := range strings.Split(a.Value()[1:], "/") {
		if current.Type != 2 {
			return item{}, core.Fail(core.NotDirectory, nil)
		}
		scan := iterator{d: d, parent: current.ID}
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
	f, err := d.resolve(ctx, a)
	if err != nil {
		return core.Metadata{}, err
	}
	return f.metadata(), selection.Check(core.Evidence{})
}

func (d *Driver) Enumerate(ctx context.Context, q core.NativeQuery) (core.Iterator, error) {
	if q.Delimiter() != "" || q.StartAfter() != "" {
		return nil, core.Fail(core.UnsupportedOperation, nil)
	}
	f, err := d.resolve(ctx, q.Address())
	if err != nil {
		return nil, err
	}
	if f.Type != 2 {
		return nil, core.Fail(core.NotDirectory, nil)
	}
	return &iterator{d: d, parent: f.ID}, nil
}

func (d *Driver) Open(ctx context.Context, a core.NativeAddress, o core.OpenOptions) (core.Opened, error) {
	if err := o.Validate(); err != nil {
		return core.Opened{}, err
	}
	if o.Mode != core.StreamRead || o.NoFollow || o.Selection.Version != "" {
		return core.Opened{}, core.Fail(core.UnsupportedOperation, nil)
	}
	if err := o.Selection.When.ForDomain(""); err != nil {
		return core.Opened{}, err
	}
	f, err := d.resolve(ctx, a)
	if err != nil {
		return core.Opened{}, err
	}
	if f.Type != 1 {
		return core.Opened{}, core.Fail(core.NotFile, nil)
	}
	if err := o.Selection.Check(core.Evidence{}); err != nil {
		return core.Opened{}, err
	}
	var grant struct{ SignedURL string }
	if err := d.control(ctx, "get_res_download_url", url.Values{"fileId": {f.ID}}, &grant); err != nil {
		return core.Opened{}, err
	}
	response, err := httpcontent.FollowDownload(grant.SignedURL, func(raw string, _ bool) (*http.Response, error) {
		u, err := url.Parse(raw)
		if err != nil || u.Scheme != "https" || u.Hostname() == "" || u.User != nil || u.Opaque != "" || u.Fragment != "" {
			return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("invalid Guangya Pan signed URL"))
		}
		return d.get(ctx, &d.content, raw, o.Range)
	}, d.cleanup.CloseBody)
	if err != nil {
		return core.Opened{}, err
	}
	switch response.StatusCode {
	case http.StatusOK, http.StatusPartialContent, http.StatusRequestedRangeNotSatisfiable:
	default:
		return core.Opened{}, errors.Join(responseError(response.StatusCode), d.cleanup.CloseBody(response.Body))
	}
	if f.Size != nil {
		return d.cleanup.OpenSizedBody(response, o.Range, f.Name, *f.Size)
	}
	return d.cleanup.OpenBody(response, o.Range, f.Name)
}
