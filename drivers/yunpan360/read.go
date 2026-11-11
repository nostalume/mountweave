package yunpan360

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/internal/childscan"
	"github.com/nostalume/mountweave/drivers/internal/httpcontent"
)

const pageSize = 50

// The tools preserve native NIDs and type markers as either strings or numbers.
type scalar string

func (s *scalar) UnmarshalJSON(b []byte) error {
	var value string
	if len(b) > 0 && b[0] == '"' {
		if err := json.Unmarshal(b, &value); err != nil {
			return err
		}
	} else {
		var number json.Number
		if err := json.Unmarshal(b, &number); err != nil {
			return err
		}
		value = number.String()
	}
	*s = scalar(value)
	return nil
}

type item struct {
	ID       scalar      `json:"nid"`
	Name     string      `json:"name"`
	FileName string      `json:"fname"`
	Dir      scalar      `json:"is_dir"`
	Type     scalar      `json:"type"`
	Size     json.Number `json:"size"`
	Count    json.Number `json:"count_size"`
	Modified json.Number `json:"modify_time"`
	MTime    json.Number `json:"mtime"`
	meta     core.Metadata
}

func (f *item) accept() error {
	if f.ID == "" {
		return core.Fail(core.InvalidUpstreamResponse, errors.New("360 Cloud Disk omitted the node ID"))
	}
	name := f.Name
	if name == "" {
		name = f.FileName
	}
	if !core.ValidName(name) {
		return core.Fail(core.UnrepresentableName, nil)
	}
	f.meta = core.Metadata{Name: name, Kind: core.File}
	switch {
	case f.Dir == "1", f.Dir == "" && (f.Type == "1" || f.Type == "dir"):
		f.meta.Kind = core.Directory
	case f.Dir == "0", f.Dir == "" && (f.Type == "0" || f.Type == "file"):
		value := f.Count
		if value == "" {
			value = f.Size
		}
		if value != "" {
			size, err := value.Int64()
			if err != nil || size < 0 {
				return core.Fail(core.InvalidUpstreamResponse, errors.New("invalid 360 Cloud Disk file size"))
			}
			f.meta.Size = &size
		}
	default:
		return core.Fail(core.InvalidUpstreamResponse, errors.New("invalid 360 Cloud Disk node type"))
	}
	value := f.Modified
	if value == "" {
		value = f.MTime
	}
	if ts, err := value.Int64(); err == nil && ts > 0 {
		modified := time.UnixMilli(ts).UTC()
		if ts < 1e12 {
			modified = time.Unix(ts, 0).UTC()
		}
		f.meta.ModifiedAt = &modified
	}
	return nil
}

type listing struct {
	driver       *Driver
	path         string
	page         int64
	done, closed bool
}

func (l *listing) next(ctx context.Context) ([]item, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if l.done {
		return nil, io.EOF
	}
	request := struct {
		Path string `json:"path"`
		Page int64  `json:"page"`
		Size int    `json:"page_size"`
	}{l.path, l.page, pageSize}
	var reply struct {
		List       []item
		Data       []item
		Nodes      []item      `json:"node_list"`
		Total      json.Number `json:"total"`
		TotalCount json.Number `json:"total_count"`
	}
	if err := l.driver.call(ctx, "file-list", request, &reply); err != nil {
		return nil, err
	}
	items := reply.List
	if items == nil {
		items = reply.Data
	}
	if items == nil {
		items = reply.Nodes
	}
	count := reply.TotalCount
	if count == "" {
		count = reply.Total
	}
	total, err := count.Int64()
	if err != nil || total < 0 || items == nil {
		return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("360 Cloud Disk did not confirm the list window"))
	}
	if reply.TotalCount != "" && reply.Total != "" {
		other, err := reply.Total.Int64()
		if err != nil || other != total {
			return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("360 Cloud Disk reported conflicting totals"))
		}
	}
	remaining := max(0, total-l.page*pageSize)
	if int64(len(items)) != min(remaining, pageSize) {
		return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("360 Cloud Disk page does not match its total"))
	}
	for i := range items {
		if err := items[i].accept(); err != nil {
			return nil, err
		}
	}
	l.done = remaining <= pageSize
	if l.done {
		return items, io.EOF
	}
	l.page++
	return items, nil
}

func (l *listing) Next(ctx context.Context, n int) ([]core.Metadata, error) {
	if l.closed {
		return nil, io.ErrClosedPipe
	}
	if n < 1 {
		return nil, core.Fail(core.InvalidRequest, nil)
	}
	items, err := l.next(ctx)
	if err != nil && err != io.EOF {
		return nil, err
	}
	metadata := make([]core.Metadata, len(items))
	for i, f := range items {
		metadata[i] = f.meta
	}
	return metadata, err
}

func (l *listing) Close() error {
	l.closed = true
	return nil
}

func (d *Driver) resolve(ctx context.Context, address core.NativeAddress) (item, error) {
	if err := d.check(ctx); err != nil {
		return item{}, err
	}
	if address.Namespace() != core.TreeNamespace {
		return item{}, core.Fail(core.UnsupportedOperation, nil)
	}
	p := address.Value()
	if err := core.ValidatePath(p); err != nil {
		return item{}, err
	}
	found := item{meta: core.Metadata{Kind: core.Directory}}
	if p == "/" {
		return found, nil
	}
	parent := "/"
	for _, name := range strings.Split(p[1:], "/") {
		if found.meta.Kind != core.Directory {
			return item{}, core.Fail(core.NotDirectory, nil)
		}
		scan := listing{driver: d, path: parent}
		child, exists, err := childscan.Unique(ctx, name, scan.next, func(f item) (string, string) { return string(f.ID), f.meta.Name })
		if err != nil {
			return item{}, err
		}
		if !exists {
			return item{}, core.Fail(core.NotFound, nil)
		}
		found, parent = child, parent+child.meta.Name+"/"
	}
	return found, nil
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
	f, err := d.resolve(ctx, address)
	if err != nil {
		return core.Metadata{}, err
	}
	return f.meta, selection.Check(core.Evidence{})
}

func (d *Driver) Enumerate(ctx context.Context, q core.NativeQuery) (core.Iterator, error) {
	if q.Delimiter() != "" || q.StartAfter() != "" {
		return nil, core.Fail(core.UnsupportedOperation, nil)
	}
	dir, err := d.resolve(ctx, q.Address())
	if err != nil {
		return nil, err
	}
	if dir.meta.Kind != core.Directory {
		return nil, core.Fail(core.NotDirectory, nil)
	}
	p := q.Address().Value()
	if p != "/" {
		p += "/"
	}
	return &listing{driver: d, path: p}, nil
}

func (d *Driver) Open(ctx context.Context, address core.NativeAddress, o core.OpenOptions) (core.Opened, error) {
	if err := o.Validate(); err != nil {
		return core.Opened{}, err
	}
	if o.Mode != core.StreamRead || o.NoFollow || o.Selection.Version != "" {
		return core.Opened{}, core.Fail(core.UnsupportedOperation, nil)
	}
	if err := o.Selection.When.ForDomain(""); err != nil {
		return core.Opened{}, err
	}
	f, err := d.resolve(ctx, address)
	if err != nil {
		return core.Opened{}, err
	}
	if f.meta.Kind != core.File {
		return core.Opened{}, core.Fail(core.NotFile, nil)
	}
	if err := o.Selection.Check(core.Evidence{}); err != nil {
		return core.Opened{}, err
	}
	var grant struct {
		URL  string      `json:"downloadUrl"`
		Name string      `json:"fname"`
		Size json.Number `json:"size"`
	}
	if err := d.call(ctx, "get-download-url", map[string]string{"nid": string(f.ID)}, &grant); err != nil {
		return core.Opened{}, err
	}
	if grant.Name != "" && grant.Name != f.meta.Name && grant.Name != address.Value() {
		return core.Opened{}, core.Fail(core.InvalidUpstreamResponse, errors.New("360 Cloud Disk grant named a different file"))
	}
	var confirmedSize *int64
	if grant.Size != "" {
		size, err := grant.Size.Int64()
		if err != nil || size < 0 {
			return core.Opened{}, core.Fail(core.InvalidUpstreamResponse, errors.New("invalid 360 Cloud Disk grant size"))
		}
		confirmedSize = &size
	}
	u, err := url.Parse(grant.URL)
	if err != nil || u.Scheme != "https" || u.Hostname() == "" || u.User != nil || u.Opaque != "" || u.Fragment != "" {
		return core.Opened{}, core.Fail(core.InvalidUpstreamResponse, errors.New("360 Cloud Disk did not grant an HTTPS download"))
	}
	response, err := httpcontent.FollowDownload(grant.URL, func(target string, _ bool) (*http.Response, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
		if err != nil {
			return nil, core.Fail(core.InvalidUpstreamResponse, nil)
		}
		req.Header.Set("User-Agent", "yunpan_mcp_server")
		if o.Range != nil {
			req.Header.Set("Range", httpcontent.Header(o.Range))
		}
		return d.request(&d.content, req)
	}, d.cleanup.CloseBody)
	if err != nil {
		return core.Opened{}, err
	}
	switch response.StatusCode {
	case http.StatusOK, http.StatusPartialContent, http.StatusRequestedRangeNotSatisfiable:
	default:
		return core.Opened{}, errors.Join(statusError(response.StatusCode), d.cleanup.CloseBody(response.Body), ctx.Err())
	}
	if confirmedSize != nil {
		return d.cleanup.OpenSizedBody(response, o.Range, f.meta.Name, *confirmedSize)
	}
	return d.cleanup.OpenBody(response, o.Range, f.meta.Name)
}
