package koodrive

import (
	"context"
	"errors"
	"io"
	"net/http"
	"slices"
	"strings"
	"time"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/internal/childscan"
	"github.com/nostalume/mountweave/drivers/internal/httpcontent"
)

type item struct {
	ID         string   `json:"id"`
	Name       string   `json:"fileName"`
	Type       string   `json:"fileType"`
	Parents    []string `json:"parentFolder"`
	Containers []string `json:"containers"`
	Recycled   bool     `json:"recycled"`
	Size       *int64   `json:"size"`
	Modified   string   `json:"editedTime"`
	meta       core.Metadata
}

type listing struct {
	driver *Driver
	parent string
	cursor string
	seen   map[string]struct{}
	done   bool
}

func (l *listing) next(ctx context.Context) ([]item, error) {
	if l.done {
		return nil, io.EOF
	}
	type page struct {
		Cursor string `json:"pageCursor"`
	}
	request := struct {
		Container string `json:"containerId"`
		Parent    string `json:"parentFileId"`
		Page      *page  `json:"pageInfo,omitempty"`
	}{Container: l.driver.container, Parent: l.driver.parentParameter(l.parent)}
	if l.cursor != "" {
		request.Page = &page{Cursor: l.cursor}
	}
	var reply struct {
		result
		Category string `json:"category"`
		Cursor   string `json:"nextCursor"`
		Files    []item `json:"files"`
	}
	if err := l.driver.call(ctx, http.MethodPost, l.driver.listRoute, request, &reply); err != nil {
		return nil, err
	}
	if err := reply.result.check(); err != nil {
		return nil, err
	}
	if reply.Category != "drive#fileList" {
		return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("invalid KooDrive listing category"))
	}
	if reply.Cursor != "" {
		if _, repeated := l.seen[reply.Cursor]; repeated {
			return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("KooDrive listing repeated a cursor"))
		}
	}
	items := reply.Files[:0]
	for _, child := range reply.Files {
		if child.ID == "" || child.ID == l.parent || child.ID == l.driver.root || child.Recycled ||
			len(child.Parents) == 0 || child.Parents[0] != l.parent ||
			!slices.Contains(child.Containers, l.driver.container) {
			return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("KooDrive returned an unrelated child"))
		}
		switch child.Type {
		case "11": // Smart folders are virtual views.
			continue
		case "10":
			child.meta.Kind = core.Directory
		case "15", "20", "26", "27", "28", "30", "35", "40", "45":
			child.meta.Kind, child.meta.Size = core.File, child.Size
		default:
			return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("unrecognized KooDrive item type"))
		}
		if !core.ValidName(child.Name) {
			return nil, core.Fail(core.UnrepresentableName, nil)
		}
		if child.meta.Size != nil && *child.meta.Size < 0 {
			return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("negative KooDrive file size"))
		}
		child.meta.Name = child.Name
		if child.Modified != "" {
			when, err := time.Parse(time.RFC3339Nano, child.Modified)
			if err != nil {
				return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("invalid KooDrive modification time"))
			}
			child.meta.ModifiedAt = &when
		}
		items = append(items, child)
	}
	l.cursor, l.done = reply.Cursor, reply.Cursor == ""
	if l.done {
		l.seen = nil
		return items, io.EOF
	}
	if l.seen == nil {
		l.seen = make(map[string]struct{})
	}
	l.seen[reply.Cursor] = struct{}{}
	return items, nil
}

func (d *Driver) resolve(ctx context.Context, address core.NativeAddress) (item, error) {
	if err := d.check(ctx, address); err != nil {
		return item{}, err
	}
	parent := item{ID: d.root, meta: core.Metadata{Kind: core.Directory}}
	if address.Value() == "/" {
		return parent, nil
	}
	for _, name := range strings.Split(strings.TrimPrefix(address.Value(), "/"), "/") {
		if parent.meta.Kind != core.Directory {
			return item{}, core.Fail(core.NotDirectory, nil)
		}
		scan := listing{driver: d, parent: parent.ID}
		child, exists, err := childscan.Unique(ctx, name, scan.next, func(f item) (string, string) {
			return f.ID, f.Name
		})
		if err != nil {
			return item{}, err
		}
		if !exists {
			return item{}, core.Fail(core.NotFound, nil)
		}
		parent = child
	}
	return parent, nil
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

func (d *Driver) Enumerate(ctx context.Context, q core.NativeQuery) (core.Iterator, error) {
	if q.Delimiter() != "" || q.StartAfter() != "" {
		return nil, core.Fail(core.UnsupportedOperation, nil)
	}
	parent, err := d.resolve(ctx, q.Address())
	if err != nil {
		return nil, err
	}
	if parent.meta.Kind != core.Directory {
		return nil, core.Fail(core.NotDirectory, nil)
	}
	return &iterator{scan: listing{driver: d, parent: parent.ID}}, nil
}

type iterator struct {
	scan   listing
	closed bool
}

func (i *iterator) Next(ctx context.Context, n int) ([]core.Metadata, error) {
	if i.closed {
		return nil, io.ErrClosedPipe
	}
	if n < 1 {
		return nil, core.Fail(core.InvalidRequest, nil)
	}
	items, err := i.scan.next(ctx)
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
	i.closed = true
	i.scan.seen = nil
	return nil
}

func (d *Driver) Open(ctx context.Context, address core.NativeAddress, o core.OpenOptions) (core.Opened, error) {
	if err := o.Validate(); err != nil {
		return core.Opened{}, err
	}
	if o.Mode != core.StreamRead || o.NoFollow || o.Selection.Version != "" || !o.Selection.When.Empty() {
		return core.Opened{}, core.Fail(core.UnsupportedOperation, nil)
	}
	f, err := d.resolve(ctx, address)
	if err != nil {
		return core.Opened{}, err
	}
	if f.meta.Kind != core.File {
		return core.Opened{}, core.Fail(core.NotFile, nil)
	}
	request := struct {
		Container string   `json:"containerId"`
		IDs       []string `json:"fileIds"`
	}{Container: d.container, IDs: []string{f.ID}}
	var reply struct {
		result
		Batch     *int `json:"batchCode"`
		Downloads []struct {
			result
			ID   string `json:"fileId"`
			Name string `json:"fileName"`
			URL  string `json:"url"`
		} `json:"downloads"`
	}
	if err := d.call(ctx, http.MethodPost, "/files/download", request, &reply); err != nil {
		return core.Opened{}, err
	}
	if err := reply.result.check(); err != nil {
		return core.Opened{}, err
	}
	if len(reply.Downloads) != 1 || reply.Downloads[0].ID != f.ID {
		return core.Opened{}, core.Fail(core.InvalidUpstreamResponse, errors.New("KooDrive download receipt has a different file identity"))
	}
	receipt := reply.Downloads[0]
	if err := receipt.result.check(); err != nil {
		return core.Opened{}, err
	}
	if reply.Batch == nil || *reply.Batch != 1 || receipt.Name != f.Name {
		return core.Opened{}, core.Fail(core.InvalidUpstreamResponse, errors.New("KooDrive did not confirm the requested download"))
	}
	if err := checkContentURL(receipt.URL); err != nil {
		return core.Opened{}, err
	}
	byteRange := httpcontent.Header(o.Range)
	response, err := httpcontent.FollowDownload(receipt.URL, func(address string, _ bool) (*http.Response, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, address, nil)
		if err != nil {
			return nil, core.Fail(core.InvalidUpstreamResponse, nil)
		}
		if byteRange != "" {
			req.Header.Set("Range", byteRange)
		}
		response, _, err := d.exchange(&d.content, req)
		return response, err
	}, d.cleanup.CloseBody)
	if err != nil {
		return core.Opened{}, err
	}
	switch response.StatusCode {
	case http.StatusOK, http.StatusPartialContent, http.StatusRequestedRangeNotSatisfiable:
		if f.Size != nil {
			return d.cleanup.OpenSizedBody(response, o.Range, f.Name, *f.Size)
		}
		return d.cleanup.OpenBody(response, o.Range, f.Name)
	default:
		return core.Opened{}, errors.Join(statusError(response.StatusCode), d.cleanup.CloseBody(response.Body))
	}
}
