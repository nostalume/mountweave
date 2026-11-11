package teambition

import (
	"context"
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

type folder struct {
	ID, ParentID, Title, Updated string
	IsArchived                   bool
}

type work struct {
	ID, ParentID, FileName, Updated string
	IsArchived                      bool
	FileSize                        *int64
	DownloadURL                     string `json:"downloadUrl"`
}

type item struct {
	id, parent string
	meta       core.Metadata
}

type listing struct {
	driver       *Driver
	parent       string
	token        string
	seen         map[string]struct{}
	done, closed bool
}

func (l *listing) next(ctx context.Context) ([]item, error) {
	if err := l.driver.check(ctx); err != nil {
		return nil, err
	}
	if l.done {
		return nil, io.EOF
	}
	q := url.Values{"projectId": {l.driver.project}, "parentId": {l.parent}, "includeArchived": {"false"}, "pageSize": {"50"}}
	if l.token != "" {
		q.Set("pageToken", l.token)
	}
	type page struct {
		Collections   []folder
		Works         []work
		NextPageToken string
	}
	reply, err := get[page](ctx, l.driver, "/work/list", q)
	if err != nil {
		return nil, err
	}
	if _, repeated := l.seen[reply.NextPageToken]; reply.NextPageToken != "" && repeated {
		return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("Teambition repeated a list token"))
	}
	items := make([]item, 0, len(reply.Collections)+len(reply.Works))
	for _, f := range reply.Collections {
		if f.IsArchived || f.ID == l.parent || f.ID == l.driver.root {
			return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("Teambition returned an invalid child folder"))
		}
		items = append(items, item{id: f.ID, parent: f.ParentID, meta: metadata(core.Directory, f.Title, f.Updated, nil)})
	}
	for _, f := range reply.Works {
		if f.IsArchived || f.FileSize != nil && *f.FileSize < 0 {
			return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("Teambition returned invalid file metadata"))
		}
		items = append(items, item{id: f.ID, parent: f.ParentID, meta: metadata(core.File, f.FileName, f.Updated, f.FileSize)})
	}
	for _, f := range items {
		if !validID(f.id) || f.parent != l.parent {
			return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("Teambition returned a child outside the selected folder"))
		}
		if !core.ValidName(f.meta.Name) {
			return nil, core.Fail(core.UnrepresentableName, nil)
		}
	}
	l.token, l.done = reply.NextPageToken, reply.NextPageToken == ""
	if l.done {
		l.seen = nil
		return items, io.EOF
	}
	if l.seen == nil {
		l.seen = make(map[string]struct{})
	}
	l.seen[l.token] = struct{}{}
	return items, nil
}

func metadata(kind core.Kind, name, updated string, size *int64) core.Metadata {
	m := core.Metadata{Kind: kind, Name: name, Size: size}
	if t, err := time.Parse(time.RFC3339Nano, updated); err == nil {
		m.ModifiedAt = &t
	}
	return m
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
	result := make([]core.Metadata, len(items))
	for i, f := range items {
		result[i] = f.meta
	}
	return result, err
}

func (l *listing) Close() error {
	l.closed, l.seen = true, nil
	return nil
}

func (d *Driver) resolve(ctx context.Context, address core.NativeAddress) (item, error) {
	if err := d.check(ctx); err != nil {
		return item{}, err
	}
	if address.Namespace() != core.TreeNamespace {
		return item{}, core.Fail(core.UnsupportedOperation, nil)
	}
	path := address.Value()
	if err := core.ValidatePath(path); err != nil {
		return item{}, err
	}
	current := item{id: d.root, meta: core.Metadata{Kind: core.Directory}}
	if path == "/" {
		return current, nil
	}
	for _, name := range strings.Split(path[1:], "/") {
		if current.meta.Kind != core.Directory {
			return item{}, core.Fail(core.NotDirectory, nil)
		}
		scan := listing{driver: d, parent: current.id}
		found, exists, err := childscan.Unique(ctx, name, scan.next, func(f item) (string, string) {
			prefix := "work:"
			if f.meta.Kind == core.Directory {
				prefix = "folder:"
			}
			return prefix + f.id, f.meta.Name
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
	parent, err := d.resolve(ctx, q.Address())
	if err != nil {
		return nil, err
	}
	if parent.meta.Kind != core.Directory {
		return nil, core.Fail(core.NotDirectory, nil)
	}
	return &listing{driver: d, parent: parent.id}, nil
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
	grants, err := get[[]work](ctx, d, "/work/query", url.Values{"workIds": {f.id}, "needSign": {"true"}})
	if err != nil {
		return core.Opened{}, err
	}
	if len(grants) != 1 || grants[0].ID != f.id || grants[0].ParentID != f.parent || grants[0].FileName != f.meta.Name ||
		grants[0].IsArchived || grants[0].FileSize != nil && *grants[0].FileSize < 0 {
		return core.Opened{}, core.Fail(core.InvalidUpstreamResponse, errors.New("Teambition did not confirm the selected file"))
	}
	grant := grants[0]
	if grant.DownloadURL == "" {
		return core.Opened{}, core.Fail(core.UnsupportedOperation, errors.New("Teambition did not grant a project file download"))
	}
	u, err := url.Parse(grant.DownloadURL)
	if err != nil || u.Scheme != "https" || u.Hostname() == "" || u.User != nil || u.Fragment != "" || u.Opaque != "" {
		return core.Opened{}, core.Fail(core.InvalidUpstreamResponse, errors.New("invalid Teambition download grant"))
	}
	wanted := httpcontent.Header(o.Range)
	response, err := httpcontent.FollowDownload(grant.DownloadURL, func(target string, _ bool) (*http.Response, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
		if err != nil {
			return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("invalid Teambition download request"))
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
		if grant.FileSize != nil {
			return d.cleanup.OpenSizedBody(response, o.Range, f.meta.Name, *grant.FileSize)
		}
		return d.cleanup.OpenBody(response, o.Range, f.meta.Name)
	default:
		return core.Opened{}, errors.Join(statusError(response.StatusCode), d.cleanup.CloseBody(response.Body), ctx.Err())
	}
}
