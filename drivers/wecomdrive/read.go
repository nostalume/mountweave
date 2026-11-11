package wecomdrive

import (
	"cmp"
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"
	"slices"
	"strings"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/internal/childscan"
	"github.com/nostalume/mountweave/drivers/internal/httpcontent"
)

type item struct {
	ID     string `json:"fileid"`
	Name   string `json:"file_name"`
	Space  string `json:"spaceid"`
	Parent string `json:"fatherid"`
	Type   uint32 `json:"file_type"`
	Status uint32 `json:"file_status"`
	Size   *int64 `json:"file_size"`
	meta   core.Metadata
}

type listing struct {
	d      *Driver
	parent string
	start  uint32
	seen   map[uint32]struct{}
	done   bool
}

func (l *listing) next(ctx context.Context, token string, limit int) ([]item, error) {
	if l.done {
		return nil, io.EOF
	}
	request := struct {
		Space  string `json:"spaceid"`
		Parent string `json:"fatherid"`
		Sort   int    `json:"sort_type"`
		Start  uint32 `json:"start"`
		Limit  int    `json:"limit"`
	}{Space: l.d.space, Parent: l.parent, Sort: 1, Start: l.start, Limit: limit}
	var reply struct {
		result
		More *bool   `json:"has_more"`
		Next *uint32 `json:"next_start"`
		List *struct {
			Items []item `json:"item"`
		} `json:"file_list"`
	}
	if err := l.d.call(ctx, "file_list", token, request, &reply); err != nil {
		return nil, err
	}
	if reply.More == nil || reply.List == nil {
		return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("Wedrive listing or continuation status is missing"))
	}
	if *reply.More {
		if reply.Next == nil || *reply.Next == 0 {
			return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("Wedrive listing did not provide a continuation"))
		}
		if _, repeated := l.seen[*reply.Next]; repeated {
			return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("Wedrive repeated a continuation"))
		}
	}
	items := reply.List.Items[:0]
	for _, child := range reply.List.Items {
		if child.ID == "" || child.ID == l.parent || child.ID == l.d.root ||
			child.Parent != l.parent || child.Space != l.d.space {
			return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("Wedrive returned an unrelated child"))
		}
		if child.Status == 2 {
			continue
		}
		if child.Status != 1 {
			return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("unrecognized Wedrive file status"))
		}
		if !core.ValidName(child.Name) {
			return nil, core.Fail(core.UnrepresentableName, nil)
		}
		child.meta = core.Metadata{Name: child.Name, Kind: core.File}
		switch child.Type {
		case 1:
			child.meta.Kind = core.Directory
		case 2:
			if child.Size != nil && *child.Size < 0 {
				return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("invalid Wedrive file size"))
			}
			child.meta.Size = child.Size
		case 3, 4, 5, 6:
			// Wedrive documents do not have a native file download.
		default:
			return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("unrecognized Wedrive file type"))
		}
		items = append(items, child)
	}
	if !*reply.More {
		l.done, l.seen = true, nil
		return items, io.EOF
	}
	if l.seen == nil {
		l.seen = make(map[uint32]struct{})
	}
	l.start = *reply.Next
	l.seen[l.start] = struct{}{}
	return items, nil
}

func (d *Driver) resolve(ctx context.Context, path string, token string) (item, error) {
	current := item{ID: d.root, Type: 1}
	for _, name := range strings.Split(path[1:], "/") {
		if current.Type != 1 {
			return item{}, core.Fail(core.NotDirectory, nil)
		}
		scan := listing{d: d, parent: current.ID}
		found, exists, err := childscan.Unique(ctx, name, func(ctx context.Context) ([]item, error) {
			return scan.next(ctx, token, 1000)
		}, func(f item) (string, string) { return f.ID, f.Name })
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

func (d *Driver) locate(ctx context.Context, address core.NativeAddress) (item, string, error) {
	if address.Namespace() != core.TreeNamespace {
		return item{}, "", core.Fail(core.UnsupportedOperation, nil)
	}
	if err := core.ValidatePath(address.Value()); err != nil {
		return item{}, "", err
	}
	if address.Value() == "/" {
		return item{ID: d.root, Type: 1, meta: core.Metadata{Kind: core.Directory}}, "", d.check(ctx)
	}
	token, err := d.token(ctx)
	if err != nil {
		return item{}, "", err
	}
	found, err := d.resolve(ctx, address.Value(), token)
	return found, token, err
}

func (d *Driver) Stat(ctx context.Context, address core.NativeAddress, selections ...core.ReadSelection) (core.Metadata, error) {
	if len(selections) > 1 {
		return core.Metadata{}, core.Fail(core.InvalidRequest, nil)
	}
	if len(selections) == 1 && (selections[0].Version != "" || !selections[0].When.Empty()) {
		return core.Metadata{}, core.Fail(core.UnsupportedOperation, nil)
	}
	f, _, err := d.locate(ctx, address)
	return f.meta, err
}

func (d *Driver) Enumerate(ctx context.Context, q core.NativeQuery) (core.Iterator, error) {
	if q.Delimiter() != "" || q.StartAfter() != "" {
		return nil, core.Fail(core.UnsupportedOperation, nil)
	}
	parent, _, err := d.locate(ctx, q.Address())
	if err != nil {
		return nil, err
	}
	if parent.Type != 1 {
		return nil, core.Fail(core.NotDirectory, nil)
	}
	return &iterator{scan: listing{d: d, parent: parent.ID}}, nil
}

type iterator struct {
	scan   listing
	closed bool
}

func (i *iterator) Next(ctx context.Context, n int) ([]core.Metadata, error) {
	if i.closed {
		return nil, io.ErrClosedPipe
	}
	if i.scan.done {
		return nil, io.EOF
	}
	if n < 1 {
		return nil, core.Fail(core.InvalidRequest, nil)
	}
	token, err := i.scan.d.token(ctx)
	if err != nil {
		return nil, err
	}
	items, err := i.scan.next(ctx, token, min(n, 1000))
	if err != nil && err != io.EOF {
		return nil, err
	}
	entries := make([]core.Metadata, len(items))
	for k, f := range items {
		entries[k] = f.meta
	}
	return entries, err
}

func (i *iterator) Close() error {
	i.closed, i.scan.seen = true, nil
	return nil
}

func (d *Driver) Open(ctx context.Context, address core.NativeAddress, o core.OpenOptions) (core.Opened, error) {
	if err := o.Validate(); err != nil {
		return core.Opened{}, err
	}
	if o.Mode != core.StreamRead || o.NoFollow || o.Selection.Version != "" || !o.Selection.When.Empty() {
		return core.Opened{}, core.Fail(core.UnsupportedOperation, nil)
	}
	f, token, err := d.locate(ctx, address)
	if err != nil {
		return core.Opened{}, err
	}
	if f.Type == 1 {
		return core.Opened{}, core.Fail(core.NotFile, nil)
	}
	if f.Type != 2 {
		return core.Opened{}, core.Fail(core.UnsupportedOperation, nil)
	}
	var receipt struct {
		result
		URL   string `json:"download_url"`
		Name  string `json:"cookie_name"`
		Value string `json:"cookie_value"`
	}
	if err := d.call(ctx, "file_download", token, fileSelection{ID: f.ID}, &receipt); err != nil {
		return core.Opened{}, err
	}
	target, err := url.Parse(receipt.URL)
	cookie := &http.Cookie{Name: receipt.Name, Value: receipt.Value}
	if err != nil || target.Scheme != "https" || target.Hostname() == "" || target.User != nil ||
		target.Fragment != "" || target.Opaque != "" || cookie.Value == "" || cookie.Valid() != nil {
		return core.Opened{}, core.Fail(core.InvalidUpstreamResponse, errors.New("invalid Wedrive download receipt"))
	}
	rangeHeader := httpcontent.Header(o.Range)
	originHost, originPort := target.Hostname(), cmp.Or(target.Port(), "443")
	original := true
	response, err := httpcontent.FollowDownload(target.String(), func(next string, _ bool) (*http.Response, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, next, nil)
		if err != nil {
			return nil, core.Fail(core.InvalidUpstreamResponse, nil)
		}
		if slices.Contains(req.URL.Query()["access_token"], token) {
			return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("Wedrive download URL includes the application token"))
		}
		original = original && strings.EqualFold(req.URL.Hostname(), originHost) && cmp.Or(req.URL.Port(), "443") == originPort
		if original {
			req.AddCookie(cookie)
		}
		if rangeHeader != "" {
			req.Header.Set("Range", rangeHeader)
		}
		return d.do(&d.content, req)
	}, d.cleanup.CloseBody)
	if err != nil {
		return core.Opened{}, err
	}
	switch response.StatusCode {
	case http.StatusOK, http.StatusPartialContent, http.StatusRequestedRangeNotSatisfiable:
		return d.cleanup.OpenBody(response, o.Range, f.Name)
	default:
		return core.Opened{}, errors.Join(statusError(response.StatusCode), d.cleanup.CloseBody(response.Body))
	}
}
