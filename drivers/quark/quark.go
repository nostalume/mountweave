// Package quark reads a Quark Drive through an authorized Open API application
// or an authenticated website session.
package quark

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/internal/childscan"
	"github.com/nostalume/mountweave/drivers/internal/httpcontent"
)

const APIOrigin = "https://open-api-drive.quark.cn"

const (
	fileAPI         = APIOrigin + "/open/v1/file/"
	rootFID         = "0"
	pageSize        = 100
	maxResolvePages = 200
	responseBudget  = 4 << 20
)

type Options struct {
	// Application attaches provider-issued credentials to the control and body
	// requests. When omitted, the two client transports supply authorization.
	Application *Application
	// Client sends requests to APIOrigin. It defaults to the standard client
	// when Application supplies credentials.
	Client *http.Client
	// ContentClient sends body requests to the exact HTTPS ContentOrigin with
	// redirects refused. It defaults to the standard client with Application.
	ContentClient *http.Client
	ContentOrigin string
	// RootPath selects a folder by path relative to the granted drive.
	// An empty path selects the drive root.
	RootPath string
}

type Driver struct {
	api, content http.Client
	application  *Application
	contentHost  string
	website      *websiteSession
	root         entry
	closed       atomic.Bool
	cleanup      httpcontent.ResponseCleanup
}

// Open verifies the selected directory with the callers' borrowed clients.
// Close any non-nil Driver returned with an error to retry response cleanup.
func Open(ctx context.Context, o Options) (*Driver, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	origin, err := url.Parse(o.ContentOrigin)
	if err != nil || !httpcontent.IsHTTPS(origin) || (origin.EscapedPath() != "" && origin.EscapedPath() != "/") ||
		origin.RawQuery != "" || origin.ForceQuery {
		return nil, core.Fail(core.InvalidRequest, nil)
	}
	if o.Application == nil && (o.Client == nil || o.ContentClient == nil || o.Client == o.ContentClient) ||
		o.Application != nil && !o.Application.valid() {
		return nil, core.Fail(core.InvalidRequest, nil)
	}
	if o.RootPath == "" {
		o.RootPath = "/"
	}
	if err := core.ValidatePath(o.RootPath); err != nil {
		return nil, err
	}
	d := &Driver{
		api: httpcontent.IsolatedClient(o.Client), content: httpcontent.IsolatedClient(o.ContentClient),
		contentHost: origin.Host, root: entry{fid: rootFID, meta: core.Metadata{Kind: core.Directory}},
	}
	if o.Application != nil {
		app := *o.Application
		d.application = &app
	}
	root, err := d.resolve(ctx, core.NativePath(o.RootPath))
	if err == nil && root.meta.Kind != core.Directory {
		err = core.Fail(core.NotDirectory, nil)
	}
	if err == nil {
		_, _, err = d.page(ctx, root.fid, 1, nil)
	}
	if err != nil {
		d.closed.Store(true)
		return d, err
	}
	d.root = root
	return d, nil
}

func (d *Driver) Close() error {
	d.closed.Store(true)
	if d.website != nil {
		return d.website.release()
	}
	return d.cleanup.Retry()
}

// RootID returns the provider identity of the mounted directory.
func (d *Driver) RootID() string { return d.root.fid }

func (d *Driver) check(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if d.closed.Load() {
		return core.Fail(core.MountUnavailable, nil)
	}
	return nil
}

func failure(status int) error {
	switch status {
	case http.StatusUnauthorized, http.StatusForbidden:
		return core.Fail(core.UpstreamAccessDenied, nil)
	case http.StatusNotFound:
		return core.Fail(core.NotFound, nil)
	case http.StatusTooManyRequests:
		return core.Fail(core.UpstreamThrottled, nil)
	case http.StatusGatewayTimeout, http.StatusRequestTimeout:
		return core.Fail(core.UpstreamTimeout, nil)
	default:
		return core.Fail(core.UpstreamFailure, nil)
	}
}

func (d *Driver) post(ctx context.Context, route string, args, data any) (token string, err error) {
	if err := d.check(ctx); err != nil {
		return "", err
	}
	encoded, err := json.Marshal(args)
	if err != nil {
		return "", core.Fail(core.InvalidRequest, nil)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, fileAPI+route, bytes.NewReader(encoded))
	if err != nil {
		return "", core.Fail(core.InvalidRequest, nil)
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", "application/json")
	if d.application != nil {
		token, err = d.application.authorize(req)
		if err != nil {
			return "", err
		}
	}
	res, err := d.api.Do(req)
	if err != nil {
		return "", errors.Join(httpcontent.UpstreamIOError(err), ctx.Err())
	}
	defer func() { err = errors.Join(err, d.cleanup.CloseBody(res.Body), ctx.Err()) }()
	if res.StatusCode != http.StatusOK {
		return "", failure(res.StatusCode)
	}
	var body struct {
		Status *int
		Errno  int
		Data   json.RawMessage
	}
	if err := httpcontent.DecodeJSON(res, responseBudget, &body); err != nil {
		return "", err
	}
	if body.Status == nil {
		return "", core.Fail(core.InvalidUpstreamResponse, nil)
	}
	if *body.Status != 0 || body.Errno != 0 {
		switch body.Errno {
		case 11000, 11001, 11017, 12003, 12004:
			return "", core.Fail(core.UpstreamAccessDenied, nil)
		default:
			return "", core.Fail(core.UpstreamFailure, nil)
		}
	}
	if len(body.Data) == 0 || bytes.Equal(body.Data, []byte("null")) || json.Unmarshal(body.Data, data) != nil {
		return "", core.Fail(core.InvalidUpstreamResponse, nil)
	}
	return token, nil
}

type entry struct {
	fid  string
	meta core.Metadata
}

type cursor struct {
	Version string `json:"version"`
	Token   string `json:"token"`
}

func (d *Driver) page(ctx context.Context, parent string, size int, from *cursor) ([]entry, *cursor, error) {
	request := struct {
		Parent string  `json:"parent_fid"`
		Size   int     `json:"size"`
		Sort   string  `json:"sort"`
		From   *cursor `json:"query_cursor,omitempty"`
	}{parent, size, "updated_at:desc", from}
	var page struct {
		Files []struct {
			FID    string       `json:"fid"`
			Parent string       `json:"parent_fid"`
			Name   string       `json:"filename"`
			Type   json.Number  `json:"file_type"`
			Size   *json.Number `json:"size"`
		} `json:"file_list"`
		Last *bool   `json:"last_page"`
		Next *cursor `json:"next_query_cursor"`
	}
	if _, err := d.post(ctx, "list", request, &page); err != nil {
		return nil, nil, err
	}
	if page.Last == nil || len(page.Files) > size {
		return nil, nil, core.Fail(core.InvalidUpstreamResponse, nil)
	}
	if *page.Last {
		page.Next = nil
	} else if page.Next == nil || page.Next.Version == "" || page.Next.Token == "" ||
		len(page.Next.Version)+len(page.Next.Token) > 65536 || from != nil && *page.Next == *from {
		return nil, nil, core.Fail(core.InvalidUpstreamResponse, errors.New("invalid Quark Drive cursor"))
	}
	entries := make([]entry, 0, len(page.Files))
	for _, file := range page.Files {
		if file.FID == "" || file.FID == rootFID || file.FID == parent || file.Parent != "" && file.Parent != parent {
			return nil, nil, core.Fail(core.InvalidUpstreamResponse, errors.New("invalid Quark Drive child identity"))
		}
		if !core.ValidName(file.Name) {
			return nil, nil, core.Fail(core.UnrepresentableName, nil)
		}
		metadata := core.Metadata{Name: file.Name}
		switch file.Type {
		case "0":
			metadata.Kind = core.Directory
		case "1":
			metadata.Kind = core.File
			if file.Size != nil {
				n, err := file.Size.Int64()
				if err != nil || n < 0 {
					return nil, nil, core.Fail(core.InvalidUpstreamResponse, nil)
				}
				metadata.Size = &n
			}
		default:
			return nil, nil, core.Fail(core.InvalidUpstreamResponse, nil)
		}
		entries = append(entries, entry{fid: file.FID, meta: metadata})
	}
	return entries, page.Next, nil
}

type listing struct {
	d            *Driver
	parent       string
	size         int
	cursor       *cursor
	done, closed bool
}

func (l *listing) next(ctx context.Context) ([]entry, error) {
	if l.done {
		return nil, io.EOF
	}
	files, next, err := l.d.page(ctx, l.parent, l.size, l.cursor)
	if err != nil {
		return nil, err
	}
	l.cursor, l.done = next, next == nil
	if l.done {
		return files, io.EOF
	}
	return files, nil
}

func (l *listing) Next(ctx context.Context, n int) ([]core.Metadata, error) {
	if l.closed {
		return nil, io.ErrClosedPipe
	}
	if n < 1 {
		return nil, core.Fail(core.InvalidRequest, nil)
	}
	if l.size == 0 {
		l.size = min(n, pageSize)
	}
	files, err := l.next(ctx)
	out := make([]core.Metadata, len(files))
	for i, file := range files {
		out[i] = file.meta
	}
	return out, err
}

func (l *listing) Close() error { l.closed = true; return nil }

type childListing interface {
	core.Iterator
	next(context.Context) ([]entry, error)
}

func (d *Driver) children(parent string, size int) childListing {
	if d.website != nil {
		return &websiteListing{driver: d, parent: parent}
	}
	return &listing{d: d, parent: parent, size: size}
}

func (d *Driver) resolve(ctx context.Context, path core.NativeAddress) (entry, error) {
	if err := d.check(ctx); err != nil {
		return entry{}, err
	}
	if path.Namespace() != core.TreeNamespace {
		return entry{}, core.Fail(core.UnsupportedOperation, nil)
	}
	if err := core.ValidatePath(path.Value()); err != nil {
		return entry{}, err
	}
	active := d.root
	if path.Value() == "/" {
		return active, nil
	}
	pages := maxResolvePages
	for _, name := range strings.Split(path.Value()[1:], "/") {
		if active.meta.Kind != core.Directory {
			return entry{}, core.Fail(core.NotDirectory, nil)
		}
		l := d.children(active.fid, pageSize)
		found, exists, err := childscan.Unique(ctx, name, func(ctx context.Context) ([]entry, error) {
			if pages == 0 {
				return nil, core.Fail(core.ResolutionBudgetExceeded, nil)
			}
			pages--
			return l.next(ctx)
		}, func(f entry) (string, string) { return f.fid, f.meta.Name })
		if err != nil {
			return entry{}, err
		}
		if !exists {
			return entry{}, core.Fail(core.NotFound, nil)
		}
		active = found
	}
	return active, nil
}

func (d *Driver) Stat(ctx context.Context, p core.NativeAddress, selections ...core.ReadSelection) (core.Metadata, error) {
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
	f, err := d.resolve(ctx, p)
	if err != nil {
		return core.Metadata{}, err
	}
	if d.website != nil && p.Value() == "/" {
		if _, err := websiteAccount(ctx, d.website.call, d.website.account); err != nil {
			return core.Metadata{}, err
		}
	}
	return f.meta, selection.Check(f.meta.Evidence)
}

func (d *Driver) Enumerate(ctx context.Context, query core.NativeQuery) (core.Iterator, error) {
	if query.Delimiter() != "" || query.StartAfter() != "" {
		return nil, core.Fail(core.UnsupportedOperation, nil)
	}
	dir, err := d.resolve(ctx, query.Address())
	if err != nil {
		return nil, err
	}
	if dir.meta.Kind != core.Directory {
		return nil, core.Fail(core.NotDirectory, nil)
	}
	return d.children(dir.fid, 0), nil
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
	if err := o.Selection.Check(f.meta.Evidence); err != nil {
		return core.Opened{}, err
	}
	if d.website != nil {
		return d.website.open(ctx, f, o.Range)
	}
	var grant struct {
		FID  string       `json:"fid"`
		URL  string       `json:"download_url"`
		Size *json.Number `json:"size"`
	}
	token, err := d.post(ctx, "get_download_url", map[string]string{"fid": f.fid}, &grant)
	if err != nil {
		return core.Opened{}, err
	}
	target, err := url.Parse(grant.URL)
	if err != nil || !httpcontent.IsHTTPS(target) || !strings.EqualFold(target.Host, d.contentHost) ||
		grant.FID != f.fid || grant.Size == nil {
		return core.Opened{}, core.Fail(core.InvalidUpstreamResponse, errors.New("invalid Quark Drive download grant"))
	}
	size, err := grant.Size.Int64()
	if err != nil || size < 0 || f.meta.Size != nil && size != *f.meta.Size {
		return core.Opened{}, core.Fail(core.InvalidUpstreamResponse, errors.New("Quark Drive download grant changed the file size"))
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target.String(), nil)
	if err != nil {
		return core.Opened{}, core.Fail(core.InvalidUpstreamResponse, nil)
	}
	req.Header.Set("Accept-Encoding", "identity")
	if o.Range != nil {
		req.Header.Set("Range", httpcontent.Header(o.Range))
	}
	if d.application != nil {
		d.application.authorizeBody(req, token)
	}
	res, err := d.content.Do(req)
	if err != nil {
		return core.Opened{}, errors.Join(httpcontent.UpstreamIOError(err), ctx.Err())
	}
	if err := ctx.Err(); err != nil {
		return core.Opened{}, errors.Join(err, d.cleanup.CloseBody(res.Body))
	}
	if res.StatusCode >= 300 && res.StatusCode < 400 {
		return core.Opened{}, errors.Join(core.Fail(core.InvalidUpstreamResponse, httpcontent.ErrContentRedirect), d.cleanup.CloseBody(res.Body), ctx.Err())
	}
	if res.StatusCode != http.StatusOK && res.StatusCode != http.StatusPartialContent && res.StatusCode != http.StatusRequestedRangeNotSatisfiable {
		return core.Opened{}, errors.Join(failure(res.StatusCode), d.cleanup.CloseBody(res.Body), ctx.Err())
	}
	return d.cleanup.OpenSizedBody(res, o.Range, f.meta.Name, size)
}
