// Package mobilepan reads an authorized China Mobile Cloud personal drive.
package mobilepan

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"math"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"time"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/internal/childscan"
	"github.com/nostalume/mountweave/drivers/internal/httpcontent"
)

// APIOrigin is the recipient of the application's Mobile Cloud business ticket.
const APIOrigin = "https://openapi.yun.139.com"

const fileAPI = APIOrigin + "/open-ai-space/richlifeApp/idaasV3/personalSaas/file/"
const rootID = "/"

type Options struct {
	// Client supplies an independently authorized Mobile Cloud account's API headers.
	// ContentClient must never attach those headers or any account credentials.
	// Both clients are borrowed; the Driver does not send requests with their cookie jars.
	Client, ContentClient *http.Client
	// RootPath is relative to the account's personal drive. Empty and "/" use its root.
	RootPath         string
	MaxResponseBytes int64
	MaxResolvePages  int
}

type Driver struct {
	api, content   http.Client
	root           entry
	limit          int64
	resolvePages   int
	closed         atomic.Bool
	cleanup        httpcontent.ResponseCleanup
	website        Website
	accountID      string
	releaseWebsite func() error
}

// Open verifies the selected folder. Close a non-nil failed result to retry
// cleanup. Credential issuance and identity persistence belong to the caller.
func Open(ctx context.Context, o Options) (*Driver, error) {
	return open(ctx, o, Website{}, "")
}

func open(ctx context.Context, o Options, website Website, accountID string) (*Driver, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if (o.Client == nil && website.Call == nil) || (o.ContentClient != nil && o.Client == o.ContentClient) || o.MaxResponseBytes < 0 || o.MaxResponseBytes == math.MaxInt64 || o.MaxResolvePages < 0 {
		return nil, core.Fail(core.InvalidRequest, nil)
	}
	if o.MaxResponseBytes == 0 {
		o.MaxResponseBytes = 4 << 20
	}
	if o.MaxResolvePages == 0 {
		o.MaxResolvePages = 200
	}
	d := &Driver{api: httpcontent.IsolatedClient(o.Client), content: httpcontent.IsolatedClient(o.ContentClient),
		root: entry{id: rootID, metadata: core.Metadata{Kind: core.Directory}}, limit: o.MaxResponseBytes, resolvePages: o.MaxResolvePages,
		website: website, accountID: accountID}
	if o.RootPath != "" && o.RootPath != "/" {
		if err := core.ValidatePath(o.RootPath); err != nil {
			return nil, err
		}
		root, err := d.resolve(ctx, core.NativePath(o.RootPath))
		if err != nil {
			d.closed.Store(true)
			return d, err
		}
		if root.metadata.Kind != core.Directory {
			d.closed.Store(true)
			return d, core.Fail(core.NotDirectory, nil)
		}
		d.root = root
	}
	if expected := website.ExpectedRootID; expected != "" && d.root.id != expected {
		return nil, core.Fail(core.UpstreamAccessDenied, ErrWebsiteBindingMismatch)
	}
	first := listing{d: d, parent: d.root.id, pageSize: 10}
	_, err := first.next(ctx)
	if err == io.EOF {
		err = ctx.Err()
	}
	if err != nil {
		d.closed.Store(true)
	}
	return d, err
}

// RootID identifies the native folder verified when the Driver opened.
func (d *Driver) RootID() string { return d.root.id }

func (d *Driver) Close() error {
	d.closed.Store(true)
	err := d.cleanup.Retry()
	if d.releaseWebsite != nil {
		err = errors.Join(err, d.releaseWebsite())
	}
	return err
}

func (d *Driver) check(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if d.closed.Load() {
		return core.Fail(core.MountUnavailable, nil)
	}
	return nil
}

func requestFailure(status int, code string) error {
	switch {
	case status == http.StatusUnauthorized, status == http.StatusForbidden, code == "13000006", code == "16110013":
		return core.Fail(core.UpstreamAccessDenied, nil)
	case status == http.StatusTooManyRequests:
		return core.Fail(core.UpstreamThrottled, nil)
	case status == http.StatusGatewayTimeout, status == http.StatusRequestTimeout:
		return core.Fail(core.UpstreamTimeout, nil)
	default:
		return core.Fail(core.UpstreamFailure, nil)
	}
}

func (d *Driver) call(ctx context.Context, route string, in, out any) (err error) {
	if err := d.check(ctx); err != nil {
		return err
	}
	data, err := json.Marshal(in)
	if err != nil {
		return core.Fail(core.InvalidRequest, nil)
	}
	if d.website.Call != nil {
		return d.website.request(ctx, route, d.accountID, data, d.limit, out)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, fileAPI+route, bytes.NewReader(data))
	if err != nil {
		return core.Fail(core.InvalidRequest, nil)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	res, err := d.api.Do(req)
	if err != nil {
		return errors.Join(httpcontent.UpstreamIOError(err), ctx.Err())
	}
	defer func() { err = errors.Join(err, d.cleanup.CloseBody(res.Body), ctx.Err()) }()
	if res.StatusCode != http.StatusOK {
		return requestFailure(res.StatusCode, "")
	}
	var reply response
	if err := httpcontent.DecodeJSON(res, d.limit, &reply); err != nil {
		return err
	}
	return reply.decode(out)
}

type response struct {
	Success json.RawMessage
	Code    json.RawMessage
	Data    json.RawMessage
}

func (reply response) decode(out any) error {

	success := token(reply.Success)
	code := token(reply.Code)
	if success == "" && code == "" {
		return core.Fail(core.InvalidUpstreamResponse, nil)
	}
	if (success != "" && success != "true" && success != "1") || (code != "" && code != "0000" && code != "0") {
		return requestFailure(0, code)
	}
	if len(reply.Data) == 0 || bytes.Equal(reply.Data, []byte("null")) || json.Unmarshal(reply.Data, out) != nil {
		return core.Fail(core.InvalidUpstreamResponse, nil)
	}
	return nil
}

func token(raw json.RawMessage) string {
	if bytes.Equal(raw, []byte("null")) {
		return ""
	}
	return strings.Trim(string(raw), `"`)
}

type entry struct {
	id       string
	metadata core.Metadata
}

type listing struct {
	d              *Driver
	parent, cursor string
	pageSize       int
	done, closed   bool
}

func (i *listing) next(ctx context.Context) ([]entry, error) {
	if err := i.d.check(ctx); err != nil {
		return nil, err
	}
	if i.done {
		return nil, io.EOF
	}
	type pageInfo struct {
		Size   int    `json:"pageSize"`
		Cursor string `json:"pageCursor,omitempty"`
	}
	req := struct {
		Parent string   `json:"parentFileId"`
		Page   pageInfo `json:"pageInfo"`
	}{i.parent, pageInfo{i.pageSize, i.cursor}}
	var page struct {
		Items []struct {
			ID       string       `json:"fileId"`
			Parent   string       `json:"parentFileId"`
			Name     string       `json:"name"`
			Type     string       `json:"type"`
			Size     *json.Number `json:"size"`
			Modified string       `json:"updatedAt"`
		} `json:"items"`
		Cursor string `json:"nextPageCursor"`
	}
	if err := i.d.call(ctx, "list", req, &page); err != nil {
		return nil, err
	}
	if page.Items == nil || len(page.Items) > i.pageSize || (page.Cursor != "" && page.Cursor == i.cursor) || len(page.Cursor) > 4096 {
		return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("invalid Mobile Cloud listing continuation"))
	}
	list := make([]entry, 0, len(page.Items))
	for _, item := range page.Items {
		if item.ID == "" || item.ID == rootID || item.ID == i.parent || len(item.ID) > 64 || item.Parent != "" && item.Parent != i.parent {
			return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("invalid Mobile Cloud child identity"))
		}
		if !core.ValidName(item.Name) {
			return nil, core.Fail(core.UnrepresentableName, nil)
		}
		m := core.Metadata{Name: item.Name}
		switch item.Type {
		case "folder":
			m.Kind = core.Directory
		case "file":
			m.Kind = core.File
			if item.Size != nil {
				size, err := item.Size.Int64()
				if err != nil || size < 0 {
					return nil, core.Fail(core.InvalidUpstreamResponse, nil)
				}
				m.Size = &size
			}
		default:
			return nil, core.Fail(core.InvalidUpstreamResponse, nil)
		}
		if stamp, err := time.Parse(time.RFC3339Nano, item.Modified); err == nil {
			m.ModifiedAt = &stamp
		}
		list = append(list, entry{item.ID, m})
	}
	i.cursor, i.done = page.Cursor, page.Cursor == ""
	if i.done {
		return list, io.EOF
	}
	return list, nil
}

func (i *listing) Next(ctx context.Context, n int) ([]core.Metadata, error) {
	if i.closed {
		return nil, io.ErrClosedPipe
	}
	if n < 1 {
		return nil, core.Fail(core.InvalidRequest, nil)
	}
	// Keep page size stable across the native cursor chain.
	if i.pageSize == 0 {
		i.pageSize = min(100, max(n, 10))
	}
	items, err := i.next(ctx)
	meta := make([]core.Metadata, len(items))
	for j, f := range items {
		meta[j] = f.metadata
	}
	return meta, err
}

func (i *listing) Close() error { i.closed = true; return nil }

func (d *Driver) resolve(ctx context.Context, p core.NativeAddress) (entry, error) {
	if err := d.check(ctx); err != nil {
		return entry{}, err
	}
	if p.Namespace() != core.TreeNamespace {
		return entry{}, core.Fail(core.UnsupportedOperation, nil)
	}
	if err := core.ValidatePath(p.Value()); err != nil {
		return entry{}, err
	}
	cur := d.root
	if p.Value() == "/" {
		return cur, nil
	}
	pages := d.resolvePages
	for _, name := range strings.Split(p.Value()[1:], "/") {
		if cur.metadata.Kind != core.Directory {
			return entry{}, core.Fail(core.NotDirectory, nil)
		}
		i := listing{d: d, parent: cur.id, pageSize: 100}
		child, ok, err := childscan.Unique(ctx, name, func(ctx context.Context) ([]entry, error) {
			if pages == 0 {
				return nil, core.Fail(core.ResolutionBudgetExceeded, nil)
			}
			pages--
			return i.next(ctx)
		}, func(f entry) (string, string) { return f.id, f.metadata.Name })
		if err != nil {
			return entry{}, err
		}
		if !ok {
			return entry{}, core.Fail(core.NotFound, nil)
		}
		cur = child
	}
	return cur, nil
}

func (d *Driver) Stat(ctx context.Context, p core.NativeAddress, selections ...core.ReadSelection) (core.Metadata, error) {
	if len(selections) > 1 {
		return core.Metadata{}, core.Fail(core.InvalidRequest, nil)
	}
	var s core.ReadSelection
	if len(selections) == 1 {
		s = selections[0]
	}
	if s.Version != "" {
		return core.Metadata{}, core.Fail(core.UnsupportedOperation, nil)
	}
	if err := s.When.ForDomain(""); err != nil {
		return core.Metadata{}, err
	}
	f, err := d.resolve(ctx, p)
	if err != nil {
		return core.Metadata{}, err
	}
	return f.metadata, s.Check(core.Evidence{})
}

func (d *Driver) Enumerate(ctx context.Context, q core.NativeQuery) (core.Iterator, error) {
	if q.Delimiter() != "" || q.StartAfter() != "" {
		return nil, core.Fail(core.UnsupportedOperation, nil)
	}
	dir, err := d.resolve(ctx, q.Address())
	if err != nil {
		return nil, err
	}
	if dir.metadata.Kind != core.Directory {
		return nil, core.Fail(core.NotDirectory, nil)
	}
	return &listing{d: d, parent: dir.id}, nil
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
	if f.metadata.Kind != core.File {
		return core.Opened{}, core.Fail(core.NotFile, nil)
	}
	if err := o.Selection.Check(core.Evidence{}); err != nil {
		return core.Opened{}, err
	}
	var grant struct {
		ID   string       `json:"fileId"`
		URL  string       `json:"url"`
		Size *json.Number `json:"size"`
	}
	if err := d.call(ctx, "getDownloadUrl", map[string]string{"fileId": f.id}, &grant); err != nil {
		return core.Opened{}, err
	}
	target, err := url.Parse(grant.URL)
	if err != nil || !httpcontent.IsHTTPS(target) || target.RawQuery == "" {
		return core.Opened{}, core.Fail(core.InvalidUpstreamResponse, errors.New("Mobile Cloud did not return a signed HTTPS download URL"))
	}
	if grant.ID != f.id && (d.website.Call == nil || grant.ID != "") {
		return core.Opened{}, core.Fail(core.InvalidUpstreamResponse, errors.New("Mobile Cloud download grant did not confirm the selected file"))
	}
	// The website grants a URL for the requested ID and may omit ID and size.
	// Use the selected listing entry's size in that case.
	var size int64
	switch {
	case grant.Size != nil:
		size, err = grant.Size.Int64()
	case d.website.Call != nil && f.metadata.Size != nil:
		size = *f.metadata.Size
	default:
		return core.Opened{}, core.Fail(core.InvalidUpstreamResponse, errors.New("Mobile Cloud did not confirm the file size"))
	}
	if err != nil || size < 0 || f.metadata.Size != nil && size != *f.metadata.Size {
		return core.Opened{}, core.Fail(core.InvalidUpstreamResponse, errors.New("Mobile Cloud download grant changed the file size"))
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target.String(), nil)
	if err != nil {
		return core.Opened{}, core.Fail(core.InvalidUpstreamResponse, nil)
	}
	req.Header.Set("Accept-Encoding", "identity")
	if o.Range != nil {
		req.Header.Set("Range", httpcontent.Header(o.Range))
	}
	res, err := d.content.Do(req)
	if err != nil {
		return core.Opened{}, errors.Join(httpcontent.UpstreamIOError(err), ctx.Err())
	}
	if err := ctx.Err(); err != nil {
		return core.Opened{}, errors.Join(err, d.cleanup.CloseBody(res.Body))
	}
	if res.StatusCode != http.StatusOK && res.StatusCode != http.StatusPartialContent && res.StatusCode != http.StatusRequestedRangeNotSatisfiable {
		return core.Opened{}, errors.Join(requestFailure(res.StatusCode, ""), d.cleanup.CloseBody(res.Body), ctx.Err())
	}
	return d.cleanup.OpenSizedBody(res, o.Range, f.metadata.Name, size)
}
