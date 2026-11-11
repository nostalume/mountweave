// Package tencentsmh mounts a Tencent Cloud Smart Media Hosting space.
package tencentsmh

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/internal/httpcontent"
)

type Options struct {
	LibraryID string
	SpaceID   string // Empty selects the single-tenant space ("-").
	RootPath  string // Absolute path in the space. Empty selects its root.
	Endpoint  string // HTTPS control origin. Empty selects https://smh.tencentcs.com.
	// AccessToken supplies a business API token for this library and space.
	// Nil selects anonymous access to a public-read library or space.
	AccessToken func(context.Context) (string, error)
	// ContentClient is independent and must never inject account credentials.
	Client, ContentClient *http.Client
	MaxResponseBytes      int64
}

type Driver struct {
	httpSource
	scope, root string
	accessToken func(context.Context) (string, error)
}

type httpSource struct {
	base         string
	api, content http.Client
	limit        int64
	closed       atomic.Bool
	cleanup      httpcontent.ResponseCleanup
}

// Open verifies the selected directory. Close a non-nil failed result to
// retry any response cleanup retained by the driver.
func Open(ctx context.Context, o Options) (*Driver, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if o.Endpoint == "" {
		o.Endpoint = "https://smh.tencentcs.com"
	}
	if o.SpaceID == "" {
		o.SpaceID = "-"
	}
	if !core.ValidName(o.LibraryID) || !core.ValidName(o.SpaceID) {
		return nil, core.Fail(core.InvalidRequest, nil)
	}
	if o.RootPath == "" {
		o.RootPath = "/"
	}
	if err := core.ValidatePath(o.RootPath); err != nil {
		return nil, err
	}
	d := &Driver{
		scope: "/" + url.PathEscape(o.LibraryID) + "/" + url.PathEscape(o.SpaceID),
		root:  o.RootPath, accessToken: o.AccessToken,
	}
	if err := d.configure(o.Endpoint, o.Client, o.ContentClient, o.MaxResponseBytes); err != nil {
		return nil, err
	}
	token, err := d.token(ctx)
	if err == nil {
		var root item
		root, err = d.item(ctx, d.root, token, false)
		if err == nil && root.Type != "dir" {
			err = core.Fail(core.NotDirectory, nil)
		}
	}
	if err != nil {
		d.closed.Store(true)
	}
	return d, err
}

func (d *httpSource) configure(origin string, api, content *http.Client, limit int64) error {
	endpoint, err := url.Parse(origin)
	if err != nil || endpoint.Scheme != "https" || endpoint.Hostname() == "" ||
		endpoint.User != nil || endpoint.Opaque != "" || endpoint.RawQuery != "" || endpoint.ForceQuery ||
		strings.Contains(origin, "#") || endpoint.Path != "" && endpoint.Path != "/" ||
		api != nil && api == content || limit < 0 || limit == math.MaxInt64 {
		return core.Fail(core.InvalidRequest, nil)
	}
	if limit == 0 {
		limit = 4 << 20
	}
	d.base, d.limit = "https://"+endpoint.Host+"/api/v1", limit
	d.api, d.content = httpcontent.IsolatedClient(api), httpcontent.IsolatedClient(content)
	return nil
}

// Close retries internal response cleanup. Both HTTP clients remain host-owned.
func (d *httpSource) Close() error {
	d.closed.Store(true)
	return d.cleanup.Retry()
}

func (d *Driver) path(a core.NativeAddress) (string, error) {
	if a.Namespace() != core.TreeNamespace {
		return "", core.Fail(core.UnsupportedOperation, nil)
	}
	if err := core.ValidatePath(a.Value()); err != nil {
		return "", err
	}
	if a.Value() == "/" {
		return d.root, nil
	}
	return strings.TrimSuffix(d.root, "/") + a.Value(), nil
}

func (d *Driver) token(ctx context.Context) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if d.closed.Load() {
		return "", core.Fail(core.MountUnavailable, nil)
	}
	if d.accessToken == nil {
		return "", nil
	}
	value, err := d.accessToken(ctx)
	if err != nil {
		return "", errors.Join(core.Fail(core.UpstreamAccessDenied, errors.New("SMH access token unavailable")), ctx.Err())
	}
	if value == "" {
		return "", core.Fail(core.UpstreamAccessDenied, errors.New("SMH access token is empty"))
	}
	return value, nil
}

func (d *Driver) json(ctx context.Context, route, path, token string, q url.Values, out any) error {
	parts := strings.Split(strings.TrimPrefix(path, "/"), "/")
	for i := range parts {
		parts[i] = url.PathEscape(parts[i])
	}
	if token != "" {
		q.Set("access_token", token)
	}
	target := d.base + route + d.scope + "/" + strings.Join(parts, "/") + "?" + q.Encode()
	return d.readJSON(ctx, target, nil, out)
}

func (d *httpSource) readJSON(ctx context.Context, target string, payload []byte, out any) (err error) {
	response, err := d.request(ctx, &d.api, target, "application/json", "", payload)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, d.cleanup.CloseBody(response.Body), ctx.Err()) }()
	if response.StatusCode == http.StatusOK {
		return httpcontent.DecodeJSON(response, d.limit, out)
	}
	// Do not include upstream messages or URLs, which can contain the token.
	if response.StatusCode == http.StatusConflict || response.StatusCode == http.StatusPreconditionFailed {
		var failure struct{ Code string }
		if httpcontent.DecodeJSON(response, d.limit, &failure) == nil && failure.Code == "ContentCasMismatch" {
			return core.Fail(core.PreconditionFailed, errors.New("SMH content changed before download authorization"))
		}
	}
	return statusError(response.StatusCode)
}

func (d *httpSource) request(ctx context.Context, client *http.Client, target, accept, byteRange string, payload []byte) (*http.Response, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if d.closed.Load() {
		return nil, core.Fail(core.MountUnavailable, nil)
	}
	method := http.MethodGet
	var body io.Reader
	if payload != nil {
		method, body = http.MethodPost, bytes.NewReader(payload)
	}
	req, err := http.NewRequestWithContext(ctx, method, target, body)
	if err != nil {
		return nil, core.Fail(core.InvalidUpstreamResponse, nil)
	}
	req.Header.Set("Accept-Encoding", "identity")
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if accept != "" {
		req.Header.Set("Accept", accept)
	}
	if byteRange != "" {
		req.Header.Set("Range", byteRange)
	}
	response, err := client.Do(req)
	if err != nil {
		return nil, errors.Join(httpcontent.UpstreamIOError(err), ctx.Err())
	}
	return response, nil
}

func (d *httpSource) openContent(ctx context.Context, response *http.Response, wanted *core.Range, name string, size *int64) (core.Opened, error) {
	switch response.StatusCode {
	case http.StatusOK, http.StatusPartialContent, http.StatusRequestedRangeNotSatisfiable:
		if size != nil {
			return d.cleanup.OpenSizedBody(response, wanted, name, *size)
		}
		return d.cleanup.OpenBody(response, wanted, name)
	default:
		return core.Opened{}, errors.Join(statusError(response.StatusCode), d.cleanup.CloseBody(response.Body), ctx.Err())
	}
}

func statusError(status int) error {
	code := core.UpstreamFailure
	switch status {
	case http.StatusUnauthorized, http.StatusForbidden:
		code = core.UpstreamAccessDenied
	case http.StatusNotFound:
		code = core.NotFound
	case http.StatusTooManyRequests:
		code = core.UpstreamThrottled
	}
	return core.Fail(code, fmt.Errorf("SMH returned HTTP %d", status))
}

var _ core.Driver = (*Driver)(nil)
