// Package kdocs mounts the KDocs Open Platform personal document space.
package kdocs

import (
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

const filesURL = "https://developer.kdocs.cn/api/v1/openapi/personal/files"

type Options struct {
	RootPath string // Exact path under My Documents. Empty selects its root.
	// AccessToken supplies the current grant for one application and account.
	AccessToken func(context.Context) (string, error)
	// Clients are borrowed. ContentClient must not inject account credentials.
	Client, ContentClient *http.Client
	MaxResponseBytes      int64
}

type Driver struct {
	root         string
	token        func(context.Context) (string, error)
	api, content http.Client
	limit        int64
	closed       atomic.Bool
	cleanup      httpcontent.ResponseCleanup
}

// Open verifies list access and binds the selected folder. Close a non-nil
// failed result to retry any response cleanup that it retains.
func Open(ctx context.Context, o Options) (*Driver, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if o.AccessToken == nil || o.Client != nil && o.Client == o.ContentClient ||
		o.MaxResponseBytes < 0 || o.MaxResponseBytes == math.MaxInt64 {
		return nil, core.Fail(core.InvalidRequest, nil)
	}
	if o.RootPath != "" {
		if err := core.ValidatePath(o.RootPath); err != nil {
			return nil, err
		}
	}
	if o.MaxResponseBytes == 0 {
		o.MaxResponseBytes = 4 << 20
	}
	d := &Driver{token: o.AccessToken, api: httpcontent.IsolatedClient(o.Client), content: httpcontent.IsolatedClient(o.ContentClient), limit: o.MaxResponseBytes}
	var err error
	if o.RootPath == "" || o.RootPath == "/" {
		children := iterator{driver: d}
		_, err = children.next(ctx, 1)
		if err == io.EOF {
			err = nil
		}
	} else {
		var root item
		root, err = d.resolve(ctx, core.NativePath(o.RootPath))
		if err == nil && root.meta.Kind != core.Directory {
			err = core.Fail(core.NotDirectory, nil)
		}
		if err == nil {
			d.root = root.ID.OpenID
		}
	}
	if err != nil {
		d.closed.Store(true)
	}
	return d, err
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

func (d *Driver) get(ctx context.Context, client *http.Client, target, byteRange string) (*http.Response, error) {
	if err := d.check(ctx); err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil || req.URL.Scheme != "https" || req.URL.Hostname() == "" || req.URL.User != nil || req.URL.Fragment != "" {
		return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("invalid KDocs request target"))
	}
	req.Header.Set("Accept-Encoding", "identity")
	if client == &d.api {
		req.Header.Set("Accept", "application/json")
	}
	if byteRange != "" {
		req.Header.Set("Range", byteRange)
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, errors.Join(httpcontent.UpstreamIOError(err), ctx.Err())
	}
	return resp, nil
}

func fetch[T any](ctx context.Context, d *Driver, route string, query url.Values) (_ T, err error) {
	var zero T
	if err := d.check(ctx); err != nil {
		return zero, err
	}
	token, err := d.token(ctx)
	if err != nil {
		return zero, errors.Join(core.Fail(core.UpstreamAccessDenied, httpcontent.PrivateIOError(err)), ctx.Err())
	}
	if strings.TrimSpace(token) == "" {
		return zero, core.Fail(core.UpstreamAccessDenied, errors.New("KDocs access token is unavailable"))
	}
	query.Set("access_token", token)
	response, err := d.get(ctx, &d.api, filesURL+route+"?"+query.Encode(), "")
	if err != nil {
		return zero, err
	}
	defer func() { err = errors.Join(err, d.cleanup.CloseBody(response.Body), ctx.Err()) }()
	if response.StatusCode != http.StatusOK {
		return zero, statusError(response.StatusCode)
	}
	var envelope struct {
		Code *int
		Data *T
	}
	if err := httpcontent.DecodeJSON(response, d.limit, &envelope); err != nil {
		if core.CodeOf(err) == core.InvalidUpstreamResponse {
			err = core.Fail(core.InvalidUpstreamResponse, errors.New("invalid KDocs JSON response"))
		}
		return zero, err
	}
	if envelope.Code == nil || *envelope.Code == 0 && envelope.Data == nil {
		return zero, core.Fail(core.InvalidUpstreamResponse, errors.New("incomplete KDocs response"))
	}
	if *envelope.Code != 0 {
		return zero, nativeError(*envelope.Code)
	}
	return *envelope.Data, nil
}

func nativeError(code int) error {
	kind := core.UpstreamFailure
	switch code {
	case 10002:
		kind = core.UpstreamTimeout
	case 10003, 20007, 20008, 20009, 30001, 30007:
		kind = core.UpstreamAccessDenied
	case 30002:
		kind = core.UpstreamThrottled
	case 30006:
		kind = core.UnsupportedOperation
	case 30012:
		kind = core.NotFound
	}
	return core.Fail(kind, fmt.Errorf("KDocs returned code %d", code))
}

func statusError(status int) error {
	code := core.UpstreamFailure
	switch {
	case status >= 300 && status < 400:
		return core.Fail(core.InvalidUpstreamResponse, errors.New("KDocs redirect refused"))
	case status == http.StatusUnauthorized || status == http.StatusForbidden:
		code = core.UpstreamAccessDenied
	case status == http.StatusNotFound:
		code = core.NotFound
	case status == http.StatusTooManyRequests:
		code = core.UpstreamThrottled
	}
	return core.Fail(code, fmt.Errorf("KDocs returned HTTP %d", status))
}

// Close retries failed internal response cleanup. Both clients are borrowed.
func (d *Driver) Close() error {
	d.closed.Store(true)
	return d.cleanup.Retry()
}

var _ core.Driver = (*Driver)(nil)
