// Package imgbed mounts a CloudFlare ImgBed directory as a read-only source.
package imgbed

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

type Options struct {
	Endpoint string // HTTPS origin of the ImgBed deployment.
	RootPath string // Absolute ImgBed directory, with "/" as the default.
	// APIClient supplies a list-scoped API token.
	APIClient *http.Client
	// ContentClient is optional. Its transport must not inject credentials.
	ContentClient    *http.Client
	MaxResponseBytes int64
}

type Driver struct {
	base, root   string
	api, content http.Client
	limit        int64
	closed       atomic.Bool
	cleanup      httpcontent.ResponseCleanup
}

// Open verifies the configured directory and list access. A non-nil failed
// result retains any failed response cleanup and must be closed.
func Open(ctx context.Context, o Options) (*Driver, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	u, err := url.Parse(o.Endpoint)
	if err != nil || u.Scheme != "https" || u.Hostname() == "" || u.User != nil || u.Opaque != "" ||
		u.RawQuery != "" || u.ForceQuery || strings.Contains(o.Endpoint, "#") || u.Path != "" && u.Path != "/" ||
		o.APIClient == nil || o.APIClient == o.ContentClient || o.MaxResponseBytes < 0 || o.MaxResponseBytes == math.MaxInt64 {
		return nil, core.Fail(core.InvalidRequest, errors.New("ImgBed requires an HTTPS origin and a separate list client"))
	}
	if o.RootPath == "" {
		o.RootPath = "/"
	}
	if err := core.ValidatePath(o.RootPath); err != nil {
		return nil, err
	}
	if strings.Contains(o.RootPath, "..") || strings.Contains(o.RootPath, ",") {
		return nil, core.Fail(core.UnrepresentableName, nil)
	}
	if o.MaxResponseBytes == 0 {
		o.MaxResponseBytes = 4 << 20
	}
	d := &Driver{
		base: "https://" + u.Host, root: strings.TrimPrefix(o.RootPath, "/"), limit: o.MaxResponseBytes,
		api: httpcontent.IsolatedClient(o.APIClient), content: httpcontent.IsolatedClient(o.ContentClient),
	}
	dir := iterator{driver: d, parent: directoryPrefix(d.root)}
	items, err := dir.Next(ctx, 1)
	if err == io.EOF {
		err = nil
	}
	if err == nil && d.root != "" && len(items) == 0 {
		err = core.Fail(core.NotFound, nil)
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

func (d *Driver) get(ctx context.Context, client *http.Client, route, byteRange string) (*http.Response, error) {
	if err := d.check(ctx); err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, d.base+route, nil)
	if err != nil {
		return nil, core.Fail(core.InvalidRequest, nil)
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
		return nil, errors.Join(core.Fail(core.UpstreamFailure, httpcontent.PrivateIOError(err)), ctx.Err())
	}
	return resp, nil
}

func responseError(status int) error {
	code := core.UpstreamFailure
	switch {
	case status >= 300 && status < 400:
		return core.Fail(core.InvalidUpstreamResponse, errors.New("ImgBed redirect refused"))
	case status == http.StatusUnauthorized || status == http.StatusForbidden:
		code = core.UpstreamAccessDenied
	case status == http.StatusNotFound:
		code = core.NotFound
	case status == http.StatusTooManyRequests:
		code = core.UpstreamThrottled
	}
	return core.Fail(code, fmt.Errorf("ImgBed returned HTTP %d", status))
}

// Close retries failed internal response cleanup. Both clients are borrowed.
func (d *Driver) Close() error {
	d.closed.Store(true)
	return d.cleanup.Retry()
}

var _ core.Driver = (*Driver)(nil)
