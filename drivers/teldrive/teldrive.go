// Package teldrive mounts a read-only folder through the Teldrive HTTP API.
package teldrive

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
	Endpoint string // HTTPS origin of the Teldrive deployment.
	RootID   string // Optional folder UUID.
	RootPath string // Optional absolute path in the authenticated user's drive.
	// Client is borrowed. Its transport supplies credentials for one account.
	Client           *http.Client
	MaxResponseBytes int64
}

type Driver struct {
	base, root string
	client     http.Client
	limit      int64
	closed     atomic.Bool
	cleanup    httpcontent.ResponseCleanup
}

// Open verifies the folder and binds its native ID. Close a non-nil failed
// result to retry any response cleanup that it retains.
func Open(ctx context.Context, o Options) (*Driver, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	u, err := url.Parse(o.Endpoint)
	if err != nil || u.Scheme != "https" || u.Hostname() == "" || u.User != nil || u.Opaque != "" ||
		u.RawQuery != "" || u.ForceQuery || strings.Contains(o.Endpoint, "#") || u.Path != "" && u.Path != "/" ||
		o.Client == nil || o.RootID != "" && o.RootPath != "" || o.MaxResponseBytes < 0 || o.MaxResponseBytes == math.MaxInt64 {
		return nil, core.Fail(core.InvalidRequest, nil)
	}
	if o.RootPath != "" {
		if err := core.ValidatePath(o.RootPath); err != nil {
			return nil, err
		}
	}
	rootID := strings.ToLower(o.RootID)
	if rootID != "" && !validID(rootID) {
		return nil, core.Fail(core.InvalidRequest, errors.New("invalid Teldrive folder UUID"))
	}
	if o.MaxResponseBytes == 0 {
		o.MaxResponseBytes = 4 << 20
	}
	d := &Driver{base: "https://" + u.Host + "/api/v1/files", limit: o.MaxResponseBytes, client: *o.Client}
	d.client.Jar = nil
	d.client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	var root item
	switch {
	case rootID != "":
		err = d.json(ctx, "/"+rootID, &root)
		if err == nil && root.ID != rootID {
			err = core.Fail(core.InvalidUpstreamResponse, errors.New("Teldrive returned a different folder ID"))
		}
		if err == nil && root.Status != "active" {
			err = core.Fail(core.NotFound, nil)
		}
		if err == nil {
			err = root.admit()
		}
	case o.RootPath != "" && o.RootPath != "/":
		root, err = d.resolve(ctx, core.NativePath(o.RootPath))
	default:
		root.meta.Kind = core.Directory
		children := iterator{driver: d}
		_, err = children.next(ctx, 1)
		if err == io.EOF {
			err = nil
		}
	}
	if err == nil && root.meta.Kind != core.Directory {
		err = core.Fail(core.NotDirectory, nil)
	}
	if err != nil {
		d.closed.Store(true)
		return d, err
	}
	d.root = root.ID
	return d, nil
}

func validID(id string) bool {
	if len(id) != 36 || id == "00000000-0000-0000-0000-000000000000" {
		return false
	}
	for i, r := range id {
		if i == 8 || i == 13 || i == 18 || i == 23 {
			if r != '-' {
				return false
			}
		} else if !strings.ContainsRune("0123456789abcdef", r) {
			return false
		}
	}
	return true
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

func (d *Driver) get(ctx context.Context, route, accept, byteRange string) (*http.Response, error) {
	if err := d.check(ctx); err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, d.base+route, nil)
	if err != nil {
		return nil, core.Fail(core.InvalidRequest, nil)
	}
	req.Header.Set("Accept-Encoding", "identity")
	if accept != "" {
		req.Header.Set("Accept", accept)
	}
	if byteRange != "" {
		req.Header.Set("Range", byteRange)
	}
	resp, err := d.client.Do(req)
	if err != nil {
		return nil, errors.Join(core.Fail(core.UpstreamFailure, httpcontent.PrivateIOError(err)), ctx.Err())
	}
	return resp, nil
}

func (d *Driver) json(ctx context.Context, route string, out any) (err error) {
	resp, err := d.get(ctx, route, "application/json", "")
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, d.cleanup.CloseBody(resp.Body), ctx.Err()) }()
	if resp.StatusCode != http.StatusOK {
		return responseError(resp.StatusCode)
	}
	return httpcontent.DecodeJSON(resp, d.limit, out)
}

func responseError(status int) error {
	code := core.UpstreamFailure
	switch {
	case status >= 300 && status < 400:
		return core.Fail(core.InvalidUpstreamResponse, errors.New("Teldrive redirect refused"))
	case status == http.StatusUnauthorized || status == http.StatusForbidden:
		code = core.UpstreamAccessDenied
	case status == http.StatusNotFound:
		code = core.NotFound
	case status == http.StatusTooManyRequests:
		code = core.UpstreamThrottled
	}
	return core.Fail(code, fmt.Errorf("Teldrive returned HTTP %d", status))
}

// Close retries failed internal response cleanup. The client is borrowed.
func (d *Driver) Close() error {
	d.closed.Store(true)
	return d.cleanup.Retry()
}

var _ core.Driver = (*Driver)(nil)
