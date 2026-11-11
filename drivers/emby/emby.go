// Package emby mounts a user-scoped Emby folder and its original files.
package emby

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/internal/httpcontent"
)

type Options struct {
	Endpoint string // Emby server URL, with an optional proxy path prefix.
	UserID   string // Native ID of the user authenticated by Client.
	RootID   string // Native ID of a browsable folder in that user's library.
	// Client supplies that user's authentication, including after credential rotation.
	Client           *http.Client
	MaxResponseBytes int64
}

type Driver struct {
	base, userRoute, root string
	client                http.Client
	limit                 int64
	closed                atomic.Bool
	cleanup               httpcontent.ResponseCleanup
}

// Open verifies the folder in the authenticated user's library. Close a non-nil
// failed result to retry response cleanup retained by the driver.
func Open(ctx context.Context, o Options) (*Driver, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	endpoint, err := url.Parse(o.Endpoint)
	if err != nil || endpoint.Scheme != "http" && endpoint.Scheme != "https" || endpoint.Hostname() == "" ||
		endpoint.User != nil || endpoint.Opaque != "" || endpoint.Fragment != "" || endpoint.ForceQuery || endpoint.RawQuery != "" ||
		!core.ValidName(o.UserID) || !core.ValidName(o.RootID) || o.Client == nil ||
		o.MaxResponseBytes < 0 || o.MaxResponseBytes == math.MaxInt64 {
		return nil, core.Fail(core.InvalidRequest, nil)
	}
	if o.MaxResponseBytes == 0 {
		o.MaxResponseBytes = 4 << 20
	}
	d := &Driver{base: strings.TrimRight(endpoint.String(), "/") + "/emby", userRoute: "/Users/" + url.PathEscape(o.UserID),
		root: o.RootID, client: *o.Client, limit: o.MaxResponseBytes}
	d.client.Jar = nil
	d.client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	err = d.confirmRoot(ctx)
	if err != nil {
		d.closed.Store(true)
	}
	return d, err
}

func (d *Driver) confirmRoot(ctx context.Context) error {
	var folder struct {
		ID       string
		IsFolder *bool
	}
	if err := d.getJSON(ctx, d.userRoute+"/Items/"+url.PathEscape(d.root), &folder); err != nil {
		return err
	}
	if folder.ID != d.root || folder.IsFolder == nil {
		return core.Fail(core.InvalidUpstreamResponse, errors.New("Emby did not confirm the requested folder identity and type"))
	}
	if !*folder.IsFolder {
		return core.Fail(core.NotDirectory, nil)
	}
	return nil
}

func (d *Driver) get(ctx context.Context, route, accept, byteRange string) (*http.Response, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if d.closed.Load() {
		return nil, core.Fail(core.MountUnavailable, nil)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, d.base+route, nil)
	if err != nil {
		return nil, core.Fail(core.InvalidRequest, nil)
	}
	if accept != "" {
		req.Header.Set("Accept", accept)
	}
	if byteRange != "" {
		req.Header.Set("Range", byteRange)
	}
	req.Header.Set("Accept-Encoding", "identity")
	resp, err := d.client.Do(req)
	if err != nil {
		return nil, errors.Join(core.Fail(core.UpstreamFailure, httpcontent.PrivateIOError(err)), ctx.Err())
	}
	return resp, nil
}

func (d *Driver) getJSON(ctx context.Context, route string, out any) (err error) {
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
	switch status {
	case http.StatusUnauthorized, http.StatusForbidden:
		code = core.UpstreamAccessDenied
	case http.StatusNotFound:
		code = core.NotFound
	case http.StatusTooManyRequests:
		code = core.UpstreamThrottled
	}
	return core.Fail(code, fmt.Errorf("Emby returned HTTP %d", status))
}

// Close retries internal response cleanup. The HTTP client remains host-owned.
func (d *Driver) Close() error {
	d.closed.Store(true)
	return d.cleanup.Retry()
}

var _ core.Driver = (*Driver)(nil)
