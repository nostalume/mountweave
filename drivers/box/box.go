// Package box mounts an authorized Box folder as a read-only source.
package box

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"sync/atomic"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/internal/httpcontent"
)

const endpoint = "https://api.box.com/2.0"

type Options struct {
	RootID string // Empty selects the account root, ID 0.
	// Client authenticates Box API requests. ContentClient is optional and
	// must be free of credentials, including any added by its transport.
	Client, ContentClient *http.Client
	MaxResponseBytes      int64
}

type Driver struct {
	root         string
	api, content http.Client
	limit        int64
	closed       atomic.Bool
	cleanup      httpcontent.ResponseCleanup
}

// Open confirms the selected folder. A non-nil failed result retains any
// response cleanup that its caller must finish with Close.
func Open(ctx context.Context, o Options) (*Driver, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if o.RootID == "" {
		o.RootID = "0"
	}
	if !core.ValidName(o.RootID) || o.Client == nil || o.Client == o.ContentClient ||
		o.MaxResponseBytes < 0 || o.MaxResponseBytes == math.MaxInt64 {
		return nil, core.Fail(core.InvalidRequest, nil)
	}
	if o.MaxResponseBytes == 0 {
		o.MaxResponseBytes = 4 << 20
	}
	d := &Driver{root: o.RootID, api: *o.Client, limit: o.MaxResponseBytes}
	if o.ContentClient != nil {
		d.content = *o.ContentClient
	}
	for _, client := range []*http.Client{&d.api, &d.content} {
		client.Jar = nil
		client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	}
	err := d.confirmRoot(ctx)
	if err != nil {
		d.closed.Store(true)
	}
	return d, err
}

func (d *Driver) confirmRoot(ctx context.Context) error {
	var folder struct{ ID, Type string }
	if err := d.getJSON(ctx, folderURL(d.root)+"?fields=id,type", &folder); err != nil {
		return err
	}
	if folder.ID != d.root || folder.Type != "folder" {
		return core.Fail(core.InvalidUpstreamResponse, errors.New("Box folder details disagree with the selected root"))
	}
	return nil
}

func folderURL(id string) string { return endpoint + "/folders/" + url.PathEscape(id) }

func (d *Driver) check(ctx context.Context, a core.NativeAddress) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if d.closed.Load() {
		return core.Fail(core.MountUnavailable, nil)
	}
	if a.Namespace() != core.TreeNamespace {
		return core.Fail(core.UnsupportedOperation, nil)
	}
	return core.ValidatePath(a.Value())
}

func (d *Driver) get(ctx context.Context, client *http.Client, target, accept, byteRange string) (*http.Response, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if d.closed.Load() {
		return nil, core.Fail(core.MountUnavailable, nil)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return nil, core.Fail(core.InvalidUpstreamResponse, nil)
	}
	if accept != "" {
		req.Header.Set("Accept", accept)
	}
	if byteRange != "" {
		req.Header.Set("Range", byteRange)
	}
	req.Header.Set("Accept-Encoding", "identity")
	resp, err := client.Do(req)
	if err != nil {
		return nil, errors.Join(core.Fail(core.UpstreamFailure, httpcontent.PrivateIOError(err)), ctx.Err())
	}
	return resp, nil
}

func (d *Driver) getJSON(ctx context.Context, target string, out any) (err error) {
	resp, err := d.get(ctx, &d.api, target, "application/json", "")
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, d.cleanup.CloseBody(resp.Body), ctx.Err()) }()
	if resp.StatusCode != http.StatusOK {
		return responseError(resp)
	}
	return httpcontent.DecodeJSON(resp, d.limit, out)
}

func responseError(resp *http.Response) error {
	code := core.UpstreamFailure
	switch resp.StatusCode {
	case http.StatusUnauthorized, http.StatusForbidden:
		code = core.UpstreamAccessDenied
	case http.StatusNotFound:
		code = core.NotFound
	case http.StatusTooManyRequests:
		code = core.UpstreamThrottled
	}
	// Box supplies a wait in seconds for a file that is not yet available.
	if resp.StatusCode == http.StatusAccepted || resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode == http.StatusServiceUnavailable {
		if seconds, err := strconv.ParseUint(resp.Header.Get("Retry-After"), 10, 64); err == nil {
			return core.Fail(code, fmt.Errorf("Box returned HTTP %d; retry after %d seconds", resp.StatusCode, seconds))
		}
	}
	return core.Fail(code, fmt.Errorf("Box returned HTTP %d", resp.StatusCode))
}

// Close retries failed internal response cleanup. The clients and their
// transports remain owned by the host.
func (d *Driver) Close() error {
	d.closed.Store(true)
	return d.cleanup.Retry()
}

var _ core.Driver = (*Driver)(nil)
