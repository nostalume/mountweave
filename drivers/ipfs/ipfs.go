// Package ipfs mounts an immutable UnixFS directory through a trusted Kubo RPC.
package ipfs

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
	Endpoint string // Kubo server URL, with an optional proxy path prefix.
	RootCID  string // CID of a UnixFS directory.
	// Client supplies any RPC authentication. A nil Client uses the default transport.
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

// Open verifies a directory root. Close a non-nil failed result to retry any
// response cleanup retained by the driver.
func Open(ctx context.Context, o Options) (*Driver, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	endpoint, err := url.Parse(o.Endpoint)
	if err != nil || endpoint.Scheme != "http" && endpoint.Scheme != "https" || endpoint.Hostname() == "" ||
		endpoint.User != nil || endpoint.Opaque != "" || endpoint.Fragment != "" || endpoint.ForceQuery || endpoint.RawQuery != "" ||
		!core.ValidName(o.RootCID) || o.MaxResponseBytes < 0 || o.MaxResponseBytes == math.MaxInt64 {
		return nil, core.Fail(core.InvalidRequest, nil)
	}
	if o.MaxResponseBytes == 0 {
		o.MaxResponseBytes = 4 << 20
	}
	d := &Driver{base: strings.TrimRight(endpoint.String(), "/") + "/api/v0/", root: o.RootCID, limit: o.MaxResponseBytes}
	if o.Client != nil {
		d.client = *o.Client
	}
	d.client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	var stat struct{ Hash, Type string }
	err = d.json(ctx, "files/stat", url.Values{"arg": {"/ipfs/" + d.root}}, &stat)
	if err == nil {
		switch {
		case !core.ValidName(stat.Hash) || stat.Type == "":
			err = core.Fail(core.InvalidUpstreamResponse, errors.New("Kubo omitted the root identity or type"))
		case stat.Type != "directory":
			err = core.Fail(core.NotDirectory, nil)
		}
	}
	if err != nil {
		d.closed.Store(true)
	}
	return d, err
}

func (d *Driver) request(ctx context.Context, command string, query url.Values) (*http.Response, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if d.closed.Load() {
		return nil, core.Fail(core.MountUnavailable, nil)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, d.base+command+"?"+query.Encode(), nil)
	if err != nil {
		return nil, core.Fail(core.InvalidRequest, nil)
	}
	req.Header.Set("Accept-Encoding", "identity")
	resp, err := d.client.Do(req)
	if err != nil {
		return nil, errors.Join(core.Fail(core.UpstreamFailure, httpcontent.PrivateIOError(err)), ctx.Err())
	}
	return resp, nil
}

func (d *Driver) json(ctx context.Context, command string, query url.Values, out any) (err error) {
	query.Set("enc", "json")
	resp, err := d.request(ctx, command, query)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, d.cleanup.CloseBody(resp.Body), ctx.Err()) }()
	if resp.StatusCode != http.StatusOK {
		return responseError(resp.StatusCode)
	}
	if err := httpcontent.DecodeJSON(resp, d.limit, out); err != nil {
		return err
	}
	return streamFailure(resp)
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
	return core.Fail(code, fmt.Errorf("Kubo returned HTTP %d", status))
}

func streamFailure(resp *http.Response) error {
	if resp.Trailer.Get("X-Stream-Error") != "" {
		return core.Fail(core.UpstreamFailure, errors.New("Kubo reported a stream error"))
	}
	return nil
}

// Close retries internal response cleanup. The HTTP client is borrowed.
func (d *Driver) Close() error {
	d.closed.Store(true)
	return d.cleanup.Retry()
}

var _ core.Driver = (*Driver)(nil)
