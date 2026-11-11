// Package googlephotos mounts one app-created Google Photos album for export.
package googlephotos

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"sync/atomic"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/internal/httpcontent"
)

const api = "https://photoslibrary.googleapis.com/v1"

type Options struct {
	AlbumID string // ID of an album created by the authorized application.
	// Client authenticates the Library API. ContentClient is optional and
	// must be credential-free, including any credentials added by its transport.
	Client, ContentClient *http.Client
	MaxResponseBytes      int64
}

type Driver struct {
	album        string
	api, content http.Client
	limit        int64
	closed       atomic.Bool
	cleanup      httpcontent.ResponseCleanup
}

// Open verifies the selected album. Close a non-nil failed result to retry
// any response cleanup retained by the driver.
func Open(ctx context.Context, o Options) (*Driver, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !core.ValidName(o.AlbumID) || o.Client == nil || o.Client == o.ContentClient ||
		o.MaxResponseBytes < 0 || o.MaxResponseBytes == math.MaxInt64 {
		return nil, core.Fail(core.InvalidRequest, nil)
	}
	if o.MaxResponseBytes == 0 {
		o.MaxResponseBytes = 4 << 20
	}
	d := &Driver{album: o.AlbumID, api: *o.Client, limit: o.MaxResponseBytes}
	if o.ContentClient != nil {
		d.content = *o.ContentClient
	}
	for _, client := range []*http.Client{&d.api, &d.content} {
		client.Jar = nil
		client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	}
	err := d.confirmAlbum(ctx)
	if err != nil {
		d.closed.Store(true)
	}
	return d, err
}

func (d *Driver) confirmAlbum(ctx context.Context) error {
	var album struct{ ID string }
	if err := d.getJSON(ctx, http.MethodGet, "/albums/"+url.PathEscape(d.album), nil, &album); err != nil {
		return err
	}
	if album.ID != d.album {
		return core.Fail(core.InvalidUpstreamResponse, errors.New("Google Photos did not confirm the selected album"))
	}
	return nil
}

func (d *Driver) request(ctx context.Context, client *http.Client, method, target string, payload []byte, byteRange string) (*http.Response, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if d.closed.Load() {
		return nil, core.Fail(core.MountUnavailable, nil)
	}
	var body io.Reader
	if payload != nil {
		body = bytes.NewReader(payload)
	}
	req, err := http.NewRequestWithContext(ctx, method, target, body)
	if err != nil {
		return nil, core.Fail(core.InvalidUpstreamResponse, nil)
	}
	if client == &d.api {
		req.Header.Set("Accept", "application/json")
	}
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
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

func (d *Driver) getJSON(ctx context.Context, method, route string, payload []byte, out any) (err error) {
	resp, err := d.request(ctx, &d.api, method, api+route, payload, "")
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
	return core.Fail(code, fmt.Errorf("Google Photos returned HTTP %d", status))
}

// Close retries internal response cleanup. The HTTP clients remain host-owned.
func (d *Driver) Close() error {
	d.closed.Store(true)
	return d.cleanup.Retry()
}

var _ core.Driver = (*Driver)(nil)
