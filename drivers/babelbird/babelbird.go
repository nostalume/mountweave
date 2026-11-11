// Package babelbird mounts authorized BabelBird Enterprise Drive folders.
package babelbird

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

// Options selects one workspace in an HTTPS BabelBird deployment.
type Options struct {
	Endpoint string
	RootID   string // "1" for enterprise files, "2" for personal files.
	RootPath string // Optional exact path under RootID.
	// Client supplies the deployment's access token. ContentClient is
	// independent and must not supply account credentials.
	Client, ContentClient *http.Client
	MaxResponseBytes      int64
}

type Driver struct {
	origin       *url.URL
	root         string
	api, content http.Client
	limit        int64
	closed       atomic.Bool
	cleanup      httpcontent.ResponseCleanup
}

// Open verifies the selected workspace and resolves its optional subdirectory.
// Close a non-nil failed result to retry retained response cleanup.
func Open(ctx context.Context, o Options) (*Driver, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	u, err := url.Parse(o.Endpoint)
	if err != nil || u.Scheme != "https" || u.Hostname() == "" || u.User != nil ||
		u.Opaque != "" || u.RawQuery != "" || u.ForceQuery || strings.ContainsRune(o.Endpoint, '#') ||
		(u.EscapedPath() != "" && u.EscapedPath() != "/") || (o.RootID != "1" && o.RootID != "2") ||
		o.Client == nil || o.Client == o.ContentClient || o.MaxResponseBytes < 0 || o.MaxResponseBytes == math.MaxInt64 {
		return nil, core.Fail(core.InvalidRequest, nil)
	}
	if o.RootPath == "" {
		o.RootPath = "/"
	}
	if err := core.ValidatePath(o.RootPath); err != nil {
		return nil, err
	}
	if o.MaxResponseBytes == 0 {
		o.MaxResponseBytes = 4 << 20
	}
	d := &Driver{origin: u, root: o.RootID, limit: o.MaxResponseBytes,
		api: httpcontent.IsolatedClient(o.Client), content: httpcontent.IsolatedClient(o.ContentClient)}
	if o.RootPath == "/" {
		check := listing{driver: d, parent: d.root, filter: folders}
		_, err = check.readPage(ctx)
		if err == io.EOF {
			err = nil
		}
	} else {
		var selected entry
		selected, err = d.resolve(ctx, core.NativePath(o.RootPath))
		if err == nil {
			if selected.kind != core.Directory {
				err = core.Fail(core.NotDirectory, nil)
			} else {
				d.root = selected.ID
			}
		}
	}
	if err != nil {
		d.closed.Store(true)
	}
	return d, err
}

// Close retries retained response cleanup. The host owns both transports.
func (d *Driver) Close() error {
	d.closed.Store(true)
	return d.cleanup.Retry()
}

func (d *Driver) active(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if d.closed.Load() {
		return core.Fail(core.MountUnavailable, nil)
	}
	return nil
}

type result struct {
	Success bool `json:"success"`
	Code    *int `json:"code"`
}

func (r result) check() error {
	if r.Code == nil || r.Success != (*r.Code == 0) {
		return core.Fail(core.InvalidUpstreamResponse, errors.New("invalid BabelBird result"))
	}
	if !r.Success {
		return core.Fail(core.UpstreamFailure, fmt.Errorf("BabelBird result code %d", *r.Code))
	}
	return nil
}

func (d *Driver) get(ctx context.Context, client *http.Client, address, accept, byteRange string) (*http.Response, error) {
	if err := d.active(ctx); err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, address, nil)
	if err != nil {
		return nil, core.Fail(core.InvalidUpstreamResponse, nil)
	}
	req.Header.Set("Accept", accept)
	req.Header.Set("Accept-Encoding", "identity")
	if byteRange != "" {
		req.Header.Set("Range", byteRange)
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, errors.Join(httpcontent.UpstreamIOError(err), ctx.Err())
	}
	return resp, nil
}

func (d *Driver) readJSON(ctx context.Context, path string, q url.Values, out any) error {
	address := d.origin.ResolveReference(&url.URL{Path: path, RawQuery: q.Encode()})
	resp, err := d.get(ctx, &d.api, address.String(), "application/json", "")
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return d.reject(ctx, resp)
	}
	err = httpcontent.DecodeJSON(resp, d.limit, out)
	return errors.Join(err, d.cleanup.CloseBody(resp.Body), ctx.Err())
}

func (d *Driver) reject(ctx context.Context, resp *http.Response) error {
	code := core.UpstreamFailure
	switch resp.StatusCode {
	case http.StatusUnauthorized, http.StatusForbidden:
		code = core.UpstreamAccessDenied
	case http.StatusNotFound:
		code = core.NotFound
	case http.StatusTooManyRequests:
		code = core.UpstreamThrottled
	case http.StatusRequestTimeout, http.StatusGatewayTimeout:
		code = core.UpstreamTimeout
	}
	return errors.Join(core.Fail(code, fmt.Errorf("BabelBird HTTP status %d", resp.StatusCode)),
		d.cleanup.CloseBody(resp.Body), ctx.Err())
}

var _ core.Driver = (*Driver)(nil)
