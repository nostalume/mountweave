// Package misskey mounts an authenticated account's Misskey Drive.
package misskey

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/internal/httpcontent"
)

type Options struct {
	Endpoint string // HTTPS origin of the Misskey instance.
	RootPath string // Absolute Drive path, with "/" as the default.
	// Client supplies a read:drive credential for the selected account.
	Client *http.Client
	// ContentClient is optional. Its transport must omit account credentials.
	ContentClient *http.Client
	// ContentOrigins explicitly allows HTTPS origins for bytes and redirects.
	ContentOrigins   []string
	MaxResponseBytes int64
}

type Driver struct {
	api, content http.Client
	base, root   string
	origins      map[string]struct{}
	limit        int64
	closed       atomic.Bool
	cleanup      httpcontent.ResponseCleanup
}

// Open verifies read access and resolves a directory in the account's Drive.
// Close a non-nil failed result to retry any retained response cleanup.
func Open(ctx context.Context, o Options) (*Driver, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	base, valid := configuredOrigin(o.Endpoint)
	if !valid || o.Client == nil || o.Client == o.ContentClient ||
		o.MaxResponseBytes < 0 || o.MaxResponseBytes == math.MaxInt64 {
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
	d := &Driver{api: httpcontent.IsolatedClient(o.Client), content: httpcontent.IsolatedClient(o.ContentClient),
		base: "https://" + base.Host + "/api/drive", limit: o.MaxResponseBytes,
		origins: make(map[string]struct{}, len(o.ContentOrigins))}
	for _, raw := range o.ContentOrigins {
		u, valid := configuredOrigin(raw)
		if !valid {
			return nil, core.Fail(core.InvalidRequest, errors.New("invalid Misskey content origin"))
		}
		d.origins[origin(u)] = struct{}{}
	}
	var err error
	if o.RootPath == "/" {
		_, err = d.page(ctx, "", "", 1, false)
	} else {
		var root item
		root, err = d.resolve(ctx, core.NativePath(o.RootPath))
		if err == nil {
			if root.kind != core.Directory {
				err = core.Fail(core.NotDirectory, nil)
			} else {
				d.root = root.ID
			}
		}
	}
	if err == nil {
		err = ctx.Err()
	}
	if err != nil {
		d.closed.Store(true)
	}
	return d, err
}

func httpsURL(u *url.URL) bool {
	return u.Scheme == "https" && u.Hostname() != "" && u.User == nil && u.Opaque == "" && u.Fragment == ""
}

func configuredOrigin(raw string) (*url.URL, bool) {
	u, err := url.Parse(raw)
	return u, err == nil && httpsURL(u) && (u.EscapedPath() == "" || u.EscapedPath() == "/") &&
		!u.ForceQuery && u.RawQuery == "" && !strings.Contains(raw, "#")
}

func origin(u *url.URL) string {
	port := u.Port()
	if port == "" {
		port = "443"
	}
	return net.JoinHostPort(strings.ToLower(u.Hostname()), port)
}

func (d *Driver) request(ctx context.Context, client *http.Client, method, target string, payload []byte, wanted *core.Range) (*http.Response, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if d.closed.Load() {
		return nil, core.Fail(core.MountUnavailable, nil)
	}
	req, err := http.NewRequestWithContext(ctx, method, target, bytes.NewReader(payload))
	if err != nil {
		return nil, core.Fail(core.InvalidUpstreamResponse, nil)
	}
	req.Header.Set("Accept-Encoding", "identity")
	if method == http.MethodPost {
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Accept", "application/json")
	}
	if wanted != nil {
		req.Header.Set("Range", httpcontent.Header(wanted))
	}
	response, err := client.Do(req)
	if err != nil {
		return nil, errors.Join(core.Fail(core.UpstreamFailure, httpcontent.PrivateIOError(err)), ctx.Err())
	}
	return response, nil
}

func (d *Driver) list(ctx context.Context, route string, body listRequest, out *[]item) (err error) {
	data, err := json.Marshal(body)
	if err != nil {
		return err
	}
	response, err := d.request(ctx, &d.api, http.MethodPost, d.base+route, data, nil)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, d.cleanup.CloseBody(response.Body), ctx.Err()) }()
	if response.StatusCode != http.StatusOK {
		return responseError(response.StatusCode)
	}
	return httpcontent.DecodeJSON(response, d.limit, out)
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
	return core.Fail(code, fmt.Errorf("Misskey returned HTTP %d", status))
}

// Close retries internal cleanup. The two HTTP clients remain host-owned.
func (d *Driver) Close() error {
	d.closed.Store(true)
	return d.cleanup.Retry()
}

var _ core.Driver = (*Driver)(nil)
