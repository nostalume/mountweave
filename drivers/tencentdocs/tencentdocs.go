// Package tencentdocs mounts a Tencent Docs folder and optionally streams document exports.
package tencentdocs

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

const endpoint = "https://docs.qq.com/openapi/drive/v2"

// Options selects a folder within the user's authorized Tencent Docs namespace.
type Options struct {
	FolderID string
	// Client supplies Tencent Docs Access-Token, Client-Id and Open-Id.
	Client *http.Client
	// ContentClient must not add credentials to delegated download requests.
	ContentClient    *http.Client
	MaxResponseBytes int64
	// AllowExport permits an Open to submit a quota-consuming export request.
	AllowExport bool
}

type Driver struct {
	root        string
	api, body   http.Client
	limit       int64
	allowExport bool
	closed      atomic.Bool
	cleanup     httpcontent.ResponseCleanup
}

// Open verifies the selected folder. Close a non-nil failed result to retry cleanup.
func Open(ctx context.Context, o Options) (*Driver, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !core.ValidName(o.FolderID) || o.Client == nil || o.Client == o.ContentClient ||
		o.MaxResponseBytes < 0 || o.MaxResponseBytes == math.MaxInt64 {
		return nil, core.Fail(core.InvalidRequest, nil)
	}
	if o.MaxResponseBytes == 0 {
		o.MaxResponseBytes = 4 << 20
	}
	d := &Driver{root: o.FolderID, limit: o.MaxResponseBytes, allowExport: o.AllowExport,
		api: httpcontent.IsolatedClient(o.Client), body: httpcontent.IsolatedClient(o.ContentClient)}
	reply, err := call[struct {
		Folder struct {
			ID     string `json:"ID"`
			Status string `json:"status"`
		} `json:"folderInfo"`
	}](ctx, d, http.MethodGet, "/folders/"+url.PathEscape(d.root)+"/metadata")
	if err == nil && (reply.Folder.ID != d.root || !strings.EqualFold(reply.Folder.Status, "normal")) {
		err = core.Fail(core.InvalidUpstreamResponse, errors.New("Tencent Docs did not confirm the selected folder"))
	}
	if err != nil {
		d.closed.Store(true)
	}
	return d, err
}

// Close retries retained response cleanup. Both HTTP clients remain host-owned.
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

func call[T any](ctx context.Context, d *Driver, method, route string) (data T, err error) {
	if err := d.active(ctx); err != nil {
		return data, err
	}
	req, err := http.NewRequestWithContext(ctx, method, endpoint+route, nil)
	if err != nil {
		return data, core.Fail(core.InvalidRequest, nil)
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Accept-Encoding", "identity")
	if method == http.MethodPost {
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	}
	resp, err := d.api.Do(req)
	if err != nil {
		return data, errors.Join(httpcontent.UpstreamIOError(err), ctx.Err())
	}
	defer func() { err = errors.Join(err, d.cleanup.CloseBody(resp.Body), ctx.Err()) }()
	if resp.StatusCode != http.StatusOK {
		return data, statusError(resp.StatusCode)
	}
	var reply struct {
		Ret  *int `json:"ret"`
		Data *T   `json:"data"`
	}
	if err := httpcontent.DecodeJSON(resp, d.limit, &reply); err != nil {
		if core.CodeOf(err) == core.InvalidUpstreamResponse {
			err = core.Fail(core.InvalidUpstreamResponse, errors.New("invalid Tencent Docs control response"))
		}
		return data, err
	}
	if reply.Ret == nil {
		return data, core.Fail(core.InvalidUpstreamResponse, nil)
	}
	switch *reply.Ret {
	case 0:
		if reply.Data == nil {
			return data, core.Fail(core.InvalidUpstreamResponse, nil)
		}
		return *reply.Data, nil
	case 10102:
		return data, core.Fail(core.NotFound, nil)
	case 10302, 10303, 10313, 37019:
		return data, core.Fail(core.UpstreamAccessDenied, fmt.Errorf("Tencent Docs result %d", *reply.Ret))
	default:
		return data, core.Fail(core.UpstreamFailure, fmt.Errorf("Tencent Docs result %d", *reply.Ret))
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
	case http.StatusGatewayTimeout:
		code = core.UpstreamTimeout
	}
	return core.Fail(code, fmt.Errorf("Tencent Docs HTTP status %d", status))
}

var _ core.Driver = (*Driver)(nil)
