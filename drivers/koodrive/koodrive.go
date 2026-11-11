// Package koodrive mounts ordinary files in Huawei Cloud KooDrive spaces.
package koodrive

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/internal/httpcontent"
)

const endpoint = "https://cn.myhuaweicloud-koodrive.com/koodrive/ose/v1"

type Options struct {
	UserID      string
	ContainerID string
	// Client signs requests for UserID on the China region KooDrive endpoint.
	// ContentClient is independent and must never inject account credentials.
	Client, ContentClient   *http.Client
	MaxResponseBytes        int64
	AllowMkdir, AllowCreate bool
	TeamSpace               bool // Mount a department or group space read-only.
}

type Driver struct {
	user, container, root   string
	listRoute               string
	api, content            http.Client
	limit                   int64
	allowMkdir, allowCreate bool
	closed                  atomic.Bool
	cleanup                 httpcontent.ResponseCleanup
}

// Open verifies the ordinary root of the selected individual or team space.
// Close a non-nil failed result to retry retained response cleanup.
func Open(ctx context.Context, o Options) (*Driver, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !core.ValidName(o.UserID) || !core.ValidName(o.ContainerID) ||
		o.Client == nil || o.Client == o.ContentClient ||
		o.MaxResponseBytes < 0 || o.MaxResponseBytes == math.MaxInt64 || o.TeamSpace && (o.AllowMkdir || o.AllowCreate) {
		return nil, core.Fail(core.InvalidRequest, nil)
	}
	if o.MaxResponseBytes == 0 {
		o.MaxResponseBytes = 4 << 20
	}
	d := &Driver{user: o.UserID, container: o.ContainerID, listRoute: "/files/0", limit: o.MaxResponseBytes,
		allowMkdir: o.AllowMkdir, allowCreate: o.AllowCreate,
		api: httpcontent.IsolatedClient(o.Client), content: httpcontent.IsolatedClient(o.ContentClient)}
	var reply struct {
		result
		Data struct {
			Type        json.RawMessage `json:"type"`
			OwnerID     string          `json:"ownerId"`
			ContainerID string          `json:"containerId"`
			Root        string          `json:"bizRootFileId"`
			Safe        string          `json:"bizSboxFileId"`
		} `json:"data"`
	}
	err := d.call(ctx, http.MethodGet, "/space/container/"+url.PathEscape(d.container), nil, &reply)
	if err == nil {
		err = reply.result.check()
	}
	if err == nil {
		space := reply.Data
		kind := strings.Trim(string(space.Type), `"`)
		switch {
		case kind != "0" && kind != "1" && kind != "2":
			err = core.Fail(core.InvalidUpstreamResponse, errors.New("invalid KooDrive space type"))
		case o.TeamSpace != (kind != "1"):
			err = core.Fail(core.UnsupportedOperation, errors.New("KooDrive space type differs from the requested mount"))
		case space.ContainerID != d.container || space.Root == "" || space.Root == space.Safe:
			err = core.Fail(core.InvalidUpstreamResponse, errors.New("KooDrive did not confirm the selected space and ordinary root"))
		case o.TeamSpace && !core.ValidName(space.OwnerID) || !o.TeamSpace && space.OwnerID != d.user:
			err = core.Fail(core.InvalidUpstreamResponse, errors.New("invalid KooDrive space owner"))
		default:
			d.root = space.Root
			if o.TeamSpace {
				d.listRoute = "/files/" + url.PathEscape(space.OwnerID) + "/0?team_type=" + kind
			}
		}
	}
	if err != nil {
		d.closed.Store(true)
	}
	return d, err
}

// Close retries failed internal response cleanup. Both HTTP clients remain host-owned.
func (d *Driver) Close() error {
	d.closed.Store(true)
	return d.cleanup.Retry()
}

func (d *Driver) parentParameter(parent string) string {
	if parent == d.root {
		return "root"
	}
	return parent
}

func (d *Driver) check(ctx context.Context, address core.NativeAddress) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if d.closed.Load() {
		return core.Fail(core.MountUnavailable, nil)
	}
	if address.Namespace() != core.TreeNamespace {
		return core.Fail(core.UnsupportedOperation, nil)
	}
	return core.ValidatePath(address.Value())
}

type result struct {
	Code json.RawMessage `json:"code"`
}

func (r result) check() error {
	code, err := strconv.ParseUint(strings.Trim(string(r.Code), `"`), 10, 64)
	if err != nil {
		return core.Fail(core.InvalidUpstreamResponse, errors.New("invalid KooDrive result code"))
	}
	if code != 0 {
		return core.Fail(core.UpstreamFailure, fmt.Errorf("KooDrive result code %d", code))
	}
	return nil
}

func (d *Driver) controlRequest(ctx context.Context, method, route string, input any) (*http.Request, error) {
	var body []byte
	if input != nil {
		var err error
		body, err = json.Marshal(input)
		if err != nil {
			return nil, core.Fail(core.InvalidRequest, err)
		}
	}
	req, err := http.NewRequestWithContext(ctx, method, endpoint+route, bytes.NewReader(body))
	if err != nil {
		return nil, core.Fail(core.InvalidRequest, nil)
	}
	req.Header.Set("Accept", "application/json")
	if input != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("X-User-Id", d.user)
	return req, nil
}

func (d *Driver) call(ctx context.Context, method, route string, input, out any) (err error) {
	req, err := d.controlRequest(ctx, method, route, input)
	if err != nil {
		return err
	}
	response, _, err := d.exchange(&d.api, req)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, d.cleanup.CloseBody(response.Body), ctx.Err()) }()
	if response.StatusCode != http.StatusOK {
		return statusError(response.StatusCode)
	}
	return httpcontent.DecodeJSON(response, d.limit, out)
}

func (d *Driver) exchange(client *http.Client, req *http.Request) (response *http.Response, dispatched bool, err error) {
	if err := req.Context().Err(); err != nil {
		return nil, false, err
	}
	if d.closed.Load() {
		return nil, false, core.Fail(core.MountUnavailable, nil)
	}
	req.Header.Set("Accept-Encoding", "identity")
	response, err = client.Do(req)
	if err != nil {
		return nil, true, errors.Join(httpcontent.UpstreamIOError(err), req.Context().Err())
	}
	return response, true, nil
}

func checkContentURL(address string) error {
	target, err := url.Parse(address)
	if err != nil || target.Scheme != "https" || target.Hostname() == "" ||
		target.User != nil || strings.ContainsRune(address, '#') || target.Opaque != "" {
		return core.Fail(core.InvalidUpstreamResponse, errors.New("invalid KooDrive content address"))
	}
	return nil
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
	}
	return core.Fail(code, errors.New("KooDrive HTTP status "+strconv.Itoa(status)))
}

var _ core.Driver = (*Driver)(nil)
