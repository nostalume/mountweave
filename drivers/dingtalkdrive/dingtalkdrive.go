// Package dingtalkdrive mounts an authorized DingTalk Drive folder.
package dingtalkdrive

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"sync/atomic"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/internal/httpcontent"
)

const endpoint = "https://api.dingtalk.com/v1.0/storage/spaces/"

type Options struct {
	SpaceID, RootID, OperatorUnionID string
	// Client authenticates control requests with an approved internal app token.
	Client *http.Client
	// ContentClient is optional. Its transport must omit account credentials.
	ContentClient    *http.Client
	MaxResponseBytes int64
}

type Driver struct {
	api, content http.Client
	space, root  string
	operator     string
	dentries     string
	limit        int64
	closed       atomic.Bool
	cleanup      httpcontent.ResponseCleanup
}

// Open verifies the selected folder's identity and space. Close a non-nil
// failed result to retry any retained response cleanup.
func Open(ctx context.Context, o Options) (*Driver, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !core.ValidName(o.SpaceID) || !core.ValidName(o.RootID) || !core.ValidName(o.OperatorUnionID) ||
		o.Client == nil || o.Client == o.ContentClient || o.MaxResponseBytes < 0 || o.MaxResponseBytes == math.MaxInt64 {
		return nil, core.Fail(core.InvalidRequest, nil)
	}
	if o.MaxResponseBytes == 0 {
		o.MaxResponseBytes = 4 << 20
	}
	d := &Driver{api: httpcontent.IsolatedClient(o.Client), content: httpcontent.IsolatedClient(o.ContentClient),
		space: o.SpaceID, root: o.RootID, operator: o.OperatorUnionID, limit: o.MaxResponseBytes,
		dentries: endpoint + url.PathEscape(o.SpaceID) + "/dentries"}
	var reply struct{ Dentry item }
	err := d.control(ctx, http.MethodPost, "/"+url.PathEscape(d.root)+"/query", nil, &reply)
	if err == nil {
		switch {
		case reply.Dentry.ID != d.root || reply.Dentry.Space != d.space || !reply.Dentry.active():
			err = core.Fail(core.InvalidUpstreamResponse, errors.New("DingTalk did not confirm the selected root"))
		case reply.Dentry.Type != "FOLDER":
			err = core.Fail(core.NotDirectory, nil)
		default:
			err = ctx.Err()
		}
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

func (d *Driver) request(client *http.Client, req *http.Request) (*http.Response, error) {
	if err := d.check(req.Context()); err != nil {
		return nil, err
	}
	if req.Header.Get("Accept-Encoding") == "" {
		req.Header.Set("Accept-Encoding", "identity")
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, errors.Join(httpcontent.UpstreamIOError(err), req.Context().Err())
	}
	return resp, nil
}

func (d *Driver) control(ctx context.Context, method, route string, q url.Values, out any) (err error) {
	if q == nil {
		q = make(url.Values, 1)
	}
	q.Set("unionId", d.operator)
	var input *bytes.Reader
	if method == http.MethodPost {
		input = bytes.NewReader([]byte("{}"))
	} else {
		input = bytes.NewReader(nil)
	}
	req, err := http.NewRequestWithContext(ctx, method, d.dentries+route+"?"+q.Encode(), input)
	if err != nil {
		return core.Fail(core.InvalidRequest, nil)
	}
	req.Header.Set("Accept", "application/json")
	if method == http.MethodPost {
		req.Header.Set("Content-Type", "application/json")
	}
	response, err := d.request(&d.api, req)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, d.cleanup.CloseBody(response.Body), ctx.Err()) }()
	if response.StatusCode != http.StatusOK {
		var reply struct{ Code string }
		_ = httpcontent.DecodeJSON(response, d.limit, &reply)
		return statusError(response.StatusCode, reply.Code)
	}
	data, err := httpcontent.ReadIdentity(response, d.limit)
	if err != nil {
		return err
	}
	if !bytes.HasPrefix(bytes.TrimSpace(data), []byte("{")) || json.Unmarshal(data, out) != nil {
		return core.Fail(core.InvalidUpstreamResponse, errors.New("invalid DingTalk response"))
	}
	return nil
}

func statusError(status int, native string) error {
	code := core.UpstreamFailure
	switch status {
	case http.StatusUnauthorized, http.StatusForbidden:
		code = core.UpstreamAccessDenied
	case http.StatusNotFound:
		code = core.NotFound
	case http.StatusTooManyRequests:
		code = core.UpstreamThrottled
	case http.StatusRequestTimeout, http.StatusGatewayTimeout:
		code = core.UpstreamTimeout
	}
	switch native {
	case "operationNotSupported", "dentryDownloadProtocolNotSupport":
		code = core.UnsupportedOperation
	case "operationTimeout":
		code = core.UpstreamTimeout
	case "permissionDenied", "orgAuthLevelNotEnough", "spaceNotExist", "dentryNotExist", "fileArchived", "paramError.spaceId", "paramError.dentryId":
	default:
		native = ""
	}
	if native != "" {
		return core.Fail(code, fmt.Errorf("DingTalk HTTP %d (%s)", status, native))
	}
	return core.Fail(code, fmt.Errorf("DingTalk HTTP %d", status))
}

// Close retries internal response cleanup. Both HTTP clients are host-owned.
func (d *Driver) Close() error {
	d.closed.Store(true)
	return d.cleanup.Retry()
}

var _ core.Driver = (*Driver)(nil)
