// Package wecomdrive mounts a folder in a WeCom Wedrive space.
package wecomdrive

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

const endpoint = "https://qyapi.weixin.qq.com/cgi-bin/wedrive/"

type Options struct {
	SpaceID, FolderID string
	// AccessToken supplies a token from a WeCom app authorized for Wedrive.
	AccessToken func(context.Context) (string, error)
	// ContentClient is independent and must never inject account credentials.
	Client, ContentClient *http.Client
	MaxResponseBytes      int64
}

type Driver struct {
	space, root  string
	accessToken  func(context.Context) (string, error)
	api, content http.Client
	limit        int64
	closed       atomic.Bool
	cleanup      httpcontent.ResponseCleanup
}

type fileSelection struct {
	ID string `json:"fileid"`
}

// Open verifies that the selected native ID is a live folder in the given space.
// Close a non-nil failed result to retry retained response cleanup.
func Open(ctx context.Context, o Options) (*Driver, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if o.SpaceID == "" || o.FolderID == "" || o.SpaceID == o.FolderID || o.AccessToken == nil ||
		o.Client != nil && o.Client == o.ContentClient ||
		o.MaxResponseBytes < 0 || o.MaxResponseBytes == math.MaxInt64 {
		return nil, core.Fail(core.InvalidRequest, nil)
	}
	if o.MaxResponseBytes == 0 {
		o.MaxResponseBytes = 4 << 20
	}
	d := &Driver{space: o.SpaceID, root: o.FolderID, accessToken: o.AccessToken, limit: o.MaxResponseBytes,
		api: httpcontent.IsolatedClient(o.Client), content: httpcontent.IsolatedClient(o.ContentClient)}
	token, err := d.token(ctx)
	if err == nil {
		var reply struct {
			result
			File item `json:"file_info"`
		}
		err = d.call(ctx, "file_info", token, fileSelection{ID: d.root}, &reply)
		if err == nil {
			switch {
			case reply.File.ID != d.root || reply.File.Space != d.space:
				err = core.Fail(core.InvalidUpstreamResponse, errors.New("Wedrive did not confirm the selected live folder"))
			case reply.File.Status == 2:
				err = core.Fail(core.NotFound, nil)
			case reply.File.Status != 1 || reply.File.Type == 0:
				err = core.Fail(core.InvalidUpstreamResponse, errors.New("Wedrive did not confirm the folder type or status"))
			case reply.File.Type != 1:
				err = core.Fail(core.NotDirectory, nil)
			}
		}
	}
	if err != nil {
		d.closed.Store(true)
	}
	return d, err
}

// Close retries internal response cleanup. Both HTTP clients remain host-owned.
func (d *Driver) Close() error {
	d.closed.Store(true)
	return d.cleanup.Retry()
}

type result struct {
	Code *int `json:"errcode"`
}

func (r result) check() error {
	if r.Code == nil {
		return core.Fail(core.InvalidUpstreamResponse, errors.New("Wedrive result code is missing"))
	}
	if *r.Code == 0 {
		return nil
	}
	code := core.UpstreamFailure
	switch *r.Code {
	case 40014, 41001, 42001, 48002:
		code = core.UpstreamAccessDenied
	case 45009:
		code = core.UpstreamThrottled
	}
	return core.Fail(code, fmt.Errorf("Wedrive result code %d", *r.Code))
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

func (d *Driver) token(ctx context.Context) (string, error) {
	if err := d.check(ctx); err != nil {
		return "", err
	}
	token, err := d.accessToken(ctx)
	if cause := ctx.Err(); cause != nil {
		return "", cause
	}
	if err != nil {
		return "", httpcontent.UpstreamIOError(err)
	}
	if token == "" {
		return "", core.Fail(core.UpstreamAccessDenied, errors.New("Wedrive access token unavailable"))
	}
	return token, nil
}

func (d *Driver) call(ctx context.Context, route, token string, in any, out interface{ check() error }) (err error) {
	data, err := json.Marshal(in)
	if err != nil {
		return core.Fail(core.InvalidRequest, err)
	}
	query := url.Values{"access_token": {token}}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint+route+"?"+query.Encode(), bytes.NewReader(data))
	if err != nil {
		return core.Fail(core.InvalidUpstreamResponse, nil)
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", "application/json")
	response, err := d.do(&d.api, req)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, d.cleanup.CloseBody(response.Body), ctx.Err()) }()
	if response.StatusCode != http.StatusOK {
		return statusError(response.StatusCode)
	}
	if err := httpcontent.DecodeJSON(response, d.limit, out); err != nil {
		return err
	}
	return out.check()
}

func (d *Driver) do(client *http.Client, req *http.Request) (*http.Response, error) {
	if err := d.check(req.Context()); err != nil {
		return nil, err
	}
	req.Header.Set("Accept-Encoding", "identity")
	response, err := client.Do(req)
	if err != nil {
		return nil, errors.Join(httpcontent.UpstreamIOError(err), req.Context().Err())
	}
	return response, nil
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
	return core.Fail(code, fmt.Errorf("Wedrive HTTP status %d", status))
}

var _ core.Driver = (*Driver)(nil)
