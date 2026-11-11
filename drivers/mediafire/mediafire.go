// Package mediafire mounts a folder through the MediaFire 1.5 Core API.
package mediafire

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"unicode/utf8"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/internal/httpcontent"
)

const api = "https://www.mediafire.com/api/1.5/"

type Options struct {
	RootKey string // Native key of the authorized folder.
	// SessionToken supplies a MediaFire v1 session token for the same account
	// throughout the mount's lifetime. The host owns login and renewal.
	SessionToken func(context.Context) (string, error)
	// Client is optional. ContentClient must never add account credentials.
	Client, ContentClient *http.Client
	MaxResponseBytes      int64
}

type Driver struct {
	root         string
	token        func(context.Context) (string, error)
	api, content http.Client
	limit        int64
	closed       atomic.Bool
	cleanup      httpcontent.ResponseCleanup
}

// Open confirms the folder key with MediaFire. Close a non-nil failed result
// to retry any response cleanup retained by the driver.
func Open(ctx context.Context, o Options) (*Driver, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !singleKey(o.RootKey) || o.SessionToken == nil || o.Client != nil && o.Client == o.ContentClient ||
		o.MaxResponseBytes < 0 || o.MaxResponseBytes == math.MaxInt64 {
		return nil, core.Fail(core.InvalidRequest, nil)
	}
	if o.MaxResponseBytes == 0 {
		o.MaxResponseBytes = 4 << 20
	}
	d := &Driver{root: o.RootKey, token: o.SessionToken, limit: o.MaxResponseBytes}
	if o.Client != nil {
		d.api = *o.Client
	}
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

// Both native key parameters also accept comma-separated lists.
func singleKey(key string) bool { return core.ValidName(key) && !strings.Contains(key, ",") }

func (d *Driver) confirmRoot(ctx context.Context) error {
	response, err := d.call(ctx, "folder/get_info", url.Values{"folder_key": {d.root}})
	if err != nil {
		return err
	}
	if response.FolderInfo.FolderKey != d.root {
		return core.Fail(core.InvalidUpstreamResponse, errors.New("MediaFire did not confirm the selected folder key"))
	}
	return nil
}

type receipt struct {
	Action, Result string
	Error          json.Number
	FolderInfo     struct{ FolderKey string } `json:"folder_info"`
	FolderContent  struct {
		Type           string      `json:"content_type"`
		Chunk          json.Number `json:"chunk_number"`
		More           string      `json:"more_chunks"`
		Files, Folders []item
	} `json:"folder_content"`
	Links []struct {
		QuickKey       string
		Error          string
		DirectDownload string      `json:"direct_download"`
		DownloadError  json.Number `json:"direct_download_error"`
	}
}

func (d *Driver) call(ctx context.Context, action string, query url.Values) (result receipt, err error) {
	if err := d.active(ctx); err != nil {
		return result, err
	}
	token, err := d.token(ctx)
	if err != nil {
		return result, httpcontent.PrivateIOError(err)
	}
	if token == "" || !utf8.ValidString(token) {
		return result, core.Fail(core.InvalidRequest, errors.New("MediaFire session token is missing or invalid"))
	}
	query.Set("session_token", token)
	query.Set("response_format", "json")
	response, err := d.get(ctx, &d.api, api+action+".php?"+query.Encode(), "")
	if err != nil {
		return result, err
	}
	defer func() { err = errors.Join(err, d.cleanup.CloseBody(response.Body), ctx.Err()) }()
	var envelope struct{ Response receipt }
	if err := httpcontent.DecodeJSON(response, d.limit, &envelope); err != nil {
		err = core.Fail(core.CodeOf(err), httpcontent.PrivateIOError(err))
		if response.StatusCode != http.StatusOK {
			err = errors.Join(httpFailure(response.StatusCode), err)
		}
		return result, err
	}
	result = envelope.Response
	if result.Action != action || result.Result != "Success" && result.Result != "Error" {
		return result, core.Fail(core.InvalidUpstreamResponse, errors.New("MediaFire returned an invalid action receipt"))
	}
	if result.Result == "Error" {
		code, _ := result.Error.Int64()
		return result, nativeFailure(response.StatusCode, code)
	}
	if response.StatusCode != http.StatusOK {
		return result, httpFailure(response.StatusCode)
	}
	return result, nil
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

func (d *Driver) get(ctx context.Context, client *http.Client, target, byteRange string) (*http.Response, error) {
	if err := d.active(ctx); err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return nil, core.Fail(core.InvalidUpstreamResponse, nil)
	}
	req.Header.Set("Accept-Encoding", "identity")
	if byteRange != "" {
		req.Header.Set("Range", byteRange)
	}
	response, err := client.Do(req)
	if err != nil {
		return nil, errors.Join(core.Fail(core.UpstreamFailure, httpcontent.PrivateIOError(err)), ctx.Err())
	}
	return response, nil
}

func statusCode(status int) core.Code {
	switch status {
	case http.StatusUnauthorized, http.StatusForbidden:
		return core.UpstreamAccessDenied
	case http.StatusNotFound:
		return core.NotFound
	case http.StatusTooManyRequests:
		return core.UpstreamThrottled
	default:
		return core.UpstreamFailure
	}
}

func httpFailure(status int) error {
	return core.Fail(statusCode(status), fmt.Errorf("MediaFire returned HTTP %d", status))
}

func nativeFailure(status int, native int64) error {
	code := statusCode(status)
	switch native {
	case 104, 105, 107, 114:
		code = core.UpstreamAccessDenied
	case 110, 112:
		code = core.NotFound
	}
	return core.Fail(code, fmt.Errorf("MediaFire returned HTTP %d, API error %d", status, native))
}

// Close retries internal response cleanup. Both clients remain host-owned.
func (d *Driver) Close() error {
	d.closed.Store(true)
	return d.cleanup.Retry()
}

var _ core.Driver = (*Driver)(nil)
