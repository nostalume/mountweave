// Package guangyapan mounts a Guangya Pan account through its Open API.
package guangyapan

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"unicode/utf8"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/internal/httpcontent"
)

const endpoint = "https://openapi.guangyapan.com/openapi/v1/file/"

type Options struct {
	ClientID string
	RootID   string // Empty selects the account's Drive root.
	// Client authenticates Open API requests for the selected user.
	Client *http.Client
	// ContentClient is optional. Its transport must omit account credentials.
	ContentClient    *http.Client
	MaxResponseBytes int64
}

type Driver struct {
	api, content   http.Client
	clientID, root string
	limit          int64
	closed         atomic.Bool
	cleanup        httpcontent.ResponseCleanup
}

// Open verifies the account root or the selected native directory.
// Close a non-nil failed result to retry any retained response cleanup.
func Open(ctx context.Context, o Options) (*Driver, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if o.ClientID == "" || strings.ContainsAny(o.ClientID, "\r\n") || !utf8.ValidString(o.ClientID) ||
		!utf8.ValidString(o.RootID) || o.Client == nil || o.Client == o.ContentClient ||
		o.MaxResponseBytes < 0 || o.MaxResponseBytes == math.MaxInt64 {
		return nil, core.Fail(core.InvalidRequest, nil)
	}
	if o.MaxResponseBytes == 0 {
		o.MaxResponseBytes = 4 << 20
	}
	d := &Driver{api: httpcontent.IsolatedClient(o.Client), content: httpcontent.IsolatedClient(o.ContentClient),
		clientID: o.ClientID, root: o.RootID, limit: o.MaxResponseBytes}
	var err error
	if d.root == "" {
		root := iterator{d: d}
		_, err = root.next(ctx)
		if err == io.EOF {
			err = nil
		}
	} else {
		var detail struct{ FileInfo item }
		err = d.control(ctx, "get_file_detail", url.Values{"fileId": {d.root}}, &detail)
		if err == nil {
			switch {
			case detail.FileInfo.ID != d.root || detail.FileInfo.Type != 1 && detail.FileInfo.Type != 2:
				err = core.Fail(core.InvalidUpstreamResponse, errors.New("Guangya Pan did not confirm the selected root"))
			case detail.FileInfo.Type == 1:
				err = core.Fail(core.NotDirectory, nil)
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

func (d *Driver) check(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if d.closed.Load() {
		return core.Fail(core.MountUnavailable, nil)
	}
	return nil
}

func (d *Driver) get(ctx context.Context, client *http.Client, target string, wanted *core.Range) (*http.Response, error) {
	if err := d.check(ctx); err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return nil, core.Fail(core.InvalidUpstreamResponse, nil)
	}
	req.Header.Set("Accept-Encoding", "identity")
	if client == &d.api {
		req.Header.Set("X-Client-Id", d.clientID)
		req.Header.Set("Accept", "application/json")
	}
	if wanted != nil {
		req.Header.Set("Range", httpcontent.Header(wanted))
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, errors.Join(httpcontent.UpstreamIOError(err), ctx.Err())
	}
	return resp, nil
}

func (d *Driver) control(ctx context.Context, route string, query url.Values, out any) (err error) {
	response, err := d.get(ctx, &d.api, endpoint+route+"?"+query.Encode(), nil)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, d.cleanup.CloseBody(response.Body), ctx.Err()) }()
	if response.StatusCode != http.StatusOK {
		return responseError(response.StatusCode)
	}
	data, err := httpcontent.ReadIdentity(response, d.limit)
	if err != nil {
		return err
	}
	var reply struct {
		Code *int
		Data json.RawMessage
	}
	if json.Unmarshal(data, &reply) != nil || reply.Code == nil {
		return core.Fail(core.InvalidUpstreamResponse, errors.New("invalid Guangya Pan API envelope"))
	}
	if *reply.Code != 0 {
		return nativeFailure(*reply.Code)
	}
	if string(reply.Data) == "null" || json.Unmarshal(reply.Data, out) != nil {
		return core.Fail(core.InvalidUpstreamResponse, errors.New("invalid Guangya Pan API result"))
	}
	return nil
}

// APIError carries the native business code without exposing request or reply details.
type APIError struct{ Code int }

func (e *APIError) Error() string { return fmt.Sprintf("Guangya Pan Open API code %d", e.Code) }

func nativeFailure(status int) error {
	code := core.UpstreamFailure
	switch status {
	case 111, 117, 120, 123, 167, 430:
		code = core.UpstreamAccessDenied
	case 146, 149:
		code = core.NotFound
	}
	return core.Fail(code, &APIError{Code: status})
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
	return core.Fail(code, fmt.Errorf("Guangya Pan returned HTTP %d", status))
}

// Close retries internal response cleanup. HTTP clients remain host-owned.
func (d *Driver) Close() error {
	d.closed.Store(true)
	return d.cleanup.Retry()
}

var _ core.Driver = (*Driver)(nil)
