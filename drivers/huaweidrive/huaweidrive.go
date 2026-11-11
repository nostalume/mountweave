// Package huaweidrive mounts personal HUAWEI Drive through HMS Drive Kit.
package huaweidrive

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"strconv"
	"sync/atomic"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/internal/httpcontent"
)

const filesURL = "https://driveapis.cloud.huawei.com.cn/drive/v1/files"

type Options struct {
	// Client adds the approved user's Drive OAuth credentials. It is borrowed.
	Client *http.Client
	// ContentClient is optional. Its transport must omit account credentials.
	ContentClient    *http.Client
	MaxResponseBytes int64
}

type Driver struct {
	api, content http.Client
	rootParent   string
	limit        int64
	closed       atomic.Bool
	cleanup      httpcontent.ResponseCleanup
}

// Open verifies access to the authorized user's personal root.
// Close a non-nil failed result to retry any retained response cleanup.
func Open(ctx context.Context, o Options) (*Driver, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if o.Client == nil || o.Client == o.ContentClient || o.MaxResponseBytes < 0 || o.MaxResponseBytes == math.MaxInt64 {
		return nil, core.Fail(core.InvalidRequest, nil)
	}
	if o.MaxResponseBytes == 0 {
		o.MaxResponseBytes = 4 << 20
	}
	d := &Driver{api: httpcontent.IsolatedClient(o.Client), content: httpcontent.IsolatedClient(o.ContentClient), limit: o.MaxResponseBytes}
	probe := listing{driver: d, parent: rootAlias, pageSize: 1}
	_, err := probe.next(ctx)
	if err == nil || err == io.EOF {
		d.rootParent, err = probe.rootParent, ctx.Err()
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

func (d *Driver) get(ctx context.Context, client *http.Client, target string, accept string, wanted *core.Range) (*http.Response, error) {
	if err := d.check(ctx); err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return nil, core.Fail(core.InvalidUpstreamResponse, nil)
	}
	req.Header.Set("Accept-Encoding", "identity")
	if accept != "" {
		req.Header.Set("Accept", accept)
	}
	if wanted != nil {
		req.Header.Set("Range", httpcontent.Header(wanted))
	}
	response, err := client.Do(req)
	if err != nil {
		return nil, errors.Join(httpcontent.UpstreamIOError(err), ctx.Err())
	}
	return response, nil
}

func (d *Driver) getJSON(ctx context.Context, target string, out any) (err error) {
	response, err := d.get(ctx, &d.api, target, "application/json", nil)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, d.cleanup.CloseBody(response.Body), ctx.Err()) }()
	if response.StatusCode != http.StatusOK {
		return d.statusError(response, true)
	}
	data, err := httpcontent.ReadIdentity(response, d.limit)
	if err != nil {
		return err
	}
	if json.Unmarshal(data, out) != nil {
		return core.Fail(core.InvalidUpstreamResponse, errors.New("invalid HUAWEI Drive JSON response"))
	}
	return nil
}

// APIError contains only HTTP and native numeric result codes.
type APIError struct{ Status, Code int }

func (e *APIError) Error() string {
	if e.Code == 0 {
		return fmt.Sprintf("HUAWEI Drive HTTP %d", e.Status)
	}
	return fmt.Sprintf("HUAWEI Drive HTTP %d (code %d)", e.Status, e.Code)
}

func (d *Driver) statusError(response *http.Response, native bool) error {
	detail := &APIError{Status: response.StatusCode}
	if native {
		var reply struct {
			Error struct{ ErrorDetail []struct{ ErrorCode string } }
		}
		if httpcontent.DecodeJSON(response, d.limit, &reply) == nil && len(reply.Error.ErrorDetail) != 0 {
			if code, err := strconv.Atoi(reply.Error.ErrorDetail[0].ErrorCode); err == nil {
				detail.Code = code
			}
		}
	}
	code := core.UpstreamFailure
	switch detail.Status {
	case http.StatusUnauthorized, http.StatusForbidden:
		code = core.UpstreamAccessDenied
	case http.StatusNotFound:
		code = core.NotFound
	case http.StatusTooManyRequests:
		code = core.UpstreamThrottled
	case http.StatusGatewayTimeout, http.StatusRequestTimeout:
		code = core.UpstreamTimeout
	}
	if detail.Code == 21004009 {
		code = core.NotFound
	}
	return core.Fail(code, detail)
}

// Close retries response cleanup. HTTP clients remain host-owned.
func (d *Driver) Close() error {
	d.closed.Store(true)
	return d.cleanup.Retry()
}

var _ core.Driver = (*Driver)(nil)
