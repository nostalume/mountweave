// Package yunpan360 mounts a personal 360 AI Cloud Disk account.
package yunpan360

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"strings"
	"sync/atomic"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/internal/httpcontent"
	"github.com/nostalume/mountweave/drivers/internal/mcptool"
)

const endpoint = "https://mcp.yunpan.com/mcp"

type Options struct {
	APIKey string `json:"-"` // Personal API key issued by 360 AI Cloud Disk.
	// Both clients are borrowed. Their transports must omit account credentials.
	Client, ContentClient *http.Client
	MaxResponseBytes      int64
}

type Driver struct {
	api, content http.Client
	key          string
	limit        int64
	requestID    atomic.Uint64
	closed       atomic.Bool
	cleanup      httpcontent.ResponseCleanup
}

// Open checks the key's access to the personal root. Close a non-nil failed
// result to retry any retained response cleanup.
func Open(ctx context.Context, o Options) (*Driver, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if o.APIKey == "" || strings.ContainsFunc(o.APIKey, func(c rune) bool { return c < '!' || c > '~' }) ||
		o.MaxResponseBytes < 0 || o.MaxResponseBytes == math.MaxInt64 {
		return nil, core.Fail(core.InvalidRequest, nil)
	}
	if o.MaxResponseBytes == 0 {
		o.MaxResponseBytes = 4 << 20
	}
	d := &Driver{api: httpcontent.IsolatedClient(o.Client), content: httpcontent.IsolatedClient(o.ContentClient), key: o.APIKey, limit: o.MaxResponseBytes}
	probe := listing{driver: d, path: "/"}
	_, err := probe.next(ctx)
	if err == nil || err == io.EOF {
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

func (d *Driver) request(client *http.Client, req *http.Request) (*http.Response, error) {
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

func (d *Driver) call(ctx context.Context, name string, args any, out any) (err error) {
	id := d.requestID.Add(1)
	payload, err := mcptool.MarshalCall(id, name, args)
	if err != nil {
		return core.Fail(core.InvalidRequest, nil)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(payload))
	if err != nil {
		return core.Fail(core.InvalidRequest, nil)
	}
	req.Header.Set("X-API-Key", d.key)
	req.Header.Set("X-ECS-Env", "prod")
	req.Header.Set("X-Sub-Channel", "open")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")
	response, err := d.request(&d.api, req)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, d.cleanup.CloseBody(response.Body), ctx.Err()) }()
	if response.StatusCode != http.StatusOK {
		return statusError(response.StatusCode)
	}
	data, err := httpcontent.ReadIdentity(response, d.limit)
	if err != nil {
		return err
	}
	reply, err := mcptool.DecodeReply(data, id)
	if err != nil {
		return err
	}
	if reply.ErrorCode != nil {
		return core.Fail(core.UpstreamFailure, fmt.Errorf("360 Cloud Disk MCP code %d", *reply.ErrorCode))
	}
	var native struct {
		Errno *int
		Data  json.RawMessage
	}
	if json.Unmarshal([]byte(reply.Text), &native) != nil {
		return core.Fail(core.UpstreamFailure, errors.New("360 Cloud Disk tool returned no structured result"))
	}
	if native.Errno != nil && *native.Errno != 0 {
		code := core.UpstreamFailure
		switch *native.Errno {
		case 1001, 1002, 1003, 3001, 3002, 3003, 3005, 3006:
			code = core.UpstreamAccessDenied
		case 3008:
			code = core.NotFound
		}
		return core.Fail(code, fmt.Errorf("360 Cloud Disk result code %d", *native.Errno))
	}
	if reply.IsError {
		return core.Fail(core.UpstreamFailure, errors.New("360 Cloud Disk tool failed"))
	}
	if name == "get-download-url" && native.Errno == nil {
		return core.Fail(core.InvalidUpstreamResponse, errors.New("360 Cloud Disk did not confirm the download grant"))
	}
	if !bytes.HasPrefix(bytes.TrimSpace(native.Data), []byte("{")) || json.Unmarshal(native.Data, out) != nil {
		return core.Fail(core.InvalidUpstreamResponse, errors.New("invalid 360 Cloud Disk tool data"))
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
	case http.StatusRequestTimeout, http.StatusGatewayTimeout:
		code = core.UpstreamTimeout
	}
	return core.Fail(code, fmt.Errorf("360 Cloud Disk returned HTTP %d", status))
}

// Close retries retained response cleanup. HTTP clients remain host-owned.
func (d *Driver) Close() error {
	d.closed.Store(true)
	return d.cleanup.Retry()
}

var _ core.Driver = (*Driver)(nil)
