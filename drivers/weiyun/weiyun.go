// Package weiyun mounts the Tencent Weiyun directory bound to a personal MCP token.
package weiyun

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

const endpoint = "https://www.weiyun.com/api/v3/mcpserver"

type Options struct {
	Token string `json:"-"` // Personal MCP token from the Weiyun authorization page.
	// Clients are borrowed. Their transports must not add account credentials.
	Client, ContentClient *http.Client
	MaxResponseBytes      int64
}

type Driver struct {
	api, content http.Client
	token, root  string
	limit        int64
	requestID    atomic.Uint64
	closed       atomic.Bool
	cleanup      httpcontent.ResponseCleanup
}

// Open verifies the token-bound root. Close a non-nil failed result to retry cleanup.
func Open(ctx context.Context, o Options) (*Driver, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if o.Token == "" || strings.ContainsFunc(o.Token, func(r rune) bool { return r < '!' || r > '~' || r == ';' }) ||
		o.MaxResponseBytes < 0 || o.MaxResponseBytes == math.MaxInt64 {
		return nil, core.Fail(core.InvalidRequest, nil)
	}
	if o.MaxResponseBytes == 0 {
		o.MaxResponseBytes = 4 << 20
	}
	d := &Driver{api: httpcontent.IsolatedClient(o.Client), content: httpcontent.IsolatedClient(o.ContentClient),
		token: o.Token, limit: o.MaxResponseBytes}
	root := iterator{d: d}
	_, err := root.next(ctx)
	if err == io.EOF || err == nil {
		d.root, err = root.observedKey, ctx.Err()
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

type clientHeader struct {
	Qua     string `json:"qua"`
	Version string `json:"version"`
}

var caller = clientHeader{Qua: "GO_1.24.0_MOUNTWEAVE_0.1.0", Version: "1.0.6"}

// The published stateless caller POSTs tools/call and decodes JSON inside MCP text content.
func (d *Driver) call(ctx context.Context, tool string, arguments any, out any) (isError bool, err error) {
	if err := d.check(ctx); err != nil {
		return false, err
	}
	id := d.requestID.Add(1)
	payload, err := mcptool.MarshalCall(id, tool, arguments)
	if err != nil {
		return false, core.Fail(core.InvalidRequest, nil)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(payload))
	if err != nil {
		return false, core.Fail(core.InvalidRequest, nil)
	}
	req.Header.Set("WyHeader", "mcp_token="+d.token)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Accept-Encoding", "identity")
	response, err := d.api.Do(req)
	if err != nil {
		return false, errors.Join(httpcontent.UpstreamIOError(err), ctx.Err())
	}
	defer func() { err = errors.Join(err, d.cleanup.CloseBody(response.Body), ctx.Err()) }()
	if response.StatusCode != http.StatusOK {
		return false, responseError(response.StatusCode)
	}
	data, err := httpcontent.ReadIdentity(response, d.limit)
	if err != nil {
		return false, err
	}
	reply, err := mcptool.DecodeReply(data, id)
	if err != nil {
		return false, err
	}
	if reply.ErrorCode != nil {
		return false, core.Fail(core.UpstreamFailure, &APIError{Code: *reply.ErrorCode})
	}
	if strings.HasPrefix(strings.TrimSpace(reply.Text), "{") && json.Unmarshal([]byte(reply.Text), out) == nil {
		return reply.IsError, nil
	}
	if reply.IsError {
		return false, core.Fail(core.UpstreamFailure, errors.New("Weiyun MCP tool failed"))
	}
	return false, core.Fail(core.InvalidUpstreamResponse, errors.New("invalid Weiyun MCP tool result"))
}

type nativeResult struct {
	Retcode int
	Error   string
}

func (r nativeResult) failure(isError bool) error {
	if r.Retcode == 0 {
		if r.Error == "" && !isError {
			return nil
		}
		return core.Fail(core.UpstreamFailure, errors.New("Weiyun MCP tool failed"))
	}
	code := core.UpstreamFailure
	switch r.Retcode {
	case 117401:
		code = core.UpstreamThrottled
	case 117402, 117405, 117407:
		code = core.UpstreamAccessDenied
	}
	return core.Fail(code, &APIError{Code: r.Retcode})
}

// APIError carries a native code without exposing the response text or credentials.
type APIError struct{ Code int }

func (e *APIError) Error() string { return fmt.Sprintf("Weiyun MCP code %d", e.Code) }

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
	return core.Fail(code, fmt.Errorf("Weiyun returned HTTP %d", status))
}

// Close retries response cleanup. HTTP clients remain host-owned.
func (d *Driver) Close() error {
	d.closed.Store(true)
	return d.cleanup.Retry()
}

var _ core.Driver = (*Driver)(nil)
