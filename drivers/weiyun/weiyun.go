// Package weiyun mounts a Tencent Weiyun directory within a personal MCP token's scope.
package weiyun

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/internal/httpcontent"
	"github.com/nostalume/mountweave/drivers/internal/mcptool"
)

// Origin hosts Weiyun's consent page and personal drive service.
const Origin = "https://www.weiyun.com"

const endpoint = Origin + "/api/v3/mcpserver"

// Authorization errors distinguish a timed-out attempt from a denied one.
var (
	ErrAuthorizationExpired  = errors.New("Weiyun authorization timed out")
	ErrAuthorizationRejected = errors.New("Weiyun rejected the authorization")
)

type Options struct {
	Token string `json:"-"` // Personal MCP token from the Weiyun authorization page.
	// RootPath is absolute within the token's scope. Empty or "/" uses the token's root.
	RootPath string
	// Clients are borrowed. Their transports must not add account credentials.
	Client, ContentClient *http.Client
	MaxResponseBytes      int64
}

// Authorize presents a consent URL and waits for a personal MCP token.
// presentURL must honor the context and show the URL only to the initiating user.
// readResponse must honor the request context, refuse redirects, bound JSON
// decoding, and close the response body. It is trusted with the token response.
func Authorize(ctx context.Context, readResponse func(*http.Request, any) (int, error), presentURL func(context.Context, string) error) (token string, err error) {
	if readResponse == nil || presentURL == nil {
		return "", core.Fail(core.InvalidRequest, nil)
	}
	ctx, cancel := context.WithTimeoutCause(ctx, 5*time.Minute, ErrAuthorizationExpired)
	defer func() {
		if cause := context.Cause(ctx); cause != nil {
			token, err = "", cause
		}
		cancel()
	}()
	if err := ctx.Err(); err != nil {
		return "", err
	}
	var nonce [8]byte
	rand.Read(nonce[:])
	code := hex.EncodeToString(nonce[:])
	if err := presentURL(ctx, Origin+"/authorize/token?code="+code); err != nil {
		return "", err
	}
	for ctx.Err() == nil {
		req, _ := http.NewRequestWithContext(ctx, http.MethodPost, Origin+"/api/v3/mcp/token/code", strings.NewReader("{\"code\":\""+code+"\"}"))
		req.Header.Set("Content-Type", "application/json")
		var reply struct {
			Token string      `json:"token"`
			Code  json.Number `json:"code"`
		}
		status, err := readResponse(req, &reply)
		if err != nil {
			return "", err
		}
		switch {
		case status == http.StatusTooManyRequests:
			return "", core.Fail(core.UpstreamThrottled, nil)
		case status == http.StatusOK && (reply.Code == "" || reply.Code == "0"):
			if !validToken(reply.Token) {
				return "", core.Fail(core.InvalidUpstreamResponse, nil)
			}
			return reply.Token, nil
		case reply.Code == "11510" && (status == http.StatusOK || status == http.StatusBadRequest):
			// Waiting for consent returns the same code as an expired session.
			select {
			case <-ctx.Done():
				return "", ctx.Err()
			case <-time.After(2 * time.Second):
			}
		case reply.Code == "117402" || status == http.StatusUnauthorized || status == http.StatusForbidden:
			return "", ErrAuthorizationRejected
		default:
			return "", core.Fail(core.UpstreamFailure, nil)
		}
	}
	return "", ctx.Err()
}

func validToken(token string) bool {
	return token != "" && !strings.ContainsFunc(token, func(r rune) bool { return r < '!' || r > '~' || r == ';' })
}

type Driver struct {
	api, content http.Client
	token        string
	tokenRoot    string
	root         directory
	limit        int64
	requestID    atomic.Uint64
	closed       atomic.Bool
	cleanup      httpcontent.ResponseCleanup
}

// Open verifies the selected root. Close a non-nil failed result to retry cleanup.
func Open(ctx context.Context, o Options) (*Driver, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !validToken(o.Token) || o.MaxResponseBytes < 0 || o.MaxResponseBytes == math.MaxInt64 {
		return nil, core.Fail(core.InvalidRequest, nil)
	}
	if o.RootPath != "" {
		if err := core.ValidatePath(o.RootPath); err != nil {
			return nil, err
		}
	}
	if o.MaxResponseBytes == 0 {
		o.MaxResponseBytes = 4 << 20
	}
	d := &Driver{api: httpcontent.IsolatedClient(o.Client), content: httpcontent.IsolatedClient(o.ContentClient),
		token: o.Token, limit: o.MaxResponseBytes}
	var err error
	if o.RootPath != "" && o.RootPath != "/" {
		folder, parent, resolveErr := d.resolve(ctx, core.NativePath(o.RootPath))
		if resolveErr != nil {
			err = resolveErr
		} else if folder.m.Kind != core.Directory {
			err = core.Fail(core.NotDirectory, nil)
		} else {
			d.root = directory{folder.id, parent}
		}
	}
	if err == nil {
		root := iterator{d: d, dir: d.root}
		_, err = root.next(ctx)
		if err == io.EOF || err == nil {
			if d.root.Key == "" {
				d.tokenRoot = root.observedKey
			}
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

type clientHeader struct {
	Qua     string `json:"qua"`
	Version string `json:"version"`
}

var caller = clientHeader{Qua: "GO_1.24.0_MOUNTWEAVE_0.1.0", Version: "1.0.6"}

// The published stateless caller POSTs tools/call and decodes JSON inside MCP text content.
func (d *Driver) call(ctx context.Context, tool string, arguments any, out interface{ failure(bool) error }) (err error) {
	if err := d.check(ctx); err != nil {
		return err
	}
	id := d.requestID.Add(1)
	payload, err := mcptool.MarshalCall(id, tool, arguments)
	if err != nil {
		return core.Fail(core.InvalidRequest, nil)
	}
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(payload))
	req.Header.Set("WyHeader", "mcp_token="+d.token)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Accept-Encoding", "identity")
	response, err := d.api.Do(req)
	if err != nil {
		return errors.Join(httpcontent.UpstreamIOError(err), ctx.Err())
	}
	var isToolError bool
	defer func() {
		err = errors.Join(err, d.cleanup.CloseBody(response.Body), ctx.Err())
		if err == nil {
			err = out.failure(isToolError)
		}
	}()
	if response.StatusCode != http.StatusOK {
		return responseError(response.StatusCode)
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
		return core.Fail(core.UpstreamFailure, &APIError{Code: *reply.ErrorCode})
	}
	if strings.HasPrefix(strings.TrimSpace(reply.Text), "{") && json.Unmarshal([]byte(reply.Text), out) == nil {
		isToolError = reply.IsError
		return nil
	}
	if reply.IsError {
		return core.Fail(core.UpstreamFailure, errors.New("Weiyun MCP tool failed"))
	}
	return core.Fail(core.InvalidUpstreamResponse, errors.New("invalid Weiyun MCP tool result"))
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
