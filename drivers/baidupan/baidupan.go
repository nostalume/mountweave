// Package baidupan connects the official Baidu Netdisk path API to Mountweave.
package baidupan

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"unicode/utf8"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/internal/upload"
)

// Options borrows an unauthenticated HTTP client. AccessToken is host-owned
// and must continue to identify the same account when its credential rotates.
type Options struct {
	RootPath         string
	AccessToken      func(context.Context) (string, error)
	Client           *http.Client
	MaxResponseBytes int64
	// MaxBufferedBytes enables stable-source multipart uploads. A nonzero
	// budget must hold at least one configured part.
	MaxBufferedBytes int64
	// PartSize is the multipart size in bytes. Zero selects 4 MiB. Hosts with
	// the corresponding native entitlements can select 16 MiB or 32 MiB.
	PartSize int64
}

type Driver struct {
	root        string
	token       func(context.Context) (string, error)
	client      http.Client
	limit       int64
	parts       *upload.Budget
	partSize    int64
	maxFileSize int64
	closed      atomic.Bool
	mu          sync.Mutex
	failed      []cleanup
}

type cleanup struct {
	body  io.Closer
	token string
}

// Open confirms an explicitly selected, authorized directory. It never infers
// an application directory or requests account-wide access. Close a non-nil
// result even after failure, since it can retain failed internal cleanup.
func Open(ctx context.Context, o Options) (*Driver, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if o.AccessToken == nil || o.Client == nil || o.MaxResponseBytes < 0 || o.MaxResponseBytes == math.MaxInt64 {
		return nil, core.Fail(core.InvalidRequest, nil)
	}
	if err := core.ValidatePath(o.RootPath); err != nil {
		return nil, err
	}
	if o.MaxResponseBytes == 0 {
		o.MaxResponseBytes = 4 << 20
	}
	partSize, maxFileSize := o.PartSize, int64(4<<30)
	switch partSize {
	case 0, partBytes:
		partSize = partBytes
	case 16 << 20:
		maxFileSize = 10 << 30
	case 32 << 20:
		maxFileSize = 20 << 30
	default:
		return nil, core.Fail(core.InvalidRequest, errors.New("multipart part size must be 4, 16, or 32 MiB"))
	}
	var parts *upload.Budget
	if o.MaxBufferedBytes != 0 {
		if err := upload.Validate(partSize, o.MaxBufferedBytes, 1); err != nil {
			return nil, core.Fail(core.InvalidRequest, err)
		}
		parts, _ = upload.New(o.MaxBufferedBytes, 1)
	}
	d := &Driver{root: o.RootPath, token: o.AccessToken, client: *o.Client, limit: o.MaxResponseBytes,
		parts: parts, partSize: partSize, maxFileSize: maxFileSize}
	d.client.Jar = nil
	d.client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	token, err := d.credential(ctx)
	if err == nil {
		_, err = d.metadata(ctx, d.root, token, false)
	}
	if err == nil {
		err = ctx.Err()
	}
	if err != nil {
		d.closed.Store(true)
	}
	return d, err
}

func (d *Driver) credential(ctx context.Context) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if d.closed.Load() {
		return "", core.Fail(core.MountUnavailable, nil)
	}
	token, err := d.token(ctx)
	if err != nil {
		return "", privateError(err, token)
	}
	if token == "" || !utf8.ValidString(token) {
		return "", core.Fail(core.InvalidRequest, errors.New("access token is empty or invalid UTF-8"))
	}
	return token, ctx.Err()
}

// do reports whether the request entered client.Do, not whether bytes reached
// the network. A mutating caller cannot infer no effect after that handoff.
func (d *Driver) do(req *http.Request, token string) (*http.Response, bool, error) {
	if err := req.Context().Err(); err != nil {
		return nil, false, err
	}
	if d.closed.Load() {
		return nil, false, core.Fail(core.MountUnavailable, nil)
	}
	req.Header.Set("User-Agent", "pan.baidu.com")
	req.Header.Set("Accept-Encoding", "identity")
	response, err := d.client.Do(req)
	if err != nil {
		err = privateError(err, token)
		if response != nil {
			err = errors.Join(err, d.closeBody(response.Body, token))
		}
		return nil, true, errors.Join(core.Fail(core.UpstreamFailure, err), req.Context().Err())
	}
	return response, true, nil
}

type reply struct {
	Errno     *int            `json:"errno"`
	Message   string          `json:"errmsg"`
	Show      string          `json:"show_msg"`
	RequestID json.RawMessage `json:"request_id"`
	Info      *[]nativeItem   `json:"info"`
	List      *[]nativeItem   `json:"list"`
	Data      *struct {
		Errno *int          `json:"errno"`
		List  *[]nativeItem `json:"list"`
	} `json:"data"`
}

func (d *Driver) call(ctx context.Context, argument url.Values, token string, post bool) (result reply, err error) {
	argument.Set("access_token", token)
	target := "https://pan.baidu.com/rest/2.0/xpan/file"
	method := http.MethodGet
	var body io.Reader
	if post {
		method, body = http.MethodPost, strings.NewReader(argument.Encode())
	} else {
		target += "?" + argument.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, method, target, body)
	if err != nil {
		return result, core.Fail(core.InvalidRequest, privateError(err, token))
	}
	if post {
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	}
	response, _, err := d.do(req, token)
	if err != nil {
		return result, err
	}
	defer func() { err = errors.Join(err, d.closeBody(response.Body, token)) }()
	data, err := d.readBody(response, token)
	if err != nil {
		return result, err
	}
	if response.StatusCode != http.StatusOK {
		return result, d.httpFailure(response, data, token)
	}
	if err := json.Unmarshal(data, &result); err != nil {
		return result, core.Fail(core.InvalidUpstreamResponse, privateError(err, token))
	}
	if result.Errno == nil {
		return result, core.Fail(core.InvalidUpstreamResponse, errors.New("native response omitted errno"))
	}
	if *result.Errno != 0 {
		return result, nativeFailure(*result.Errno, result, token)
	}
	if !post && result.Data != nil {
		if result.Data.Errno == nil {
			return result, core.Fail(core.InvalidUpstreamResponse, errors.New("native list envelope omitted errno"))
		}
		if *result.Data.Errno != 0 {
			return result, nativeFailure(*result.Data.Errno, result, token)
		}
		result.List = result.Data.List
	}
	return result, nil
}

func (d *Driver) readBody(response *http.Response, token string) ([]byte, error) {
	if coding := response.Header.Get("Content-Encoding"); coding != "" && coding != "identity" {
		return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("native response is not identity encoded"))
	}
	data, err := io.ReadAll(io.LimitReader(response.Body, d.limit+1))
	err = privateError(err, token)
	if int64(len(data)) > d.limit {
		return nil, errors.Join(core.Fail(core.ResponseBudgetExceeded, nil), err)
	}
	if err != nil {
		return nil, core.Fail(core.InvalidUpstreamResponse, err)
	}
	return data, nil
}

func nativeFailure(errno int, result reply, token string) error {
	code := core.UpstreamFailure
	switch errno {
	case -6, -7, 20011, 20013, 20016, 20017, 31045, 42213:
		code = core.UpstreamAccessDenied
	case -9, 31066:
		code = core.NotFound
	case 20012, 31034:
		code = core.UpstreamThrottled
	}
	return core.Fail(code, fmt.Errorf("Baidu returned errno %d (request %s): %s %s",
		errno, safeText(string(result.RequestID), token), safeText(result.Message, token), safeText(result.Show, token)))
}

func (d *Driver) httpFailure(response *http.Response, data []byte, token string) error {
	code := core.UpstreamFailure
	switch response.StatusCode {
	case 401, 403:
		code = core.UpstreamAccessDenied
	case 429, 503:
		code = core.UpstreamThrottled
	case 504:
		code = core.UpstreamTimeout
	}
	return core.Fail(code, fmt.Errorf("Baidu returned HTTP %d (retry-after %q): %s",
		response.StatusCode, safeText(response.Header.Get("Retry-After"), token), safeText(string(data), token)))
}

var urls = regexp.MustCompile(`https?://[^\s"'<>]+`)

func safeText(text, token string) string {
	if token != "" {
		text = strings.ReplaceAll(text, token, "[redacted]")
		text = strings.ReplaceAll(text, url.QueryEscape(token), "[redacted]")
	}
	return urls.ReplaceAllString(text, "[redacted URL]")
}

// Request URLs contain capabilities. Remove them from failed request/cleanup
// diagnostics, preserving independent causes and never treating failed EOF as
// normal Iterator termination. This work runs only on the error path.
func privateError(err error, token string) error {
	if err == nil {
		return nil
	}
	if failure, ok := err.(*url.Error); ok {
		return privateError(failure.Err, token)
	}
	if err == io.EOF {
		return io.ErrUnexpectedEOF
	}
	if failure, ok := err.(*core.Error); ok {
		copy := *failure
		copy.Cause = privateError(failure.Cause, token)
		return &copy
	}
	text := safeText(err.Error(), token)
	switch failure := err.(type) {
	case interface{ Unwrap() []error }:
		causes := []error{errors.New(text)}
		for _, cause := range failure.Unwrap() {
			causes = append(causes, privateError(cause, token))
		}
		return errors.Join(causes...)
	case interface{ Unwrap() error }:
		return errors.Join(errors.New(text), privateError(failure.Unwrap(), token))
	}
	if text == err.Error() {
		return err
	}
	return errors.New(text)
}

func (d *Driver) closeBody(body io.Closer, token string) error {
	if body == nil {
		return nil
	}
	err := privateError(body.Close(), token)
	if err != nil {
		d.mu.Lock()
		d.failed = append(d.failed, cleanup{body, token})
		d.mu.Unlock()
	}
	return err
}

// Close retries retained internal cleanup after Core has joined active uses.
// The borrowed client, transport and token provider are never closed.
func (d *Driver) Close() error {
	d.closed.Store(true)
	d.mu.Lock()
	pending := d.failed
	d.failed = nil
	d.mu.Unlock()
	var err error
	for _, failed := range pending {
		err = errors.Join(err, d.closeBody(failed.body, failed.token))
	}
	return err
}
