// Package feishudrive connects a Feishu Drive folder to Mountweave.
package feishudrive

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"math"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"unicode/utf8"

	core "github.com/nostalume/mountweave"
)

const endpoint = "https://open.feishu.cn/open-apis/drive/v1"

// Options selects an authorized folder token and borrows a client that
// authenticates requests to the Feishu API for one user or tenant.
type Options struct {
	RootToken        string
	Client           *http.Client
	MaxResponseBytes int64
	MaxResolvePages  int
	// ExportFormats selects one whole-document format per native document type.
	ExportFormats map[string]string
	// AllowRemove enables moving an uploaded file to the native recycle bin.
	AllowRemove bool
}

type Driver struct {
	root         string
	client       http.Client
	limit        int64
	resolvePages int
	exports      map[string]string
	allowRemove  bool
	closed       atomic.Bool
	mu           sync.Mutex
	failed       []io.Closer
}

// Open verifies the selected folder. A non-nil failed result still owns
// response cleanup and must be closed by its creator.
func Open(ctx context.Context, options Options) (*Driver, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if options.Client == nil || !validToken(options.RootToken) || options.MaxResponseBytes < 0 ||
		options.MaxResponseBytes == math.MaxInt64 || options.MaxResolvePages < 0 {
		return nil, core.Fail(core.InvalidRequest, nil)
	}
	if options.MaxResponseBytes == 0 {
		options.MaxResponseBytes = 4 << 20
	}
	exports := maps.Clone(options.ExportFormats)
	for source, format := range exports {
		if !supportsExport(source, format) {
			return nil, core.Fail(core.InvalidRequest, errors.New("unsupported Feishu document export format"))
		}
	}
	d := &Driver{root: options.RootToken, client: *options.Client, limit: options.MaxResponseBytes,
		resolvePages: options.MaxResolvePages, exports: exports, allowRemove: options.AllowRemove}
	d.client.Jar = nil
	d.client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	_, err := d.page(ctx, d.root, "", 1)
	if err != nil {
		d.closed.Store(true)
	}
	return d, err
}

func validToken(s string) bool {
	return s != "" && s != "." && s != ".." && utf8.ValidString(s) && !strings.ContainsAny(s, "/\\")
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

// request reports whether client.Do was entered, even when no response arrives.
func (d *Driver) request(ctx context.Context, method, route, byteRange string, body io.Reader) (*http.Response, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	if d.closed.Load() {
		return nil, false, core.Fail(core.MountUnavailable, nil)
	}
	req, err := http.NewRequestWithContext(ctx, method, endpoint+route, body)
	if err != nil {
		return nil, false, core.Fail(core.InvalidRequest, nil)
	}
	req.Header.Set("Accept-Encoding", "identity")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
		req.GetBody = nil
	}
	if byteRange != "" {
		req.Header.Set("Range", byteRange)
	}
	resp, err := d.client.Do(req)
	if err != nil {
		var request *url.Error
		if errors.As(err, &request) {
			err = request.Err
		}
		err = nativeIO(err)
		if resp != nil {
			err = errors.Join(err, d.closeBody(resp.Body))
		}
		return nil, true, errors.Join(core.Fail(core.UpstreamFailure, err), ctx.Err())
	}
	return resp, true, nil
}

// A transport or cleanup EOF must not end a native directory iterator.
func nativeIO(err error) error {
	if errors.Is(err, io.EOF) {
		return fmt.Errorf("native I/O failed: %v: %w", err, io.ErrUnexpectedEOF)
	}
	return err
}

// reply reports a successful native envelope independently of response cleanup.
// Its data may be absent for operations that have no response fields.
func (d *Driver) reply(resp *http.Response) (data *json.RawMessage, nativeCode int64, err error) {
	defer func() { err = errors.Join(err, d.closeBody(resp.Body)) }()
	if encoding := resp.Header.Get("Content-Encoding"); encoding != "" && encoding != "identity" {
		return nil, 0, core.Fail(core.InvalidUpstreamResponse, nil)
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, d.limit+1))
	if int64(len(raw)) > d.limit {
		return nil, 0, core.Fail(core.ResponseBudgetExceeded, nativeIO(err))
	}
	if err != nil {
		return nil, 0, core.Fail(core.UpstreamFailure, nativeIO(err))
	}
	var reply struct {
		Code *int64          `json:"code"`
		Msg  string          `json:"msg"`
		Data json.RawMessage `json:"data"`
	}
	decode := json.Unmarshal(raw, &reply)
	if decode == nil && reply.Code != nil {
		nativeCode = *reply.Code
	}
	if resp.StatusCode != http.StatusOK || nativeCode != 0 {
		code := core.UpstreamFailure
		switch resp.StatusCode {
		case http.StatusUnauthorized, http.StatusForbidden:
			code = core.UpstreamAccessDenied
		case http.StatusNotFound:
			code = core.NotFound
		case http.StatusTooManyRequests:
			code = core.UpstreamThrottled
		}
		if reply.Code == nil {
			return nil, nativeCode, core.Fail(code, fmt.Errorf("Feishu returned HTTP %d", resp.StatusCode))
		}
		return nil, nativeCode, core.Fail(code, fmt.Errorf("Feishu returned HTTP %d, code %d: %s", resp.StatusCode, *reply.Code, reply.Msg))
	}
	if decode != nil || reply.Code == nil {
		return nil, 0, core.Fail(core.InvalidUpstreamResponse, decode)
	}
	return &reply.Data, 0, nil
}

func (d *Driver) envelope(resp *http.Response, out any) error {
	data, _, err := d.reply(resp)
	if data == nil {
		return err
	}
	if len(*data) == 0 || string(*data) == "null" || out == nil {
		return errors.Join(core.Fail(core.InvalidUpstreamResponse, nil), err)
	}
	if decode := json.Unmarshal(*data, out); decode != nil {
		return errors.Join(core.Fail(core.InvalidUpstreamResponse, decode), err)
	}
	return err
}

func (d *Driver) closeBody(body io.Closer) error {
	if err := nativeIO(body.Close()); err != nil {
		d.mu.Lock()
		d.failed = append(d.failed, body)
		d.mu.Unlock()
		return err
	}
	return nil
}

// Close retries failed response cleanup. The client and transport stay borrowed.
func (d *Driver) Close() error {
	d.closed.Store(true)
	d.mu.Lock()
	bodies := d.failed
	d.failed = nil
	d.mu.Unlock()
	var err error
	for _, body := range bodies {
		err = errors.Join(err, d.closeBody(body))
	}
	return err
}

var _ core.Driver = (*Driver)(nil)
