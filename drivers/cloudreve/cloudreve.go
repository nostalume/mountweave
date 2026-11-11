// Package cloudreve adapts a prepared Cloudreve V4 account connection to Mountweave.
package cloudreve

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/internal/upload"
)

// Options borrows prepared clients. The host owns authentication and transports;
// ContentClient receives only native content URLs and callback capabilities.
type Options struct {
	APIClient        *http.Client  `json:"-"`
	ContentClient    *http.Client  `json:"-"`
	Endpoint         string        `json:"endpoint"`
	RootURI          string        `json:"root_uri"`
	MaxBufferedBytes int64         `json:"max_buffered_bytes,omitempty"`
	MaxResponseBytes int64         `json:"max_response_bytes,omitempty"`
	RangeAttempts    int           `json:"range_attempts,omitempty"`
	CleanupTimeout   time.Duration `json:"cleanup_timeout,omitempty"`
}

// Validate checks static settings without using clients or acquiring resources.
func (o Options) Validate() error {
	u, err := url.Parse(o.Endpoint)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" ||
		u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || u.Opaque != "" ||
		(u.Path != "" && u.Path != "/") {
		return core.Fail(core.InvalidRequest, errors.New("an HTTP or HTTPS API origin is required"))
	}
	if _, err := parseURI(o.RootURI); err != nil {
		return core.Fail(core.InvalidRequest, err)
	}
	if o.MaxBufferedBytes < 0 || o.MaxResponseBytes < 0 || o.RangeAttempts < 0 || o.CleanupTimeout < 0 {
		return core.Fail(core.InvalidRequest, nil)
	}
	return nil
}

type Driver struct {
	opts         Options
	api, content *http.Client
	buffer       *upload.Budget
	closed       atomic.Bool
	nextUpload   atomic.Uint64
	mu           sync.Mutex
	failedBodies []io.Closer
	pending      map[uint64]pendingUpload
}

type item struct {
	Type     *int              `json:"type"`
	ID       string            `json:"id"`
	Name     string            `json:"name"`
	Path     string            `json:"path"`
	Size     *int64            `json:"size"`
	Modified *time.Time        `json:"updated_at"`
	Metadata map[string]string `json:"metadata"`
}

func (f item) metadata() (core.Metadata, error) {
	if _, redirect := f.Metadata["sys:shared_redirect"]; redirect {
		return core.Metadata{}, core.Fail(core.UnsupportedOperation, errors.New("shared redirects are not ordinary tree entries"))
	}
	if f.ID == "" || f.Type == nil || (*f.Type != 0 && *f.Type != 1) || (f.Size != nil && *f.Size < 0) {
		return core.Metadata{}, core.Fail(core.InvalidUpstreamResponse, nil)
	}
	meta := core.Metadata{Name: f.Name, Kind: core.Directory, ModifiedAt: f.Modified}
	if *f.Type == 0 {
		meta.Kind, meta.Size = core.File, f.Size
	}
	return meta, nil
}

// Open validates the actual root directory. RootURI remains a native path,
// not a fixed object ID. A non-nil failed Driver owns response cleanup only.
func Open(ctx context.Context, opts Options) (*Driver, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := opts.Validate(); err != nil {
		return nil, err
	}
	if opts.APIClient == nil || opts.ContentClient == nil {
		return nil, errors.New("prepared API and content clients are required")
	}
	opts.Endpoint = strings.TrimSuffix(opts.Endpoint, "/")
	root, _ := parseURI(opts.RootURI)
	opts.RootURI = root.String()
	if opts.RangeAttempts == 0 {
		opts.RangeAttempts = 1
	}
	d := &Driver{opts: opts, api: isolatedClient(opts.APIClient), content: isolatedClient(opts.ContentClient), pending: make(map[uint64]pendingUpload)}
	if opts.MaxBufferedBytes != 0 {
		d.buffer, _ = upload.New(opts.MaxBufferedBytes, 1)
	}
	f, err := d.resolve(ctx, "/")
	if err == nil && *f.Type != 1 {
		err = core.Fail(core.NotDirectory, nil)
	}
	if err == nil {
		err = ctx.Err()
	}
	if err != nil {
		d.closed.Store(true)
		return d, err
	}
	return d, nil
}

func isolatedClient(client *http.Client) *http.Client {
	copy := *client
	copy.Jar = nil
	copy.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &copy
}

// parseURI admits only an ordinary account tree, never a search or share view.
func parseURI(value string) (*url.URL, error) {
	u, err := url.Parse(value)
	if err != nil || u.Scheme != "cloudreve" || u.Host != "my" || u.User != nil || u.Opaque != "" ||
		u.RawQuery != "" || u.ForceQuery || u.Fragment != "" {
		return nil, errors.New("an ordinary cloudreve://my URI is required")
	}
	p := u.Path
	if p == "" || p == "/" {
		u.Path, u.RawPath = "", ""
		return u, nil
	}
	if err := core.ValidatePath(p); err != nil {
		return nil, err
	}
	return u, nil
}

func (d *Driver) apiURL(route string) string { return d.opts.Endpoint + "/api/v4" + route }

func (d *Driver) apiRequest(ctx context.Context, method, target string, body io.Reader, contentType string, length int64) (*http.Response, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if d.closed.Load() {
		return nil, core.Fail(core.MountUnavailable, nil)
	}
	u, err := url.Parse(target)
	origin, _ := url.Parse(d.opts.Endpoint)
	if err != nil || u.Scheme != origin.Scheme || !strings.EqualFold(u.Host, origin.Host) ||
		u.User != nil || u.Fragment != "" || !strings.HasPrefix(u.Path, "/api/v4/") {
		return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("invalid Cloudreve API target"))
	}
	req, err := http.NewRequestWithContext(ctx, method, target, body)
	if err != nil {
		return nil, core.Fail(core.InvalidRequest, err)
	}
	req.Header.Set("Accept", "application/json")
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	req.ContentLength = length
	if length == 0 {
		req.Body = http.NoBody
	}
	response, err := d.api.Do(req)
	if err != nil {
		return nil, networkError(err)
	}
	return response, nil
}

func validContentURL(target string) error {
	u, err := url.Parse(target)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil || u.Fragment != "" || u.Opaque != "" {
		return core.Fail(core.InvalidUpstreamResponse, errors.New("invalid content target"))
	}
	return nil
}

// decode returns true only for a successful native envelope and decoded data.
// Response cleanup can still fail after that receipt has been established.
func (d *Driver) decode(response *http.Response, out any) (decoded bool, err error) {
	defer func() { err = errors.Join(err, d.closeBody(response.Body)) }()
	var native struct {
		Code       *int            `json:"code"`
		Msg        string          `json:"msg"`
		Error      string          `json:"error"`
		Aggregated json.RawMessage `json:"aggregated_error"`
		Data       json.RawMessage `json:"data"`
	}
	var body io.Reader = response.Body
	var limited *io.LimitedReader
	if max := d.opts.MaxResponseBytes; max > 0 && max < math.MaxInt64 {
		limited = &io.LimitedReader{R: body, N: max + 1}
		body = limited
	}
	decoder := json.NewDecoder(body)
	err = decoder.Decode(&native)
	if err == nil {
		var extra any
		if tail := decoder.Decode(&extra); tail != io.EOF {
			err = errors.Join(errors.New("unexpected trailing Cloudreve response data"), tail)
		}
	}
	if limited != nil && limited.N == 0 {
		return false, core.Fail(core.ResponseBudgetExceeded, err)
	}
	if err != nil || native.Code == nil {
		return false, errors.Join(core.Fail(core.InvalidUpstreamResponse, err), httpFailure(response.StatusCode))
	}
	if *native.Code != 0 {
		return false, core.Fail(nativeCode(*native.Code), fmt.Errorf("Cloudreve %d: %s: %s %s", *native.Code, native.Msg, native.Error, native.Aggregated))
	}
	if err := httpFailure(response.StatusCode); err != nil {
		return false, err
	}
	if out != nil {
		if len(native.Data) == 0 || string(native.Data) == "null" {
			return false, core.Fail(core.InvalidUpstreamResponse, errors.New("missing Cloudreve response data"))
		}
		if err := json.Unmarshal(native.Data, out); err != nil {
			return false, core.Fail(core.InvalidUpstreamResponse, err)
		}
	}
	return true, nil
}

func nativeCode(code int) core.Code {
	switch code {
	case 401, 403, 40005, 40007, 40008, 40017, 40018, 40020:
		return core.UpstreamAccessDenied
	case 404, 40011, 40016, 40044:
		return core.NotFound
	case 40004:
		return core.AlreadyExists
	case 409, 412:
		return core.PreconditionFailed
	case 40006, 40019:
		return core.UnsupportedOperation
	case 408, 504:
		return core.UpstreamTimeout
	case 429:
		return core.UpstreamThrottled
	default:
		return core.UpstreamFailure
	}
}

func httpFailure(status int) error {
	if status >= 200 && status < 300 {
		return nil
	}
	return core.Fail(nativeCode(status), fmt.Errorf("Cloudreve HTTP status %d", status))
}

func networkError(err error) error {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	var timeout net.Error
	if errors.As(err, &timeout) && timeout.Timeout() {
		return core.Fail(core.UpstreamTimeout, err)
	}
	return core.Fail(core.UpstreamFailure, err)
}
