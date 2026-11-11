// Package yandex connects native Yandex Disk paths to Mountweave.
package yandex

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/internal/httpcontent"
)

const Endpoint = "https://cloud-api.yandex.net/v1/disk"

// Options selects disk:/ or app:/ and an actual directory. Clients and their
// transports remain borrowed. Token is sent only to the control endpoint.
// A supplied ContentClient must not inject account authentication. Its default
// is independent of Client, not a copy of an authenticated control transport.
type Options struct {
	Endpoint, RootPath, Token string
	Client, ContentClient     *http.Client
	MaxResponseBytes          int64
}

type Driver struct {
	endpoint               *url.URL
	root, canonical, token string
	api, content           http.Client
	limit                  int64
	closed                 atomic.Bool
	mu                     sync.Mutex
	failed                 []io.Closer
	protected              httpcontent.ResponseCleanup
}

// Open validates options and confirms the directory root. A non-nil failed
// result retains response cleanup and must be closed by its creator.
func Open(ctx context.Context, o Options) (*Driver, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if o.Endpoint == "" {
		o.Endpoint = Endpoint
	}
	endpoint, err := url.Parse(o.Endpoint)
	if err != nil || endpoint.Host == "" || (endpoint.Scheme != "https" && endpoint.Scheme != "http") ||
		endpoint.User != nil || endpoint.Opaque != "" || endpoint.RawQuery != "" || endpoint.Fragment != "" ||
		o.MaxResponseBytes < 0 || o.MaxResponseBytes == math.MaxInt64 || strings.ContainsAny(o.Token, "\r\n") {
		return nil, core.Fail(core.InvalidRequest, nil)
	}
	if o.RootPath == "" {
		o.RootPath = "disk:/"
	}
	root, err := nativePath(o.RootPath)
	if err != nil {
		return nil, err
	}
	endpoint.Path = strings.TrimSuffix(endpoint.Path, "/")
	if endpoint.Path == "" || core.ValidatePath(endpoint.Path) != nil {
		return nil, core.Fail(core.InvalidRequest, nil)
	}
	endpoint.RawPath = ""
	if o.MaxResponseBytes == 0 {
		o.MaxResponseBytes = 4 << 20
	}
	api := o.Client
	if api == nil {
		api = http.DefaultClient
	}
	content := o.ContentClient
	if content == nil {
		content = &http.Client{}
	}
	d := &Driver{endpoint: endpoint, root: root, token: o.Token, api: *api, content: *content, limit: o.MaxResponseBytes}
	if !strings.HasPrefix(root, "app:") {
		d.canonical = root
	}
	d.api.Jar, d.content.Jar = nil, nil
	refuse := func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	d.api.CheckRedirect = refuse
	d.content.CheckRedirect = func(req *http.Request, via []*http.Request) (err error) {
		defer func() {
			body := req.Response.Body
			req.Response.Body = http.NoBody
			err = errors.Join(err, d.protected.CloseBody(body))
		}()
		u := req.URL
		if len(via) == 0 || len(via) >= 10 || req.Method != "GET" || u.Host == "" ||
			(u.Scheme != "https" && u.Scheme != "http") || u.User != nil || u.Opaque != "" || u.Fragment != "" ||
			(via[len(via)-1].URL.Scheme == "https" && u.Scheme != "https") {
			return core.Fail(core.InvalidUpstreamResponse, errors.New("invalid content redirect"))
		}
		req.Header = make(http.Header)
		req.Header.Set("Accept-Encoding", "identity")
		if value := via[0].Header.Get("Range"); value != "" {
			req.Header.Set("Range", value)
		}
		return nil
	}
	meta, err := d.Stat(ctx, core.NativePath("/"))
	if err == nil && meta.Kind != core.Directory {
		err = core.Fail(core.NotDirectory, nil)
	}
	return d, err
}

// nativePath applies only the API's documented /foo == disk:/foo spelling.
// It does not clean, case-fold or otherwise reinterpret native path components.
func nativePath(value string) (string, error) {
	scope, rest := "disk:", value
	if strings.HasPrefix(value, "disk:") {
		rest = value[5:]
	} else if strings.HasPrefix(value, "app:") {
		scope, rest = "app:", value[4:]
	}
	if rest == "//" {
		return "", core.Fail(core.InvalidPath, nil)
	}
	if len(rest) > 1 {
		rest = strings.TrimSuffix(rest, "/")
	}
	if err := core.ValidatePath(rest); err != nil {
		return "", err
	}
	return scope + rest, nil
}

func (d *Driver) target(ctx context.Context, p core.NativeAddress) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if d.closed.Load() {
		return "", core.Fail(core.MountUnavailable, nil)
	}
	if p.Namespace() != core.TreeNamespace {
		return "", core.Fail(core.UnsupportedOperation, nil)
	}
	if err := core.ValidatePath(p.Value()); err != nil {
		return "", err
	}
	if p.Value() == "/" {
		return d.root, nil
	}
	return strings.TrimSuffix(d.root, "/") + p.Value(), nil
}

func (d *Driver) address(route string, query url.Values) *url.URL {
	u := *d.endpoint
	u.Path += route
	u.RawQuery = query.Encode()
	return &u
}

func (d *Driver) request(ctx context.Context, method string, target *url.URL) (*http.Response, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if d.closed.Load() {
		return nil, core.Fail(core.MountUnavailable, nil)
	}
	req, err := http.NewRequestWithContext(ctx, method, target.String(), nil)
	if err != nil {
		return nil, core.Fail(core.InvalidRequest, err)
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Accept-Encoding", "identity")
	if d.token != "" {
		req.Header.Set("Authorization", "OAuth "+d.token)
	}
	response, err := d.api.Do(req)
	if err != nil {
		if response != nil {
			err = errors.Join(err, d.closeBody(response.Body))
		}
		return nil, networkError(ctx, err)
	}
	return response, nil
}

func networkError(ctx context.Context, err error) error {
	var failure *url.Error
	if errors.As(err, &failure) {
		err = failure.Err
	}
	err = failedEOF(err)
	return errors.Join(core.Fail(core.UpstreamFailure, err), ctx.Err())
}

// The valid result distinguishes a complete typed JSON receipt from independent
// response cleanup errors. A partial decoder output is never confirmation.
func (d *Driver) decode(response *http.Response, output any, expected ...int) (valid bool, err error) {
	defer func() { err = errors.Join(err, d.closeBody(response.Body)) }()
	if encoding := response.Header.Get("Content-Encoding"); encoding != "" && encoding != "identity" {
		return false, core.Fail(core.InvalidUpstreamResponse, nil)
	}
	data, err := io.ReadAll(io.LimitReader(response.Body, d.limit+1))
	if int64(len(data)) > d.limit {
		return false, errors.Join(core.Fail(core.ResponseBudgetExceeded, nil), failedEOF(err))
	}
	if err != nil {
		return false, failedEOF(err)
	}
	status := http.StatusOK
	if len(expected) != 0 {
		status = expected[0]
	}
	if response.StatusCode != status {
		return false, statusError(response.StatusCode, data)
	}
	if err := json.Unmarshal(data, output); err != nil {
		return false, core.Fail(core.InvalidUpstreamResponse, err)
	}
	return true, nil
}

// Only validated native enumeration EOF means completion. Physical I/O and
// cleanup EOF failures must never become a terminal Core page.
func failedEOF(err error) error {
	if errors.Is(err, io.EOF) {
		return fmt.Errorf("native I/O failed: %v: %w", err, io.ErrUnexpectedEOF)
	}
	return err
}

func statusError(status int, data []byte) error {
	code := core.UpstreamFailure
	switch status {
	case 401, 403:
		code = core.UpstreamAccessDenied
	case 404:
		code = core.NotFound
	case 409:
		code = core.AlreadyExists
	case 412:
		code = core.PreconditionFailed
	case 429, 503:
		code = core.UpstreamThrottled
	}
	return core.Fail(code, fmt.Errorf("Yandex returned HTTP %d: %s", status, strings.TrimSpace(string(data))))
}

func (d *Driver) closeBody(body io.Closer) error {
	if body == nil {
		return nil
	}
	err := failedEOF(body.Close())
	if err != nil {
		d.mu.Lock()
		d.failed = append(d.failed, body)
		d.mu.Unlock()
	}
	return err
}

// Close retries owned response cleanup. Core has canceled and joined active
// uses; borrowed clients and transports remain host-owned.
func (d *Driver) Close() error {
	d.closed.Store(true)
	d.mu.Lock()
	failed := d.failed
	d.failed = nil
	d.mu.Unlock()
	var err error
	for _, body := range failed {
		err = errors.Join(err, d.closeBody(body))
	}
	return errors.Join(err, d.protected.Retry())
}

// App-folder requests retain app:/ authorization scope even when the API
// reports the constructor-observed absolute disk:/ path in its metadata.
func (d *Driver) observed(target string) string {
	return strings.TrimSuffix(d.canonical, "/") + strings.TrimPrefix(target, strings.TrimSuffix(d.root, "/"))
}

type resource struct {
	Name     string        `json:"name"`
	Path     string        `json:"path"`
	Type     string        `json:"type"`
	Size     *int64        `json:"size"`
	Modified *time.Time    `json:"modified"`
	Embedded *resourcePage `json:"_embedded"`
}

type resourcePage struct {
	Items  []resource `json:"items"`
	Path   string     `json:"path"`
	Offset *int64     `json:"offset"`
	Limit  *int64     `json:"limit"`
	Total  *int64     `json:"total"`
}

func (r resource) metadata(expected string) (core.Metadata, error) {
	actual, err := nativePath(r.Path)
	if err != nil || actual != expected {
		return core.Metadata{}, core.Fail(core.InvalidUpstreamResponse, errors.New("resource differs from requested path"))
	}
	name := path.Base(strings.TrimPrefix(strings.TrimPrefix(expected, "disk:"), "app:"))
	if name == "/" {
		name = ""
	}
	if r.Name != name && name != "" {
		return core.Metadata{}, core.Fail(core.InvalidUpstreamResponse, errors.New("resource name differs from requested name"))
	}
	meta := core.Metadata{Name: r.Name, Kind: core.Directory, ModifiedAt: r.Modified}
	switch r.Type {
	case "dir":
	case "file":
		if r.Size != nil && *r.Size < 0 {
			return meta, core.Fail(core.InvalidUpstreamResponse, nil)
		}
		meta.Kind, meta.Size = core.File, r.Size
	default:
		return meta, core.Fail(core.UnsupportedOperation, errors.New("unsupported resource type"))
	}
	return meta, nil
}

func (d *Driver) Stat(ctx context.Context, p core.NativeAddress, selections ...core.ReadSelection) (core.Metadata, error) {
	if len(selections) > 1 {
		return core.Metadata{}, core.Fail(core.InvalidRequest, nil)
	}
	var selection core.ReadSelection
	if len(selections) == 1 {
		selection = selections[0]
	}
	if selection.Version != "" {
		return core.Metadata{}, core.Fail(core.UnsupportedOperation, nil)
	}
	if err := selection.When.ForDomain(""); err != nil {
		return core.Metadata{}, err
	}
	target, err := d.target(ctx, p)
	if err != nil {
		return core.Metadata{}, err
	}
	response, err := d.request(ctx, "GET", d.address("/resources", url.Values{"path": {target}, "fields": {"name,path,type,size,modified"}}))
	if err != nil {
		return core.Metadata{}, err
	}
	var r resource
	if _, err = d.decode(response, &r); err != nil {
		return core.Metadata{}, err
	}
	if d.canonical == "" && p.Value() == "/" && strings.HasPrefix(d.root, "app:") {
		actual, e := nativePath(r.Path)
		relative := strings.TrimPrefix(d.root, "app:")
		if relative != "/" && !strings.HasSuffix(actual, relative) {
			return core.Metadata{}, core.Fail(core.InvalidUpstreamResponse, errors.New("app-folder reply changed relative root"))
		}
		if e != nil || (!strings.HasPrefix(actual, "disk:") && actual != target) {
			return core.Metadata{}, core.Fail(core.InvalidUpstreamResponse, errors.New("invalid app-folder canonical root"))
		}
		meta, e := r.metadata(actual)
		if e != nil {
			return meta, e
		}
		if meta.Kind != core.Directory {
			return meta, core.Fail(core.NotDirectory, nil)
		}
		d.canonical = actual
	}
	meta, err := r.metadata(d.observed(target))
	if p.Value() == "/" {
		meta.Name = ""
	}
	if err == nil {
		err = selection.Check(meta.Evidence)
	}
	return meta, err
}

type link struct {
	Href      string `json:"href"`
	Method    string `json:"method"`
	Templated bool   `json:"templated"`
}

func (l link) target() (*url.URL, error) {
	u, err := url.Parse(l.Href)
	if err != nil || l.Href == "" || l.Method != "GET" || l.Templated || u.Host == "" ||
		(u.Scheme != "https" && u.Scheme != "http") || u.User != nil || u.Fragment != "" || u.Opaque != "" {
		return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("invalid native GET capability"))
	}
	return u, nil
}

func (d *Driver) Open(ctx context.Context, p core.NativeAddress, o core.OpenOptions) (core.Opened, error) {
	if err := o.Validate(); err != nil {
		return core.Opened{}, err
	}
	if o.Mode != core.StreamRead || o.NoFollow || o.Selection.Version != "" {
		return core.Opened{}, core.Fail(core.UnsupportedOperation, nil)
	}
	if err := o.Selection.When.ForDomain(""); err != nil {
		return core.Opened{}, err
	}
	target, err := d.target(ctx, p)
	if err != nil {
		return core.Opened{}, err
	}
	if p.Value() == "/" {
		return core.Opened{}, core.Fail(core.NotFile, nil)
	}
	response, err := d.request(ctx, "GET", d.address("/resources/download", url.Values{"path": {target}}))
	if err != nil {
		return core.Opened{}, err
	}
	var capability link
	if _, err = d.decode(response, &capability); err != nil {
		return core.Opened{}, err
	}
	u, err := capability.target()
	if err == nil && d.endpoint.Scheme == "https" && u.Scheme != "https" {
		err = core.Fail(core.InvalidUpstreamResponse, errors.New("content capability downgraded HTTPS"))
	}
	if err != nil {
		return core.Opened{}, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return core.Opened{}, core.Fail(core.InvalidUpstreamResponse, nil)
	}
	req.Header.Set("Accept-Encoding", "identity")
	if o.Range != nil {
		req.Header.Set("Range", httpcontent.Header(o.Range))
	}
	response, err = d.content.Do(req)
	if err != nil {
		failure := core.Fail(core.UpstreamFailure, httpcontent.PrivateIOError(err))
		if response != nil {
			return core.Opened{}, errors.Join(failure, d.protected.CloseBody(response.Body), ctx.Err())
		}
		return core.Opened{}, errors.Join(failure, ctx.Err())
	}
	switch response.StatusCode {
	case http.StatusOK, http.StatusPartialContent, http.StatusRequestedRangeNotSatisfiable:
	default:
		return core.Opened{}, errors.Join(statusError(response.StatusCode, nil), d.protected.CloseBody(response.Body))
	}
	opened, err := d.protected.OpenBody(response, o.Range, path.Base(p.Value()))
	if err != nil {
		return core.Opened{}, err
	}
	if tag := response.Header.Get("ETag"); (core.Validator{Tag: tag}).Valid() {
		opened.Metadata.Evidence.Validator.Tag = tag
	}
	if err := o.Selection.Check(opened.Metadata.Evidence); err != nil {
		return core.Opened{}, errors.Join(err, d.protected.CloseBody(opened.Body))
	}
	return opened, nil
}

var _ core.Driver = (*Driver)(nil)
