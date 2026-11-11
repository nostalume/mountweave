// Package webdav connects a rooted WebDAV collection to Mountweave.
package webdav

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"

	core "github.com/nostalume/mountweave"
)

// Options selects a remote collection. Client is borrowed, including its
// transport and authentication. Username/Password add Basic authentication;
// leave both empty when the supplied client owns authentication.
type Options struct {
	RootURL          string       `json:"root_url"`
	Username         string       `json:"username,omitempty"`
	Password         string       `json:"password,omitempty"`
	Client           *http.Client `json:"-"`
	MaxResponseBytes int64        `json:"max_response_bytes,omitempty"`
}

// Validate checks static settings only. Open always performs the same checks.
func (o Options) Validate() error {
	u, err := url.Parse(o.RootURL)
	if err != nil || u.Host == "" || (u.Scheme != "https" && u.Scheme != "http") ||
		u.User != nil || u.Opaque != "" || u.RawQuery != "" || u.Fragment != "" ||
		o.MaxResponseBytes < 0 || o.MaxResponseBytes == math.MaxInt64 || strings.Contains(o.Username, ":") ||
		(o.Username == "" && o.Password != "") {
		return core.Fail(core.InvalidRequest, errors.New("a collection URL and valid response budget are required"))
	}
	_, err = uriPath(u)
	return err
}

type Driver struct {
	options Options
	root    *url.URL
	client  http.Client
	closed  atomic.Bool
	mu      sync.Mutex
	failed  []io.Closer
}

// Open verifies the collection. A non-nil failed Driver retains response cleanup
// and must be closed. No client or input ownership is transferred to the Driver.
func Open(ctx context.Context, options Options) (*Driver, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := options.Validate(); err != nil {
		return nil, err
	}
	if options.MaxResponseBytes == 0 {
		options.MaxResponseBytes = 4 << 20
	}
	root, _ := url.Parse(options.RootURL)
	root.Path = strings.TrimSuffix(root.Path, "/") + "/"
	if root.RawPath != "" {
		root.RawPath = strings.TrimSuffix(root.RawPath, "/") + "/"
	}
	client := options.Client
	if client == nil {
		client = http.DefaultClient
	}
	d := &Driver{options: options, root: root, client: *client}
	d.client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	meta, err := d.stat(ctx, d.root)
	if err == nil && meta.Kind != core.Directory {
		err = core.Fail(core.NotDirectory, nil)
	}
	if err != nil {
		return d, err
	}
	return d, nil
}

func (d *Driver) target(p core.NativeAddress) (*url.URL, error) {
	if d.closed.Load() {
		return nil, core.Fail(core.MountUnavailable, nil)
	}
	if p.Namespace() != core.TreeNamespace {
		return nil, core.Fail(core.UnsupportedOperation, nil)
	}
	if err := core.ValidatePath(p.Value()); err != nil {
		return nil, err
	}
	u := *d.root
	child := strings.TrimPrefix(p.Value(), "/")
	u.Path += child
	if u.RawPath != "" {
		u.RawPath += (&url.URL{Path: child}).EscapedPath()
	}
	return &u, nil
}

// uriPath decodes each segment once and refuses encoded separators. It never
// cleans a path or changes stored spelling to make it fit the tree model.
func uriPath(u *url.URL) (string, error) {
	for _, encoded := range strings.Split(u.EscapedPath(), "/") {
		part, err := url.PathUnescape(encoded)
		if err != nil || strings.Contains(part, "/") {
			return "", core.Fail(core.UnrepresentableName, err)
		}
	}
	p := strings.TrimSuffix(u.Path, "/")
	if p == "" {
		p = "/"
	}
	return p, core.ValidatePath(p)
}

func sameOrigin(a, b *url.URL) bool {
	return a.Scheme == b.Scheme && strings.EqualFold(a.Host, b.Host) && b.User == nil && b.Fragment == ""
}

func (d *Driver) href(base *url.URL, text string, continuation bool) (*url.URL, string, error) {
	ref, err := url.Parse(text)
	if err != nil || text == "" {
		return nil, "", core.Fail(core.InvalidUpstreamResponse, err)
	}
	for _, part := range strings.Split(ref.Path, "/") {
		if part == "." || part == ".." {
			return nil, "", core.Fail(core.InvalidUpstreamResponse, errors.New("dot segments in WebDAV reference"))
		}
	}
	u := base.ResolveReference(ref)
	p, err := uriPath(u)
	root, _ := uriPath(d.root)
	if err != nil || !sameOrigin(d.root, u) || !core.Within(p, root) || (!continuation && u.RawQuery != "") {
		return nil, "", core.Fail(core.InvalidUpstreamResponse, errors.New("WebDAV reference leaves the selected collection"))
	}
	return u, p, nil
}

func (d *Driver) request(ctx context.Context, method string, target *url.URL, body io.Reader, headers http.Header) (*http.Response, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if d.closed.Load() {
		return nil, core.Fail(core.MountUnavailable, nil)
	}
	req, err := http.NewRequestWithContext(ctx, method, target.String(), body)
	if err != nil {
		return nil, core.Fail(core.InvalidRequest, err)
	}
	req.Header = headers
	if req.Header == nil {
		req.Header = make(http.Header)
	}
	req.Header.Set("Accept-Encoding", "identity")
	if d.options.Username != "" {
		req.SetBasicAuth(d.options.Username, d.options.Password)
	}
	if raw := req.Header.Get("Content-Length"); raw != "" {
		_, err = fmt.Sscan(raw, &req.ContentLength)
		if err != nil {
			return nil, core.Fail(core.InvalidRequest, err)
		}
		if req.ContentLength == 0 {
			req.Body = http.NoBody
		}
	}
	response, err := d.client.Do(req)
	// Collection canonicalization is a single, exact spelling adjustment, not
	// general redirect following. Never replay a mutation or borrowed input.
	if err == nil && method == "PROPFIND" && (response.StatusCode == 301 || response.StatusCode == 302 || response.StatusCode == 307 || response.StatusCode == 308) {
		next, parseErr := req.URL.Parse(response.Header.Get("Location"))
		if parseErr == nil && sameOrigin(req.URL, next) && next.EscapedPath() == req.URL.EscapedPath()+"/" && next.RawQuery == req.URL.RawQuery && req.GetBody != nil {
			if err = d.closeBody(response.Body); err != nil {
				return nil, err
			}
			req = req.Clone(ctx)
			req.URL = next
			req.Body, err = req.GetBody()
			if err != nil {
				return nil, err
			}
			response, err = d.client.Do(req)
		}
	}
	if err != nil {
		var failure *url.Error
		if errors.As(err, &failure) {
			err = failure.Err // Do not expose an authenticated continuation URL.
		}
		err = unexpectedEOF(err)
		if response != nil {
			err = errors.Join(err, d.closeBody(response.Body))
		}
		return nil, errors.Join(core.Fail(core.UpstreamFailure, err), ctx.Err())
	}
	response.Request = req
	return response, nil
}

func statusError(method string, status int) error {
	code := core.UpstreamFailure
	switch status {
	case 401, 403:
		code = core.UpstreamAccessDenied
	case 404:
		code = core.NotFound
	case 412, 304:
		code = core.PreconditionFailed
	case 429:
		code = core.UpstreamThrottled
	}
	return core.Fail(code, fmt.Errorf("WebDAV %s returned HTTP %d", method, status))
}

// An HTTP failure must not be mistaken for the end of an iterator.
func unexpectedEOF(err error) error {
	if errors.Is(err, io.EOF) {
		return incompleteResponse{err}
	}
	return err
}

type incompleteResponse struct{ error }

func (e incompleteResponse) Error() string {
	return e.error.Error() + ": " + io.ErrUnexpectedEOF.Error()
}

// Do not unwrap the EOF sentinel, but keep independent causes discoverable.
func (e incompleteResponse) Is(target error) bool {
	return target == io.ErrUnexpectedEOF || target != io.EOF && errors.Is(e.error, target)
}

func (e incompleteResponse) As(target any) bool { return errors.As(e.error, target) }

func (d *Driver) closeBody(body io.Closer) error {
	if body == nil {
		return nil
	}
	err := unexpectedEOF(body.Close())
	if err != nil {
		d.mu.Lock()
		d.failed = append(d.failed, body)
		d.mu.Unlock()
	}
	return err
}

// Close retries owned response cleanup. Borrowed clients and transports stay open.
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
