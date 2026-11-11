// Package bunny connects a rooted Bunny Storage REST directory to Mountweave.
package bunny

import (
	"context"
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

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/internal/httpcontent"
)

// Options selects a directory URL containing the storage zone and optional
// subdirectory. Client and its transport remain borrowed. AccessKey is the zone
// password; leave it empty only if the borrowed transport supplies authorization.
// MaxResponseBytes bounds each metadata response, not file content.
type Options struct {
	RootURL          string
	AccessKey        string
	Client           *http.Client
	MaxResponseBytes int64
}

type Driver struct {
	root     *url.URL
	client   http.Client
	options  Options
	listings chan struct{}
	closed   atomic.Bool
	mu       sync.Mutex
	failed   []io.Closer
}

// Open validates configuration and probes the selected directory. A non-nil
// failed Driver still owns response cleanup and must be closed.
func Open(ctx context.Context, o Options) (*Driver, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	root, err := url.Parse(o.RootURL)
	if err != nil || root.Host == "" || root.User != nil || root.Opaque != "" || root.RawQuery != "" || root.Fragment != "" ||
		(root.Scheme != "https" && root.Scheme != "http") || o.MaxResponseBytes < 0 || o.MaxResponseBytes == math.MaxInt64 || strings.ContainsAny(o.AccessKey, "\r\n") {
		return nil, core.Fail(core.InvalidRequest, errors.New("a rooted storage URL and valid response budget are required"))
	}
	for _, part := range strings.Split(root.EscapedPath(), "/") {
		decoded, e := url.PathUnescape(part)
		if e != nil || strings.Contains(decoded, "/") {
			return nil, core.Fail(core.InvalidRequest, e)
		}
	}
	name := strings.TrimSuffix(root.Path, "/")
	if core.ValidatePath(name) != nil || name == "/" || name == "" {
		return nil, core.Fail(core.InvalidRequest, nil)
	}
	root.Path, root.RawPath = name+"/", ""
	if o.MaxResponseBytes == 0 {
		o.MaxResponseBytes = 4 << 20
	}
	client := o.Client
	if client == nil {
		client = http.DefaultClient
	}
	d := &Driver{root: root, client: *client, options: o, listings: make(chan struct{}, 5)}
	d.client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	i := d.enumeration(ctx, root)
	_, err = i.Next(ctx, 1)
	if err == io.EOF {
		err = nil
	}
	err = errors.Join(err, d.closeOwned(i))
	return d, err
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
	u.Path += strings.TrimPrefix(p.Value(), "/")
	return &u, nil
}

func (d *Driver) request(ctx context.Context, method string, target *url.URL, headers http.Header) (*http.Response, error) {
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
	req.Header = headers
	if req.Header == nil {
		req.Header = make(http.Header)
	}
	req.Header.Set("Accept-Encoding", "identity")
	if d.options.AccessKey != "" {
		req.Header.Set("AccessKey", d.options.AccessKey)
	}
	response, err := d.client.Do(req)
	if err != nil {
		var failure *url.Error
		if errors.As(err, &failure) {
			err = failure.Err
		}
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		if response != nil {
			err = errors.Join(err, d.closeOwned(response.Body))
		}
		return nil, errors.Join(core.Fail(core.UpstreamFailure, err), ctx.Err())
	}
	return response, nil
}

func (d *Driver) responseError(response *http.Response) error {
	data, err := io.ReadAll(io.LimitReader(response.Body, 4096))
	code := core.UpstreamFailure
	switch response.StatusCode {
	case 401, 403:
		code = core.UpstreamAccessDenied
	case 404:
		code = core.NotFound
	case 412, 304:
		code = core.PreconditionFailed
	case 429:
		code = core.UpstreamThrottled
	case 400:
		if strings.Contains(strings.ToLower(string(data)), "unable to process request") {
			code = core.UpstreamThrottled
		}
	}
	return core.Fail(code, errors.Join(fmt.Errorf("Bunny HTTP %d: %s", response.StatusCode, strings.TrimSpace(string(data))), err))
}

func (d *Driver) closeOwned(body io.Closer) error {
	if body == nil {
		return nil
	}
	err := body.Close()
	if err != nil {
		d.mu.Lock()
		d.failed = append(d.failed, body)
		d.mu.Unlock()
	}
	return err
}

// Close retries failed owned response cleanup, leaving the borrowed client open.
func (d *Driver) Close() error {
	d.closed.Store(true)
	d.mu.Lock()
	failed := d.failed
	d.failed = nil
	d.mu.Unlock()
	var err error
	for _, closer := range failed {
		err = errors.Join(err, d.closeOwned(closer))
	}
	return err
}

func (d *Driver) Stat(ctx context.Context, p core.NativeAddress, selections ...core.ReadSelection) (meta core.Metadata, err error) {
	if len(selections) > 1 {
		return meta, core.Fail(core.InvalidRequest, nil)
	}
	var selection core.ReadSelection
	if len(selections) == 1 {
		selection = selections[0]
	}
	if selection.Version != "" {
		return meta, core.Fail(core.UnsupportedOperation, nil)
	}
	if err = selection.When.ForDomain(""); err != nil {
		return
	}
	target, err := d.target(p)
	if err != nil {
		return meta, err
	}
	if p.Value() == "/" {
		i := d.enumeration(ctx, target)
		_, err = i.Next(ctx, 1)
		if err == io.EOF {
			err = nil
		}
		err = errors.Join(err, d.closeOwned(i))
		meta.Kind = core.Directory
	} else {
		parent := *target
		parent.Path = path.Dir(parent.Path) + "/"
		i := d.enumeration(ctx, &parent)
		defer func() { err = errors.Join(err, d.closeOwned(i)) }()
		found := false
		for {
			items, failure := i.Next(ctx, core.MaxPage)
			if failure != nil && failure != io.EOF {
				return meta, failure
			}
			for _, item := range items {
				if item.Name == path.Base(target.Path) {
					if found {
						return meta, core.Fail(core.PathAmbiguous, nil)
					}
					meta, found = item, true
				}
			}
			if failure == io.EOF {
				break
			}
		}
		if !found {
			return meta, core.Fail(core.NotFound, nil)
		}
	}
	if err == nil {
		err = selection.Check(meta.Evidence)
	}
	return
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
	meta, err := d.Stat(ctx, p)
	if err != nil {
		return core.Opened{}, err
	}
	if meta.Kind != core.File {
		return core.Opened{}, core.Fail(core.NotFile, nil)
	}
	target, err := d.target(p)
	if err != nil {
		return core.Opened{}, err
	}
	headers := make(http.Header)
	if o.Range != nil {
		headers.Set("Range", httpcontent.Header(o.Range))
	}
	response, err := d.request(ctx, http.MethodGet, target, headers)
	if err != nil {
		return core.Opened{}, err
	}
	if response.StatusCode != 200 && response.StatusCode != 206 && response.StatusCode != 416 {
		opened := core.Opened{Body: response.Body}
		return opened, d.responseError(response)
	}
	opened, err := httpcontent.Open(response, o.Range)
	opened.Metadata.Name = meta.Name
	if tag := response.Header.Get("ETag"); tag != "" {
		opened.Metadata.Evidence.Validator.Tag = tag
	}
	if err == nil {
		err = o.Selection.Check(opened.Metadata.Evidence)
	}
	return opened, err
}

// RemoveSubtree maps only explicit recursive intent. It never enables zone-root
// deletion, never follows redirects and never retries an uncertain mutation.
func (d *Driver) RemoveSubtree(ctx context.Context, p core.NativeAddress, _ core.TreeOptions) (core.Change, error) {
	unchanged := core.Change{Effect: core.Unchanged}
	if err := ctx.Err(); err != nil {
		return unchanged, err
	}
	target, err := d.target(p)
	if err != nil {
		return unchanged, err
	}
	if p.Value() == "/" {
		return unchanged, core.Fail(core.UnsupportedOperation, nil)
	}
	response, err := d.request(ctx, http.MethodDelete, target, nil)
	if err != nil {
		return core.Change{Effect: core.Unknown}, err
	}
	result := core.Change{Effect: core.Unknown}
	if response.StatusCode == http.StatusOK {
		result.Effect = core.Applied
	} else {
		err = d.responseError(response)
		if response.StatusCode == 401 || response.StatusCode == 403 || response.StatusCode == 404 || response.StatusCode == 429 || core.CodeOf(err) == core.UpstreamThrottled {
			result.Effect = core.Unchanged
		}
	}
	return result, errors.Join(err, d.closeOwned(response.Body), ctx.Err())
}

var _ core.Driver = (*Driver)(nil)
var _ core.SubtreeRemover = (*Driver)(nil)
