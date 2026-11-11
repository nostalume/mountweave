// Package autoindex mounts a read-only Nginx JSON directory index.
package autoindex

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"mime"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"unicode/utf8"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/internal/httpcontent"
)

type Options struct {
	RootURL string
	// Client is borrowed and may supply authentication for an HTTPS root.
	Client           *http.Client
	MaxResponseBytes int64
}

type Driver struct {
	root    string
	client  http.Client
	limit   int64
	closed  atomic.Bool
	cleanup httpcontent.ResponseCleanup
}

// Open verifies that the root serves an autoindex JSON directory. Close a
// non-nil failed result to finish any response cleanup that it retains.
func Open(ctx context.Context, o Options) (*Driver, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	root, err := url.Parse(o.RootURL)
	if err != nil || root.Hostname() == "" || root.User != nil || root.Opaque != "" ||
		root.ForceQuery || root.RawQuery != "" || strings.Contains(o.RootURL, "#") ||
		(root.Scheme != "https" && (root.Scheme != "http" || o.Client != nil)) ||
		o.MaxResponseBytes < 0 || o.MaxResponseBytes == math.MaxInt64 {
		return nil, core.Fail(core.InvalidRequest, nil)
	}
	path := strings.TrimSuffix(root.EscapedPath(), "/")
	if path != "" {
		for _, segment := range strings.Split(path[1:], "/") {
			name, err := url.PathUnescape(segment)
			if err != nil || !core.ValidName(name) {
				return nil, core.Fail(core.InvalidRequest, errors.New("invalid autoindex root path component"))
			}
		}
	}
	if o.MaxResponseBytes == 0 {
		o.MaxResponseBytes = 4 << 20
	}
	d := &Driver{root: root.Scheme + "://" + root.Host + path + "/", limit: o.MaxResponseBytes}
	if o.Client != nil {
		d.client = *o.Client
	}
	d.client.Jar = nil
	d.client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	_, err = d.list(ctx, d.root)
	if err != nil {
		d.closed.Store(true)
	}
	return d, err
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

func (d *Driver) get(ctx context.Context, target, accept, byteRange string) (*http.Response, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if d.closed.Load() {
		return nil, core.Fail(core.MountUnavailable, nil)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return nil, core.Fail(core.InvalidUpstreamResponse, nil)
	}
	req.Header.Set("Accept-Encoding", "identity")
	if accept != "" {
		req.Header.Set("Accept", accept)
	}
	if byteRange != "" {
		req.Header.Set("Range", byteRange)
	}
	response, err := d.client.Do(req)
	if err != nil {
		return nil, errors.Join(core.Fail(core.UpstreamFailure, httpcontent.PrivateIOError(err)), ctx.Err())
	}
	return response, nil
}

type item struct {
	Name, Type string
	Size       *int64
	Modified   string `json:"mtime"`
	meta       core.Metadata
}

func (d *Driver) list(ctx context.Context, target string) (_ []item, err error) {
	response, err := d.get(ctx, target, "application/json", "")
	if err != nil {
		return nil, err
	}
	defer func() { err = errors.Join(err, d.cleanup.CloseBody(response.Body), ctx.Err()) }()
	if response.StatusCode != http.StatusOK {
		return nil, responseError(response.StatusCode)
	}
	media, _, err := mime.ParseMediaType(response.Header.Get("Content-Type"))
	if err != nil || media != "application/json" {
		return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("autoindex did not return a JSON directory"))
	}
	data, err := httpcontent.ReadIdentity(response, d.limit)
	if err != nil {
		return nil, err
	}
	if !utf8.Valid(data) {
		return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("autoindex returned non-UTF-8 JSON"))
	}
	var items []item
	if err := json.Unmarshal(data, &items); err != nil || items == nil {
		return nil, core.Fail(core.InvalidUpstreamResponse, err)
	}
	for i := range items {
		f := &items[i]
		if !core.ValidName(f.Name) {
			return nil, core.Fail(core.UnrepresentableName, nil)
		}
		f.meta.Name = f.Name
		switch f.Type {
		case "file":
			if f.Size == nil || *f.Size < 0 {
				return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("invalid autoindex file size"))
			}
			f.meta.Kind, f.meta.Size = core.File, f.Size
		case "directory":
			f.meta.Kind = core.Directory
		case "other":
			continue
		default:
			return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("unknown autoindex item type"))
		}
		if f.Modified != "" {
			t, err := http.ParseTime(f.Modified)
			if err != nil {
				return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("invalid autoindex modification time"))
			}
			f.meta.ModifiedAt = &t
		}
	}
	return items, nil
}

func responseError(status int) error {
	code := core.UpstreamFailure
	switch {
	case status >= 300 && status < 400:
		return core.Fail(core.InvalidUpstreamResponse, errors.New("autoindex redirect refused"))
	case status == http.StatusUnauthorized || status == http.StatusForbidden:
		code = core.UpstreamAccessDenied
	case status == http.StatusNotFound:
		code = core.NotFound
	case status == http.StatusTooManyRequests:
		code = core.UpstreamThrottled
	}
	return core.Fail(code, fmt.Errorf("autoindex returned HTTP %d", status))
}

// Close retries failed internal cleanup. The client and transport are borrowed.
func (d *Driver) Close() error {
	d.closed.Store(true)
	return d.cleanup.Retry()
}

var _ core.Driver = (*Driver)(nil)
