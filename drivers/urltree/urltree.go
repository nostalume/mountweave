// Package urltree mounts a read-only tree of explicitly configured HTTPS URLs.
package urltree

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"path"
	"slices"
	"strings"
	"sync/atomic"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/internal/childscan"
	"github.com/nostalume/mountweave/drivers/internal/httpcontent"
)

type File struct {
	Path string // Absolute mount-relative Core tree path.
	URL  string // Absolute HTTPS URL, optionally with a signed query.
}

type Options struct {
	Files []File
	// Transport is borrowed and must not inject account credentials.
	// A nil Transport uses the default HTTP transport.
	Transport http.RoundTripper
}

type node struct {
	url      string
	meta     core.Metadata
	children []core.Metadata
}

type Driver struct {
	nodes   map[string]*node
	client  http.Client
	closed  atomic.Bool
	cleanup httpcontent.ResponseCleanup
}

// New validates the configuration and builds the directory projection without
// contacting the endpoints. The returned driver owns its configuration copy.
func New(o Options) (*Driver, error) {
	nodes := map[string]*node{"/": {meta: core.Metadata{Kind: core.Directory}}}
	for _, f := range o.Files {
		if err := core.ValidatePath(f.Path); err != nil {
			return nil, err
		}
		if nodes[f.Path] != nil {
			return nil, core.Fail(core.InvalidRequest, fmt.Errorf("URL tree path already exists: %q", f.Path))
		}
		target, err := url.Parse(f.URL)
		if err != nil || target.Scheme != "https" || target.Hostname() == "" ||
			target.User != nil || target.Opaque != "" || strings.Contains(f.URL, "#") {
			return nil, core.Fail(core.InvalidRequest, fmt.Errorf("invalid HTTPS URL for path %q", f.Path))
		}
		nodes[f.Path] = &node{url: f.URL, meta: core.Metadata{Name: path.Base(f.Path), Kind: core.File}}
		for parent := path.Dir(f.Path); ; parent = path.Dir(parent) {
			if prior := nodes[parent]; prior != nil {
				if prior.meta.Kind != core.Directory {
					return nil, core.Fail(core.InvalidRequest, fmt.Errorf("URL tree file is also a parent: %q", parent))
				}
				break
			}
			nodes[parent] = &node{meta: core.Metadata{Name: path.Base(parent), Kind: core.Directory}}
		}
	}
	for p, n := range nodes {
		if p != "/" {
			parent := nodes[path.Dir(p)]
			parent.children = append(parent.children, n.meta)
		}
	}
	for _, n := range nodes {
		slices.SortFunc(n.children, func(a, b core.Metadata) int { return strings.Compare(a.Name, b.Name) })
	}
	return &Driver{nodes: nodes, client: http.Client{
		Transport:     o.Transport,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}}, nil
}

func (d *Driver) lookup(ctx context.Context, address core.NativeAddress) (*node, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if d.closed.Load() {
		return nil, core.Fail(core.MountUnavailable, nil)
	}
	if address.Namespace() != core.TreeNamespace {
		return nil, core.Fail(core.UnsupportedOperation, nil)
	}
	if err := core.ValidatePath(address.Value()); err != nil {
		return nil, err
	}
	n := d.nodes[address.Value()]
	if n == nil {
		for p := path.Dir(address.Value()); ; p = path.Dir(p) {
			if parent := d.nodes[p]; parent != nil {
				if parent.meta.Kind != core.Directory {
					return nil, core.Fail(core.NotDirectory, nil)
				}
				break
			}
		}
		return nil, core.Fail(core.NotFound, nil)
	}
	return n, nil
}

func (d *Driver) Stat(ctx context.Context, address core.NativeAddress, selections ...core.ReadSelection) (core.Metadata, error) {
	if len(selections) > 1 {
		return core.Metadata{}, core.Fail(core.InvalidRequest, nil)
	}
	if len(selections) == 1 && (selections[0].Version != "" || !selections[0].When.Empty()) {
		return core.Metadata{}, core.Fail(core.UnsupportedOperation, nil)
	}
	n, err := d.lookup(ctx, address)
	if err != nil {
		return core.Metadata{}, err
	}
	return n.meta, nil
}

func (d *Driver) Enumerate(ctx context.Context, q core.NativeQuery) (core.Iterator, error) {
	if q.Delimiter() != "" || q.StartAfter() != "" {
		return nil, core.Fail(core.UnsupportedOperation, nil)
	}
	n, err := d.lookup(ctx, q.Address())
	if err != nil {
		return nil, err
	}
	if n.meta.Kind != core.Directory {
		return nil, core.Fail(core.NotDirectory, nil)
	}
	return childscan.Items(slices.Clone(n.children)), nil
}

func (d *Driver) Open(ctx context.Context, address core.NativeAddress, o core.OpenOptions) (core.Opened, error) {
	if err := o.Validate(); err != nil {
		return core.Opened{}, err
	}
	if o.Mode != core.StreamRead || o.Selection.Version != "" || !o.Selection.When.Empty() {
		return core.Opened{}, core.Fail(core.UnsupportedOperation, nil)
	}
	n, err := d.lookup(ctx, address)
	if err != nil {
		return core.Opened{}, err
	}
	if n.meta.Kind != core.File {
		return core.Opened{}, core.Fail(core.NotFile, nil)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, n.url, nil)
	if err != nil {
		return core.Opened{}, core.Fail(core.InvalidRequest, nil)
	}
	req.Header.Set("Accept-Encoding", "identity")
	if byteRange := httpcontent.Header(o.Range); byteRange != "" {
		req.Header.Set("Range", byteRange)
	}
	response, err := d.client.Do(req)
	if err != nil {
		return core.Opened{}, errors.Join(core.Fail(core.UpstreamFailure, httpcontent.PrivateIOError(err)), ctx.Err())
	}
	switch response.StatusCode {
	case http.StatusOK, http.StatusPartialContent, http.StatusRequestedRangeNotSatisfiable:
		return d.cleanup.OpenBody(response, o.Range, n.meta.Name)
	default:
		return core.Opened{}, errors.Join(responseError(response.StatusCode), d.cleanup.CloseBody(response.Body))
	}
}

func responseError(status int) error {
	code := core.UpstreamFailure
	switch {
	case status >= 300 && status < 400:
		return core.Fail(core.InvalidUpstreamResponse, errors.New("URL tree redirect refused"))
	case status == http.StatusUnauthorized || status == http.StatusForbidden:
		code = core.UpstreamAccessDenied
	case status == http.StatusNotFound:
		code = core.NotFound
	case status == http.StatusTooManyRequests:
		code = core.UpstreamThrottled
	}
	return core.Fail(code, fmt.Errorf("URL tree endpoint returned HTTP %d", status))
}

// Close retries failed internal cleanup. The transport is borrowed.
func (d *Driver) Close() error {
	d.closed.Store(true)
	return d.cleanup.Retry()
}

var _ core.Driver = (*Driver)(nil)
