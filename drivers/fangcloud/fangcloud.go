// Package fangcloud connects an authenticated 360 Fangcloud personal space to Mountweave.
package fangcloud

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"sync"
	"sync/atomic"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/internal/httpcontent"
)

const endpoint = "https://open.fangcloud.com"

type Options struct {
	// RootID selects a native personal-space folder. RootPath instead resolves
	// an exact path from the user's personal root. Both zero select that root.
	RootID   int64
	RootPath string
	// Client authenticates requests to the fixed control origin. ContentClient
	// is independent and must never inject account credentials.
	Client, ContentClient *http.Client
	MaxResponseBytes      int64
	MaxResolvePages       int
}

type Driver struct {
	root         item
	api, content http.Client
	limit        int64
	resolvePages int
	closed       atomic.Bool
	mu           sync.Mutex
	failed       []io.Closer
	protected    httpcontent.ResponseCleanup
}

// Open confirms the selected directory. A non-nil failed result retains
// response cleanup and must be closed by its creator.
func Open(ctx context.Context, o Options) (*Driver, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if o.Client == nil || o.Client == o.ContentClient || o.RootID < 0 || o.RootID != 0 && o.RootPath != "" ||
		o.MaxResponseBytes < 0 || o.MaxResponseBytes == math.MaxInt64 || o.MaxResolvePages < 0 {
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
	d := &Driver{root: item{meta: core.Metadata{Kind: core.Directory}}, api: *o.Client,
		limit: o.MaxResponseBytes, resolvePages: o.MaxResolvePages}
	if o.ContentClient != nil {
		d.content = *o.ContentClient
	}
	for _, client := range []*http.Client{&d.api, &d.content} {
		client.Jar = nil
		client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	}
	var err error
	if o.RootID != 0 {
		d.root, err = d.folder(ctx, o.RootID)
	} else if o.RootPath != "" && o.RootPath != "/" {
		d.root, err = d.resolve(ctx, o.RootPath)
	}
	if err == nil && d.root.meta.Kind != core.Directory {
		err = core.Fail(core.NotDirectory, nil)
	}
	if err == nil {
		_, _, err = d.readPage(ctx, d.root.ID, 0)
	}
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

func (d *Driver) closeBody(body io.Closer, private bool) error {
	if body == nil {
		return nil
	}
	if private {
		return d.protected.CloseBody(body)
	}
	err := nativeFailure(body.Close(), false)
	if err != nil {
		d.mu.Lock()
		d.failed = append(d.failed, body)
		d.mu.Unlock()
	}
	return err
}

// Close retries failed internal cleanup. Core joins active calls before
// retirement. Clients and their transports remain host-owned.
func (d *Driver) Close() error {
	d.closed.Store(true)
	d.mu.Lock()
	failed := d.failed
	d.failed = nil
	d.mu.Unlock()
	var err error
	for _, body := range failed {
		err = errors.Join(err, d.closeBody(body, false))
	}
	return errors.Join(err, d.protected.Retry())
}

type unexpectedEOF struct{ cause error }

func (e unexpectedEOF) Error() string { return e.cause.Error() + ": " + io.ErrUnexpectedEOF.Error() }
func (e unexpectedEOF) Is(target error) bool {
	return target == io.ErrUnexpectedEOF || target != io.EOF && errors.Is(e.cause, target)
}

func nativeFailure(err error, private bool) error {
	if err == nil {
		return nil
	}
	if private {
		return httpcontent.PrivateIOError(err)
	}
	if errors.Is(err, io.EOF) {
		return unexpectedEOF{err}
	}
	return err
}

func (d *Driver) exchange(client *http.Client, req *http.Request, private bool) (*http.Response, error) {
	if err := req.Context().Err(); err != nil {
		return nil, err
	}
	if d.closed.Load() {
		return nil, core.Fail(core.MountUnavailable, nil)
	}
	req.Header.Set("Accept-Encoding", "identity")
	response, err := client.Do(req)
	if err == nil {
		return response, nil
	}
	err = nativeFailure(err, private)
	if response != nil {
		err = errors.Join(err, d.closeBody(response.Body, private))
	}
	return nil, errors.Join(core.Fail(core.UpstreamFailure, err), req.Context().Err())
}

func (d *Driver) call(ctx context.Context, route string, query url.Values, out any, private bool) (err error) {
	target := endpoint + route
	if len(query) != 0 {
		target += "?" + query.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return core.Fail(core.InvalidRequest, err)
	}
	req.Header.Set("Accept", "application/json")
	response, err := d.exchange(&d.api, req, private)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, d.closeBody(response.Body, private), ctx.Err()) }()
	if encoding := response.Header.Get("Content-Encoding"); encoding != "" && encoding != "identity" {
		return core.Fail(core.InvalidUpstreamResponse, nil)
	}
	data, err := io.ReadAll(io.LimitReader(response.Body, d.limit+1))
	if int64(len(data)) > d.limit {
		return core.Fail(core.ResponseBudgetExceeded, nativeFailure(err, private))
	}
	if err != nil {
		return core.Fail(core.UpstreamFailure, nativeFailure(err, private))
	}
	if response.StatusCode != http.StatusOK {
		code := core.UpstreamFailure
		switch response.StatusCode {
		case http.StatusUnauthorized, http.StatusForbidden:
			code = core.UpstreamAccessDenied
		case http.StatusNotFound:
			code = core.NotFound
		case http.StatusTooManyRequests, http.StatusServiceUnavailable:
			code = core.UpstreamThrottled
		}
		if private {
			return core.Fail(code, fmt.Errorf("Fangcloud returned HTTP %d", response.StatusCode))
		}
		return core.Fail(code, fmt.Errorf("Fangcloud returned HTTP %d: %s", response.StatusCode, data))
	}
	if err := json.Unmarshal(data, out); err != nil {
		if private {
			return core.Fail(core.InvalidUpstreamResponse, nil)
		}
		return core.Fail(core.InvalidUpstreamResponse, err)
	}
	return nil
}

var _ core.Driver = (*Driver)(nil)
