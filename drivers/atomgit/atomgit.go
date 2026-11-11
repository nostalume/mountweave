// Package atomgit mounts an immutable AtomGit commit as a read-only tree.
package atomgit

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"unicode/utf8"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/internal/httpcontent"
)

const api = "https://api.atomgit.com/api/v5/repos/"

// Options selects a repository and ref. A nil Client permits anonymous access
// where the AtomGit API allows it.
type Options struct {
	Owner, Repository string
	Ref, RootPath     string
	Client            *http.Client
	MaxResponseBytes  int64
}

type Driver struct {
	endpoint, commit, root string
	client                 http.Client
	limit                  int64
	closed                 atomic.Bool
	cleanup                httpcontent.ResponseCleanup
}

// Open resolves the ref once and optionally mounts a selected subdirectory.
// Close a non-nil failed result to retry retained response cleanup.
func Open(ctx context.Context, o Options) (*Driver, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !core.ValidName(o.Owner) || !core.ValidName(o.Repository) ||
		o.Ref == "" || o.Ref == "." || o.Ref == ".." || !utf8.ValidString(o.Ref) ||
		strings.ContainsRune(o.Ref, 0) || o.MaxResponseBytes < 0 || o.MaxResponseBytes == math.MaxInt64 {
		return nil, core.Fail(core.InvalidRequest, nil)
	}
	if o.RootPath == "" {
		o.RootPath = "/"
	}
	if err := core.ValidatePath(o.RootPath); err != nil {
		return nil, err
	}
	if o.MaxResponseBytes == 0 {
		o.MaxResponseBytes = 4 << 20
	}
	d := &Driver{endpoint: api + url.PathEscape(o.Owner) + "/" + url.PathEscape(o.Repository),
		client: httpcontent.IsolatedClient(o.Client), limit: o.MaxResponseBytes}
	var result struct {
		SHA string `json:"sha"`
	}
	err := d.readJSON(ctx, "/commits/"+url.PathEscape(o.Ref), &result)
	if err == nil {
		if !validSHA(result.SHA) || validSHA(o.Ref) && !strings.EqualFold(result.SHA, o.Ref) {
			err = core.Fail(core.InvalidUpstreamResponse, errors.New("AtomGit did not confirm the selected commit"))
		} else {
			d.commit = strings.ToLower(result.SHA)
		}
	}
	if err == nil && o.RootPath != "/" {
		var selected entry
		selected, err = d.resolve(ctx, core.NativePath(o.RootPath))
		if err == nil {
			if selected.kind != core.Directory {
				err = core.Fail(core.NotDirectory, nil)
			} else {
				d.root = selected.Path
			}
		}
	}
	if err != nil {
		d.closed.Store(true)
	}
	return d, err
}

func validSHA(s string) bool {
	if len(s) != 40 {
		return false
	}
	var hash [20]byte
	_, err := hex.Decode(hash[:], []byte(s))
	return err == nil
}

// Close retries retained response cleanup. The host still owns the transport.
func (d *Driver) Close() error {
	d.closed.Store(true)
	return d.cleanup.Retry()
}

func (d *Driver) active(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if d.closed.Load() {
		return core.Fail(core.MountUnavailable, nil)
	}
	return nil
}

func (d *Driver) get(ctx context.Context, route, accept string, wanted *core.Range) (*http.Response, error) {
	if err := d.active(ctx); err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, d.endpoint+route, nil)
	if err != nil {
		return nil, core.Fail(core.InvalidRequest, nil)
	}
	req.Header.Set("Accept", accept)
	req.Header.Set("Accept-Encoding", "identity")
	if wanted != nil {
		req.Header.Set("Range", httpcontent.Header(wanted))
	}
	resp, err := d.client.Do(req)
	if err != nil {
		return nil, errors.Join(httpcontent.UpstreamIOError(err), ctx.Err())
	}
	return resp, nil
}

func (d *Driver) readJSON(ctx context.Context, route string, out any) error {
	resp, err := d.get(ctx, route, "application/json", nil)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return d.reject(ctx, resp)
	}
	err = httpcontent.DecodeJSON(resp, d.limit, out)
	return errors.Join(err, d.cleanup.CloseBody(resp.Body), ctx.Err())
}

func (d *Driver) reject(ctx context.Context, resp *http.Response) error {
	code := core.UpstreamFailure
	switch resp.StatusCode {
	case http.StatusUnauthorized, http.StatusForbidden:
		code = core.UpstreamAccessDenied
	case http.StatusNotFound:
		code = core.NotFound
	case http.StatusTooManyRequests:
		code = core.UpstreamThrottled
	case http.StatusRequestTimeout, http.StatusGatewayTimeout:
		code = core.UpstreamTimeout
	}
	return errors.Join(core.Fail(code, fmt.Errorf("AtomGit HTTP status %d", resp.StatusCode)),
		d.cleanup.CloseBody(resp.Body), ctx.Err())
}

var _ core.Driver = (*Driver)(nil)
