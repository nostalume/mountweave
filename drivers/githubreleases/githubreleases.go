// Package githubreleases mounts the uploaded assets of one GitHub release.
package githubreleases

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"sync/atomic"
	"unicode/utf8"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/internal/httpcontent"
)

type Options struct {
	Owner, Repository, Tag string
	// A nil Client reads public releases anonymously. ContentClient is
	// independent and must never inject account credentials.
	Client, ContentClient *http.Client
	MaxResponseBytes      int64
}

type Driver struct {
	base, release string
	api, content  http.Client
	limit         int64
	closed        atomic.Bool
	cleanup       httpcontent.ResponseCleanup
}

// Open fixes the native release identity selected by Tag. A non-nil result
// returned with an error may retain cleanup and must be closed by its caller.
func Open(ctx context.Context, o Options) (*Driver, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !core.ValidName(o.Owner) || !core.ValidName(o.Repository) || !utf8.ValidString(o.Tag) ||
		o.Tag == "" || o.Tag == "." || o.Tag == ".." || o.Client != nil && o.Client == o.ContentClient ||
		o.MaxResponseBytes < 0 || o.MaxResponseBytes == math.MaxInt64 {
		return nil, core.Fail(core.InvalidRequest, nil)
	}
	if o.MaxResponseBytes == 0 {
		o.MaxResponseBytes = 4 << 20
	}
	d := &Driver{
		base:    "https://api.github.com/repos/" + url.PathEscape(o.Owner) + "/" + url.PathEscape(o.Repository) + "/releases",
		limit:   o.MaxResponseBytes,
		api:     httpcontent.IsolatedClient(o.Client),
		content: httpcontent.IsolatedClient(o.ContentClient),
	}
	var release struct {
		ID    int64
		Tag   string `json:"tag_name"`
		Draft bool
	}
	_, err := d.getJSON(ctx, "/tags/"+url.PathEscape(o.Tag), &release)
	if err == nil && (release.ID <= 0 || release.Tag != o.Tag || release.Draft) {
		err = core.Fail(core.InvalidUpstreamResponse, errors.New("GitHub did not confirm the selected published release"))
	}
	if err != nil {
		d.closed.Store(true)
		return d, err
	}
	d.release = "/" + strconv.FormatInt(release.ID, 10)
	return d, nil
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

func (d *Driver) get(ctx context.Context, client *http.Client, target, accept, byteRange string) (*http.Response, error) {
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
	if client == &d.api {
		req.Header.Set("X-GitHub-Api-Version", "2022-11-28")
	}
	req.Header.Set("Accept", accept)
	req.Header.Set("Accept-Encoding", "identity")
	if byteRange != "" {
		req.Header.Set("Range", byteRange)
	}
	response, err := client.Do(req)
	if err != nil {
		return nil, errors.Join(core.Fail(core.UpstreamFailure, httpcontent.PrivateIOError(err)), ctx.Err())
	}
	return response, nil
}

func (d *Driver) getJSON(ctx context.Context, route string, out any) (_ http.Header, err error) {
	response, err := d.get(ctx, &d.api, d.base+route, "application/vnd.github+json", "")
	if err != nil {
		return nil, err
	}
	defer func() { err = errors.Join(err, d.cleanup.CloseBody(response.Body), ctx.Err()) }()
	if response.StatusCode != http.StatusOK {
		return nil, responseError(response)
	}
	if err := httpcontent.DecodeJSON(response, d.limit, out); err != nil {
		return nil, err
	}
	return response.Header, nil
}

func responseError(response *http.Response) error {
	status := response.StatusCode
	code := core.UpstreamFailure
	switch {
	case status == http.StatusTooManyRequests || status == http.StatusForbidden &&
		(response.Header.Get("X-RateLimit-Remaining") == "0" || response.Header.Get("Retry-After") != ""):
		code = core.UpstreamThrottled
	case status == http.StatusUnauthorized || status == http.StatusForbidden:
		code = core.UpstreamAccessDenied
	case status == http.StatusNotFound:
		code = core.NotFound
	}
	return core.Fail(code, fmt.Errorf("GitHub returned HTTP %d", status))
}

// Close retries failed internal response cleanup. The host owns both clients.
func (d *Driver) Close() error {
	d.closed.Store(true)
	return d.cleanup.Retry()
}

var _ core.Driver = (*Driver)(nil)
