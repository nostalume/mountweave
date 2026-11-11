// Package cnbreleases mounts the assets of a published CNB tag.
package cnbreleases

import (
	"context"
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

type Options struct {
	// Repository is the CNB repository slug, including its organization path.
	Repository, Tag string
	// Client authenticates to api.cnb.cool with repo-release:r. ContentClient
	// must be independent and must never inject account credentials.
	Client, ContentClient *http.Client
	MaxResponseBytes      int64
}

type Driver struct {
	base, tag    string
	api, content http.Client
	limit        int64
	closed       atomic.Bool
	cleanup      httpcontent.ResponseCleanup
}

// Open confirms that the tag selects a published release. A non-nil failed
// result may own response cleanup and must be closed by its caller.
func Open(ctx context.Context, o Options) (*Driver, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	repo := strings.Split(o.Repository, "/")
	if len(repo) < 2 || o.Client == nil || o.Client == o.ContentClient ||
		o.Tag == "" || o.Tag == "." || o.Tag == ".." || !utf8.ValidString(o.Tag) ||
		o.MaxResponseBytes < 0 || o.MaxResponseBytes == math.MaxInt64 {
		return nil, core.Fail(core.InvalidRequest, nil)
	}
	for i, segment := range repo {
		if !core.ValidName(segment) || segment == "-" {
			return nil, core.Fail(core.InvalidRequest, nil)
		}
		repo[i] = url.PathEscape(segment)
	}
	if o.MaxResponseBytes == 0 {
		o.MaxResponseBytes = 4 << 20
	}
	d := &Driver{
		base:    "https://api.cnb.cool/" + strings.Join(repo, "/") + "/-/releases",
		tag:     o.Tag,
		limit:   o.MaxResponseBytes,
		api:     httpcontent.IsolatedClient(o.Client),
		content: httpcontent.IsolatedClient(o.ContentClient),
	}
	_, err := d.assets(ctx)
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
	if accept != "" {
		req.Header.Set("Accept", accept)
	}
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

func (d *Driver) assets(ctx context.Context) (_ []asset, err error) {
	target := d.base + "/tags/" + url.PathEscape(d.tag)
	response, err := d.get(ctx, &d.api, target, "application/json", "")
	if err != nil {
		return nil, err
	}
	defer func() { err = errors.Join(err, d.cleanup.CloseBody(response.Body), ctx.Err()) }()
	if response.StatusCode != http.StatusOK {
		return nil, responseError(response.StatusCode)
	}
	var release struct {
		ID     string
		Tag    string `json:"tag_name"`
		Draft  *bool
		Assets []asset
	}
	if err := httpcontent.DecodeJSON(response, d.limit, &release); err != nil {
		return nil, err
	}
	if release.ID == "" || release.Tag != d.tag || release.Draft == nil {
		return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("CNB did not confirm the selected release"))
	}
	if *release.Draft {
		return nil, core.Fail(core.NotFound, nil)
	}
	for _, f := range release.Assets {
		if f.ID == "" || f.Size != nil && *f.Size < 0 {
			return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("invalid CNB release asset"))
		}
		if !core.ValidName(f.Name) {
			return nil, core.Fail(core.UnrepresentableName, nil)
		}
	}
	return release.Assets, nil
}

func responseError(status int) error {
	code := core.UpstreamFailure
	switch status {
	case http.StatusUnauthorized, http.StatusForbidden:
		code = core.UpstreamAccessDenied
	case http.StatusNotFound:
		code = core.NotFound
	case http.StatusTooManyRequests:
		code = core.UpstreamThrottled
	}
	return core.Fail(code, fmt.Errorf("CNB returned HTTP %d", status))
}

// Close retries failed internal response cleanup. Both clients are borrowed.
func (d *Driver) Close() error {
	d.closed.Store(true)
	return d.cleanup.Retry()
}

var _ core.Driver = (*Driver)(nil)
