// Package codeartsrepo mounts a fixed Huawei Cloud CodeArts Repo commit.
package codeartsrepo

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/internal/httpcontent"
)

// Options identifies an immutable commit in a CodeArts Repo v4 repository.
type Options struct {
	// Endpoint is the trusted HTTPS API origin for the repository's region.
	Endpoint     string
	RepositoryID int32
	CommitSHA    string
	RootPath     string
	// Client authenticates requests to Endpoint for the selected repository.
	Client           *http.Client
	MaxResponseBytes int64
}

type Driver struct {
	endpoint, commit, root string
	client                 http.Client
	limit                  int64
	closed                 atomic.Bool
	cleanup                httpcontent.ResponseCleanup
}

// Open verifies the selected commit. Close a non-nil failed result to retry response cleanup.
func Open(ctx context.Context, o Options) (*Driver, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	u, err := url.Parse(o.Endpoint)
	if err != nil || u.Scheme != "https" || u.Hostname() == "" || u.User != nil ||
		u.Opaque != "" || u.RawQuery != "" || u.ForceQuery || strings.ContainsRune(o.Endpoint, '#') ||
		(u.EscapedPath() != "" && u.EscapedPath() != "/") ||
		o.RepositoryID < 1 || !validCommit(o.CommitSHA) || o.Client == nil ||
		o.MaxResponseBytes < 0 || o.MaxResponseBytes == math.MaxInt64 {
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
	d := &Driver{endpoint: "https://" + u.Host + "/v4/repositories/" +
		strconv.FormatInt(int64(o.RepositoryID), 10) + "/repository",
		commit: strings.ToLower(o.CommitSHA), limit: o.MaxResponseBytes, client: httpcontent.IsolatedClient(o.Client)}
	var reply struct {
		ID string `json:"id"`
	}
	_, err = d.readJSON(ctx, "/commits", url.Values{"sha": {d.commit}, "stats": {"false"}}, &reply)
	if err == nil && !strings.EqualFold(d.commit, reply.ID) {
		err = core.Fail(core.InvalidUpstreamResponse, errors.New("CodeArts Repo did not confirm the selected commit"))
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

func validCommit(sha string) bool {
	if len(sha) != 40 {
		return false
	}
	var decoded [20]byte
	_, err := hex.Decode(decoded[:], []byte(sha))
	return err == nil
}

// Close retries retained response cleanup. The HTTP client remains host-owned.
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

func (d *Driver) get(ctx context.Context, route string, query url.Values, accept string) (*http.Response, error) {
	if err := d.active(ctx); err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, d.endpoint+route+"?"+query.Encode(), nil)
	if err != nil {
		return nil, core.Fail(core.InvalidRequest, nil)
	}
	req.Header.Set("Accept", accept)
	req.Header.Set("Accept-Encoding", "identity")
	resp, err := d.client.Do(req)
	if err != nil {
		return nil, errors.Join(httpcontent.UpstreamIOError(err), ctx.Err())
	}
	if resp.StatusCode != http.StatusOK {
		return nil, errors.Join(statusError(resp.StatusCode), d.cleanup.CloseBody(resp.Body), ctx.Err())
	}
	return resp, nil
}

func (d *Driver) readJSON(ctx context.Context, route string, query url.Values, out any) (http.Header, error) {
	resp, err := d.get(ctx, route, query, "application/json")
	if err != nil {
		return nil, err
	}
	err = httpcontent.DecodeJSON(resp, d.limit, out)
	return resp.Header, errors.Join(err, d.cleanup.CloseBody(resp.Body), ctx.Err())
}

func statusError(status int) error {
	code := core.UpstreamFailure
	switch status {
	case http.StatusUnauthorized, http.StatusForbidden:
		code = core.UpstreamAccessDenied
	case http.StatusNotFound:
		code = core.NotFound
	case http.StatusTooManyRequests:
		code = core.UpstreamThrottled
	case http.StatusRequestTimeout, http.StatusGatewayTimeout:
		code = core.UpstreamTimeout
	}
	return core.Fail(code, fmt.Errorf("CodeArts Repo HTTP status %d", status))
}

var _ core.Driver = (*Driver)(nil)
