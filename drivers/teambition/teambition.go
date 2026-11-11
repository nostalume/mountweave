// Package teambition mounts an authorized Teambition project folder.
package teambition

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/internal/httpcontent"
)

const endpoint = "https://open.teambition.com/api/v3"

type Options struct {
	OrganizationID, OperatorID, ProjectID, RootID string
	// Client authenticates control requests with an approved app access token.
	Client *http.Client
	// ContentClient is optional. Its transport must omit account credentials.
	ContentClient    *http.Client
	MaxResponseBytes int64
}

type Driver struct {
	api, content           http.Client
	organization, operator string
	project, root          string
	limit                  int64
	closed                 atomic.Bool
	cleanup                httpcontent.ResponseCleanup
}

// Open confirms the selected collection. Close a non-nil failed result to
// retry any retained HTTP response cleanup.
func Open(ctx context.Context, o Options) (*Driver, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !validID(o.OrganizationID) || !validID(o.OperatorID) || !validID(o.ProjectID) || !validID(o.RootID) ||
		o.Client == nil || o.Client == o.ContentClient || o.MaxResponseBytes < 0 || o.MaxResponseBytes == math.MaxInt64 {
		return nil, core.Fail(core.InvalidRequest, nil)
	}
	if o.MaxResponseBytes == 0 {
		o.MaxResponseBytes = 4 << 20
	}
	d := &Driver{api: httpcontent.IsolatedClient(o.Client), content: httpcontent.IsolatedClient(o.ContentClient),
		organization: o.OrganizationID, operator: o.OperatorID, project: o.ProjectID, root: o.RootID, limit: o.MaxResponseBytes}
	folders, err := get[[]folder](ctx, d, "/collection/query", url.Values{"collectionIds": {d.root}})
	if err == nil {
		switch {
		case len(folders) == 0:
			err = core.Fail(core.NotFound, nil)
		case len(folders) != 1 || folders[0].ID != d.root:
			err = core.Fail(core.InvalidUpstreamResponse, errors.New("Teambition did not confirm the selected root"))
		default:
			err = ctx.Err()
		}
	}
	if err != nil {
		d.closed.Store(true)
	}
	return d, err
}

func validID(s string) bool {
	return core.ValidName(s) && !strings.ContainsRune(s, ',')
}

func (d *Driver) check(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if d.closed.Load() {
		return core.Fail(core.MountUnavailable, nil)
	}
	return nil
}

func (d *Driver) request(req *http.Request, client *http.Client) (*http.Response, error) {
	if err := d.check(req.Context()); err != nil {
		return nil, err
	}
	req.Header.Set("Accept-Encoding", "identity")
	resp, err := client.Do(req)
	if err != nil {
		return nil, errors.Join(httpcontent.UpstreamIOError(err), req.Context().Err())
	}
	return resp, nil
}

func get[T any](ctx context.Context, d *Driver, route string, q url.Values) (result T, err error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint+route+"?"+q.Encode(), nil)
	if err != nil {
		return result, core.Fail(core.InvalidRequest, nil)
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("X-Tenant-Id", d.organization)
	req.Header.Set("X-Tenant-Type", "organization")
	req.Header.Set("X-Operator-Id", d.operator)
	resp, err := d.request(req, &d.api)
	if err != nil {
		return result, err
	}
	defer func() { err = errors.Join(err, d.cleanup.CloseBody(resp.Body), ctx.Err()) }()
	if resp.StatusCode != http.StatusOK {
		return result, statusError(resp.StatusCode)
	}
	data, err := httpcontent.ReadIdentity(resp, d.limit)
	if err != nil {
		return result, err
	}
	var envelope struct {
		Code   *int
		Result json.RawMessage
	}
	if json.Unmarshal(data, &envelope) != nil {
		return result, core.Fail(core.InvalidUpstreamResponse, errors.New("invalid Teambition response"))
	}
	if envelope.Code != nil && *envelope.Code != http.StatusOK {
		return result, statusError(*envelope.Code)
	}
	var decoded *T
	if json.Unmarshal(envelope.Result, &decoded) != nil || decoded == nil {
		return result, core.Fail(core.InvalidUpstreamResponse, errors.New("invalid Teambition result"))
	}
	return *decoded, nil
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
	return core.Fail(code, fmt.Errorf("Teambition returned status %d", status))
}

// Close retries internal HTTP response cleanup. The clients remain host-owned.
func (d *Driver) Close() error {
	d.closed.Store(true)
	return d.cleanup.Retry()
}

var _ core.Driver = (*Driver)(nil)
