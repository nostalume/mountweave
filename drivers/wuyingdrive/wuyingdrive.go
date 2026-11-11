// Package wuyingdrive mounts a personal or team space in a WUYING enterprise drive.
package wuyingdrive

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/internal/httpcontent"
)

const rootID = "root"

type Options struct {
	Region, DriveID string
	// Set exactly one of EndUserID (personal space) and GroupID (team space).
	EndUserID, GroupID string
	// Client's transport signs WUYING ECD OpenAPI requests with RAM credentials.
	Client *http.Client `json:"-"`
	// ContentClient is optional. Its transport must omit account credentials.
	ContentClient    *http.Client `json:"-"`
	MaxResponseBytes int64
}

type Driver struct {
	region, drive, scopeField, scopeID string
	endpoint                           string
	api, content                       http.Client
	limit                              int64
	closed                             atomic.Bool
	cleanup                            httpcontent.ResponseCleanup
}

// Open verifies access to the selected space. Close a non-nil failed
// result to retry retained response cleanup.
func Open(ctx context.Context, o Options) (*Driver, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	scopeField, scopeID := "EndUserId", o.EndUserID
	if o.GroupID != "" {
		if o.EndUserID != "" {
			return nil, core.Fail(core.InvalidRequest, nil)
		}
		scopeField, scopeID = "GroupId", o.GroupID
	}
	if !validRegion(o.Region) || !core.ValidName(o.DriveID) || !core.ValidName(scopeID) ||
		o.Client == nil || o.Client == o.ContentClient || o.MaxResponseBytes < 0 || o.MaxResponseBytes == math.MaxInt64 {
		return nil, core.Fail(core.InvalidRequest, nil)
	}
	if o.MaxResponseBytes == 0 {
		o.MaxResponseBytes = 4 << 20
	}
	d := &Driver{region: o.Region, drive: o.DriveID, scopeField: scopeField, scopeID: scopeID, limit: o.MaxResponseBytes,
		endpoint: "https://ecd." + o.Region + ".aliyuncs.com/",
		api:      httpcontent.IsolatedClient(o.Client), content: httpcontent.IsolatedClient(o.ContentClient)}
	if err := d.confirmRoot(ctx); err != nil {
		d.closed.Store(true)
		return d, err
	}
	return d, nil
}

func validRegion(region string) bool {
	if region == "" || len(region) > 63 || region[0] == '-' || region[len(region)-1] == '-' {
		return false
	}
	for _, c := range region {
		if !(c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '-') {
			return false
		}
	}
	return true
}

func (d *Driver) confirmRoot(ctx context.Context) error {
	scan := listing{driver: d, parent: rootID}
	_, err := scan.page(ctx, 1)
	if err == io.EOF {
		return nil
	}
	return err
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

type result struct {
	Success *bool
	Code    string
}

func (r result) check() error {
	if r.Success == nil || *r.Success && r.Code != "" && r.Code != "success" {
		return core.Fail(core.InvalidUpstreamResponse, errors.New("WUYING returned an invalid result status"))
	}
	if !*r.Success {
		return statusError(http.StatusOK, r.Code)
	}
	return nil
}

func (d *Driver) control(ctx context.Context, action string, q url.Values, out any) (err error) {
	q.Set("RegionId", d.region)
	q.Set("CdsId", d.drive)
	q.Set(d.scopeField, d.scopeID)
	query := strings.ReplaceAll(q.Encode(), "+", "%20")
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, d.endpoint+"?"+query, nil)
	if err != nil {
		return core.Fail(core.InvalidRequest, nil)
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("x-acs-action", action)
	req.Header.Set("x-acs-version", "2020-09-30")
	response, err := d.request(req, &d.api)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, d.cleanup.CloseBody(response.Body), ctx.Err()) }()
	if response.StatusCode == http.StatusOK {
		return httpcontent.DecodeJSON(response, d.limit, out)
	}
	var native result
	if response.StatusCode >= 400 {
		err = httpcontent.DecodeJSON(response, d.limit, &native)
	}
	return errors.Join(statusError(response.StatusCode, native.Code), err)
}

func statusError(status int, native string) error {
	code := core.UpstreamFailure
	switch {
	case status == http.StatusUnauthorized || status == http.StatusForbidden:
		code = core.UpstreamAccessDenied
	case native == "Not.Found":
		return core.Fail(core.NotFound, errors.New("WUYING did not find the selected cloud drive"))
	case status == http.StatusNotFound:
		code = core.NotFound
	case status == http.StatusTooManyRequests:
		code = core.UpstreamThrottled
	case status == http.StatusRequestTimeout || status == http.StatusGatewayTimeout:
		code = core.UpstreamTimeout
	}
	if status == http.StatusOK {
		return core.Fail(code, errors.New("WUYING rejected the OpenAPI request"))
	}
	return core.Fail(code, fmt.Errorf("WUYING returned HTTP %d", status))
}

// Close retries HTTP response cleanup. Both clients remain host-owned.
func (d *Driver) Close() error {
	d.closed.Store(true)
	return d.cleanup.Retry()
}

var _ core.Driver = (*Driver)(nil)
