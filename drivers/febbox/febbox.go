// Package febbox connects an authorized user's own FEB Box files to Mountweave.
package febbox

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"mime/multipart"
	"net/http"
	"net/url"
	"sync/atomic"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/internal/httpcontent"
)

const controlURL = "https://api.febbox.com/oauth"

// Options selects a native folder ID or an account-relative path. The default
// is the account's own root. Client supplies authorization to api.febbox.com.
// ContentClient is optional and its transport must be free of account credentials.
type Options struct {
	RootID, RootPath      string
	Client, ContentClient *http.Client
	MaxResponseBytes      int64 // Zero uses 4 MiB per control response.
	MaxResolvePages       int   // Zero leaves path lookup bounded by upstream and context.
}

type Driver struct {
	root         item
	api, content http.Client
	limit        int64
	resolvePages int
	cleanup      httpcontent.ResponseCleanup
	closed       atomic.Bool
}

// Open verifies the selected directory with the supplied authorization.
// A non-nil Driver returned with an error retains any failed response cleanup.
func Open(ctx context.Context, o Options) (*Driver, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if o.Client == nil || o.ContentClient == o.Client || o.RootID != "" && o.RootPath != "" ||
		o.MaxResponseBytes < 0 || o.MaxResponseBytes == math.MaxInt64 || o.MaxResolvePages < 0 ||
		o.RootID != "" && !httpcontent.ValidDecimalID(o.RootID) {
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
	d := &Driver{
		root: item{ID: "0", meta: core.Metadata{Kind: core.Directory}},
		api:  httpcontent.IsolatedClient(o.Client), content: httpcontent.IsolatedClient(o.ContentClient),
		limit: o.MaxResponseBytes, resolvePages: o.MaxResolvePages,
	}
	var root item
	var err error
	switch {
	case o.RootPath != "" && o.RootPath != "/":
		root, err = d.resolve(ctx, o.RootPath)
	default:
		id := o.RootID
		if id == "" {
			id = "0"
		}
		root, err = d.readRoot(ctx, id)
	}
	if err == nil && root.meta.Kind != core.Directory {
		err = core.Fail(core.NotDirectory, nil)
	}
	if err != nil {
		if cleanupErr := d.Close(); cleanupErr != nil {
			return d, errors.Join(err, cleanupErr)
		}
		return nil, err
	}
	d.root = root
	return d, nil
}

// Close retires the Driver and retries cleanup it still owns.
func (d *Driver) Close() error {
	d.closed.Store(true)
	return d.cleanup.Retry()
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

func (d *Driver) call(ctx context.Context, module string, fields url.Values, out any) (err error) {
	if err := ctx.Err(); err != nil {
		return err
	}
	if d.closed.Load() {
		return core.Fail(core.MountUnavailable, nil)
	}
	var payload bytes.Buffer
	form := multipart.NewWriter(&payload)
	if err := form.WriteField("module", module); err != nil {
		return err
	}
	for name, values := range fields {
		for _, value := range values {
			if err := form.WriteField(name, value); err != nil {
				return err
			}
		}
	}
	if err := form.Close(); err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, controlURL, &payload)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", form.FormDataContentType())
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Accept-Encoding", "identity")
	response, err := d.api.Do(req)
	if err != nil {
		return errors.Join(httpcontent.UpstreamIOError(err), ctx.Err())
	}
	defer func() { err = errors.Join(err, d.cleanup.CloseBody(response.Body)) }()
	if response.StatusCode != http.StatusOK {
		return statusError(response.StatusCode, 0, true)
	}
	var reply struct {
		Code *int64          `json:"code"`
		Data json.RawMessage `json:"data"`
	}
	if err := httpcontent.DecodeJSON(response, d.limit, &reply); err != nil {
		return errors.Join(err, ctx.Err())
	}
	if reply.Code == nil {
		return core.Fail(core.InvalidUpstreamResponse, nil)
	}
	if *reply.Code != 1 {
		return statusError(response.StatusCode, *reply.Code, true)
	}
	if len(reply.Data) == 0 || string(reply.Data) == "null" || json.Unmarshal(reply.Data, out) != nil {
		return core.Fail(core.InvalidUpstreamResponse, nil)
	}
	return ctx.Err()
}

func statusError(status int, native int64, control bool) error {
	code := core.UpstreamFailure
	switch {
	case status == http.StatusTooManyRequests || native == -429:
		code = core.UpstreamThrottled
	case status == http.StatusUnauthorized || status == http.StatusForbidden:
		code = core.UpstreamAccessDenied
	case status == http.StatusNotFound && control:
		code = core.NotFound
	}
	return core.Fail(code, fmt.Errorf("FEB Box returned HTTP %d, code %d", status, native))
}

func (d *Driver) Open(ctx context.Context, address core.NativeAddress, options core.OpenOptions) (core.Opened, error) {
	if err := options.Validate(); err != nil {
		return core.Opened{}, err
	}
	if options.Mode != core.StreamRead || options.NoFollow || options.Selection.Version != "" {
		return core.Opened{}, core.Fail(core.UnsupportedOperation, nil)
	}
	if err := options.Selection.When.ForDomain(""); err != nil {
		return core.Opened{}, err
	}
	if err := d.check(ctx, address); err != nil {
		return core.Opened{}, err
	}
	file, err := d.resolve(ctx, address.Value())
	if err != nil {
		return core.Opened{}, err
	}
	if file.meta.Kind != core.File {
		return core.Opened{}, core.Fail(core.NotFile, nil)
	}
	if err := options.Selection.Check(file.meta.Evidence); err != nil {
		return core.Opened{}, err
	}
	var signed []struct {
		ID     httpcontent.DecimalID `json:"fid"`
		Parent httpcontent.DecimalID `json:"parent_id"`
		Size   *int64                `json:"file_size"`
		URL    string                `json:"download_url"`
	}
	if err := d.call(ctx, "file_get_download_url", url.Values{"fids[]": {string(file.ID)}}, &signed); err != nil {
		return core.Opened{}, err
	}
	if len(signed) != 1 || signed[0].ID != file.ID || signed[0].Parent != file.Parent ||
		signed[0].Size != nil && *signed[0].Size < 0 {
		return core.Opened{}, core.Fail(core.InvalidUpstreamResponse, errors.New("download receipt disagrees with the selected file"))
	}
	grant := signed[0]
	u, err := url.Parse(grant.URL)
	if err != nil || !httpcontent.IsHTTPS(u) {
		return core.Opened{}, core.Fail(core.InvalidUpstreamResponse, errors.New("invalid original download URL"))
	}
	response, err := httpcontent.FollowDownload(u.String(), func(target string, _ bool) (*http.Response, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
		if err != nil {
			return nil, core.Fail(core.InvalidUpstreamResponse, nil)
		}
		req.Header.Set("Accept-Encoding", "identity")
		if options.Range != nil {
			req.Header.Set("Range", httpcontent.Header(options.Range))
		}
		response, err := d.content.Do(req)
		if err != nil {
			return nil, errors.Join(httpcontent.UpstreamIOError(err), ctx.Err())
		}
		return response, nil
	}, d.cleanup.CloseBody)
	if err != nil {
		return core.Opened{}, err
	}
	switch response.StatusCode {
	case http.StatusOK, http.StatusPartialContent, http.StatusRequestedRangeNotSatisfiable:
	default:
		return core.Opened{}, errors.Join(statusError(response.StatusCode, 0, false), d.cleanup.CloseBody(response.Body))
	}
	if grant.Size != nil {
		return d.cleanup.OpenSizedBody(response, options.Range, file.Name, *grant.Size)
	}
	return d.cleanup.OpenBody(response, options.Range, file.Name)
}

var _ core.Driver = (*Driver)(nil)
