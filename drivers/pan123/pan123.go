// Package pan123 connects ordinary 123 Open account files to Mountweave.
package pan123

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"sync/atomic"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/internal/httpcontent"
	"github.com/nostalume/mountweave/drivers/internal/upload"
)

const (
	endpoint        = "https://open-api.123pan.com"
	downloadRoute   = "/api/v1/file/download_info"
	directLinkRoute = "/api/v1/direct-link/url"
)

// Options selects either an account-root-relative RootPath or a native RootID;
// neither means account root 0. Client borrows control authentication from the
// host. ContentClient is independent and must not inject account credentials.
type Options struct {
	RootID, RootPath      string
	Client, ContentClient *http.Client
	MaxResponseBytes      int64
	MaxResolvePages       int
	// AllowMkdir enables native directory creation with the control Client.
	AllowMkdir bool
	// AllowRemove enables moving a single ordinary file to the recycle bin.
	AllowRemove bool
	// UseDirectLinks selects the official direct-link grant for reads from an
	// already enabled space. It does not enable direct-link space.
	UseDirectLinks bool
	// UploadClient uses the same Open account as Client. Its transport must
	// refuse requests outside its trusted 123 Open upload origins. Set it with
	// a positive shared payload budget to enable writes.
	UploadClient   *http.Client
	MaxUploadBytes int64
	// PresignedMultipart selects V1 for large-file multipart uploads. Single
	// uploads still use UploadClient. Signed slices use ContentClient.
	PresignedMultipart bool
}

type Driver struct {
	root         item
	api, content http.Client
	limit        int64
	resolvePages int
	allowMkdir   bool
	allowRemove  bool
	directLinks  bool
	uploadClient *http.Client
	uploadBudget *upload.Budget
	singleLimit  int64
	presigned    bool
	closed       atomic.Bool
	cleanup      httpcontent.ResponseCleanup
}

// Open validates options and confirms the directory root. A non-nil failed
// result retains response cleanup and must be closed by its creator.
func Open(ctx context.Context, o Options) (*Driver, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if o.Client == nil || (o.RootID != "" && o.RootPath != "") ||
		o.MaxResponseBytes < 0 || o.MaxResponseBytes == math.MaxInt64 || o.MaxResolvePages < 0 ||
		o.MaxUploadBytes < 0 || (o.UploadClient != nil) != (o.MaxUploadBytes > 0) ||
		o.ContentClient != nil && (o.ContentClient == o.Client || o.ContentClient == o.UploadClient) ||
		o.PresignedMultipart && o.UploadClient == nil {
		return nil, core.Fail(core.InvalidRequest, nil)
	}
	if o.RootID != "" && !httpcontent.ValidDecimalID(o.RootID) {
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
	d := &Driver{root: item{ID: "0", meta: core.Metadata{Kind: core.Directory}}, api: httpcontent.IsolatedClient(o.Client),
		content: httpcontent.IsolatedClient(o.ContentClient), limit: o.MaxResponseBytes, resolvePages: o.MaxResolvePages,
		allowMkdir: o.AllowMkdir, allowRemove: o.AllowRemove, directLinks: o.UseDirectLinks, presigned: o.PresignedMultipart}
	d.content.CheckRedirect = d.cleanup.RedirectDownload
	if o.UploadClient != nil {
		budget, err := upload.New(o.MaxUploadBytes, 1)
		if err != nil {
			return nil, core.Fail(core.InvalidRequest, err)
		}
		client := httpcontent.IsolatedClient(o.UploadClient)
		d.uploadClient, d.uploadBudget = &client, budget
		d.singleLimit = min(o.MaxUploadBytes, singleUploadBytes)
	}
	var err error
	if o.RootPath != "" && o.RootPath != "/" {
		d.root, err = d.resolve(ctx, o.RootPath)
	} else if o.RootID != "" && o.RootID != "0" {
		d.root, err = d.info(ctx, o.RootID)
	} else {
		_, _, err = d.readPage(ctx, "0", "", 1)
	}
	if err == nil && d.root.meta.Kind != core.Directory {
		err = core.Fail(core.NotDirectory, nil)
	}
	if err != nil {
		d.closed.Store(true)
	}
	return d, err
}

type nativeID = httpcontent.DecimalID

func (d *Driver) check(ctx context.Context, p core.NativeAddress) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if d.closed.Load() {
		return core.Fail(core.MountUnavailable, nil)
	}
	if p.Namespace() != core.TreeNamespace {
		return core.Fail(core.UnsupportedOperation, nil)
	}
	return core.ValidatePath(p.Value())
}

func nativeFailure(err error, private bool) error {
	if !private {
		return httpcontent.ControlIOError(err)
	}
	if errors.Is(err, httpcontent.ErrContentRedirect) {
		return httpcontent.ErrContentRedirect
	}
	if failure := httpcontent.PrivateIOError(err); failure != httpcontent.ErrPrivateIO {
		return failure
	}
	return errors.New("123 private HTTP I/O failed")
}

func (d *Driver) closeBody(body io.Closer, private bool) error {
	if private {
		return d.cleanup.CloseBody(body)
	}
	return d.cleanup.CloseControlBody(body)
}

// Close retries owned response cleanup after Core has joined active uses.
func (d *Driver) Close() error {
	d.closed.Store(true)
	return d.cleanup.Retry()
}

func (d *Driver) call(ctx context.Context, route string, query url.Values, out any) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if d.closed.Load() {
		return core.Fail(core.MountUnavailable, nil)
	}
	target := endpoint + route
	if len(query) != 0 {
		target += "?" + query.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return core.Fail(core.InvalidRequest, err)
	}
	_, err = d.request(&d.api, req, out)
	return err
}

func controlPost(ctx context.Context, route string, fields any) (*http.Request, error) {
	data, err := json.Marshal(fields)
	if err != nil {
		return nil, core.Fail(core.InvalidRequest, err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint+route, bytes.NewReader(data))
	if err != nil {
		return nil, core.Fail(core.InvalidRequest, err)
	}
	req.GetBody = nil
	req.Header.Set("Content-Type", "application/json")
	return req, nil
}

func (d *Driver) exchange(client *http.Client, req *http.Request, private bool) (*http.Response, error) {
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

// request reports decoded data independently of a subsequent close failure.
func (d *Driver) request(client *http.Client, req *http.Request, out any) (bool, error) {
	decoded, _, err, closeErr := d.requestReply(client, req, out)
	return decoded, errors.Join(err, closeErr)
}

// requestReply distinguishes an unusable receipt from a native rejection.
func (d *Driver) requestReply(client *http.Client, req *http.Request, out any) (decoded, lost bool, err, closeErr error) {
	req.Header.Set("Platform", "open_platform")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Accept-Encoding", "identity")
	private := req.Method == http.MethodPost || req.URL.Path == downloadRoute || req.URL.Path == directLinkRoute
	response, err := d.exchange(client, req, private)
	if err != nil {
		return false, true, err, nil
	}
	defer func() { closeErr = d.closeBody(response.Body, private) }()
	// A gateway failure may hide a successful native commit.
	gateway := response.StatusCode == http.StatusBadGateway || response.StatusCode == http.StatusGatewayTimeout
	lost = response.StatusCode == http.StatusOK || gateway
	if encoding := response.Header.Get("Content-Encoding"); encoding != "" && encoding != "identity" {
		return false, lost, core.Fail(core.InvalidUpstreamResponse, nil), nil
	}
	data, err := io.ReadAll(io.LimitReader(response.Body, d.limit+1))
	err = nativeFailure(err, private)
	if int64(len(data)) > d.limit {
		return false, lost, core.Fail(core.ResponseBudgetExceeded, err), nil
	}
	if err != nil {
		return false, lost, core.Fail(core.UpstreamFailure, err), nil
	}
	var reply struct {
		Code    *int64          `json:"code"`
		Message string          `json:"message"`
		Trace   string          `json:"x-traceID"`
		Data    json.RawMessage `json:"data"`
	}
	decodeErr := json.Unmarshal(data, &reply)
	if response.StatusCode != 200 || (decodeErr == nil && reply.Code != nil && *reply.Code != 0) {
		code := core.UpstreamFailure
		native := int64(0)
		if reply.Code != nil {
			native = *reply.Code
		}
		switch {
		case req.URL.Path == downloadRoute && native == 5113:
			code = core.UpstreamThrottled
		case response.StatusCode == 401 || response.StatusCode == 403 || native == 401 || native == 5113:
			code = core.UpstreamAccessDenied
		case response.StatusCode == 404 || native == 5066:
			code = core.NotFound
		case response.StatusCode == 429 || response.StatusCode == 503 || native == 429:
			code = core.UpstreamThrottled
		}
		if private {
			return false, gateway, core.Fail(code, fmt.Errorf("123 returned HTTP %d, code %d", response.StatusCode, native)), nil
		}
		return false, gateway, core.Fail(code, fmt.Errorf("123 returned HTTP %d, code %d, trace %s: %s", response.StatusCode, native, reply.Trace, reply.Message)), nil
	}
	if decodeErr != nil || reply.Code == nil {
		return false, true, core.Fail(core.InvalidUpstreamResponse, decodeErr), nil
	}
	if out == nil {
		return true, false, nil, nil
	}
	if len(reply.Data) == 0 || string(reply.Data) == "null" {
		return false, true, core.Fail(core.InvalidUpstreamResponse, nil), nil
	}
	if err := json.Unmarshal(reply.Data, out); err != nil {
		return false, true, core.Fail(core.InvalidUpstreamResponse, err), nil
	}
	return true, false, nil, nil
}

func (d *Driver) Stat(ctx context.Context, p core.NativeAddress, selections ...core.ReadSelection) (core.Metadata, error) {
	if len(selections) > 1 {
		return core.Metadata{}, core.Fail(core.InvalidRequest, nil)
	}
	var selection core.ReadSelection
	if len(selections) == 1 {
		selection = selections[0]
	}
	if selection.Version != "" {
		return core.Metadata{}, core.Fail(core.UnsupportedOperation, nil)
	}
	if err := selection.When.ForDomain(""); err != nil {
		return core.Metadata{}, err
	}
	if err := d.check(ctx, p); err != nil {
		return core.Metadata{}, err
	}
	file, err := d.resolve(ctx, p.Value())
	if err != nil {
		return core.Metadata{}, err
	}
	if p.Value() == "/" {
		if file.ID == "0" {
			_, _, err = d.readPage(ctx, "0", "", 1)
		} else {
			file, err = d.info(ctx, string(file.ID))
		}
		if err != nil {
			return core.Metadata{}, err
		}
	}
	meta := file.meta
	if p.Value() == "/" {
		meta.Name = ""
	}
	return meta, selection.Check(meta.Evidence)
}

func (d *Driver) Open(ctx context.Context, p core.NativeAddress, o core.OpenOptions) (core.Opened, error) {
	if err := o.Validate(); err != nil {
		return core.Opened{}, err
	}
	if o.Mode != core.StreamRead || o.NoFollow || o.Selection.Version != "" {
		return core.Opened{}, core.Fail(core.UnsupportedOperation, nil)
	}
	selectionErr := o.Selection.Check(core.Evidence{})
	if selectionErr != nil && core.CodeOf(selectionErr) != core.PreconditionFailed {
		return core.Opened{}, selectionErr
	}
	if err := d.check(ctx, p); err != nil {
		return core.Opened{}, err
	}
	file, err := d.resolve(ctx, p.Value())
	if err != nil {
		return core.Opened{}, err
	}
	if file.meta.Kind != core.File {
		return core.Opened{}, core.Fail(core.NotFile, nil)
	}
	if selectionErr != nil {
		return core.Opened{}, selectionErr
	}
	route, idParam := downloadRoute, "fileId"
	if d.directLinks {
		route, idParam = directLinkRoute, "fileID"
	}
	var capability struct {
		DownloadURL string `json:"downloadUrl"`
		DirectURL   string `json:"url"`
	}
	if err := d.call(ctx, route, url.Values{idParam: {string(file.ID)}}, &capability); err != nil {
		return core.Opened{}, err
	}
	target := capability.DownloadURL
	if d.directLinks {
		target = capability.DirectURL
	}
	u, err := url.Parse(target)
	if err != nil || !httpcontent.IsHTTPS(u) {
		return core.Opened{}, core.Fail(core.InvalidUpstreamResponse, errors.New("invalid content capability"))
	}
	req, err := http.NewRequestWithContext(ctx, "GET", u.String(), nil)
	if err != nil {
		return core.Opened{}, core.Fail(core.InvalidUpstreamResponse, nil)
	}
	req.Header.Set("Accept-Encoding", "identity")
	if o.Range != nil {
		req.Header.Set("Range", httpcontent.Header(o.Range))
	}
	response, err := d.exchange(&d.content, req, true)
	if err != nil {
		return core.Opened{}, err
	}
	if response.StatusCode != http.StatusOK && response.StatusCode != http.StatusPartialContent && response.StatusCode != http.StatusRequestedRangeNotSatisfiable {
		return core.Opened{}, errors.Join(core.Fail(core.UpstreamFailure, fmt.Errorf("123 content returned HTTP %d", response.StatusCode)),
			d.closeBody(response.Body, true))
	}
	opened, err := httpcontent.Open(response, o.Range)
	if o.Range == nil && response.StatusCode == http.StatusRequestedRangeNotSatisfiable {
		err = core.Fail(core.InvalidUpstreamResponse, errors.New("range rejected without a range request"))
	}
	if err != nil {
		return core.Opened{}, errors.Join(err, d.closeBody(response.Body, true))
	}
	opened.Metadata.Name = file.Name
	return opened, nil
}

var _ core.Driver = (*Driver)(nil)
