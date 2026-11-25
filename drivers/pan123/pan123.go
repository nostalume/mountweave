// Package pan123 connects 123 Open and website account files to Mountweave.
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
	"time"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/internal/httpcontent"
	"github.com/nostalume/mountweave/drivers/internal/upload"
)

const (
	// ControlHost is the only recipient of 123 Open control credentials.
	ControlHost     = "open-api.123pan.com"
	endpoint        = "https://" + ControlHost
	tokenRoute      = "/api/v1/access_token"
	accountRoute    = "/api/v1/user/info"
	downloadRoute   = "/api/v1/file/download_info"
	directLinkRoute = "/api/v1/direct-link/url"
)

// Authorization is a short-lived token bound to a verified account.
type Authorization struct {
	AccessToken   string    `json:"accessToken"`
	ExpiresAt     time.Time `json:"expiredAt"`
	UID, Nickname string    `json:"-"`
}

// Authorize issues a token and verifies its UID. Leave expectedUID empty for
// initial authorization. A mismatch returns core.PreconditionFailed. The client
// is borrowed without its cookies or redirects.
func Authorize(ctx context.Context, client *http.Client, clientID, clientSecret, expectedUID string) (_ Authorization, err error) {
	if err := ctx.Err(); err != nil {
		return Authorization{}, err
	}
	if clientID == "" || clientSecret == "" {
		return Authorization{}, core.Fail(core.InvalidRequest, nil)
	}
	req, err := controlPost(ctx, tokenRoute, struct {
		ID     string `json:"clientID"`
		Secret string `json:"clientSecret"`
	}{clientID, clientSecret})
	if err != nil {
		return Authorization{}, err
	}
	httpClient := httpcontent.IsolatedClient(client)
	if httpClient.Timeout == 0 {
		httpClient.Timeout = 30 * time.Second
	}
	reader := responseReader{limit: 64 << 10}
	defer func() { err = errors.Join(err, reader.cleanup.Retry()) }()
	var grant Authorization
	if _, err := reader.request(&httpClient, req, &grant); err != nil || ctx.Err() != nil {
		return Authorization{}, errors.Join(err, ctx.Err())
	}
	if grant.AccessToken == "" || !time.Now().Before(grant.ExpiresAt) {
		return Authorization{}, core.Fail(core.InvalidUpstreamResponse, nil)
	}
	req, err = http.NewRequestWithContext(ctx, http.MethodGet, endpoint+accountRoute, nil)
	if err != nil {
		return Authorization{}, err
	}
	req.Header.Set("Authorization", "Bearer "+grant.AccessToken)
	var account struct {
		UID      nativeID `json:"uid"`
		Nickname string   `json:"nickname"`
	}
	if _, err := reader.request(&httpClient, req, &account); err != nil || ctx.Err() != nil {
		return Authorization{}, errors.Join(err, ctx.Err())
	}
	if account.UID == "" || account.UID == "0" {
		return Authorization{}, core.Fail(core.InvalidUpstreamResponse, nil)
	}
	grant.UID, grant.Nickname = string(account.UID), account.Nickname
	if expectedUID != "" && grant.UID != expectedUID {
		return Authorization{}, core.Fail(core.PreconditionFailed, errors.New("123 Open account does not match the saved UID"))
	}
	return grant, nil
}

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
	*reader
	api          http.Client
	allowMkdir   bool
	allowRemove  bool
	directLinks  bool
	uploadClient *http.Client
	uploadBudget *upload.Budget
	singleLimit  int64
	presigned    bool
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
	d := &Driver{reader: newReader(o.ContentClient, o.MaxResolvePages), api: httpcontent.IsolatedClient(o.Client),
		allowMkdir: o.AllowMkdir, allowRemove: o.AllowRemove, directLinks: o.UseDirectLinks, presigned: o.PresignedMultipart}
	d.limit = o.MaxResponseBytes
	d.page, d.verifyRoot, d.grant = d.readPage, d.statRoot, d.download
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

type responseReader struct {
	limit   int64
	cleanup httpcontent.ResponseCleanup
}

func (d *responseReader) closeBody(body io.Closer, private bool) error {
	if private {
		return d.cleanup.CloseBody(body)
	}
	return d.cleanup.CloseControlBody(body)
}

// Close retries owned response cleanup after Core has joined active uses.
func (d *reader) Close() error {
	d.closed.Store(true)
	return d.cleanup.Retry()
}

func (d *Driver) call(ctx context.Context, route string, query url.Values, out any) error {
	if err := d.active(ctx); err != nil {
		return err
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

func (d *responseReader) exchange(client *http.Client, req *http.Request, private bool) (*http.Response, error) {
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
func (d *responseReader) request(client *http.Client, req *http.Request, out any) (bool, error) {
	decoded, _, err, closeErr := d.requestReply(client, req, out)
	return decoded, errors.Join(err, closeErr)
}

// requestReply distinguishes an unusable receipt from a native rejection.
func (d *responseReader) requestReply(client *http.Client, req *http.Request, out any) (decoded, lost bool, err, closeErr error) {
	req.Header.Set("Platform", "open_platform")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Accept-Encoding", "identity")
	authorization := req.URL.Path == tokenRoute || req.URL.Path == accountRoute
	private := authorization || req.Method == http.MethodPost || req.URL.Path == downloadRoute || req.URL.Path == directLinkRoute
	response, err := d.exchange(client, req, private)
	if err != nil {
		return false, true, err, nil
	}
	defer func() { closeErr = d.closeBody(response.Body, private) }()
	// A gateway failure may hide a successful native commit.
	gateway := response.StatusCode == http.StatusBadGateway || response.StatusCode == http.StatusGatewayTimeout
	lost = response.StatusCode == http.StatusOK || gateway
	var reply struct {
		Code    *int64          `json:"code"`
		Message string          `json:"message"`
		Trace   string          `json:"x-traceID"`
		Data    json.RawMessage `json:"data"`
	}
	if !authorization || response.StatusCode == http.StatusOK {
		if encoding := response.Header.Get("Content-Encoding"); encoding != "" && encoding != "identity" {
			return false, lost, core.Fail(core.InvalidUpstreamResponse, nil), nil
		}
		data, readErr := io.ReadAll(io.LimitReader(response.Body, d.limit+1))
		if authorization {
			defer clear(data)
			defer func() { clear(reply.Data) }()
		}
		err = nativeFailure(readErr, private)
		if int64(len(data)) > d.limit {
			return false, lost, core.Fail(core.ResponseBudgetExceeded, err), nil
		}
		if err != nil {
			return false, lost, core.Fail(core.UpstreamFailure, err), nil
		}
		err = json.Unmarshal(data, &reply)
	}
	if response.StatusCode != 200 || (err == nil && reply.Code != nil && *reply.Code != 0) {
		code := core.UpstreamFailure
		native := int64(0)
		if reply.Code != nil {
			native = *reply.Code
		}
		switch {
		case req.URL.Path == downloadRoute && native == 5113:
			code = core.UpstreamThrottled
		case response.StatusCode == 401 || response.StatusCode == 403 || !authorization && (native == 401 || native == 5113):
			code = core.UpstreamAccessDenied
		case !authorization && (response.StatusCode == 404 || native == 5066):
			code = core.NotFound
		case response.StatusCode == 429 || !authorization && (response.StatusCode == 503 || native == 429):
			code = core.UpstreamThrottled
		}
		if private {
			return false, gateway, core.Fail(code, fmt.Errorf("123 returned HTTP %d, code %d", response.StatusCode, native)), nil
		}
		return false, gateway, core.Fail(code, fmt.Errorf("123 returned HTTP %d, code %d, trace %s: %s", response.StatusCode, native, reply.Trace, reply.Message)), nil
	}
	if err == nil && reply.Code != nil {
		if out == nil {
			return true, false, nil, nil
		}
		if len(reply.Data) != 0 && string(reply.Data) != "null" {
			if err = json.Unmarshal(reply.Data, out); err == nil {
				return true, false, nil, nil
			}
		}
	}
	if authorization {
		err = nil
	}
	return false, true, core.Fail(core.InvalidUpstreamResponse, err), nil
}

func (d *reader) Stat(ctx context.Context, p core.NativeAddress, selections ...core.ReadSelection) (core.Metadata, error) {
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
		file, err = d.verifyRoot(ctx)
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

func (d *reader) Open(ctx context.Context, p core.NativeAddress, o core.OpenOptions) (core.Opened, error) {
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
	target, size, err := d.grant(ctx, file)
	if err != nil {
		return core.Opened{}, err
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
	if size != nil {
		return d.cleanup.OpenSizedBody(response, o.Range, file.Name, *size)
	}
	return d.cleanup.OpenBody(response, o.Range, file.Name)
}

var _ core.Driver = (*Driver)(nil)
