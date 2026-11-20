// Package pan115 connects ordinary 115 Open account files to Mountweave.
package pan115

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"mime/multipart"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/internal/httpcontent"
)

const (
	ControlHost   = "proapi.115.com"
	endpoint      = "https://" + ControlHost
	downloadRoute = "/open/ufile/downurl"
)

// Options selects either an account-root-relative RootPath or a native RootID.
// An empty selection means ID 0. Client is a borrowed, authenticated control
// client; its credential owner handles token rotation and atomic persistence.
// ContentClient is independent and must not inject account credentials.
type Options struct {
	RootID, RootPath      string
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
	cleanup      httpcontent.ResponseCleanup
}

// Open validates settings, resolves the root and confirms readable directory
// state. A non-nil result on error retains cleanup and must be closed.
func Open(ctx context.Context, o Options) (*Driver, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if o.Client == nil || o.Client == o.ContentClient || (o.RootID != "" && o.RootPath != "") ||
		o.MaxResponseBytes < 0 || o.MaxResponseBytes == math.MaxInt64 || o.MaxResolvePages < 0 {
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
	d := &Driver{root: item{ID: "0", Category: "0", meta: core.Metadata{Kind: core.Directory}}, api: httpcontent.IsolatedClient(o.Client),
		content: httpcontent.IsolatedClient(o.ContentClient), limit: cmp.Or(o.MaxResponseBytes, 4<<20), resolvePages: o.MaxResolvePages}
	d.content.CheckRedirect = d.cleanup.RedirectDownload
	var err error
	if o.RootPath != "" && o.RootPath != "/" {
		d.root, err = d.resolve(ctx, o.RootPath)
	} else if o.RootID != "" && o.RootID != "0" {
		d.root, err = d.info(ctx, o.RootID)
	}
	if err == nil && d.root.Encrypted != nil && *d.root.Encrypted != 0 {
		err = core.Fail(core.UnsupportedOperation, errors.New("encrypted directory contents are not available"))
	}
	if err == nil && d.root.Category != "0" {
		err = core.Fail(core.NotDirectory, nil)
	}
	if err == nil {
		var p page
		p, err = d.readPage(ctx, string(d.root.ID), 0, 1)
		if err == nil && d.root.ID != "0" && !p.readable && (d.root.Encrypted == nil || *d.root.Encrypted != 0) {
			err = core.Fail(core.InvalidUpstreamResponse, errors.New("directory encryption state was not reported"))
		}
	}
	d.root.Encrypted = nil
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
	return errors.New("115 content I/O failed")
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

const passport = "https://passportapi.115.com/open/"
const scanStatus = "https://qrcodeapi.115.com/get/status/"

// AuthorizationClient decodes a bounded JSON response without redirects or request replay.
type AuthorizationClient func(*http.Request, any) (int, error)

var (
	ErrApplicationRejected = errors.New("115 Open rejected the application")
	ErrRefreshRejected     = errors.New("115 Open rejected the refresh credential")
	ErrScanExpired         = errors.New("The 115 authorization code is no longer valid.")
)

// TokenReceipt reports the credentials in an accepted token reply. A nonempty
// Refresh is authoritative even when Access cannot be used.
type TokenReceipt struct {
	Access, Refresh string
	ExpiresAt       time.Time
}

// Authorize returns tokens and the verified account ID and name. The caller
// supplies the PKCE proof and deadline. present may return a cleanup for the code.
func (send AuthorizationClient) Authorize(ctx context.Context, appID, verifier, challenge string, present func(string) (func(), error)) (TokenReceipt, int64, string, error) {
	if present == nil {
		return TokenReceipt{}, 0, "", core.Fail(core.InvalidRequest, nil)
	}
	var device struct {
		UID  string `json:"uid"`
		Time *int64 `json:"time"`
		Sign string `json:"sign"`
		QR   string `json:"qrcode"`
	}
	if err := send.request(ctx, passport+"authDeviceCode", url.Values{
		"client_id": {appID}, "code_challenge": {challenge}, "code_challenge_method": {"sha256"},
	}, "", &device); err != nil {
		return TokenReceipt{}, 0, "", err
	} else if device.UID == "" || device.Time == nil || device.Sign == "" || device.QR == "" {
		return TokenReceipt{}, 0, "", core.Fail(core.InvalidUpstreamResponse, nil)
	}
	closeQR, err := present(device.QR)
	if err != nil {
		return TokenReceipt{}, 0, "", err
	} else if closeQR != nil {
		defer closeQR()
	}

	poll := scanStatus + "?" + (url.Values{"uid": {device.UID}, "time": {strconv.FormatInt(*device.Time, 10)}, "sign": {device.Sign}}).Encode()
scan:
	for {
		var state struct{ Status *int }
		err := send.request(ctx, poll, nil, "", &state)
		switch {
		case err != nil && !errors.Is(err, context.DeadlineExceeded):
			return TokenReceipt{}, 0, "", err
		case err == nil && state.Status != nil && *state.Status == 2:
			break scan
		case err == nil && (state.Status == nil || *state.Status != 0 && *state.Status != 1):
			return TokenReceipt{}, 0, "", core.Fail(core.InvalidUpstreamResponse, nil)
		}
		select {
		case <-ctx.Done():
			return TokenReceipt{}, 0, "", ctx.Err()
		case <-time.After(time.Second):
		}
	}
	token, err := send.token(ctx, "deviceCodeToToken", url.Values{"uid": {device.UID}, "code_verifier": {verifier}})
	if err != nil {
		return TokenReceipt{}, 0, "", err
	}
	var account struct {
		ID   int64  `json:"user_id"`
		Name string `json:"user_name"`
	}
	if err := send.request(ctx, endpoint+"/open/user/info", nil, token.Access, &account); err != nil {
		return TokenReceipt{}, 0, "", err
	} else if account.ID <= 0 {
		return TokenReceipt{}, 0, "", core.Fail(core.InvalidUpstreamResponse, nil)
	}
	return token, account.ID, account.Name, nil
}

// Refresh attempts a single rotation. The host persists its checkpoint before
// calling and retains a confirmed replacement even if access cannot be used.
func (send AuthorizationClient) Refresh(ctx context.Context, refresh string) (TokenReceipt, error) {
	return send.token(ctx, "refreshToken", url.Values{"refresh_token": {refresh}})
}

func (send AuthorizationClient) token(ctx context.Context, route string, fields url.Values) (TokenReceipt, error) {
	var reply struct {
		Refresh  string `json:"refresh_token"`
		Access   string `json:"access_token"`
		Lifetime int64  `json:"expires_in"`
	}
	if err := send.request(ctx, passport+route, fields, "", &reply); err != nil {
		return TokenReceipt{}, err
	} else if reply.Refresh == "" || reply.Access == "" || reply.Lifetime <= 0 || reply.Lifetime > math.MaxInt64/int64(time.Second) {
		return TokenReceipt{Refresh: reply.Refresh}, core.Fail(core.InvalidUpstreamResponse, nil)
	}
	return TokenReceipt{Access: reply.Access, Refresh: reply.Refresh, ExpiresAt: time.Now().Add(time.Duration(reply.Lifetime) * time.Second)}, nil
}

func (send AuthorizationClient) request(ctx context.Context, target string, fields url.Values, access string, out any) error {
	if send == nil {
		return core.Fail(core.InvalidRequest, nil)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return err
	}
	if fields != nil {
		form := fields.Encode()
		req.Method, req.Body, req.ContentLength = http.MethodPost, io.NopCloser(strings.NewReader(form)), int64(len(form))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	} else if access != "" {
		req.Header.Set("Authorization", "Bearer "+access)
	}
	var reply struct {
		State, Data json.RawMessage
		Code        int
	}
	status, err := send(req, &reply)
	if err != nil {
		return err
	}
	switch reply.Code {
	case 40140100, 40140106, 40140107, 40140108, 40140109, 40140110, 40140118:
		return fmt.Errorf("%w (%d).", ErrApplicationRejected, reply.Code)
	case 40140114, 40140115, 40140116, 40140119, 40140120, 40140137:
		return fmt.Errorf("%w (%d).", ErrRefreshRejected, reply.Code)
	case 40140117:
		return core.Fail(core.UpstreamThrottled, nil)
	}
	switch state := string(reply.State); {
	case status == http.StatusOK && strings.HasPrefix(target, scanStatus+"?") && (state == "0" || state == "false"):
		return ErrScanExpired
	case status != http.StatusOK || reply.Code != 0 || state != "1" && state != "true":
		return core.Fail(core.UpstreamFailure, fmt.Errorf("115 authorization returned HTTP %d and code %d", status, reply.Code))
	case json.Unmarshal(reply.Data, out) != nil:
		return core.Fail(core.InvalidUpstreamResponse, nil)
	}
	return nil
}

type envelope struct {
	State   *bool  `json:"state"`
	Code    int64  `json:"code"`
	Errno   int64  `json:"errno"`
	Message string `json:"message"`
}

func (d *Driver) call(ctx context.Context, method, route string, query url.Values, fields map[string]string, out any) (err error) {
	if err := ctx.Err(); err != nil {
		return err
	}
	if d.closed.Load() {
		return core.Fail(core.MountUnavailable, nil)
	}
	var body bytes.Buffer
	contentType := ""
	if fields != nil {
		writer := multipart.NewWriter(&body)
		for k, v := range fields {
			if err := writer.WriteField(k, v); err != nil {
				return err
			}
		}
		if err := writer.Close(); err != nil {
			return err
		}
		contentType = writer.FormDataContentType()
	}
	target := endpoint + route
	if len(query) != 0 {
		target += "?" + query.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, method, target, &body)
	if err != nil {
		return core.Fail(core.InvalidRequest, err)
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Accept-Encoding", "identity")
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	private := route == downloadRoute
	response, err := d.exchange(&d.api, req, private)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, d.closeBody(response.Body, private)) }()
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
	var status envelope
	decodeErr := json.Unmarshal(data, &status)
	if response.StatusCode != 200 || (decodeErr == nil && status.State != nil && !*status.State) {
		code := core.UpstreamFailure
		switch response.StatusCode {
		case 401, 403:
			code = core.UpstreamAccessDenied
		case 404:
			code = core.NotFound
		case 429, 503:
			code = core.UpstreamThrottled
		}
		detail := fmt.Sprintf("115 returned HTTP %d, code %d, errno %d", response.StatusCode, status.Code, status.Errno)
		if !private {
			detail += ": " + status.Message
		}
		return core.Fail(code, errors.New(detail))
	}
	if decodeErr != nil || status.State == nil || !*status.State {
		return core.Fail(core.InvalidUpstreamResponse, nativeFailure(decodeErr, private))
	}
	if err := json.Unmarshal(data, out); err != nil {
		return core.Fail(core.InvalidUpstreamResponse, nativeFailure(err, private))
	}
	return nil
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
	if p.Value() == "/" && file.ID == "0" {
		if _, err = d.readPage(ctx, "0", 0, 1); err != nil {
			return core.Metadata{}, err
		}
	}
	if p.Value() == "/" && file.ID != "0" {
		file, err = d.info(ctx, string(file.ID))
		if err != nil {
			return core.Metadata{}, err
		}
	}
	meta := file.meta
	if p.Value() == "/" {
		meta.Name = ""
	}
	if err == nil {
		err = selection.Check(meta.Evidence)
	}
	return meta, err
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
	if file.Encrypted != nil && *file.Encrypted != 0 {
		return core.Opened{}, core.Fail(core.UnsupportedOperation, errors.New("encrypted file contents are not available"))
	}
	if file.Category != "1" {
		return core.Opened{}, core.Fail(core.NotFile, nil)
	}
	if selectionErr != nil {
		return core.Opened{}, selectionErr
	}
	if file.Pick == "" {
		return core.Opened{}, core.Fail(core.InvalidUpstreamResponse, errors.New("file omitted download capability reference"))
	}
	var out struct {
		Data map[string]struct {
			Size *int64 `json:"file_size"`
			URL  struct {
				URL string `json:"url"`
			} `json:"url"`
		} `json:"data"`
	}
	if err = d.call(ctx, "POST", downloadRoute, nil, map[string]string{"pick_code": file.Pick}, &out); err != nil {
		return core.Opened{}, err
	}
	capability, ok := out.Data[string(file.ID)]
	if !ok || (capability.Size != nil && *capability.Size < 0) {
		return core.Opened{}, core.Fail(core.InvalidUpstreamResponse, errors.New("download receipt differs from requested file"))
	}
	u, err := url.Parse(capability.URL.URL)
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
	var opened core.Opened
	status := response.StatusCode
	switch {
	case status == http.StatusRequestedRangeNotSatisfiable && o.Range == nil:
		err = core.Fail(core.InvalidUpstreamResponse, errors.New("range rejected without a range request"))
	case status == http.StatusOK || status == http.StatusPartialContent || status == http.StatusRequestedRangeNotSatisfiable:
		if capability.Size == nil {
			opened, err = httpcontent.Open(response, o.Range)
		} else {
			opened, err = httpcontent.OpenSized(response, o.Range, *capability.Size)
		}
	default:
		err = core.Fail(core.UpstreamFailure, fmt.Errorf("115 content returned HTTP %d", status))
	}
	if err != nil {
		return core.Opened{}, errors.Join(err, d.closeBody(response.Body, true))
	}
	opened.Metadata.Name = file.Name
	return opened, nil
}

var _ core.Driver = (*Driver)(nil)
