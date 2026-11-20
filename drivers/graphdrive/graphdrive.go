// Package graphdrive adapts a prepared Microsoft Graph connection to Mountweave.
package graphdrive

import (
	"cmp"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/internal/httpcontent"
	"github.com/nostalume/mountweave/drivers/internal/upload"
)

const Endpoint = "https://graph.microsoft.com"

var (
	ErrDeviceExpired       = errors.New("Microsoft device code expired")
	ErrSignInRequired      = errors.New("Microsoft Entra requires sign-in")
	ErrApplicationSettings = errors.New("invalid Microsoft Entra application settings")
	ErrScopeMissing        = errors.New("Microsoft Graph grant is missing a required scope")
	ErrAccountChanged      = errors.New("Microsoft Graph returned a different tenant account")
)

var entraUUID = regexp.MustCompile("^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")

// DeviceApp selects a public Microsoft Entra client and one tenant.
type DeviceApp struct {
	ClientID, Tenant string
}

// Normalize validates the application and selects a canonical tenant.
func (a DeviceApp) Normalize() (DeviceApp, error) {
	a.ClientID, a.Tenant = strings.ToLower(a.ClientID), strings.ToLower(a.Tenant)
	if a.Tenant == "9188040d-6c67-4c5b-b112-36a304b66dad" {
		a.Tenant = "consumers"
	}
	if !validEntraUUID(a.ClientID) || a.Tenant != "consumers" && !validEntraUUID(a.Tenant) {
		return DeviceApp{}, ErrApplicationSettings
	}
	return a, nil
}

func validEntraUUID(id string) bool {
	return entraUUID.MatchString(id) && id != "00000000-0000-0000-0000-000000000000"
}

// AuthorizationScopes are the delegated permissions for the built-in read workflow.
func AuthorizationScopes() []string { return []string{"Files.Read", "User.Read"} }

// Grant binds tokens to a verified account. The host publishes or persists it.
type Grant struct {
	Account, Label, Access, Refresh string
	Expires                         time.Time
}

type tokenReply struct {
	Access   string `json:"access_token"`
	Refresh  string `json:"refresh_token"`
	Scope    string `json:"scope"`
	Type     string `json:"token_type"`
	Lifetime int64  `json:"expires_in"`
	Error    string `json:"error"`
}

// Exchange takes a presenter for initial authorization, or the account and
// refresh credential for renewal.
// request sends the given HTTP request without redirects and bounds and decodes
// its JSON response. It must respect cancellation and protect credential data.
func (a DeviceApp) Exchange(ctx context.Context, scopes []string, prior Grant, request func(*http.Request, any) (int, error), present func(string, string) error) (Grant, error) {
	app, err := a.Normalize()
	if err != nil {
		return Grant{}, err
	}
	origin := "https://login.microsoftonline.com/" + app.Tenant + "/oauth2/v2.0/"
	post := func(ctx context.Context, route string, form url.Values, result any) (int, error) {
		form.Set("client_id", app.ClientID)
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, origin+route, strings.NewReader(form.Encode()))
		if err != nil {
			return 0, err
		}
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		return request(req, result)
	}
	if request == nil || (present == nil) != (prior.Refresh != "") || (prior.Account == "") != (prior.Refresh == "") {
		return Grant{}, core.Fail(core.InvalidRequest, nil)
	}
	scope := strings.Join(scopes, " ") + " offline_access"
	var token tokenReply
	if present != nil {
		err = authorizeDevice(ctx, scope, post, present, &token)
	} else {
		status, e := post(ctx, "token", url.Values{
			"grant_type": {"refresh_token"}, "refresh_token": {prior.Refresh}, "scope": {scope},
		}, &token)
		err = authorizationResponse(status, token.Error, e)
	}
	if err != nil {
		return Grant{}, err
	}
	return app.bind(ctx, scopes, prior, token, request)
}

func (a DeviceApp) bind(ctx context.Context, scopes []string, prior Grant, token tokenReply, request func(*http.Request, any) (int, error)) (Grant, error) {
	refresh := cmp.Or(token.Refresh, prior.Refresh)
	if token.Access == "" || refresh == "" || !strings.EqualFold(token.Type, "bearer") ||
		token.Lifetime <= 0 || token.Lifetime > int64(math.MaxInt64)/int64(time.Second) {
		return Grant{}, core.Fail(core.InvalidUpstreamResponse, nil)
	}
	granted := strings.Fields(token.Scope)
	for _, scope := range scopes {
		if !slices.ContainsFunc(granted, func(field string) bool {
			return strings.EqualFold(field, scope) || strings.EqualFold(field, Endpoint+"/"+scope)
		}) {
			return Grant{}, ErrScopeMissing
		}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, Endpoint+"/v1.0/me?$select=id,displayName", nil)
	if err != nil {
		return Grant{}, err
	}
	req.Header.Set("Authorization", "Bearer "+token.Access)
	var me struct{ ID, DisplayName string }
	status, err := request(req, &me)
	if err := authorizationResponse(status, "", err); err != nil {
		return Grant{}, err
	}
	if me.ID == "" {
		return Grant{}, core.Fail(core.InvalidUpstreamResponse, nil)
	}
	account := a.Tenant + "/" + me.ID
	if prior.Account != "" && prior.Account != account {
		return Grant{}, ErrAccountChanged
	}
	return Grant{Account: account, Label: cmp.Or(me.DisplayName, me.ID) + " (" + account + ")",
		Access: token.Access, Refresh: refresh, Expires: time.Now().Add(time.Duration(token.Lifetime) * time.Second)}, nil
}

func authorizeDevice(ctx context.Context, scope string, post func(context.Context, string, url.Values, any) (int, error), present func(string, string) error, token *tokenReply) error {
	var device struct {
		DeviceCode   string `json:"device_code"`
		UserCode     string `json:"user_code"`
		Verification string `json:"verification_uri"`
		Expires      int64  `json:"expires_in"`
		Interval     int64  `json:"interval"`
		Error        string `json:"error"`
	}
	start := time.Now()
	status, err := post(ctx, "devicecode", url.Values{"scope": {scope}}, &device)
	if err := authorizationResponse(status, device.Error, err); err != nil {
		return err
	}
	verify, err := url.Parse(device.Verification)
	hosts := []string{"microsoft.com", "www.microsoft.com", "login.microsoftonline.com", "aka.ms"}
	if err != nil || !httpcontent.IsHTTPS(verify) || !slices.Contains(hosts, strings.ToLower(verify.Host)) ||
		verify.RawQuery != "" || verify.ForceQuery || device.DeviceCode == "" || device.UserCode == "" ||
		device.Expires <= 0 || device.Expires > int64(math.MaxInt64)/int64(time.Second) || device.Interval < 0 {
		return core.Fail(core.InvalidUpstreamResponse, nil)
	}
	ctx, cancel := context.WithDeadlineCause(ctx, start.Add(time.Duration(device.Expires)*time.Second), ErrDeviceExpired)
	defer cancel()
	if err := context.Cause(ctx); err != nil {
		return err
	}
	if err := present(verify.String(), device.UserCode); err != nil {
		return err
	}
	deadline, _ := ctx.Deadline()
	interval := cmp.Or(device.Interval, 5)
	form := url.Values{"grant_type": {"urn:ietf:params:oauth:grant-type:device_code"}, "device_code": {device.DeviceCode}}
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		timer.Reset(time.Duration(min(interval, device.Expires)) * time.Second)
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-timer.C:
		}
		if !time.Now().Before(deadline) {
			<-ctx.Done()
			return context.Cause(ctx)
		}
		*token = tokenReply{}
		status, err := post(ctx, "token", form, token)
		if cause := context.Cause(ctx); cause != nil {
			return cause
		}
		switch {
		case errors.Is(err, context.DeadlineExceeded):
			interval = min(interval, device.Expires) * 2
		case err != nil:
			return err
		case status == http.StatusBadRequest && token.Error == "authorization_pending":
		case status == http.StatusBadRequest && token.Error == "slow_down":
			interval = min(interval, device.Expires) + 5
		default:
			return authorizationResponse(status, token.Error, nil)
		}
	}
}

func authorizationResponse(status int, code string, err error) error {
	if err != nil || status == http.StatusOK {
		return err
	}
	switch code {
	case "access_denied", "authorization_declined":
		return context.Canceled
	case "expired_token", "bad_verification_code":
		return ErrDeviceExpired
	case "invalid_grant", "interaction_required", "consent_required":
		return ErrSignInRequired
	case "invalid_client", "unauthorized_client", "invalid_scope", "invalid_request":
		return core.Fail(core.InvalidRequest, nil)
	}
	switch status {
	case http.StatusUnauthorized, http.StatusForbidden, http.StatusTooManyRequests:
		return statusError(status)
	}
	return core.Fail(core.UpstreamFailure, nil)
}

// Options borrows an authenticated APIClient and an unauthenticated ContentClient.
// The host owns their transports, credential providers and request replay policy.
type Options struct {
	APIClient        *http.Client  `json:"-"`
	ContentClient    *http.Client  `json:"-"`
	Endpoint         string        `json:"endpoint,omitempty"`
	DriveID          string        `json:"drive_id"`
	RootID           string        `json:"root_id,omitempty"`
	RootPath         string        `json:"root_path,omitempty"`
	ShareURL         string        `json:"-"`
	ChunkSize        int64         `json:"chunk_size,omitempty"`
	MaxBufferedBytes int64         `json:"max_buffered_bytes,omitempty"`
	MaxResponseBytes int64         `json:"max_response_bytes,omitempty"`
	RangeAttempts    int           `json:"range_attempts,omitempty"`
	CleanupTimeout   time.Duration `json:"cleanup_timeout,omitempty"`
}

// Validate checks static settings without using clients or acquiring resources.
func (o Options) Validate() error {
	if o.ShareURL != "" {
		u, err := url.Parse(o.ShareURL)
		if err != nil || !utf8.ValidString(o.ShareURL) || u.Scheme != "https" || u.Hostname() == "" ||
			u.User != nil || u.Opaque != "" || u.Fragment != "" || o.DriveID != "" || o.RootID != "" || o.RootPath != "" ||
			o.ChunkSize != 0 || o.MaxBufferedBytes != 0 || o.MaxResponseBytes == math.MaxInt64 {
			return core.Fail(core.InvalidRequest, errors.New("a share URL selects one already accessible, read-only root"))
		}
	} else if o.DriveID == "" || !utf8.ValidString(o.DriveID) || !utf8.ValidString(o.RootID) ||
		(o.RootID == "") == (o.RootPath == "") {
		return core.Fail(core.InvalidRequest, errors.New("a drive and exactly one root ID or root path are required"))
	}
	if o.RootPath != "" {
		if err := core.ValidatePath(o.RootPath); err != nil {
			return err
		}
	}
	if o.Endpoint != "" {
		u, err := url.Parse(o.Endpoint)
		if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
			return core.Fail(core.InvalidRequest, errors.New("an HTTPS API origin is required"))
		}
	}
	if o.MaxResponseBytes < 0 || o.RangeAttempts < 0 || o.CleanupTimeout < 0 ||
		o.ChunkSize < 0 || o.MaxBufferedBytes < 0 || (o.ChunkSize == 0) != (o.MaxBufferedBytes == 0) {
		return core.Fail(core.InvalidRequest, nil)
	}
	if o.ChunkSize != 0 {
		if o.ChunkSize%327680 != 0 || o.ChunkSize >= 60<<20 {
			return core.Fail(core.InvalidRequest, errors.New("chunk size must be a multiple of 320 KiB below 60 MiB"))
		}
		return upload.Validate(o.ChunkSize, o.MaxBufferedBytes, 0)
	}
	return nil
}

type Driver struct {
	opts             Options
	api, content     *http.Client
	root             item
	readOnly         bool
	buffer           *upload.Budget
	closed           atomic.Bool
	nextUpload       atomic.Uint64
	mu               sync.Mutex
	failedBodies     []io.Closer
	privateResponses httpcontent.ResponseCleanup
	pending          map[uint64]pendingUpload
}

type item struct {
	ID       string     `json:"id"`
	Name     string     `json:"name"`
	ETag     string     `json:"eTag"`
	Size     *int64     `json:"size"`
	Modified *time.Time `json:"lastModifiedDateTime"`
	File     *struct{}  `json:"file"`
	Folder   *struct{}  `json:"folder"`
	Remote   *struct{}  `json:"remoteItem"`
	Package  *struct{}  `json:"package"`
	Parent   *struct {
		ID      string `json:"id"`
		DriveID string `json:"driveId"`
	} `json:"parentReference"`
	DownloadURL string `json:"@microsoft.graph.downloadUrl"`
}

func (f item) metadata() (core.Metadata, error) {
	if f.Remote != nil || f.Package != nil {
		return core.Metadata{}, core.Fail(core.UnsupportedOperation, nil)
	}
	if f.ID == "" || (f.File == nil) == (f.Folder == nil) || (f.Size != nil && *f.Size < 0) {
		return core.Metadata{}, core.Fail(core.InvalidUpstreamResponse, nil)
	}
	meta := core.Metadata{Name: f.Name, Kind: core.Directory, ModifiedAt: f.Modified}
	if f.File != nil {
		meta.Kind, meta.Size = core.File, f.Size
	}
	return meta, nil
}

// Open retrieves and binds an actual directory ID. A non-nil failed Driver
// retains failed response cleanup and must be closed; clients remain borrowed.
func Open(ctx context.Context, opts Options) (*Driver, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := opts.Validate(); err != nil {
		return nil, err
	}
	if opts.APIClient == nil || opts.ContentClient == nil {
		return nil, errors.New("prepared API and content clients are required")
	}
	if opts.Endpoint == "" {
		opts.Endpoint = Endpoint
	}
	opts.Endpoint = strings.TrimSuffix(opts.Endpoint, "/")
	if opts.RangeAttempts == 0 {
		opts.RangeAttempts = 1
	}
	shareURL := opts.ShareURL
	opts.ShareURL = ""
	d := &Driver{opts: opts, api: isolatedClient(opts.APIClient), content: isolatedClient(opts.ContentClient),
		readOnly: shareURL != "", pending: make(map[uint64]pendingUpload)}
	if shareURL != "" {
		if d.opts.MaxResponseBytes == 0 {
			d.opts.MaxResponseBytes = 4 << 20
		}
		encoded := "u!" + base64.RawURLEncoding.EncodeToString([]byte(shareURL))
		query := url.Values{"$select": {"id,name,folder,file,parentReference,remoteItem,package"}}
		shared, err := d.getItem(ctx, "/shares/"+encoded+"/driveItem?"+query.Encode())
		if err == nil && shared.Folder == nil {
			err = core.Fail(core.NotDirectory, nil)
		}
		if err == nil && (shared.Parent == nil || !core.ValidName(shared.Parent.DriveID)) {
			err = core.Fail(core.InvalidUpstreamResponse, errors.New("Graph did not identify the shared folder's drive"))
		}
		if err != nil {
			d.closed.Store(true)
			return d, err
		}
		d.opts.DriveID, d.opts.RootID = shared.Parent.DriveID, shared.ID
	}
	if opts.ChunkSize != 0 {
		var err error
		d.buffer, err = upload.New(opts.MaxBufferedBytes, int(min(opts.MaxBufferedBytes/opts.ChunkSize, int64(math.MaxInt))))
		if err != nil {
			return nil, err
		}
	}
	route := "/drives/" + url.PathEscape(d.opts.DriveID) + "/root"
	if d.opts.RootID != "" {
		route = "/drives/" + url.PathEscape(d.opts.DriveID) + "/items/" + url.PathEscape(d.opts.RootID)
	}
	root, err := d.getItem(ctx, route)
	if err == nil && d.opts.RootID != "" && root.ID != d.opts.RootID {
		err = core.Fail(core.InvalidUpstreamResponse, nil)
	}
	if err == nil && root.Folder == nil {
		err = core.Fail(core.NotDirectory, nil)
	}
	if err == nil {
		d.root = root
		if opts.RootPath != "" {
			root, err = d.resolve(ctx, opts.RootPath)
		}
	}
	if err == nil && root.Folder == nil {
		err = core.Fail(core.NotDirectory, nil)
	}
	if err == nil {
		err = ctx.Err()
	}
	if err != nil {
		d.closed.Store(true)
		return d, err
	}
	d.root = root
	return d, nil
}

func isolatedClient(client *http.Client) *http.Client {
	copy := *client
	copy.Jar = nil
	copy.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &copy
}

func (d *Driver) apiURL(route string) string { return d.opts.Endpoint + "/v1.0" + route }

func (d *Driver) apiRequest(ctx context.Context, method, target string, body io.Reader, contentType string, length int64) (*http.Response, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if d.closed.Load() {
		return nil, core.Fail(core.MountUnavailable, nil)
	}
	u, err := url.Parse(target)
	origin, _ := url.Parse(d.opts.Endpoint)
	if err != nil || u.Scheme != "https" || !strings.EqualFold(u.Host, origin.Host) || u.User != nil || u.Fragment != "" || !strings.HasPrefix(u.Path, "/v1.0/") {
		return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("invalid Graph request target"))
	}
	req, err := http.NewRequestWithContext(ctx, method, target, body)
	if err != nil {
		return nil, core.Fail(core.InvalidRequest, err)
	}
	req.Header.Set("Accept", "application/json")
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	req.ContentLength = length
	if length == 0 {
		req.Body = http.NoBody
	}
	response, err := d.api.Do(req)
	if err != nil {
		failure := networkError(err)
		if d.readOnly {
			failure = privateFailure(failure)
		}
		return nil, failure
	}
	return response, nil
}

func (d *Driver) decode(response *http.Response, out any) (decoded bool, err error) {
	defer func() { err = errors.Join(err, d.closeBody(response.Body)) }()
	if d.readOnly {
		err := httpcontent.DecodeJSON(response, d.opts.MaxResponseBytes, out)
		if err != nil && core.CodeOf(err) == core.InvalidUpstreamResponse {
			err = core.Fail(core.InvalidUpstreamResponse, nil)
		}
		return err == nil, err
	}
	return decodeJSON(response.Body, d.opts.MaxResponseBytes, out)
}

func decodeJSON(body io.Reader, max int64, out any) (bool, error) {
	var limited *io.LimitedReader
	if max > 0 && max < math.MaxInt64 {
		limited = &io.LimitedReader{R: body, N: max + 1}
		body = limited
	}
	decoder := json.NewDecoder(body)
	err := decoder.Decode(out)
	if err == nil {
		var extra any
		if tail := decoder.Decode(&extra); tail != io.EOF {
			err = errors.Join(errors.New("unexpected trailing Graph response data"), tail)
		}
	}
	if limited != nil && limited.N == 0 {
		return false, core.Fail(core.ResponseBudgetExceeded, err)
	}
	if err != nil {
		return false, core.Fail(core.InvalidUpstreamResponse, err)
	}
	return true, nil
}

func privateFailure(err error) error {
	return core.Fail(core.CodeOf(err), httpcontent.PrivateIOError(err))
}

func networkError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	var timeout net.Error
	if errors.As(err, &timeout) && timeout.Timeout() {
		return core.Fail(core.UpstreamTimeout, err)
	}
	return core.Fail(core.UpstreamFailure, err)
}

// responseFailure keeps a recoverable remote failure separate from cleanup,
// which cannot be erased by successfully retrying the native operation.
func (d *Driver) responseFailure(response *http.Response) (failure, cleanup error) {
	if d.readOnly {
		return statusError(response.StatusCode), d.closeBody(response.Body)
	}
	var native struct {
		Error struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	_, decodeErr := decodeJSON(response.Body, d.opts.MaxResponseBytes, &native)
	var cause error
	if native.Error.Code != "" || native.Error.Message != "" {
		cause = fmt.Errorf("Graph %s: %s", native.Error.Code, native.Error.Message)
	}
	return errors.Join(statusError(response.StatusCode), cause, decodeErr), d.closeBody(response.Body)
}

func statusError(status int) error {
	code := core.UpstreamFailure
	switch status {
	case 401, 403:
		code = core.UpstreamAccessDenied
	case 404, 410:
		code = core.NotFound
	case 409:
		code = core.AlreadyExists
	case 412:
		code = core.PreconditionFailed
	case 408, 504:
		code = core.UpstreamTimeout
	case 429:
		code = core.UpstreamThrottled
	}
	return core.Fail(code, fmt.Errorf("Graph HTTP status %d", status))
}
