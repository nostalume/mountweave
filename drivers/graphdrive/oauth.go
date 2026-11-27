package graphdrive

import (
	"cmp"
	"context"
	"errors"
	"math"
	"net/http"
	"net/url"
	"regexp"
	"slices"
	"strings"
	"time"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/internal/httpcontent"
)

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
