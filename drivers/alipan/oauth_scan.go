package alipan

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/alipan/credential"
)

var (
	ErrAuthorizationRejected     = errors.New("Aliyun Drive rejected the application or authorization")
	ErrAuthorizationExpired      = errors.New("the Aliyun Drive authorization code expired")
	ErrAuthorizationQR           = errors.New("Aliyun Drive did not return a usable QR code")
	ErrAuthorizationNotRenewable = errors.New("Aliyun Drive did not issue a renewable authorization")
	ErrAuthorizationScope        = errors.New("Aliyun Drive authorization does not cover the requested account or scope")
)

const maxScanResponseBytes = 64 << 10

// Authorization is the verified result of one scan.
type Authorization struct {
	Token  credential.Token
	Scopes []string
	Drives DriveInfo
}

// DriveInfo describes the drives belonging to the verified account.
type DriveInfo struct {
	Account  string `json:"user_id"`
	Name     string `json:"name"`
	Default  string `json:"default_drive_id"`
	Resource string `json:"resource_drive_id"`
	Backup   string `json:"backup_drive_id"`
}

func (d DriveInfo) HasDrive(id string) bool {
	return id != "" && (id == d.Default || id == d.Resource || id == d.Backup)
}

// Authorize scans for consent and verifies the granted scopes and account.
// The caller bounds the attempt with ctx and may return a presentation cleanup.
func (o OAuth) Authorize(ctx context.Context, scopes []string, present func(string) (func(), error)) (Authorization, error) {
	if o.ClientID == "" || present == nil {
		return Authorization{}, core.Fail(core.InvalidRequest, nil)
	}
	o, err := o.prepare()
	if err != nil {
		return Authorization{}, err
	}
	var nonce [32]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return Authorization{}, err
	}
	verifier := base64.RawURLEncoding.EncodeToString(nonce[:])
	hash := sha256.Sum256([]byte(verifier))
	var device struct {
		Code string `json:"qrCodeUrl"`
		ID   string `json:"sid"`
	}
	fields := map[string]any{
		"client_id": o.ClientID, "scopes": scopes,
		"code_challenge": base64.RawURLEncoding.EncodeToString(hash[:]), "code_challenge_method": "S256",
	}
	if err := o.scanJSON(ctx, http.MethodPost, "/oauth/authorize/qrcode", fields, "", &device); err != nil {
		return Authorization{}, err
	}
	if !validAuthorizationLink(device.Code) || !validAuthorizationSession(device.ID) {
		return Authorization{}, core.Fail(core.InvalidUpstreamResponse, ErrAuthorizationQR)
	}
	if err := ctx.Err(); err != nil {
		return Authorization{}, err
	}
	release, err := present(device.Code)
	if release != nil {
		defer release()
	}
	if err != nil {
		return Authorization{}, err
	}
	path := "/oauth/qrcode/" + device.ID + "/status"
	for {
		var state struct {
			Status string `json:"status"`
			Code   string `json:"authCode"`
		}
		if err := o.scanJSON(ctx, http.MethodGet, path, nil, "", &state); err != nil {
			return Authorization{}, err
		}
		switch state.Status {
		case "LoginSuccess":
			if state.Code == "" {
				return Authorization{}, core.Fail(core.InvalidUpstreamResponse, nil)
			}
			return o.confirmAuthorization(ctx, scopes, state.Code, verifier)
		case "WaitLogin", "ScanSuccess":
		case "QRCodeExpired":
			return Authorization{}, ErrAuthorizationExpired
		default:
			return Authorization{}, core.Fail(core.InvalidUpstreamResponse, nil)
		}
		select {
		case <-ctx.Done():
			return Authorization{}, ctx.Err()
		case <-time.After(time.Second):
		}
	}
}

func validAuthorizationLink(raw string) bool {
	link, err := url.Parse(raw)
	if err != nil || link.Scheme != "https" || link.User != nil || (link.Port() != "" && link.Port() != "443") {
		return false
	}
	host := strings.ToLower(link.Hostname())
	return host == "alipan.com" || strings.HasSuffix(host, ".alipan.com") ||
		host == "aliyundrive.com" || strings.HasSuffix(host, ".aliyundrive.com")
}

func validAuthorizationSession(id string) bool {
	return id != "" && len(id) <= 256 && strings.IndexFunc(id, func(ch rune) bool {
		return !(ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z' || ch >= '0' && ch <= '9' || ch == '-' || ch == '_')
	}) < 0
}

func (o OAuth) confirmAuthorization(ctx context.Context, scopes []string, code, verifier string) (Authorization, error) {
	token, err := o.exchange(ctx, map[string]string{
		"client_id": o.ClientID, "grant_type": "authorization_code", "code": code, "code_verifier": verifier,
	}, true)
	if err != nil {
		return Authorization{}, err
	}
	var permissions struct {
		Account string `json:"id"`
		Scopes  []struct {
			Scope string `json:"scope"`
		} `json:"scopes"`
	}
	if err := o.scanJSON(ctx, http.MethodGet, "/oauth/users/scopes", nil, token.Access, &permissions); err != nil {
		return Authorization{}, err
	}
	var granted []string
	for _, permission := range permissions.Scopes {
		granted = append(granted, strings.Fields(permission.Scope)...)
	}
	slices.Sort(granted)
	granted = slices.Compact(granted)
	for _, scope := range scopes {
		if !slices.Contains(granted, scope) {
			return Authorization{}, ErrAuthorizationScope
		}
	}
	if permissions.Account == "" {
		return Authorization{}, ErrAuthorizationScope
	}
	drives, err := o.driveInfo(ctx, permissions.Account, token.Access)
	if err != nil {
		return Authorization{}, err
	}
	return Authorization{Token: token, Scopes: granted, Drives: drives}, nil
}

// DriveInfo reads the drives of the expected account using its existing access token.
func (o OAuth) DriveInfo(ctx context.Context, account, access string) (DriveInfo, error) {
	if account == "" || access == "" {
		return DriveInfo{}, core.Fail(core.InvalidRequest, nil)
	}
	o, err := o.prepare()
	if err != nil {
		return DriveInfo{}, err
	}
	return o.driveInfo(ctx, account, access)
}

func (o OAuth) driveInfo(ctx context.Context, account, access string) (DriveInfo, error) {
	var drives DriveInfo
	if err := o.scanJSON(ctx, http.MethodPost, "/adrive/v1.0/user/getDriveInfo", nil, access, &drives); err != nil {
		return DriveInfo{}, err
	}
	if drives.Account != account || drives.Default == "" && drives.Resource == "" && drives.Backup == "" {
		return DriveInfo{}, ErrAuthorizationScope
	}
	return drives, nil
}

func (o OAuth) scanJSON(ctx context.Context, method, route string, fields any, access string, out any) error {
	response, err := o.request(ctx, method, route, fields, access, true)
	if err != nil {
		return err
	}
	err = o.decodeOAuth(ctx, response, out, true)
	closeErr := closeTokenResponse(response.Body)
	if cause := ctx.Err(); cause != nil {
		err = cause
	}
	return errors.Join(err, closeErr)
}

func (o OAuth) decodeOAuth(ctx context.Context, response *http.Response, out any, scan bool) error {
	status := response.StatusCode
	if !scan {
		if status < 200 || status >= 300 {
			return statusError(status, "", nil)
		}
		return decode(response.Body, o.MaxResponseBytes, out)
	}
	switch status {
	case http.StatusOK, http.StatusBadRequest, http.StatusUnauthorized:
		limit := o.MaxResponseBytes
		if limit == 0 || limit > maxScanResponseBytes {
			limit = maxScanResponseBytes
		}
		if err := decode(response.Body, limit, out); err != nil {
			return scanError(ctx, err, core.Fail(core.InvalidUpstreamResponse, nil))
		}
		if status == http.StatusOK {
			return nil
		}
		fallthrough
	case http.StatusForbidden:
		return scanError(ctx, nil, ErrAuthorizationRejected)
	case http.StatusTooManyRequests:
		return core.Fail(core.UpstreamThrottled, nil)
	default:
		return core.Fail(core.UpstreamFailure, fmt.Errorf("Aliyun Drive returned HTTP %d", status))
	}
}
