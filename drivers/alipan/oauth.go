package alipan

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/alipan/credential"
)

// OAuth uses an application's Aliyun Drive Open API. The caller supplies the
// application identity, presents consent, and owns credential storage.
type OAuth struct {
	Client                           *http.Client
	Endpoint, ClientID, ClientSecret string
	MaxResponseBytes                 int64
}

// Refresh exchanges an authorized application's refresh token.
func (o OAuth) Refresh(ctx context.Context, refreshToken string) (credential.Token, error) {
	return o.refresh(ctx, refreshToken, false)
}

// RefreshScan exchanges a refresh token issued by Authorize without a client secret.
func (o OAuth) RefreshScan(ctx context.Context, refreshToken string) (credential.Token, error) {
	return o.refresh(ctx, refreshToken, true)
}

func (o OAuth) refresh(ctx context.Context, refreshToken string, scan bool) (credential.Token, error) {
	if o.Client == nil || o.ClientID == "" || refreshToken == "" || o.MaxResponseBytes < 0 {
		return credential.Token{}, errors.New("a client, application identity, refresh token, and nonnegative policy are required")
	}
	o, err := o.prepare()
	if err != nil {
		return credential.Token{}, err
	}
	fields := map[string]string{"grant_type": "refresh_token", "refresh_token": refreshToken, "client_id": o.ClientID}
	if !scan {
		fields["client_secret"] = o.ClientSecret
	}
	return o.exchange(ctx, fields, scan)
}

func (o OAuth) exchange(ctx context.Context, fields map[string]string, scan bool) (credential.Token, error) {
	started := time.Now()
	response, err := o.request(ctx, http.MethodPost, "/oauth/access_token", fields, "", scan)
	if err != nil {
		return credential.Token{}, err
	}
	var out struct {
		Access  string `json:"access_token"`
		Refresh string `json:"refresh_token"`
		Type    string `json:"token_type"`
		Expires int64  `json:"expires_in"`
	}
	err = o.decodeOAuth(ctx, response, &out, scan)
	closeErr := closeTokenResponse(response.Body)
	if err != nil {
		return credential.Token{}, errors.Join(err, closeErr)
	}
	if scan {
		started = time.Now()
	}
	token := credential.Token{Access: out.Access, Refresh: out.Refresh}
	if out.Expires > 0 && out.Expires <= int64((1<<63-1)/time.Second) {
		token.ExpiresAt = started.Add(time.Duration(out.Expires) * time.Second)
	}
	if out.Access == "" || out.Refresh == "" || !strings.EqualFold(out.Type, "Bearer") || token.ExpiresAt.IsZero() {
		if scan {
			token.Access, token.ExpiresAt = "", time.Time{}
			if out.Refresh == "" {
				return token, errors.Join(ErrAuthorizationNotRenewable, closeErr)
			}
		}
		return token, errors.Join(core.Fail(core.InvalidUpstreamResponse, nil), closeErr)
	}
	return token, closeErr
}

func (o OAuth) prepare() (OAuth, error) {
	if o.Client == nil || o.MaxResponseBytes < 0 {
		return OAuth{}, errors.New("a client and nonnegative response budget are required")
	}
	if o.Endpoint == "" {
		o.Endpoint = Endpoint
	}
	if err := validEndpoint(o.Endpoint); err != nil {
		return OAuth{}, err
	}
	o.Endpoint = strings.TrimSuffix(o.Endpoint, "/")
	o.Client = isolatedClient(o.Client)
	return o, nil
}

func (o OAuth) request(ctx context.Context, method, route string, fields any, access string, scan bool) (*http.Response, error) {
	var body io.Reader
	if fields != nil {
		payload, err := json.Marshal(fields)
		if err != nil {
			return nil, err
		}
		body = bytes.NewReader(payload)
	}
	req, err := http.NewRequestWithContext(ctx, method, o.Endpoint+route, body)
	if err != nil {
		return nil, err
	}
	if fields != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if access != "" {
		req.Header.Set("Authorization", "Bearer "+access)
	}
	response, err := o.Client.Do(req)
	if err == nil {
		return response, nil
	}
	if scan {
		return nil, scanError(ctx, err, core.Fail(core.UpstreamFailure, nil))
	}
	return nil, networkError(err)
}

func scanError(ctx context.Context, err, fallback error) error {
	if cause := ctx.Err(); cause != nil {
		return cause
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return context.DeadlineExceeded
	}
	return fallback
}

// The error retains a body whose close failed; Source retains this error while
// reporting the unfinished resource instead of claiming successful retirement.
type tokenCloseFailure struct {
	body  io.Closer
	cause error
}

func (e *tokenCloseFailure) Error() string { return "token response close failed" }
func (e *tokenCloseFailure) Unwrap() error { return e.cause }
func closeTokenResponse(body io.Closer) error {
	if err := body.Close(); err != nil {
		return &tokenCloseFailure{body: body, cause: err}
	}
	return nil
}
