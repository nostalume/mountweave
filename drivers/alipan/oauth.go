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

// OAuth supplies an authorized application's token endpoint. It does not
// discover client secrets, acquire consent, or save credentials.
type OAuth struct {
	Client                           *http.Client
	Endpoint, ClientID, ClientSecret string
	MaxResponseBytes                 int64
}

func (o OAuth) Refresh(ctx context.Context, refreshToken string) (credential.Token, error) {
	endpoint := o.Endpoint
	if endpoint == "" {
		endpoint = Endpoint
	}
	if o.Client == nil || o.ClientID == "" || refreshToken == "" || o.MaxResponseBytes < 0 {
		return credential.Token{}, errors.New("a client, application identity, refresh token, and nonnegative policy are required")
	}
	if err := validEndpoint(endpoint); err != nil {
		return credential.Token{}, err
	}
	payload, err := json.Marshal(map[string]string{"grant_type": "refresh_token", "refresh_token": refreshToken, "client_id": o.ClientID, "client_secret": o.ClientSecret})
	if err != nil {
		return credential.Token{}, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimSuffix(endpoint, "/")+"/oauth/access_token", bytes.NewReader(payload))
	if err != nil {
		return credential.Token{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	started := time.Now()
	response, err := isolatedClient(o.Client).Do(req)
	if err != nil {
		return credential.Token{}, networkError(err)
	}
	var out struct {
		Access  string `json:"access_token"`
		Refresh string `json:"refresh_token"`
		Type    string `json:"token_type"`
		Expires int64  `json:"expires_in"`
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return credential.Token{}, errors.Join(statusError(response.StatusCode, "", nil), closeTokenResponse(response.Body))
	}
	err = decode(response.Body, o.MaxResponseBytes, &out)
	closeErr := closeTokenResponse(response.Body)
	if err != nil {
		return credential.Token{}, errors.Join(err, closeErr)
	}
	token := credential.Token{Access: out.Access, Refresh: out.Refresh}
	if out.Expires > 0 && out.Expires <= int64((1<<63-1)/time.Second) {
		token.ExpiresAt = started.Add(time.Duration(out.Expires) * time.Second)
	}
	if out.Access == "" || out.Refresh == "" || !strings.EqualFold(out.Type, "Bearer") || token.ExpiresAt.IsZero() {
		return token, errors.Join(core.Fail(core.InvalidUpstreamResponse, nil), closeErr)
	}
	return token, closeErr
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
