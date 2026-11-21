package alipan_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"testing"

	core "github.com/nostalume/mountweave"
	driver "github.com/nostalume/mountweave/drivers/alipan"
)

func TestOAuthRefreshWireCompatibility(t *testing.T) {
	const complete = `{"token_type":"Bearer","access_token":"known-access","refresh_token":"known-refresh","expires_in":3600}`
	const partial = `{"token_type":"other","access_token":"unusable","refresh_token":"known-refresh","expires_in":3600}`
	for _, tc := range []struct {
		name, secret, body string
		scan               bool
		cancel             bool
		status             int
		wantRefresh        string
		wantError          core.Code
	}{
		{name: "existing empty field", status: 201, body: complete, wantRefresh: "known-refresh"},
		{name: "existing app secret", secret: "fixture-secret", status: 201, body: complete, wantRefresh: "known-refresh"},
		{name: "scan rejects non-200", scan: true, status: 201, body: complete, wantError: core.UpstreamFailure},
		{name: "scan retains known rotation", scan: true, status: 200, body: partial, wantRefresh: "known-refresh", wantError: core.InvalidUpstreamResponse},
		{name: "cancelled rejection", scan: true, cancel: true, status: 401, body: "{}"},
		{name: "cancelled after rotation", scan: true, cancel: true, status: 200, body: complete, wantRefresh: "known-refresh"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			client := &http.Client{Transport: transport(func(r *http.Request) (*http.Response, error) {
				var fields map[string]string
				if err := json.NewDecoder(r.Body).Decode(&fields); err != nil {
					t.Fatal(err)
				}
				secret, sent := fields["client_secret"]
				if sent == tc.scan || secret != tc.secret || r.Header.Get("Authorization") != "" ||
					r.Method != http.MethodPost || r.URL.Path != "/oauth/access_token" || r.URL.Host != "openapi.alipan.com" {
					t.Fatal("unexpected refresh request")
				}
				if tc.cancel {
					cancel()
				}
				return response(r, tc.status, tc.body), nil
			})}
			oauth := driver.OAuth{Client: client, ClientID: "fixture-app", ClientSecret: tc.secret}
			refresh := oauth.Refresh
			if tc.scan {
				refresh = oauth.RefreshScan
			}
			token, err := refresh(ctx, "prior-refresh")
			wrongError := err == nil && tc.wantError != "" || err != nil && core.CodeOf(err) != tc.wantError
			if tc.cancel && tc.status == http.StatusUnauthorized {
				wrongError = !errors.Is(err, context.Canceled) || errors.Is(err, driver.ErrAuthorizationRejected)
			}
			if token.Refresh != tc.wantRefresh || wrongError {
				t.Fatalf("refresh: got refresh %q, %v", token.Refresh, err)
			}
			if tc.wantError != "" && (token.Access != "" || !token.ExpiresAt.IsZero()) {
				t.Fatal("scan exposed an unusable access token")
			}
		})
	}
}
