package quark

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/internal/httpcontent"
)

// Application authorizes requests with a Quark-approved application. The caller
// supplies and renews the grant for one account.
type Application struct {
	ClientID, SignKey string
	ClientToken       string // Optional application token issued by Quark.
	AccessToken       func(context.Context) (string, error)
}

func (a Application) valid() bool {
	return a.ClientID != "" && a.SignKey != "" && a.AccessToken != nil &&
		validCookie("x_pan_client_id", a.ClientID) && validCookie("x_pan_client_token", a.ClientToken)
}

func validCookie(name, value string) bool {
	return (&http.Cookie{Name: name, Value: value}).Valid() == nil
}

func (a Application) authorize(req *http.Request) (string, error) {
	ctx := req.Context()
	token, err := a.AccessToken(ctx)
	if err != nil {
		return "", errors.Join(core.Fail(core.UpstreamAccessDenied, httpcontent.PrivateIOError(err)), ctx.Err())
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if strings.TrimSpace(token) == "" || !validCookie("x_pan_access_token", token) {
		return "", core.Fail(core.UpstreamAccessDenied, errors.New("Quark Drive access token is unavailable or invalid"))
	}

	var id [16]byte
	rand.Read(id[:])
	id[6] = id[6]&0x0f | 0x40
	id[8] = id[8]&0x3f | 0x80
	requestID := fmt.Sprintf("%x-%x-%x-%x-%x", id[:4], id[4:6], id[6:8], id[8:10], id[10:])
	query := req.URL.Query()
	query.Set("req_id", requestID)
	query.Set("access_token", token)
	req.URL.RawQuery = query.Encode()

	timestamp := strconv.FormatInt(time.Now().UnixMilli(), 10)
	signature := sha256.Sum256([]byte(req.Method + "&" + req.URL.Path + "&" + timestamp + "&" + a.SignKey))
	req.Header.Set("x-pan-client-id", a.ClientID)
	req.Header.Set("x-pan-tm", timestamp)
	req.Header.Set("x-pan-token", hex.EncodeToString(signature[:]))
	req.Header.Set("Authorization", "Bearer "+token)
	if a.ClientToken != "" {
		req.Header.Set("x-pan-client-token", a.ClientToken)
		req.Header.Set("x-client-token", a.ClientToken)
	}
	return token, nil
}

func (a Application) authorizeBody(req *http.Request, accessToken string) {
	req.AddCookie(&http.Cookie{Name: "x_pan_client_id", Value: a.ClientID})
	req.AddCookie(&http.Cookie{Name: "x_pan_access_token", Value: accessToken})
	if a.ClientToken != "" {
		req.AddCookie(&http.Cookie{Name: "x_pan_client_token", Value: a.ClientToken})
	}
}
