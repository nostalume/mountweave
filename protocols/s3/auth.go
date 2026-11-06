package s3

import (
	"context"
	"crypto/hmac"
	"encoding/hex"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	core "github.com/nostalume/mountweave"
)

// Identity binds an access key's current credentials to a Core principal.
type Identity struct {
	Principal       core.Principal
	SecretAccessKey string
	SessionToken    string
}

const (
	signingAlgorithm = "AWS4-HMAC-SHA256"
	emptyPayload     = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
	unsignedPayload  = "UNSIGNED-PAYLOAD"
	trailerPayload   = "STREAMING-UNSIGNED-PAYLOAD-TRAILER"
)

func (h *Handler) authenticate(r *http.Request, target *url.URL, now time.Time) (core.Principal, string, error) {
	denied := fault(http.StatusForbidden, "SignatureDoesNotMatch")
	auth, ok := strings.CutPrefix(r.Header.Get("Authorization"), signingAlgorithm+" ")
	parts := strings.Split(auth, ",")
	if !ok || len(parts) != 3 || len(r.Header.Values("Authorization")) != 1 {
		return "", "", denied
	}
	credential, c := strings.CutPrefix(strings.TrimSpace(parts[0]), "Credential=")
	headers, hs := strings.CutPrefix(strings.TrimSpace(parts[1]), "SignedHeaders=")
	signature, s := strings.CutPrefix(strings.TrimSpace(parts[2]), "Signature=")
	scope := strings.Split(credential, "/")
	date := r.Header.Get("X-Amz-Date")
	t, err := time.Parse("20060102T150405Z", date)
	if !c || !hs || !s || !isDigest(signature) || len(scope) != 5 || scope[0] == "" || err != nil ||
		scope[1] != t.Format("20060102") || scope[2] != h.options.Region || scope[3] != "s3" || scope[4] != "aws4_request" ||
		now.Sub(t) > 15*time.Minute || t.Sub(now) > 15*time.Minute || len(r.Header.Values("X-Amz-Date")) != 1 {
		return "", "", denied
	}
	payload := r.Header.Get("X-Amz-Content-Sha256")
	if len(r.Header.Values("X-Amz-Content-Sha256")) != 1 || payload == "" {
		return "", "", denied
	}
	names := strings.Split(headers, ";")
	// The signer receives only the headers the client claims to have signed.
	// Every header that changes an operation or body interpretation must be protected.
	if !slices.Contains(names, "host") || !slices.Contains(names, "x-amz-date") {
		return "", "", denied
	}
	for key := range r.Header {
		name := strings.ToLower(key)
		if protectedHeader(name) && !slices.Contains(names, name) {
			return "", "", denied
		}
	}
	identity, err := h.options.LookupIdentity(r.Context(), scope[0])
	if err != nil {
		return "", "", fault(http.StatusForbidden, "AccessDenied", err)
	}
	if identity.Principal == "" || identity.SecretAccessKey == "" ||
		!hmac.Equal([]byte(identity.SessionToken), []byte(r.Header.Get("X-Amz-Security-Token"))) ||
		len(r.Header.Values("X-Amz-Security-Token")) > 1 {
		return "", "", denied
	}
	u := *target
	u.Host, u.Scheme = r.Host, "http"
	if r.TLS != nil {
		u.Scheme = "https"
	}
	signed := &http.Request{Method: r.Method, URL: &u, Host: r.Host, Header: make(http.Header), ContentLength: -1}
	for _, name := range names {
		switch name {
		case "host":
		case "content-length":
			signed.ContentLength = r.ContentLength
		default:
			values := r.Header.Values(name)
			if len(values) == 0 {
				return "", "", denied
			}
			signed.Header[http.CanonicalHeaderKey(name)] = slices.Clone(values)
		}
	}
	credentials := aws.Credentials{AccessKeyID: scope[0], SecretAccessKey: identity.SecretAccessKey, SessionToken: identity.SessionToken}
	// The SDK signer caches derived keys by access key ID and date. A fresh
	// signer keeps the host's current secret authoritative after rotation.
	signer := v4.NewSigner(func(o *v4.SignerOptions) { o.DisableURIPathEscaping = true })
	if err := signer.SignHTTP(r.Context(), credentials, signed, payload, "s3", h.options.Region, t); err != nil {
		return "", "", fault(http.StatusForbidden, "SignatureDoesNotMatch", err)
	}
	expected := signingAlgorithm + " Credential=" + credential + ", SignedHeaders=" + headers + ", Signature=" + signature
	if !hmac.Equal([]byte(expected), []byte(signed.Header.Get("Authorization"))) {
		return "", "", denied
	}
	return identity.Principal, payload, nil
}

func protectedHeader(name string) bool {
	switch name {
	case "x-amz-content-sha256", "x-amz-user-agent":
		return false // The payload hash has a dedicated field in the canonical request.
	case "content-encoding", "range", "content-md5", "content-type":
		return true
	}
	return strings.HasPrefix(name, "x-amz-") || strings.HasPrefix(name, "if-")
}

func isDigest(s string) bool {
	if len(s) != 64 {
		return false
	}
	_, err := hex.DecodeString(s)
	return err == nil
}

// IdentityLookup resolves current credentials for an access key on each request.
type IdentityLookup func(context.Context, string) (Identity, error)
