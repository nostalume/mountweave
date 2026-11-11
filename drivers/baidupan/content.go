package baidupan

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"
	"slices"
	"strings"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/internal/httpcontent"
)

func (d *Driver) Open(ctx context.Context, p core.NativeAddress, o core.OpenOptions) (opened core.Opened, err error) {
	if err := o.Validate(); err != nil {
		return core.Opened{}, err
	}
	if o.Mode != core.StreamRead || o.NoFollow || o.Selection.Version != "" {
		return core.Opened{}, core.Fail(core.UnsupportedOperation, nil)
	}
	// Apply a failed absence condition only after admitting the native file.
	selectionErr := o.Selection.When.Check(core.Evidence{}, true)
	if selectionErr != nil && core.CodeOf(selectionErr) != core.PreconditionFailed {
		return core.Opened{}, selectionErr
	}
	full, err := d.address(p)
	if err != nil {
		return core.Opened{}, err
	}
	token, err := d.credential(ctx)
	if err != nil {
		return core.Opened{}, err
	}
	f, err := d.metadata(ctx, full, token, true)
	if err != nil {
		return core.Opened{}, err
	}
	if f.meta.Kind != core.File {
		return core.Opened{}, core.Fail(core.NotFile, nil)
	}
	if selectionErr != nil {
		return core.Opened{}, selectionErr
	}
	wanted := o.Range
	if wanted == nil && *f.meta.Size > 0 {
		wanted = &core.Range{End: -1}
	}
	response, err := d.body(ctx, f.link, token, wanted)
	if err != nil {
		return core.Opened{}, err
	}
	// Only an admitted body is transferred. Close failed responses here so
	// cleanup errors go through the same capability redaction as request errors.
	defer func() {
		if err != nil {
			opened = core.Opened{}
			err = errors.Join(err, d.closeBody(response.Body, token))
		}
	}()
	if response.StatusCode == http.StatusRequestedRangeNotSatisfiable {
		if o.Range == nil {
			return opened, core.Fail(core.InvalidUpstreamResponse, errors.New("native download rejected the full-file request (HTTP 416)"))
		}
		return opened, &core.Error{Code: core.RangeNotSatisfiable}
	}
	if response.StatusCode == http.StatusOK && *f.meta.Size > 0 {
		return opened, core.Fail(core.InvalidUpstreamResponse, errors.New("native download ignored Range; content cannot be verified"))
	}
	if response.StatusCode != http.StatusOK && response.StatusCode != http.StatusPartialContent {
		data, readErr := d.readBody(response, token)
		return opened, errors.Join(d.httpFailure(response, data, token), readErr)
	}
	opened, err = httpcontent.Open(response, wanted)
	if err != nil {
		return opened, err
	}
	if response.StatusCode == http.StatusOK {
		if response.ContentLength > 0 {
			return opened, core.Fail(core.InvalidUpstreamResponse, errors.New("empty native file returned nonempty content"))
		}
		if response.ContentLength < 0 {
			var probe [1]byte
			n, readErr := io.ReadFull(response.Body, probe[:])
			if n != 0 || readErr != io.EOF {
				return opened, core.Fail(core.InvalidUpstreamResponse, privateError(readErr, token))
			}
		}
		opened.Length = f.meta.Size
	} else {
		if opened.Range == nil || opened.Range.Total != *f.meta.Size {
			return opened, core.Fail(core.InvalidUpstreamResponse, errors.New("native metadata and HTTP range total disagree"))
		}
		if o.Range == nil {
			opened.Range = nil
		}
	}
	opened.Metadata = f.meta
	return opened, nil
}

func contentURL(target *url.URL) bool {
	return target != nil && target.Scheme == "https" && target.Host != "" && target.User == nil && target.Opaque == "" && target.Fragment == ""
}

func sameOrigin(a, b *url.URL) bool {
	port := func(u *url.URL) string {
		if u.Port() == "" {
			return "443"
		}
		return u.Port()
	}
	return strings.EqualFold(a.Hostname(), b.Hostname()) && port(a) == port(b)
}

// appendToken preserves the native query bytes and signatures. It is used only
// while this content chain has never crossed the original authorized origin.
func appendToken(target *url.URL, token string) *url.URL {
	copy := *target
	credential := "access_token=" + url.QueryEscape(token)
	if copy.RawQuery == "" {
		copy.RawQuery = credential
		return &copy
	}
	parts := strings.Split(copy.RawQuery, "&")
	kept, replaced := parts[:0], false
	for _, part := range parts {
		key, _, _ := strings.Cut(part, "=")
		key, err := url.QueryUnescape(key)
		if err == nil && key == "access_token" {
			if !replaced {
				kept = append(kept, credential)
				replaced = true
			}
		} else {
			kept = append(kept, part)
		}
	}
	if !replaced {
		kept = append(kept, credential)
	}
	copy.RawQuery = strings.Join(kept, "&")
	return &copy
}

func (d *Driver) body(ctx context.Context, link, token string, wanted *core.Range) (*http.Response, error) {
	target, err := url.Parse(link)
	if err != nil || !contentURL(target) {
		return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("native download URL is not an absolute HTTPS URL"))
	}
	origin := target
	authenticated := true
	for requests := 0; requests < 10; requests++ {
		if authenticated {
			target = appendToken(target, token)
		} else if slices.Contains(target.Query()["access_token"], token) {
			return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("native download redirect includes the account token"))
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, target.String(), nil)
		if err != nil {
			return nil, core.Fail(core.InvalidUpstreamResponse, privateError(err, token))
		}
		if wanted != nil {
			req.Header.Set("Range", httpcontent.Header(wanted))
		}
		response, _, err := d.do(req, token)
		if err != nil {
			return nil, err
		}
		switch response.StatusCode {
		case 301, 302, 303, 307, 308:
		default:
			return response, nil
		}
		location := response.Header.Get("Location")
		next, parseErr := target.Parse(location)
		if parseErr != nil || location == "" || !contentURL(next) {
			err = core.Fail(core.InvalidUpstreamResponse, errors.New("native download redirect is not an absolute HTTPS URL"))
		}
		err = errors.Join(err, d.closeBody(response.Body, token))
		if err != nil {
			return nil, err
		}
		if !sameOrigin(origin, next) {
			authenticated = false
		}
		target = next
	}
	return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("native download redirect limit exceeded"))
}
