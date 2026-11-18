package quark

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	core "github.com/nostalume/mountweave"
)

func (s *websiteSession) open(ctx context.Context, file entry, selection *core.Range) (core.Opened, error) {
	size := *file.meta.Size
	var wanted *core.ContentRange
	var rangeHeader string
	if selection != nil {
		actual, err := selection.Resolve(size)
		if err != nil {
			return core.Opened{}, err
		}
		wanted = &actual
		rangeHeader = fmt.Sprintf("bytes=%d-%d", actual.Start, actual.End)
	}
	var opened core.Opened
	body, err := s.transfer(ctx, func(setup context.Context) (WebsiteDownload, error) {
		target, err := s.grant(setup, file.fid, size)
		if err != nil {
			return WebsiteDownload{}, err
		}
		var page websiteDownloadPage
		return WebsiteDownload{
			URL: target,
			Start: func(ctx context.Context) error {
				observed, err := s.startDownload(ctx, target, rangeHeader)
				if err == nil {
					page = observed
				}
				return err
			},
			Stop: func(ctx context.Context) error {
				_, err := s.call(ctx, websiteStopDownload, json.RawMessage("null"))
				return err
			},
			Check: func(status int64, headers http.Header) (int64, bool, error) {
				if page.Origin == "" {
					return 0, false, core.Fail(core.InvalidUpstreamResponse, nil)
				}
				var err error
				opened, err = websiteDownloadMetadata(status, headers, file.meta, wanted, page)
				if err != nil {
					return 0, false, err
				}
				length := size
				if opened.Length != nil {
					length = *opened.Length
				}
				return length, headers.Get("Content-Length") == "", nil
			},
		}, nil
	})
	if err == nil {
		err = ctx.Err()
	}
	if err == nil && (body == nil || opened.Metadata.Kind != core.File) {
		err = core.Fail(core.InvalidUpstreamResponse, nil)
	}
	if err != nil {
		if body != nil {
			err = errors.Join(err, body.Close())
		}
		return core.Opened{}, err
	}
	opened.Body = body
	return opened, nil
}

func (s *websiteSession) grant(ctx context.Context, fid string, size int64) (string, error) {
	reply, err := callWebsite(ctx, s.call, websiteRequest{Account: s.account, FID: fid})
	if err != nil {
		return "", err
	}
	if reply.Account != s.account || len(reply.Grants) != 1 || reply.Grants[0].FID != fid {
		return "", core.Fail(core.InvalidUpstreamResponse, nil)
	}
	grant := reply.Grants[0]
	if grant.Size != nil {
		observed, err := grant.Size.Int64()
		if err != nil || observed != size {
			return "", core.Fail(core.InvalidUpstreamResponse, nil)
		}
	}
	invalid := core.Fail(core.InvalidUpstreamResponse, nil)
	target, err := url.Parse(grant.URL)
	if err != nil || target.Scheme != "https" || target.User != nil || target.Opaque != "" ||
		strings.Contains(grant.URL, "#") || (target.Port() != "" && target.Port() != "443") {
		return "", invalid
	}
	host := strings.ToLower(target.Hostname())
	if host != "quark.cn" && !strings.HasSuffix(host, ".quark.cn") {
		return "", invalid
	}
	return target.String(), nil
}

type websiteDownloadPage struct {
	Origin   string
	Cross    bool
	Conflict bool
	SignIn   bool `json:"sign_in"`
}

func (s *websiteSession) startDownload(ctx context.Context, address, rangeHeader string) (websiteDownloadPage, error) {
	var page websiteDownloadPage
	args, err := json.Marshal(map[string]string{"url": address, "range": rangeHeader, "account": s.account})
	if err != nil {
		return page, err
	}
	raw, err := s.call(ctx, websiteStartDownload, args)
	if err != nil {
		return page, err
	}
	if json.Unmarshal(raw, &page) != nil {
		return page, core.Fail(core.InvalidUpstreamResponse, nil)
	}
	if page.Conflict {
		return page, core.Fail(core.UpstreamAccessDenied, ErrWebsiteBindingMismatch)
	}
	if page.SignIn {
		return page, core.Fail(core.UpstreamAccessDenied, ErrWebsiteSignIn)
	}
	if page.Origin == "" {
		return page, core.Fail(core.InvalidUpstreamResponse, errors.New("Quark file page could not start the download"))
	}
	return page, nil
}

func websiteDownloadMetadata(status int64, headers http.Header, file core.Metadata, wanted *core.ContentRange, page websiteDownloadPage) (opened core.Opened, err error) {
	switch status {
	case http.StatusUnauthorized, http.StatusForbidden:
		return opened, core.Fail(core.UpstreamAccessDenied, errors.New("Quark refused the download"))
	case http.StatusTooManyRequests:
		return opened, core.Fail(core.UpstreamThrottled, nil)
	case http.StatusRequestedRangeNotSatisfiable:
		return opened, core.Fail(core.RangeNotSatisfiable, nil)
	case http.StatusOK, http.StatusPartialContent:
	default:
		return opened, core.Fail(core.UpstreamFailure, fmt.Errorf("Quark download HTTP %d", status))
	}
	bad := core.Fail(core.InvalidUpstreamResponse, errors.New("Quark file response did not match the selected file"))
	for _, name := range []string{"Content-Length", "Content-Range", "Content-Encoding", "Access-Control-Allow-Origin", "Access-Control-Allow-Credentials"} {
		if len(headers.Values(name)) > 1 {
			return opened, bad
		}
	}
	if page.Cross && (headers.Get("Access-Control-Allow-Origin") != page.Origin || headers.Get("Access-Control-Allow-Credentials") != "true") {
		return opened, bad
	}
	if encoding := headers.Get("Content-Encoding"); encoding != "" && !strings.EqualFold(encoding, "identity") {
		return opened, bad
	}
	length := *file.Size
	if wanted == nil {
		if status != http.StatusOK || headers.Get("Content-Range") != "" {
			return opened, bad
		}
	} else {
		if status != http.StatusPartialContent || headers.Get("Content-Range") != fmt.Sprintf("bytes %d-%d/%d", wanted.Start, wanted.End, wanted.Total) {
			return opened, bad
		}
		length = wanted.End - wanted.Start + 1
	}
	if raw := headers.Get("Content-Length"); raw != "" {
		received, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || received != length {
			return opened, bad
		}
		opened.Length = &length
	}
	opened.Metadata = file
	if wanted != nil {
		opened.Length, opened.Range = &length, wanted
	}
	return opened, nil
}
