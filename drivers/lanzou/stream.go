package lanzou

import (
	"context"
	"errors"
	"fmt"
	"mime"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/chromedp/cdproto/fetch"
	browserio "github.com/chromedp/cdproto/io"
	"github.com/chromedp/cdproto/network"
	"github.com/chromedp/chromedp"
	core "github.com/nostalume/mountweave"
	transport "github.com/nostalume/mountweave/browser"
	"github.com/nostalume/mountweave/drivers/internal/httpcontent"
)

func streamLanZou(ctx, parent context.Context, target *url.URL, wanted *core.Range) (core.Opened, error) {
	tab, closeTab := chromedp.NewContext(parent, chromedp.WithNewBrowserContext())
	life, cancelLife := context.WithCancel(tab)
	stop := context.AfterFunc(ctx, cancelLife)
	closeSession := func() { stop(); cancelLife(); closeTab() }
	handshake, cancel := context.WithTimeout(life, 20*time.Second)
	defer cancel()
	stopHandshake := context.AfterFunc(handshake, cancelLife)
	defer stopHandshake()
	transferred := false
	defer func() {
		if !transferred {
			closeSession()
		}
	}()
	if err := chromedp.Do(life); err != nil {
		return core.Opened{}, lanZouBrowserError(handshake)
	}
	if _, err := chromedp.Call(handshake, fetch.Enable, fetch.EnableParams{Patterns: []*fetch.RequestPattern{{URLPattern: "*"}}}); err != nil {
		return core.Opened{}, lanZouBrowserError(handshake)
	}
	events := chromedp.Events(handshake, fetch.RequestPaused)
	go func() { _, _ = chromedp.Run(life, chromedp.Navigate(target.String())) }()
	for event, eventErr := range events {
		if eventErr != nil {
			return core.Opened{}, lanZouBrowserError(handshake)
		}
		if event.Request == nil {
			return core.Opened{}, core.Fail(core.InvalidUpstreamResponse, nil)
		}
		source, parseErr := url.Parse(event.Request.URL)
		allowed := parseErr == nil && lanZouHTTPS(source) && event.Request.Method == http.MethodGet &&
			(strings.HasSuffix(source.Hostname(), ".lanrar.com") || strings.HasSuffix(source.Hostname(), ".webgetstore.com"))
		if !allowed {
			_, err := chromedp.Call(handshake, fetch.FailRequest, fetch.FailRequestParams{RequestID: event.RequestID, ErrorReason: network.ErrorReasonBlockedByClient})
			if err != nil || event.ResourceType == network.ResourceTypeDocument {
				return core.Opened{}, core.Fail(core.InvalidUpstreamResponse, errors.New("LanZou sent the download to an unexpected destination"))
			}
			continue
		}
		next := fetch.ContinueRequestParams{RequestID: event.RequestID}
		if event.ResponseStatusCode == 0 && event.ResponseErrorReason == "" && event.ResourceType == network.ResourceTypeDocument {
			intercept := true
			next.InterceptResponse = &intercept
			if wanted != nil && strings.HasSuffix(source.Hostname(), ".webgetstore.com") {
				next.Headers = transport.RangeHeaders(event.Request.Headers, httpcontent.Header(wanted))
			}
		}
		if event.ResponseStatusCode != 0 && event.ResourceType == network.ResourceTypeDocument {
			h := transport.ResponseHeaders(event.ResponseHeaders)
			kind, _, _ := mime.ParseMediaType(h.Get("Content-Disposition"))
			if event.ResponseStatusCode == http.StatusRequestedRangeNotSatisfiable || kind == "attachment" {
				opened, err := lanZouStreamMetadata(event.ResponseStatusCode, h, wanted)
				if err != nil {
					return core.Opened{}, err
				}
				stream, err := chromedp.Call(handshake, fetch.TakeResponseBodyAsStream, fetch.TakeResponseBodyAsStreamParams{RequestID: event.RequestID})
				if err != nil {
					return core.Opened{}, lanZouBrowserError(handshake)
				}
				remaining := int64(-1)
				if opened.Length != nil {
					remaining = *opened.Length
				}
				closeBody := sync.OnceValue(func() error {
					defer closeSession()
					closing, end := context.WithTimeout(tab, 2*time.Second)
					defer end()
					_, err := chromedp.Call(closing, browserio.Close, browserio.CloseParams{Handle: stream.Stream})
					if err != nil && life.Err() == nil {
						return lanZouBrowserError(closing)
					}
					return nil
				})
				opened.Body = transport.NewBody(life, stream.Stream, closeBody, errors.New("LanZou browser download failed"), remaining, false)
				transferred = true
				return opened, nil
			}
			if event.ResponseStatusCode == http.StatusTooManyRequests {
				return core.Opened{}, core.Fail(core.UpstreamThrottled, nil)
			}
		}
		if _, err := chromedp.Call(handshake, fetch.ContinueRequest, next); err != nil {
			return core.Opened{}, lanZouBrowserError(handshake)
		}
	}
	return core.Opened{}, lanZouBrowserError(handshake)
}

func lanZouBrowserError(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return core.Fail(core.UpstreamFailure, errors.New("LanZou browser download failed"))
}

func lanZouStreamMetadata(status int64, h http.Header, wanted *core.Range) (core.Opened, error) {
	o := core.Opened{Metadata: core.Metadata{Kind: core.File}}
	text := h.Get("Content-Range")
	if status == http.StatusRequestedRangeNotSatisfiable {
		failure := &core.Error{Code: core.RangeNotSatisfiable}
		var size int64
		if _, err := fmt.Sscanf(text, "bytes */%d", &size); err == nil && size >= 0 && text == fmt.Sprintf("bytes */%d", size) {
			failure.Total = &size
		}
		return o, failure
	}
	if strings.HasPrefix(h.Get("Content-Type"), "text/html") || (h.Get("Content-Encoding") != "" && h.Get("Content-Encoding") != "identity") {
		return o, core.Fail(core.InvalidUpstreamResponse, nil)
	}
	if raw := h.Get("Content-Length"); raw != "" {
		length, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || length < 0 {
			return o, core.Fail(core.InvalidUpstreamResponse, nil)
		}
		o.Length, o.Metadata.Size = &length, &length
	}
	switch status {
	case http.StatusOK:
		if text != "" {
			return o, core.Fail(core.InvalidUpstreamResponse, nil)
		}
	case http.StatusPartialContent:
		var actual core.ContentRange
		if _, err := fmt.Sscanf(text, "bytes %d-%d/%d", &actual.Start, &actual.End, &actual.Total); err != nil || wanted == nil ||
			text != fmt.Sprintf("bytes %d-%d/%d", actual.Start, actual.End, actual.Total) {
			return o, core.Fail(core.InvalidUpstreamResponse, nil)
		}
		expected, err := wanted.Resolve(actual.Total)
		length := actual.End - actual.Start + 1
		if err != nil || expected != actual || o.Length != nil && *o.Length != length {
			return o, core.Fail(core.InvalidUpstreamResponse, nil)
		}
		o.Range, o.Length, o.Metadata.Size = &actual, &length, &actual.Total
	default:
		return o, core.Fail(core.UpstreamFailure, errors.New("LanZou did not return a file response"))
	}
	return o, nil
}
