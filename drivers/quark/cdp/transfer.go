// Package quarkcdp connects a borrowed CDP tab to the Quark website driver.
package quarkcdp

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/chromedp/cdproto/cdp"
	"github.com/chromedp/cdproto/fetch"
	browserio "github.com/chromedp/cdproto/io"
	"github.com/chromedp/cdproto/network"
	"github.com/chromedp/chromedp"
	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/browser"
	"github.com/nostalume/mountweave/drivers/quark"
)

var fetchPattern = strings.NewReplacer(`\`, `\\`, `*`, `\*`, `?`, `\?`)

// Transfer uses an initialized tab supplied by the host. Create and reuse one
// transfer per tab so each body has exclusive use of Fetch until Close.
func Transfer(tab context.Context) quark.WebsiteTransfer {
	slot := make(chan struct{}, 1)
	broken := false
	return func(ctx context.Context, prepare func(context.Context) (quark.WebsiteDownload, error)) (body io.ReadCloser, err error) {
		select {
		case slot <- struct{}{}:
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-tab.Done():
			return nil, core.Fail(core.MountUnavailable, nil)
		}
		if err := ctx.Err(); err != nil {
			<-slot
			return nil, err
		}
		if broken || tab.Err() != nil {
			<-slot
			return nil, core.Fail(core.MountUnavailable, errors.New("Browser download session needs to be reopened"))
		}

		life, cancel := context.WithCancel(tab)
		stop := context.AfterFunc(ctx, cancel)
		setup, stopSetup := context.WithTimeout(life, 25*time.Second)
		defer stopSetup()

		var paused fetch.RequestID
		var handle browserio.StreamHandle
		var download quark.WebsiteDownload
		enabled, started := false, false
		closeBody := sync.OnceValue(func() error {
			stop()
			cancel()
			defer func() { <-slot }()
			if !enabled || tab.Err() != nil {
				return nil
			}
			clean, finish := context.WithTimeout(tab, 2*time.Second)
			defer finish()
			var closeErr error
			if handle != "" {
				if _, err := chromedp.Call(clean, browserio.Close, browserio.CloseParams{Handle: handle}); err != nil {
					closeErr = core.Fail(core.UpstreamFailure, errors.New("Browser body stream did not close"))
				}
			}
			if paused != "" {
				_, _ = chromedp.Call(clean, fetch.FailRequest, fetch.FailRequestParams{
					RequestID: paused, ErrorReason: network.ErrorReasonBlockedByClient,
				})
			}
			if started {
				if err := download.Stop(clean); err != nil {
					broken = true
					closeErr = errors.Join(closeErr, core.Fail(core.UpstreamFailure, errors.New("Browser request did not close")))
				}
			}
			if _, err := chromedp.Call(clean, fetch.Disable, cdp.Empty{}); err != nil {
				broken = true
				closeErr = errors.Join(closeErr, core.Fail(core.UpstreamFailure, errors.New("Browser response observation did not close")))
			}
			return closeErr
		})
		defer func() {
			if body == nil {
				err = errors.Join(err, closeBody())
			}
		}()
		download, err = prepare(setup)
		if err != nil {
			return nil, err
		}
		address := download.URL
		events := chromedp.Events(setup, fetch.RequestPaused)
		pattern := fetchPattern.Replace(address)
		enabled = true // Disable the interceptor even if its enable response is lost.
		if _, err := chromedp.Call(setup, fetch.Enable, fetch.EnableParams{Patterns: []*fetch.RequestPattern{{
			URLPattern: pattern, RequestStage: fetch.RequestStageResponse,
		}}}); err != nil {
			return nil, browser.TransferFailure(ctx, setup, "Browser could not observe the response")
		}
		started = true // Stop the request even if Start loses its reply.
		if err := download.Start(setup); err != nil {
			return nil, err
		}
		for event, eventErr := range events {
			if eventErr != nil {
				break
			}
			paused = event.RequestID
			if event.Request == nil {
				return nil, core.Fail(core.InvalidUpstreamResponse, nil)
			}
			if event.Request.URL != address || event.Request.Method != http.MethodGet {
				if _, err := chromedp.Call(setup, fetch.ContinueResponse, fetch.ContinueResponseParams{RequestID: paused}); err != nil {
					return nil, browser.TransferFailure(ctx, setup, "Browser could not release a website response")
				}
				paused = ""
				continue
			}
			// Chromium can classify a page fetch as XHR in the Fetch domain.
			if event.RedirectedRequestID != "" || event.ResponseErrorReason != "" ||
				event.ResourceType != network.ResourceTypeFetch && event.ResourceType != network.ResourceTypeXHR {
				return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("Browser did not receive the selected file"))
			}
			length, checkEOF, err := download.Check(event.ResponseStatusCode, browser.ResponseHeaders(event.ResponseHeaders))
			if err != nil {
				return nil, err
			}
			stream, err := chromedp.Call(setup, fetch.TakeResponseBodyAsStream, fetch.TakeResponseBodyAsStreamParams{RequestID: paused})
			handle = stream.Stream
			if err != nil || handle == "" {
				return nil, browser.TransferFailure(ctx, setup, "Browser did not expose the file stream")
			}
			return browser.NewBody(life, handle, closeBody, errors.New("Browser file could not be read"), length, checkEOF), nil
		}
		return nil, browser.TransferFailure(ctx, setup, "Browser did not receive the selected file response")
	}
}
