package pan115cdp

import (
	"context"
	"errors"
	"io"
	"mime"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/chromedp/cdproto/browser"
	"github.com/chromedp/cdproto/cdp"
	"github.com/chromedp/cdproto/fetch"
	streamio "github.com/chromedp/cdproto/io"
	"github.com/chromedp/cdproto/network"
	"github.com/chromedp/cdproto/page"
	"github.com/chromedp/cdproto/runtime"
	"github.com/chromedp/cdproto/target"
	"github.com/chromedp/chromedp"
	core "github.com/nostalume/mountweave"
	transport "github.com/nostalume/mountweave/browser"
	"github.com/nostalume/mountweave/drivers/pan115"
)

type capturedBody struct {
	body io.ReadCloser
	err  error
}

// Read triggers the current website's ordinary download and hands off the
// already authorized response from its related tab. The returned body owns
// the selection slot until its transport has been released.
func (s *Session) Read(ctx context.Context, account, parentID string, file pan115.WebsiteItem) (io.ReadCloser, error) {
	if account == "" || !validDirectoryID(parentID) || file.ParentID != parentID || !validDirectoryID(file.ID) ||
		file.Directory || file.Size < 0 || file.Offset < 0 || !core.ValidName(file.Name) {
		return nil, core.Fail(core.InvalidRequest, nil)
	}
	life, release, err := s.borrow(ctx)
	if err != nil {
		return nil, err
	}
	closeSelection := sync.OnceFunc(func() {
		clean, cancel := context.WithTimeout(context.WithoutCancel(life), 2*time.Second)
		defer cancel()
		if state, err := selection(clean, file); err == nil && state.Bound && state.Selected == 1 && state.Row.visible() {
			_ = state.Row.click(clean)
		}
		release()
	})
	setup, finish := context.WithTimeout(life, 20*time.Second)
	defer finish()
	view, err := s.showDirectory(setup, account, parentID, file.Offset, false)
	if err != nil {
		release()
		return nil, err
	}
	index := slices.IndexFunc(view.Items, func(current pan115.WebsiteItem) bool {
		return !current.Directory && current.ID == file.ID && current.Name == file.Name && current.Size == file.Size
	})
	if index < 0 {
		release()
		return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("115 website file changed after directory resolution"))
	}
	button, err := prepareFile(setup, file, index)
	if err != nil {
		closeSelection()
		if core.CodeOf(err) != core.UpstreamAccessDenied && core.CodeOf(err) != core.InvalidUpstreamResponse {
			err = transport.TransferFailure(ctx, setup, "115 could not open the ordinary download")
		}
		return nil, err
	}
	body, err := s.captureDownload(life, setup, button, file.Size, closeSelection)
	if err != nil {
		closeSelection()
	}
	return body, err
}

func (s *Session) captureDownload(life, setup context.Context, button control, size int64, release func()) (io.ReadCloser, error) {
	own, err := chromedp.Call(setup, target.GetTargetInfo, target.GetTargetInfoParams{})
	if err != nil || own.TargetInfo == nil {
		return nil, transport.TransferFailure(life, setup, "could not identify the 115 download page")
	}
	if _, err = chromedp.CallBrowser(setup, browser.SetDownloadBehavior, browser.SetDownloadBehaviorParams{
		Behavior: browser.SetDownloadBehaviorBehaviorDeny, EventsEnabled: new(true), BrowserContextID: own.TargetInfo.BrowserContextID,
	}); err != nil {
		return nil, transport.TransferFailure(life, setup, "could not configure the 115 download page")
	}
	watch, stop := context.WithCancel(setup)
	defer stop()
	windows := chromedp.Events(watch, page.WindowOpen)
	tabs := chromedp.BrowserEvents(watch, target.AttachedToTarget)
	if _, err = chromedp.CallBrowser(setup, target.SetAutoAttach, target.SetAutoAttachParams{
		AutoAttach: true, WaitForDebuggerOnStart: true, Flatten: new(true),
		Filter: target.Filter{{Type: "page"}, {Exclude: true}},
	}); err != nil {
		return nil, transport.TransferFailure(life, setup, "could not observe 115 download tabs")
	}
	finish := func() error {
		clean, cancel := context.WithTimeout(context.WithoutCancel(life), 2*time.Second)
		defer cancel()
		_, err := chromedp.CallBrowser(clean, target.SetAutoAttach, target.SetAutoAttachParams{Flatten: new(true)})
		if err != nil {
			return core.Fail(core.UpstreamFailure, errors.New("could not clear 115 download tab observation"))
		}
		return nil
	}
	result := make(chan capturedBody, 1)
	go func() {
		for event, err := range tabs {
			if err != nil {
				break
			}
			if !event.WaitingForDebugger {
				continue
			}
			info := event.TargetInfo
			if info == nil || info.Type != "page" || info.Subtype != "" ||
				info.OpenerID != own.TargetInfo.TargetID || info.BrowserContextID != own.TargetInfo.BrowserContextID {
				releaseDownloadHold(life, event.SessionID)
				continue
			}
			address := ""
			for opened, err := range windows {
				if err == nil && opened.UserGesture && downloadAddress(opened.URL) {
					address = opened.URL
				}
				break
			}
			body, err := s.readDownloadChild(life, watch, event, address, size, release)
			result <- capturedBody{body, err}
			return
		}
		result <- capturedBody{err: transport.TransferFailure(life, setup, "115 did not open a related download tab")}
	}()
	if err = button.click(watch); err != nil {
		stop()
	}
	opened := <-result
	if finishErr := finish(); finishErr != nil {
		if opened.body != nil {
			finishErr = errors.Join(finishErr, opened.body.Close())
		}
		return nil, errors.Join(opened.err, finishErr)
	}
	if err != nil && opened.body != nil {
		return nil, errors.Join(transport.TransferFailure(life, setup, "could not select ordinary 115 download"), opened.body.Close())
	}
	return opened.body, opened.err
}

func releaseDownloadHold(ctx context.Context, session target.SessionID) {
	clean, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
	defer cancel()
	_, _ = chromedp.CallBrowser(clean, target.DetachFromTarget, target.DetachFromTargetParams{SessionID: session})
}

func (s *Session) closeTransfer(ctx context.Context) error {
	id := s.pending
	if _, err := chromedp.CallBrowser(ctx, target.CloseTarget, target.CloseTargetParams{TargetID: id}); err != nil {
		live, listErr := chromedp.CallBrowser(ctx, target.GetTargets, target.GetTargetsParams{})
		if listErr != nil || slices.ContainsFunc(live.TargetInfos, func(info *target.Info) bool { return info != nil && info.TargetID == id }) {
			return core.Fail(core.UpstreamFailure, errors.New("could not close the 115 transfer tab"))
		}
	}
	s.pending = ""
	return nil
}

func (s *Session) readDownloadChild(life, watch context.Context, event target.EventAttachedToTarget, address string, size int64, release func()) (_ io.ReadCloser, err error) {
	id := event.TargetInfo.TargetID
	s.pending = id
	child, detach := chromedp.NewContext(context.WithoutCancel(life), chromedp.WithTargetID(id), chromedp.WithDetachOnCancel())
	hold := sync.OnceFunc(func() { releaseDownloadHold(life, event.SessionID) })
	defer hold()
	bodyCtx, stop := context.WithCancel(child)
	stopRead := context.AfterFunc(life, stop)
	var handle streamio.StreamHandle
	var request fetch.RequestID
	cleanup := sync.OnceValue(func() error {
		stopRead()
		stop()
		clean, cancel := context.WithTimeout(context.WithoutCancel(child), 2*time.Second)
		defer cancel()
		defer detach()
		if handle != "" {
			_, _ = chromedp.Call(clean, streamio.Close, streamio.CloseParams{Handle: handle})
		}
		if request != "" {
			_, _ = chromedp.Call(clean, fetch.FailRequest, fetch.FailRequestParams{RequestID: request, ErrorReason: network.ErrorReasonAborted})
		}
		return s.closeTransfer(clean)
	})
	defer func() {
		if err != nil {
			err = errors.Join(err, cleanup())
		}
	}()
	if address == "" {
		return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("115 did not confirm the ordinary download destination"))
	}
	stopSetup := context.AfterFunc(watch, detach)
	if err := chromedp.Do(child); err != nil || !stopSetup() {
		stopSetup()
		return nil, transport.TransferFailure(life, watch, "could not attach the 115 transfer tab")
	}
	capture, cancel := context.WithCancel(child)
	stopCapture := context.AfterFunc(watch, cancel)
	defer stopCapture()
	defer cancel()
	frame, err := topFrame(capture)
	if err != nil {
		return nil, transport.TransferFailure(life, capture, "could not identify the 115 transfer frame")
	}
	replies := chromedp.Events(capture, fetch.RequestPaused)
	if _, err := chromedp.Call(capture, fetch.Enable, fetch.EnableParams{Patterns: []*fetch.RequestPattern{
		{RequestStage: fetch.RequestStageResponse, ResourceType: network.ResourceTypeDocument},
	}}); err != nil {
		return nil, transport.TransferFailure(life, capture, "could not observe the 115 attachment")
	}
	hold()
	if _, err := chromedp.Call(capture, runtime.RunIfWaitingForDebugger, cdp.Empty{}); err != nil {
		return nil, transport.TransferFailure(life, capture, "could not resume the 115 transfer tab")
	}
	var previous fetch.RequestID
	redirects := 0
	for reply, err := range replies {
		if err != nil {
			break
		}
		// A hop must belong to the current child document and to the exact
		// paused request that authorized this destination.
		if reply.FrameID != frame.ID || reply.Request == nil || reply.Request.URL != address ||
			reply.Request.Method != http.MethodGet || reply.RedirectedRequestID != previous {
			_, err := chromedp.Call(capture, fetch.ContinueResponse, fetch.ContinueResponseParams{RequestID: reply.RequestID})
			if err != nil {
				break
			}
			continue
		}
		request = reply.RequestID
		switch reply.ResponseStatusCode {
		case http.StatusMovedPermanently, http.StatusFound, http.StatusSeeOther,
			http.StatusTemporaryRedirect, http.StatusPermanentRedirect:
			next, err := attachmentDestination(reply, address)
			if err != nil {
				return nil, err
			}
			if redirects == 5 {
				return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("115 attachment redirect limit exceeded"))
			}
			if _, err := chromedp.Call(capture, fetch.ContinueResponse, fetch.ContinueResponseParams{RequestID: request}); err != nil {
				return nil, transport.TransferFailure(life, capture, "could not continue the 115 attachment")
			}
			address, previous, request = next, request, ""
			redirects++
			continue
		}
		if err := validateAttachment(reply, size); err != nil {
			return nil, err
		}
		stream, err := chromedp.Call(capture, fetch.TakeResponseBodyAsStream, fetch.TakeResponseBodyAsStreamParams{RequestID: request})
		if err != nil || stream.Stream == "" {
			return nil, transport.TransferFailure(life, capture, "could not obtain the 115 response stream")
		}
		handle = stream.Stream
		closeBody := sync.OnceValue(func() error {
			defer release()
			return cleanup()
		})
		return transport.NewBody(bodyCtx, handle, closeBody, errors.New("could not read the 115 attachment"), size, true), nil
	}
	return nil, transport.TransferFailure(life, capture, "115 did not return the selected attachment")
}

func downloadAddress(raw string) bool {
	u, err := url.Parse(raw)
	return err == nil && u.Scheme == "https" && u.User == nil && u.Hostname() != "" && u.Fragment == "" && u.Opaque == ""
}

func attachmentDestination(reply fetch.EventRequestPaused, address string) (string, error) {
	failure := errors.New("115 returned an invalid attachment redirect")
	locations := transport.ResponseHeaders(reply.ResponseHeaders).Values("Location")
	if reply.ResponseErrorReason != "" || reply.NetworkID == "" || len(locations) != 1 {
		return "", core.Fail(core.InvalidUpstreamResponse, failure)
	}
	current, _ := url.Parse(address) // The window destination and prior hops have already been checked.
	location := strings.TrimSpace(locations[0])
	next, err := current.Parse(location)
	if location == "" || err != nil || !downloadAddress(next.String()) {
		return "", core.Fail(core.InvalidUpstreamResponse, failure)
	}
	return next.String(), nil
}

func validateAttachment(reply fetch.EventRequestPaused, size int64) error {
	switch reply.ResponseStatusCode {
	case http.StatusUnauthorized, http.StatusForbidden:
		return core.Fail(core.UpstreamAccessDenied, errors.New("115 refused the selected attachment"))
	case http.StatusTooManyRequests:
		return core.Fail(core.UpstreamThrottled, nil)
	}
	headers := transport.ResponseHeaders(reply.ResponseHeaders)
	dispositions, lengths, encodings := headers.Values("Content-Disposition"), headers.Values("Content-Length"), headers.Values("Content-Encoding")
	kind, _, err := mime.ParseMediaType(headers.Get("Content-Disposition"))
	if reply.ResponseStatusCode != http.StatusOK || reply.ResponseErrorReason != "" || reply.NetworkID == "" ||
		err != nil || len(dispositions) != 1 || kind != "attachment" ||
		len(lengths) > 1 || len(lengths) == 1 && lengths[0] != strconv.FormatInt(size, 10) ||
		len(encodings) > 1 || len(encodings) == 1 && !strings.EqualFold(encodings[0], "identity") ||
		len(headers.Values("Content-Range")) != 0 {
		return core.Fail(core.InvalidUpstreamResponse, errors.New("115 did not return the complete bound file"))
	}
	return nil
}
