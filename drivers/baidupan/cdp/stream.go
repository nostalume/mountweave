// Package baiducdp observes ordinary downloads in a dedicated Baidu website tab.
package baiducdp

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"mime"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/chromedp/cdproto/browser"
	"github.com/chromedp/cdproto/cdp"
	"github.com/chromedp/cdproto/fetch"
	browserio "github.com/chromedp/cdproto/io"
	"github.com/chromedp/cdproto/network"
	"github.com/chromedp/cdproto/page"
	"github.com/chromedp/cdproto/target"
	"github.com/chromedp/chromedp"
	core "github.com/nostalume/mountweave"
	transport "github.com/nostalume/mountweave/browser"
	"github.com/nostalume/mountweave/drivers/baidupan"
)

const Origin = "https://pan.baidu.com"

// WebsiteURL opens the personal drive in a dedicated browser tab.
const WebsiteURL = Origin + drivePath

// New borrows an initialized tab in a dedicated browser context. The host
// continues to own its lifetime. Use a single Session per tab.
func New(tab context.Context) *Session {
	return &Session{tab: tab, origin: Origin, slot: make(chan struct{}, 1)}
}

// NewWithGuardedDownloads allows the session to start the site's file action.
// The host must verify native external navigation blocking and prevent local
// network access before handing over the tab.
func NewWithGuardedDownloads(tab context.Context) *Session {
	s := New(tab)
	s.guardedDownloads = true
	return s
}

type Session struct {
	tab              context.Context
	origin           string
	slot             chan struct{}
	guardedDownloads bool
	broken           bool          // guarded by slot
	listing          directoryView // guarded by slot
}

// Account returns the opaque identity of the authenticated website account.
func (s *Session) Account(ctx context.Context) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	call, cancel := context.WithTimeout(s.tab, 3*time.Second)
	stop := context.AfterFunc(ctx, cancel)
	defer func() { stop(); cancel() }()
	id, err := chromedp.Run(call, chromedp.Evaluate[string](accountScript+strconv.Quote(s.origin)+")", chromedp.EvalAwaitPromise))
	if ctx.Err() != nil {
		return "", ctx.Err()
	}
	if err != nil {
		return "", transport.TransferFailure(ctx, call, "could not confirm the Baidu website account")
	}
	if id == "" {
		return "", core.Fail(core.UpstreamAccessDenied, errors.New("Baidu website sign-in required"))
	}
	return id, nil
}

const accountScript = `(async origin => {
	if (location.origin !== origin || window !== window.top || ![true,1,"1"].includes(window.locals?.isLogin)) return "";
	const value = window.locals.uk ?? window.locals.userInfo?.uk;
	if ((typeof value !== "string" && !Number.isSafeInteger(value)) || !/^[1-9][0-9]{0,24}$/.test(String(value))) return "";
	const hash = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(String(value)));
	return Array.from(new Uint8Array(hash), byte => byte.toString(16).padStart(2, "0")).join("");
})(`

// CaptureOriginal invokes begin after opening the receipt listeners. The action
// must use the current website's ordinary download flow for the selected file
// and honor cancellation. Unavailable, paid or interactive verification
// outcomes should be returned by the action. The response owns the exclusive
// tab slot until its body closes.
func (s *Session) CaptureOriginal(ctx context.Context, file baidupan.WebsiteFile, begin func(context.Context) error) (result baidupan.WebsiteDownload, err error) {
	return s.captureOriginal(ctx, file, begin, nil)
}

func (s *Session) captureOriginal(ctx context.Context, file baidupan.WebsiteFile, begin, watch func(context.Context) error) (result baidupan.WebsiteDownload, err error) {
	if file.ID == 0 || file.Account == "" || file.Size < 0 || begin == nil {
		return result, core.Fail(core.InvalidRequest, nil)
	}
	select {
	case s.slot <- struct{}{}:
	case <-ctx.Done():
		return result, ctx.Err()
	case <-s.tab.Done():
		return result, core.Fail(core.MountUnavailable, nil)
	}
	s.listing = directoryView{}
	life, cancel := context.WithCancel(s.tab)
	stop := context.AfterFunc(ctx, cancel)
	setup, finishSetup := context.WithTimeout(life, 25*time.Second)
	defer finishSetup()

	var readers sync.WaitGroup
	var handle browserio.StreamHandle
	var paused fetch.RequestID
	fetchEnabled := false
	closeBody := sync.OnceValue(func() error {
		stop()
		cancel()
		defer func() { <-s.slot }()
		readers.Wait()
		if !fetchEnabled || s.tab.Err() != nil {
			return nil
		}
		clean, finish := context.WithTimeout(s.tab, 2*time.Second)
		defer finish()
		var cleanupErr error
		if handle != "" {
			if _, err := chromedp.Call(clean, browserio.Close, browserio.CloseParams{Handle: handle}); err != nil {
				cleanupErr = core.Fail(core.UpstreamFailure, errors.New("Baidu browser stream did not close"))
			}
		}
		if paused != "" {
			_, _ = chromedp.Call(clean, fetch.FailRequest, fetch.FailRequestParams{RequestID: paused, ErrorReason: network.ErrorReasonAborted})
		}
		if _, err := chromedp.Call(clean, fetch.Disable, cdp.Empty{}); err != nil {
			s.broken = true
			cleanupErr = errors.Join(cleanupErr, core.Fail(core.UpstreamFailure, errors.New("Baidu browser observation did not close")))
		}
		return cleanupErr
	})
	defer func() {
		if result.Response == nil {
			err = errors.Join(err, closeBody())
		}
	}()
	if err := ctx.Err(); err != nil {
		return result, err
	}
	if s.broken || s.tab.Err() != nil {
		return result, core.Fail(core.MountUnavailable, nil)
	}
	if err := s.checkAccount(setup, file.Account); err != nil {
		return result, err
	}
	tree, err := chromedp.Call(setup, page.GetFrameTree, cdp.Empty{})
	if err != nil || tree.FrameTree == nil || tree.FrameTree.Frame == nil {
		return result, transport.TransferFailure(ctx, setup, "could not identify the Baidu drive page")
	}
	browserContext := chromedp.FromContext(s.tab).BrowserContextID
	if browserContext == "" {
		// Chromedp can attach to an existing target without recording its context.
		current, err := chromedp.Call(setup, target.GetTargetInfo, target.GetTargetInfoParams{})
		if err != nil || current.TargetInfo == nil {
			return result, transport.TransferFailure(ctx, setup, "could not identify the Baidu browser context")
		}
		browserContext = current.TargetInfo.BrowserContextID
	}
	if _, err := chromedp.CallBrowser(setup, browser.SetDownloadBehavior, browser.SetDownloadBehaviorParams{
		Behavior:         browser.SetDownloadBehaviorBehaviorDeny,
		BrowserContextID: browserContext,
	}); err != nil {
		return result, transport.TransferFailure(ctx, setup, "could not disable browser-managed saving")
	}
	max, total, noPost := int64(1<<20), int64(4<<20), int64(0)
	if _, err := chromedp.Call(setup, network.Enable, network.EnableParams{
		MaxResourceBufferSize: &max, MaxTotalBufferSize: &total, MaxPostDataSize: &noPost,
	}); err != nil {
		return result, transport.TransferFailure(ctx, setup, "could not observe the Baidu download grant")
	}
	receipt := newDownloadReceipt(s.origin, tree.FrameTree.Frame.ID)
	requests := chromedp.Events(setup, network.RequestWillBeSent)
	responses := chromedp.Events(setup, network.ResponseReceived)
	attachments := chromedp.Events(setup, fetch.RequestPaused)
	patterns := []*fetch.RequestPattern{
		{ResourceType: network.ResourceTypeDocument, RequestStage: fetch.RequestStageResponse},
		{ResourceType: network.ResourceTypeOther, RequestStage: fetch.RequestStageResponse},
	}
	fetchEnabled = true
	if _, err := chromedp.Call(setup, fetch.Enable, fetch.EnableParams{Patterns: patterns}); err != nil {
		return result, transport.TransferFailure(ctx, setup, "could not observe the Baidu attachment")
	}
	readers.Go(func() {
		for event, err := range requests {
			if err != nil {
				return
			}
			receipt.request(event)
		}
	})
	readers.Go(func() {
		for event, err := range responses {
			if err != nil {
				return
			}
			if err := receipt.grant(setup, file.ID, event); err != nil {
				receipt.fail(err)
				finishSetup()
				return
			}
		}
	})
	started := make(chan error, 1)
	readers.Go(func() {
		err := begin(setup)
		started <- err
		if err == nil && watch != nil {
			err = watch(setup)
		}
		if err != nil && setup.Err() == nil {
			receipt.fail(err)
			finishSetup()
		}
	})
	for event, eventErr := range attachments {
		if eventErr != nil {
			break
		}
		paused = event.RequestID
		target, headers, length := matchableAttachment(event, file.Size)
		if target != nil && receipt.matches(setup, event.NetworkID, event.FrameID, target.String()) {
			select {
			case err := <-started:
				if err != nil {
					return result, err
				}
			case <-setup.Done():
				return result, transport.TransferFailure(ctx, setup, "Baidu website download action did not finish")
			}
			if err := s.checkAccount(setup, file.Account); err != nil {
				return result, err
			}
			stream, err := chromedp.Call(setup, fetch.TakeResponseBodyAsStream, fetch.TakeResponseBodyAsStreamParams{RequestID: paused})
			handle = stream.Stream
			if err != nil || handle == "" {
				return result, transport.TransferFailure(ctx, setup, "browser did not expose the Baidu file stream")
			}
			response := &http.Response{StatusCode: http.StatusOK, Header: headers, ContentLength: length,
				Request: &http.Request{Method: http.MethodGet, URL: target},
				Body:    transport.NewBody(life, handle, closeBody, errors.New("Baidu browser file could not be read"), file.Size, true)}
			return baidupan.WebsiteDownload{File: file, Response: response}, nil
		}
		if _, err := chromedp.Call(setup, fetch.ContinueResponse, fetch.ContinueResponseParams{RequestID: paused}); err != nil {
			break
		}
		paused = ""
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	receipt.mu.Lock()
	failure := receipt.failure
	receipt.mu.Unlock()
	if failure != nil {
		return result, failure
	}
	return result, transport.TransferFailure(ctx, setup, "browser did not receive the selected Baidu attachment")
}

func (s *Session) checkAccount(ctx context.Context, expected string) error {
	account, err := s.Account(ctx)
	if err != nil {
		return err
	}
	if account != expected {
		return core.Fail(core.UpstreamAccessDenied, errors.New("Baidu website account changed"))
	}
	return nil
}

func httpsURL(address string) *url.URL {
	u, err := url.Parse(address)
	if err != nil || u.Scheme != "https" || u.Hostname() == "" || u.User != nil || u.Opaque != "" || u.Fragment != "" {
		return nil
	}
	return u
}

func matchableAttachment(event fetch.EventRequestPaused, size int64) (*url.URL, http.Header, int64) {
	if event.Request == nil || event.Request.Method != http.MethodGet || event.NetworkID == "" ||
		event.FrameID == "" || event.ResponseErrorReason != "" || event.ResponseStatusCode != http.StatusOK {
		return nil, nil, 0
	}
	headers := transport.ResponseHeaders(event.ResponseHeaders)
	disposition, _, _ := mime.ParseMediaType(headers.Get("Content-Disposition"))
	encoding := strings.ToLower(headers.Get("Content-Encoding"))
	if disposition != "attachment" || len(headers.Values("Content-Disposition")) != 1 || headers.Get("Content-Range") != "" ||
		(encoding != "" && encoding != "identity") {
		return nil, nil, 0
	}
	length := int64(-1)
	if values := headers.Values("Content-Length"); len(values) > 0 {
		var err error
		length, err = strconv.ParseInt(values[0], 10, 64)
		if err != nil || len(values) != 1 || length != size {
			return nil, nil, 0
		}
	}
	return httpsURL(event.Request.URL), headers, length
}

type downloadTrace struct {
	first, last [sha256.Size]byte
	frame       cdp.FrameID
}

type downloadReceipt struct {
	origin string
	top    cdp.FrameID

	mu      sync.Mutex
	grants  map[network.RequestID]string
	traces  map[network.RequestID]downloadTrace
	address string
	failure error
	updated chan struct{}
}

func newDownloadReceipt(origin string, top cdp.FrameID) *downloadReceipt {
	return &downloadReceipt{origin: origin, top: top, grants: make(map[network.RequestID]string),
		traces: make(map[network.RequestID]downloadTrace), updated: make(chan struct{}, 1)}
}

func (r *downloadReceipt) changed() {
	select {
	case r.updated <- struct{}{}:
	default:
	}
}

func (r *downloadReceipt) fail(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.failure == nil {
		r.failure = err
		r.changed()
	}
}

func (r *downloadReceipt) request(event network.EventRequestWillBeSent) {
	if event.Request == nil || event.Request.Method != http.MethodGet {
		return
	}
	u := httpsURL(event.Request.URL)
	r.mu.Lock()
	defer r.mu.Unlock()
	if u != nil && u.Scheme+"://"+u.Host == r.origin && u.Path == "/api/download" && event.FrameID == r.top &&
		u.Query().Get("type") == "dlink" && len(r.grants) < 32 {
		r.grants[event.RequestID] = u.Query().Get("fidlist")
		r.changed()
	}
	if event.Type != network.ResourceTypeDocument || event.FrameID == "" || event.FrameID == r.top {
		return
	}
	if u == nil {
		delete(r.traces, event.RequestID)
		return
	}
	fingerprint := sha256.Sum256([]byte(event.Request.URL))
	if event.RedirectResponse == nil {
		if len(r.traces) < 128 {
			r.traces[event.RequestID] = downloadTrace{first: fingerprint, last: fingerprint, frame: event.FrameID}
			r.changed()
		}
		return
	}
	trace, found := r.traces[event.RequestID]
	switch event.RedirectResponse.Status {
	case 301, 302, 303, 307, 308:
		if found && trace.frame == event.FrameID && trace.last == sha256.Sum256([]byte(event.RedirectResponse.URL)) {
			trace.last = fingerprint
			r.traces[event.RequestID] = trace
			r.changed()
			return
		}
	}
	delete(r.traces, event.RequestID)
}

func (r *downloadReceipt) grant(ctx context.Context, id uint64, event network.EventResponseReceived) error {
	if event.Response == nil {
		return nil
	}
	u := httpsURL(event.Response.URL)
	if u == nil || u.Scheme+"://"+u.Host != r.origin || u.Path != "/api/download" || u.Query().Get("type") != "dlink" {
		return nil
	}
	proof, cancel := context.WithTimeout(ctx, 600*time.Millisecond)
	defer cancel()
	var selected string
	for selected == "" {
		r.mu.Lock()
		selected = r.grants[event.RequestID]
		r.mu.Unlock()
		if selected == "" {
			select {
			case <-proof.Done():
				return nil
			case <-time.After(20 * time.Millisecond):
			}
		}
	}
	if event.Response.Status == http.StatusUnauthorized || event.Response.Status == http.StatusForbidden {
		return core.Fail(core.UpstreamAccessDenied, errors.New("Baidu denied the ordinary download"))
	}
	if event.Response.Status != http.StatusOK || event.Response.FromDiskCache || event.Response.FromServiceWorker ||
		!strings.Contains(event.Response.MimeType, "json") {
		return core.Fail(core.InvalidUpstreamResponse, errors.New("invalid Baidu download grant response"))
	}
	var body json.RawMessage
	for body == nil {
		result, err := chromedp.Call(proof, network.GetResponseBody, network.GetResponseBodyParams{RequestID: event.RequestID})
		if err == nil {
			body = result.Body
			break
		}
		select {
		case <-proof.Done():
			return transport.TransferFailure(ctx, proof, "could not read the Baidu download grant")
		case <-time.After(35 * time.Millisecond):
		}
	}
	address, err := baidupan.WebsiteDownloadTarget(id, []byte(selected), body)
	if err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.address != "" && r.address != address {
		return core.Fail(core.InvalidUpstreamResponse, errors.New("Baidu returned two download grants"))
	}
	r.address = address
	r.changed()
	return nil
}

func (r *downloadReceipt) matches(ctx context.Context, id network.RequestID, frame cdp.FrameID, final string) bool {
	if frame == r.top {
		return false
	}
	deadline := time.NewTimer(1500 * time.Millisecond)
	defer deadline.Stop()
	for {
		r.mu.Lock()
		trace, found := r.traces[id]
		matched := found && r.failure == nil && r.address != "" && frame == trace.frame &&
			trace.first == sha256.Sum256([]byte(r.address)) && trace.last == sha256.Sum256([]byte(final))
		r.mu.Unlock()
		if matched {
			return true
		}
		select {
		case <-ctx.Done():
			return false
		case <-deadline.C:
			return false
		case <-r.updated:
		}
	}
}
