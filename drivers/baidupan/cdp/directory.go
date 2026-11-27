package baiducdp

import (
	"context"
	"encoding/json"
	"errors"
	"iter"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/chromedp/cdproto/cdp"
	"github.com/chromedp/cdproto/network"
	"github.com/chromedp/cdproto/page"
	"github.com/chromedp/chromedp"
	core "github.com/nostalume/mountweave"
	transport "github.com/nostalume/mountweave/browser"
	"github.com/nostalume/mountweave/drivers/baidupan"
)

const (
	drivePath         = "/disk/main"
	listPath          = "/api/list"
	directoryPageSize = 100
	directoryBudget   = 4 << 20
)

type directoryView struct {
	dir    string
	frame  cdp.FrameID
	loader cdp.LoaderID
	number int
	full   bool
}

// ReadPage navigates the normal personal drive and observes its directory
// response. It holds the same tab slot as original-file capture.
func (s *Session) ReadPage(ctx context.Context, dir string, number int) (_ baidupan.WebsitePage, err error) {
	if err := core.ValidatePath(dir); err != nil {
		return baidupan.WebsitePage{}, err
	}
	if number < 1 {
		return baidupan.WebsitePage{}, core.Fail(core.InvalidRequest, nil)
	}
	select {
	case s.slot <- struct{}{}:
	case <-ctx.Done():
		return baidupan.WebsitePage{}, ctx.Err()
	case <-s.tab.Done():
		return baidupan.WebsitePage{}, core.Fail(core.MountUnavailable, nil)
	}
	defer func() {
		if err != nil {
			s.listing = directoryView{}
		}
		<-s.slot
	}()
	if err := ctx.Err(); err != nil {
		return baidupan.WebsitePage{}, err
	}
	if s.broken || s.tab.Err() != nil {
		return baidupan.WebsitePage{}, core.Fail(core.MountUnavailable, nil)
	}

	call, cancel := context.WithTimeout(s.tab, 25*time.Second)
	stop := context.AfterFunc(ctx, cancel)
	defer func() { stop(); cancel() }()
	max, total, noPost := int64(directoryBudget), int64(2*directoryBudget), int64(0)
	if _, err := chromedp.Call(call, network.Enable, network.EnableParams{
		MaxResourceBufferSize: &max, MaxTotalBufferSize: &total, MaxPostDataSize: &noPost,
	}); err != nil {
		return baidupan.WebsitePage{}, transport.TransferFailure(ctx, call, "could not observe Baidu directory receipts")
	}
	request, stopRequests := iter.Pull2(chromedp.Events(call, network.RequestWillBeSent))
	defer stopRequests()
	response, stopResponses := iter.Pull2(chromedp.Events(call, network.ResponseReceived))
	defer stopResponses()
	finished, stopFinished := iter.Pull2(chromedp.Events(call, network.LoadingFinished))
	defer stopFinished()

	frame := topFrame(call)
	view := s.listing
	if view.number == 0 || number != view.number+1 || view.dir != dir || frame == nil ||
		frame.ID != view.frame || frame.LoaderID != view.loader || !s.onDirectory(frame, dir) {
		navigate := chromedp.Navigate(s.origin + drivePath + "#/index?category=all&path=" + url.QueryEscape(dir))
		if s.onDirectory(frame, dir) {
			navigate = chromedp.Reload()
		}
		if err := chromedp.Do(call, navigate); err != nil {
			return baidupan.WebsitePage{}, transport.TransferFailure(ctx, call, "could not open the Baidu personal directory")
		}
		frame = topFrame(call)
		if !s.onDirectory(frame, dir) {
			return baidupan.WebsitePage{}, core.Fail(core.UpstreamAccessDenied, errors.New("Baidu personal directory is unavailable"))
		}
		view = directoryView{dir: dir, frame: frame.ID, loader: frame.LoaderID}
	}

	account, err := s.Account(call)
	if err != nil {
		return baidupan.WebsitePage{}, err
	}
	var body json.RawMessage
	for current := view.number + 1; current <= number; current++ {
		if current > 1 {
			if !view.full {
				return baidupan.WebsitePage{}, core.Fail(core.InvalidRequest, errors.New("Baidu directory reached its final page"))
			}
			if err := s.scrollDirectory(call); err != nil {
				return baidupan.WebsitePage{}, err
			}
		}
		body, err = s.directoryReceipt(ctx, call, &view, current, request, response, finished)
		if err != nil {
			return baidupan.WebsitePage{}, err
		}
		var shape struct {
			Errno *int
			List  []struct{}
		}
		if json.Unmarshal(body, &shape) != nil || shape.Errno == nil {
			return baidupan.WebsitePage{}, core.Fail(core.InvalidUpstreamResponse, errors.New("Baidu directory receipt has invalid JSON"))
		}
		view.number, view.full = current, *shape.Errno == 0 && len(shape.List) == directoryPageSize
	}
	frame = topFrame(call)
	if !s.onDirectory(frame, dir) || frame.ID != view.frame || frame.LoaderID != view.loader {
		return baidupan.WebsitePage{}, core.Fail(core.InvalidUpstreamResponse, errors.New("Baidu directory view changed during read"))
	}
	if err := s.checkAccount(call, account); err != nil {
		return baidupan.WebsitePage{}, err
	}
	if err := ctx.Err(); err != nil {
		return baidupan.WebsitePage{}, err
	}
	s.listing = view
	return baidupan.WebsitePage{Account: account, Directory: dir, Number: number, Body: body}, nil
}

func topFrame(ctx context.Context) *cdp.Frame {
	tree, err := chromedp.Call(ctx, page.GetFrameTree, cdp.Empty{})
	if err != nil || tree.FrameTree == nil {
		return nil
	}
	return tree.FrameTree.Frame
}

func (s *Session) onDirectory(frame *cdp.Frame, dir string) bool {
	if frame == nil || frame.ID == "" || frame.LoaderID == "" {
		return false
	}
	document := httpsURL(frame.URL)
	if document == nil || document.Path != drivePath || document.Scheme+"://"+document.Host != s.origin {
		return false
	}
	hash, err := url.Parse(strings.TrimPrefix(frame.URLFragment, "#"))
	if err != nil || hash.Path != "/index" {
		return false
	}
	query := hash.Query()
	return query.Get("category") == "all" && query.Get("search") == "" && directoryPath(query.Get("path")) == dir
}

func directoryPath(raw string) string {
	if raw == "" {
		return "/"
	}
	return raw
}

func (s *Session) scrollDirectory(ctx context.Context) error {
	const scroll = `(origin => {
		if (location.origin !== origin || window !== window.top) return false;
		const bodies = [...document.querySelectorAll(".wp-s-pan-table__body,.nd-table__body")].filter(el =>
			el.getClientRects().length && /auto|scroll/.test(getComputedStyle(el).overflowY) && el.scrollHeight > el.clientHeight);
		if (bodies.length !== 1) return false;
		bodies[0].scrollTop = bodies[0].scrollHeight;
		return true;
	})(`
	wait, cancel := context.WithTimeout(ctx, 4*time.Second)
	defer cancel()
	for {
		ok, err := chromedp.Run(wait, chromedp.Evaluate[bool](scroll+strconv.Quote(s.origin)+")"))
		if err == nil && ok {
			return nil
		}
		if err != nil || chromedp.Do(wait, chromedp.Sleep(100*time.Millisecond)) != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return core.Fail(core.UnsupportedOperation, errors.New("Baidu native directory scroll is unavailable"))
		}
	}
}

func (s *Session) directoryReceipt(ctx, call context.Context, view *directoryView, number int,
	request func() (network.EventRequestWillBeSent, error, bool), response func() (network.EventResponseReceived, error, bool),
	finished func() (network.EventLoadingFinished, error, bool),
) (json.RawMessage, error) {
	for {
		event, err, ok := response()
		if err != nil || !ok {
			return nil, transport.TransferFailure(ctx, call, "Baidu directory response did not arrive")
		}
		res := event.Response
		if event.FrameID != view.frame || res == nil ||
			(event.Type != network.ResourceTypeXHR && event.Type != network.ResourceTypeFetch) ||
			!s.sameListURL(res.URL, view.dir, number) ||
			res.FromDiskCache || res.FromServiceWorker || res.FromPrefetchCache {
			continue
		}
		if event.LoaderID != view.loader {
			// The first directory view can be replaced during the site's
			// navigation. Bind only to the current top frame of that directory.
			frame := topFrame(call)
			if view.number != 0 || !s.onDirectory(frame, view.dir) ||
				frame.ID != view.frame || frame.LoaderID != event.LoaderID {
				continue
			}
			view.loader = frame.LoaderID
		}
		for {
			eventRequest, err, ok := request()
			if err != nil || !ok {
				return nil, transport.TransferFailure(ctx, call, "Baidu directory request was not observed")
			}
			if eventRequest.RequestID != event.RequestID {
				continue
			}
			if eventRequest.FrameID != view.frame || eventRequest.LoaderID != view.loader ||
				eventRequest.RedirectResponse != nil || eventRequest.Request == nil ||
				eventRequest.Request.Method != http.MethodGet || eventRequest.Request.URL != res.URL {
				return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("Baidu directory response does not match its request"))
			}
			break
		}
		switch res.Status {
		case http.StatusUnauthorized, http.StatusForbidden:
			return nil, core.Fail(core.UpstreamAccessDenied, errors.New("Baidu directory request was denied"))
		case http.StatusOK:
		default:
			return nil, core.Fail(core.UpstreamFailure, errors.New("Baidu directory request failed"))
		}
		for {
			completion, err, ok := finished()
			if err != nil || !ok {
				return nil, transport.TransferFailure(ctx, call, "Baidu directory response did not finish")
			}
			if completion.RequestID == event.RequestID {
				break
			}
		}
		body, err := chromedp.Call(call, network.GetResponseBody, network.GetResponseBodyParams{RequestID: event.RequestID})
		if err != nil {
			return nil, transport.TransferFailure(ctx, call, "could not read the completed Baidu directory response")
		}
		if len(body.Body) > directoryBudget {
			return nil, core.Fail(core.ResponseBudgetExceeded, nil)
		}
		return json.RawMessage(body.Body), nil
	}
}

func (s *Session) sameListURL(address, dir string, number int) bool {
	u := httpsURL(address)
	if u == nil || u.Scheme+"://"+u.Host != s.origin || u.Path != listPath || u.EscapedPath() != u.Path {
		return false
	}
	query, err := url.ParseQuery(u.RawQuery)
	return err == nil && len(query["dir"]) <= 1 && len(query["page"]) == 1 && len(query["num"]) == 1 &&
		directoryPath(query.Get("dir")) == dir && query.Get("page") == strconv.Itoa(number) &&
		query.Get("num") == strconv.Itoa(directoryPageSize)
}
