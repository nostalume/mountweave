// Package pan123cdp drives the normal file page of an isolated 123 Pan session.
package pan123cdp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/chromedp/cdproto/browser"
	"github.com/chromedp/cdproto/cdp"
	"github.com/chromedp/cdproto/network"
	"github.com/chromedp/cdproto/page"
	"github.com/chromedp/cdproto/target"
	"github.com/chromedp/chromedp"
	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/pan123"
)

const Origin = "https://yun.123pan.cn"

const (
	accountPath = "/b/api/user/info"
	listPath    = "/b/api/file/list/new"
	trafficPath = "/b/api/file/download/traffic/check"
	grantPath   = "/b/api/v2/file/download_info"
	maxReceipt  = 512 << 10
)

// New borrows an initialized tab in a dedicated browser context. The host owns
// the tab, and a single Session serializes its file page actions.
func New(tab context.Context) *Session {
	return &Session{tab: tab, origin: Origin, turn: make(chan struct{}, 1)}
}

type Session struct {
	tab    context.Context
	origin string
	turn   chan struct{}
	view   directory
}

var _ pan123.WebsiteSession = (*Session)(nil)

func (s *Session) Account(ctx context.Context) (account string, err error) {
	err = s.run(ctx, func(ctx context.Context, receipts *observation) error {
		view, err := s.openDirectory(ctx, receipts, "0")
		account = view.account
		return err
	})
	return account, err
}

func (s *Session) List(ctx context.Context, parentID, cursor string) (result pan123.WebsitePage, err error) {
	if !validParent(parentID) || len(cursor) > 256 || cursor == "0" || cursor == "-1" {
		return result, core.Fail(core.InvalidRequest, nil)
	}
	err = s.run(ctx, func(ctx context.Context, receipts *observation) error {
		view := s.view
		if cursor != "" && view.parent == parentID && view.next == cursor {
			receipts.frame, receipts.loader = view.frame, view.loader
			return s.next(ctx, receipts, &view, &result)
		}
		view, err := s.openDirectory(ctx, receipts, parentID)
		if err != nil {
			return err
		}
		if err := s.list(ctx, receipts, &view, "", &result); err != nil || cursor == "" {
			return err
		}
		// Another operation may have navigated away between pages.
		seen := make(map[string]struct{})
		for view.next != "" {
			if _, duplicate := seen[view.next]; duplicate {
				return invalid("123 Pan directory pagination repeated a marker")
			}
			seen[view.next] = struct{}{}
			selected := view.next == cursor
			if err := s.next(ctx, receipts, &view, &result); err != nil || selected {
				return err
			}
		}
		return core.Fail(core.InvalidRequest, errors.New("123 Pan directory cursor is no longer present"))
	})
	return result, err
}

func (s *Session) Download(ctx context.Context, parentID string, file pan123.WebsiteFile) (result pan123.WebsiteGrant, err error) {
	if !validParent(parentID) || !validID(file.ID) || file.Account == "" || !core.ValidName(file.Name) || file.Size < 0 {
		return result, core.Fail(core.InvalidRequest, nil)
	}
	err = s.run(ctx, func(ctx context.Context, receipts *observation) error {
		// The UI may try to save a file after issuing the grant. The independent
		// request owns the body, so saving in this dedicated context is disabled.
		info, err := chromedp.Call(ctx, target.GetTargetInfo, target.GetTargetInfoParams{})
		if err != nil || info.TargetInfo == nil {
			return invalid("could not identify the 123 Pan browser context")
		}
		if _, err := chromedp.CallBrowser(ctx, browser.SetDownloadBehavior, browser.SetDownloadBehaviorParams{
			Behavior: browser.SetDownloadBehaviorBehaviorDeny, BrowserContextID: info.TargetInfo.BrowserContextID,
		}); err != nil {
			return invalid("could not disable 123 Pan browser-managed saving")
		}
		view, err := s.openDirectory(ctx, receipts, parentID)
		if err != nil {
			return err
		}
		if view.account != file.Account {
			return pan123.ErrWebsiteBindingMismatch
		}
		if err := s.findFile(ctx, receipts, &view, file); err != nil {
			return err
		}
		if err := s.selectFile(ctx, file.ID); err != nil {
			return err
		}
		address, err := s.ordinaryGrant(ctx, receipts, file.ID)
		if err == nil {
			result = pan123.WebsiteGrant{Account: view.account, FileID: file.ID, URL: address}
		}
		return err
	})
	return result, err
}

func (s *Session) run(ctx context.Context, action func(context.Context, *observation) error) error {
	select {
	case s.turn <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	case <-s.tab.Done():
		return core.Fail(core.MountUnavailable, nil)
	}
	defer func() { <-s.turn }()
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.tab.Err() != nil {
		return core.Fail(core.MountUnavailable, nil)
	}
	call, cancel := context.WithTimeout(s.tab, 25*time.Second)
	stop := context.AfterFunc(ctx, cancel)
	defer stop()

	max, total, noPost := int64(maxReceipt), int64(4*maxReceipt), int64(0)
	_, err := chromedp.Call(call, network.Enable, network.EnableParams{
		MaxResourceBufferSize: &max, MaxTotalBufferSize: &total, MaxPostDataSize: &noPost,
	})
	var observers sync.WaitGroup
	if err == nil {
		observed := make(chan receipt, 24)
		events := chromedp.Events(call, network.ResponseReceived)
		observers.Go(func() {
			defer close(observed)
			for ev, err := range events {
				if err != nil {
					return
				}
				reply, ok := s.receipt(&ev)
				if !ok {
					continue
				}
				select {
				case observed <- reply:
				case <-call.Done():
					return
				}
			}
		})
		err = action(call, &observation{replies: observed})
	} else {
		err = invalid("could not observe 123 Pan website receipts")
	}
	deadline := call.Err()
	cancel()
	observers.Wait()
	switch {
	case ctx.Err() != nil:
		s.view = directory{}
		return ctx.Err()
	case s.tab.Err() != nil:
		s.view = directory{}
		return core.Fail(core.MountUnavailable, nil)
	case errors.Is(deadline, context.DeadlineExceeded):
		s.view = directory{}
		return core.Fail(core.UpstreamTimeout, err)
	case err != nil:
		s.view = directory{}
	}
	return err
}

type directory struct {
	frame   cdp.FrameID
	loader  cdp.LoaderID
	account string
	parent  string
	next    string
}

func (s *Session) openDirectory(ctx context.Context, observed *observation, parent string) (directory, error) {
	s.view = directory{}
	address := s.origin + "/"
	if parent != "0" {
		address += "?homeFilePath=" + parent
	}
	if err := chromedp.Do(ctx, chromedp.Navigate(address)); err != nil {
		return directory{}, invalid("could not open the 123 Pan file page")
	}
	tree, err := chromedp.Call(ctx, page.GetFrameTree, cdp.Empty{})
	if err != nil || tree.FrameTree == nil || tree.FrameTree.Frame == nil {
		return directory{}, invalid("could not identify the 123 Pan file page")
	}
	frame := tree.FrameTree.Frame
	location, err := url.Parse(frame.URL)
	if err != nil || location.Scheme+"://"+location.Host != s.origin || location.Path != "/" && location.Path != "/home" {
		return directory{}, pan123.ErrWebsiteSignIn
	}
	observed.frame, observed.loader = frame.ID, frame.LoaderID
	var account struct {
		UID nativeNumber
	}
	wait, cancel := context.WithTimeout(ctx, 7*time.Second)
	defer cancel()
	if _, err := observed.await(wait, accountPath, nil, &account); err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			signIn, _ := chromedp.Run(ctx, chromedp.Evaluate[bool](signInScript))
			if signIn {
				return directory{}, pan123.ErrWebsiteSignIn
			}
			return directory{}, core.Fail(core.UpstreamTimeout, errors.New("123 Pan account receipt did not arrive"))
		}
		return directory{}, err
	}
	if !validID(string(account.UID)) {
		return directory{}, pan123.ErrWebsiteSignIn
	}
	return directory{frame: frame.ID, loader: frame.LoaderID, account: string(account.UID), parent: parent}, nil
}

func (s *Session) list(ctx context.Context, observed *observation, view *directory, cursor string, result *pan123.WebsitePage) error {
	var data struct {
		InfoList []struct {
			ID     nativeNumber `json:"FileId"`
			Parent nativeNumber `json:"ParentFileId"`
			Name   string       `json:"FileName"`
			Type   *int
			Size   nativeNumber
		}
		Next json.RawMessage
	}
	_, err := observed.await(ctx, listPath, func(u *url.URL) bool {
		query := u.Query()
		marker := query.Get("next")
		return query.Get("parentFileId") == view.parent && (cursor == marker || cursor == "" && (marker == "0" || marker == "-1"))
	}, &data)
	if err != nil {
		return err
	}
	if data.InfoList == nil {
		return invalid("123 Pan directory receipt has no file list")
	}
	next, err := marker(data.Next)
	if err != nil || next != "" && next == cursor {
		return invalid("123 Pan directory receipt has an invalid next marker")
	}
	items := make([]pan123.WebsiteItem, 0, len(data.InfoList))
	for _, row := range data.InfoList {
		dir := row.Type != nil && *row.Type == 1
		bytes, err := strconv.ParseInt(string(row.Size), 10, 64)
		if !validID(string(row.ID)) || string(row.Parent) != view.parent || !core.ValidName(row.Name) ||
			row.Type == nil || *row.Type != 0 && !dir || !dir && (err != nil || bytes < 0) {
			return invalid("123 Pan directory receipt has an invalid file entry")
		}
		items = append(items, pan123.WebsiteItem{ID: string(row.ID), ParentID: view.parent, Name: row.Name, Directory: dir, Size: bytes})
	}
	view.next = next
	s.view = *view
	*result = pan123.WebsitePage{Account: view.account, Items: items, Next: next}
	return nil
}

func (s *Session) next(ctx context.Context, observed *observation, view *directory, result *pan123.WebsitePage) error {
	if err := control(ctx, s.origin, "end", ""); err != nil {
		return err
	}
	return s.list(ctx, observed, view, view.next, result)
}

func (s *Session) findFile(ctx context.Context, observed *observation, view *directory, file pan123.WebsiteFile) error {
	var result pan123.WebsitePage
	if err := s.list(ctx, observed, view, "", &result); err != nil {
		return err
	}
	seen := make(map[string]struct{})
	for {
		for _, row := range result.Items {
			if row.ID != file.ID {
				continue
			}
			if row.Directory || row.Name != file.Name || row.Size != file.Size {
				return invalid("123 Pan selected file no longer matches its listing")
			}
			return nil
		}
		if view.next == "" {
			return core.Fail(core.NotFound, nil)
		}
		if _, duplicate := seen[view.next]; duplicate {
			return invalid("123 Pan directory pagination repeated a marker")
		}
		seen[view.next] = struct{}{}
		if err := s.next(ctx, observed, view, &result); err != nil {
			return err
		}
	}
}

func (s *Session) selectFile(ctx context.Context, id string) error {
	if err := click(ctx, s.origin, "checkbox", id); err != nil {
		return err
	}
	return click(ctx, s.origin, "download", id)
}

func (s *Session) ordinaryGrant(ctx context.Context, observed *observation, id string) (string, error) {
	var traffic struct {
		Blocked  *bool  `json:"isBlocked"`
		Exceeded *bool  `json:"isTrafficExceeded"`
		Offer    string `json:"cashierEnv"`
	}
	reply, err := observed.await(ctx, trafficPath, nil, &traffic)
	if err != nil {
		return "", err
	}
	var request struct{ FIDs []nativeNumber }
	if err := postData(ctx, reply, &request); err != nil || len(request.FIDs) != 1 || string(request.FIDs[0]) != id {
		return "", invalid("123 Pan traffic receipt did not match the selected file")
	}
	if traffic.Blocked == nil || traffic.Exceeded == nil {
		return "", invalid("123 Pan traffic receipt did not declare eligibility")
	}
	if *traffic.Exceeded {
		return "", core.Fail(core.UpstreamAccessDenied, errors.New("123 Pan download traffic limit reached"))
	}
	if *traffic.Blocked {
		if traffic.Offer != "e1" {
			return "", core.Fail(core.UpstreamAccessDenied, errors.New("123 Pan download requires a different action on the website"))
		}
		if err := click(ctx, s.origin, "ordinary", ""); err != nil {
			return "", err
		}
	}
	var grant struct {
		FileID nativeNumber
		Path   string                    `json:"downloadPath"`
		Hosts  []struct{ Prefix string } `json:"dispatchList"`
	}
	reply, err = observed.await(ctx, grantPath, nil, &grant)
	if err != nil {
		return "", err
	}
	var selected struct{ FileID nativeNumber }
	if err := postData(ctx, reply, &selected); err != nil || string(selected.FileID) != id ||
		string(grant.FileID) != id || grant.Path == "" || len(grant.Hosts) == 0 {
		return "", invalid("123 Pan download grant did not match the selected file")
	}
	base, err := url.Parse(grant.Hosts[0].Prefix)
	if err != nil || !endpoint(base) || base.Path != "" && base.Path != "/" ||
		base.RawQuery != "" || base.Fragment != "" {
		return "", invalid("123 Pan download grant has an invalid delivery origin")
	}
	path, err := url.Parse(grant.Path)
	if err != nil || path.Scheme != "" || path.Host != "" || path.Fragment != "" || path.Path == "" || path.Path[0] != '/' {
		return "", invalid("123 Pan download grant has an invalid delivery path")
	}
	return base.ResolveReference(path).String(), nil
}

func endpoint(u *url.URL) bool {
	return u != nil && u.Scheme == "https" && u.Hostname() != "" && u.User == nil && (u.Port() == "" || u.Port() == "443")
}

func invalid(message string) error {
	return core.Fail(core.InvalidUpstreamResponse, errors.New(message))
}

type nativeNumber string

func (n *nativeNumber) UnmarshalJSON(raw []byte) error {
	value := string(raw)
	if len(value) > 0 && value[0] == '"' {
		if err := json.Unmarshal(raw, &value); err != nil {
			return err
		}
	}
	id, err := strconv.ParseUint(value, 10, 64)
	if err == nil && strconv.FormatUint(id, 10) == value {
		*n = nativeNumber(value)
		return nil
	}
	return errors.New("invalid 123 Pan native number")
}

func validID(s string) bool {
	id, err := strconv.ParseUint(s, 10, 64)
	return err == nil && id != 0 && strconv.FormatUint(id, 10) == s
}

func validParent(s string) bool { return s == "0" || validID(s) }

func marker(raw json.RawMessage) (string, error) {
	if string(raw) == "-1" || string(raw) == `"-1"` || string(raw) == "0" || string(raw) == `"0"` {
		return "", nil
	}
	var value string
	if len(raw) != 0 && raw[0] == '"' && json.Unmarshal(raw, &value) == nil && len(value) <= 256 {
		return value, nil
	}
	var number nativeNumber
	if err := json.Unmarshal(raw, &number); err != nil {
		return "", err
	}
	return string(number), nil
}

type receipt struct {
	*network.EventResponseReceived
	address *url.URL
}

func (s *Session) receipt(ev *network.EventResponseReceived) (receipt, bool) {
	if ev.Response == nil || ev.Type != network.ResourceTypeXHR && ev.Type != network.ResourceTypeFetch {
		return receipt{}, false
	}
	u, err := url.Parse(ev.Response.URL)
	if err != nil {
		return receipt{}, false
	}
	if u.Scheme+"://"+u.Host != s.origin {
		switch u.Hostname() {
		case "api.123278.com", "api.123panapi.com", "api.123panapi.cn":
			if !endpoint(u) {
				return receipt{}, false
			}
		default:
			return receipt{}, false
		}
	}
	switch u.EscapedPath() {
	case accountPath, listPath, trafficPath, grantPath:
		return receipt{ev, u}, true
	default:
		return receipt{}, false
	}
}

type observation struct {
	replies <-chan receipt
	frame   cdp.FrameID
	loader  cdp.LoaderID
	queued  []receipt
}

func (o *observation) await(ctx context.Context, route string, matches func(*url.URL) bool, data any) (receipt, error) {
	match := func(reply receipt) bool {
		return reply.address.Path == route && (matches == nil || matches(reply.address))
	}
	for i, reply := range o.queued {
		if match(reply) {
			o.queued = append(o.queued[:i], o.queued[i+1:]...)
			return reply, reply.read(ctx, data)
		}
	}
	for {
		select {
		case <-ctx.Done():
			return receipt{}, fmt.Errorf("123 Pan %s receipt: %w", route, ctx.Err())
		case reply, ok := <-o.replies:
			if !ok {
				return receipt{}, invalid("123 Pan website observation closed")
			}
			if reply.FrameID != o.frame || reply.LoaderID != o.loader {
				continue
			}
			if match(reply) {
				return reply, reply.read(ctx, data)
			}
			if len(o.queued) < cap(o.replies) {
				o.queued = append(o.queued, reply)
			}
		}
	}
}

func (r receipt) read(ctx context.Context, data any) error {
	status := r.Response.Status
	if status == http.StatusUnauthorized || status == http.StatusForbidden {
		if r.address.Path == accountPath {
			return pan123.ErrWebsiteSignIn
		}
		return core.Fail(core.UpstreamAccessDenied, errors.New("123 Pan declined the website request"))
	}
	if status != http.StatusOK {
		return core.Fail(core.UpstreamFailure, errors.New("123 Pan website request failed"))
	}
	var body []byte
	for range 12 {
		reply, err := chromedp.Call(ctx, network.GetResponseBody, network.GetResponseBodyParams{RequestID: r.RequestID})
		if err == nil && len(reply.Body) > 0 {
			body = reply.Body
			break
		}
		if err := chromedp.Do(ctx, chromedp.Sleep(50*time.Millisecond)); err != nil {
			return err
		}
	}
	var envelope struct {
		Code *int
		Data json.RawMessage
	}
	if len(body) == 0 {
		return invalid("123 Pan website receipt body was unavailable")
	}
	if len(body) > maxReceipt || json.Unmarshal(body, &envelope) != nil || envelope.Code == nil {
		return invalid("123 Pan returned an invalid website receipt")
	}
	if *envelope.Code != 0 && (*envelope.Code != 200 || r.address.Path != accountPath && r.address.Path != listPath) {
		if r.address.Path == accountPath {
			return pan123.ErrWebsiteSignIn
		}
		return core.Fail(core.UpstreamAccessDenied, errors.New("123 Pan declined the website operation"))
	}
	if json.Unmarshal(envelope.Data, data) != nil || string(envelope.Data) == "null" {
		return invalid("123 Pan returned invalid website data")
	}
	return nil
}

func postData(ctx context.Context, r receipt, data any) error {
	payload, err := chromedp.Call(ctx, network.GetRequestPostData, network.GetRequestPostDataParams{RequestID: r.RequestID})
	if err != nil || len(payload.PostData) > 32<<10 {
		return invalid("123 Pan website request could not be checked")
	}
	return json.Unmarshal(payload.PostData, data)
}
