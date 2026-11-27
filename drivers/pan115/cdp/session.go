package pan115cdp

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"iter"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"sync"
	"time"

	protocol "github.com/chromedp/cdproto/cdp"
	"github.com/chromedp/cdproto/network"
	"github.com/chromedp/cdproto/page"
	"github.com/chromedp/cdproto/target"
	"github.com/chromedp/chromedp"
	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/pan115"
)

const Origin = "https://f.115.com"

const (
	directoryPath = "/storage/allfiles"
	accountPath   = "/app/1.0/web/26.0/user/base_info"
	maxListing    = 512 << 10
)

type Session struct {
	tab     context.Context
	slot    chan struct{}
	view    directoryView
	pending target.ID // view and pending are guarded by slot
}

type directoryView struct {
	frame   protocol.FrameID
	loader  protocol.LoaderID
	receipt pan115.WebsitePage
}

// New takes a tab from a dedicated browser instance for this session.
func New(tab context.Context) *Session {
	return &Session{tab: tab, slot: make(chan struct{}, 1)}
}

func (s *Session) borrow(ctx context.Context) (context.Context, func(), error) {
	select {
	case s.slot <- struct{}{}:
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	case <-s.tab.Done():
		return nil, nil, core.Fail(core.MountUnavailable, nil)
	}
	life, cancel := context.WithCancel(s.tab)
	stop := context.AfterFunc(ctx, cancel)
	release := sync.OnceFunc(func() { stop(); cancel(); <-s.slot })
	if err := ctx.Err(); err != nil {
		release()
		return nil, nil, err
	}
	if life.Err() != nil {
		release()
		return nil, nil, core.Fail(core.MountUnavailable, nil)
	}
	if s.pending != "" {
		clean, finish := context.WithTimeout(life, 2*time.Second)
		err := s.closeTransfer(clean)
		finish()
		if err != nil {
			release()
			return nil, nil, err
		}
	}
	return life, release, nil
}

func (s *Session) Account(ctx context.Context) (string, error) {
	life, release, err := s.borrow(ctx)
	if err != nil {
		return "", err
	}
	defer release()
	result, err := s.showDirectory(life, "", "0", 0, true)
	return result.Account, err
}

func (s *Session) List(ctx context.Context, account, parentID string, offset int64, limit int) (pan115.WebsitePage, error) {
	if account == "" || !validDirectoryID(parentID) || offset < 0 || limit < 1 {
		return pan115.WebsitePage{}, core.Fail(core.InvalidRequest, nil)
	}
	life, release, err := s.borrow(ctx)
	if err != nil {
		return pan115.WebsitePage{}, err
	}
	defer release()
	result, err := s.showDirectory(life, account, parentID, offset, offset == 0)
	if err != nil {
		return pan115.WebsitePage{}, err
	}
	start := int(offset - result.Offset)
	result.Items = slices.Clone(result.Items[start : start+min(len(result.Items)-start, limit)])
	result.Offset, result.Limit = offset, min(result.Limit, int64(limit))
	return result, nil
}

// showDirectory retains one native receipt under the session lease. Core may
// consume a single native page in several smaller slices.
func (s *Session) showDirectory(ctx context.Context, account, parentID string, offset int64, refresh bool) (_ pan115.WebsitePage, err error) {
	setup, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	defer func() {
		if err != nil && setup.Err() != nil {
			err = directoryFailure(ctx, setup)
		}
	}()
	view := s.view
	var frame *protocol.Frame
	if !refresh && view.receipt.Account == account && view.receipt.ParentID == parentID {
		frame, _ = topFrame(setup)
	}
	if frame == nil || view.frame != frame.ID || view.loader != frame.LoaderID || !directoryFrame(frame, parentID) {
		s.view = directoryView{}
		frame, view.receipt, err = loadDirectory(setup, account, parentID)
		if err != nil {
			return pan115.WebsitePage{}, err
		}
		view.frame, view.loader = frame.ID, frame.LoaderID
	}
	if !containsOffset(view.receipt, offset) {
		start := offset / view.receipt.Limit * view.receipt.Limit
		if offset >= view.receipt.Count || start == view.receipt.Offset {
			return pan115.WebsitePage{}, badDirectory("115 website directory page cannot advance")
		}
		s.view = directoryView{}
		responses := chromedp.Events(setup, network.ResponseReceived)
		if err := jumpToPage(setup, start/view.receipt.Limit+1); err != nil {
			return pan115.WebsitePage{}, err
		}
		view.receipt, err = observeDirectory(setup, frame, view.receipt.Account, parentID, start, responses)
		if err != nil {
			return pan115.WebsitePage{}, err
		}
		if !containsOffset(view.receipt, offset) {
			return pan115.WebsitePage{}, badDirectory("115 website returned a different directory page")
		}
	}
	if err := ctx.Err(); err != nil {
		return pan115.WebsitePage{}, err
	}
	s.view = view
	return view.receipt, nil
}

func containsOffset(p pan115.WebsitePage, offset int64) bool {
	return offset >= p.Offset && offset-p.Offset < int64(len(p.Items)) || offset == 0 && p.Count == 0
}

func loadDirectory(setup context.Context, account, parentID string) (*protocol.Frame, pan115.WebsitePage, error) {
	resource, total, post := int64(maxListing), int64(4*maxListing), int64(1)
	if _, err := chromedp.Call(setup, network.Enable, network.EnableParams{
		MaxResourceBufferSize: &resource, MaxTotalBufferSize: &total, MaxPostDataSize: &post,
	}); err != nil {
		return nil, pan115.WebsitePage{}, directoryFailure(setup, setup)
	}
	if _, err := chromedp.Call(setup, network.SetCacheDisabled, network.SetCacheDisabledParams{CacheDisabled: true}); err != nil {
		return nil, pan115.WebsitePage{}, directoryFailure(setup, setup)
	}
	responses := chromedp.Events(setup, network.ResponseReceived)
	query := url.Values{"cid": {parentID}, "mode": {"wangpan"}}
	if err := chromedp.Do(setup, chromedp.Navigate(Origin+directoryPath+"?"+query.Encode())); err != nil {
		return nil, pan115.WebsitePage{}, directoryFailure(setup, setup)
	}
	frame, err := topFrame(setup)
	if err != nil {
		return nil, pan115.WebsitePage{}, directoryFailure(setup, setup)
	}
	if !directoryFrame(frame, parentID) {
		if address, err := url.Parse(frame.URL); err == nil && address.Scheme == "https" && address.Host == "f.115.com" &&
			(address.Path == "/" || address.Path == "/login") {
			return nil, pan115.WebsitePage{}, core.Fail(core.UpstreamAccessDenied, pan115.ErrWebsiteSignIn)
		}
		return nil, pan115.WebsitePage{}, badDirectory("115 website did not open the requested directory")
	}
	listed, err := observeDirectory(setup, frame, account, parentID, -1, responses)
	return frame, listed, err
}

func observeDirectory(setup context.Context, frame *protocol.Frame, account, parentID string, offset int64, responses iter.Seq2[network.EventResponseReceived, error]) (pan115.WebsitePage, error) {
	var observedAccount string
	if offset >= 0 {
		observedAccount = account // subsequent pages must come from the same verified document
	}
	var result pan115.WebsitePage
	for event, err := range responses {
		if err != nil {
			break
		}
		if event.FrameID != frame.ID || event.LoaderID != frame.LoaderID || event.Response == nil ||
			event.Response.FromDiskCache || event.Response.FromServiceWorker ||
			event.Type != network.ResourceTypeXHR && event.Type != network.ResourceTypeFetch {
			continue
		}
		address, err := url.Parse(event.Response.URL)
		if err != nil || address.Scheme != "https" || address.User != nil || address.Fragment != "" {
			continue
		}
		switch {
		case address.Host == "passportapi.115.com" && address.EscapedPath() == accountPath:
			observedAccount, err = readAccount(setup, event)
			if err == nil && account != "" && account != observedAccount {
				err = core.Fail(core.UpstreamAccessDenied, pan115.ErrWebsiteBindingMismatch)
			}
		case address.Host == "webapi.115.com" && address.EscapedPath() == "/files":
			query := address.Query()
			start, ok := directoryQuery(query, parentID)
			if !ok || offset >= 0 && start != offset {
				continue
			}
			result, err = readPage(setup, event, query, parentID, start)
		default:
			continue
		}
		if err != nil {
			if address.Host == "passportapi.115.com" {
				return pan115.WebsitePage{}, errors.Join(errors.New("115 account receipt"), err)
			}
			return pan115.WebsitePage{}, err
		}
		if observedAccount != "" && result.Items != nil {
			if err := setup.Err(); err != nil {
				return pan115.WebsitePage{}, err
			}
			result.Account = observedAccount
			return result, nil
		}
	}
	return pan115.WebsitePage{}, directoryFailure(setup, setup)
}

func topFrame(ctx context.Context) (*protocol.Frame, error) {
	result, err := chromedp.Call(ctx, page.GetFrameTree, protocol.Empty{})
	if err != nil {
		return nil, err
	}
	if result.FrameTree == nil || result.FrameTree.Frame == nil {
		return nil, errors.New("115 browser page is unavailable")
	}
	return result.FrameTree.Frame, nil
}

func directoryFrame(frame *protocol.Frame, parentID string) bool {
	address, err := url.Parse(frame.URL)
	if err != nil || address.Scheme != "https" || address.Host != "f.115.com" || address.User != nil ||
		address.EscapedPath() != directoryPath {
		return false
	}
	query := address.Query()
	return query.Get("cid") == parentID || parentID == "0" && !query.Has("cid")
}

func directoryQuery(q url.Values, parentID string) (int64, bool) {
	offset, err := strconv.ParseInt(q.Get("offset"), 10, 64)
	return offset, err == nil && offset >= 0 && q.Get("cid") == parentID && q.Get("aid") == "1" &&
		q.Get("show_dir") == "1" && q.Get("type") == "0" && q.Get("format") == "json" &&
		q.Get("fc") == "" && q.Get("star") == "" && q.Get("is_share") == "" && q.Get("suffix") == ""
}

func readAccount(ctx context.Context, event network.EventResponseReceived) (string, error) {
	var result struct {
		State json.RawMessage
		Data  struct {
			ID json.Number `json:"user_id"`
		}
	}
	if err := readReceipt(ctx, event, &result); err != nil {
		if core.CodeOf(err) == core.UpstreamAccessDenied {
			return "", core.Fail(core.UpstreamAccessDenied, pan115.ErrWebsiteSignIn)
		}
		return "", err
	}
	switch string(result.State) {
	case "1", `"1"`, "true":
	case "0", `"0"`, "false":
		return "", core.Fail(core.UpstreamAccessDenied, pan115.ErrWebsiteSignIn)
	default:
		return "", badDirectory("115 website account state is invalid")
	}
	id := result.Data.ID.String()
	if id == "0" || !validDirectoryID(id) {
		return "", badDirectory("115 website account identity is invalid")
	}
	sum := sha256.Sum256([]byte(Origin + ":" + id))
	return hex.EncodeToString(sum[:]), nil
}

func readPage(ctx context.Context, event network.EventResponseReceived, query url.Values, parentID string, offset int64) (pan115.WebsitePage, error) {
	var result struct {
		State  bool
		CID    json.Number
		Count  json.Number
		Offset json.Number
		Limit  json.Number
		Data   []struct {
			File   json.Number `json:"fid"`
			Folder json.Number `json:"cid"`
			Parent json.Number `json:"pid"`
			Name   string      `json:"n"`
			Size   json.Number `json:"s"`
		}
	}
	if err := readReceipt(ctx, event, &result); err != nil {
		return pan115.WebsitePage{}, err
	}
	if !result.State {
		return pan115.WebsitePage{}, core.Fail(core.UpstreamAccessDenied, errors.New("115 website declined the directory request"))
	}
	count, countErr := result.Count.Int64()
	limit, limitErr := strconv.ParseInt(query.Get("limit"), 10, 64)
	if result.Limit != "" {
		limit, limitErr = result.Limit.Int64()
	}
	n := int64(len(result.Data))
	if countErr != nil || limitErr != nil || limit < 1 || limit > 1150 || n > limit || count < offset || n > count-offset ||
		(result.Offset != json.Number(strconv.FormatInt(offset, 10)) && (offset != 0 || result.Offset != "")) ||
		result.CID.String() != parentID || result.Data == nil || (n == 0 && count != 0) {
		return pan115.WebsitePage{}, badDirectory("115 website directory receipt is invalid")
	}
	resultPage := pan115.WebsitePage{ParentID: parentID, Offset: offset, Limit: limit, Count: count, Items: make([]pan115.WebsiteItem, 0, n)}
	seen := make(map[string]struct{}, n)
	for _, item := range result.Data {
		entry := pan115.WebsiteItem{ID: item.File.String(), ParentID: item.Folder.String(), Name: item.Name}
		if item.File == "" {
			entry.ID, entry.ParentID, entry.Directory = item.Folder.String(), item.Parent.String(), true
		} else {
			size, err := item.Size.Int64()
			if err != nil || size < 0 {
				return pan115.WebsitePage{}, badDirectory("115 website file size is invalid")
			}
			entry.Size = size
		}
		if _, ok := seen[entry.ID]; ok || !validDirectoryID(entry.ID) || entry.ID == "0" || entry.ID == parentID ||
			entry.ParentID != parentID || !core.ValidName(entry.Name) {
			return pan115.WebsitePage{}, badDirectory("115 website listing changed the file or parent identity")
		}
		seen[entry.ID] = struct{}{}
		resultPage.Items = append(resultPage.Items, entry)
	}
	return resultPage, nil
}

func readReceipt(ctx context.Context, event network.EventResponseReceived, result any) error {
	switch event.Response.Status {
	case http.StatusUnauthorized, http.StatusForbidden:
		return core.Fail(core.UpstreamAccessDenied, errors.New("115 website declined the request"))
	case http.StatusTooManyRequests:
		return core.Fail(core.UpstreamThrottled, nil)
	case http.StatusOK:
	default:
		return core.Fail(core.UpstreamFailure, errors.New("115 website directory request failed"))
	}
	for range 6 {
		reply, err := chromedp.Call(ctx, network.GetResponseBody, network.GetResponseBodyParams{RequestID: event.RequestID})
		if err == nil && len(reply.Body) != 0 {
			if len(reply.Body) > maxListing {
				return badDirectory("115 website receipt is invalid")
			}
			if err := json.Unmarshal(reply.Body, result); err != nil {
				var field *json.UnmarshalTypeError
				if errors.As(err, &field) && field.Field != "" {
					return badDirectory("115 website receipt field has an unexpected type: " + field.Field)
				}
				return badDirectory("115 website receipt is invalid")
			}
			return nil
		}
		if err := chromedp.Do(ctx, chromedp.Sleep(100*time.Millisecond)); err != nil {
			break
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return badDirectory("115 website receipt body was unavailable")
}

func validDirectoryID(id string) bool {
	if len(id) == 0 || len(id) > 25 || len(id) > 1 && id[0] == '0' {
		return false
	}
	for i := range len(id) {
		if id[i] < '0' || id[i] > '9' {
			return false
		}
	}
	return true
}

func badDirectory(message string) error {
	return core.Fail(core.InvalidUpstreamResponse, errors.New(message))
}

func directoryFailure(ctx, setup context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if errors.Is(setup.Err(), context.DeadlineExceeded) {
		return core.Fail(core.UpstreamTimeout, errors.New("115 website directory receipt timed out"))
	}
	return core.Fail(core.UpstreamFailure, errors.New("115 website directory could not be opened"))
}
