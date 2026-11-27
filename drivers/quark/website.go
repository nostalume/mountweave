package quark

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"time"

	core "github.com/nostalume/mountweave"
)

const (
	// WebsiteOrigin hosts the authenticated Quark Drive page.
	WebsiteOrigin   = "https://pan.quark.cn"
	websitePageSize = 50
	maxWebsitePages = 200
)

var (
	// ErrWebsiteSignIn identifies a website session that needs authentication.
	ErrWebsiteSignIn = errors.New("sign in to Quark")
	// ErrWebsiteBindingMismatch identifies a change in the selected account or folder.
	ErrWebsiteBindingMismatch = errors.New("the Quark website account or folder differs from the selected binding")
	websiteFID                = regexp.MustCompile("^[A-Za-z0-9_-]{1,128}$")
)

// WebsiteCall evaluates a provider script in the authenticated top-level page
// and returns the JSON string it produced.
type WebsiteCall func(context.Context, string, json.RawMessage) (json.RawMessage, error)

// WebsiteTransfer acquires a response stream from the authenticated page. It
// calls prepare while it has exclusive use of that page's response interceptor.
// It must call Start and Check before transferring the body. After Start is
// attempted, transfer failure or body Close must release the request via Stop.
type WebsiteTransfer func(ctx context.Context, prepare func(context.Context) (WebsiteDownload, error)) (io.ReadCloser, error)

// WebsiteDownload identifies the exact response to observe. Check validates
// the observed response before the transfer takes its stream and returns the
// expected body length and whether the reader must confirm EOF at that length.
type WebsiteDownload struct {
	URL   string
	Start func(context.Context) error
	Stop  func(context.Context) error
	Check func(status int64, headers http.Header) (length int64, checkEOF bool, err error)
}

type WebsiteOptions struct {
	Call     WebsiteCall
	Transfer WebsiteTransfer
	// Account binds operations to the account returned by WebsiteAccount.
	Account  string
	RootPath string
	// RootID optionally binds RootPath to the folder verified by an earlier open.
	RootID string
	// Release retires the borrowed website session after ownership is
	// transferred by a successful OpenWebsite.
	Release func() error
}

type websiteSession struct {
	call     WebsiteCall
	transfer WebsiteTransfer
	account  string
	release  func() error
}

// OpenWebsite verifies the account, resolves the selected directory and checks
// its first page. The caller owns the session until OpenWebsite returns a driver.
func OpenWebsite(ctx context.Context, o WebsiteOptions) (*Driver, error) {
	if o.Call == nil || o.Transfer == nil || o.Release == nil || !validWebsiteAccount(o.Account) {
		return nil, core.Fail(core.InvalidRequest, nil)
	}
	d := &Driver{
		website: &websiteSession{call: o.Call, transfer: o.Transfer, account: o.Account, release: sync.OnceValue(o.Release)},
		root:    entry{fid: rootFID, meta: core.Metadata{Kind: core.Directory}},
	}
	root, err := d.resolve(ctx, core.NativePath(o.RootPath))
	if err != nil {
		return nil, err
	}
	if root.meta.Kind != core.Directory {
		return nil, core.Fail(core.NotDirectory, nil)
	}
	if o.RootID != "" && root.fid != o.RootID {
		return nil, core.Fail(core.UpstreamAccessDenied, ErrWebsiteBindingMismatch)
	}
	probe := websiteListing{driver: d, parent: root.fid}
	if _, err := probe.next(ctx); err != nil && err != io.EOF {
		return nil, err
	}
	d.root = root
	return d, nil
}

// WebsiteAccount verifies access to the drive root and returns an opaque
// account binding for future opens.
func WebsiteAccount(ctx context.Context, call WebsiteCall) (string, error) {
	return websiteAccount(ctx, call, "")
}

func validWebsiteAccount(account string) bool {
	if len(account) != 64 || account != strings.ToLower(account) {
		return false
	}
	var digest [32]byte
	_, err := hex.Decode(digest[:], []byte(account))
	return err == nil
}

func matchWebsiteAccount(observed, expected string) error {
	if !validWebsiteAccount(observed) {
		return core.Fail(core.InvalidUpstreamResponse, nil)
	}
	if expected != "" && observed != expected {
		return core.Fail(core.UpstreamAccessDenied, ErrWebsiteBindingMismatch)
	}
	return nil
}

func websiteAccount(ctx context.Context, call WebsiteCall, account string) (string, error) {
	reply, err := callWebsite(ctx, call, websiteRequest{Account: account, Parent: rootFID, Page: 1, Size: 1})
	if err != nil {
		return "", err
	}
	if reply.Page != 1 || reply.Total == nil || *reply.Total < 0 || reply.Items == nil || len(reply.Items) != min(1, *reply.Total) {
		return "", core.Fail(core.InvalidUpstreamResponse, nil)
	}
	if err := matchWebsiteAccount(reply.Account, account); err != nil {
		return "", err
	}
	return reply.Account, nil
}

type websiteRequest struct {
	Account      string `json:"account,omitempty"`
	Parent       string `json:"parent,omitempty"`
	Page         int    `json:"page,omitempty"`
	Size         int    `json:"size,omitempty"`
	FID          string `json:"fid,omitempty"`
	RecycleUntil int64  `json:"recycle_until,omitempty"`
}

type websiteReply struct {
	Account  string        `json:"account"`
	Conflict bool          `json:"conflict"`
	HTTP     int           `json:"http"`
	Code     *int          `json:"code"`
	SignIn   bool          `json:"sign_in"`
	Page     int           `json:"page"`
	Total    *int          `json:"total"`
	Items    []websiteItem `json:"items"`
	Task     string        `json:"task"`
	Grants   []struct {
		FID  string       `json:"fid"`
		Size *json.Number `json:"size"`
		URL  string       `json:"download_url"`
	} `json:"grants"`
}

func callWebsite(ctx context.Context, call WebsiteCall, request websiteRequest) (websiteReply, error) {
	var reply websiteReply
	if err := ctx.Err(); err != nil {
		return reply, err
	}
	if call == nil {
		return reply, core.Fail(core.InvalidRequest, nil)
	}
	data, err := json.Marshal(request)
	if err != nil {
		return reply, err
	}
	raw, err := call(ctx, websiteFileRequest, data)
	if err != nil {
		return reply, err
	}
	if err := ctx.Err(); err != nil {
		return reply, err
	}
	if json.Unmarshal(raw, &reply) != nil {
		return reply, core.Fail(core.InvalidUpstreamResponse, nil)
	}
	if reply.Conflict {
		return reply, core.Fail(core.UpstreamAccessDenied, ErrWebsiteBindingMismatch)
	}
	if reply.SignIn || reply.Code != nil && *reply.Code == 31001 &&
		(reply.HTTP == http.StatusOK || reply.HTTP == http.StatusUnauthorized) {
		return reply, core.Fail(core.UpstreamAccessDenied, ErrWebsiteSignIn)
	}
	switch reply.HTTP {
	case http.StatusOK:
		if reply.Code == nil {
			return reply, core.Fail(core.InvalidUpstreamResponse, nil)
		}
		if *reply.Code == 0 {
			return reply, nil
		}
	case http.StatusUnauthorized, http.StatusForbidden:
		return reply, core.Fail(core.UpstreamAccessDenied, nil)
	case http.StatusRequestTimeout:
		return reply, core.Fail(core.UpstreamTimeout, nil)
	case http.StatusTooManyRequests:
		return reply, core.Fail(core.UpstreamThrottled, nil)
	case 0:
		return reply, core.Fail(core.InvalidUpstreamResponse, nil)
	}
	return reply, core.Fail(core.UpstreamFailure, nil)
}

func (d *Driver) RemoveWithDisposition(ctx context.Context, target core.NativeAddress, options core.RemoveOptions) (core.Change, error) {
	if options.Disposition != core.RemoveTrash {
		return core.Change{Effect: core.Unchanged}, core.Fail(core.UnsupportedOperation, nil)
	}
	options.Disposition = ""
	return d.Remove(ctx, target, options)
}

// Remove sends one regular file to Trash through the bound website account.
func (d *Driver) Remove(ctx context.Context, address core.NativeAddress, options ...core.RemoveOptions) (core.Change, error) {
	unchanged := core.Change{Effect: core.Unchanged}
	if len(options) > 1 {
		return unchanged, core.Fail(core.InvalidRequest, nil)
	}
	if d.website == nil || len(options) == 1 && (options[0].Disposition != "" || !options[0].When.Empty() ||
		options[0].Target != "" && options[0].Target != core.RemoveNonDirectory) {
		return unchanged, core.Fail(core.UnsupportedOperation, nil)
	}
	file, err := d.resolve(ctx, address)
	if err != nil {
		return unchanged, err
	}
	if file.meta.Kind != core.File {
		return unchanged, core.Fail(core.UnsupportedOperation, nil)
	}
	if err := d.check(ctx); err != nil {
		return unchanged, err
	}
	// Join the browser's bounded request after dispatch. Canceling the caller's
	// evaluation alone would leave its page promise free to issue a late POST.
	settle, cancel := context.WithTimeout(context.WithoutCancel(ctx), 13*time.Second)
	deadline, _ := settle.Deadline()
	unknown := core.Change{Effect: core.Unknown}
	s := d.website
	reply, err := callWebsite(settle, s.call, websiteRequest{
		Account: s.account, FID: file.fid, RecycleUntil: deadline.Add(-time.Second).UnixMilli(),
	})
	cancel()
	if err != nil {
		return unknown, errors.Join(err, ctx.Err())
	}
	if err := matchWebsiteAccount(reply.Account, s.account); err != nil {
		return unknown, err
	}
	if !websiteFID.MatchString(reply.Task) {
		return unknown, core.Fail(core.InvalidUpstreamResponse, nil)
	}
	// Check the admitted file ID in its original parent after the task completes.
	list := websiteListing{driver: d, parent: file.parent}
	for {
		files, err := list.next(ctx)
		if err != nil && err != io.EOF {
			return unknown, err
		}
		for _, current := range files {
			if current.fid == file.fid {
				return unknown, core.Fail(core.InvalidUpstreamResponse, nil)
			}
		}
		if err == io.EOF {
			return core.Change{Effect: core.Applied}, nil
		}
	}
}

type websiteItem struct {
	FID    string       `json:"fid"`
	Parent string       `json:"pdir_fid"`
	Name   string       `json:"file_name"`
	Dir    *bool        `json:"dir"`
	File   *bool        `json:"file"`
	Type   *json.Number `json:"file_type"`
	Size   *json.Number `json:"size"`
}

func (item websiteItem) kind() core.Kind {
	switch {
	case item.Dir != nil && *item.Dir && (item.File == nil || !*item.File):
		return core.Directory
	case item.File != nil && *item.File && (item.Dir == nil || !*item.Dir):
		return core.File
	case item.Dir == nil && item.File == nil && item.Type != nil:
		switch value, err := item.Type.Int64(); {
		case err == nil && value == 0:
			return core.Directory
		case err == nil && value == 1:
			return core.File
		}
	}
	return ""
}

func (item websiteItem) entry(parent string) (entry, error) {
	if !websiteFID.MatchString(item.FID) || item.FID == rootFID || item.FID == parent ||
		item.Parent != "" && item.Parent != parent {
		return entry{}, core.Fail(core.InvalidUpstreamResponse, nil)
	}
	metadata := core.Metadata{Name: item.Name, Kind: item.kind()}
	switch metadata.Kind {
	case core.Directory:
	case core.File:
		if item.Size == nil {
			return entry{}, core.Fail(core.InvalidUpstreamResponse, nil)
		}
		size, err := item.Size.Int64()
		if err != nil || size < 0 {
			return entry{}, core.Fail(core.InvalidUpstreamResponse, nil)
		}
		metadata.Size = &size
	default:
		return entry{}, core.Fail(core.InvalidUpstreamResponse, nil)
	}
	if !core.ValidName(item.Name) {
		return entry{}, core.Fail(core.UnrepresentableName, nil)
	}
	return entry{fid: item.FID, parent: parent, meta: metadata}, nil
}

type websiteListing struct {
	driver      *Driver
	parent      string
	page, total int
	seen        map[string]struct{}
	done        bool
}

func (l *websiteListing) next(ctx context.Context) ([]entry, error) {
	if err := l.driver.check(ctx); err != nil {
		return nil, err
	}
	if l.done {
		return nil, io.EOF
	}
	if l.page == maxWebsitePages {
		return nil, core.Fail(core.ResolutionBudgetExceeded, nil)
	}
	session := l.driver.website
	page := l.page + 1
	reply, err := callWebsite(ctx, session.call, websiteRequest{Account: session.account, Parent: l.parent, Page: page, Size: websitePageSize})
	if err != nil {
		return nil, err
	}
	if err := matchWebsiteAccount(reply.Account, session.account); err != nil {
		return nil, err
	}
	offset := (page - 1) * websitePageSize
	if reply.Page != page || reply.Total == nil || reply.Items == nil || *reply.Total < offset ||
		l.page != 0 && *reply.Total != l.total || len(reply.Items) != min(websitePageSize, *reply.Total-offset) {
		return nil, core.Fail(core.InvalidUpstreamResponse, nil)
	}
	files := make([]entry, 0, len(reply.Items))
	ids := make(map[string]struct{}, len(reply.Items))
	for _, item := range reply.Items {
		_, alreadySeen := l.seen[item.FID]
		_, duplicate := ids[item.FID]
		if alreadySeen || duplicate {
			return nil, core.Fail(core.InvalidUpstreamResponse, nil)
		}
		file, err := item.entry(l.parent)
		if err != nil {
			return nil, err
		}
		ids[file.fid] = struct{}{}
		files = append(files, file)
	}
	l.page, l.total = page, *reply.Total
	if page*websitePageSize >= l.total {
		l.done = true
		l.seen = nil
		return files, io.EOF
	}
	if l.seen == nil {
		l.seen = ids
	} else {
		for id := range ids {
			l.seen[id] = struct{}{}
		}
	}
	return files, nil
}

func (l *websiteListing) Next(ctx context.Context, _ int) ([]core.Metadata, error) {
	files, err := l.next(ctx)
	if len(files) == 0 {
		return nil, err
	}
	result := make([]core.Metadata, len(files))
	for i, file := range files {
		result[i] = file.meta
	}
	return result, err
}

func (l *websiteListing) Close() error {
	l.seen = nil
	l.done = true
	return nil
}
