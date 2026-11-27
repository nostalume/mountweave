package guangyapan

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"path"
	"sync"
	"time"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/internal/childscan"
	"github.com/nostalume/mountweave/drivers/internal/httpcontent"
)

const WebsiteURL = "https://www.guangyapan.com/"
const websitePageSize = 50

var (
	ErrWebsiteSignIn          = errors.New("sign in to Guangya Pan")
	ErrWebsiteBindingMismatch = errors.New("Guangya Pan website account or folder changed")
)

// BrowserCall runs one scoped operation in the signed-in Guangya Pan web app.
// Account credentials and first-party request headers remain in the browser.
type BrowserCall func(context.Context, string, json.RawMessage) (json.RawMessage, error)

type BrowserSession struct{ call BrowserCall }

func NewBrowserSession(call BrowserCall) *BrowserSession { return &BrowserSession{call: call} }

type WebsiteOptions struct {
	Session       *BrowserSession
	Account       string
	RootPath      string
	RootID        string
	ContentClient *http.Client
	Release       func() error
}

type websiteAccess struct {
	session *BrowserSession
	account string
	mu      sync.Mutex
	release func() error
}

// Website is a Driver mounted through the signed-in website.
type Website struct{ *Driver }

var _ core.DirectoryRelocator = (*Website)(nil)

// OpenWebsite verifies the selected folder using an existing website session.
// On success the Driver owns Release. On failure the caller retains it.
func OpenWebsite(ctx context.Context, o WebsiteOptions) (*Website, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if o.Session == nil || o.Session.call == nil || !websiteAccount(o.Account) {
		return nil, core.Fail(core.InvalidRequest, nil)
	}
	if o.RootPath == "" {
		o.RootPath = "/"
	}
	d := &Driver{content: httpcontent.IsolatedClient(o.ContentClient), website: &websiteAccess{session: o.Session, account: o.Account}}
	folder, err := d.resolve(ctx, core.NativePath(o.RootPath))
	if err != nil {
		return nil, err
	}
	if folder.Type != 2 {
		return nil, core.Fail(core.NotDirectory, nil)
	}
	d.root = folder.ID
	if o.RootID != "" && o.RootID != d.RootID() {
		return nil, core.Fail(core.UpstreamAccessDenied, ErrWebsiteBindingMismatch)
	}
	first := iterator{d: d, parent: d.root}
	if _, err := first.next(ctx); err != nil && err != io.EOF {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	d.website.release = o.Release
	return &Website{d}, nil
}

// Mkdir creates a directory and preserves an existing file or directory with the same name.
func (d *Website) Mkdir(ctx context.Context, target core.NativeAddress) (core.Change, error) {
	unchanged := core.Change{Effect: core.Unchanged}
	if d == nil || d.Driver == nil || d.website == nil {
		return unchanged, core.Fail(core.UnsupportedOperation, nil)
	}
	if err := d.check(ctx); err != nil {
		return unchanged, err
	}
	if target.Namespace() != core.TreeNamespace {
		return unchanged, core.Fail(core.UnsupportedOperation, nil)
	}
	if err := core.ValidatePath(target.Value()); err != nil {
		return unchanged, err
	}
	if target.Value() == "/" {
		unchanged.Before = core.TargetPresent
		return unchanged, core.Fail(core.AlreadyExists, nil)
	}
	parent, err := d.resolve(ctx, core.NativePath(path.Dir(target.Value())))
	if err != nil {
		return unchanged, err
	}
	if parent.Type != 2 {
		return unchanged, core.Fail(core.NotDirectory, nil)
	}
	if err := d.check(ctx); err != nil {
		return unchanged, err
	}
	name := path.Base(target.Value())
	r, err := d.website.session.request(ctx, browserRequest{
		Operation: "mkdir", Account: d.website.account, ParentID: parent.ID, Name: name,
	})
	if r.Status == http.StatusOK && r.Account == d.website.account && core.CodeOf(err) == core.UpstreamFailure &&
		(r.Code == 159 || r.Code == 160) {
		unchanged.Before = core.TargetPresent
		return unchanged, core.Fail(core.AlreadyExists, nil)
	}
	if err != nil {
		return core.Change{Effect: core.Unknown}, err
	}
	folder := r.Folder
	if folder.ID == "" || folder.ID == parent.ID || folder.Name != name || folder.Type != 2 ||
		(folder.Parent != nil && *folder.Parent != parent.ID) {
		return core.Change{Effect: core.Unknown}, core.Fail(core.InvalidUpstreamResponse, errors.New("Guangya Pan did not confirm the new directory"))
	}
	return core.Change{Effect: core.Applied, Before: core.TargetMissing}, nil
}

// Relocate renames one regular file within its current directory without replacing an existing entry.
func (d *Website) Relocate(ctx context.Context, source, target core.NativeAddress, policies ...core.MoveOptions) (core.Change, error) {
	unchanged := core.Change{Effect: core.Unchanged}
	if len(policies) > 1 {
		return unchanged, core.Fail(core.InvalidRequest, nil)
	}
	if d == nil || d.Driver == nil || d.website == nil ||
		source.Namespace() != core.TreeNamespace || target.Namespace() != core.TreeNamespace ||
		len(policies) == 1 && (!policies[0].Source.Empty() || !policies[0].Target.Empty() ||
			policies[0].Existing != "" && policies[0].Existing != core.CreateNew) {
		return unchanged, core.Fail(core.UnsupportedOperation, nil)
	}
	if err := core.ValidatePath(target.Value()); err != nil {
		return unchanged, err
	}
	if source.Value() == "/" || target.Value() == "/" || source == target {
		return unchanged, core.Fail(core.InvalidRequest, nil)
	}
	if path.Dir(source.Value()) != path.Dir(target.Value()) {
		return unchanged, core.Fail(core.UnsupportedOperation, nil)
	}
	file, err := d.resolve(ctx, source)
	if err != nil {
		return unchanged, err
	}
	if file.Type != 1 {
		return unchanged, core.Fail(core.UnsupportedOperation, nil)
	}
	if err := d.check(ctx); err != nil {
		return unchanged, err
	}
	reply, err := d.website.session.request(ctx, browserRequest{
		Operation: "rename", Account: d.website.account, FileID: file.ID, Name: path.Base(target.Value()),
	})
	if reply.Status == http.StatusOK && reply.Account == d.website.account && reply.Code == 160 &&
		core.CodeOf(err) == core.UpstreamFailure {
		unchanged.Before = core.TargetPresent
		return unchanged, core.Fail(core.AlreadyExists, nil)
	}
	unknown := core.Change{Effect: core.Unknown}
	if err != nil {
		return unknown, err
	}
	confirmed, err := d.resolve(ctx, target)
	if err != nil || confirmed.Type != 1 || confirmed.ID != file.ID || confirmed.ParentID != file.ParentID {
		return unknown, core.Fail(core.InvalidUpstreamResponse, err)
	}
	return core.Change{Effect: core.Applied, Before: core.TargetMissing}, nil
}

// RelocateInto moves a regular file into a selected directory and returns its actual name.
func (d *Website) RelocateInto(ctx context.Context, source, directory core.NativeAddress) (string, core.Change, error) {
	unchanged := core.Change{Effect: core.Unchanged}
	if d == nil || d.Driver == nil || d.website == nil {
		return "", unchanged, core.Fail(core.UnsupportedOperation, nil)
	}
	if source == directory || source.Namespace() == core.TreeNamespace && source.Value() == "/" {
		return "", unchanged, core.Fail(core.InvalidRequest, nil)
	}
	file, err := d.resolve(ctx, source)
	if err != nil {
		return "", unchanged, err
	}
	if file.Type != 1 {
		return "", unchanged, core.Fail(core.UnsupportedOperation, nil)
	}
	parent, err := d.resolve(ctx, directory)
	if err != nil {
		return "", unchanged, err
	}
	if parent.Type != 2 {
		return "", unchanged, core.Fail(core.NotDirectory, nil)
	}
	if parent.ID == file.ParentID {
		return "", unchanged, core.Fail(core.InvalidRequest, nil)
	}
	if err := d.check(ctx); err != nil {
		return "", unchanged, err
	}

	w := d.website
	unknown := core.Change{Effect: core.Unknown}
	receipt, err := w.session.request(ctx, browserRequest{
		Operation: "move", Account: w.account, FileID: file.ID, ParentID: parent.ID,
	})
	if err != nil {
		return "", unknown, err
	}
	if receipt.TaskID == "" || len(receipt.TaskID) > 128 {
		return "", unknown, core.Fail(core.InvalidUpstreamResponse, errors.New("Guangya Pan did not issue a valid move task"))
	}
	if err := w.awaitMove(ctx, receipt.TaskID); err != nil {
		return "", unknown, err
	}

	page := iterator{d: d.Driver, parent: parent.ID}
	result, found, err := childscan.Unique(ctx, file.ID, page.next, func(f item) (string, string) { return f.ID, f.ID })
	if err != nil || !found || result.Type != 1 {
		return "", unknown, core.Fail(core.InvalidUpstreamResponse, errors.Join(err, errors.New("Guangya Pan did not confirm the moved file in the requested directory")))
	}
	// Reuse the ordinary exact lookup to reject names which cannot be reopened.
	confirmed, err := d.resolve(ctx, core.NativePath(path.Join(directory.Value(), result.Name)))
	if err != nil || confirmed.ID != file.ID || confirmed.ParentID != parent.ID || confirmed.Type != 1 {
		return "", unknown, core.Fail(core.InvalidUpstreamResponse, errors.Join(err, errors.New("Guangya Pan returned an inaccessible destination name")))
	}
	return result.Name, core.Change{Effect: core.Applied, Before: core.TargetMissing}, nil
}

func (w *websiteAccess) awaitMove(ctx context.Context, id string) error {
	poll, cancel := context.WithTimeout(ctx, 12*time.Second)
	defer cancel()
	for {
		r, err := w.session.request(poll, browserRequest{Operation: "move-status", Account: w.account, TaskID: id})
		if err != nil {
			return err
		}
		if r.TaskID != id || r.State == nil {
			return core.Fail(core.InvalidUpstreamResponse, errors.New("Guangya Pan did not confirm the selected move task"))
		}
		switch *r.State {
		case 2:
			if r.DetailCode != nil && *r.DetailCode != 0 {
				return core.Fail(core.UpstreamFailure, errors.New("Guangya Pan could not complete the move"))
			}
			return nil
		case 3:
			return core.Fail(core.UpstreamFailure, errors.New("Guangya Pan could not complete the move"))
		case 0, 1:
		default:
			return core.Fail(core.InvalidUpstreamResponse, errors.New("Guangya Pan returned an unknown move state"))
		}
		timer := time.NewTimer(time.Second)
		select {
		case <-poll.Done():
			timer.Stop()
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return core.Fail(core.UpstreamTimeout, poll.Err())
		case <-timer.C:
		}
	}
}

func (d *Driver) RootID() string {
	if d.website != nil && d.root == "" {
		return "/"
	}
	return d.root
}

func websiteAccount(account string) bool {
	if len(account) != 64 {
		return false
	}
	_, err := hex.DecodeString(account)
	return err == nil
}

func (s *BrowserSession) Account(ctx context.Context) (string, error) {
	r, err := s.request(ctx, browserRequest{Operation: "account"})
	return r.Account, err
}

type browserRequest struct {
	Operation string
	Account   string
	ParentID  string
	FileID    string
	TaskID    string `json:",omitempty"`
	Index     int64
	PageSize  int
	Size      int64
	Name      string `json:",omitempty"`
}

type browserReply struct {
	Account, FileID, SignedURL string
	TaskID                     string
	State, DetailCode          *int
	SignIn                     bool `json:"sign_in"`
	Mismatch, Denied           bool
	Status                     int
	Code                       int
	Total                      *int64
	Page                       *int64
	Folder                     struct {
		item
		Parent *string `json:"parentId"`
	}
	List []struct {
		item
		Parent *string `json:"parentId"`
	}
}

func (s *BrowserSession) request(ctx context.Context, q browserRequest) (browserReply, error) {
	var result browserReply
	if s == nil || s.call == nil {
		return result, core.Fail(core.InvalidRequest, nil)
	}
	args, err := json.Marshal(q)
	if err != nil {
		return result, core.Fail(core.InvalidRequest, nil)
	}
	data, err := s.call(ctx, browserCall, args)
	if err != nil {
		return result, err
	}
	if json.Unmarshal(data, &result) != nil {
		return result, core.Fail(core.InvalidUpstreamResponse, nil)
	}
	switch {
	case result.SignIn || result.Status == http.StatusUnauthorized:
		return result, core.Fail(core.UpstreamAccessDenied, ErrWebsiteSignIn)
	case result.Mismatch || (websiteAccount(result.Account) && q.Account != "" && q.Account != result.Account):
		return result, core.Fail(core.UpstreamAccessDenied, ErrWebsiteBindingMismatch)
	case result.Denied:
		return result, core.Fail(core.UnsupportedOperation, errors.New("ordinary Guangya Pan website download is not offered"))
	case result.Status == 0:
		return result, core.Fail(core.InvalidUpstreamResponse, nil)
	case result.Status != http.StatusOK:
		return result, responseError(result.Status)
	case result.Code != 0:
		return result, core.Fail(core.UpstreamFailure, errors.New("Guangya Pan website rejected the request"))
	case !websiteAccount(result.Account):
		return result, core.Fail(core.InvalidUpstreamResponse, nil)
	}
	return result, nil
}

func (w *websiteAccess) page(ctx context.Context, parent string, index int64) ([]item, int64, error) {
	r, err := w.session.request(ctx, browserRequest{Operation: "list", Account: w.account, ParentID: parent, Index: index, PageSize: websitePageSize})
	if err != nil {
		return nil, 0, err
	}
	if r.Total == nil || *r.Total < 0 || len(r.List) > websitePageSize || r.Page != nil && *r.Page != index {
		return nil, 0, core.Fail(core.InvalidUpstreamResponse, errors.New("invalid Guangya Pan website page"))
	}
	items := make([]item, 0, len(r.List))
	for _, f := range r.List {
		if f.ID == "" || f.Parent != nil && *f.Parent != parent || f.Type != 1 && f.Type != 2 || f.Size != nil && *f.Size < 0 {
			return nil, 0, core.Fail(core.InvalidUpstreamResponse, errors.New("invalid Guangya Pan website file identity or parent"))
		}
		if !core.ValidName(f.Name) {
			return nil, 0, core.Fail(core.UnrepresentableName, nil)
		}
		f.ParentID = parent
		items = append(items, f.item)
	}
	return items, *r.Total, nil
}

func (w *websiteAccess) download(ctx context.Context, f item) (string, error) {
	if f.Size == nil {
		return "", core.Fail(core.UnsupportedOperation, errors.New("Guangya Pan website did not confirm the file size"))
	}
	r, err := w.session.request(ctx, browserRequest{Operation: "download", Account: w.account, FileID: f.ID, Size: *f.Size})
	if err != nil {
		return "", err
	}
	if r.FileID != f.ID || r.SignedURL == "" {
		return "", core.Fail(core.InvalidUpstreamResponse, errors.New("Guangya Pan website did not confirm the download file"))
	}
	return r.SignedURL, nil
}

func (w *websiteAccess) close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.release == nil {
		return nil
	}
	if err := w.release(); err != nil {
		return err
	}
	w.release = nil
	return nil
}
