package mediafire

import (
	"bytes"
	"context"
	"errors"
	"io"
	"math"
	"net/http"
	"net/url"
	"path"
	"strings"
	"sync"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/internal/upload"
)

var (
	ErrWebsiteSignIn          = errors.New("sign in to MediaFire")
	ErrWebsiteBindingMismatch = errors.New("the MediaFire account or directory differs from the selected binding")
)

// WebsiteSession uses the authenticated website. Each operation must respect
// cancellation and report the account and object associated with its receipt.
type WebsiteSession interface {
	List(context.Context, WebsiteListRequest) (WebsitePage, error)
	Download(context.Context, WebsiteFile) (WebsiteDownload, error)
}

type WebsiteListRequest struct {
	FolderKey, ContentType string
	Chunk                  int64
	ChunkSize              int
}

// WebsitePage carries the native folder/get_content response envelope.
type WebsitePage struct {
	Account string
	Status  int
	Body    []byte
}

// WebsiteFile binds the listed file to its ordinary download page.
type WebsiteFile struct {
	Account, ID, Name, URL string
	Size                   int64
}

// WebsiteDownload transfers the exact file response to the Driver for
// validation and closing, including any body returned with an error.
type WebsiteDownload struct {
	Account, FileID string
	Response        *http.Response
}

type WebsiteOptions struct {
	Session          WebsiteSession
	Account          string
	RootPath, RootID string
	MaxResponseBytes int64
	Upload           *WebsiteUploadOptions
	// Release closes the session after ownership transfers on successful open.
	Release func() error
}

type Website struct {
	*Driver
	upload *upload.Staging
}

type websiteAccess struct {
	session WebsiteSession
	account string
	closeMu sync.Mutex
	release func() error
}

// OpenWebsite resolves the selected directory and checks its listing receipt.
// The caller keeps ownership of the session if the open fails.
func OpenWebsite(ctx context.Context, o WebsiteOptions) (*Website, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if o.Session == nil || o.Release == nil || !websiteAccount(o.Account) ||
		o.RootID != "" && !singleKey(o.RootID) || o.MaxResponseBytes < 0 || o.MaxResponseBytes == math.MaxInt64 {
		return nil, core.Fail(core.InvalidRequest, nil)
	}
	if o.RootPath == "" {
		o.RootPath = "/"
	}
	if o.MaxResponseBytes == 0 {
		o.MaxResponseBytes = 4 << 20
	}
	upload, err := newWebsiteUpload(o.Upload)
	if err != nil {
		return nil, err
	}
	d := &Driver{root: "myfiles", limit: o.MaxResponseBytes,
		website: &websiteAccess{session: o.Session, account: o.Account}}
	root, err := d.lookup(ctx, core.NativePath(o.RootPath))
	if err != nil {
		return nil, err
	}
	if root.meta.Kind != core.Directory {
		return nil, core.Fail(core.NotDirectory, nil)
	}
	if o.RootID != "" && o.RootID != root.key {
		return nil, core.Fail(core.UpstreamAccessDenied, ErrWebsiteBindingMismatch)
	}
	d.root = root.key
	if err := d.confirmRoot(ctx); err != nil {
		return nil, err
	}
	d.website.release = o.Release
	return &Website{Driver: d, upload: upload}, nil
}

func (w *Website) RootID() string { return w.root }

// Mkdir creates a directory at the exact path using MediaFire's native skip policy.
func (w *Website) Mkdir(ctx context.Context, target core.NativeAddress) (core.Change, error) {
	unchanged := core.Change{Effect: core.Unchanged}
	session, ok := w.website.session.(*BrowserSession)
	if !ok || target.Namespace() != core.TreeNamespace {
		return unchanged, core.Fail(core.UnsupportedOperation, nil)
	}
	at := target.Value()
	if err := core.ValidatePath(at); err != nil {
		return unchanged, err
	}
	parent, err := w.lookup(ctx, core.NativePath(path.Dir(at)))
	if err != nil {
		return unchanged, err
	}
	if parent.meta.Kind != core.Directory {
		return unchanged, core.Fail(core.NotDirectory, nil)
	}
	if at == "/" {
		unchanged.Before = core.TargetPresent
		return unchanged, core.Fail(core.AlreadyExists, nil)
	}
	name := path.Base(at)
	unknown := core.Change{Effect: core.Unknown}
	r, err := session.request(ctx, browserRequest{Operation: "mkdir", Account: w.website.account, ObjectID: parent.key, Name: name})
	if err = websiteError(ctx, err); err != nil {
		return unknown, err
	}
	if err := w.website.checkAccount(r.Account); err != nil {
		return unknown, err
	}
	child, err := w.child(ctx, parent.key, name)
	if err != nil {
		return unknown, core.Fail(core.InvalidUpstreamResponse, err)
	}
	if r.ObjectID == "" {
		unchanged.Before = core.TargetPresent
		return unchanged, core.Fail(core.AlreadyExists, nil)
	}
	if child.key != r.ObjectID || child.meta.Kind != core.Directory {
		return unknown, core.Fail(core.InvalidUpstreamResponse, nil)
	}
	return core.Change{Effect: core.Applied, Before: core.TargetMissing}, nil
}

// RelocateInto moves one regular file to a directory, allowing MediaFire
// to allocate an unused name and returning that name after verification.
func (w *Website) RelocateInto(ctx context.Context, source, directory core.NativeAddress) (string, core.Change, error) {
	unchanged := core.Change{Effect: core.Unchanged}
	session, ok := w.website.session.(*BrowserSession)
	if !ok {
		return "", unchanged, core.Fail(core.UnsupportedOperation, nil)
	}
	file, err := w.lookup(ctx, source)
	if err != nil {
		return "", unchanged, err
	}
	if file.meta.Kind != core.File {
		return "", unchanged, core.Fail(core.UnsupportedOperation, nil)
	}
	parent, err := w.lookup(ctx, directory)
	if err != nil {
		return "", unchanged, err
	}
	if parent.meta.Kind != core.Directory {
		return "", unchanged, core.Fail(core.NotDirectory, nil)
	}
	if path.Dir(source.Value()) == directory.Value() {
		return "", unchanged, core.Fail(core.InvalidRequest, nil)
	}
	if err := w.active(ctx); err != nil {
		return "", unchanged, err
	}

	unknown := core.Change{Effect: core.Unknown}
	r, err := session.request(ctx, browserRequest{Operation: "move", Account: w.website.account, ObjectID: file.key, ParentID: parent.key})
	if err := websiteError(ctx, err); err != nil {
		return "", unknown, err
	}
	if err := w.website.checkAccount(r.Account); err != nil {
		return "", unknown, err
	}
	if r.ObjectID != file.key || !core.ValidName(r.Name) {
		return "", unknown, core.Fail(core.InvalidUpstreamResponse, nil)
	}
	confirmed, err := w.lookup(ctx, directory)
	if err != nil || confirmed.key != parent.key || confirmed.meta.Kind != core.Directory {
		return "", unknown, core.Fail(core.InvalidUpstreamResponse, err)
	}
	child, err := w.child(ctx, parent.key, r.Name)
	if err != nil || child.key != file.key || child.meta.Kind != core.File {
		return "", unknown, core.Fail(core.InvalidUpstreamResponse, err)
	}
	return r.Name, core.Change{Effect: core.Applied, Before: core.TargetMissing}, nil
}

// Remove recycles one file using the bound browser account.
func (w *Website) Remove(ctx context.Context, p core.NativeAddress, options ...core.RemoveOptions) (core.Change, error) {
	unchanged := core.Change{Effect: core.Unchanged}
	if len(options) > 1 {
		return unchanged, core.Fail(core.InvalidRequest, nil)
	}
	if len(options) == 1 {
		o := options[0]
		if o.Disposition != "" || !o.When.Empty() || o.Target != "" && o.Target != core.RemoveNonDirectory {
			return unchanged, core.Fail(core.UnsupportedOperation, nil)
		}
	}
	return w.recycle(ctx, p, core.File)
}

func (w *Website) RemoveWithDisposition(ctx context.Context, target core.NativeAddress, options core.RemoveOptions) (core.Change, error) {
	if options.Disposition != core.RemoveTrash {
		return core.Change{Effect: core.Unchanged}, core.Fail(core.UnsupportedOperation, nil)
	}
	options.Disposition = ""
	return w.Remove(ctx, target, options)
}

// RemoveSubtree sends the exact selected directory and its descendants to Trash.
func (w *Website) RemoveSubtree(ctx context.Context, p core.NativeAddress, _ core.TreeOptions) (core.Change, error) {
	return w.recycle(ctx, p, core.Directory)
}

func (w *Website) recycle(ctx context.Context, p core.NativeAddress, kind core.Kind) (core.Change, error) {
	unchanged := core.Change{Effect: core.Unchanged}
	session, ok := w.website.session.(*BrowserSession)
	if !ok {
		return unchanged, core.Fail(core.UnsupportedOperation, nil)
	}
	target, err := w.lookup(ctx, p)
	if err != nil {
		return unchanged, err
	}
	operation := "recycle"
	if kind == core.Directory {
		if target.meta.Kind != core.Directory {
			return unchanged, core.Fail(core.NotDirectory, nil)
		}
		if target.key == w.root || target.key == "myfiles" {
			return unchanged, core.Fail(core.UnsupportedOperation, nil)
		}
		operation = "recycle-folder"
	} else if target.meta.Kind != core.File {
		return unchanged, core.Fail(core.UnsupportedOperation, nil)
	}
	if err := w.active(ctx); err != nil {
		return unchanged, err
	}
	r, err := session.request(ctx, browserRequest{Operation: operation, Account: w.website.account, ObjectID: target.key})
	if err = websiteError(ctx, err); err != nil {
		return core.Change{Effect: core.Unknown}, err
	}
	if err := w.website.checkAccount(r.Account); err != nil {
		return core.Change{Effect: core.Unknown}, err
	}
	if r.ObjectID != target.key {
		return core.Change{Effect: core.Unknown}, core.Fail(core.InvalidUpstreamResponse, errors.New("MediaFire did not confirm the selected recycled item"))
	}
	return core.Change{Effect: core.Applied}, nil
}

func websiteAccount(account string) bool {
	return len(account) <= 256 && core.ValidName(account)
}

func (w *websiteAccess) checkAccount(account string) error {
	if !websiteAccount(account) {
		return core.Fail(core.InvalidUpstreamResponse, errors.New("MediaFire website receipt omitted the account identity"))
	}
	if account != w.account {
		return core.Fail(core.UpstreamAccessDenied, ErrWebsiteBindingMismatch)
	}
	return nil
}

func websiteError(ctx context.Context, err error) error {
	if cause := ctx.Err(); cause != nil {
		return cause
	}
	if errors.Is(err, ErrWebsiteSignIn) || errors.Is(err, ErrWebsiteBindingMismatch) {
		return core.Fail(core.UpstreamAccessDenied, err)
	}
	if errors.Is(err, io.EOF) {
		return core.Fail(core.InvalidUpstreamResponse, io.ErrUnexpectedEOF)
	}
	return err
}

func (d *Driver) listWebsite(ctx context.Context, q WebsiteListRequest) (receipt, error) {
	if err := d.active(ctx); err != nil {
		return receipt{}, err
	}
	page, err := d.website.session.List(ctx, q)
	if err := websiteError(ctx, err); err != nil {
		return receipt{}, err
	}
	if page.Status != http.StatusOK {
		return receipt{}, httpFailure(page.Status)
	}
	if err := d.website.checkAccount(page.Account); err != nil {
		return receipt{}, err
	}
	res := &http.Response{StatusCode: page.Status, Body: io.NopCloser(bytes.NewReader(page.Body))}
	return d.decodeReceipt(res, "folder/get_content")
}

func (d *Driver) downloadWebsite(ctx context.Context, file item) (core.Opened, error) {
	raw := file.Links.NormalDownload
	if raw == "" {
		return core.Opened{}, core.Fail(core.UnsupportedOperation, errors.New("MediaFire did not offer an ordinary download"))
	}
	if !ordinaryWebsiteURL(raw, file.key) {
		return core.Opened{}, core.Fail(core.InvalidUpstreamResponse, errors.New("MediaFire ordinary link does not identify the listed file"))
	}
	result, err := d.website.session.Download(ctx, WebsiteFile{Account: d.website.account, ID: file.key,
		Name: file.meta.Name, Size: *file.meta.Size, URL: raw})
	err = websiteError(ctx, err)
	if err == nil {
		err = d.website.checkAccount(result.Account)
	}
	res := result.Response
	if err == nil && (result.FileID != file.key || res == nil || res.Body == nil) {
		err = core.Fail(core.InvalidUpstreamResponse, errors.New("MediaFire website did not confirm the complete selected file"))
	}
	if err == nil && res.StatusCode != http.StatusOK {
		err = httpFailure(res.StatusCode)
	}
	if err != nil {
		if res != nil {
			err = errors.Join(err, d.cleanup.CloseBody(res.Body))
		}
		return core.Opened{}, err
	}
	return d.cleanup.OpenSizedBody(res, nil, file.meta.Name, *file.meta.Size)
}

func ordinaryWebsiteURL(raw, key string) bool {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "https" || u.Host != "www.mediafire.com" || u.User != nil || u.Opaque != "" {
		return false
	}
	file, ok := strings.CutPrefix(u.EscapedPath(), "/file/")
	id, _, _ := strings.Cut(file, "/")
	return ok && id == key
}

func (w *websiteAccess) close() error {
	w.closeMu.Lock()
	defer w.closeMu.Unlock()
	if w.release == nil {
		return nil
	}
	if err := w.release(); err != nil {
		return err
	}
	w.release = nil
	return nil
}

var (
	_ core.Driver             = (*Website)(nil)
	_ core.DirectoryMaker     = (*Website)(nil)
	_ core.DirectoryRelocator = (*Website)(nil)
	_ core.SubtreeRemover     = (*Website)(nil)
)
