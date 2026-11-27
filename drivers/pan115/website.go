package pan115

import (
	"context"
	"errors"
	"io"
	"sync"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/internal/httpcontent"
)

var (
	ErrWebsiteSignIn          = errors.New("sign in to 115 Cloud Drive")
	ErrWebsiteBindingMismatch = errors.New("the 115 website account or folder differs from the selected binding")
)

// WebsiteSession operates within an authenticated first-party website session.
// List and Read honor cancellation and verify the account and exact selection
// for each operation. Read owns any native authorization and must bind the
// returned stream to the requested file. The receiver owns any returned body.
type WebsiteSession interface {
	List(ctx context.Context, account, parentID string, offset int64, limit int) (WebsitePage, error)
	Read(ctx context.Context, account, parentID string, file WebsiteItem) (io.ReadCloser, error)
}

type websiteRecycler interface {
	Recycle(ctx context.Context, account, parentID string, file WebsiteItem) (core.Change, error)
}

// WebsiteItem is one native listing entry with an exact file size when it is a file.
type WebsiteItem struct {
	ID, ParentID, Name string
	Directory          bool
	Size               int64
	Offset             int64 // position in the listing, used to reopen the native page
}

// WebsitePage reports the exact account and directory for an offset-based page.
// Items is non-nil, including for an empty directory.
type WebsitePage struct {
	Account, ParentID    string
	Offset, Limit, Count int64
	Items                []WebsiteItem
}

// WebsiteOptions binds the ordinary website session to a selected root.
// RootID optionally verifies the current identity of RootPath.
type WebsiteOptions struct {
	Session                   WebsiteSession
	Account, RootPath, RootID string
	Release                   func() error
}

type websiteBinding struct {
	session WebsiteSession
	account string
	closeMu sync.Mutex
	release func() error
}

func (b *websiteBinding) close() error {
	b.closeMu.Lock()
	defer b.closeMu.Unlock()
	if b.release == nil {
		return nil
	}
	if err := b.release(); err != nil {
		return err
	}
	b.release = nil
	return nil
}

// OpenWebsite verifies the account and root before taking ownership of Release.
// An empty RootPath selects the account root. The website exposes browsing and
// full sequential downloads. A session with Recycle can remove regular files.
func OpenWebsite(ctx context.Context, o WebsiteOptions) (*Driver, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if o.Session == nil || o.Release == nil || o.Account == "" || o.RootID != "" && !httpcontent.ValidDecimalID(o.RootID) {
		return nil, core.Fail(core.InvalidRequest, nil)
	}
	if o.RootPath == "" {
		o.RootPath = "/"
	}
	if err := core.ValidatePath(o.RootPath); err != nil {
		return nil, err
	}
	d := &Driver{
		root:    item{ID: "0", Category: "0", meta: core.Metadata{Kind: core.Directory}},
		website: &websiteBinding{session: o.Session, account: o.Account, release: o.Release},
	}
	root, err := d.resolve(ctx, o.RootPath)
	if err != nil {
		return nil, err
	}
	if root.meta.Kind != core.Directory {
		return nil, core.Fail(core.NotDirectory, nil)
	}
	if o.RootID != "" && o.RootID != string(root.ID) {
		return nil, core.Fail(core.UpstreamAccessDenied, ErrWebsiteBindingMismatch)
	}
	if _, err := d.readPage(ctx, string(root.ID), 0, 1); err != nil {
		return nil, err
	}
	d.root = root
	return d, nil
}

// RootID returns the native identity of the mounted directory.
func (d *Driver) RootID() string { return string(d.root.ID) }

func websiteError(ctx context.Context, err error) error {
	if cause := ctx.Err(); cause != nil {
		return cause
	}
	if (errors.Is(err, ErrWebsiteSignIn) || errors.Is(err, ErrWebsiteBindingMismatch)) &&
		core.CodeOf(err) != core.UpstreamAccessDenied {
		return core.Fail(core.UpstreamAccessDenied, err)
	}
	if errors.Is(err, io.EOF) {
		return core.Fail(core.InvalidUpstreamResponse, io.ErrUnexpectedEOF)
	}
	return err
}

func (b *websiteBinding) readPage(ctx context.Context, parent string, offset int64, limit int) (page, error) {
	view, err := b.session.List(ctx, b.account, parent, offset, limit)
	if err := websiteError(ctx, err); err != nil {
		return page{}, err
	}
	if view.Account == "" {
		return page{}, core.Fail(core.InvalidUpstreamResponse, errors.New("website page omitted the account"))
	}
	if view.Account != b.account || view.ParentID != parent {
		return page{}, core.Fail(core.UpstreamAccessDenied, ErrWebsiteBindingMismatch)
	}
	if view.Items == nil || len(view.Items) > limit {
		return page{}, core.Fail(core.InvalidUpstreamResponse, errors.New("invalid website directory page"))
	}
	p := page{ID: nativeID(parent), Offset: &view.Offset, Limit: &view.Limit, Count: &view.Count,
		Data: make([]item, len(view.Items)), readable: true}
	for i, row := range view.Items {
		if !httpcontent.ValidDecimalID(row.ID) {
			return page{}, core.Fail(core.InvalidUpstreamResponse, errors.New("invalid website file identity"))
		}
		f := item{ID: nativeID(row.ID), Parent: nativeID(row.ParentID), Name: row.Name, Category: "0", websiteOffset: view.Offset + int64(i)}
		if !row.Directory {
			f.Category, f.Size = "1", &row.Size
		}
		p.Data[i] = f
	}
	return preparePage(p, parent, offset)
}

func (d *Driver) openWebsiteBody(ctx context.Context, file item) (core.Opened, error) {
	b := d.website
	selected := websiteFile(file)
	body, err := b.session.Read(ctx, b.account, selected.ParentID, selected)
	if err := websiteError(ctx, err); err != nil {
		if body != nil {
			err = errors.Join(err, d.closeBody(body, true))
		}
		return core.Opened{}, err
	}
	if body == nil {
		return core.Opened{}, core.Fail(core.InvalidUpstreamResponse, errors.New("website download omitted the body"))
	}
	return core.Opened{Metadata: file.meta, Length: file.Size, Body: body}, nil
}

func websiteFile(file item) WebsiteItem {
	return WebsiteItem{ID: string(file.ID), ParentID: string(file.Parent), Name: file.Name,
		Size: *file.Size, Offset: file.websiteOffset}
}

// Remove can recycle one regular file through the authenticated website.
func (d *Driver) Remove(ctx context.Context, address core.NativeAddress, policies ...core.RemoveOptions) (core.Change, error) {
	unchanged := core.Change{Effect: core.Unchanged}
	if len(policies) > 1 {
		return unchanged, core.Fail(core.InvalidRequest, nil)
	}
	if len(policies) == 1 {
		policy := policies[0]
		if policy.Disposition != "" || !policy.When.Empty() || policy.Target != "" && policy.Target != core.RemoveNonDirectory {
			return unchanged, core.Fail(core.UnsupportedOperation, nil)
		}
	}
	if err := d.check(ctx, address); err != nil {
		return unchanged, err
	}
	if d.website == nil {
		return unchanged, core.Fail(core.UnsupportedOperation, nil)
	}
	recycler, ok := d.website.session.(websiteRecycler)
	if !ok {
		return unchanged, core.Fail(core.UnsupportedOperation, nil)
	}
	if address.Value() == "/" {
		return unchanged, core.Fail(core.InvalidRequest, nil)
	}
	file, err := d.resolve(ctx, address.Value())
	if err != nil {
		return unchanged, err
	}
	if file.meta.Kind != core.File {
		return unchanged, core.Fail(core.NotFile, nil)
	}
	selected := websiteFile(file)
	change, err := recycler.Recycle(ctx, d.website.account, selected.ParentID, selected)
	return change, websiteError(ctx, err)
}

func (d *Driver) RemoveWithDisposition(ctx context.Context, target core.NativeAddress, options core.RemoveOptions) (core.Change, error) {
	if options.Disposition != core.RemoveTrash {
		return core.Change{Effect: core.Unchanged}, core.Fail(core.UnsupportedOperation, nil)
	}
	options.Disposition = ""
	return d.Remove(ctx, target, options)
}

var _ core.Remover = (*Driver)(nil)
