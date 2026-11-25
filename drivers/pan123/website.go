package pan123

import (
	"context"
	"errors"
	"io"
	"net/http"
	"sync"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/internal/httpcontent"
)

var (
	ErrWebsiteSignIn          = errors.New("sign in to 123 Cloud Drive")
	ErrWebsiteBindingMismatch = errors.New("the 123 website account or folder differs from the selected binding")
)

// WebsiteSession borrows an authenticated first-party session. Each call must
// honor cancellation and bind the response to its observed account and exact
// request. Calls may overlap; a browser implementation can serialize them.
type WebsiteSession interface {
	List(ctx context.Context, parentID, cursor string) (WebsitePage, error)
	Download(ctx context.Context, parentID string, file WebsiteFile) (WebsiteGrant, error)
}

// WebsitePage is one directory batch. An empty Next marks the last batch.
type WebsitePage struct {
	Account string
	Items   []WebsiteItem
	Next    string
}

type WebsiteItem struct {
	ID, ParentID, Name string
	Directory          bool
	Size               int64
}

type WebsiteFile struct {
	Account, ID, Name string
	Size              int64
}

// WebsiteGrant holds the ephemeral HTTPS address for a single confirmed file.
type WebsiteGrant struct {
	Account, FileID, URL string
}

type WebsiteOptions struct {
	Session  WebsiteSession
	Account  string
	RootPath string
	// RootID optionally checks the previously verified identity of RootPath.
	RootID          string
	MaxResolvePages int
	// ContentClient sends only delegated body requests. Its transport must
	// not attach credentials. The driver's client copy has no cookie jar.
	ContentClient *http.Client
	// Release closes the session after ownership transfers on a successful open.
	// Close may call Release again if it fails.
	Release func() error
}

// Website exposes the read-only filesystem granted by an authenticated website.
type Website struct {
	*reader
	session WebsiteSession
	account string
	closeMu sync.Mutex
	release func() error
}

// OpenWebsite resolves and verifies the chosen directory and its first page.
// On failure the caller retains ownership of the session.
func OpenWebsite(ctx context.Context, o WebsiteOptions) (*Website, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if o.Session == nil || o.Release == nil || !validWebsiteID(o.Account) || o.Account == "0" ||
		o.RootID != "" && !validWebsiteID(o.RootID) || o.MaxResolvePages < 0 {
		return nil, core.Fail(core.InvalidRequest, nil)
	}
	if o.RootPath == "" {
		o.RootPath = "/"
	}
	if err := core.ValidatePath(o.RootPath); err != nil {
		return nil, err
	}
	d := &Website{reader: newReader(o.ContentClient, o.MaxResolvePages), session: o.Session, account: o.Account}
	d.page, d.verifyRoot, d.grant = d.readPage, d.statRoot, d.download
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
	d.root = root
	if _, err := d.statRoot(ctx); err != nil {
		return nil, err
	}
	d.release = o.Release
	return d, nil
}

// RootID returns the provider identity of the selected directory.
func (d *Website) RootID() string { return string(d.root.ID) }

func (d *Website) Close() error {
	err := d.reader.Close()
	d.closeMu.Lock()
	defer d.closeMu.Unlock()
	if d.release != nil {
		if releaseErr := d.release(); releaseErr != nil {
			return errors.Join(err, releaseErr)
		}
		d.release = nil
	}
	return err
}

func (d *Website) statRoot(ctx context.Context) (item, error) {
	_, _, err := d.readPage(ctx, d.RootID(), "", 1)
	return d.root, err
}

func validWebsiteID(id string) bool { return len(id) <= 20 && httpcontent.ValidDecimalID(id) }

func (d *Website) verifyAccount(account string) error {
	if !validWebsiteID(account) || account == "0" {
		return core.Fail(core.InvalidUpstreamResponse, errors.New("website response omitted a valid account"))
	}
	if account != d.account {
		return core.Fail(core.UpstreamAccessDenied, ErrWebsiteBindingMismatch)
	}
	return nil
}

func websiteCallError(ctx context.Context, err error) error {
	if cause := ctx.Err(); cause != nil {
		return cause
	}
	if errors.Is(err, ErrWebsiteSignIn) || errors.Is(err, ErrWebsiteBindingMismatch) {
		if core.CodeOf(err) != core.UpstreamAccessDenied {
			return core.Fail(core.UpstreamAccessDenied, err)
		}
	}
	if errors.Is(err, io.EOF) {
		return core.Fail(core.InvalidUpstreamResponse, io.ErrUnexpectedEOF)
	}
	return err
}

func (d *Website) readPage(ctx context.Context, parent, after string, _ int) ([]item, string, error) {
	page, err := d.session.List(ctx, parent, after)
	if err := websiteCallError(ctx, err); err != nil {
		return nil, "", err
	}
	if err := d.verifyAccount(page.Account); err != nil {
		return nil, "", err
	}
	next := page.Next
	switch {
	case next == "":
		next = "-1"
	case next == after || next == "-1" || len(next) > 1024:
		return nil, "", core.Fail(core.InvalidUpstreamResponse, errors.New("invalid website continuation"))
	}
	active := make([]item, 0, len(page.Items))
	for _, f := range page.Items {
		if !validWebsiteID(f.ID) || f.ID == "0" || f.ParentID != parent || f.ID == parent || !f.Directory && f.Size < 0 {
			return nil, "", core.Fail(core.InvalidUpstreamResponse, errors.New("website listing changed parent or omitted a file fact"))
		}
		if !core.ValidName(f.Name) {
			return nil, "", core.Fail(core.UnrepresentableName, nil)
		}
		meta := core.Metadata{Name: f.Name, Kind: core.Directory}
		if !f.Directory {
			size := f.Size
			meta.Kind, meta.Size = core.File, &size
		}
		active = append(active, item{ID: nativeID(f.ID), Parent: nativeID(parent), Name: f.Name, meta: meta})
	}
	return active, next, nil
}

func (d *Website) download(ctx context.Context, f item) (string, *int64, error) {
	request := WebsiteFile{Account: d.account, ID: string(f.ID), Name: f.Name, Size: *f.meta.Size}
	grant, err := d.session.Download(ctx, string(f.Parent), request)
	if err := websiteCallError(ctx, err); err != nil {
		return "", nil, err
	}
	if err := d.verifyAccount(grant.Account); err != nil {
		return "", nil, err
	}
	if grant.FileID != request.ID {
		return "", nil, core.Fail(core.InvalidUpstreamResponse, errors.New("website grant changed file ID"))
	}
	return grant.URL, f.meta.Size, nil
}

var _ core.Driver = (*Website)(nil)
