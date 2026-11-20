package lanzou

import (
	"context"
	"net/http"
	"net/url"
	"strconv"
	"sync"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/internal/httpcontent"
)

const (
	websiteOrigin = "https://up.woozooo.com"
	WebsiteURL    = websiteOrigin + "/mydisk.php"
	LoginURL      = websiteOrigin + "/account.php?action=login"
	RootFolderID  = "-1"
)

// WebsiteOptions binds a signed-in page to a LanZou folder.
// The Driver adopts Release only after verification succeeds.
type WebsiteOptions struct {
	Call           WebsiteCall
	Tab            context.Context
	ShareTransport http.RoundTripper
	Release        func() error
	RootID         string
	Account        string
	// Download can supply an equivalent transfer from a verified download URL.
	// By default the Driver streams from a separate browser context under Tab.
	Download Download
}

// ValidRootID reports whether id is the canonical decimal form of a folder ID.
func ValidRootID(id string) bool {
	n, err := strconv.ParseInt(id, 10, 64)
	return err == nil && (n > 0 || id == RootFolderID) && strconv.FormatInt(n, 10) == id
}

// OpenWebsite verifies the saved account and folder on the authenticated page.
func OpenWebsite(ctx context.Context, o WebsiteOptions) (*Driver, error) {
	if o.Call == nil || o.Release == nil || (o.Download == nil && o.Tab == nil) || !ValidRootID(o.RootID) {
		return nil, core.Fail(core.InvalidRequest, nil)
	}
	d := &Driver{
		call: o.Call, download: o.Download, account: o.Account,
		root:    lanZouEntry{id: o.RootID, meta: core.Metadata{Kind: core.Directory}},
		control: httpcontent.IsolatedClient(&http.Client{Transport: o.ShareTransport}),
	}
	folders, account, err := d.page(ctx, lanZouPageRequest{Task: folderTask, Folder: o.RootID})
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	d.firstFolders, d.account, d.release = folders, account, sync.OnceValue(o.Release)
	if d.download == nil {
		d.download = func(ctx context.Context, target *url.URL, want *core.Range) (core.Opened, error) {
			return streamLanZou(ctx, o.Tab, target, want)
		}
	}
	return d, nil
}

// Account returns the subject verified by the native directory response.
func (d *Driver) Account() string { return d.account }

// FolderID resolves a path and verifies that its native directory can be read.
func (d *Driver) FolderID(ctx context.Context, path string) (string, error) {
	folder, err := d.resolve(ctx, core.NativePath(path))
	if err != nil {
		return "", err
	}
	if folder.meta.Kind != core.Directory {
		return "", core.Fail(core.NotDirectory, nil)
	}
	if folder.id != d.root.id {
		if _, err := d.folders(ctx, folder.id); err != nil {
			return "", err
		}
	}
	_, _, err = d.page(ctx, lanZouPageRequest{Task: fileTask, Folder: folder.id, Page: 1})
	if err != nil {
		return "", err
	}
	return folder.id, nil
}
