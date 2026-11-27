package tianyipan

import (
	"context"
	"errors"
	"io"
	"mime"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/internal/childscan"
	"github.com/nostalume/mountweave/drivers/internal/httpcontent"
)

type Website struct {
	session *BrowserSession
	account string
	root    item
	content http.Client
	cleanup httpcontent.ResponseCleanup

	closed  atomic.Bool
	closeMu sync.Mutex
	release func() error
}

func OpenWebsite(ctx context.Context, o WebsiteOptions) (*Website, error) {
	if o.Session == nil || o.Session.call == nil || o.Release == nil || !accountID(o.Account) ||
		o.RootID != "" && !objectID(o.RootID) {
		return nil, core.Fail(core.InvalidRequest, nil)
	}
	if o.RootPath == "" {
		o.RootPath = "/"
	}
	w := &Website{session: o.Session, account: o.Account, root: item{ID: rootID, Folder: true},
		content: httpcontent.IsolatedClient(o.ContentClient)}
	w.content.CheckRedirect = w.cleanup.RedirectDownload
	selected, err := w.resolve(ctx, core.NativePath(o.RootPath))
	if err != nil {
		return nil, err
	}
	if !selected.Folder {
		return nil, core.Fail(core.NotDirectory, nil)
	}
	if o.RootID != "" && o.RootID != selected.ID {
		return nil, core.Fail(core.UpstreamAccessDenied, ErrWebsiteBindingMismatch)
	}
	probe := listing{w: w, folder: selected.ID}
	if _, err := probe.read(ctx); err != nil && err != io.EOF {
		return nil, err
	}
	w.root, w.release = selected, o.Release
	return w, nil
}

func (w *Website) RootID() string { return w.root.ID }

func (w *Website) Close() error {
	w.closed.Store(true)
	w.closeMu.Lock()
	defer w.closeMu.Unlock()
	err := w.cleanup.Retry()
	if w.release != nil {
		if releaseErr := w.release(); releaseErr != nil {
			return errors.Join(err, releaseErr)
		}
		w.release = nil
	}
	return err
}

func (w *Website) active(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if w.closed.Load() {
		return core.Fail(core.MountUnavailable, nil)
	}
	return nil
}

type listing struct {
	w      *Website
	folder string
	page   int
	done   bool
	closed bool
}

func (l *listing) read(ctx context.Context) ([]item, error) {
	if err := l.w.active(ctx); err != nil {
		return nil, err
	}
	if l.done {
		return nil, io.EOF
	}
	page := l.page + 1
	result, err := l.w.session.request(ctx, websiteRequest{
		Operation: "list", Account: l.w.account, ID: l.folder, Page: page,
	})
	if err != nil {
		return nil, err
	}
	if result.Folder != l.folder || result.Page != page || result.Count == nil ||
		*result.Count < 0 || result.Items == nil || len(result.Items) > pageSize {
		return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("invalid Tianyi folder page"))
	}
	seen := make(map[string]struct{}, len(result.Items))
	for _, child := range result.Items {
		if !objectID(child.ID) || child.ID == rootID || child.ID == l.folder || (!child.Folder && child.ID[0] == '-') {
			return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("invalid Tianyi child identity"))
		}
		if _, exists := seen[child.ID]; exists {
			return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("repeated Tianyi child identity"))
		}
		seen[child.ID] = struct{}{}
		if !core.ValidName(child.Name) {
			return nil, core.Fail(core.UnrepresentableName, nil)
		}
		if !child.Folder && (child.Size == nil || *child.Size < 0) {
			return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("invalid Tianyi file size"))
		}
	}
	offset := uint64(l.page) * pageSize
	count := uint64(*result.Count)
	if len(result.Items) != 0 && offset+uint64(len(result.Items)) > count ||
		offset+pageSize < count && len(result.Items) != pageSize ||
		offset+pageSize >= count && offset < count && offset+uint64(len(result.Items)) != count {
		return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("Tianyi page did not cover the reported directory entries"))
	}
	l.page, l.done = page, offset+pageSize >= count
	if l.done {
		return result.Items, io.EOF
	}
	return result.Items, nil
}

func (l *listing) Next(ctx context.Context, n int) ([]core.Metadata, error) {
	if l.closed {
		return nil, io.ErrClosedPipe
	}
	if n < 1 {
		return nil, core.Fail(core.InvalidRequest, nil)
	}
	items, err := l.read(ctx)
	values := make([]core.Metadata, len(items))
	for i, entry := range items {
		values[i] = entry.metadata()
	}
	return values, err
}

func (l *listing) Close() error { l.closed = true; return nil }

func (i item) metadata() core.Metadata {
	if i.Folder {
		return core.Metadata{Kind: core.Directory, Name: i.Name}
	}
	return core.Metadata{Kind: core.File, Name: i.Name, Size: i.Size}
}

func (w *Website) resolve(ctx context.Context, path core.NativeAddress) (item, error) {
	if err := w.active(ctx); err != nil {
		return item{}, err
	}
	if path.Namespace() != core.TreeNamespace {
		return item{}, core.Fail(core.UnsupportedOperation, nil)
	}
	if err := core.ValidatePath(path.Value()); err != nil {
		return item{}, err
	}
	selected := w.root
	if path.Value() == "/" {
		return selected, nil
	}
	budget := 256
	for _, name := range strings.Split(path.Value()[1:], "/") {
		if !selected.Folder {
			return item{}, core.Fail(core.NotDirectory, nil)
		}
		pages := listing{w: w, folder: selected.ID}
		child, found, err := childscan.Unique(ctx, name, func(ctx context.Context) ([]item, error) {
			if budget == 0 {
				return nil, core.Fail(core.ResolutionBudgetExceeded, nil)
			}
			budget--
			return pages.read(ctx)
		}, func(i item) (string, string) { return i.ID, i.Name })
		if err != nil {
			return item{}, err
		}
		if !found {
			return item{}, core.Fail(core.NotFound, nil)
		}
		selected = child
	}
	return selected, nil
}

func (w *Website) Stat(ctx context.Context, path core.NativeAddress, reads ...core.ReadSelection) (core.Metadata, error) {
	if len(reads) > 1 {
		return core.Metadata{}, core.Fail(core.InvalidRequest, nil)
	}
	var selection core.ReadSelection
	if len(reads) == 1 {
		selection = reads[0]
	}
	if err := verifySelection(selection); err != nil {
		return core.Metadata{}, err
	}
	file, err := w.resolve(ctx, path)
	if err != nil {
		return core.Metadata{}, err
	}
	return file.metadata(), selection.Check(core.Evidence{})
}

func (w *Website) Enumerate(ctx context.Context, query core.NativeQuery) (core.Iterator, error) {
	if query.Delimiter() != "" || query.StartAfter() != "" {
		return nil, core.Fail(core.UnsupportedOperation, nil)
	}
	folder, err := w.resolve(ctx, query.Address())
	if err != nil {
		return nil, err
	}
	if !folder.Folder {
		return nil, core.Fail(core.NotDirectory, nil)
	}
	return &listing{w: w, folder: folder.ID}, nil
}

func verifySelection(read core.ReadSelection) error {
	if read.Version != "" {
		return core.Fail(core.UnsupportedOperation, nil)
	}
	return read.When.ForDomain("")
}

func (w *Website) Open(ctx context.Context, path core.NativeAddress, o core.OpenOptions) (core.Opened, error) {
	if err := o.Validate(); err != nil {
		return core.Opened{}, err
	}
	if o.Mode != core.StreamRead || o.NoFollow || o.Range != nil {
		return core.Opened{}, core.Fail(core.UnsupportedOperation, nil)
	}
	if err := verifySelection(o.Selection); err != nil {
		return core.Opened{}, err
	}
	file, err := w.resolve(ctx, path)
	if err != nil {
		return core.Opened{}, err
	}
	if file.Folder {
		return core.Opened{}, core.Fail(core.NotFile, nil)
	}
	if err := o.Selection.Check(core.Evidence{}); err != nil {
		return core.Opened{}, err
	}
	// The personal website offers ordinary downloads up to 1 GiB.
	if *file.Size > maxFileSize {
		return core.Opened{}, core.Fail(core.UnsupportedOperation, nil)
	}
	grant, err := w.session.request(ctx, websiteRequest{Operation: "grant", Account: w.account, ID: file.ID})
	if err != nil {
		return core.Opened{}, err
	}
	target, err := url.Parse(grant.URL)
	if err != nil || grant.FileID != file.ID || !httpcontent.IsHTTPS(target) {
		return core.Opened{}, core.Fail(core.InvalidUpstreamResponse, errors.New("invalid Tianyi download grant"))
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target.String(), nil)
	if err != nil {
		return core.Opened{}, core.Fail(core.InvalidUpstreamResponse, nil)
	}
	req.Header.Set("Accept-Encoding", "identity")
	response, err := w.content.Do(req)
	if ctx.Err() != nil {
		if response != nil {
			return core.Opened{}, errors.Join(ctx.Err(), w.cleanup.CloseBody(response.Body))
		}
		return core.Opened{}, ctx.Err()
	}
	if err != nil {
		if errors.Is(err, httpcontent.ErrContentRedirect) {
			return core.Opened{}, core.Fail(core.InvalidUpstreamResponse, httpcontent.ErrContentRedirect)
		}
		return core.Opened{}, httpcontent.UpstreamIOError(err)
	}
	if response.StatusCode == http.StatusUnauthorized || response.StatusCode == http.StatusForbidden {
		return core.Opened{}, errors.Join(core.Fail(core.UpstreamAccessDenied, nil), w.cleanup.CloseBody(response.Body))
	}
	if response.StatusCode != http.StatusOK {
		return core.Opened{}, errors.Join(core.Fail(core.UpstreamFailure, nil), w.cleanup.CloseBody(response.Body))
	}
	kind, params, err := mime.ParseMediaType(response.Header.Get("Content-Disposition"))
	if err != nil || kind != "attachment" || params["filename"] != "" && params["filename"] != file.Name {
		return core.Opened{}, errors.Join(core.Fail(core.InvalidUpstreamResponse, errors.New("Tianyi did not return the selected attachment")), w.cleanup.CloseBody(response.Body))
	}
	return w.cleanup.OpenSizedBody(response, nil, file.Name, *file.Size)
}

var _ core.Driver = (*Website)(nil)
