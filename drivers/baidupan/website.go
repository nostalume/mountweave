package baidupan

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"path"
	"strconv"
	"sync"

	core "github.com/nostalume/mountweave"
)

const websitePageSize = 100

// WebsitePage binds the original response to the account and directory request
// observed by the website transport. Number is the one-based native page.
type WebsitePage struct {
	Account, Directory string
	Number             int
	Body               json.RawMessage
}

type WebsiteOptions struct {
	Account  string
	RootPath string
	RootID   string // Optional ID checked on open. The account root uses "0".
	// ReadPage obtains a completed first-party directory response in the active
	// session. It binds the observed account and successful request, without
	// copying browser credentials. One call owns the whole navigation and receipt.
	// It must honor cancellation and be safe for concurrent calls.
	ReadPage func(context.Context, string, int) (WebsitePage, error)
	// ReadFile opens the exact selected original through the authenticated site.
	// It is optional for directory-only sources and must honor cancellation.
	ReadFile func(context.Context, WebsiteFile) (WebsiteDownload, error)
	// Release closes the borrowed session after a successful open.
	Release func() error
}

// Website implements Core reads through an authenticated website transport.
type Website struct {
	account, root, id string
	read              func(context.Context, string, int) (WebsitePage, error)
	openFile          func(context.Context, WebsiteFile) (WebsiteDownload, error)
	life              context.Context
	close             func() error
}

// OpenWebsite verifies the account, directory and its first page before taking
// ownership of Release. An unsuccessful open leaves the session with the caller.
func OpenWebsite(ctx context.Context, options WebsiteOptions) (*Website, error) {
	if options.Account == "" || options.ReadPage == nil || options.Release == nil {
		return nil, core.Fail(core.InvalidRequest, nil)
	}
	if err := core.ValidatePath(options.RootPath); err != nil {
		return nil, err
	}
	life, cancel := context.WithCancel(context.Background())
	d := &Website{account: options.Account, root: options.RootPath, read: options.ReadPage, openFile: options.ReadFile, life: life}
	root, err := d.lookup(ctx, d.root)
	if err == nil && root.meta.Kind != core.Directory {
		err = core.Fail(core.NotDirectory, nil)
	}
	if err == nil {
		d.id = strconv.FormatUint(root.id, 10)
		if options.RootID != "" && options.RootID != d.id {
			err = core.Fail(core.UpstreamAccessDenied, errors.New("website root differs from the saved binding"))
		}
	}
	if err == nil {
		_, err = d.readPage(ctx, life, d.root, 1)
	}
	if err != nil {
		cancel()
		return nil, err
	}
	d.close = sync.OnceValue(func() error {
		cancel()
		return options.Release()
	})
	return d, nil
}

func (d *Website) RootID() string { return d.id }

func (d *Website) Close() error { return d.close() }

func (d *Website) Stat(ctx context.Context, address core.NativeAddress, selections ...core.ReadSelection) (core.Metadata, error) {
	if len(selections) > 1 {
		return core.Metadata{}, core.Fail(core.InvalidRequest, nil)
	}
	if len(selections) == 1 && (selections[0].Version != "" || !selections[0].When.Empty()) {
		return core.Metadata{}, core.Fail(core.UnsupportedOperation, nil)
	}
	full, err := d.address(address)
	if err != nil {
		return core.Metadata{}, err
	}
	if err := ctx.Err(); err != nil {
		return core.Metadata{}, err
	}
	if address.Value() == "/" {
		if _, err := d.readPage(ctx, d.life, full, 1); err != nil {
			return core.Metadata{}, err
		}
		return core.Metadata{Kind: core.Directory}, nil
	}
	node, err := d.lookup(ctx, full)
	return node.meta, err
}

func (d *Website) Enumerate(ctx context.Context, query core.NativeQuery) (core.Iterator, error) {
	if query.Delimiter() != "" || query.StartAfter() != "" {
		return nil, core.Fail(core.UnsupportedOperation, nil)
	}
	full, err := d.address(query.Address())
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if query.Address().Value() != "/" {
		node, err := d.lookup(ctx, full)
		if err != nil {
			return nil, err
		}
		if node.meta.Kind != core.Directory {
			return nil, core.Fail(core.NotDirectory, nil)
		}
	}
	life, cancel := context.WithCancel(d.life)
	return &websiteIterator{driver: d, directory: full, pages: websitePages{number: 1}, life: life, cancel: cancel}, nil
}

func (d *Website) address(address core.NativeAddress) (string, error) {
	if d.life.Err() != nil {
		return "", core.Fail(core.MountUnavailable, nil)
	}
	return rootedPath(d.root, address)
}

type websiteNode struct {
	meta core.Metadata
	id   uint64
}

func (d *Website) lookup(ctx context.Context, target string) (websiteNode, error) {
	if target == "/" {
		return websiteNode{meta: core.Metadata{Kind: core.Directory}}, nil
	}
	dir, name := path.Dir(target), path.Base(target)
	pages := websitePages{number: 1}
	for {
		nodes, err := pages.next(ctx, d.life, d, dir)
		if err != nil && err != io.EOF {
			return websiteNode{}, err
		}
		for _, node := range nodes {
			if node.meta.Name == name {
				return node, nil
			}
		}
		if err == io.EOF {
			return websiteNode{}, core.Fail(core.NotFound, nil)
		}
	}
}

func (d *Website) readPage(ctx, life context.Context, dir string, number int) ([]websiteNode, error) {
	call, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(life, cancel)
	defer func() { stop(); cancel() }()
	if err := call.Err(); err != nil {
		return nil, err
	}
	if err := life.Err(); err != nil {
		return nil, err
	}
	got, err := d.read(call, dir, number)
	if err := call.Err(); err != nil {
		return nil, err
	}
	if err := life.Err(); err != nil {
		return nil, err
	}
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil, core.Fail(core.InvalidUpstreamResponse, io.ErrUnexpectedEOF)
		}
		return nil, err
	}
	if got.Account == "" || got.Directory != dir || got.Number != number {
		return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("website page receipt does not identify the requested directory and page"))
	}
	if got.Account != d.account {
		return nil, core.Fail(core.UpstreamAccessDenied, errors.New("website account differs from the saved binding"))
	}
	if len(got.Body) > 4<<20 {
		return nil, core.Fail(core.ResponseBudgetExceeded, nil)
	}
	var native reply
	if json.Unmarshal(got.Body, &native) != nil || native.Errno == nil {
		return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("invalid website directory response"))
	}
	if *native.Errno != 0 {
		return nil, nativeFailure(*native.Errno, reply{}, "")
	}
	if native.List == nil || len(*native.List) > websitePageSize {
		return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("invalid website directory page size"))
	}
	rows := make([]websiteNode, 0, len(*native.List))
	names, ids := make(map[string]bool, len(*native.List)), make(map[uint64]bool, len(*native.List))
	for _, raw := range *native.List {
		if raw.Errno != nil && *raw.Errno != 0 {
			return nil, nativeFailure(*raw.Errno, reply{}, "")
		}
		node, err := acceptChild(raw, dir)
		if err != nil {
			return nil, err
		}
		if *raw.ID == 0 || ids[*raw.ID] || names[node.meta.Name] {
			return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("website directory returned a duplicate or missing item identity"))
		}
		ids[*raw.ID], names[node.meta.Name] = true, true
		rows = append(rows, websiteNode{node.meta, *raw.ID})
	}
	return rows, nil
}

type websitePages struct {
	number int
	before [websitePageSize]uint64
}

func (p *websitePages) next(ctx, life context.Context, driver *Website, dir string) ([]websiteNode, error) {
	if p.number == 0 {
		return nil, io.EOF
	}
	rows, err := driver.readPage(ctx, life, dir, p.number)
	if err != nil {
		return nil, err
	}
	if len(rows) < websitePageSize {
		p.number = 0
		return rows, io.EOF
	}
	var identities [websitePageSize]uint64
	for k, row := range rows {
		identities[k] = row.id
	}
	if identities == p.before {
		return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("website directory repeated the preceding full page"))
	}
	p.before = identities
	p.number++
	return rows, nil
}

type websiteIterator struct {
	driver    *Website
	directory string
	pages     websitePages
	life      context.Context
	cancel    context.CancelFunc
}

func (i *websiteIterator) Close() error {
	i.cancel()
	return nil
}

func (i *websiteIterator) Next(ctx context.Context, n int) ([]core.Metadata, error) {
	if i.life.Err() != nil {
		return nil, io.ErrClosedPipe
	}
	if n < 1 {
		return nil, core.Fail(core.InvalidRequest, nil)
	}
	rows, err := i.pages.next(ctx, i.life, i.driver, i.directory)
	if err != nil && err != io.EOF {
		return nil, err
	}
	batch := make([]core.Metadata, len(rows))
	for k, row := range rows {
		batch[k] = row.meta
	}
	return batch, err
}

var _ core.Driver = (*Website)(nil)
