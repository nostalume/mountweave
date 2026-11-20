package uc

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"io"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"

	core "github.com/nostalume/mountweave"
)

const (
	ucPageSize   = 50
	ucPageBudget = 200
)

type ucOpenBody func(context.Context, ucFile, core.OpenOptions) (core.Opened, error)

type ucFile struct {
	id, route string
	meta      core.Metadata
}

// Driver exposes read-only access to a verified UC folder.
type Driver struct {
	account string
	root    ucFile
	call    WebsiteCall
	start   ucOpenBody
	release func() error
	closed  atomic.Bool
}

type ucMetadataRequest struct {
	Operation string `json:"operation"`
	Account   string `json:"account,omitempty"`
	Parent    string `json:"parent,omitempty"`
	Page      int    `json:"page,omitempty"`
}

type ucMetadataReply struct {
	Status  int          `json:"status"`
	Account string       `json:"account"`
	Page    int          `json:"page"`
	Total   *int         `json:"total"`
	Items   []ucListItem `json:"items"`
}

type ucListItem struct {
	ID     string `json:"fid"`
	Parent string `json:"parent"`
	Name   string `json:"name"`
	Kind   *int   `json:"kind"`
	Size   *int64 `json:"size"`
}

var (
	ucFID       = regexp.MustCompile("^[A-Za-z0-9_-]{1,128}$")
	ucRouteName = strings.NewReplacer("/", "*100", "-", "*101", "%", "*102")
)

func validUCAccount(account string) bool {
	if len(account) != 64 || account != strings.ToLower(account) {
		return false
	}
	var digest [32]byte
	_, err := hex.Decode(digest[:], []byte(account))
	return err == nil
}

func readUCMetadata(ctx context.Context, call WebsiteCall, request ucMetadataRequest) (ucMetadataReply, error) {
	if err := ctx.Err(); err != nil {
		return ucMetadataReply{}, err
	}
	var reply ucMetadataReply
	data, err := json.Marshal(request)
	if err != nil {
		return reply, err
	}
	raw, err := call(ctx, ucBrowserMetadata, data)
	if err != nil {
		return reply, err
	}
	if json.Unmarshal(raw, &reply) != nil {
		return reply, core.Fail(core.InvalidUpstreamResponse, nil)
	}
	if err := ctx.Err(); err != nil {
		return reply, err
	}
	switch reply.Status {
	case 401:
		return reply, core.Fail(core.UpstreamAccessDenied, ErrWebsiteSignIn)
	case 403:
		return reply, core.Fail(core.UpstreamAccessDenied, nil)
	case 429:
		return reply, core.Fail(core.UpstreamThrottled, nil)
	case 408:
		return reply, core.Fail(core.UpstreamTimeout, nil)
	case 200:
		if !validUCAccount(reply.Account) {
			return reply, core.Fail(core.InvalidUpstreamResponse, nil)
		}
		if request.Account != "" && request.Account != reply.Account {
			return reply, core.Fail(core.UpstreamAccessDenied, ErrWebsiteBindingMismatch)
		}
		return reply, nil
	default:
		if reply.Status == 0 {
			return reply, core.Fail(core.InvalidUpstreamResponse, nil)
		}
		return reply, core.Fail(core.UpstreamFailure, nil)
	}
}

// OpenWebsite checks the bound account, root identity and first native page.
func OpenWebsite(ctx context.Context, o WebsiteOptions) (*Driver, error) {
	if o.Tab == nil || o.Call == nil || o.Release == nil || !validUCAccount(o.Account) {
		return nil, core.Fail(core.InvalidRequest, nil)
	}
	if err := core.ValidatePath(o.RootPath); err != nil {
		return nil, err
	}
	origin, endpoint, err := o.downloadEndpoints()
	if err != nil {
		return nil, err
	}
	d := &Driver{
		account: o.Account,
		root:    ucFile{id: "0", route: "/list/all", meta: core.Metadata{Kind: core.Directory}},
		call:    o.Call,
		start:   openBrowserDownload(o.Tab, o.Call, o.Account, origin, endpoint),
	}
	root, err := d.resolve(ctx, core.NativePath(o.RootPath))
	if err != nil {
		return nil, err
	}
	if root.meta.Kind != core.Directory {
		return nil, core.Fail(core.NotDirectory, nil)
	}
	if o.RootID != "" && root.id != o.RootID {
		return nil, core.Fail(core.UpstreamAccessDenied, ErrWebsiteBindingMismatch)
	}
	d.root = root
	page := ucListing{driver: d, parent: root}
	if _, err := page.next(ctx); err != nil && err != io.EOF {
		return nil, err
	}
	d.release = sync.OnceValue(o.Release)
	return d, nil
}

// RootID returns the native identity of the mounted folder.
func (d *Driver) RootID() string { return d.root.id }

func (d *Driver) Close() error {
	d.closed.Store(true)
	return d.release()
}

func (d *Driver) checkAccount(ctx context.Context) error {
	_, err := readUCMetadata(ctx, d.call, ucMetadataRequest{Operation: "account", Account: d.account})
	return err
}

func (d *Driver) ready(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if d.closed.Load() {
		return core.Fail(core.MountUnavailable, nil)
	}
	return nil
}

type ucListing struct {
	driver      *Driver
	parent      ucFile
	page, total int
	seen        map[string]struct{}
	done        bool
}

func (i *ucListing) next(ctx context.Context) ([]ucFile, error) {
	if err := i.driver.ready(ctx); err != nil {
		return nil, err
	}
	if i.done {
		return nil, io.EOF
	}
	if i.page == ucPageBudget {
		return nil, core.Fail(core.ResolutionBudgetExceeded, nil)
	}
	page := i.page + 1
	reply, err := readUCMetadata(ctx, i.driver.call, ucMetadataRequest{
		Operation: "page", Account: i.driver.account, Parent: i.parent.id, Page: page,
	})
	if err != nil {
		return nil, err
	}
	if reply.Page != page || reply.Total == nil || reply.Items == nil ||
		*reply.Total < (page-1)*ucPageSize || i.page != 0 && *reply.Total != i.total ||
		len(reply.Items) != min(ucPageSize, *reply.Total-(page-1)*ucPageSize) {
		return nil, core.Fail(core.InvalidUpstreamResponse, nil)
	}
	if i.seen == nil {
		i.seen = make(map[string]struct{})
	}
	files := make([]ucFile, 0, len(reply.Items))
	for _, item := range reply.Items {
		if _, duplicate := i.seen[item.ID]; duplicate {
			return nil, core.Fail(core.InvalidUpstreamResponse, nil)
		}
		file, err := item.file(i.parent)
		if err != nil {
			return nil, err
		}
		i.seen[item.ID] = struct{}{}
		files = append(files, file)
	}
	i.page, i.total = page, *reply.Total
	if page*ucPageSize >= i.total {
		i.done = true
		i.seen = nil
		return files, io.EOF
	}
	return files, nil
}

func (i *ucListing) Next(ctx context.Context, _ int) ([]core.Metadata, error) {
	files, err := i.next(ctx)
	if len(files) == 0 {
		return nil, err
	}
	result := make([]core.Metadata, len(files))
	for index, file := range files {
		result[index] = file.meta
	}
	return result, err
}

func (i *ucListing) Close() error {
	i.seen = nil
	i.done = true
	return nil
}

func (item ucListItem) file(parent ucFile) (ucFile, error) {
	if !ucFID.MatchString(item.ID) || item.ID == "0" || item.ID == parent.id ||
		item.Parent != "" && item.Parent != parent.id || item.Kind == nil {
		return ucFile{}, core.Fail(core.InvalidUpstreamResponse, nil)
	}
	if !core.ValidName(item.Name) {
		return ucFile{}, core.Fail(core.UnrepresentableName, nil)
	}
	file := ucFile{id: item.ID, route: parent.route, meta: core.Metadata{Name: item.Name}}
	switch *item.Kind {
	case 0:
		file.meta.Kind = core.Directory
		if strings.Contains(file.id, "-") {
			file.route = ""
		}
		if file.route != "" {
			file.route += "/" + file.id + "-" + ucRouteName.Replace(file.meta.Name)
		}
	case 1:
		if item.Size == nil || *item.Size < 0 {
			return ucFile{}, core.Fail(core.InvalidUpstreamResponse, nil)
		}
		file.meta.Kind, file.meta.Size = core.File, item.Size
	default:
		return ucFile{}, core.Fail(core.InvalidUpstreamResponse, nil)
	}
	return file, nil
}

func (d *Driver) resolve(ctx context.Context, address core.NativeAddress) (ucFile, error) {
	if err := d.ready(ctx); err != nil {
		return ucFile{}, err
	}
	if address.Namespace() != core.TreeNamespace {
		return ucFile{}, core.Fail(core.UnsupportedOperation, nil)
	}
	p := address.Value()
	if err := core.ValidatePath(p); err != nil {
		return ucFile{}, err
	}
	found := d.root
	if p == "/" {
		return found, nil
	}
	pages := 0
	for _, name := range strings.Split(p[1:], "/") {
		if found.meta.Kind != core.Directory {
			return ucFile{}, core.Fail(core.NotDirectory, nil)
		}
		list := ucListing{driver: d, parent: found}
		found = ucFile{}
		for {
			if pages >= ucPageBudget {
				return ucFile{}, core.Fail(core.ResolutionBudgetExceeded, nil)
			}
			pages++
			files, err := list.next(ctx)
			if err != nil && err != io.EOF {
				return ucFile{}, err
			}
			for _, file := range files {
				if file.meta.Name == name {
					if found.id != "" {
						return ucFile{}, core.Fail(core.PathAmbiguous, nil)
					}
					found = file
				}
			}
			if err == io.EOF {
				break
			}
		}
		if found.id == "" {
			return ucFile{}, core.Fail(core.NotFound, nil)
		}
	}
	return found, nil
}

func ucReadSelection(selection core.ReadSelection) error {
	if selection.Version != "" {
		return core.Fail(core.UnsupportedOperation, nil)
	}
	return selection.When.ForDomain("")
}

func (d *Driver) Stat(ctx context.Context, address core.NativeAddress, selections ...core.ReadSelection) (core.Metadata, error) {
	if len(selections) > 1 {
		return core.Metadata{}, core.Fail(core.InvalidRequest, nil)
	}
	if len(selections) == 1 {
		if err := ucReadSelection(selections[0]); err != nil {
			return core.Metadata{}, err
		}
	}
	file, err := d.resolve(ctx, address)
	if err != nil {
		return core.Metadata{}, err
	}
	if address.Value() == "/" {
		if err := d.checkAccount(ctx); err != nil {
			return core.Metadata{}, err
		}
	}
	if len(selections) == 1 {
		return file.meta, selections[0].Check(file.meta.Evidence)
	}
	return file.meta, nil
}

func (d *Driver) Enumerate(ctx context.Context, q core.NativeQuery) (core.Iterator, error) {
	if q.Delimiter() != "" || q.StartAfter() != "" {
		return nil, core.Fail(core.UnsupportedOperation, nil)
	}
	parent, err := d.resolve(ctx, q.Address())
	if err != nil {
		return nil, err
	}
	if parent.meta.Kind != core.Directory {
		return nil, core.Fail(core.NotDirectory, nil)
	}
	return &ucListing{driver: d, parent: parent}, nil
}

func (d *Driver) Open(ctx context.Context, address core.NativeAddress, options core.OpenOptions) (core.Opened, error) {
	if err := options.Validate(); err != nil {
		return core.Opened{}, err
	}
	if options.Mode != core.StreamRead || options.NoFollow {
		return core.Opened{}, core.Fail(core.UnsupportedOperation, nil)
	}
	if err := ucReadSelection(options.Selection); err != nil {
		return core.Opened{}, err
	}
	file, err := d.resolve(ctx, address)
	if err != nil {
		return core.Opened{}, err
	}
	if file.meta.Kind != core.File {
		return core.Opened{}, core.Fail(core.NotFile, nil)
	}
	if err := options.Selection.Check(file.meta.Evidence); err != nil {
		return core.Opened{}, err
	}
	return d.start(ctx, file, options)
}
