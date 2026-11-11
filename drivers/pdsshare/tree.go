package pdsshare

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"time"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/internal/childscan"
)

type item struct {
	resourceScope
	Domain   string     `json:"domain_id"`
	ID       string     `json:"file_id"`
	Parent   string     `json:"parent_file_id"`
	Name     string     `json:"name"`
	Type     string     `json:"type"`
	Status   string     `json:"status"`
	Size     *int64     `json:"size"`
	Modified *time.Time `json:"updated_at"`
}

func (f item) metadata() core.Metadata {
	meta := core.Metadata{Name: f.Name, Kind: core.File, Size: f.Size, ModifiedAt: f.Modified}
	if f.Type == "folder" {
		meta.Kind, meta.Size = core.Directory, nil
	}
	return meta
}

type page struct {
	Items []item `json:"items"`
	Next  string `json:"next_marker"`
}

func (d *Driver) inScope(f item) bool {
	if d.scope.DriveID == "" {
		return f.ShareID == "" || f.ShareID == d.scope.ShareID
	}
	return f.DriveID == d.scope.DriveID && f.ShareID == "" &&
		(f.Domain == "" || strings.EqualFold(f.Domain, d.domain)) &&
		(f.Status == "" || f.Status == "available")
}

func (d *Driver) page(ctx context.Context, parent, marker string, limit int) (page, error) {
	var p page
	token, err := d.credential(ctx)
	if err != nil {
		return p, err
	}
	fields := struct {
		resourceScope
		Parent string `json:"parent_file_id"`
		Marker string `json:"marker,omitempty"`
		Limit  int    `json:"limit"`
	}{d.scope, parent, marker, limit}
	if err := d.post(ctx, "/v2/file/list", token, fields, &p); err != nil {
		return p, err
	}
	if p.Items == nil {
		return p, core.Fail(core.InvalidUpstreamResponse, errors.New("PDS page is missing items"))
	}
	virtualRoot := d.scope.ShareID != "" && parent == d.root
	for _, f := range p.Items {
		// The virtual share root can contain items from distinct native parents.
		if f.ID == "" || f.ID == "root" || f.ID == d.root || f.ID == parent || !virtualRoot && f.Parent != parent ||
			!d.inScope(f) ||
			f.Type != "file" && f.Type != "folder" || f.Size != nil && *f.Size < 0 {
			return p, core.Fail(core.InvalidUpstreamResponse, errors.New("invalid PDS child identity or scope"))
		}
		if !core.ValidName(f.Name) {
			return p, core.Fail(core.UnrepresentableName, nil)
		}
	}
	return p, ctx.Err()
}

type listing struct {
	driver         *Driver
	parent, marker string
	seen           map[string]struct{}
	done           bool
}

func (l *listing) next(ctx context.Context, n int) ([]item, error) {
	if l.done {
		return nil, io.EOF
	}
	p, err := l.driver.page(ctx, l.parent, l.marker, min(100, n))
	if err != nil {
		return nil, err
	}
	if p.Next == "" {
		l.done, l.seen = true, nil
		return p.Items, io.EOF
	}
	if _, repeated := l.seen[p.Next]; repeated {
		return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("PDS repeated a page marker"))
	}
	if l.seen == nil {
		l.seen = make(map[string]struct{})
	}
	l.marker = p.Next
	l.seen[l.marker] = struct{}{}
	return p.Items, nil
}

func (d *Driver) lookup(ctx context.Context, address core.NativeAddress) (item, error) {
	if err := ctx.Err(); err != nil {
		return item{}, err
	}
	if d.closed.Load() {
		return item{}, core.Fail(core.MountUnavailable, nil)
	}
	if address.Namespace() != core.TreeNamespace {
		return item{}, core.Fail(core.UnsupportedOperation, nil)
	}
	value := address.Value()
	if err := core.ValidatePath(value); err != nil {
		return item{}, err
	}
	current := item{ID: d.root, Type: "folder"}
	if value == "/" {
		return current, nil
	}
	for _, name := range strings.Split(value[1:], "/") {
		if current.Type != "folder" {
			return item{}, core.Fail(core.NotDirectory, nil)
		}
		l := listing{driver: d, parent: current.ID}
		pages := 0
		child, found, err := childscan.Unique(ctx, name, func(ctx context.Context) ([]item, error) {
			if d.resolvePages > 0 && pages == d.resolvePages {
				return nil, core.Fail(core.ResolutionBudgetExceeded, nil)
			}
			pages++
			return l.next(ctx, 100)
		}, func(f item) (string, string) { return f.ID, f.Name })
		if err != nil {
			return item{}, err
		}
		if !found {
			return item{}, core.Fail(core.NotFound, nil)
		}
		current = child
	}
	return current, nil
}

func (d *Driver) Stat(ctx context.Context, address core.NativeAddress, options ...core.ReadSelection) (core.Metadata, error) {
	if len(options) > 1 {
		return core.Metadata{}, core.Fail(core.InvalidRequest, nil)
	}
	var selection core.ReadSelection
	if len(options) == 1 {
		selection = options[0]
	}
	if selection.Version != "" {
		return core.Metadata{}, core.Fail(core.UnsupportedOperation, nil)
	}
	if err := selection.When.ForDomain(""); err != nil {
		return core.Metadata{}, err
	}
	f, err := d.lookup(ctx, address)
	if err != nil {
		return core.Metadata{}, err
	}
	if address.Value() == "/" {
		if _, err := d.page(ctx, f.ID, "", 1); err != nil {
			return core.Metadata{}, err
		}
	}
	return f.metadata(), selection.Check(core.Evidence{})
}

func (d *Driver) Enumerate(ctx context.Context, query core.NativeQuery) (core.Iterator, error) {
	if query.Delimiter() != "" || query.StartAfter() != "" {
		return nil, core.Fail(core.UnsupportedOperation, nil)
	}
	f, err := d.lookup(ctx, query.Address())
	if err != nil {
		return nil, err
	}
	if f.Type != "folder" {
		return nil, core.Fail(core.NotDirectory, nil)
	}
	life, cancel := context.WithCancel(context.WithoutCancel(ctx))
	return &iterator{listing: listing{driver: d, parent: f.ID}, life: life, cancel: cancel}, nil
}

type iterator struct {
	listing
	life   context.Context
	cancel context.CancelFunc
	mu     sync.Mutex
	failed error
}

func (i *iterator) Next(ctx context.Context, n int) ([]core.Metadata, error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.life.Err() != nil {
		return nil, io.ErrClosedPipe
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if n < 1 {
		return nil, core.Fail(core.InvalidRequest, nil)
	}
	if i.failed != nil {
		return nil, i.failed
	}
	call, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(i.life, cancel)
	defer stop()
	defer cancel()
	files, err := i.listing.next(call, n)
	if cause := call.Err(); cause != nil && !errors.Is(err, cause) {
		if err == io.EOF {
			err = nil
		}
		err = errors.Join(err, cause)
	}
	if err != nil && err != io.EOF {
		i.failed = err
		return nil, err
	}
	items := make([]core.Metadata, len(files))
	for n, file := range files {
		items[n] = file.metadata()
	}
	return items, err
}

func (i *iterator) Close() error {
	i.cancel()
	i.mu.Lock()
	i.seen = nil
	i.mu.Unlock()
	return nil
}
