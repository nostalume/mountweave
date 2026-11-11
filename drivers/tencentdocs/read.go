package tencentdocs

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/internal/childscan"
)

type item struct {
	ID       string `json:"ID"`
	Title    string `json:"title"`
	Type     string `json:"type"`
	Status   string `json:"status"`
	Shortcut string `json:"shortcutID"`
	Modified int64  `json:"lastModifyTime"`
}

func (f item) identity() string {
	if f.Shortcut != "" {
		return f.Shortcut
	}
	return f.ID
}

func (f item) directory() bool { return f.Type == "folder" && f.Shortcut == "" }

func (f item) metadata() core.Metadata {
	meta := core.Metadata{Name: f.Title, Kind: core.File}
	if f.directory() {
		meta.Kind = core.Directory
	}
	if f.Modified > 0 {
		t := time.Unix(f.Modified, 0)
		meta.ModifiedAt = &t
	}
	return meta
}

type listing struct {
	driver       *Driver
	parent       string
	start        int
	done, closed bool
}

func (l *listing) next(ctx context.Context) ([]item, error) {
	if l.closed {
		return nil, io.ErrClosedPipe
	}
	if err := l.driver.active(ctx); err != nil {
		return nil, err
	}
	if l.done {
		return nil, io.EOF
	}
	route := "/folders/" + url.PathEscape(l.parent) + "?start=" + strconv.Itoa(l.start) + "&limit=20"
	page, err := call[struct {
		Next *int   `json:"next"`
		List []item `json:"list"`
	}](ctx, l.driver, http.MethodGet, route)
	if err != nil {
		return nil, err
	}
	if page.Next == nil || (*page.Next != 0 && (*page.Next <= l.start || page.List == nil)) || len(page.List) > 20 {
		return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("invalid Tencent Docs page"))
	}
	items := page.List[:0]
	for _, f := range page.List {
		id := f.identity()
		switch {
		case strings.EqualFold(f.Status, "trash"):
			continue
		case !strings.EqualFold(f.Status, "normal") || f.Type == "" || !core.ValidName(f.ID) ||
			!core.ValidName(id) || id == l.parent || id == l.driver.root:
			return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("invalid Tencent Docs child"))
		case !core.ValidName(f.Title):
			return nil, core.Fail(core.UnrepresentableName, nil)
		}
		items = append(items, f)
	}
	l.start, l.done = *page.Next, *page.Next == 0
	if l.done {
		return items, io.EOF
	}
	return items, nil
}

func (l *listing) Next(ctx context.Context, n int) ([]core.Metadata, error) {
	if n < 1 {
		return nil, core.Fail(core.InvalidRequest, nil)
	}
	items, err := l.next(ctx)
	if err != nil && err != io.EOF {
		return nil, err
	}
	metadata := make([]core.Metadata, len(items))
	for i, f := range items {
		metadata[i] = f.metadata()
	}
	return metadata, err
}

func (l *listing) Close() error { l.closed = true; return nil }

func (d *Driver) resolve(ctx context.Context, p core.NativeAddress) (item, error) {
	if err := d.active(ctx); err != nil {
		return item{}, err
	}
	if p.Namespace() != core.TreeNamespace {
		return item{}, core.Fail(core.UnsupportedOperation, nil)
	}
	if err := core.ValidatePath(p.Value()); err != nil {
		return item{}, err
	}
	parent := item{ID: d.root, Type: "folder"}
	if p.Value() == "/" {
		return parent, nil
	}
	for _, name := range strings.Split(p.Value()[1:], "/") {
		if !parent.directory() {
			return item{}, core.Fail(core.NotDirectory, nil)
		}
		page := listing{driver: d, parent: parent.ID}
		child, found, err := childscan.Unique(ctx, name, page.next, func(f item) (string, string) {
			return f.identity(), f.Title
		})
		if err != nil {
			return item{}, err
		}
		if !found {
			return item{}, core.Fail(core.NotFound, nil)
		}
		parent = child
	}
	return parent, nil
}

func (d *Driver) Stat(ctx context.Context, p core.NativeAddress, selections ...core.ReadSelection) (core.Metadata, error) {
	if len(selections) > 1 {
		return core.Metadata{}, core.Fail(core.InvalidRequest, nil)
	}
	if len(selections) == 1 && (selections[0].Version != "" || !selections[0].When.Empty()) {
		return core.Metadata{}, core.Fail(core.UnsupportedOperation, nil)
	}
	f, err := d.resolve(ctx, p)
	if err != nil {
		return core.Metadata{}, err
	}
	return f.metadata(), nil
}

func (d *Driver) Enumerate(ctx context.Context, q core.NativeQuery) (core.Iterator, error) {
	if q.Delimiter() != "" || q.StartAfter() != "" {
		return nil, core.Fail(core.UnsupportedOperation, nil)
	}
	f, err := d.resolve(ctx, q.Address())
	if err != nil {
		return nil, err
	}
	if !f.directory() {
		return nil, core.Fail(core.NotDirectory, nil)
	}
	return &listing{driver: d, parent: f.ID}, nil
}
