package atomgit

import (
	"context"
	"errors"
	"io"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/internal/childscan"
)

const pageSize = 100

type entry struct {
	SHA  string `json:"sha"`
	Name string `json:"name"`
	Path string `json:"path"`
	Type string `json:"type"`
	Mode string `json:"mode"`
	kind core.Kind
}

func (e entry) metadata() core.Metadata {
	return core.Metadata{Name: e.Name, Kind: e.kind}
}

type listing struct {
	driver *Driver
	parent string
	first  string
	page   int
	closed bool
}

func (l *listing) next(ctx context.Context) ([]entry, error) {
	if l.closed {
		return nil, io.ErrClosedPipe
	}
	if err := l.driver.active(ctx); err != nil {
		return nil, err
	}
	if l.page == 0 {
		return nil, io.EOF
	}
	// Numbered pages use a fixed size. Core slices the resulting batch for
	// callers and retains only the remainder of the current page.
	q := url.Values{"page": {strconv.Itoa(l.page)}, "per_page": {strconv.Itoa(pageSize)}}
	prefix := l.parent
	if prefix != "" {
		q.Set("file_path", prefix)
		prefix += "/"
	}
	var result struct {
		Tree []entry `json:"tree"`
		SHA  string  `json:"sha"`
	}
	err := l.driver.readJSON(ctx, "/git/trees/"+l.driver.commit+"?"+q.Encode(), &result)
	if err != nil {
		return nil, err
	}
	if result.Tree == nil || len(result.Tree) > pageSize ||
		(result.SHA != "" && !strings.EqualFold(result.SHA, l.driver.commit)) {
		return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("invalid AtomGit tree page"))
	}
	if len(result.Tree) == 0 {
		l.page = 0
		return nil, io.EOF
	}
	for i := range result.Tree {
		e := &result.Tree[i]
		if !core.ValidName(e.Name) {
			return nil, core.Fail(core.UnrepresentableName, nil)
		}
		if e.Path != prefix+e.Name || !validSHA(e.SHA) {
			return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("AtomGit returned an unrelated tree entry"))
		}
		switch {
		case e.Mode == "040000" && e.Type == "tree":
			e.kind = core.Directory
		case (e.Mode == "100644" || e.Mode == "100755") && e.Type == "blob":
			e.kind = core.File
		case e.Mode == "120000" && e.Type == "blob", e.Mode == "160000" && e.Type == "commit":
			// Keep the name for exact lookup without following the entry.
		default:
			return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("invalid AtomGit tree mode"))
		}
	}
	if result.Tree[0].Name == l.first || l.page == math.MaxInt {
		return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("AtomGit tree page did not advance"))
	}
	l.first = result.Tree[0].Name
	l.page++
	return result.Tree, nil
}

func (l *listing) Next(ctx context.Context, n int) ([]core.Metadata, error) {
	if n < 1 {
		return nil, core.Fail(core.InvalidRequest, nil)
	}
	items, err := l.next(ctx)
	if err != nil {
		return nil, err
	}
	result := make([]core.Metadata, 0, len(items))
	for _, e := range items {
		if e.kind != "" {
			result = append(result, e.metadata())
		}
	}
	return result, nil
}

func (l *listing) Close() error { l.closed = true; return nil }

func (d *Driver) resolve(ctx context.Context, address core.NativeAddress) (entry, error) {
	if err := d.active(ctx); err != nil {
		return entry{}, err
	}
	if address.Namespace() != core.TreeNamespace {
		return entry{}, core.Fail(core.UnsupportedOperation, nil)
	}
	if err := core.ValidatePath(address.Value()); err != nil {
		return entry{}, err
	}
	at := entry{Path: d.root, kind: core.Directory}
	if address.Value() == "/" {
		return at, nil
	}
	for _, name := range strings.Split(address.Value()[1:], "/") {
		if at.kind != core.Directory {
			return entry{}, core.Fail(core.NotDirectory, nil)
		}
		pages := listing{driver: d, parent: at.Path, page: 1}
		child, found, err := childscan.Unique(ctx, name, pages.next, func(e entry) (string, string) {
			return e.SHA, e.Name
		})
		if err != nil {
			return entry{}, err
		}
		if !found {
			return entry{}, core.Fail(core.NotFound, nil)
		}
		if child.kind == "" {
			return entry{}, core.Fail(core.UnsupportedOperation, nil)
		}
		at = child
	}
	return at, nil
}

func (d *Driver) Stat(ctx context.Context, address core.NativeAddress, selections ...core.ReadSelection) (core.Metadata, error) {
	if len(selections) > 1 {
		return core.Metadata{}, core.Fail(core.InvalidRequest, nil)
	}
	var selection core.ReadSelection
	if len(selections) == 1 {
		selection = selections[0]
	}
	if selection.Version != "" {
		return core.Metadata{}, core.Fail(core.UnsupportedOperation, nil)
	}
	if err := selection.When.ForDomain(""); err != nil {
		return core.Metadata{}, err
	}
	e, err := d.resolve(ctx, address)
	if err != nil {
		return core.Metadata{}, err
	}
	return e.metadata(), selection.Check(core.Evidence{})
}

func (d *Driver) Enumerate(ctx context.Context, q core.NativeQuery) (core.Iterator, error) {
	if q.Delimiter() != "" || q.StartAfter() != "" {
		return nil, core.Fail(core.UnsupportedOperation, nil)
	}
	at, err := d.resolve(ctx, q.Address())
	if err != nil {
		return nil, err
	}
	if at.kind != core.Directory {
		return nil, core.Fail(core.NotDirectory, nil)
	}
	return &listing{driver: d, parent: at.Path, page: 1}, nil
}

func (d *Driver) Open(ctx context.Context, address core.NativeAddress, o core.OpenOptions) (core.Opened, error) {
	if err := o.Validate(); err != nil {
		return core.Opened{}, err
	}
	if o.Mode != core.StreamRead || o.NoFollow || o.Selection.Version != "" {
		return core.Opened{}, core.Fail(core.UnsupportedOperation, nil)
	}
	if err := o.Selection.When.ForDomain(""); err != nil {
		return core.Opened{}, err
	}
	at, err := d.resolve(ctx, address)
	if err != nil {
		return core.Opened{}, err
	}
	if at.kind != core.File {
		return core.Opened{}, core.Fail(core.NotFile, nil)
	}
	if err := o.Selection.Check(core.Evidence{}); err != nil {
		return core.Opened{}, err
	}
	resp, err := d.get(ctx, "/raw/"+url.PathEscape(at.Path)+"?ref="+d.commit, "*/*", o.Range)
	if err != nil {
		return core.Opened{}, err
	}
	switch resp.StatusCode {
	case http.StatusOK, http.StatusPartialContent, http.StatusRequestedRangeNotSatisfiable:
		return d.cleanup.OpenBody(resp, o.Range, at.Name)
	default:
		return core.Opened{}, d.reject(ctx, resp)
	}
}
