package codeartsrepo

import (
	"context"
	"errors"
	"io"
	"net/url"
	"strconv"
	"strings"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/internal/childscan"
)

type entry struct {
	ID   string `json:"id"`
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
	driver        *Driver
	parent        string
	offset, total int
	done, closed  bool
}

func (l *listing) next(ctx context.Context, limit int) ([]entry, error) {
	if l.closed {
		return nil, io.ErrClosedPipe
	}
	if err := l.driver.active(ctx); err != nil {
		return nil, err
	}
	if l.done {
		return nil, io.EOF
	}
	query := url.Values{"ref": {l.driver.commit}, "recursive": {"false"},
		"offset": {strconv.Itoa(l.offset)}, "limit": {strconv.Itoa(limit)}}
	prefix := l.parent
	if prefix != "" {
		query.Set("path", l.parent)
		prefix += "/"
	}
	var items []entry
	header, err := l.driver.readJSON(ctx, "/trees", query, &items)
	if err != nil {
		return nil, err
	}
	total, err := strconv.ParseUint(header.Get("X-Total"), 10, 31)
	if err != nil || items == nil || len(items) > limit || len(items) > int(total)-l.offset ||
		(l.offset != 0 && int(total) != l.total) || (len(items) == 0 && l.offset != int(total)) {
		return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("invalid CodeArts Repo tree page"))
	}
	for i := range items {
		e := &items[i]
		if !core.ValidName(e.Name) {
			return nil, core.Fail(core.UnrepresentableName, nil)
		}
		if !core.ValidName(e.ID) || e.Path != prefix+e.Name {
			return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("CodeArts Repo returned an unrelated tree entry"))
		}
		switch {
		case e.Mode == "040000" && e.Type == "tree":
			e.kind = core.Directory
		case (e.Mode == "100644" || e.Mode == "100755") && e.Type == "blob":
			e.kind = core.File
		case e.Mode == "120000" && e.Type == "blob", e.Mode == "160000" && e.Type == "commit":
			// Keep special entries in the lookup scan without following them.
		default:
			return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("invalid CodeArts Repo tree mode"))
		}
	}
	l.total, l.offset = int(total), l.offset+len(items)
	l.done = l.offset == l.total
	if l.done {
		return items, io.EOF
	}
	return items, nil
}

func (l *listing) Next(ctx context.Context, n int) ([]core.Metadata, error) {
	if n < 1 {
		return nil, core.Fail(core.InvalidRequest, nil)
	}
	items, err := l.next(ctx, min(n, 100))
	if err != nil && err != io.EOF {
		return nil, err
	}
	metadata := make([]core.Metadata, 0, len(items))
	for _, e := range items {
		if e.kind != "" {
			metadata = append(metadata, e.metadata())
		}
	}
	return metadata, err
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
	parent := entry{Path: d.root, kind: core.Directory}
	if address.Value() == "/" {
		return parent, nil
	}
	for _, name := range strings.Split(address.Value()[1:], "/") {
		if parent.kind != core.Directory {
			return entry{}, core.Fail(core.NotDirectory, nil)
		}
		pages := listing{driver: d, parent: parent.Path}
		child, found, err := childscan.Unique(ctx, name, func(ctx context.Context) ([]entry, error) {
			return pages.next(ctx, 100)
		}, func(e entry) (string, string) { return e.ID, e.Name })
		if err != nil {
			return entry{}, err
		}
		if !found {
			return entry{}, core.Fail(core.NotFound, nil)
		}
		if child.kind == "" {
			return entry{}, core.Fail(core.UnsupportedOperation, nil)
		}
		parent = child
	}
	return parent, nil
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
	e, err := d.resolve(ctx, q.Address())
	if err != nil {
		return nil, err
	}
	if e.kind != core.Directory {
		return nil, core.Fail(core.NotDirectory, nil)
	}
	return &listing{driver: d, parent: e.Path}, nil
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
	e, err := d.resolve(ctx, address)
	if err != nil {
		return core.Opened{}, err
	}
	if e.kind != core.File {
		return core.Opened{}, core.Fail(core.NotFile, nil)
	}
	if err := o.Selection.Check(core.Evidence{}); err != nil {
		return core.Opened{}, err
	}
	resp, err := d.get(ctx, "/blobs/"+url.PathEscape(e.ID)+"/raw", url.Values{"file_path": {e.Path}}, "*/*")
	if err != nil {
		return core.Opened{}, err
	}
	return d.cleanup.OpenBody(resp, nil, e.Name)
}
