package babelbird

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/internal/childscan"
	"github.com/nostalume/mountweave/drivers/internal/httpcontent"
)

const (
	pageSize = 50
	files    = "1"
	folders  = "2"
)

type entry struct {
	ID   string `json:"fileId"`
	Name string `json:"name"`
	kind core.Kind
}

func (e entry) metadata() core.Metadata { return core.Metadata{Name: e.Name, Kind: e.kind} }

type listing struct {
	driver         *Driver
	parent, filter string
	page, pages    int
	total, seen    int
	closed         bool
}

func (l *listing) readPage(ctx context.Context) ([]entry, error) {
	q := url.Values{"parent_id": {l.parent}, "type": {l.filter},
		"page": {strconv.Itoa(l.page)}, "page_size": {strconv.Itoa(pageSize)}}
	var reply struct {
		result
		Entries []entry `json:"files"`
		Total   *int    `json:"total"`
		Pages   *int    `json:"pages"`
	}
	if err := l.driver.readJSON(ctx, "/nd/api/file/list_dir", q, &reply); err != nil {
		return nil, err
	}
	if err := reply.result.check(); err != nil {
		return nil, err
	}
	if reply.Total == nil || reply.Pages == nil || reply.Entries == nil || len(reply.Entries) > pageSize {
		return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("invalid BabelBird directory page"))
	}
	if l.page == 0 {
		l.total, l.pages = *reply.Total, *reply.Pages
	}
	remaining := l.total - l.seen
	last := l.pages == 0 || l.page == l.pages-1
	if l.pages < 0 || *reply.Pages != l.pages || *reply.Total != l.total || (l.pages == 0 && l.total != 0) ||
		len(reply.Entries) > remaining || (last && len(reply.Entries) != remaining) ||
		(!last && (len(reply.Entries) == 0 || len(reply.Entries) == remaining)) {
		return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("inconsistent BabelBird pagination"))
	}
	kind := core.File
	if l.filter == folders {
		kind = core.Directory
	}
	for i := range reply.Entries {
		e := &reply.Entries[i]
		if !core.ValidName(e.Name) {
			return nil, core.Fail(core.UnrepresentableName, nil)
		}
		switch e.ID {
		case "", "1", "2", "3", "4", l.parent, l.driver.root:
			return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("invalid BabelBird child ID"))
		}
		e.kind = kind
	}
	l.seen += len(reply.Entries)
	l.page++
	if last {
		return reply.Entries, io.EOF
	}
	return reply.Entries, nil
}

// next joins two typed enumerations. The common list response does not
// specify a reliable type discriminator in each entry.
func (l *listing) next(ctx context.Context) ([]entry, error) {
	if l.closed {
		return nil, io.ErrClosedPipe
	}
	if l.filter == "" {
		return nil, io.EOF
	}
	entries, err := l.readPage(ctx)
	if err != io.EOF {
		return entries, err
	}
	if l.filter == files {
		l.filter = ""
		return entries, io.EOF
	}
	l.filter, l.page, l.seen = files, 0, 0
	if len(entries) == 0 {
		return l.next(ctx)
	}
	return entries, nil
}

func (l *listing) Next(ctx context.Context, n int) ([]core.Metadata, error) {
	if n < 1 {
		return nil, core.Fail(core.InvalidRequest, nil)
	}
	entries, err := l.next(ctx)
	items := make([]core.Metadata, len(entries))
	for i, e := range entries {
		items[i] = e.metadata()
	}
	return items, err
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
	at := entry{ID: d.root, kind: core.Directory}
	if address.Value() == "/" {
		return at, nil
	}
	for _, name := range strings.Split(address.Value()[1:], "/") {
		if at.kind != core.Directory {
			return entry{}, core.Fail(core.NotDirectory, nil)
		}
		scan := listing{driver: d, parent: at.ID, filter: folders}
		child, found, err := childscan.Unique(ctx, name, scan.next, func(e entry) (string, string) {
			return e.ID, e.Name
		})
		if err != nil {
			return entry{}, err
		}
		if !found {
			return entry{}, core.Fail(core.NotFound, nil)
		}
		at = child
	}
	return at, nil
}

func (d *Driver) Stat(ctx context.Context, address core.NativeAddress, selections ...core.ReadSelection) (core.Metadata, error) {
	if len(selections) > 1 {
		return core.Metadata{}, core.Fail(core.InvalidRequest, nil)
	}
	if len(selections) == 1 && (selections[0].Version != "" || !selections[0].When.Empty()) {
		return core.Metadata{}, core.Fail(core.UnsupportedOperation, nil)
	}
	e, err := d.resolve(ctx, address)
	return e.metadata(), err
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
	return &listing{driver: d, parent: at.ID, filter: folders}, nil
}

func (d *Driver) Open(ctx context.Context, address core.NativeAddress, o core.OpenOptions) (core.Opened, error) {
	if err := o.Validate(); err != nil {
		return core.Opened{}, err
	}
	if o.Mode != core.StreamRead || o.NoFollow || o.Selection.Version != "" || !o.Selection.When.Empty() {
		return core.Opened{}, core.Fail(core.UnsupportedOperation, nil)
	}
	at, err := d.resolve(ctx, address)
	if err != nil {
		return core.Opened{}, err
	}
	if at.kind != core.File {
		return core.Opened{}, core.Fail(core.NotFile, nil)
	}
	var reply struct {
		result
		URL string `json:"url"`
	}
	if err := d.readJSON(ctx, "/api/file/getFileDownloadUrl.do", url.Values{"fileId": {at.ID}}, &reply); err != nil {
		return core.Opened{}, err
	}
	if err := reply.result.check(); err != nil {
		return core.Opened{}, err
	}
	u, err := url.Parse(reply.URL)
	if err != nil || u.Scheme != "https" || u.Hostname() == "" || u.User != nil ||
		u.Opaque != "" || strings.ContainsRune(reply.URL, '#') {
		return core.Opened{}, core.Fail(core.InvalidUpstreamResponse, errors.New("invalid BabelBird download URL"))
	}
	byteRange := httpcontent.Header(o.Range)
	resp, err := httpcontent.FollowDownload(reply.URL, func(address string, _ bool) (*http.Response, error) {
		return d.get(ctx, &d.content, address, "*/*", byteRange)
	}, d.cleanup.CloseBody)
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
