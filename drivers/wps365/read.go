package wps365

import (
	"context"
	"errors"
	"io"
	"net/url"
	"strconv"
	"strings"
	"sync"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/internal/childscan"
)

type item struct {
	ID     string `json:"id"`
	Drive  string `json:"drive_id"`
	Parent string `json:"parent_id"`
	Name   string `json:"name"`
	Type   string `json:"type"`
	Size   *int64 `json:"size"`
}

func (f item) metadata() core.Metadata {
	if f.Type == "folder" {
		return core.Metadata{Name: f.Name, Kind: core.Directory}
	}
	return core.Metadata{Name: f.Name, Kind: core.File, Size: f.Size}
}

func (d *Driver) children(ctx context.Context, parent, token string, n int) ([]item, string, error) {
	n = min(n, 500)
	query := url.Values{"page_size": {strconv.Itoa(n)}}
	if token != "" {
		query.Set("page_token", token)
	}
	var page struct {
		Items []item `json:"items"`
		Next  string `json:"next_page_token"`
	}
	if err := d.get(ctx, d.fileURL(parent)+"/children?"+query.Encode(), &page); err != nil {
		return nil, "", err
	}
	if page.Items == nil || len(page.Items) > n {
		return nil, "", core.Fail(core.InvalidUpstreamResponse, errors.New("invalid WPS page"))
	}
	for _, f := range page.Items {
		if f.Drive != d.drive || f.Parent != parent || !core.ValidName(f.ID) || f.ID == "0" || f.ID == parent ||
			f.Size != nil && *f.Size < 0 {
			return nil, "", core.Fail(core.InvalidUpstreamResponse, errors.New("invalid WPS child identity"))
		}
		if !core.ValidName(f.Name) {
			return nil, "", core.Fail(core.UnrepresentableName, nil)
		}
		if f.Type != "folder" && f.Type != "file" && f.Type != "shortcut" {
			return nil, "", core.Fail(core.InvalidUpstreamResponse, errors.New("unknown WPS child type"))
		}
	}
	return page.Items, page.Next, nil
}

type listing struct {
	d             *Driver
	parent, token string
	seen          map[string]struct{}
	done          bool
}

func (l *listing) next(ctx context.Context, n int) ([]item, error) {
	if l.done {
		return nil, io.EOF
	}
	items, token, err := l.d.children(ctx, l.parent, l.token, n)
	if err != nil {
		return nil, err
	}
	if token == "" {
		l.done, l.seen = true, nil
		return items, io.EOF
	}
	if _, repeated := l.seen[token]; repeated {
		return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("WPS repeated a page token"))
	}
	if l.seen == nil {
		l.seen = make(map[string]struct{})
	}
	l.token = token
	l.seen[token] = struct{}{}
	return items, nil
}

func (d *Driver) resolve(ctx context.Context, path string) (item, error) {
	current := item{ID: d.root, Type: "folder"}
	if path == "/" {
		return current, nil
	}
	for _, name := range strings.Split(path[1:], "/") {
		if current.Type != "folder" {
			return item{}, core.Fail(core.NotDirectory, nil)
		}
		pages := listing{d: d, parent: current.ID}
		child, found, err := childscan.Unique(ctx, name, func(ctx context.Context) ([]item, error) {
			return pages.next(ctx, 500)
		}, func(f item) (string, string) { return f.ID, f.Name })
		if err != nil {
			return item{}, err
		}
		if !found {
			return item{}, core.Fail(core.NotFound, nil)
		}
		if child.Type == "shortcut" {
			return item{}, core.Fail(core.UnsupportedOperation, errors.New("WPS shortcuts are outside the file view"))
		}
		current = child
	}
	return current, nil
}

func (d *Driver) Enumerate(ctx context.Context, q core.NativeQuery) (core.Iterator, error) {
	if q.Delimiter() != "" || q.StartAfter() != "" {
		return nil, core.Fail(core.UnsupportedOperation, nil)
	}
	if err := d.check(ctx, q.Address()); err != nil {
		return nil, err
	}
	folder, err := d.resolve(ctx, q.Address().Value())
	if err != nil {
		return nil, err
	}
	if folder.Type != "folder" {
		return nil, core.Fail(core.NotDirectory, nil)
	}
	life, cancel := context.WithCancel(context.WithoutCancel(ctx))
	return &iterator{listing: listing{d: d, parent: folder.ID}, life: life, cancel: cancel}, nil
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
	defer cancel()
	defer stop()
	files, err := i.next(call, n)
	if err != nil && err != io.EOF {
		i.failed = err
		return nil, err
	}
	if err := call.Err(); err != nil {
		i.failed = err
		return nil, err
	}
	result := make([]core.Metadata, 0, len(files))
	for _, file := range files {
		if file.Type != "shortcut" {
			result = append(result, file.metadata())
		}
	}
	return result, err
}

func (i *iterator) Close() error {
	i.cancel()
	i.mu.Lock()
	i.seen = nil
	i.mu.Unlock()
	return nil
}
