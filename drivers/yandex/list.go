package yandex

import (
	"context"
	"errors"
	"io"
	"net/url"
	"strconv"
	"sync"

	core "github.com/nostalume/mountweave"
)

func (d *Driver) Enumerate(ctx context.Context, q core.NativeQuery) (core.Iterator, error) {
	if q.Delimiter() != "" || q.StartAfter() != "" {
		return nil, core.Fail(core.UnsupportedOperation, nil)
	}
	target, err := d.target(ctx, q.Address())
	if err != nil {
		return nil, err
	}
	life, cancel := context.WithCancel(context.WithoutCancel(ctx))
	return &iterator{d: d, target: target, life: life, cancel: cancel}, nil
}

type iterator struct {
	d            *Driver
	target       string
	life         context.Context
	cancel       context.CancelFunc
	mu           sync.Mutex
	offset       int64
	done, closed bool
	failed       error
}

func (i *iterator) Close() error {
	i.cancel()
	i.mu.Lock()
	defer i.mu.Unlock()
	i.closed = true
	return nil
}

func (i *iterator) Next(ctx context.Context, n int) (items []core.Metadata, err error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.closed {
		return nil, io.ErrClosedPipe
	}
	if n < 1 {
		return nil, core.Fail(core.InvalidRequest, nil)
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	if i.failed != nil {
		return nil, i.failed
	}
	if i.done {
		return nil, io.EOF
	}
	call, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(i.life, cancel)
	defer func() {
		stop()
		cancel()
		if err != nil && err != io.EOF {
			i.failed = err
		}
	}()
	if i.d.closed.Load() {
		return nil, core.Fail(core.MountUnavailable, nil)
	}
	limit := min(n, 1000)
	query := url.Values{"path": {i.target}, "offset": {strconv.FormatInt(i.offset, 10)}, "limit": {strconv.Itoa(limit)}, "sort": {"name"}}
	response, err := i.d.request(call, "GET", i.d.address("/resources", query))
	if err != nil {
		return nil, err
	}
	var r resource
	if _, err = i.d.decode(response, &r); err != nil {
		return nil, err
	}
	meta, err := r.metadata(i.d.observed(i.target))
	if err != nil {
		return nil, err
	}
	if meta.Kind != core.Directory {
		return nil, core.Fail(core.NotDirectory, nil)
	}
	page := r.Embedded
	if page == nil || page.Offset == nil || page.Limit == nil || page.Total == nil || page.Items == nil ||
		*page.Offset != i.offset || *page.Limit < 1 || *page.Total < i.offset || int64(len(page.Items)) > *page.Limit || int64(len(page.Items)) > *page.Total-i.offset {
		return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("invalid native page bounds"))
	}
	parent, e := nativePath(page.Path)
	if e != nil || parent != i.d.observed(i.target) {
		return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("listing changed parent"))
	}
	if len(page.Items) == 0 && i.offset < *page.Total {
		return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("empty page has no advancing offset"))
	}
	items = make([]core.Metadata, 0, len(page.Items))
	for _, child := range page.Items {
		if !core.ValidName(child.Name) {
			return nil, core.Fail(core.UnrepresentableName, nil)
		}
		expected := i.d.observed(i.target)
		if expected[len(expected)-1] != '/' {
			expected += "/"
		}
		meta, err := child.metadata(expected + child.Name)
		if err != nil {
			return nil, err
		}
		items = append(items, meta)
	}
	i.offset += int64(len(page.Items))
	i.done = i.offset == *page.Total
	if i.done {
		return items, io.EOF
	}
	return items, nil
}
