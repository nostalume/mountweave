package upyun

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"sync"
	"time"

	core "github.com/nostalume/mountweave"
)

const endMarker = "g2gCZAAEbmV4dGQAA2VvZg"

type listItem struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Length   *int64 `json:"length"`
	Modified *int64 `json:"last_modified"`
}

func (d *Driver) Enumerate(ctx context.Context, q core.NativeQuery) (core.Iterator, error) {
	if q.Delimiter() != "" || q.StartAfter() != "" {
		return nil, core.Fail(core.UnsupportedOperation, nil)
	}
	if _, err := d.target(ctx, q.Address()); err != nil {
		return nil, err
	}
	life, cancel := context.WithCancel(context.WithoutCancel(ctx))
	return &iterator{d: d, address: q.Address(), life: life, cancel: cancel}, nil
}

type iterator struct {
	d            *Driver
	address      core.NativeAddress
	life         context.Context
	cancel       context.CancelFunc
	mu           sync.Mutex
	marker       string
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
	target, err := i.d.target(call, i.address)
	if err != nil {
		return nil, err
	}
	headers := http.Header{"Accept": {"application/json"}, "X-Upyun-Folder": {"true"}}
	headers.Set("X-List-Limit", strconv.Itoa(min(n, 4096)))
	if i.marker != "" {
		headers.Set("X-List-Iter", i.marker)
	}
	response, err := i.d.request(call, "GET", target, headers)
	if err != nil {
		return nil, err
	}
	defer func() {
		if cleanup := i.d.closeBody(response.Body); cleanup != nil {
			if err == io.EOF {
				err = nil
			}
			err = errors.Join(err, cleanup)
		}
	}()
	if response.StatusCode != http.StatusOK {
		return nil, i.d.failure(response)
	}
	data, err := i.d.readResponse(response)
	if err != nil {
		return nil, err
	}
	var page struct {
		Files []listItem `json:"files"`
		Iter  string     `json:"iter"`
	}
	if e := json.Unmarshal(data, &page); e != nil {
		return nil, core.Fail(core.InvalidUpstreamResponse, e)
	}
	header := response.Header.Get("X-Upyun-List-Iter")
	if page.Iter == "" {
		page.Iter = header
	}
	if page.Files == nil || page.Iter == "" || (header != "" && header != page.Iter) || page.Iter == i.marker {
		return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("invalid directory continuation"))
	}
	items = make([]core.Metadata, 0, len(page.Files))
	for _, item := range page.Files {
		if !core.ValidName(item.Name) {
			return nil, core.Fail(core.UnrepresentableName, nil)
		}
		meta := core.Metadata{Name: item.Name, Kind: core.File}
		if item.Type == "folder" {
			meta.Kind = core.Directory
		} else if item.Type == "" {
			return nil, core.Fail(core.InvalidUpstreamResponse, nil)
		}
		if meta.Kind == core.File {
			if item.Length != nil && *item.Length < 0 {
				return nil, core.Fail(core.InvalidUpstreamResponse, nil)
			}
			meta.Size = item.Length
		}
		if item.Modified != nil {
			t := time.Unix(*item.Modified, 0).UTC()
			meta.ModifiedAt = &t
		}
		items = append(items, meta)
	}
	i.marker = page.Iter
	i.done = page.Iter == endMarker
	if i.done {
		return items, io.EOF
	}
	return items, nil
}
