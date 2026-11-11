package bunny

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	core "github.com/nostalume/mountweave"
)

type object struct {
	Name      *string `json:"ObjectName"`
	Path      *string `json:"Path"`
	Directory *bool   `json:"IsDirectory"`
	Size      *int64  `json:"Length"`
	Modified  string  `json:"LastChanged"`
}

// One native response is streamed across public pages. Its own context survives
// a successful Next call; cancellation or Close terminates that response.
type iterator struct {
	driver       *Driver
	target       *url.URL
	life         context.Context
	cancel       context.CancelFunc
	mu           sync.Mutex
	response     *http.Response
	reader       *io.LimitedReader
	decoder      *json.Decoder
	borrowed     bool
	done, closed bool
	failed       error
}

func (d *Driver) enumeration(ctx context.Context, target *url.URL) *iterator {
	life, cancel := context.WithCancel(context.WithoutCancel(ctx))
	u := *target
	u.Path = strings.TrimSuffix(u.Path, "/") + "/"
	return &iterator{driver: d, target: &u, life: life, cancel: cancel}
}

func (d *Driver) Enumerate(ctx context.Context, q core.NativeQuery) (core.Iterator, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if q.Delimiter() != "" || q.StartAfter() != "" {
		return nil, core.Fail(core.UnsupportedOperation, nil)
	}
	target, err := d.target(q.Address())
	if err != nil {
		return nil, err
	}
	return d.enumeration(ctx, target), nil
}

func (i *iterator) Close() error {
	i.cancel()
	i.mu.Lock()
	defer i.mu.Unlock()
	i.closed = true
	return i.closeResponse()
}

func (i *iterator) closeResponse() error {
	if i.response != nil {
		if err := i.response.Body.Close(); err != nil {
			return err
		}
		i.response, i.reader, i.decoder = nil, nil, nil
	}
	if i.borrowed {
		<-i.driver.listings
		i.borrowed = false
	}
	return nil
}

func (i *iterator) decodeError(err error) error {
	if err == io.EOF {
		err = io.ErrUnexpectedEOF
	}
	if i.reader.N == 0 {
		return core.Fail(core.ResponseBudgetExceeded, err)
	}
	return core.Fail(core.InvalidUpstreamResponse, err)
}

func (i *iterator) fetch() error {
	select {
	case i.driver.listings <- struct{}{}:
		i.borrowed = true
	case <-i.life.Done():
		return i.life.Err()
	}
	response, err := i.driver.request(i.life, http.MethodGet, i.target, nil)
	if err != nil {
		return err
	}
	i.response = response
	if response.StatusCode != http.StatusOK {
		return i.driver.responseError(response)
	}
	if encoding := response.Header.Get("Content-Encoding"); encoding != "" && encoding != "identity" {
		return core.Fail(core.InvalidUpstreamResponse, nil)
	}
	i.reader = &io.LimitedReader{R: response.Body, N: i.driver.options.MaxResponseBytes + 1}
	i.decoder = json.NewDecoder(i.reader)
	token, err := i.decoder.Token()
	if err != nil {
		return i.decodeError(err)
	}
	if token != json.Delim('[') {
		return core.Fail(core.InvalidUpstreamResponse, errors.New("directory response is not an array"))
	}
	return nil
}

func (i *iterator) Next(ctx context.Context, n int) (items []core.Metadata, err error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	if n < 1 {
		return nil, core.Fail(core.InvalidRequest, nil)
	}
	if i.closed {
		return nil, io.ErrClosedPipe
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
	joined := make(chan struct{})
	stop := context.AfterFunc(ctx, func() { i.cancel(); close(joined) })
	defer func() {
		if !stop() {
			<-joined
		}
		if cause := ctx.Err(); cause != nil {
			err = errors.Join(err, cause)
		}
		if err != nil && err != io.EOF {
			i.failed = errors.Join(err, i.closeResponse())
			err = i.failed
		}
	}()
	if i.decoder == nil {
		if err = i.fetch(); err != nil {
			return nil, err
		}
	}
	for len(items) < n {
		if !i.decoder.More() {
			token, e := i.decoder.Token()
			if e != nil {
				return nil, i.decodeError(e)
			}
			if token != json.Delim(']') {
				return nil, core.Fail(core.InvalidUpstreamResponse, nil)
			}
			var extra json.RawMessage
			e = i.decoder.Decode(&extra)
			if i.reader.N == 0 {
				return nil, core.Fail(core.ResponseBudgetExceeded, e)
			}
			if e != io.EOF {
				return nil, core.Fail(core.InvalidUpstreamResponse, e)
			}
			if err = i.closeResponse(); err != nil {
				return nil, err
			}
			i.done = true
			return items, io.EOF
		}
		var raw object
		if err = i.decoder.Decode(&raw); err != nil {
			return nil, i.decodeError(err)
		}
		if i.reader.N == 0 {
			return nil, core.Fail(core.ResponseBudgetExceeded, nil)
		}
		if raw.Name == nil || raw.Path == nil || raw.Directory == nil || *raw.Path != i.target.Path {
			return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("listing item does not belong to the requested directory"))
		}
		if !core.ValidName(*raw.Name) {
			return nil, core.Fail(core.UnrepresentableName, nil)
		}
		meta := core.Metadata{Name: *raw.Name, Kind: core.File}
		if *raw.Directory {
			meta.Kind = core.Directory
		} else {
			if raw.Size == nil || *raw.Size < 0 {
				return nil, core.Fail(core.InvalidUpstreamResponse, nil)
			}
			meta.Size = raw.Size
		}
		// A datetime without an explicit offset is not silently assigned a timezone.
		if modified, e := time.Parse(time.RFC3339Nano, raw.Modified); e == nil {
			meta.ModifiedAt = &modified
		}
		items = append(items, meta)
	}
	return items, nil
}
