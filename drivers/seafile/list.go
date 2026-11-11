package seafile

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"

	core "github.com/nostalume/mountweave"
)

type entry struct {
	Name     string          `json:"name"`
	Type     string          `json:"type"`
	Size     *int64          `json:"size"`
	Modified json.RawMessage `json:"mtime"`
	Parent   *string         `json:"parent_dir"`
}

func (e entry) metadata() (core.Metadata, error) {
	if !core.ValidName(e.Name) {
		return core.Metadata{}, core.Fail(core.UnrepresentableName, nil)
	}
	meta := core.Metadata{Name: e.Name, Kind: core.Directory}
	switch e.Type {
	case "file":
		if e.Size == nil || *e.Size < 0 {
			return meta, core.Fail(core.InvalidUpstreamResponse, nil)
		}
		meta.Kind, meta.Size = core.File, e.Size
	case "dir":
	default:
		return meta, core.Fail(core.UnsupportedOperation, errors.New("unsupported native entry type"))
	}
	modified, err := modifiedTime(e.Modified)
	meta.ModifiedAt = modified
	return meta, err
}

type iterator struct {
	d            *Driver
	native       string
	life         context.Context
	cancel       context.CancelFunc
	mu           sync.Mutex
	response     *http.Response
	reader       *io.LimitedReader
	decoder      *json.Decoder
	done, closed bool
	failed       error
}

func (d *Driver) Enumerate(ctx context.Context, q core.NativeQuery) (core.Iterator, error) {
	if q.Delimiter() != "" || q.StartAfter() != "" {
		return nil, core.Fail(core.UnsupportedOperation, nil)
	}
	native, err := d.native(ctx, q.Address())
	if err != nil {
		return nil, err
	}
	life, cancel := context.WithCancel(context.WithoutCancel(ctx))
	return &iterator{d: d, native: native, life: life, cancel: cancel}, nil
}

func (i *iterator) Close() error {
	i.cancel()
	i.mu.Lock()
	defer i.mu.Unlock()
	i.closed = true
	return i.closeResponse()
}

func (i *iterator) closeResponse() error {
	if i.response == nil {
		return nil
	}
	if err := i.response.Body.Close(); err != nil {
		return unexpectedEOF(err)
	}
	i.response, i.reader, i.decoder = nil, nil, nil
	return nil
}

func (i *iterator) decodeError(err error) error {
	if err == io.EOF {
		err = io.ErrUnexpectedEOF
	}
	if i.reader.N == 0 {
		return core.Fail(core.ResponseBudgetExceeded, err)
	}
	return jsonError(err)
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
			if err == io.EOF {
				err = nil
			}
			err = errors.Join(err, cause)
		}
		if err != nil && err != io.EOF {
			i.failed = errors.Join(err, i.closeResponse())
			err = i.failed
		}
	}()
	if i.decoder == nil {
		response, e := i.d.request(i.life, i.d.apiPath("api2", "dir/"), url.Values{"p": {i.native}})
		if e != nil {
			return nil, e
		}
		i.response = response
		if response.StatusCode != http.StatusOK {
			return nil, i.d.responseError(response)
		}
		if encoding := response.Header.Get("Content-Encoding"); encoding != "" && encoding != "identity" {
			return nil, core.Fail(core.InvalidUpstreamResponse, nil)
		}
		i.reader = &io.LimitedReader{R: response.Body, N: i.d.limit + 1}
		i.decoder = json.NewDecoder(i.reader)
		token, e := i.decoder.Token()
		if e != nil {
			return nil, i.decodeError(e)
		}
		if token != json.Delim('[') {
			return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("directory response is not an array"))
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
				return nil, core.Fail(core.ResponseBudgetExceeded, nil)
			}
			if e != io.EOF {
				return nil, jsonError(e)
			}
			if err = i.closeResponse(); err != nil {
				return nil, err
			}
			i.done = true
			return items, io.EOF
		}
		var e entry
		if err = i.decoder.Decode(&e); err != nil {
			return nil, i.decodeError(err)
		}
		if i.reader.N == 0 {
			return nil, core.Fail(core.ResponseBudgetExceeded, nil)
		}
		if e.Parent != nil && strings.TrimSuffix(*e.Parent, "/") != strings.TrimSuffix(i.native, "/") {
			return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("entry does not belong to the requested directory"))
		}
		meta, failure := e.metadata()
		if failure != nil {
			return nil, failure
		}
		items = append(items, meta)
	}
	return items, nil
}
