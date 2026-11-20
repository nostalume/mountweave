package browser

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/chromedp/cdproto/fetch"
	browserio "github.com/chromedp/cdproto/io"
	"github.com/chromedp/cdproto/network"
	"github.com/chromedp/chromedp"
	core "github.com/nostalume/mountweave"
)

// ResponseHeaders projects CDP response headers into HTTP headers.
func ResponseHeaders(entries []*fetch.HeaderEntry) http.Header {
	headers := make(http.Header, len(entries))
	for _, entry := range entries {
		headers.Add(entry.Name, entry.Value)
	}
	return headers
}

// RangeHeaders copies request headers and replaces any Range field.
func RangeHeaders(headers network.Headers, value string) []*fetch.HeaderEntry {
	entries := make([]*fetch.HeaderEntry, 0, len(headers)+1)
	for name, current := range headers {
		if !strings.EqualFold(name, "Range") {
			entries = append(entries, &fetch.HeaderEntry{Name: name, Value: fmt.Sprint(current)})
		}
	}
	return append(entries, &fetch.HeaderEntry{Name: "Range", Value: value})
}

// NewBody reads a browser-owned stream and delegates closing it to close.
// A negative remaining size represents an unknown length.
func NewBody(ctx context.Context, handle browserio.StreamHandle, close func() error, failure error, remaining int64, checkEOF bool) io.ReadCloser {
	return &browserDownloadBody{ctx: ctx, handle: handle, close: close, failure: failure, remaining: remaining, checkEOF: checkEOF}
}

type browserDownloadBody struct {
	ctx       context.Context
	handle    browserio.StreamHandle
	close     func() error
	failure   error
	remaining int64
	checkEOF  bool
	eof       bool
}

func (r *browserDownloadBody) Read(p []byte) (int, error) {
	if r.eof {
		return 0, io.EOF
	}
	if len(p) == 0 {
		return 0, nil
	}
	chunk, err := r.next(int64(min(len(p), 64<<10)))
	if err != nil {
		return 0, err
	}
	size := int64(len(chunk.Data))
	if len(chunk.Data) > len(p) || (r.remaining >= 0 && size > r.remaining) {
		return 0, core.Fail(core.InvalidUpstreamResponse, nil)
	}
	if r.remaining >= 0 {
		r.remaining -= size
	}
	n := copy(p, chunk.Data)
	r.eof = chunk.EOF
	if r.checkEOF && r.remaining == 0 && !r.eof {
		end, err := r.next(1)
		if err != nil {
			return n, err
		}
		if !end.EOF || len(end.Data) != 0 {
			return n, core.Fail(core.InvalidUpstreamResponse, nil)
		}
		r.eof = true
	}
	if r.eof {
		if r.remaining > 0 {
			return n, io.ErrUnexpectedEOF
		}
		return n, io.EOF
	}
	return n, nil
}

func (r *browserDownloadBody) next(size int64) (browserio.ReadResult, error) {
	chunk, err := chromedp.Call(r.ctx, browserio.Read, browserio.ReadParams{Handle: r.handle, Size: size})
	if err != nil {
		if err := r.ctx.Err(); err != nil {
			return chunk, err
		}
		return chunk, core.Fail(core.UpstreamFailure, r.failure)
	}
	return chunk, nil
}

func (r *browserDownloadBody) Close() error { return r.close() }

// TransferFailure reports caller cancellation before browser setup failure.
func TransferFailure(caller, setup context.Context, message string) error {
	if err := caller.Err(); err != nil {
		return err
	}
	if errors.Is(setup.Err(), context.DeadlineExceeded) {
		return core.Fail(core.UpstreamTimeout, errors.New(message))
	}
	return core.Fail(core.UpstreamFailure, errors.New(message))
}
