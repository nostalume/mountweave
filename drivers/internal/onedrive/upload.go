// Package onedrive sends bytes through a native OneDrive upload session.
package onedrive

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/internal/upload"
)

type Session struct {
	URL            string `json:"uploadUrl"`
	Gone           bool   `json:"-"`
	FinalAttempted bool   `json:"-"`
}

type Options struct {
	Client           *http.Client
	Attempts         int
	MaxResponseBytes int64
	CloseBody        func(io.Closer) error
	// Replayable verifies each new payload through an immutable exact range.
	Replayable bool
	// Resume queries an existing session before selecting the first range.
	Resume bool
	// BeforeComplete runs once before a PUT that may complete native content.
	BeforeComplete func(context.Context) error
}

// ValidateURL admits the native session once, before installing its cleanup.
// Upload and native cleanup use the same admitted URL without rewriting it.
func ValidateURL(target string) error {
	u, err := url.Parse(target)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.Fragment != "" {
		return core.Fail(core.InvalidUpstreamResponse, errors.New("invalid OneDrive upload target"))
	}
	return nil
}

// Upload borrows input and lease until every request returns. Session.URL must
// have passed ValidateURL. A returned response transfers its Body to the caller,
// even with an error; only intermediate progress responses are consumed here.
// FinalAttempted records an unconfirmed final request and is cleared when native
// progress confirms more bytes are needed. It never proves logical publication
// or clears a caller's durable possible-publication record.
func Upload(ctx context.Context, session *Session, input io.Reader, size int64, lease *upload.Lease, options Options) (*http.Response, error) {
	if session == nil || input == nil || size < 1 || lease == nil || options.Client == nil || options.CloseBody == nil || options.Attempts < 0 || options.MaxResponseBytes < 0 {
		return nil, core.Fail(core.InvalidRequest, nil)
	}
	var source core.Replayable
	if options.Replayable {
		var ok bool
		source, ok = input.(core.Replayable)
		if !ok || source.SourceVersion() == "" {
			return nil, core.Fail(core.InvalidRequest, errors.New("range upload requires an immutable replayable source"))
		}
	} else if options.Resume {
		return nil, core.Fail(core.InvalidRequest, errors.New("resuming requires replayable input"))
	}
	if options.Attempts == 0 {
		options.Attempts = 1
	}
	buffer := lease.Buffer()
	missing := missingRange{end: size}
	if options.Resume {
		var response *http.Response
		var err error
		queries := options.Attempts
		missing, response, err = options.query(ctx, session, size, &queries)
		if response != nil || err != nil {
			return response, err
		}
	}
	base, limit := missing.start, missing.end
	filled := 0
	complete := false
	uncertain, barrier := options.Resume, false
	for base < size {
		want := int(min(int64(len(buffer)), limit-base))
		if filled < want {
			var err error
			if source != nil {
				err = upload.ReadRange(ctx, source, base+int64(filled), buffer[filled:want])
			} else if base+int64(want) == size {
				err = upload.ReadExact(ctx, input, buffer[filled:want])
				complete = err == nil
			} else {
				var n int
				n, err = upload.ReadChunk(ctx, input, buffer[filled:want])
				if err == nil && n != want-filled || err == io.EOF {
					err = io.ErrUnexpectedEOF
				}
			}
			if err != nil {
				return nil, err
			}
			filled = want
		}
		advanced := false
		queries := options.Attempts
		var retryErr error
		for attempt := 0; attempt < options.Attempts; attempt++ {
			count := int(min(int64(filled), limit-base))
			end := base + int64(count)
			potential := end == size || complete || source != nil && uncertain
			var response *http.Response
			sent := false
			err := lease.Send(ctx, func() error {
				body := upload.Borrow(buffer[:count])
				defer body.Close()
				req, err := http.NewRequestWithContext(ctx, http.MethodPut, session.URL, body)
				if err != nil {
					return err
				}
				req.ContentLength = int64(count)
				req.Header.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", base, end-1, size))
				if err = ctx.Err(); err != nil {
					return err
				}
				if potential && !barrier && options.BeforeComplete != nil {
					if err = options.BeforeComplete(ctx); err != nil {
						return err
					}
					barrier = true
				}
				if err = ctx.Err(); err != nil {
					return err
				}
				if potential {
					session.FinalAttempted = true
				}
				sent = true
				response, err = options.Client.Do(req)
				if response != nil {
					response.Request = req
				}
				return networkError(err)
			})
			if err == nil && (response.StatusCode == http.StatusOK || response.StatusCode == http.StatusCreated) {
				if !potential || source == nil && !complete {
					return response, core.Fail(core.InvalidUpstreamResponse, errors.New("premature upload publication"))
				}
				return response, nil
			}
			if err != nil && (!sent || ctx.Err() != nil) {
				return nil, errors.Join(retryErr, err)
			}
			var next missingRange
			var present bool
			if err != nil || response.StatusCode == http.StatusRequestTimeout || response.StatusCode == http.StatusTooManyRequests || response.StatusCode >= 500 || response.StatusCode == http.StatusRequestedRangeNotSatisfiable {
				original := err
				if response != nil {
					var cleanup error
					original, cleanup = options.Failure(response)
					if cleanup != nil {
						return nil, errors.Join(retryErr, original, cleanup)
					}
				}
				retryErr = errors.Join(retryErr, original)
				next, response, err = options.query(ctx, session, size, &queries)
				uncertain = true
			} else if response.StatusCode != http.StatusAccepted {
				// This PUT was explicitly refused. A failed progress GET above
				// cannot provide that proof and keeps the unconfirmed flag.
				if response.StatusCode >= 400 && response.StatusCode < 500 {
					session.FinalAttempted = false
				}
				session.Gone = response.StatusCode == http.StatusNotFound || response.StatusCode == http.StatusGone
				return response, retryErr
			} else {
				next, present, err = options.progress(response, size)
				response = nil
				if err == nil && !present {
					next, response, err = options.query(ctx, session, size, &queries)
					uncertain = true
				}
			}
			if response != nil || err != nil {
				return response, errors.Join(retryErr, err)
			}
			if source == nil && (next.start < base || next.start > base+int64(filled)) {
				return nil, errors.Join(retryErr, core.Fail(core.InvalidUpstreamResponse, errors.New("upload progress left the retained range")))
			}
			uncertain = uncertain || potential
			session.FinalAttempted = false
			limit = next.end
			if next.start != base {
				if next.start > base && next.start <= base+int64(filled) {
					used := int(next.start - base)
					copy(buffer, buffer[used:filled])
					filled -= used
				} else {
					filled = 0
					uncertain = true
				}
				base = next.start
				advanced = true
				break
			}
		}
		if !advanced {
			return nil, errors.Join(retryErr, core.Fail(core.UpstreamFailure, errors.New("upload made no confirmed progress")))
		}
	}
	return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("upload omitted its final receipt"))
}

func (o Options) decode(response *http.Response, out any) (err, cleanup error) {
	var body io.Reader = response.Body
	var limited *io.LimitedReader
	if o.MaxResponseBytes > 0 && o.MaxResponseBytes < math.MaxInt64 {
		limited = &io.LimitedReader{R: body, N: o.MaxResponseBytes + 1}
		body = limited
	}
	decoder := json.NewDecoder(body)
	err = decoder.Decode(out)
	if err == nil {
		var extra any
		if tail := decoder.Decode(&extra); tail != io.EOF {
			err = errors.Join(errors.New("unexpected trailing upload response data"), tail)
		}
	}
	if limited != nil && limited.N == 0 {
		err = core.Fail(core.ResponseBudgetExceeded, err)
	} else if err != nil {
		err = core.Fail(core.InvalidUpstreamResponse, err)
	}
	return err, o.CloseBody(response.Body)
}

type missingRange struct{ start, end int64 }

// query consumes upload-session observations, never a file receipt. Incomplete
// range lists are queried only within the configured attempt bound.
func (o Options) query(ctx context.Context, session *Session, size int64, remaining *int) (missingRange, *http.Response, error) {
	for *remaining > 0 {
		*remaining -= 1
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, session.URL, nil)
		if err != nil {
			return missingRange{}, nil, err
		}
		response, err := o.Client.Do(req)
		if err != nil {
			return missingRange{}, nil, networkError(err)
		}
		response.Request = req
		if response.StatusCode != http.StatusOK {
			session.Gone = response.StatusCode == http.StatusNotFound || response.StatusCode == http.StatusGone
			return missingRange{}, response, nil
		}
		next, present, err := o.progress(response, size)
		if err != nil || present {
			return next, nil, err
		}
	}
	return missingRange{}, nil, core.Fail(core.UpstreamFailure, errors.New("upload progress query budget exhausted"))
}

func (o Options) progress(response *http.Response, size int64) (missingRange, bool, error) {
	var state struct {
		Ranges []string `json:"nextExpectedRanges"`
	}
	err, cleanup := o.decode(response, &state)
	if err != nil || cleanup != nil {
		return missingRange{}, false, errors.Join(err, cleanup)
	}
	next := missingRange{start: size, end: size}
	for _, text := range state.Ranges {
		start, end, ok := strings.Cut(text, "-")
		lo, e := strconv.ParseInt(start, 10, 64)
		if !ok || e != nil || lo < 0 || lo >= size {
			return missingRange{}, false, core.Fail(core.InvalidUpstreamResponse, errors.New("invalid missing upload range"))
		}
		limit := size
		if end != "" {
			hi, e := strconv.ParseInt(end, 10, 64)
			if e != nil || hi < lo || hi >= size {
				return missingRange{}, false, core.Fail(core.InvalidUpstreamResponse, errors.New("invalid missing upload range"))
			}
			limit = hi + 1
		}
		if lo < next.start || lo == next.start && limit < next.end {
			next = missingRange{start: lo, end: limit}
		}
	}
	return next, len(state.Ranges) != 0, nil
}

// Failure consumes a failed native response and preserves status, provider
// cause and cleanup independently, including terminal responses from Upload.
func (o Options) Failure(response *http.Response) (failure, cleanup error) {
	var native struct {
		Error struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	err, cleanup := o.decode(response, &native)
	var cause error
	if native.Error.Code != "" || native.Error.Message != "" {
		cause = fmt.Errorf("OneDrive %s: %s", native.Error.Code, native.Error.Message)
	}
	code := core.UpstreamFailure
	switch response.StatusCode {
	case 401, 403:
		code = core.UpstreamAccessDenied
	case 404, 410:
		code = core.NotFound
	case 409:
		code = core.AlreadyExists
	case 412:
		code = core.PreconditionFailed
	case 408, 504:
		code = core.UpstreamTimeout
	case 429:
		code = core.UpstreamThrottled
	}
	return errors.Join(core.Fail(code, fmt.Errorf("OneDrive HTTP status %d", response.StatusCode)), cause, err), cleanup
}

func networkError(err error) error {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	var timeout net.Error
	if errors.As(err, &timeout) && timeout.Timeout() {
		return core.Fail(core.UpstreamTimeout, err)
	}
	return core.Fail(core.UpstreamFailure, err)
}
