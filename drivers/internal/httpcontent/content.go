// Package httpcontent shares bounded HTTP response decoding and download redirect handling.
package httpcontent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"

	core "github.com/nostalume/mountweave"
)

// IsolatedClient copies a borrowed client's settings and transport. Drivers
// handle redirects and request credentials explicitly; the copy has no cookie jar.
func IsolatedClient(client *http.Client) http.Client {
	var c http.Client
	if client != nil {
		c = *client
	}
	c.Jar = nil
	c.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return c
}

func Header(wanted *core.Range) string {
	if wanted == nil {
		return ""
	}
	if wanted.Suffix {
		return fmt.Sprintf("bytes=-%d", wanted.Start)
	}
	if wanted.End < 0 {
		return fmt.Sprintf("bytes=%d-", wanted.Start)
	}
	return fmt.Sprintf("bytes=%d-%d", wanted.Start, wanted.End)
}

// Open interprets 200, 206 and 416 responses. The caller supplies the native name
// and owns Body even on error; no response is closed, wrapped or retained here.
func Open(response *http.Response, wanted *core.Range) (core.Opened, error) {
	opened := core.Opened{Metadata: core.Metadata{Kind: core.File}, Body: response.Body}
	if response.StatusCode == http.StatusRequestedRangeNotSatisfiable {
		failure := &core.Error{Code: core.RangeNotSatisfiable}
		var total int64
		text := response.Header.Get("Content-Range")
		if _, err := fmt.Sscanf(text, "bytes */%d", &total); err == nil && total >= 0 && text == fmt.Sprintf("bytes */%d", total) {
			failure.Total = &total
		}
		return opened, failure
	}
	if response.StatusCode != http.StatusOK && response.StatusCode != http.StatusPartialContent {
		return opened, core.Fail(core.InvalidUpstreamResponse, nil)
	}
	if encoding := response.Header.Get("Content-Encoding"); encoding != "" && encoding != "identity" {
		return opened, core.Fail(core.InvalidUpstreamResponse, nil)
	}
	if response.ContentLength >= 0 {
		length := response.ContentLength
		opened.Length = &length
		opened.Metadata.Size = &length
	}
	if modified, err := http.ParseTime(response.Header.Get("Last-Modified")); err == nil {
		opened.Metadata.ModifiedAt = &modified
	}
	text := response.Header.Get("Content-Range")
	if response.StatusCode == http.StatusPartialContent {
		var actual core.ContentRange
		_, err := fmt.Sscanf(text, "bytes %d-%d/%d", &actual.Start, &actual.End, &actual.Total)
		if err != nil || wanted == nil || text != fmt.Sprintf("bytes %d-%d/%d", actual.Start, actual.End, actual.Total) {
			return opened, core.Fail(core.InvalidUpstreamResponse, err)
		}
		expected, err := wanted.Resolve(actual.Total)
		if err != nil || expected != actual || opened.Length == nil || *opened.Length != actual.End-actual.Start+1 {
			return opened, core.Fail(core.InvalidUpstreamResponse, err)
		}
		opened.Range = &actual
		total := actual.Total
		opened.Metadata.Size = &total
	} else if text != "" {
		return opened, core.Fail(core.InvalidUpstreamResponse, nil)
	}
	return opened, nil
}

// OpenSized interprets the response and checks the complete size confirmed by
// the download grant. The caller owns Body even on error.
func OpenSized(response *http.Response, wanted *core.Range, size int64) (core.Opened, error) {
	opened, err := Open(response, wanted)
	var total *int64
	if err == nil {
		total = opened.Metadata.Size
	} else if failure, ok := err.(*core.Error); ok && failure.Code == core.RangeNotSatisfiable {
		total = failure.Total
	}
	if total != nil && *total != size {
		return opened, core.Fail(core.InvalidUpstreamResponse, errors.New("download size differs from its receipt"))
	}
	if err == nil && opened.Length == nil {
		opened.Body = &verifiedBody{ReadCloser: opened.Body, remaining: size}
	}
	return opened, err
}

// ReadIdentity reads a bounded response. The caller validates the byte limit,
// checks the status, and owns the body.
func ReadIdentity(response *http.Response, limit int64) ([]byte, error) {
	if encoding := response.Header.Get("Content-Encoding"); encoding != "" && encoding != "identity" {
		return nil, core.Fail(core.InvalidUpstreamResponse, nil)
	}
	data, err := io.ReadAll(io.LimitReader(response.Body, limit+1))
	if int64(len(data)) > limit {
		return nil, core.Fail(core.ResponseBudgetExceeded, PrivateIOError(err))
	}
	if err != nil {
		return nil, UpstreamIOError(err)
	}
	return data, nil
}

func DecodeJSON(response *http.Response, limit int64, out any) error {
	data, err := ReadIdentity(response, limit)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(data, out); err != nil {
		return core.Fail(core.InvalidUpstreamResponse, err)
	}
	return nil
}

// DecimalID decodes a canonical unsigned decimal ID without a float conversion.
// Providers may send the same ID as a JSON string or number.
type DecimalID string

func (v *DecimalID) UnmarshalJSON(data []byte) error {
	id := string(data)
	if len(data) != 0 && data[0] == '"' {
		if err := json.Unmarshal(data, &id); err != nil {
			return err
		}
	}
	if !ValidDecimalID(id) {
		return errors.New("invalid native ID")
	}
	*v = DecimalID(id)
	return nil
}

func ValidDecimalID(id string) bool {
	if id == "" || (len(id) > 1 && id[0] == '0') {
		return false
	}
	for _, c := range id {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}

var ErrContentRedirect = errors.New("invalid content redirect")

func IsHTTPS(u *url.URL) bool {
	return u != nil && u.Scheme == "https" && u.Hostname() != "" && u.User == nil && u.Opaque == "" && u.Fragment == ""
}

// RedirectDownload follows only private GETs and forwards the original range.
// The cleanup owner closes the current response before the next request.
func (c *ResponseCleanup) RedirectDownload(req *http.Request, via []*http.Request) (err error) {
	if len(via) != 0 && via[0].Method != http.MethodGet {
		return http.ErrUseLastResponse
	}
	defer func() {
		body := req.Response.Body
		req.Response.Body = http.NoBody
		err = errors.Join(err, c.CloseBody(body))
	}()
	if len(via) == 0 || len(via) >= 10 || req.Method != http.MethodGet || !IsHTTPS(req.URL) {
		return core.Fail(core.InvalidUpstreamResponse, ErrContentRedirect)
	}
	req.Header = http.Header{"Accept-Encoding": {"identity"}}
	if value := via[0].Header.Get("Range"); value != "" {
		req.Header.Set("Range", value)
	}
	return nil
}

// FollowDownload closes each redirect response before fetching its target.
// Keep account credentials off delegated requests, even for redirects back
// to the API. The caller scopes any download credentials to the native grant
// and owns the terminal response.
func FollowDownload(target string, fetch func(target string, delegated bool) (*http.Response, error), closeBody func(io.Closer) error) (*http.Response, error) {
	for hop := 0; ; hop++ {
		response, err := fetch(target, hop != 0)
		if err != nil {
			return nil, err
		}
		switch response.StatusCode {
		case http.StatusMovedPermanently, http.StatusFound, http.StatusSeeOther, http.StatusTemporaryRedirect, http.StatusPermanentRedirect:
		default:
			return response, nil
		}
		location := response.Header.Get("Location")
		closeErr := closeBody(response.Body)
		base, err := url.Parse(target)
		if err == nil {
			base, err = base.Parse(location)
		}
		if hop >= 9 || location == "" || strings.ContainsRune(location, '#') || err != nil || !IsHTTPS(base) {
			return nil, errors.Join(core.Fail(core.InvalidUpstreamResponse, ErrContentRedirect), closeErr)
		}
		if closeErr != nil {
			return nil, closeErr
		}
		target = base.String()
	}
}

// UpstreamIOError classifies a failed HTTP exchange and removes private details.
func UpstreamIOError(err error) error {
	if err == nil {
		return nil
	}
	code := core.UpstreamFailure
	var timeout net.Error
	if errors.Is(err, context.DeadlineExceeded) || errors.As(err, &timeout) && timeout.Timeout() {
		code = core.UpstreamTimeout
	}
	return core.Fail(code, PrivateIOError(err))
}

// ErrPrivateIO is the fallback when the transport cause contains private details.
var ErrPrivateIO = errors.New("HTTP download I/O failed")

// PrivateIOError preserves actionable I/O causes without exposing a signed URL
// or account material from the transport or a failed response close.
func PrivateIOError(err error) error {
	if err == nil {
		return nil
	}
	for _, cause := range []error{context.Canceled, context.DeadlineExceeded, io.ErrUnexpectedEOF, io.ErrClosedPipe} {
		if errors.Is(err, cause) {
			return cause
		}
	}
	if errors.Is(err, io.EOF) {
		return io.ErrUnexpectedEOF
	}
	return ErrPrivateIO
}

// ControlIOError retains the native cause without the URL wrapper.
// A transport or cleanup EOF must not signal a successful listing end.
func ControlIOError(err error) error {
	var wrapped *url.Error
	if errors.As(err, &wrapped) {
		err = wrapped.Err
	}
	if errors.Is(err, io.EOF) {
		return fmt.Errorf("native I/O failed: %v: %w", err, io.ErrUnexpectedEOF)
	}
	return err
}

// ResponseCleanup retains failed closes of HTTP responses owned by a Driver.
type ResponseCleanup struct {
	mu     sync.Mutex
	failed []failedResponse
}

type failedResponse struct {
	body    io.Closer
	private bool
}

// OpenBody transfers a valid download body to the caller. The Driver retains
// ownership and closes the body when the upstream response is invalid.
func (c *ResponseCleanup) OpenBody(response *http.Response, wanted *core.Range, name string) (core.Opened, error) {
	return c.openBody(response, wanted, name, nil)
}

// OpenSizedBody checks the complete size promised by the native download grant.
// Without an HTTP length it keeps the stream unframed and checks bytes through EOF.
func (c *ResponseCleanup) OpenSizedBody(response *http.Response, wanted *core.Range, name string, size int64) (core.Opened, error) {
	return c.openBody(response, wanted, name, &size)
}

func (c *ResponseCleanup) openBody(response *http.Response, wanted *core.Range, name string, size *int64) (core.Opened, error) {
	var opened core.Opened
	var err error
	switch {
	case wanted == nil && response.StatusCode == http.StatusRequestedRangeNotSatisfiable:
		err = core.Fail(core.InvalidUpstreamResponse, errors.New("range rejected without a range request"))
	case size != nil:
		opened, err = OpenSized(response, wanted, *size)
	default:
		opened, err = Open(response, wanted)
	}
	if err != nil {
		return core.Opened{}, errors.Join(err, c.CloseBody(response.Body))
	}
	opened.Metadata.Name = name
	return opened, nil
}

// verifiedBody checks an unframed download through EOF without exposing extra
// bytes. Serialize concurrent reads without blocking the underlying Close.
type verifiedBody struct {
	io.ReadCloser
	mu        sync.Mutex
	remaining int64
	err       error
}

func (b *verifiedBody) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.err != nil {
		return 0, b.err
	}
	if b.remaining == 0 {
		var extra [1]byte
		n, err := b.ReadCloser.Read(extra[:])
		if n != 0 {
			err = core.Fail(core.InvalidUpstreamResponse, errors.New("download exceeds its receipt size"))
		}
		b.err = err
		return 0, err
	}
	if int64(len(p)) > b.remaining {
		p = p[:b.remaining]
	}
	n, err := b.ReadCloser.Read(p)
	b.remaining -= int64(n)
	if err == io.EOF && b.remaining != 0 {
		err = io.ErrUnexpectedEOF
	}
	b.err = err
	return n, err
}

func (c *ResponseCleanup) CloseBody(body io.Closer) error {
	return c.closeBody(body, true)
}

// CloseControlBody retains native diagnostics from a non-private response.
func (c *ResponseCleanup) CloseControlBody(body io.Closer) error {
	return c.closeBody(body, false)
}

func (c *ResponseCleanup) closeBody(body io.Closer, private bool) error {
	if body == nil {
		return nil
	}
	err := body.Close()
	if err == nil {
		return nil
	}
	c.mu.Lock()
	c.failed = append(c.failed, failedResponse{body: body, private: private})
	c.mu.Unlock()
	if private {
		return PrivateIOError(err)
	}
	return ControlIOError(err)
}

// Retry retries each retained close once. The Driver calls it after active
// operations drain. Remaining failures stay available for a later retry.
func (c *ResponseCleanup) Retry() error {
	c.mu.Lock()
	failed := c.failed
	c.failed = nil
	c.mu.Unlock()
	var err error
	for _, response := range failed {
		err = errors.Join(err, c.closeBody(response.body, response.private))
	}
	return err
}
