// Package dropbox connects native Dropbox files to Mountweave.
package dropbox

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"sync"
	"sync/atomic"
	"unicode/utf8"

	core "github.com/nostalume/mountweave"
)

// Root selects the API's application default, current account root or a fixed
// namespace. Its zero value preserves the application's authorized default.
type Root struct {
	mode uint8
	id   string
}

func AccountRoot() Root { return Root{mode: 1} }

func NamespaceRoot(id string) Root { return Root{mode: 2, id: id} }

// ExportOptions explicitly permits native export after unsupported_file.
// An empty Format accepts the native default; a nonempty format is validated
// by the native export operation.
type ExportOptions struct {
	Format string
}

// Options borrows one prepared client for the two fixed Dropbox API hosts.
// The host owns credentials, a fixed actor and transports. It must not inject
// or rewrite Path-Root. RootPath is relative to the selected namespace.
type Options struct {
	Client           *http.Client
	Root             Root
	RootPath         string
	Export           *ExportOptions
	MaxResponseBytes int64
	AllowCreate      bool
}

type Driver struct {
	client      http.Client
	root        item
	pathRoot    string
	export      *ExportOptions
	limit       int64
	allowCreate bool
	closed      atomic.Bool
	mu          sync.Mutex
	failed      []responseCleanup
}

type responseCleanup struct {
	body    io.Closer
	private bool
}

// Open confirms the selected directory. A failed non-nil result retains cleanup
// and must be closed; the borrowed client and its credentials remain host-owned.
func Open(ctx context.Context, o Options) (*Driver, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if o.Client == nil || o.MaxResponseBytes < 0 || o.MaxResponseBytes == math.MaxInt64 ||
		o.Root.mode > 2 || (o.Root.mode == 2 && !namespaceID(o.Root.id)) ||
		(o.Export != nil && !utf8.ValidString(o.Export.Format)) {
		return nil, core.Fail(core.InvalidRequest, nil)
	}
	if o.RootPath == "" {
		o.RootPath = "/"
	}
	if err := core.ValidatePath(o.RootPath); err != nil {
		return nil, err
	}
	if o.MaxResponseBytes == 0 {
		o.MaxResponseBytes = 4 << 20
	}
	d := &Driver{client: *o.Client, root: item{meta: core.Metadata{Kind: core.Directory}},
		limit: o.MaxResponseBytes, allowCreate: o.AllowCreate}
	if o.Export != nil {
		chosen := *o.Export
		d.export = &chosen
	}
	d.client.Jar = nil
	d.client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	var err error
	if o.Root.mode == 1 {
		var account struct {
			Root struct {
				ID string `json:"root_namespace_id"`
			} `json:"root_info"`
		}
		err = d.rpc(ctx, "/users/get_current_account", nil, &account)
		if err == nil {
			if !namespaceID(account.Root.ID) {
				err = core.Fail(core.InvalidUpstreamResponse, errors.New("account root namespace is missing"))
			} else {
				d.pathRoot = rootHeader("root", account.Root.ID)
			}
		}
	} else if o.Root.mode == 2 {
		d.pathRoot = rootHeader("namespace_id", o.Root.id)
	}
	if err == nil {
		if o.RootPath == "/" {
			_, _, _, err = d.page(ctx, d.root, "", 1)
		} else {
			d.root, err = d.resolve(ctx, o.RootPath)
			if err == nil && d.root.meta.Kind != core.Directory {
				err = core.Fail(core.NotDirectory, nil)
			}
		}
	}
	if err == nil {
		err = ctx.Err()
	}
	if err != nil {
		d.closed.Store(true)
	}
	return d, err
}

func namespaceID(id string) bool {
	if id == "" {
		return false
	}
	for _, c := range id {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-' || c == '_' || c == ':') {
			return false
		}
	}
	return true
}

func rootHeader(tag, id string) string {
	data, _ := json.Marshal(map[string]string{".tag": tag, tag: id})
	return string(data)
}

func (d *Driver) do(req *http.Request) (*http.Response, bool, error) {
	if err := req.Context().Err(); err != nil {
		return nil, false, err
	}
	if d.closed.Load() {
		return nil, false, core.Fail(core.MountUnavailable, nil)
	}
	if d.pathRoot != "" {
		req.Header.Set("Dropbox-API-Path-Root", d.pathRoot)
	}
	req.Header.Set("Accept-Encoding", "identity")
	private := req.Header.Get("Dropbox-API-Arg") != ""
	response, err := d.client.Do(req)
	if err != nil {
		var failure *url.Error
		if errors.As(err, &failure) {
			err = failure.Err
		}
		if response != nil {
			err = errors.Join(err, d.closeBody(response.Body, private))
		}
		err = failedEOF(err)
		if private {
			err = sessionIOError(err)
		}
		return nil, true, errors.Join(core.Fail(core.UpstreamFailure, err), req.Context().Err())
	}
	return response, true, nil
}

func (d *Driver) rpc(ctx context.Context, route string, argument, output any) (err error) {
	payload, err := json.Marshal(argument)
	if err != nil {
		return core.Fail(core.InvalidRequest, err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://api.dropboxapi.com/2"+route, bytes.NewReader(payload))
	if err != nil {
		return core.Fail(core.InvalidRequest, err)
	}
	req.Header.Set("Content-Type", "application/json")
	response, _, err := d.do(req)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, d.closeBody(response.Body, false)) }()
	if response.StatusCode != http.StatusOK {
		_, err = d.failure(response, readFailure)
		return err
	}
	return d.readJSON(response, output, false)
}

func (d *Driver) readBody(response *http.Response, private bool) ([]byte, error) {
	if coding := response.Header.Get("Content-Encoding"); coding != "" && coding != "identity" {
		return nil, core.Fail(core.InvalidUpstreamResponse, nil)
	}
	data, err := io.ReadAll(io.LimitReader(response.Body, d.limit+1))
	err = failedEOF(err)
	if private && err != nil {
		err = sessionIOError(err)
	}
	if int64(len(data)) > d.limit {
		return data[:d.limit], errors.Join(core.Fail(core.ResponseBudgetExceeded, nil), err)
	}
	if err != nil {
		return data, core.Fail(core.InvalidUpstreamResponse, err)
	}
	return data, nil
}

func (d *Driver) readJSON(response *http.Response, output any, private bool) error {
	data, err := d.readBody(response, private)
	if err != nil {
		return err
	}
	return d.decode(data, output)
}
func (d *Driver) decode(data []byte, output any) error {
	if int64(len(data)) > d.limit {
		return core.Fail(core.ResponseBudgetExceeded, nil)
	}
	if err := json.Unmarshal(data, output); err != nil {
		return core.Fail(core.InvalidUpstreamResponse, err)
	}
	return nil
}

type failureStyle uint8

const (
	readFailure failureStyle = iota
	uploadFailure
	sessionFailure
)

type writeFailure struct {
	Tag      string `json:".tag"`
	Conflict struct {
		Tag string `json:".tag"`
	} `json:"conflict"`
}

// failure consumes a bounded error body but does not close it. Only a complete
// native error union can select a different representation in the same Open.
func (d *Driver) failure(response *http.Response, style failureStyle) (string, error) {
	var problem struct {
		Error json.RawMessage `json:"error"`
	}
	data, readErr := d.readBody(response, style == sessionFailure)
	if readErr == nil {
		readErr = d.decode(data, &problem)
	}
	var native struct {
		Tag  string `json:".tag"`
		Path struct {
			writeFailure
			Reason json.RawMessage `json:"reason"`
		} `json:"path"`
	}
	if readErr == nil {
		readErr = d.decode(problem.Error, &native)
	}
	code := core.UpstreamFailure
	switch response.StatusCode {
	case 401, 403:
		code = core.UpstreamAccessDenied
	case 429, 503:
		code = core.UpstreamThrottled
	case 504:
		code = core.UpstreamTimeout
	}
	diagnostic := string(data)
	if style != readFailure {
		// The error body may include a resumable upload session identifier.
		diagnostic = "upload failed"
	}
	if readErr == nil && response.StatusCode == http.StatusConflict {
		switch native.Tag {
		case "unsupported_file":
			code = core.UnsupportedOperation
		case "incorrect_offset", "lookup_failed":
			if style == sessionFailure {
				diagnostic = "upload session " + native.Tag
			}
		case "path":
			if style != readFailure {
				write := native.Path.writeFailure
				if style == uploadFailure {
					write = writeFailure{}
					readErr = d.decode(native.Path.Reason, &write)
				}
				if readErr == nil && write.Tag == "conflict" {
					switch write.Conflict.Tag {
					case "file", "folder":
						code, diagnostic = core.AlreadyExists, "upload name conflict"
					case "file_ancestor":
						code, diagnostic = core.NotDirectory, "upload ancestor is a file"
					}
				}
				break
			}
			switch native.Path.Tag {
			case "not_found":
				code = core.NotFound
			case "not_file":
				code = core.NotFile
			case "not_folder":
				code = core.NotDirectory
			}
		}
	}
	if response.StatusCode == http.StatusUnprocessableEntity && native.Tag == "no_permission" {
		code = core.UpstreamAccessDenied
	}
	reason := fmt.Errorf("Dropbox returned HTTP %d (request %q; retry-after %q): %s", response.StatusCode, response.Header.Get("X-Dropbox-Request-Id"), response.Header.Get("Retry-After"), diagnostic)
	err := core.Fail(code, errors.Join(reason, readErr))
	if readErr != nil {
		return "", err
	}
	return native.Tag, err
}

// Failed I/O EOF is not successful enumeration. Preserve the diagnostic and
// independent causes while removing only failed EOF leaves from its error tree.
func failedEOF(err error) error {
	if !errors.Is(err, io.EOF) {
		return err
	}
	causes := []error{errors.New(err.Error())}
	var collect func(error)
	collect = func(cause error) {
		if !errors.Is(cause, io.EOF) {
			causes = append(causes, cause)
			return
		}
		switch failure := cause.(type) {
		case interface{ Unwrap() []error }:
			for _, child := range failure.Unwrap() {
				collect(child)
			}
		case interface{ Unwrap() error }:
			collect(failure.Unwrap())
		default:
			causes = append(causes, io.ErrUnexpectedEOF)
		}
	}
	collect(err)
	return core.Fail(core.CodeOf(err), errors.Join(causes...))
}

// Session I/O errors can include private request headers in transport messages.
func sessionIOError(err error) error {
	causes := []error{errors.New("Dropbox upload session I/O failed")}
	for _, cause := range []error{context.Canceled, context.DeadlineExceeded, io.ErrUnexpectedEOF, io.ErrClosedPipe} {
		if errors.Is(err, cause) {
			causes = append(causes, cause)
		}
	}
	return core.Fail(core.CodeOf(err), errors.Join(causes...))
}

func (d *Driver) closeBody(body io.Closer, private bool) error {
	if body == nil {
		return nil
	}
	err := failedEOF(body.Close())
	if err != nil {
		d.mu.Lock()
		d.failed = append(d.failed, responseCleanup{body, private})
		d.mu.Unlock()
		if private {
			return sessionIOError(err)
		}
	}
	return err
}

// Close retries retained short-response cleanup after Core has joined active
// uses. It does not close borrowed clients, transports or credentials.
func (d *Driver) Close() error {
	d.closed.Store(true)
	d.mu.Lock()
	failed := d.failed
	d.failed = nil
	d.mu.Unlock()
	var err error
	for _, response := range failed {
		err = errors.Join(err, d.closeBody(response.body, response.private))
	}
	return err
}
