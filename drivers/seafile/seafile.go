// Package seafile connects an ordinary Seafile library using its native API.
package seafile

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/internal/httpcontent"
)

// Options selects a library and a library-relative directory. Clients and their
// transports remain borrowed. ContentClient must not inject account credentials;
// its default is independent of Client. MaxResponseBytes bounds metadata only.
type Options struct {
	ServerURL, LibraryID, RootPath, Token string
	Client, ContentClient                 *http.Client
	MaxResponseBytes                      int64
}

type Driver struct {
	server               *url.URL
	library, root, token string
	api, content         http.Client
	limit                int64
	closed               atomic.Bool
	mu                   sync.Mutex
	failed               []io.Closer
}

// Open verifies the library's encryption state and selected directory. A non-nil
// result on error retains failed response cleanup and must be closed.
func Open(ctx context.Context, o Options) (*Driver, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	server, err := url.Parse(o.ServerURL)
	if err != nil || server.Host == "" || (server.Scheme != "https" && server.Scheme != "http") ||
		server.User != nil || server.RawQuery != "" || server.Fragment != "" || server.Opaque != "" ||
		!core.ValidName(o.LibraryID) || strings.ContainsAny(o.Token, "\r\n") ||
		o.MaxResponseBytes < 0 || o.MaxResponseBytes == math.MaxInt64 {
		return nil, core.Fail(core.InvalidRequest, nil)
	}
	for _, encoded := range strings.Split(server.EscapedPath(), "/") {
		decoded, e := url.PathUnescape(encoded)
		if e != nil || strings.Contains(decoded, "/") {
			return nil, core.Fail(core.InvalidPath, e)
		}
	}
	base := strings.TrimSuffix(server.Path, "/")
	if base != "" && core.ValidatePath(base) != nil {
		return nil, core.Fail(core.InvalidPath, nil)
	}
	server.Path, server.RawPath = base, ""
	if o.RootPath == "" {
		o.RootPath = "/"
	}
	if err := core.ValidatePath(o.RootPath); err != nil {
		return nil, err
	}
	if o.MaxResponseBytes == 0 {
		o.MaxResponseBytes = 4 << 20
	}
	d := &Driver{server: server, library: o.LibraryID, root: o.RootPath, token: o.Token,
		api: httpcontent.IsolatedClient(o.Client), content: httpcontent.IsolatedClient(o.ContentClient), limit: o.MaxResponseBytes}
	_, err = d.libraryInfo(ctx)
	if err == nil && d.root != "/" {
		_, err = d.directoryInfo(ctx, d.root)
	}
	return d, err
}

func (d *Driver) native(ctx context.Context, p core.NativeAddress) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if d.closed.Load() {
		return "", core.Fail(core.MountUnavailable, nil)
	}
	if p.Namespace() != core.TreeNamespace {
		return "", core.Fail(core.UnsupportedOperation, nil)
	}
	if err := core.ValidatePath(p.Value()); err != nil {
		return "", err
	}
	if d.root == "/" {
		return p.Value(), nil
	}
	if p.Value() == "/" {
		return d.root, nil
	}
	return d.root + p.Value(), nil
}

func (d *Driver) request(ctx context.Context, suffix string, query url.Values) (*http.Response, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if d.closed.Load() {
		return nil, core.Fail(core.MountUnavailable, nil)
	}
	u := *d.server
	u.Path += suffix
	u.RawQuery = query.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, core.Fail(core.InvalidRequest, err)
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Accept-Encoding", "identity")
	if d.token != "" {
		req.Header.Set("Authorization", "Token "+d.token)
	}
	response, err := d.api.Do(req)
	if err != nil {
		err = networkError(err)
		if response != nil {
			err = errors.Join(err, d.closeOwned(response.Body))
		}
		return nil, err
	}
	return response, nil
}

func unexpectedEOF(err error) error {
	if errors.Is(err, io.EOF) {
		return fmt.Errorf("%v: %w", err, io.ErrUnexpectedEOF)
	}
	return err
}

func networkError(err error) error {
	var failure *url.Error
	if errors.As(err, &failure) {
		err = failure.Err
	}
	err = unexpectedEOF(err)
	return core.Fail(core.UpstreamFailure, err)
}

func (d *Driver) responseError(response *http.Response) error {
	status := response.StatusCode
	code := core.UpstreamFailure
	switch status {
	case 401, 403:
		code = core.UpstreamAccessDenied
	case 404:
		code = core.NotFound
	case 429:
		code = core.UpstreamThrottled
	case 440:
		code = core.UnsupportedOperation
	}
	var problem struct {
		Message string `json:"error_msg"`
	}
	err := d.decodeJSON(response, &problem)
	reason := fmt.Errorf("Seafile returned HTTP %d", status)
	if err == nil && problem.Message != "" {
		reason = fmt.Errorf("Seafile returned HTTP %d: %s", status, problem.Message)
	}
	return core.Fail(code, errors.Join(reason, err))
}

func jsonError(err error) error {
	err = unexpectedEOF(err)
	return core.Fail(core.InvalidUpstreamResponse, err)
}

// decode owns a short metadata response, including failed cleanup.
func (d *Driver) decode(response *http.Response, output any) (err error) {
	defer func() { err = errors.Join(err, d.closeOwned(response.Body)) }()
	if response.StatusCode != http.StatusOK {
		return d.responseError(response)
	}
	return d.decodeJSON(response, output)
}

func (d *Driver) decodeJSON(response *http.Response, output any) (err error) {
	if encoding := response.Header.Get("Content-Encoding"); encoding != "" && encoding != "identity" {
		return core.Fail(core.InvalidUpstreamResponse, nil)
	}
	reader := &io.LimitedReader{R: response.Body, N: d.limit + 1}
	decoder := json.NewDecoder(reader)
	if err = decoder.Decode(output); err != nil {
		err = jsonError(err)
	}
	if err == nil {
		var extra json.RawMessage
		if tail := decoder.Decode(&extra); tail != io.EOF {
			err = jsonError(tail)
		}
	}
	if reader.N == 0 {
		err = core.Fail(core.ResponseBudgetExceeded, err)
	}
	return err
}

func (d *Driver) closeOwned(body io.Closer) error {
	if body == nil {
		return nil
	}
	err := body.Close()
	if err != nil {
		d.mu.Lock()
		d.failed = append(d.failed, body)
		d.mu.Unlock()
	}
	return unexpectedEOF(err)
}

// Close retries owned response cleanup; borrowed clients remain open.
func (d *Driver) Close() error {
	d.closed.Store(true)
	d.mu.Lock()
	failed := d.failed
	d.failed = nil
	d.mu.Unlock()
	var err error
	for _, body := range failed {
		err = errors.Join(err, d.closeOwned(body))
	}
	return err
}

func (d *Driver) apiPath(version, tail string) string {
	return "/" + version + "/repos/" + d.library + "/" + tail
}

func (d *Driver) libraryInfo(ctx context.Context) (core.Metadata, error) {
	response, err := d.request(ctx, d.apiPath("api2", ""), nil)
	if err != nil {
		return core.Metadata{}, err
	}
	var library struct {
		ID        string          `json:"id"`
		Encrypted *bool           `json:"encrypted"`
		Modified  json.RawMessage `json:"mtime"`
	}
	if err = d.decode(response, &library); err != nil {
		return core.Metadata{}, err
	}
	if library.ID != d.library || library.Encrypted == nil {
		return core.Metadata{}, core.Fail(core.InvalidUpstreamResponse, nil)
	}
	if *library.Encrypted {
		return core.Metadata{}, core.Fail(core.UnsupportedOperation, errors.New("encrypted libraries require a separate access contract"))
	}
	modified, err := modifiedTime(library.Modified)
	return core.Metadata{Kind: core.Directory, ModifiedAt: modified}, err
}

func modifiedTime(raw json.RawMessage) (*time.Time, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return nil, nil
	}
	var modified time.Time
	if raw[0] == '"' {
		var text string
		if err := json.Unmarshal(raw, &text); err != nil {
			return nil, jsonError(err)
		}
		t, err := time.Parse(time.RFC3339Nano, text)
		if err != nil {
			return nil, jsonError(err)
		}
		modified = t
	} else {
		value, err := strconv.ParseInt(string(raw), 10, 64)
		if err != nil {
			return nil, jsonError(err)
		}
		modified = time.Unix(value, 0).UTC()
	}
	return &modified, nil
}

func (d *Driver) directoryInfo(ctx context.Context, native string) (core.Metadata, error) {
	response, err := d.request(ctx, d.apiPath("api/v2.1", "dir/detail/"), url.Values{"path": {native}})
	if err != nil {
		return core.Metadata{}, err
	}
	var detail struct {
		Repo     string          `json:"repo_id"`
		Name     string          `json:"name"`
		Path     string          `json:"path"`
		Modified json.RawMessage `json:"mtime"`
	}
	if err = d.decode(response, &detail); err != nil {
		return core.Metadata{}, err
	}
	if detail.Repo != d.library || detail.Name != path.Base(native) || strings.TrimSuffix(detail.Path, "/") != native {
		return core.Metadata{}, core.Fail(core.InvalidUpstreamResponse, errors.New("directory identity differs from the requested path"))
	}
	modified, err := modifiedTime(detail.Modified)
	return core.Metadata{Name: detail.Name, Kind: core.Directory, ModifiedAt: modified}, err
}

func (d *Driver) Stat(ctx context.Context, p core.NativeAddress, selections ...core.ReadSelection) (core.Metadata, error) {
	if len(selections) > 1 {
		return core.Metadata{}, core.Fail(core.InvalidRequest, nil)
	}
	var selection core.ReadSelection
	if len(selections) == 1 {
		selection = selections[0]
	}
	if selection.Version != "" {
		return core.Metadata{}, core.Fail(core.UnsupportedOperation, nil)
	}
	if err := selection.When.ForDomain(""); err != nil {
		return core.Metadata{}, err
	}
	native, err := d.native(ctx, p)
	if err != nil {
		return core.Metadata{}, err
	}
	var meta core.Metadata
	if native == "/" {
		meta, err = d.libraryInfo(ctx)
	} else {
		response, failure := d.request(ctx, d.apiPath("api2", "file/detail/"), url.Values{"p": {native}})
		if failure != nil {
			return meta, failure
		}
		if response.StatusCode == http.StatusNotFound {
			if err = d.closeOwned(response.Body); err != nil {
				return meta, err
			}
			meta, err = d.directoryInfo(ctx, native)
		} else {
			var item entry
			if err = d.decode(response, &item); err == nil {
				meta, err = item.metadata()
				if err == nil && (meta.Name != path.Base(native) || meta.Kind != core.File) {
					err = core.Fail(core.InvalidUpstreamResponse, nil)
				}
			}
		}
	}
	if p.Value() == "/" {
		meta.Name = ""
	}
	if err == nil {
		err = selection.Check(meta.Evidence)
	}
	return meta, err
}

func (d *Driver) Open(ctx context.Context, p core.NativeAddress, o core.OpenOptions) (core.Opened, error) {
	if err := o.Validate(); err != nil {
		return core.Opened{}, err
	}
	if o.Mode != core.StreamRead || o.NoFollow || o.Selection.Version != "" {
		return core.Opened{}, core.Fail(core.UnsupportedOperation, nil)
	}
	selectionErr := o.Selection.Check(core.Evidence{})
	if selectionErr != nil && core.CodeOf(selectionErr) != core.PreconditionFailed {
		return core.Opened{}, selectionErr
	}
	native, err := d.native(ctx, p)
	if err != nil {
		return core.Opened{}, err
	}
	if p.Value() == "/" {
		return core.Opened{}, core.Fail(core.NotFile, nil)
	}
	response, err := d.request(ctx, d.apiPath("api2", "file/"), url.Values{"p": {native}})
	if err != nil {
		return core.Opened{}, err
	}
	var target string
	if err = d.decode(response, &target); err != nil {
		return core.Opened{}, err
	}
	u, err := url.Parse(target)
	if err != nil || u.Host == "" || u.User != nil || u.Fragment != "" || u.Opaque != "" ||
		(u.Scheme != "https" && !(u.Scheme == "http" && d.server.Scheme == "http")) {
		return core.Opened{}, core.Fail(core.InvalidUpstreamResponse, errors.New("invalid content capability"))
	}
	if selectionErr != nil {
		return core.Opened{}, selectionErr
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return core.Opened{}, core.Fail(core.InvalidUpstreamResponse, nil)
	}
	req.Header.Set("Accept-Encoding", "identity")
	if o.Range != nil {
		req.Header.Set("Range", httpcontent.Header(o.Range))
	}
	bodyResponse, err := d.content.Do(req)
	if err != nil {
		err = networkError(err)
		if bodyResponse != nil {
			err = errors.Join(err, d.closeOwned(bodyResponse.Body))
		}
		return core.Opened{}, err
	}
	opened := core.Opened{Body: bodyResponse.Body}
	if bodyResponse.StatusCode != 200 && bodyResponse.StatusCode != 206 && bodyResponse.StatusCode != 416 {
		return opened, d.responseError(bodyResponse)
	}
	opened, err = httpcontent.Open(bodyResponse, o.Range)
	opened.Metadata.Name = path.Base(p.Value())
	// The native fileserver's unquoted file-ID ETag is not an HTTP validator, and
	// its download Last-Modified can mean response time rather than file mtime.
	opened.Metadata.ModifiedAt = nil
	return opened, err
}

var _ core.Driver = (*Driver)(nil)
