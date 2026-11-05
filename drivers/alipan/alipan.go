// Package alipan implements rooted file access using Aliyun Drive Open's public API.
package alipan

import (
	"bytes"
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
	"sync/atomic"
	"time"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/alipan/credential"
	"github.com/nostalume/mountweave/drivers/internal/upload"
)

const Endpoint = "https://openapi.alipan.com"

// Options describes an already authorized drive root, not a saved configuration.
// Credentials transfer to the driver only when New succeeds. Client is borrowed.
type Options struct {
	Client                    *http.Client
	Endpoint, DriveID, RootID string
	Credentials               *credential.Source
	// Zero leaves these optional host policies unset.
	MaxResolvePages  int
	MaxResponseBytes int64
	// Both values are required to enable known-length new-file uploads.
	PartSize, MaxBufferedBytes int64
	PartConcurrency            int
	// Zero selects a one-second interval between running deletion-task queries.
	DeletionPollInterval time.Duration
}
type Driver struct {
	opts         Options
	client       *http.Client
	closed       atomic.Bool
	mu           sync.Mutex
	failedBodies []io.Closer
	closeErr     error
	buffer       *upload.Budget
}

func New(opts Options) (*Driver, error) {
	if opts.Endpoint == "" {
		opts.Endpoint = Endpoint
	}
	if opts.Client == nil || opts.Credentials == nil || opts.DriveID == "" || opts.RootID == "" || opts.MaxResolvePages < 0 || opts.MaxResponseBytes < 0 {
		return nil, errors.New("a client, credential owner, prepared drive root, and nonnegative policies are required")
	}
	if err := validEndpoint(opts.Endpoint); err != nil {
		return nil, err
	}
	if opts.PartSize < 0 || opts.MaxBufferedBytes < 0 || opts.PartConcurrency < 0 || opts.DeletionPollInterval < 0 || (opts.PartSize == 0) != (opts.MaxBufferedBytes == 0) || opts.PartSize > 5<<30 {
		return nil, core.Fail(core.InvalidRequest, nil)
	}
	d := &Driver{opts: opts, client: isolatedClient(opts.Client)}
	if opts.PartSize != 0 {
		var err error
		d.buffer, err = upload.New(opts.PartSize, opts.MaxBufferedBytes, opts.PartConcurrency)
		if err != nil {
			return nil, core.Fail(core.InvalidRequest, err)
		}
	}
	return d, nil
}
func isolatedClient(client *http.Client) *http.Client {
	copy := *client
	copy.Jar = nil
	copy.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &copy
}
func validEndpoint(endpoint string) error {
	u, err := url.Parse(endpoint)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
		return errors.New("an HTTPS API origin without credentials, path, or query is required")
	}
	return nil
}
func (d *Driver) Close() error {
	d.closed.Store(true)
	err := d.opts.Credentials.Close()
	d.mu.Lock()
	defer d.mu.Unlock()
	return errors.Join(err, d.closeErr)
}
func (d *Driver) closeBody(body io.Closer) error {
	err := body.Close()
	if err != nil {
		d.mu.Lock()
		d.failedBodies = append(d.failedBodies, body)
		d.closeErr = errors.Join(d.closeErr, err)
		d.mu.Unlock()
	}
	return err
}
func (d *Driver) admit(ctx context.Context, p string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := core.ValidatePath(p); err != nil {
		return err
	}
	if d.closed.Load() {
		return core.Fail(core.MountUnavailable, nil)
	}
	return nil
}

type nativeFile struct {
	ID       string     `json:"file_id"`
	Drive    string     `json:"drive_id"`
	Parent   string     `json:"parent_file_id"`
	Name     string     `json:"name"`
	Type     string     `json:"type"`
	Size     *int64     `json:"size"`
	Modified *time.Time `json:"updated_at"`
}

func (f nativeFile) metadata() (core.Metadata, error) {
	if !core.ValidName(f.Name) {
		return core.Metadata{}, core.Fail(core.UnrepresentableName, nil)
	}
	meta := core.Metadata{Name: f.Name, ModifiedAt: f.Modified}
	switch f.Type {
	case "folder":
		meta.Kind = core.Directory
	case "file":
		meta.Kind = core.File
		meta.Size = f.Size
		if f.Size != nil && *f.Size < 0 {
			return core.Metadata{}, core.Fail(core.InvalidUpstreamResponse, nil)
		}
	default:
		return core.Metadata{}, core.Fail(core.InvalidUpstreamResponse, nil)
	}
	return meta, nil
}

type nativePage struct {
	Items *[]nativeFile `json:"items"`
	Next  string        `json:"next_marker"`
}

func (d *Driver) page(ctx context.Context, parent, marker string, n int) ([]nativeFile, string, error) {
	var out nativePage
	err := d.post(ctx, "/adrive/v1.0/openFile/list", map[string]any{"drive_id": d.opts.DriveID, "parent_file_id": parent, "marker": marker, "limit": n}, &out)
	if err != nil {
		return nil, "", err
	}
	if out.Items == nil || (out.Next != "" && out.Next == marker) {
		return nil, "", core.Fail(core.InvalidUpstreamResponse, nil)
	}
	for _, f := range *out.Items {
		if f.ID == "" || f.Drive != d.opts.DriveID || f.Parent != parent {
			return nil, "", core.Fail(core.InvalidUpstreamResponse, nil)
		}
		if _, err := f.metadata(); err != nil {
			return nil, "", err
		}
	}
	return *out.Items, out.Next, nil
}

// resolve scans every native page for each segment before claiming uniqueness.
// It keeps one candidate and one page, never a directory-wide ID cache.
func (d *Driver) resolve(ctx context.Context, p string) (nativeFile, error) {
	if err := d.admit(ctx, p); err != nil {
		return nativeFile{}, err
	}
	current := nativeFile{ID: d.opts.RootID, Drive: d.opts.DriveID, Type: "folder"}
	if p == "/" {
		return current, nil
	}
	for _, part := range strings.Split(strings.TrimPrefix(p, "/"), "/") {
		if current.Type != "folder" {
			return nativeFile{}, core.Fail(core.NotDirectory, nil)
		}
		found, exists, err := d.child(ctx, current.ID, part)
		if err != nil {
			return nativeFile{}, err
		}
		if !exists {
			return nativeFile{}, core.Fail(core.NotFound, nil)
		}
		current = found
	}
	return current, nil
}
func (d *Driver) child(ctx context.Context, parent, name string) (found nativeFile, exists bool, err error) {
	marker := ""
	for pages := 0; ; pages++ {
		if d.opts.MaxResolvePages > 0 && pages >= d.opts.MaxResolvePages {
			return found, exists, core.Fail(core.ResolutionBudgetExceeded, nil)
		}
		batch, next, e := d.page(ctx, parent, marker, 100)
		if e != nil {
			return found, exists, e
		}
		for _, f := range batch {
			if f.Name == name {
				if exists {
					return found, exists, core.Fail(core.PathAmbiguous, nil)
				}
				found, exists = f, true
			}
		}
		if next == "" {
			return
		}
		marker = next
	}
}
func (d *Driver) Stat(ctx context.Context, pAddress core.NativeAddress, selections ...core.ReadSelection) (meta core.Metadata, err error) {
	if len(selections) > 1 {
		return meta, core.Fail(core.InvalidRequest, nil)
	}
	var selection core.ReadSelection
	if len(selections) == 1 {
		selection = selections[0]
	}
	if selection.Version != "" {
		return meta, core.Fail(core.UnsupportedOperation, nil)
	}
	defer func() {
		if err == nil {
			err = selection.Check(meta.Evidence)
		}
	}()
	if pAddress.Namespace() != core.TreeNamespace {
		return core.Metadata{}, core.Fail(core.UnsupportedOperation, nil)
	}
	p := pAddress.Value()

	f, err := d.resolve(ctx, p)
	if err != nil {
		return core.Metadata{}, err
	}
	if p == "/" {
		return core.Metadata{Kind: core.Directory}, nil
	}
	return f.metadata()
}
func (d *Driver) Enumerate(ctx context.Context, pAddress core.NativeQuery) (core.Iterator, error) {
	if pAddress.Address().Namespace() != core.TreeNamespace || pAddress.Delimiter() != "" || pAddress.StartAfter() != "" {
		return nil, core.Fail(core.UnsupportedOperation, nil)
	}
	p := pAddress.Address().Value()

	f, err := d.resolve(ctx, p)
	if err != nil {
		return nil, err
	}
	if f.Type != "folder" {
		return nil, core.Fail(core.NotDirectory, nil)
	}
	return &iterator{driver: d, parent: f.ID}, nil
}

type iterator struct {
	driver         *Driver
	parent, marker string
	done, closed   bool
	files          map[string]nativeFile
}

func (i *iterator) Close() error { i.closed = true; i.files = nil; return nil }
func (i *iterator) Next(ctx context.Context, n int) ([]core.Metadata, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if i.closed {
		return nil, io.ErrClosedPipe
	}
	if i.done {
		return nil, io.EOF
	}
	if i.driver.closed.Load() {
		return nil, core.Fail(core.MountUnavailable, nil)
	}
	if n < 1 {
		return nil, core.Fail(core.InvalidRequest, nil)
	}
	if n > 100 {
		n = 100
	}
	batch, next, err := i.driver.page(ctx, i.parent, i.marker, n)
	if err != nil {
		return nil, err
	}
	items := make([]core.Metadata, 0, len(batch))
	for _, f := range batch {
		if i.files != nil {
			if _, exists := i.files[f.Name]; exists {
				return nil, core.Fail(core.PathAmbiguous, nil)
			}
			i.files[f.Name] = f
		}
		meta, err := f.metadata()
		if err != nil {
			return nil, err
		}
		items = append(items, meta)
	}
	i.marker = next
	i.done = next == ""
	if i.done {
		return items, io.EOF
	}
	return items, nil
}

type nativeError struct {
	Code string `json:"code"`
}

func statusError(status int, code string, cause error) error {
	projected := core.UpstreamFailure
	switch {
	case status == 429 || code == "TooManyRequests":
		projected = core.UpstreamThrottled
	case code == "NotFound.FileId":
		projected = core.NotFound
	case status == 401 || status == 403:
		projected = core.UpstreamAccessDenied
	case status == 408 || status == 504:
		projected = core.UpstreamTimeout
	}
	return core.Fail(projected, cause)
}
func networkError(err error) error {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	var timeout net.Error
	if errors.As(err, &timeout) && timeout.Timeout() {
		return core.Fail(core.UpstreamTimeout, err)
	}
	return core.Fail(core.UpstreamFailure, err)
}
func decode(body io.Reader, max int64, out any) error {
	var limited *io.LimitedReader
	if max > 0 && max < int64(1<<63-1) {
		limited = &io.LimitedReader{R: body, N: max + 1}
		body = limited
	}
	dec := json.NewDecoder(body)
	err := dec.Decode(out)
	if err == nil {
		var extra any
		if trailing := dec.Decode(&extra); trailing != io.EOF {
			err = errors.New("unexpected trailing response data")
		}
	}
	if limited != nil && limited.N == 0 {
		return core.Fail(core.ResponseBudgetExceeded, err)
	}
	if err != nil {
		return core.Fail(core.InvalidUpstreamResponse, err)
	}
	return nil
}
func (d *Driver) post(ctx context.Context, route string, input, out any) error {
	_, err := d.call(ctx, route, input, out, true)
	return err
}

// call distinguishes reads, which may replay an auth rejection, from mutations.
// A rejected mutation refreshes credentials for the next call without replaying.
func (d *Driver) call(ctx context.Context, route string, input, out any, replay bool) (rejected bool, err error) {
	payload, err := json.Marshal(input)
	if err != nil {
		return true, err
	}
	access, err := d.opts.Credentials.Access(ctx)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return true, err
		}
		return true, core.Fail(core.UpstreamAccessDenied, err)
	}
	for attempt := 0; attempt < 2; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimSuffix(d.opts.Endpoint, "/")+route, bytes.NewReader(payload))
		if err != nil {
			return true, err
		}
		req.Header.Set("Authorization", "Bearer "+access)
		req.Header.Set("Content-Type", "application/json")
		response, err := d.client.Do(req)
		if err != nil {
			return false, networkError(err)
		}
		if response.StatusCode >= 200 && response.StatusCode < 300 {
			err = decode(response.Body, d.opts.MaxResponseBytes, out)
			return false, errors.Join(err, d.closeBody(response.Body))
		}
		var failure nativeError
		decodeErr := decode(response.Body, d.opts.MaxResponseBytes, &failure)
		closeErr := d.closeBody(response.Body)
		rejected = response.StatusCode == 400 || response.StatusCode == 401 || response.StatusCode == 403 || response.StatusCode == 404 || response.StatusCode == 409 || response.StatusCode == 429
		if closeErr != nil {
			return rejected, errors.Join(statusError(response.StatusCode, failure.Code, decodeErr), closeErr)
		}
		if attempt == 0 && (failure.Code == "AccessTokenExpired" || failure.Code == "AccessTokenInvalid") {
			access, err = d.opts.Credentials.Reject(ctx, access)
			if err != nil {
				return true, core.Fail(core.UpstreamAccessDenied, err)
			}
			if !replay {
				return true, errors.Join(statusError(response.StatusCode, failure.Code, decodeErr), ctx.Err())
			}
			continue
		}
		return rejected, statusError(response.StatusCode, failure.Code, decodeErr)
	}
	return true, core.Fail(core.UpstreamAccessDenied, nil)
}
func rangeHeader(r *core.Range) string {
	if r.Suffix {
		return fmt.Sprintf("bytes=-%d", r.Start)
	}
	if r.End < 0 {
		return fmt.Sprintf("bytes=%d-", r.Start)
	}
	return fmt.Sprintf("bytes=%d-%d", r.Start, r.End)
}
func (d *Driver) Open(ctx context.Context, pAddress core.NativeAddress, options core.OpenOptions) (core.Opened, error) {
	if err := options.Validate(); err != nil {
		return core.Opened{}, err
	}
	if options.Mode != core.StreamRead {
		return core.Opened{Change: core.Change{Effect: core.Unchanged}}, core.Fail(core.UnsupportedOperation, nil)
	}
	wanted := options.Range
	if options.Selection.Version != "" {
		return core.Opened{}, core.Fail(core.UnsupportedOperation, nil)
	}
	if pAddress.Namespace() != core.TreeNamespace {
		return core.Opened{}, core.Fail(core.UnsupportedOperation, nil)
	}
	p := pAddress.Value()

	if wanted != nil && !wanted.Valid() {
		return core.Opened{}, core.Fail(core.InvalidRange, nil)
	}
	f, err := d.resolve(ctx, p)
	if err != nil {
		return core.Opened{}, err
	}
	if f.Type != "file" {
		return core.Opened{}, core.Fail(core.NotFile, nil)
	}
	// IDs and temporary download URLs do not establish a representation validator.
	if err := options.Selection.Check(core.Evidence{}); err != nil {
		return core.Opened{}, err
	}
	return d.openFile(ctx, f, wanted)
}

func (d *Driver) openFile(ctx context.Context, f nativeFile, wanted *core.Range) (core.Opened, error) {
	if err := d.admit(ctx, "/"); err != nil {
		return core.Opened{}, err
	}
	if f.Type != "file" {
		return core.Opened{}, core.Fail(core.NotFile, nil)
	}
	var link struct {
		URL    string `json:"url"`
		Method string `json:"method"`
	}
	if err := d.post(ctx, "/adrive/v1.0/openFile/getDownloadUrl", map[string]string{"drive_id": d.opts.DriveID, "file_id": f.ID}, &link); err != nil {
		return core.Opened{}, err
	}
	u, err := url.Parse(link.URL)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.Fragment != "" || link.Method != "GET" {
		return core.Opened{}, core.Fail(core.InvalidUpstreamResponse, errors.New("invalid download target"))
	}
	// This is a fresh request: provider bearer credentials never reach a download origin.
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return core.Opened{}, core.Fail(core.InvalidUpstreamResponse, err)
	}
	req.Header.Set("Accept-Encoding", "identity")
	if wanted != nil {
		req.Header.Set("Range", rangeHeader(wanted))
	}
	response, err := d.client.Do(req)
	if err != nil {
		return core.Opened{}, networkError(err)
	}
	opened := core.Opened{Metadata: core.Metadata{Name: f.Name, Kind: core.File}, Body: response.Body}
	if response.StatusCode != 200 && response.StatusCode != 206 {
		failure := statusError(response.StatusCode, "", nil)
		if response.StatusCode == 416 {
			failure = &core.Error{Code: core.RangeNotSatisfiable}
			var total int64
			text := response.Header.Get("Content-Range")
			if _, err := fmt.Sscanf(text, "bytes */%d", &total); err == nil && total >= 0 && text == fmt.Sprintf("bytes */%d", total) {
				failure.(*core.Error).Total = &total
			}
		}
		return opened, failure
	}
	if response.Header.Get("Content-Encoding") != "" && response.Header.Get("Content-Encoding") != "identity" {
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
	if response.StatusCode == 206 {
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
