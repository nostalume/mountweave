// Package alipan implements rooted file access using Aliyun Drive Open's public API.
package alipan

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"math/bits"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/alipan/credential"
	"github.com/nostalume/mountweave/drivers/internal/childscan"
	"github.com/nostalume/mountweave/drivers/internal/httpcontent"
	"github.com/nostalume/mountweave/drivers/internal/upload"
)

const Endpoint = "https://openapi.alipan.com"

// Options selects a drive and either a prepared RootID or a drive-relative
// RootPath. New and Open validate settings before using runtime resources.
// Client is borrowed. See New and Open for ownership.
type Options struct {
	Client      *http.Client       `json:"-"`
	Credentials *credential.Source `json:"-"`
	Endpoint    string             `json:"endpoint,omitempty"`
	DriveID     string             `json:"drive_id"`
	RootID      string             `json:"root_id,omitempty"`
	RootPath    string             `json:"root_path,omitempty"`
	// Zero leaves these optional host policies unset.
	MaxResolvePages  int   `json:"max_resolve_pages,omitempty"`
	MaxResponseBytes int64 `json:"max_response_bytes,omitempty"`
	// Both values are required to enable known-length new-file uploads.
	PartSize         int64 `json:"part_size,omitempty"`
	MaxBufferedBytes int64 `json:"max_buffered_bytes,omitempty"`
	PartConcurrency  int   `json:"part_concurrency,omitempty"`
	// Zero selects a one-second interval between running deletion-task queries.
	DeletionPollInterval time.Duration `json:"deletion_poll_interval,omitempty"`
}

// Validate checks static settings without opening a connection or acquiring a
// credential owner. New and Open separately require Client and Credentials.
func (o Options) Validate() error {
	if o.DriveID == "" || (o.RootID == "") == (o.RootPath == "") {
		return core.Fail(core.InvalidRequest, errors.New("a drive and exactly one root ID or root path are required"))
	}
	if o.RootPath != "" {
		if err := core.ValidatePath(o.RootPath); err != nil {
			return err
		}
	}
	if o.Endpoint != "" {
		if err := validEndpoint(o.Endpoint); err != nil {
			return core.Fail(core.InvalidRequest, err)
		}
	}
	if o.MaxResolvePages < 0 || o.MaxResponseBytes < 0 || o.PartConcurrency < 0 || o.DeletionPollInterval < 0 ||
		o.PartSize < 0 || o.MaxBufferedBytes < 0 || (o.PartSize == 0) != (o.MaxBufferedBytes == 0) || o.PartSize > 5<<30 {
		return core.Fail(core.InvalidRequest, nil)
	}
	if o.PartSize != 0 {
		if err := upload.Validate(o.PartSize, o.MaxBufferedBytes, o.PartConcurrency); err != nil {
			return core.Fail(core.InvalidRequest, err)
		}
	}
	return nil
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

// New constructs a prepared root without network I/O. Only success transfers
// Credentials. Use Open to resolve a RootPath with a cancellable context.
func New(opts Options) (*Driver, error) {
	if err := opts.Validate(); err != nil {
		return nil, err
	}
	if opts.RootPath != "" {
		return nil, core.Fail(core.InvalidRequest, errors.New("root path resolution requires Open with a context"))
	}
	return newDriver(opts)
}

func newDriver(opts Options) (*Driver, error) {
	if opts.Endpoint == "" {
		opts.Endpoint = Endpoint
	}
	if opts.Client == nil || opts.Credentials == nil {
		return nil, errors.New("a client and credential owner are required")
	}
	d := &Driver{opts: opts, client: isolatedClient(opts.Client)}
	if opts.PartSize != 0 {
		var err error
		d.buffer, err = upload.New(opts.MaxBufferedBytes, opts.PartConcurrency)
		if err != nil {
			return nil, core.Fail(core.InvalidRequest, err)
		}
	}
	return d, nil
}

// Open constructs a root, resolving RootPath once from the selected drive's
// native root. Every returned Driver owns Credentials, including on error;
// callers must close a non-nil failed result. A nil result leaves Credentials
// with the caller.
// Failed results refuse operations but retain any resources needing cleanup.
func Open(ctx context.Context, opts Options) (*Driver, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := opts.Validate(); err != nil {
		return nil, err
	}
	rootPath := opts.RootPath
	if rootPath != "" {
		opts.RootID, opts.RootPath = "root", ""
	}
	d, err := newDriver(opts)
	if err != nil || rootPath == "" {
		return d, err
	}
	root, err := d.resolve(ctx, rootPath)
	if err == nil && root.Type != "folder" {
		err = core.Fail(core.NotDirectory, nil)
	}
	if err == nil {
		err = ctx.Err()
	}
	if err != nil {
		d.closed.Store(true)
		return d, err
	}
	d.opts.RootID = root.ID
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
	failed := d.failedBodies
	d.failedBodies, d.closeErr = nil, nil
	for _, body := range failed {
		if retryErr := body.Close(); retryErr != nil {
			d.failedBodies = append(d.failedBodies, body)
			d.closeErr = errors.Join(d.closeErr, retryErr)
		}
	}
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
	ID         string     `json:"file_id"`
	Drive      string     `json:"drive_id"`
	Parent     string     `json:"parent_file_id"`
	Name       string     `json:"name"`
	Type       string     `json:"type"`
	Size       *int64     `json:"size"`
	Modified   *time.Time `json:"updated_at"`
	PunishFlag int        `json:"punish_flag"`
}

// metadata projects an entry whose native fields were validated by page.
func (f nativeFile) metadata() core.Metadata {
	meta := core.Metadata{Name: f.Name, ModifiedAt: f.Modified}
	if f.Type == "folder" {
		meta.Kind = core.Directory
	} else {
		meta.Kind = core.File
		meta.Size = f.Size
	}
	return meta
}

type nativePage struct {
	Items *[]nativeFile `json:"items"`
	Next  string        `json:"next_marker"`
}

func (d *Driver) page(ctx context.Context, parent, marker string, n int) ([]nativeFile, string, error) {
	var out nativePage
	err := d.post(ctx, "/adrive/v1.0/openFile/list", map[string]any{"drive_id": d.opts.DriveID, "parent_file_id": parent, "marker": marker, "limit": n}, &out)
	if err != nil {
		return nil, "", pageFailure(err)
	}
	if out.Items == nil || (out.Next != "" && out.Next == marker) {
		return nil, "", core.Fail(core.InvalidUpstreamResponse, nil)
	}
	for _, f := range *out.Items {
		if f.ID == "" || f.Drive != d.opts.DriveID || f.Parent != parent {
			return nil, "", core.Fail(core.InvalidUpstreamResponse, nil)
		}
		if !core.ValidName(f.Name) {
			return nil, "", core.Fail(core.UnrepresentableName, nil)
		}
		if (f.Type != "folder" && f.Type != "file") || (f.Type == "file" && f.Size != nil && *f.Size < 0) {
			return nil, "", core.Fail(core.InvalidUpstreamResponse, nil)
		}
	}
	return *out.Items, out.Next, nil
}

// A failed native page cannot advertise enumeration EOF. Keep its classification,
// diagnostic and independent causes while replacing only failed EOF leaves.
func pageFailure(err error) error {
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
func (d *Driver) child(ctx context.Context, parent, name string) (nativeFile, bool, error) {
	scan := iterator{driver: d, parent: parent}
	return childscan.Unique(ctx, name, func(ctx context.Context) ([]nativeFile, error) {
		if d.opts.MaxResolvePages > 0 && scan.pages >= uint64(d.opts.MaxResolvePages) {
			return nil, core.Fail(core.ResolutionBudgetExceeded, nil)
		}
		return scan.next(ctx, 100)
	}, func(f nativeFile) (string, string) { return f.ID, f.Name })
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
	return f.metadata(), nil
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
	checkpoint     string
	pages          uint64
	done, closed   bool
	files          map[string]nativeFile
}

func (i *iterator) Close() error { i.closed = true; i.files = nil; return nil }
func (i *iterator) Next(ctx context.Context, n int) ([]core.Metadata, error) {
	batch, err := i.next(ctx, n)
	if err != nil && err != io.EOF {
		return nil, err
	}
	items := make([]core.Metadata, 0, len(batch))
	for _, f := range batch {
		items = append(items, f.metadata())
	}
	return items, err
}

func (i *iterator) next(ctx context.Context, n int) ([]nativeFile, error) {
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
	batch, next, err := i.driver.page(ctx, i.parent, i.marker, min(n, 100))
	if err != nil {
		return nil, err
	}
	if next != "" && next == i.checkpoint {
		return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("native continuation cycle"))
	}
	if i.files != nil {
		for _, f := range batch {
			if previous, exists := i.files[f.Name]; exists {
				if previous.ID == f.ID {
					return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("native child identity repeated"))
				}
				return nil, core.Fail(core.PathAmbiguous, nil)
			}
			i.files[f.Name] = f
		}
	}
	i.marker, i.done = next, next == ""
	if i.done {
		return batch, io.EOF
	}
	// Power-of-two checkpoints detect a repeating cycle without saving every cursor.
	i.pages++
	if bits.OnesCount64(i.pages) == 1 {
		i.checkpoint = next
	}
	return batch, nil
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

// call returns the native command's acceptance independently of response cleanup.
// A rejected mutation refreshes credentials for the next call without replaying.
func (d *Driver) call(ctx context.Context, route string, input, out any, replay bool) (core.Effect, error) {
	payload, err := json.Marshal(input)
	if err != nil {
		return core.Unchanged, err
	}
	access, err := d.opts.Credentials.Access(ctx)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return core.Unchanged, err
		}
		return core.Unchanged, core.Fail(core.UpstreamAccessDenied, err)
	}
	for attempt := 0; attempt < 2; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimSuffix(d.opts.Endpoint, "/")+route, bytes.NewReader(payload))
		if err != nil {
			return core.Unchanged, err
		}
		req.Header.Set("Authorization", "Bearer "+access)
		req.Header.Set("Content-Type", "application/json")
		if err := ctx.Err(); err != nil {
			return core.Unchanged, err
		}
		response, err := d.client.Do(req)
		if err != nil {
			return core.Unknown, networkError(err)
		}
		effect := core.Unknown
		if response.StatusCode >= 200 && response.StatusCode < 300 {
			err = decode(response.Body, d.opts.MaxResponseBytes, out)
			if err == nil {
				effect = core.Applied
			}
			return effect, errors.Join(err, d.closeBody(response.Body))
		}
		var failure nativeError
		decodeErr := decode(response.Body, d.opts.MaxResponseBytes, &failure)
		closeErr := d.closeBody(response.Body)
		switch response.StatusCode {
		case 400, 401, 403, 404, 409, 429:
			effect = core.Unchanged
		}
		if closeErr != nil {
			return effect, errors.Join(statusError(response.StatusCode, failure.Code, decodeErr), closeErr)
		}
		if attempt == 0 && (failure.Code == "AccessTokenExpired" || failure.Code == "AccessTokenInvalid") {
			access, err = d.opts.Credentials.Reject(ctx, access)
			if err != nil {
				return core.Unchanged, core.Fail(core.UpstreamAccessDenied, err)
			}
			if !replay {
				return core.Unchanged, errors.Join(statusError(response.StatusCode, failure.Code, decodeErr), ctx.Err())
			}
			continue
		}
		return effect, statusError(response.StatusCode, failure.Code, decodeErr)
	}
	return core.Unchanged, core.Fail(core.UpstreamAccessDenied, nil)
}
func (d *Driver) Open(ctx context.Context, pAddress core.NativeAddress, options core.OpenOptions) (core.Opened, error) {
	if err := options.Validate(); err != nil {
		return core.Opened{}, err
	}
	if options.Mode != core.StreamRead {
		return core.Opened{Change: core.Change{Effect: core.Unchanged}}, core.Fail(core.UnsupportedOperation, nil)
	}
	if options.Selection.Version != "" {
		return core.Opened{}, core.Fail(core.UnsupportedOperation, nil)
	}
	if pAddress.Namespace() != core.TreeNamespace {
		return core.Opened{}, core.Fail(core.UnsupportedOperation, nil)
	}
	p := pAddress.Value()

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
	return d.openFile(ctx, f, options.Range)
}

func (d *Driver) openFile(ctx context.Context, f nativeFile, wanted *core.Range) (core.Opened, error) {
	if err := d.admit(ctx, "/"); err != nil {
		return core.Opened{}, err
	}
	if f.Type != "file" {
		return core.Opened{}, core.Fail(core.NotFile, nil)
	}
	// A frozen file's grant can point to a notice instead of the original bytes.
	if f.PunishFlag == 2 {
		return core.Opened{}, core.Fail(core.UpstreamAccessDenied, errors.New("Aliyun Drive file is frozen"))
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
		return core.Opened{}, core.Fail(core.InvalidUpstreamResponse, nil)
	}
	req.Header.Set("Accept-Encoding", "identity")
	if wanted != nil {
		req.Header.Set("Range", httpcontent.Header(wanted))
	}
	response, err := d.client.Do(req)
	if errors.Is(err, context.Canceled) {
		return core.Opened{}, context.Canceled
	}
	if err != nil {
		return core.Opened{}, httpcontent.UpstreamIOError(err)
	}
	if response.StatusCode != http.StatusOK && response.StatusCode != http.StatusPartialContent && response.StatusCode != http.StatusRequestedRangeNotSatisfiable {
		return core.Opened{Metadata: core.Metadata{Name: f.Name, Kind: core.File}, Body: response.Body}, statusError(response.StatusCode, "", nil)
	}
	opened, err := httpcontent.Open(response, wanted)
	opened.Metadata.Name = f.Name
	return opened, err
}
