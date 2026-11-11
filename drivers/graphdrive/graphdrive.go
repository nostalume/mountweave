// Package graphdrive adapts a prepared Microsoft Graph connection to Mountweave.
package graphdrive

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/internal/httpcontent"
	"github.com/nostalume/mountweave/drivers/internal/upload"
)

const Endpoint = "https://graph.microsoft.com"

// Options borrows an authenticated APIClient and an unauthenticated ContentClient.
// The host owns their transports, credential providers and request replay policy.
type Options struct {
	APIClient        *http.Client  `json:"-"`
	ContentClient    *http.Client  `json:"-"`
	Endpoint         string        `json:"endpoint,omitempty"`
	DriveID          string        `json:"drive_id"`
	RootID           string        `json:"root_id,omitempty"`
	RootPath         string        `json:"root_path,omitempty"`
	ShareURL         string        `json:"-"`
	ChunkSize        int64         `json:"chunk_size,omitempty"`
	MaxBufferedBytes int64         `json:"max_buffered_bytes,omitempty"`
	MaxResponseBytes int64         `json:"max_response_bytes,omitempty"`
	RangeAttempts    int           `json:"range_attempts,omitempty"`
	CleanupTimeout   time.Duration `json:"cleanup_timeout,omitempty"`
}

// Validate checks static settings without using clients or acquiring resources.
func (o Options) Validate() error {
	if o.ShareURL != "" {
		u, err := url.Parse(o.ShareURL)
		if err != nil || !utf8.ValidString(o.ShareURL) || u.Scheme != "https" || u.Hostname() == "" ||
			u.User != nil || u.Opaque != "" || u.Fragment != "" || o.DriveID != "" || o.RootID != "" || o.RootPath != "" ||
			o.ChunkSize != 0 || o.MaxBufferedBytes != 0 || o.MaxResponseBytes == math.MaxInt64 {
			return core.Fail(core.InvalidRequest, errors.New("a share URL selects one already accessible, read-only root"))
		}
	} else if o.DriveID == "" || !utf8.ValidString(o.DriveID) || !utf8.ValidString(o.RootID) ||
		(o.RootID == "") == (o.RootPath == "") {
		return core.Fail(core.InvalidRequest, errors.New("a drive and exactly one root ID or root path are required"))
	}
	if o.RootPath != "" {
		if err := core.ValidatePath(o.RootPath); err != nil {
			return err
		}
	}
	if o.Endpoint != "" {
		u, err := url.Parse(o.Endpoint)
		if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
			return core.Fail(core.InvalidRequest, errors.New("an HTTPS API origin is required"))
		}
	}
	if o.MaxResponseBytes < 0 || o.RangeAttempts < 0 || o.CleanupTimeout < 0 ||
		o.ChunkSize < 0 || o.MaxBufferedBytes < 0 || (o.ChunkSize == 0) != (o.MaxBufferedBytes == 0) {
		return core.Fail(core.InvalidRequest, nil)
	}
	if o.ChunkSize != 0 {
		if o.ChunkSize%327680 != 0 || o.ChunkSize >= 60<<20 {
			return core.Fail(core.InvalidRequest, errors.New("chunk size must be a multiple of 320 KiB below 60 MiB"))
		}
		return upload.Validate(o.ChunkSize, o.MaxBufferedBytes, 0)
	}
	return nil
}

type Driver struct {
	opts             Options
	api, content     *http.Client
	root             item
	readOnly         bool
	buffer           *upload.Budget
	closed           atomic.Bool
	nextUpload       atomic.Uint64
	mu               sync.Mutex
	failedBodies     []io.Closer
	privateResponses httpcontent.ResponseCleanup
	pending          map[uint64]pendingUpload
}

type item struct {
	ID       string     `json:"id"`
	Name     string     `json:"name"`
	ETag     string     `json:"eTag"`
	Size     *int64     `json:"size"`
	Modified *time.Time `json:"lastModifiedDateTime"`
	File     *struct{}  `json:"file"`
	Folder   *struct{}  `json:"folder"`
	Remote   *struct{}  `json:"remoteItem"`
	Package  *struct{}  `json:"package"`
	Parent   *struct {
		ID      string `json:"id"`
		DriveID string `json:"driveId"`
	} `json:"parentReference"`
	DownloadURL string `json:"@microsoft.graph.downloadUrl"`
}

func (f item) metadata() (core.Metadata, error) {
	if f.Remote != nil || f.Package != nil {
		return core.Metadata{}, core.Fail(core.UnsupportedOperation, nil)
	}
	if f.ID == "" || (f.File == nil) == (f.Folder == nil) || (f.Size != nil && *f.Size < 0) {
		return core.Metadata{}, core.Fail(core.InvalidUpstreamResponse, nil)
	}
	meta := core.Metadata{Name: f.Name, Kind: core.Directory, ModifiedAt: f.Modified}
	if f.File != nil {
		meta.Kind, meta.Size = core.File, f.Size
	}
	return meta, nil
}

// Open retrieves and binds an actual directory ID. A non-nil failed Driver
// retains failed response cleanup and must be closed; clients remain borrowed.
func Open(ctx context.Context, opts Options) (*Driver, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := opts.Validate(); err != nil {
		return nil, err
	}
	if opts.APIClient == nil || opts.ContentClient == nil {
		return nil, errors.New("prepared API and content clients are required")
	}
	if opts.Endpoint == "" {
		opts.Endpoint = Endpoint
	}
	opts.Endpoint = strings.TrimSuffix(opts.Endpoint, "/")
	if opts.RangeAttempts == 0 {
		opts.RangeAttempts = 1
	}
	shareURL := opts.ShareURL
	opts.ShareURL = ""
	d := &Driver{opts: opts, api: isolatedClient(opts.APIClient), content: isolatedClient(opts.ContentClient),
		readOnly: shareURL != "", pending: make(map[uint64]pendingUpload)}
	if shareURL != "" {
		if d.opts.MaxResponseBytes == 0 {
			d.opts.MaxResponseBytes = 4 << 20
		}
		encoded := "u!" + base64.RawURLEncoding.EncodeToString([]byte(shareURL))
		query := url.Values{"$select": {"id,name,folder,file,parentReference,remoteItem,package"}}
		shared, err := d.getItem(ctx, "/shares/"+encoded+"/driveItem?"+query.Encode())
		if err == nil && shared.Folder == nil {
			err = core.Fail(core.NotDirectory, nil)
		}
		if err == nil && (shared.Parent == nil || !core.ValidName(shared.Parent.DriveID)) {
			err = core.Fail(core.InvalidUpstreamResponse, errors.New("Graph did not identify the shared folder's drive"))
		}
		if err != nil {
			d.closed.Store(true)
			return d, err
		}
		d.opts.DriveID, d.opts.RootID = shared.Parent.DriveID, shared.ID
	}
	if opts.ChunkSize != 0 {
		var err error
		d.buffer, err = upload.New(opts.MaxBufferedBytes, int(min(opts.MaxBufferedBytes/opts.ChunkSize, int64(math.MaxInt))))
		if err != nil {
			return nil, err
		}
	}
	route := "/drives/" + url.PathEscape(d.opts.DriveID) + "/root"
	if d.opts.RootID != "" {
		route = "/drives/" + url.PathEscape(d.opts.DriveID) + "/items/" + url.PathEscape(d.opts.RootID)
	}
	root, err := d.getItem(ctx, route)
	if err == nil && d.opts.RootID != "" && root.ID != d.opts.RootID {
		err = core.Fail(core.InvalidUpstreamResponse, nil)
	}
	if err == nil && root.Folder == nil {
		err = core.Fail(core.NotDirectory, nil)
	}
	if err == nil {
		d.root = root
		if opts.RootPath != "" {
			root, err = d.resolve(ctx, opts.RootPath)
		}
	}
	if err == nil && root.Folder == nil {
		err = core.Fail(core.NotDirectory, nil)
	}
	if err == nil {
		err = ctx.Err()
	}
	if err != nil {
		d.closed.Store(true)
		return d, err
	}
	d.root = root
	return d, nil
}

func isolatedClient(client *http.Client) *http.Client {
	copy := *client
	copy.Jar = nil
	copy.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &copy
}

func (d *Driver) apiURL(route string) string { return d.opts.Endpoint + "/v1.0" + route }

func (d *Driver) apiRequest(ctx context.Context, method, target string, body io.Reader, contentType string, length int64) (*http.Response, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if d.closed.Load() {
		return nil, core.Fail(core.MountUnavailable, nil)
	}
	u, err := url.Parse(target)
	origin, _ := url.Parse(d.opts.Endpoint)
	if err != nil || u.Scheme != "https" || !strings.EqualFold(u.Host, origin.Host) || u.User != nil || u.Fragment != "" || !strings.HasPrefix(u.Path, "/v1.0/") {
		return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("invalid Graph request target"))
	}
	req, err := http.NewRequestWithContext(ctx, method, target, body)
	if err != nil {
		return nil, core.Fail(core.InvalidRequest, err)
	}
	req.Header.Set("Accept", "application/json")
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	req.ContentLength = length
	if length == 0 {
		req.Body = http.NoBody
	}
	response, err := d.api.Do(req)
	if err != nil {
		failure := networkError(err)
		if d.readOnly {
			failure = privateFailure(failure)
		}
		return nil, failure
	}
	return response, nil
}

func (d *Driver) decode(response *http.Response, out any) (decoded bool, err error) {
	defer func() { err = errors.Join(err, d.closeBody(response.Body)) }()
	if d.readOnly {
		err := httpcontent.DecodeJSON(response, d.opts.MaxResponseBytes, out)
		if err != nil && core.CodeOf(err) == core.InvalidUpstreamResponse {
			err = core.Fail(core.InvalidUpstreamResponse, nil)
		}
		return err == nil, err
	}
	return decodeJSON(response.Body, d.opts.MaxResponseBytes, out)
}

func decodeJSON(body io.Reader, max int64, out any) (bool, error) {
	var limited *io.LimitedReader
	if max > 0 && max < math.MaxInt64 {
		limited = &io.LimitedReader{R: body, N: max + 1}
		body = limited
	}
	decoder := json.NewDecoder(body)
	err := decoder.Decode(out)
	if err == nil {
		var extra any
		if tail := decoder.Decode(&extra); tail != io.EOF {
			err = errors.Join(errors.New("unexpected trailing Graph response data"), tail)
		}
	}
	if limited != nil && limited.N == 0 {
		return false, core.Fail(core.ResponseBudgetExceeded, err)
	}
	if err != nil {
		return false, core.Fail(core.InvalidUpstreamResponse, err)
	}
	return true, nil
}

func privateFailure(err error) error {
	return core.Fail(core.CodeOf(err), httpcontent.PrivateIOError(err))
}

func networkError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	var timeout net.Error
	if errors.As(err, &timeout) && timeout.Timeout() {
		return core.Fail(core.UpstreamTimeout, err)
	}
	return core.Fail(core.UpstreamFailure, err)
}

// responseFailure keeps a recoverable remote failure separate from cleanup,
// which cannot be erased by successfully retrying the native operation.
func (d *Driver) responseFailure(response *http.Response) (failure, cleanup error) {
	if d.readOnly {
		return statusError(response.StatusCode), d.closeBody(response.Body)
	}
	var native struct {
		Error struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	_, decodeErr := decodeJSON(response.Body, d.opts.MaxResponseBytes, &native)
	var cause error
	if native.Error.Code != "" || native.Error.Message != "" {
		cause = fmt.Errorf("Graph %s: %s", native.Error.Code, native.Error.Message)
	}
	return errors.Join(statusError(response.StatusCode), cause, decodeErr), d.closeBody(response.Body)
}

func statusError(status int) error {
	code := core.UpstreamFailure
	switch status {
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
	return core.Fail(code, fmt.Errorf("Graph HTTP status %d", status))
}
