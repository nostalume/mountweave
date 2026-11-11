// Package googledrive connects Google Drive V3 files to Mountweave.
package googledrive

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"unicode/utf8"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/internal/httpcontent"
)

const endpoint = "https://www.googleapis.com/drive/v3"

// Options borrows a prepared authenticated Client and an independent content
// client. RootID and account-root-relative RootPath are alternative selections;
// if neither is set, the native root alias is used. ExportFormats chooses the
// byte representation for native Workspace MIME types.
type Options struct {
	RootID, RootPath, RootResourceKey string
	Client, ContentClient             *http.Client
	ExportFormats                     map[string]string
	MaxResponseBytes                  int64
	MaxResolvePages                   int
}

type Driver struct {
	root         item
	api, content http.Client
	exports      map[string]string
	limit        int64
	resolvePages int
	closed       atomic.Bool
	cleanup      httpcontent.ResponseCleanup
}

// Open confirms an actual directory and fixes optional export choices. A
// non-nil result on error retains cleanup and must be closed by its creator.
func Open(ctx context.Context, o Options) (*Driver, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if o.Client == nil || o.Client == o.ContentClient || (o.RootID != "" && o.RootPath != "") ||
		o.MaxResponseBytes < 0 || o.MaxResponseBytes == math.MaxInt64 || o.MaxResolvePages < 0 ||
		!utf8.ValidString(o.RootID) || strings.ContainsAny(o.RootID, "\r\n") ||
		!headerPart(o.RootResourceKey) || (o.RootResourceKey != "" && !headerPart(o.RootID)) {
		return nil, core.Fail(core.InvalidRequest, nil)
	}
	if o.RootPath != "" {
		if err := core.ValidatePath(o.RootPath); err != nil {
			return nil, err
		}
	}
	exports := make(map[string]string, len(o.ExportFormats))
	for source, target := range o.ExportFormats {
		if source == "" || target == "" || !utf8.ValidString(source) || !utf8.ValidString(target) {
			return nil, core.Fail(core.InvalidRequest, errors.New("export MIME types must be nonempty"))
		}
		if reason := unavailableContent(source); reason != "" {
			return nil, core.Fail(core.InvalidRequest, errors.New(reason))
		}
		exports[source] = target
	}
	if o.MaxResponseBytes == 0 {
		o.MaxResponseBytes = 4 << 20
	}
	content := o.ContentClient
	if content == nil {
		content = &http.Client{}
	}
	d := &Driver{api: *o.Client, content: *content, exports: exports, limit: o.MaxResponseBytes, resolvePages: o.MaxResolvePages}
	d.api.Jar, d.content.Jar = nil, nil
	refuse := func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	d.api.CheckRedirect, d.content.CheckRedirect = refuse, refuse
	id := o.RootID
	if id == "" {
		id = "root"
	}
	key := o.RootResourceKey
	if o.RootPath != "" {
		key = ""
	}
	root, err := d.getItem(ctx, item{ID: id, ResourceKey: key})
	if err == nil && root.meta.Kind != core.Directory {
		err = core.Fail(core.NotDirectory, nil)
	}
	if err == nil {
		d.root = root
		if o.RootPath != "" && o.RootPath != "/" {
			root, err = d.resolve(ctx, o.RootPath)
		}
	}
	if err == nil && root.meta.Kind != core.Directory {
		err = core.Fail(core.NotDirectory, nil)
	}
	if err == nil {
		if root.ResourceKey == "" {
			root.ResourceKey = o.RootResourceKey
		}
		if root.ResourceKey != "" && !headerPart(root.ID) {
			err = core.Fail(core.InvalidUpstreamResponse, errors.New("root ID cannot carry its resource key"))
		}
		d.root = root
	}
	if err == nil && len(exports) != 0 {
		err = d.confirmExports(ctx)
	}
	if err == nil {
		err = ctx.Err()
	}
	if err != nil {
		d.closed.Store(true)
	}
	return d, err
}

func headerPart(value string) bool {
	return utf8.ValidString(value) && !strings.ContainsAny(value, "\r\n/,")
}

func apiURL(route string, query url.Values) *url.URL {
	u, _ := url.Parse(endpoint + route)
	u.RawQuery = query.Encode()
	return u
}

func resourceHeader(req *http.Request, f item) {
	if f.ResourceKey != "" {
		req.Header.Set("X-Goog-Drive-Resource-Keys", f.ID+"/"+f.ResourceKey)
	}
}

func (d *Driver) do(client *http.Client, req *http.Request, private bool) (*http.Response, error) {
	if err := req.Context().Err(); err != nil {
		return nil, err
	}
	if d.closed.Load() {
		return nil, core.Fail(core.MountUnavailable, nil)
	}
	response, err := client.Do(req)
	if err != nil {
		if private {
			err = httpcontent.PrivateIOError(err)
		} else {
			var failure *url.Error
			if errors.As(err, &failure) {
				err = failure.Err
			}
			err = failedEOF(err)
		}
		if response != nil && response.Body != nil {
			err = errors.Join(err, d.cleanup.CloseBody(response.Body))
		}
		return nil, errors.Join(core.Fail(core.UpstreamFailure, err), req.Context().Err())
	}
	return response, nil
}

func (d *Driver) json(ctx context.Context, target *url.URL, ref item, output any) (err error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target.String(), nil)
	if err != nil {
		return core.Fail(core.InvalidRequest, err)
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Accept-Encoding", "identity")
	resourceHeader(req, ref)
	response, err := d.do(&d.api, req, false)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, d.cleanup.CloseBody(response.Body)) }()
	if response.StatusCode != http.StatusOK {
		return d.responseFailure(response, true)
	}
	return d.readJSON(response, output)
}

func (d *Driver) readJSON(response *http.Response, output any) error {
	if coding := response.Header.Get("Content-Encoding"); coding != "" && coding != "identity" {
		return core.Fail(core.InvalidUpstreamResponse, nil)
	}
	data, err := io.ReadAll(io.LimitReader(response.Body, d.limit+1))
	if int64(len(data)) > d.limit {
		return errors.Join(core.Fail(core.ResponseBudgetExceeded, nil), failedEOF(err))
	}
	if err != nil {
		return core.Fail(core.InvalidUpstreamResponse, failedEOF(err))
	}
	if err := json.Unmarshal(data, output); err != nil {
		return core.Fail(core.InvalidUpstreamResponse, err)
	}
	return nil
}

func (d *Driver) responseFailure(response *http.Response, native bool) error {
	code := core.UpstreamFailure
	switch response.StatusCode {
	case 401, 403:
		code = core.UpstreamAccessDenied
	case 404:
		code = core.NotFound
	case 429, 503:
		code = core.UpstreamThrottled
	}
	if !native {
		return core.Fail(code, fmt.Errorf("Google Drive content endpoint returned HTTP %d", response.StatusCode))
	}
	var problem struct {
		Error struct {
			Message string `json:"message"`
			Errors  []struct {
				Reason string `json:"reason"`
			} `json:"errors"`
		} `json:"error"`
	}
	readErr := d.readJSON(response, &problem)
	reason := problem.Error.Message
	for _, detail := range problem.Error.Errors {
		if detail.Reason == "rateLimitExceeded" || detail.Reason == "userRateLimitExceeded" {
			code = core.UpstreamThrottled
		}
		if detail.Reason != "" {
			reason += " [" + detail.Reason + "]"
		}
	}
	return core.Fail(code, errors.Join(fmt.Errorf("Google Drive returned HTTP %d: %s", response.StatusCode, reason), readErr))
}

// Failed I/O EOF is not successful native enumeration. Rewrite only affected
// branches, retaining diagnostics, classifications and independent causes.
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

// Close retries retained short-response cleanup after Core has canceled and
// joined active uses. Clients, transports and credentials remain host-owned.
func (d *Driver) Close() error {
	d.closed.Store(true)
	return d.cleanup.Retry()
}
