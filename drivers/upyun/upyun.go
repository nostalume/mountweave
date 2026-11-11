// Package upyun connects a rooted USS directory using its native REST API.
package upyun

import (
	"context"
	"crypto/hmac"
	"crypto/md5"
	"crypto/sha1"
	"encoding/base64"
	"encoding/hex"
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

// Options selects an actual endpoint/bucket/subdirectory URL. Client and its
// transport remain borrowed. Operator/Password add native request signing; both
// may be empty when the client supplies authentication. MaxResponseBytes bounds
// each metadata/error response, not content; zero selects 4 MiB.
type Options struct {
	RootURL            string
	Operator, Password string
	Client             *http.Client
	MaxResponseBytes   int64
}

type Driver struct {
	root     *url.URL
	client   http.Client
	operator string
	key      []byte
	limit    int64
	closed   atomic.Bool
	mu       sync.Mutex
	failed   []io.Closer
}

// Open validates options and observes the directory root. A non-nil Driver on
// error still owns response cleanup and must be closed by its creator.
func Open(ctx context.Context, o Options) (*Driver, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	u, err := url.Parse(o.RootURL)
	if err != nil || u.Host == "" || (u.Scheme != "https" && u.Scheme != "http") || u.User != nil ||
		u.RawQuery != "" || u.Fragment != "" || u.Opaque != "" ||
		o.MaxResponseBytes < 0 || o.MaxResponseBytes == math.MaxInt64 ||
		strings.ContainsAny(o.Operator, ":\r\n") || (o.Operator == "") != (o.Password == "") {
		return nil, core.Fail(core.InvalidRequest, errors.New("a root URL, paired credentials and valid response budget are required"))
	}
	for _, part := range strings.Split(u.EscapedPath(), "/") {
		decoded, e := url.PathUnescape(part)
		if e != nil || strings.Contains(decoded, "/") {
			return nil, core.Fail(core.InvalidPath, e)
		}
	}
	root := strings.TrimSuffix(u.Path, "/")
	if root == "" || root == "/" || core.ValidatePath(root) != nil {
		return nil, core.Fail(core.InvalidPath, nil)
	}
	u.Path, u.RawPath = root+"/", ""
	client := o.Client
	if client == nil {
		client = http.DefaultClient
	}
	limit := o.MaxResponseBytes
	if limit == 0 {
		limit = 4 << 20
	}
	d := &Driver{root: u, client: *client, operator: o.Operator, limit: limit}
	if o.Operator != "" {
		sum := md5.Sum([]byte(o.Password))
		d.key = []byte(hex.EncodeToString(sum[:]))
	}
	d.client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	meta, err := d.Stat(ctx, core.NativePath("/"))
	if err == nil && meta.Kind != core.Directory {
		err = core.Fail(core.NotDirectory, nil)
	}
	return d, err
}

func (d *Driver) target(ctx context.Context, p core.NativeAddress) (*url.URL, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if d.closed.Load() {
		return nil, core.Fail(core.MountUnavailable, nil)
	}
	if p.Namespace() != core.TreeNamespace {
		return nil, core.Fail(core.UnsupportedOperation, nil)
	}
	if err := core.ValidatePath(p.Value()); err != nil {
		return nil, err
	}
	u := *d.root
	u.Path += strings.TrimPrefix(p.Value(), "/")
	return &u, nil
}

func signature(key []byte, method, uri, date, digest string) string {
	text := method + "&" + uri + "&" + date
	if digest != "" {
		text += "&" + digest
	}
	mac := hmac.New(sha1.New, key)
	_, _ = mac.Write([]byte(text))
	return base64.StdEncoding.EncodeToString(mac.Sum(nil))
}

func (d *Driver) request(ctx context.Context, method string, target *url.URL, headers http.Header) (*http.Response, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if d.closed.Load() {
		return nil, core.Fail(core.MountUnavailable, nil)
	}
	req, err := http.NewRequestWithContext(ctx, method, target.String(), nil)
	if err != nil {
		return nil, core.Fail(core.InvalidRequest, err)
	}
	req.Header = headers
	if req.Header == nil {
		req.Header = make(http.Header)
	}
	req.Header.Set("Accept-Encoding", "identity")
	if d.operator != "" {
		date := time.Now().UTC().Format(http.TimeFormat)
		req.Header.Set("Date", date)
		req.Header.Set("Authorization", "UPYUN "+d.operator+":"+signature(d.key, req.Method, req.URL.RequestURI(), date, req.Header.Get("Content-MD5")))
	}
	response, err := d.client.Do(req)
	if err != nil {
		var failure *url.Error
		if errors.As(err, &failure) {
			err = failure.Err
		}
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		if response != nil {
			err = errors.Join(err, d.closeBody(response.Body))
		}
		return nil, errors.Join(core.Fail(core.UpstreamFailure, err), ctx.Err())
	}
	return response, nil
}

func (d *Driver) readResponse(response *http.Response) ([]byte, error) {
	if encoding := response.Header.Get("Content-Encoding"); encoding != "" && encoding != "identity" {
		return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("encoded native metadata response"))
	}
	data, err := io.ReadAll(io.LimitReader(response.Body, d.limit+1))
	if int64(len(data)) > d.limit {
		return nil, core.Fail(core.ResponseBudgetExceeded, err)
	}
	return data, err
}

func statusError(status int, detail []byte) error {
	code := core.UpstreamFailure
	switch status {
	case 401, 403:
		code = core.UpstreamAccessDenied
	case 404:
		code = core.NotFound
	case 412:
		code = core.PreconditionFailed
	case 429, 503:
		code = core.UpstreamThrottled
	}
	var native struct {
		Code int `json:"code"`
	}
	if json.Unmarshal(detail, &native) == nil {
		switch native.Code {
		case 40300005:
			code = core.DirectoryNotEmpty
		case 40300015:
			code = core.NotFile
		case 40600002:
			code = core.AlreadyExists
		}
	}
	return core.Fail(code, fmt.Errorf("USS returned HTTP %d: %s", status, strings.TrimSpace(string(detail))))
}

func (d *Driver) failure(response *http.Response) error {
	data, err := d.readResponse(response)
	return errors.Join(statusError(response.StatusCode, data), err)
}

func (d *Driver) closeBody(body io.Closer) error {
	if body == nil {
		return nil
	}
	err := body.Close()
	if err != nil {
		d.mu.Lock()
		d.failed = append(d.failed, body)
		d.mu.Unlock()
	}
	return err
}

// Close retries owned metadata/error response cleanup. The host retains the
// client and transport; bodies handed to Core remain Core-owned.
func (d *Driver) Close() error {
	d.closed.Store(true)
	d.mu.Lock()
	bodies := d.failed
	d.failed = nil
	d.mu.Unlock()
	var err error
	for _, body := range bodies {
		err = errors.Join(err, d.closeBody(body))
	}
	return err
}

func (d *Driver) Stat(ctx context.Context, p core.NativeAddress, selections ...core.ReadSelection) (core.Metadata, error) {
	if len(selections) > 1 {
		return core.Metadata{}, core.Fail(core.InvalidRequest, nil)
	}
	var selection core.ReadSelection
	if len(selections) == 1 {
		selection = selections[0]
	}
	if err := selection.When.ForDomain(""); err != nil {
		return core.Metadata{}, err
	}
	if selection.Version != "" {
		return core.Metadata{}, core.Fail(core.UnsupportedOperation, nil)
	}
	target, err := d.target(ctx, p)
	if err != nil {
		return core.Metadata{}, err
	}
	response, err := d.request(ctx, "HEAD", target, nil)
	if err != nil {
		return core.Metadata{}, err
	}
	if response.StatusCode != http.StatusOK {
		return core.Metadata{}, errors.Join(d.failure(response), d.closeBody(response.Body))
	}
	meta := core.Metadata{Name: path.Base(p.Value()), Kind: core.Kind(response.Header.Get("x-upyun-file-type"))}
	if p.Value() == "/" {
		meta.Name = ""
	}
	switch meta.Kind {
	case core.File:
		size, e := strconv.ParseInt(response.Header.Get("x-upyun-file-size"), 10, 64)
		if e != nil || size < 0 {
			err = core.Fail(core.InvalidUpstreamResponse, e)
		} else {
			meta.Size = &size
		}
	case "folder":
		meta.Kind = core.Directory
	default:
		err = core.Fail(core.InvalidUpstreamResponse, errors.New("missing native resource type"))
	}
	// x-upyun-file-date is creation time, not ModifiedAt. Content-MD5 is not a validator.
	if text := response.Header.Get("Last-Modified"); text != "" {
		modified, e := http.ParseTime(text)
		if e != nil {
			err = errors.Join(err, core.Fail(core.InvalidUpstreamResponse, e))
		} else {
			meta.ModifiedAt = &modified
		}
	}
	if text := response.Header.Get("ETag"); text != "" {
		meta.Evidence.Validator.Tag = text
	}
	err = errors.Join(err, d.closeBody(response.Body))
	if err == nil {
		err = selection.Check(meta.Evidence)
	}
	return meta, err
}

func (d *Driver) Open(ctx context.Context, p core.NativeAddress, o core.OpenOptions) (core.Opened, error) {
	if err := o.Validate(); err != nil {
		return core.Opened{}, err
	}
	if err := o.Selection.When.ForDomain(""); err != nil {
		return core.Opened{}, err
	}
	if o.Mode != core.StreamRead || o.NoFollow || o.Selection.Version != "" {
		return core.Opened{Change: core.Change{Effect: core.Unchanged}}, core.Fail(core.UnsupportedOperation, nil)
	}
	target, err := d.target(ctx, p)
	if err != nil {
		return core.Opened{}, err
	}
	if p.Value() == "/" {
		return core.Opened{}, core.Fail(core.NotFile, nil)
	}
	headers := http.Header{"X-Upyun-Folder": {"false"}}
	if o.Range != nil {
		headers.Set("Range", httpcontent.Header(o.Range))
	}
	response, err := d.request(ctx, "GET", target, headers)
	if err != nil {
		return core.Opened{}, err
	}
	if response.StatusCode != 200 && response.StatusCode != 206 && response.StatusCode != 416 {
		return core.Opened{}, errors.Join(d.failure(response), d.closeBody(response.Body))
	}
	opened, err := httpcontent.Open(response, o.Range)
	opened.Metadata.Name = path.Base(p.Value())
	if text := response.Header.Get("ETag"); text != "" {
		opened.Metadata.Evidence.Validator.Tag = text
	}
	if response.Header.Get("x-upyun-file-type") == "folder" {
		err = core.Fail(core.NotFile, nil)
	}
	if err == nil {
		err = o.Selection.Check(opened.Metadata.Evidence)
	}
	return opened, err
}

func (d *Driver) Mkdir(ctx context.Context, p core.NativeAddress) (core.Change, error) {
	return d.mutate(ctx, p, "POST", http.Header{"Folder": {"true"}, "X-Upyun-Folder": {"true"}})
}

func (d *Driver) Remove(ctx context.Context, p core.NativeAddress, policies ...core.RemoveOptions) (core.Change, error) {
	unchanged := core.Change{Effect: core.Unchanged}
	if len(policies) > 1 {
		return unchanged, core.Fail(core.InvalidRequest, nil)
	}
	if len(policies) == 1 {
		if err := policies[0].When.Validate(); err != nil {
			return unchanged, err
		}
		if !policies[0].When.Empty() || policies[0].Target != "" {
			return unchanged, core.Fail(core.UnsupportedOperation, nil)
		}
	}
	return d.mutate(ctx, p, "DELETE", nil)
}

func (d *Driver) mutate(ctx context.Context, p core.NativeAddress, method string, headers http.Header) (core.Change, error) {
	unchanged := core.Change{Effect: core.Unchanged}
	target, err := d.target(ctx, p)
	if err != nil {
		return unchanged, err
	}
	if p.Value() == "/" {
		return unchanged, core.Fail(core.InvalidRequest, nil)
	}
	response, err := d.request(ctx, method, target, headers)
	if err != nil {
		return core.Change{Effect: core.Unknown}, err
	}
	result := core.Change{Effect: core.Unknown}
	if response.StatusCode == http.StatusOK {
		result.Effect = core.Applied
		_, err = d.readResponse(response)
	} else {
		if response.StatusCode >= 400 && response.StatusCode < 500 && response.StatusCode != 408 {
			result.Effect = core.Unchanged
		}
		err = d.failure(response)
	}
	return result, errors.Join(err, d.closeBody(response.Body), ctx.Err())
}

var _ core.Driver = (*Driver)(nil)
var _ core.DirectoryMaker = (*Driver)(nil)
var _ core.Remover = (*Driver)(nil)
