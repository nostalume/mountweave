// Package pan115 connects ordinary 115 Open account files to Mountweave.
package pan115

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"mime/multipart"
	"net/http"
	"net/url"
	"sync/atomic"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/internal/httpcontent"
)

const (
	endpoint      = "https://proapi.115.com"
	downloadRoute = "/open/ufile/downurl"
)

// Options selects either an account-root-relative RootPath or a native RootID.
// An empty selection means ID 0. Client is a borrowed, authenticated control
// client; its credential owner handles token rotation and atomic persistence.
// ContentClient is independent and must not inject account credentials.
type Options struct {
	RootID, RootPath      string
	Client, ContentClient *http.Client
	MaxResponseBytes      int64
	MaxResolvePages       int
}

type Driver struct {
	root         item
	api, content http.Client
	limit        int64
	resolvePages int
	closed       atomic.Bool
	cleanup      httpcontent.ResponseCleanup
}

// Open validates settings, resolves the root and confirms readable directory
// state. A non-nil result on error retains cleanup and must be closed.
func Open(ctx context.Context, o Options) (*Driver, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if o.Client == nil || o.Client == o.ContentClient || (o.RootID != "" && o.RootPath != "") ||
		o.MaxResponseBytes < 0 || o.MaxResponseBytes == math.MaxInt64 || o.MaxResolvePages < 0 {
		return nil, core.Fail(core.InvalidRequest, nil)
	}
	if o.RootID != "" && !httpcontent.ValidDecimalID(o.RootID) {
		return nil, core.Fail(core.InvalidRequest, nil)
	}
	if o.RootPath != "" {
		if err := core.ValidatePath(o.RootPath); err != nil {
			return nil, err
		}
	}
	if o.MaxResponseBytes == 0 {
		o.MaxResponseBytes = 4 << 20
	}
	d := &Driver{root: item{ID: "0", Category: "0", meta: core.Metadata{Kind: core.Directory}}, api: httpcontent.IsolatedClient(o.Client),
		content: httpcontent.IsolatedClient(o.ContentClient), limit: o.MaxResponseBytes, resolvePages: o.MaxResolvePages}
	d.content.CheckRedirect = d.cleanup.RedirectDownload
	var err error
	if o.RootPath != "" && o.RootPath != "/" {
		d.root, err = d.resolve(ctx, o.RootPath)
	} else if o.RootID != "" && o.RootID != "0" {
		d.root, err = d.info(ctx, o.RootID)
	}
	if err == nil && d.root.Encrypted != nil && *d.root.Encrypted != 0 {
		err = core.Fail(core.UnsupportedOperation, errors.New("encrypted directory contents are not available"))
	}
	if err == nil && d.root.Category != "0" {
		err = core.Fail(core.NotDirectory, nil)
	}
	if err == nil {
		var p page
		p, err = d.readPage(ctx, string(d.root.ID), 0, 1)
		if err == nil && d.root.ID != "0" && !p.readable && (d.root.Encrypted == nil || *d.root.Encrypted != 0) {
			err = core.Fail(core.InvalidUpstreamResponse, errors.New("directory encryption state was not reported"))
		}
	}
	d.root.Encrypted = nil
	if err != nil {
		d.closed.Store(true)
	}
	return d, err
}

type nativeID = httpcontent.DecimalID

func (d *Driver) check(ctx context.Context, p core.NativeAddress) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if d.closed.Load() {
		return core.Fail(core.MountUnavailable, nil)
	}
	if p.Namespace() != core.TreeNamespace {
		return core.Fail(core.UnsupportedOperation, nil)
	}
	return core.ValidatePath(p.Value())
}

func nativeFailure(err error, private bool) error {
	if !private {
		return httpcontent.ControlIOError(err)
	}
	if errors.Is(err, httpcontent.ErrContentRedirect) {
		return httpcontent.ErrContentRedirect
	}
	if failure := httpcontent.PrivateIOError(err); failure != httpcontent.ErrPrivateIO {
		return failure
	}
	return errors.New("115 content I/O failed")
}

func (d *Driver) closeBody(body io.Closer, private bool) error {
	if private {
		return d.cleanup.CloseBody(body)
	}
	return d.cleanup.CloseControlBody(body)
}

// Close retries owned response cleanup after Core has joined active uses.
func (d *Driver) Close() error {
	d.closed.Store(true)
	return d.cleanup.Retry()
}

func (d *Driver) exchange(client *http.Client, req *http.Request, private bool) (*http.Response, error) {
	response, err := client.Do(req)
	if err == nil {
		return response, nil
	}
	err = nativeFailure(err, private)
	if response != nil {
		err = errors.Join(err, d.closeBody(response.Body, private))
	}
	return nil, errors.Join(core.Fail(core.UpstreamFailure, err), req.Context().Err())
}

type envelope struct {
	State   *bool  `json:"state"`
	Code    int64  `json:"code"`
	Errno   int64  `json:"errno"`
	Message string `json:"message"`
}

func (d *Driver) call(ctx context.Context, method, route string, query url.Values, fields map[string]string, out any) (err error) {
	if err := ctx.Err(); err != nil {
		return err
	}
	if d.closed.Load() {
		return core.Fail(core.MountUnavailable, nil)
	}
	var body bytes.Buffer
	contentType := ""
	if fields != nil {
		writer := multipart.NewWriter(&body)
		for k, v := range fields {
			if err := writer.WriteField(k, v); err != nil {
				return err
			}
		}
		if err := writer.Close(); err != nil {
			return err
		}
		contentType = writer.FormDataContentType()
	}
	target := endpoint + route
	if len(query) != 0 {
		target += "?" + query.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, method, target, &body)
	if err != nil {
		return core.Fail(core.InvalidRequest, err)
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Accept-Encoding", "identity")
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	private := route == downloadRoute
	response, err := d.exchange(&d.api, req, private)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, d.closeBody(response.Body, private)) }()
	if encoding := response.Header.Get("Content-Encoding"); encoding != "" && encoding != "identity" {
		return core.Fail(core.InvalidUpstreamResponse, nil)
	}
	data, err := io.ReadAll(io.LimitReader(response.Body, d.limit+1))
	if int64(len(data)) > d.limit {
		return core.Fail(core.ResponseBudgetExceeded, nativeFailure(err, private))
	}
	if err != nil {
		return core.Fail(core.UpstreamFailure, nativeFailure(err, private))
	}
	var status envelope
	decodeErr := json.Unmarshal(data, &status)
	if response.StatusCode != 200 || (decodeErr == nil && status.State != nil && !*status.State) {
		code := core.UpstreamFailure
		switch response.StatusCode {
		case 401, 403:
			code = core.UpstreamAccessDenied
		case 404:
			code = core.NotFound
		case 429, 503:
			code = core.UpstreamThrottled
		}
		detail := fmt.Sprintf("115 returned HTTP %d, code %d, errno %d", response.StatusCode, status.Code, status.Errno)
		if !private {
			detail += ": " + status.Message
		}
		return core.Fail(code, errors.New(detail))
	}
	if decodeErr != nil || status.State == nil || !*status.State {
		return core.Fail(core.InvalidUpstreamResponse, nativeFailure(decodeErr, private))
	}
	if err := json.Unmarshal(data, out); err != nil {
		return core.Fail(core.InvalidUpstreamResponse, nativeFailure(err, private))
	}
	return nil
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
	if err := d.check(ctx, p); err != nil {
		return core.Metadata{}, err
	}
	file, err := d.resolve(ctx, p.Value())
	if err != nil {
		return core.Metadata{}, err
	}
	if p.Value() == "/" && file.ID == "0" {
		if _, err = d.readPage(ctx, "0", 0, 1); err != nil {
			return core.Metadata{}, err
		}
	}
	if p.Value() == "/" && file.ID != "0" {
		file, err = d.info(ctx, string(file.ID))
		if err != nil {
			return core.Metadata{}, err
		}
	}
	meta := file.meta
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
	if err := d.check(ctx, p); err != nil {
		return core.Opened{}, err
	}
	file, err := d.resolve(ctx, p.Value())
	if err != nil {
		return core.Opened{}, err
	}
	if file.Encrypted != nil && *file.Encrypted != 0 {
		return core.Opened{}, core.Fail(core.UnsupportedOperation, errors.New("encrypted file contents are not available"))
	}
	if file.Category != "1" {
		return core.Opened{}, core.Fail(core.NotFile, nil)
	}
	if selectionErr != nil {
		return core.Opened{}, selectionErr
	}
	if file.Pick == "" {
		return core.Opened{}, core.Fail(core.InvalidUpstreamResponse, errors.New("file omitted download capability reference"))
	}
	var out struct {
		Data map[string]struct {
			Size *int64 `json:"file_size"`
			URL  struct {
				URL string `json:"url"`
			} `json:"url"`
		} `json:"data"`
	}
	if err = d.call(ctx, "POST", downloadRoute, nil, map[string]string{"pick_code": file.Pick}, &out); err != nil {
		return core.Opened{}, err
	}
	capability, ok := out.Data[string(file.ID)]
	if !ok || (capability.Size != nil && *capability.Size < 0) {
		return core.Opened{}, core.Fail(core.InvalidUpstreamResponse, errors.New("download receipt differs from requested file"))
	}
	u, err := url.Parse(capability.URL.URL)
	if err != nil || !httpcontent.IsHTTPS(u) {
		return core.Opened{}, core.Fail(core.InvalidUpstreamResponse, errors.New("invalid content capability"))
	}
	req, err := http.NewRequestWithContext(ctx, "GET", u.String(), nil)
	if err != nil {
		return core.Opened{}, core.Fail(core.InvalidUpstreamResponse, nil)
	}
	req.Header.Set("Accept-Encoding", "identity")
	if o.Range != nil {
		req.Header.Set("Range", httpcontent.Header(o.Range))
	}
	response, err := d.exchange(&d.content, req, true)
	if err != nil {
		return core.Opened{}, err
	}
	var opened core.Opened
	status := response.StatusCode
	switch {
	case status == http.StatusRequestedRangeNotSatisfiable && o.Range == nil:
		err = core.Fail(core.InvalidUpstreamResponse, errors.New("range rejected without a range request"))
	case status == http.StatusOK || status == http.StatusPartialContent || status == http.StatusRequestedRangeNotSatisfiable:
		if capability.Size == nil {
			opened, err = httpcontent.Open(response, o.Range)
		} else {
			opened, err = httpcontent.OpenSized(response, o.Range, *capability.Size)
		}
	default:
		err = core.Fail(core.UpstreamFailure, fmt.Errorf("115 content returned HTTP %d", status))
	}
	if err != nil {
		return core.Opened{}, errors.Join(err, d.closeBody(response.Body, true))
	}
	opened.Metadata.Name = file.Name
	return opened, nil
}

var _ core.Driver = (*Driver)(nil)
