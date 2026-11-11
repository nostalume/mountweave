// Package wps365 mounts files from a WPS 365 drive through its V7 API.
package wps365

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"sync/atomic"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/internal/httpcontent"
)

const endpoint = "https://openapi.wps.cn"

// Options selects an authorized drive and an optional folder ID. Client supplies
// WPS authorization and any KSO-1 signing. ContentClient must be credential-free.
type Options struct {
	DriveID, RootID       string
	Client, ContentClient *http.Client
	MaxResponseBytes      int64
	AllowMkdir            bool // Permit creating directories through the control client.
	AllowRemove           bool // Permit removing ordinary files through the control client.
}

type Driver struct {
	drive, root  string
	api, content http.Client
	limit        int64
	allowMkdir   bool
	allowRemove  bool
	closed       atomic.Bool
	cleanup      httpcontent.ResponseCleanup
}

// Open verifies the selected root with the V7 API. If it returns a non-nil
// driver and an error, the caller still owns its cleanup.
func Open(ctx context.Context, o Options) (*Driver, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if o.RootID == "" {
		o.RootID = "0"
	}
	if !core.ValidName(o.DriveID) || !core.ValidName(o.RootID) || o.Client == nil ||
		o.Client == o.ContentClient || o.MaxResponseBytes < 0 || o.MaxResponseBytes == math.MaxInt64 {
		return nil, core.Fail(core.InvalidRequest, nil)
	}
	if o.MaxResponseBytes == 0 {
		o.MaxResponseBytes = 4 << 20
	}
	d := &Driver{drive: o.DriveID, root: o.RootID, api: *o.Client, limit: o.MaxResponseBytes,
		allowMkdir: o.AllowMkdir, allowRemove: o.AllowRemove}
	if o.ContentClient != nil {
		d.content = *o.ContentClient
	}
	noRedirect := func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	d.api.Jar, d.content.Jar = nil, nil
	d.api.CheckRedirect, d.content.CheckRedirect = noRedirect, noRedirect
	if err := d.checkRoot(ctx); err != nil {
		d.closed.Store(true)
		return d, err
	}
	return d, nil
}

func (d *Driver) checkRoot(ctx context.Context) error {
	if d.root == "0" {
		_, _, err := d.children(ctx, d.root, "", 1)
		return err
	}
	var root item
	if err := d.get(ctx, d.fileURL(d.root)+"/meta", &root); err != nil {
		return err
	}
	if root.ID != d.root || root.Drive != d.drive {
		return core.Fail(core.InvalidUpstreamResponse, errors.New("root identity does not match the requested folder"))
	}
	if root.Type != "folder" {
		return core.Fail(core.NotDirectory, nil)
	}
	return nil
}

func (d *Driver) fileURL(id string) string {
	return endpoint + "/v7/drives/" + url.PathEscape(d.drive) + "/files/" + url.PathEscape(id)
}

func (d *Driver) active(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if d.closed.Load() {
		return core.Fail(core.MountUnavailable, nil)
	}
	return nil
}

func (d *Driver) check(ctx context.Context, p core.NativeAddress) error {
	if err := d.active(ctx); err != nil {
		return err
	}
	if p.Namespace() != core.TreeNamespace {
		return core.Fail(core.UnsupportedOperation, nil)
	}
	return core.ValidatePath(p.Value())
}

func (d *Driver) get(ctx context.Context, target string, out any) error {
	if err := d.active(ctx); err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return core.Fail(core.InvalidRequest, nil)
	}
	_, err = d.request(req, out)
	return err
}

// request confirms an API envelope independently of response cleanup.
func (d *Driver) request(req *http.Request, out any) (confirmed bool, err error) {
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Accept-Encoding", "identity")
	response, err := d.api.Do(req)
	if err != nil {
		return false, errors.Join(exchangeFailure(httpcontent.PrivateIOError(err)), req.Context().Err())
	}
	defer func() { err = errors.Join(err, exchangeFailure(d.cleanup.CloseBody(response.Body))) }()
	if response.StatusCode != http.StatusOK && req.Method == http.MethodGet {
		return false, upstream(response.StatusCode, 0)
	}
	data, err := httpcontent.ReadIdentity(response, d.limit)
	if err != nil {
		return false, err
	}
	var reply struct {
		Code *int64          `json:"code"`
		Data json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(data, &reply); err != nil || reply.Code == nil {
		if response.StatusCode != http.StatusOK {
			return false, upstream(response.StatusCode, 0)
		}
		return false, core.Fail(core.InvalidUpstreamResponse, err)
	}
	if response.StatusCode != http.StatusOK || *reply.Code != 0 {
		return false, upstream(response.StatusCode, *reply.Code)
	}
	if out == nil {
		return true, nil
	}
	if len(reply.Data) == 0 || string(reply.Data) == "null" {
		return false, core.Fail(core.InvalidUpstreamResponse, nil)
	}
	if err := json.Unmarshal(reply.Data, out); err != nil {
		return false, core.Fail(core.InvalidUpstreamResponse, err)
	}
	return true, nil
}

func upstream(status int, native int64) error {
	code := core.UpstreamFailure
	switch {
	case native == 400008007:
		code = core.AlreadyExists
	case status == 401 || status == 403 || native/1_000_000 == 401 || native/1_000_000 == 403:
		code = core.UpstreamAccessDenied
	case status == 404 || native/1_000_000 == 404 || native == 400008009 || native == 400008011:
		code = core.NotFound
	case status == 429 || status == 503 || native/1_000_000 == 429:
		code = core.UpstreamThrottled
	}
	return core.Fail(code, fmt.Errorf("WPS returned HTTP %d, code %d", status, native))
}

func exchangeFailure(err error) error {
	if err == nil {
		return nil
	}
	return core.Fail(core.UpstreamFailure, err)
}

// Close retries failed response cleanup. Both clients are borrowed.
func (d *Driver) Close() error {
	d.closed.Store(true)
	return exchangeFailure(d.cleanup.Retry())
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
	if p.Value() == "/" {
		if err := d.checkRoot(ctx); err != nil {
			return core.Metadata{}, err
		}
		return core.Metadata{Kind: core.Directory}, selection.Check(core.Evidence{})
	}
	file, err := d.resolve(ctx, p.Value())
	if err != nil {
		return core.Metadata{}, err
	}
	return file.metadata(), selection.Check(core.Evidence{})
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
	if file.Type != "file" {
		return core.Opened{}, core.Fail(core.NotFile, nil)
	}
	if selectionErr != nil {
		return core.Opened{}, selectionErr
	}
	var download struct {
		URL string `json:"url"`
	}
	if err := d.get(ctx, d.fileURL(file.ID)+"/download", &download); err != nil {
		return core.Opened{}, err
	}
	u, err := url.Parse(download.URL)
	if err != nil || u.Scheme != "https" || u.Hostname() == "" || u.User != nil || u.Opaque != "" || u.Fragment != "" {
		return core.Opened{}, core.Fail(core.InvalidUpstreamResponse, errors.New("invalid WPS download address"))
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return core.Opened{}, core.Fail(core.InvalidUpstreamResponse, nil)
	}
	req.Header.Set("Accept-Encoding", "identity")
	if value := httpcontent.Header(o.Range); value != "" {
		req.Header.Set("Range", value)
	}
	response, err := d.content.Do(req)
	if err != nil {
		return core.Opened{}, errors.Join(exchangeFailure(httpcontent.PrivateIOError(err)), ctx.Err())
	}
	switch response.StatusCode {
	case http.StatusOK, http.StatusPartialContent, http.StatusRequestedRangeNotSatisfiable:
		return d.cleanup.OpenBody(response, o.Range, file.Name)
	default:
		return core.Opened{}, errors.Join(upstream(response.StatusCode, 0), d.cleanup.CloseBody(response.Body))
	}
}

var _ core.Driver = (*Driver)(nil)
