// Package pdsshare exposes Alibaba Cloud PDS Standard Mode drives and shares.
package pdsshare

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"time"
	"unicode/utf8"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/internal/httpcontent"
)

// Options identifies one PDS tenant and one share. Password is optional.
// The two HTTP clients are borrowed and must not add account credentials.
type Options struct {
	DomainID         string
	ShareID          string
	Password         string
	Client           *http.Client
	ContentClient    *http.Client
	MaxResponseBytes int64
	MaxResolvePages  int
}

// DriveOptions identifies one drive in a PDS tenant. Client supplies OAuth or
// AccessKey authentication. ContentClient must not add account credentials.
type DriveOptions struct {
	DomainID         string
	DriveID          string
	RootID           string // Empty selects the drive root.
	Client           *http.Client
	ContentClient    *http.Client
	MaxResponseBytes int64
	MaxResolvePages  int
}

type resourceScope struct {
	DriveID string `json:"drive_id,omitempty"`
	ShareID string `json:"share_id,omitempty"`
}

type fileRequest struct {
	resourceScope
	FileID string `json:"file_id"`
}

type Driver struct {
	scope         resourceScope
	root          string
	domain        string
	password      string
	responseLimit int64
	resolvePages  int
	api, content  http.Client
	endpoint      string
	renew         chan struct{}
	token         string
	expiry        time.Time
	closed        atomic.Bool
	cleanup       httpcontent.ResponseCleanup
}

// Open verifies access to the selected share. Close a non-nil failed Driver
// to retry response cleanup.
func Open(ctx context.Context, options Options) (*Driver, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if options.ShareID == "" || !utf8.ValidString(options.ShareID) || !utf8.ValidString(options.Password) {
		return nil, core.Fail(core.InvalidRequest, nil)
	}
	d, err := newDriver(options.DomainID, options.Client, options.ContentClient, options.MaxResponseBytes, options.MaxResolvePages)
	if err != nil {
		return nil, err
	}
	d.scope.ShareID, d.root, d.password = options.ShareID, "root", options.Password
	d.renew = make(chan struct{}, 1)
	if _, err = d.page(ctx, d.root, "", 1); err != nil {
		d.closed.Store(true)
	}
	return d, err
}

// OpenDrive verifies access to a Standard Mode drive or a folder in it.
// Close a non-nil failed Driver to retry response cleanup.
func OpenDrive(ctx context.Context, options DriveOptions) (*Driver, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if options.DriveID == "" || !utf8.ValidString(options.DriveID) || !utf8.ValidString(options.RootID) ||
		options.Client == nil || options.Client == options.ContentClient {
		return nil, core.Fail(core.InvalidRequest, nil)
	}
	d, err := newDriver(options.DomainID, options.Client, options.ContentClient, options.MaxResponseBytes, options.MaxResolvePages)
	if err != nil {
		return nil, err
	}
	d.scope.DriveID, d.root = options.DriveID, options.RootID
	if d.root == "" {
		d.root = "root"
	}
	if d.root == "root" {
		_, err = d.page(ctx, d.root, "", 1)
	} else {
		var folder item
		err = d.post(ctx, "/v2/file/get", "", fileRequest{resourceScope: d.scope, FileID: d.root}, &folder)
		if err == nil {
			switch {
			case folder.ID != d.root || !d.inScope(folder):
				err = core.Fail(core.InvalidUpstreamResponse, errors.New("PDS did not confirm the selected drive and folder"))
			case folder.Type != "folder":
				err = core.Fail(core.NotDirectory, nil)
			}
		}
	}
	if err != nil {
		d.closed.Store(true)
	}
	return d, err
}

func newDriver(domain string, client, content *http.Client, responseLimit int64, resolvePages int) (*Driver, error) {
	if !domainLabel(domain) || responseLimit < 0 || responseLimit == math.MaxInt64 || resolvePages < 0 {
		return nil, core.Fail(core.InvalidRequest, nil)
	}
	if responseLimit == 0 {
		responseLimit = 4 << 20
	}
	return &Driver{domain: domain, endpoint: "https://" + domain + ".api.aliyunpds.com",
		api: httpcontent.IsolatedClient(client), content: httpcontent.IsolatedClient(content),
		responseLimit: responseLimit, resolvePages: resolvePages}, nil
}

func domainLabel(value string) bool {
	if len(value) == 0 || len(value) > 63 || value[0] == '-' || value[len(value)-1] == '-' {
		return false
	}
	for _, c := range value {
		if c != '-' && (c < 'a' || c > 'z') && (c < 'A' || c > 'Z') && (c < '0' || c > '9') {
			return false
		}
	}
	return true
}

func (d *Driver) credential(ctx context.Context) (string, error) {
	if d.scope.ShareID == "" {
		return "", ctx.Err()
	}
	select {
	case d.renew <- struct{}{}:
		defer func() { <-d.renew }()
	case <-ctx.Done():
		return "", ctx.Err()
	}
	if d.token != "" && time.Now().Before(d.expiry) {
		return d.token, nil
	}
	var issued struct {
		Token  string    `json:"share_token"`
		Expiry time.Time `json:"expire_time"`
	}
	request := struct {
		Share    string `json:"share_id"`
		Password string `json:"share_pwd,omitempty"`
	}{d.scope.ShareID, d.password}
	if err := d.post(ctx, "/v2/share_link/get_share_token", "", request, &issued); err != nil {
		return "", err
	}
	if issued.Token == "" || strings.IndexFunc(issued.Token, func(c rune) bool { return c < 0x21 || c > 0x7e }) >= 0 ||
		!time.Now().Before(issued.Expiry) {
		return "", core.Fail(core.InvalidUpstreamResponse, errors.New("PDS did not issue a live share token"))
	}
	d.token, d.expiry = issued.Token, issued.Expiry
	return d.token, nil
}

func (d *Driver) post(ctx context.Context, route, token string, input, output any) (err error) {
	if err := ctx.Err(); err != nil {
		return err
	}
	if d.closed.Load() {
		return core.Fail(core.MountUnavailable, nil)
	}
	payload, err := json.Marshal(input)
	if err != nil {
		return core.Fail(core.InvalidRequest, err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, d.endpoint+route, bytes.NewReader(payload))
	if err != nil {
		return core.Fail(core.InvalidRequest, err)
	}
	req.GetBody = nil
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept-Encoding", "identity")
	if token != "" {
		req.Header.Set("X-Share-Token", token)
	}
	resp, err := d.exchange(&d.api, req)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, d.cleanup.CloseBody(resp.Body)) }()
	if resp.StatusCode != http.StatusOK {
		var failure struct {
			Code string `json:"code"`
		}
		decodeErr := httpcontent.DecodeJSON(resp, d.responseLimit, &failure)
		return errors.Join(responseError(resp.StatusCode, failure.Code), decodeErr)
	}
	if err := httpcontent.DecodeJSON(resp, d.responseLimit, output); err != nil {
		return err
	}
	return ctx.Err()
}

func responseError(status int, reason string) error {
	code := core.UpstreamFailure
	switch status {
	case http.StatusUnauthorized, http.StatusForbidden:
		code = core.UpstreamAccessDenied
	case http.StatusNotFound:
		code = core.NotFound
	case http.StatusTooManyRequests:
		code = core.UpstreamThrottled
	}
	if reason == "InvalidResource.SharePwd" {
		return core.Fail(core.UpstreamAccessDenied, errors.New("PDS rejected the share password"))
	}
	return core.Fail(code, fmt.Errorf("PDS returned HTTP %d", status))
}

func (d *Driver) exchange(client *http.Client, req *http.Request) (*http.Response, error) {
	resp, err := client.Do(req)
	if err == nil {
		return resp, nil
	}
	err = httpcontent.UpstreamIOError(err)
	if resp != nil && resp.Body != nil {
		err = errors.Join(err, d.cleanup.CloseBody(resp.Body))
	}
	return nil, errors.Join(err, req.Context().Err())
}

// Close retries failed response cleanup without closing either transport.
func (d *Driver) Close() error {
	d.closed.Store(true)
	return d.cleanup.Retry()
}

func (d *Driver) Open(ctx context.Context, address core.NativeAddress, options core.OpenOptions) (core.Opened, error) {
	if err := options.Validate(); err != nil {
		return core.Opened{}, err
	}
	if options.Mode != core.StreamRead || options.NoFollow || options.Selection.Version != "" {
		return core.Opened{}, core.Fail(core.UnsupportedOperation, nil)
	}
	condition := options.Selection.Check(core.Evidence{})
	if condition != nil && core.CodeOf(condition) != core.PreconditionFailed {
		return core.Opened{}, condition
	}
	file, err := d.lookup(ctx, address)
	if err != nil {
		return core.Opened{}, err
	}
	if file.Type != "file" {
		return core.Opened{}, core.Fail(core.NotFile, nil)
	}
	if condition != nil {
		return core.Opened{}, condition
	}
	token, err := d.credential(ctx)
	if err != nil {
		return core.Opened{}, err
	}
	fields := fileRequest{resourceScope: d.scope, FileID: file.ID}
	var download struct {
		URL    string `json:"url"`
		Method string `json:"method"`
		Size   *int64 `json:"size"`
	}
	if err := d.post(ctx, "/v2/file/get_download_url", token, fields, &download); err != nil {
		return core.Opened{}, err
	}
	u, err := url.Parse(download.URL)
	if err != nil || u.Scheme != "https" || u.Hostname() == "" || u.User != nil || u.Fragment != "" || u.Opaque != "" ||
		download.Method != "" && download.Method != http.MethodGet || download.Size != nil && *download.Size < 0 {
		return core.Opened{}, core.Fail(core.InvalidUpstreamResponse, errors.New("invalid PDS download capability"))
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return core.Opened{}, core.Fail(core.InvalidUpstreamResponse, nil)
	}
	req.Header.Set("Accept-Encoding", "identity")
	if value := httpcontent.Header(options.Range); value != "" {
		req.Header.Set("Range", value)
	}
	resp, err := d.exchange(&d.content, req)
	if err != nil {
		return core.Opened{}, err
	}
	switch resp.StatusCode {
	case http.StatusOK, http.StatusPartialContent, http.StatusRequestedRangeNotSatisfiable:
		if download.Size != nil {
			return d.cleanup.OpenSizedBody(resp, options.Range, file.Name, *download.Size)
		}
		return d.cleanup.OpenBody(resp, options.Range, file.Name)
	default:
		return core.Opened{}, errors.Join(responseError(resp.StatusCode, ""), d.cleanup.CloseBody(resp.Body))
	}
}

var _ core.Driver = (*Driver)(nil)
