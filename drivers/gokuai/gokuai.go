// Package gokuai mounts an administrator-authorized Gokuai enterprise library.
package gokuai

import (
	"context"
	"crypto/hmac"
	"crypto/sha1"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"maps"
	"math"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/internal/httpcontent"
)

const endpoint = "https://yk3.gokuai.com/m-open/1/file/"

type Options struct {
	// LibraryID and LibrarySecret are issued by an enterprise administrator.
	LibraryID     string
	LibrarySecret string `json:"-"`
	// RootPath is an absolute library path. Empty or "/" mounts the whole library.
	RootPath string
	// Client is optional and is used only for signed control requests.
	Client *http.Client `json:"-"`
	// ContentClient is optional. Its transport must omit account credentials.
	ContentClient    *http.Client `json:"-"`
	MaxResponseBytes int64
}

type Driver struct {
	api, content  http.Client
	library, root string
	secret        []byte
	limit         int64
	closed        atomic.Bool
	cleanup       httpcontent.ResponseCleanup
}

// Open verifies the selected library root. Close any non-nil failed result to
// retry retained response cleanup.
func Open(ctx context.Context, o Options) (*Driver, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if o.LibraryID == "" || strings.ContainsAny(o.LibraryID, "\r\n") || o.LibrarySecret == "" ||
		o.Client != nil && o.Client == o.ContentClient || o.MaxResponseBytes < 0 || o.MaxResponseBytes == math.MaxInt64 {
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
	d := &Driver{api: httpcontent.IsolatedClient(o.Client), content: httpcontent.IsolatedClient(o.ContentClient),
		library: o.LibraryID, secret: []byte(o.LibrarySecret), root: o.RootPath[1:], limit: o.MaxResponseBytes}
	var err error
	if d.root == "" {
		list := listing{driver: d}
		_, err = list.Next(ctx, 1)
		if err == io.EOF {
			err = nil
		}
	} else {
		var f item
		f, err = d.entry(ctx, d.root)
		if err == nil && *f.Dir != 1 {
			err = core.Fail(core.NotDirectory, nil)
		}
	}
	if err != nil {
		d.closed.Store(true)
	}
	return d, err
}

func (d *Driver) path(a core.NativeAddress) (string, error) {
	if a.Namespace() != core.TreeNamespace {
		return "", core.Fail(core.UnsupportedOperation, nil)
	}
	p := a.Value()
	if err := core.ValidatePath(p); err != nil {
		return "", err
	}
	if p == "/" {
		return d.root, nil
	}
	return childPath(d.root, p[1:]), nil
}

func childPath(parent, name string) string {
	if parent == "" {
		return name
	}
	return parent + "/" + name
}

func (d *Driver) check(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if d.closed.Load() {
		return core.Fail(core.MountUnavailable, nil)
	}
	return nil
}

func (d *Driver) request(req *http.Request, client *http.Client) (*http.Response, error) {
	if err := d.check(req.Context()); err != nil {
		return nil, err
	}
	req.Header.Set("Accept-Encoding", "identity")
	resp, err := client.Do(req)
	if err != nil {
		return nil, errors.Join(httpcontent.UpstreamIOError(err), req.Context().Err())
	}
	return resp, nil
}

func (d *Driver) control(ctx context.Context, route string, fields url.Values, out any) (err error) {
	fields.Set("org_client_id", d.library)
	fields.Set("dateline", strconv.FormatInt(time.Now().Unix(), 10))
	// The library API signs sorted, unescaped field values separated by LF.
	values := make([]string, 0, len(fields))
	for _, key := range slices.Sorted(maps.Keys(fields)) {
		values = append(values, fields.Get(key))
	}
	mac := hmac.New(sha1.New, d.secret)
	_, _ = io.WriteString(mac, strings.Join(values, "\n"))
	fields.Set("sign", base64.StdEncoding.EncodeToString(mac.Sum(nil)))
	form := strings.ReplaceAll(fields.Encode(), "+", "%20")
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint+route, strings.NewReader(form))
	if err != nil {
		return core.Fail(core.InvalidRequest, nil)
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := d.request(req, &d.api)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, d.cleanup.CloseBody(resp.Body), ctx.Err()) }()
	if resp.StatusCode == http.StatusOK {
		return httpcontent.DecodeJSON(resp, d.limit, out)
	}
	var native struct {
		Code int `json:"error_code"`
	}
	if resp.StatusCode >= 400 {
		err = httpcontent.DecodeJSON(resp, d.limit, &native)
	}
	return errors.Join(statusError(resp.StatusCode, native.Code), err)
}

func statusError(status, native int) error {
	code := core.UpstreamFailure
	switch {
	case native == 50305:
		code = core.UnsupportedOperation
	case native == 404023:
		// The server could not obtain a download URL for this file.
	case status == http.StatusUnauthorized || status == http.StatusForbidden || native == 40501 || native == 40502:
		code = core.UpstreamAccessDenied
	case status == http.StatusNotFound:
		code = core.NotFound
	case status == http.StatusTooManyRequests:
		code = core.UpstreamThrottled
	case status == http.StatusRequestTimeout || status == http.StatusGatewayTimeout:
		code = core.UpstreamTimeout
	}
	if native != 0 {
		return core.Fail(code, fmt.Errorf("Gokuai returned HTTP %d, code %d", status, native))
	}
	return core.Fail(code, fmt.Errorf("Gokuai returned HTTP %d", status))
}

// Close retries HTTP response cleanup. The clients remain host-owned.
func (d *Driver) Close() error {
	d.closed.Store(true)
	return d.cleanup.Retry()
}

var _ core.Driver = (*Driver)(nil)
