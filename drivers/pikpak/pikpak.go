// Package pikpak mounts a PikPak drive or folder for reading.
package pikpak

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"sync/atomic"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/internal/httpcontent"
)

// ControlHost is the fixed HTTPS host that receives PikPak credentials.
const ControlHost = "api-drive.mypikpak.com"

const filesURL = "https://" + ControlHost + "/drive/v1/files"

// Options selects a concrete folder ID, or the credential's current drive
// root when RootID is empty. Client supplies authentication to ControlHost.
// DeviceID is a stable, 32-character hexadecimal client identifier.
// ContentClient is optional and must not add account credentials. Both clients
// borrow the host's transport policy, including routing for signed HTTP URLs.
type Options struct {
	RootID, DeviceID      string
	Client, ContentClient *http.Client
	MaxResponseBytes      int64 // Zero uses 4 MiB per control response.
}

type Driver struct {
	root, device string
	api, content http.Client
	limit        int64
	closed       atomic.Bool
	cleanup      httpcontent.ResponseCleanup
}

// Open verifies the selected directory. A non-nil result with an error owns
// failed response cleanup and must be closed by the caller.
func Open(ctx context.Context, o Options) (*Driver, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !ValidLocation(o.RootID, o.DeviceID) ||
		o.Client == nil || o.Client == o.ContentClient || o.MaxResponseBytes < 0 || o.MaxResponseBytes == math.MaxInt64 {
		return nil, core.Fail(core.InvalidRequest, nil)
	}
	if o.MaxResponseBytes == 0 {
		o.MaxResponseBytes = 4 << 20
	}
	d := &Driver{root: o.RootID, device: o.DeviceID, api: httpcontent.IsolatedClient(o.Client),
		content: httpcontent.IsolatedClient(o.ContentClient), limit: o.MaxResponseBytes}
	d.content.CheckRedirect = d.cleanup.RedirectDownload
	var err error
	if d.root == "" {
		scan := listing{driver: d}
		_, err = scan.next(ctx)
		if err == io.EOF {
			err = ctx.Err()
		}
	} else {
		var folder nativeItem
		err = d.getJSON(ctx, fileURL(d.root, "ALL"), &folder)
		if err == nil && folder.ID != d.root {
			err = core.Fail(core.InvalidUpstreamResponse, errors.New("PikPak returned a different root ID"))
		}
		if err == nil && folder.Kind != "drive#folder" {
			err = core.Fail(core.NotDirectory, nil)
		}
	}
	if err != nil {
		if cleanupErr := d.Close(); cleanupErr != nil {
			return d, errors.Join(err, cleanupErr)
		}
		return nil, err
	}
	return d, nil
}

// ValidLocation checks the saved folder and device identifiers without a request.
func ValidLocation(rootID, deviceID string) bool {
	if len(deviceID) != 32 || rootID != "" && !validID(rootID) {
		return false
	}
	var decoded [16]byte
	_, err := hex.Decode(decoded[:], []byte(deviceID))
	return err == nil
}

func validID(id string) bool { return id != "*" && core.ValidName(id) }

func fileURL(id, usage string) string { return filesURL + "/" + url.PathEscape(id) + "?usage=" + usage }

func (d *Driver) check(ctx context.Context, address core.NativeAddress) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if d.closed.Load() {
		return core.Fail(core.MountUnavailable, nil)
	}
	if address.Namespace() != core.TreeNamespace {
		return core.Fail(core.UnsupportedOperation, nil)
	}
	return core.ValidatePath(address.Value())
}

func (d *Driver) getJSON(ctx context.Context, target string, output any) (err error) {
	if d.closed.Load() {
		return core.Fail(core.MountUnavailable, nil)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return core.Fail(core.InvalidUpstreamResponse, nil)
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("X-Device-Id", d.device)
	res, err := d.api.Do(req)
	if err != nil {
		return errors.Join(core.Fail(core.UpstreamFailure, httpcontent.ControlIOError(err)), ctx.Err())
	}
	defer func() { err = errors.Join(err, d.cleanup.CloseControlBody(res.Body), ctx.Err()) }()
	if res.StatusCode != http.StatusOK {
		return responseError(res.StatusCode)
	}
	return httpcontent.DecodeJSON(res, d.limit, output)
}

func responseError(status int) error {
	code := core.UpstreamFailure
	switch status {
	case http.StatusUnauthorized, http.StatusForbidden:
		code = core.UpstreamAccessDenied
	case http.StatusNotFound:
		code = core.NotFound
	case http.StatusTooManyRequests:
		code = core.UpstreamThrottled
	}
	return core.Fail(code, fmt.Errorf("PikPak returned HTTP %d", status))
}

// Close retires the Driver and retries its failed response cleanup. The clients
// and transports remain owned by the host.
func (d *Driver) Close() error {
	d.closed.Store(true)
	return d.cleanup.Retry()
}

var _ core.Driver = (*Driver)(nil)
