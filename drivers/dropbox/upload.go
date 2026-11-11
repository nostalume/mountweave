package dropbox

import (
	"context"
	"errors"
	"io"
	"net/http"
	"path"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/internal/upload"
)

const maxSingleUpload int64 = 150 << 20

// Create streams an exclusive native upload to the selected parent.
func (d *Driver) Create(ctx context.Context, p core.NativeAddress, input io.Reader, length *int64) (result core.Change, err error) {
	result.Effect = core.Unchanged
	if !d.allowCreate || p.Namespace() != core.TreeNamespace {
		return result, core.Fail(core.UnsupportedOperation, nil)
	}
	if input == nil {
		return result, core.Fail(core.InvalidRequest, nil)
	}
	size := int64(-1)
	if length != nil {
		size = *length
		if size < 0 {
			return result, core.Fail(core.InvalidRequest, nil)
		}
		if size > maxSessionUpload {
			return result, core.Fail(core.CapacityExceeded, nil)
		}
		length = &size
	}
	if err := d.admit(ctx, p.Value()); err != nil {
		return result, err
	}
	if p.Value() == "/" {
		result.Before = core.TargetPresent
		return result, core.Fail(core.AlreadyExists, nil)
	}
	parent, err := d.walk(ctx, path.Dir(p.Value()))
	if err != nil {
		return result, err
	}
	if parent.meta.Kind != core.Directory {
		return result, core.Fail(core.NotDirectory, nil)
	}
	name := path.Base(p.Value())
	arg := map[string]any{"path": childPath(parent, name), "mode": "add", "autorename": false, "strict_conflict": true}
	if size > maxSingleUpload {
		return d.createSession(ctx, parent.ID, name, arg, input, size)
	}
	var body *upload.Body
	var payload io.ReadCloser = http.NoBody
	if size == 0 {
		if err := upload.ReadExact(ctx, input, nil); err != nil {
			return result, err
		}
	} else {
		body = upload.NewBody(ctx, input, length, maxSingleUpload)
		defer body.Finish()
		payload = body
	}
	response, entered, sendErr := d.sendUpload(ctx, "/files/upload", arg, payload, size)
	if entered {
		result.Effect = core.Unknown
	}
	complete, read := true, int64(0)
	var inputErr error
	if body != nil {
		complete, inputErr = body.Finish()
		read = body.PayloadBytes()
	}
	if sendErr != nil {
		return result, errors.Join(sendErr, inputErr)
	}
	defer func() { err = errors.Join(err, d.closeBody(response.Body, false), ctx.Err()) }()
	if response.StatusCode != http.StatusOK {
		_, failure := d.failure(response, uploadFailure)
		if core.CodeOf(failure) == core.AlreadyExists {
			result.Effect = core.Unchanged
		}
		return result, errors.Join(failure, inputErr)
	}
	actual, err := d.uploadedSize(response, parent.ID, name, false)
	if err != nil {
		return result, errors.Join(err, inputErr)
	}
	if actual > read {
		failure := core.Fail(core.InvalidUpstreamResponse, errors.New("native receipt disagrees with the submitted file"))
		return result, errors.Join(failure, inputErr)
	}
	if inputErr != nil {
		return core.Change{Effect: core.Incomplete, Before: core.TargetMissing}, inputErr
	}
	if !complete || actual != read {
		return result, core.Fail(core.InvalidUpstreamResponse, errors.New("native receipt does not confirm the complete input"))
	}
	return core.Change{Effect: core.Applied, Before: core.TargetMissing}, nil
}

func (d *Driver) sendUpload(ctx context.Context, route string, arg any, payload io.ReadCloser, size int64) (*http.Response, bool, error) {
	req, err := contentRequest(ctx, http.MethodPost, route, arg, payload, route != "/files/upload")
	if err != nil {
		return nil, false, err
	}
	req.GetBody = nil
	req.Header.Set("Content-Type", "application/octet-stream")
	req.ContentLength = size
	return d.do(req)
}

func (d *Driver) uploadedSize(response *http.Response, parentID, name string, private bool) (int64, error) {
	var native nativeMetadata
	if err := d.readJSON(response, &native, private); err != nil {
		return 0, err
	}
	created, err := accept(native, true)
	if err != nil {
		return 0, err
	}
	if created.meta.Name != name || created.ID == parentID || created.ID == d.root.ID {
		return 0, core.Fail(core.InvalidUpstreamResponse, errors.New("native receipt disagrees with the submitted file"))
	}
	return *native.Size, nil
}

var _ core.FileCreator = (*Driver)(nil)
