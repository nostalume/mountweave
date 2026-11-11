package webdav

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strconv"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/internal/upload"
)

// Create streams a new resource with a native absence condition. It does not
// promise invisible staging or delete a possibly replaced name after failure.
func (d *Driver) Create(ctx context.Context, p core.NativeAddress, input io.Reader, length *int64) (core.Change, error) {
	unchanged := core.Change{Effect: core.Unchanged}
	if err := ctx.Err(); err != nil {
		return unchanged, err
	}
	if input == nil || length != nil && *length < 0 || p.Value() == "/" {
		return unchanged, core.Fail(core.InvalidRequest, nil)
	}
	target, err := d.target(p)
	if err != nil {
		return unchanged, err
	}
	if err = ctx.Err(); err != nil {
		return unchanged, err
	}
	body := upload.NewBody(ctx, input, length)
	headers := http.Header{"If-None-Match": {"*"}, "Content-Type": {"application/octet-stream"}}
	if length != nil {
		headers.Set("Content-Length", strconv.FormatInt(*length, 10))
		if *length == 0 {
			if _, err = body.Read(make([]byte, 1)); err != io.EOF {
				body.Finish()
				return unchanged, err
			}
		}
	}
	response, requestErr := d.request(ctx, http.MethodPut, target, body, headers)
	complete, inputErr := body.Finish()
	if requestErr != nil {
		return core.Change{Effect: core.Unknown}, errors.Join(requestErr, inputErr)
	}
	err = errors.Join(inputErr, ctx.Err(), d.closeBody(response.Body))
	if response.StatusCode == http.StatusCreated {
		if inputErr != nil {
			return core.Change{Effect: core.Incomplete}, err
		}
		if !complete {
			return core.Change{Effect: core.Unknown}, errors.Join(err, core.Fail(core.InvalidUpstreamResponse, errors.New("PUT did not consume complete input")))
		}
		return core.Change{Effect: core.Applied}, err
	}
	failure := statusError(http.MethodPut, response.StatusCode)
	if response.StatusCode == http.StatusPreconditionFailed {
		return unchanged, errors.Join(core.Fail(core.AlreadyExists, failure), err)
	}
	switch response.StatusCode {
	case 401, 403, 404, 405, 409, 415, 423, 429:
		return unchanged, errors.Join(failure, err)
	}
	return core.Change{Effect: core.Unknown}, errors.Join(failure, err)
}

func (d *Driver) Mkdir(ctx context.Context, p core.NativeAddress) (core.Change, error) {
	unchanged := core.Change{Effect: core.Unchanged}
	if err := ctx.Err(); err != nil {
		return unchanged, err
	}
	target, err := d.target(p)
	if err != nil {
		return unchanged, err
	}
	if p.Value() == "/" {
		return unchanged, core.Fail(core.AlreadyExists, nil)
	}
	response, err := d.request(ctx, "MKCOL", target, nil, nil)
	if err != nil {
		return core.Change{Effect: core.Unknown}, err
	}
	err = errors.Join(ctx.Err(), d.closeBody(response.Body))
	if response.StatusCode == http.StatusCreated {
		return core.Change{Effect: core.Applied}, err
	}
	failure := statusError("MKCOL", response.StatusCode)
	if response.StatusCode == http.StatusMethodNotAllowed {
		failure = core.Fail(core.AlreadyExists, failure)
	}
	if response.StatusCode >= 400 && response.StatusCode < 500 {
		return unchanged, errors.Join(failure, err)
	}
	return core.Change{Effect: core.Unknown}, errors.Join(failure, err)
}

// RemoveSubtree maps an explicit recursive grant to one native DELETE.
func (d *Driver) RemoveSubtree(ctx context.Context, p core.NativeAddress, _ core.TreeOptions) (core.Change, error) {
	result := core.Change{Effect: core.Unchanged}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	target, err := d.target(p)
	if err != nil {
		return result, err
	}
	if p.Value() == "/" {
		return result, core.Fail(core.UnsupportedOperation, nil)
	}
	response, err := d.request(ctx, http.MethodDelete, target, nil, http.Header{"Depth": {"infinity"}})
	if err != nil {
		return core.Change{Effect: core.Unknown}, err
	}
	err = errors.Join(ctx.Err(), d.closeBody(response.Body))
	status := response.StatusCode
	switch {
	case status == http.StatusOK || status == http.StatusNoContent:
		result.Effect = core.Applied
		return result, err
	case status >= 400 && status < 500 && status != http.StatusRequestTimeout:
		// A top-level WebDAV client error reports complete refusal.
	default:
		// A 207 omits default successes, so it does not prove which changes occurred.
		result.Effect = core.Unknown
	}
	return result, errors.Join(statusError(http.MethodDelete, status), err)
}

var (
	_ core.FileCreator    = (*Driver)(nil)
	_ core.DirectoryMaker = (*Driver)(nil)
	_ core.SubtreeRemover = (*Driver)(nil)
)
