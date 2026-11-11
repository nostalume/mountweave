package dropbox

import (
	"context"
	"errors"
	"io"
	"net/http"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/internal/upload"
)

const maxSessionUpload int64 = (1 << 41) - (1 << 22)

type sessionCursor struct {
	ID     string `json:"session_id"`
	Offset int64  `json:"offset"`
}

func (d *Driver) createSession(ctx context.Context, parentID, name string, commit map[string]any, input io.Reader, size int64) (result core.Change, err error) {
	result.Effect = core.Unchanged
	// Keep the first real EOF terminal across request boundaries.
	input = io.MultiReader(input)
	var cursor sessionCursor
	for cursor.Offset < size {
		length := min(size-cursor.Offset, maxSingleUpload)
		part := input
		if cursor.Offset+length < size {
			// Only the last staged part checks the caller's actual EOF.
			part = io.LimitReader(input, length)
		}
		route := "/files/upload_session/append_v2"
		var arg any = map[string]any{"cursor": cursor}
		var output any
		var started struct {
			ID string `json:"session_id"`
		}
		if cursor.Offset == 0 {
			route, arg, output = "/files/upload_session/start", struct{}{}, &started
		}
		if err := d.stageUpload(ctx, route, arg, part, length, output); err != nil {
			return result, err
		}
		if cursor.Offset == 0 {
			if started.ID == "" {
				return result, core.Fail(core.InvalidUpstreamResponse, errors.New("native upload session ID is missing"))
			}
			cursor.ID = started.ID
		}
		cursor.Offset += length
	}

	// Publish only after the final append verified the real source EOF.
	arg := map[string]any{"cursor": cursor, "commit": commit}
	response, entered, err := d.sendUpload(ctx, "/files/upload_session/finish", arg, http.NoBody, 0)
	if entered {
		result.Effect = core.Unknown
	}
	if err != nil {
		return result, err
	}
	defer func() { err = errors.Join(err, d.closeBody(response.Body, true), ctx.Err()) }()
	if response.StatusCode != http.StatusOK {
		_, err := d.failure(response, sessionFailure)
		if core.CodeOf(err) == core.AlreadyExists {
			result.Effect = core.Unchanged
		}
		return result, err
	}
	actual, err := d.uploadedSize(response, parentID, name, true)
	if err != nil {
		return result, err
	}
	if actual != size {
		return result, core.Fail(core.InvalidUpstreamResponse, errors.New("native receipt does not confirm the complete input"))
	}
	return core.Change{Effect: core.Applied, Before: core.TargetMissing}, nil
}

func (d *Driver) stageUpload(ctx context.Context, route string, arg any, input io.Reader, length int64, output any) (err error) {
	body := upload.NewBody(ctx, input, &length)
	response, _, err := d.sendUpload(ctx, route, arg, body, length)
	complete, inputErr := body.Finish()
	if err != nil {
		return errors.Join(err, inputErr)
	}
	defer func() { err = errors.Join(err, d.closeBody(response.Body, true), ctx.Err()) }()
	if response.StatusCode != http.StatusOK {
		_, err := d.failure(response, sessionFailure)
		return errors.Join(err, inputErr)
	}
	if inputErr != nil {
		return inputErr
	}
	if !complete {
		return core.Fail(core.InvalidUpstreamResponse, errors.New("native upload accepted an incomplete part"))
	}
	if output != nil {
		return d.readJSON(response, output, true)
	}
	_, err = d.readBody(response, true)
	return err
}
