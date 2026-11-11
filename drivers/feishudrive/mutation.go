package feishudrive

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"

	core "github.com/nostalume/mountweave"
)

// Remove moves one selected uploaded file to the native recycle bin.
func (d *Driver) Remove(ctx context.Context, address core.NativeAddress, options ...core.RemoveOptions) (core.Change, error) {
	result := core.Change{Effect: core.Unchanged}
	if len(options) > 1 {
		return result, core.Fail(core.InvalidRequest, nil)
	}
	if !d.allowRemove || len(options) == 1 && (options[0].Target != "" || !options[0].When.Empty()) {
		return result, core.Fail(core.UnsupportedOperation, nil)
	}
	if err := d.check(ctx, address); err != nil {
		return result, err
	}
	file, err := d.resolve(ctx, address.Value())
	if err != nil {
		return result, err
	}
	if file.Kind != core.File || file.exportType != "" {
		return result, core.Fail(core.UnsupportedOperation, nil)
	}

	route := "/files/" + url.PathEscape(file.Token) + "?type=file&async=false"
	resp, attempted, err := d.request(ctx, http.MethodDelete, route, "", nil)
	if attempted {
		result.Effect = core.Unknown
	}
	if err != nil {
		return result, err
	}
	data, nativeCode, err := d.reply(resp)
	if nativeCode == 1061007 { // The selected token had already been deleted.
		result.Effect = core.Unchanged
		return result, core.Fail(core.NotFound, err)
	}
	if data == nil {
		return result, err
	}
	var receipt struct {
		TaskID string `json:"task_id"`
	}
	if len(*data) != 0 {
		if decode := json.Unmarshal(*data, &receipt); decode != nil {
			return result, errors.Join(core.Fail(core.InvalidUpstreamResponse, decode), err)
		}
	}
	if receipt.TaskID != "" {
		return result, errors.Join(core.Fail(core.UpstreamFailure, errors.New("Feishu accepted deletion as an asynchronous task")), err)
	}
	result.Effect = core.Applied
	return result, err
}

var _ core.Remover = (*Driver)(nil)
