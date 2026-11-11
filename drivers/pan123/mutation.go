package pan123

import (
	"context"
	"encoding/json"
	"errors"
	"path"

	core "github.com/nostalume/mountweave"
)

// Mkdir creates one new directory in the selected native parent.
func (d *Driver) Mkdir(ctx context.Context, address core.NativeAddress) (core.Change, error) {
	result := core.Change{Effect: core.Unchanged}
	if err := d.check(ctx, address); err != nil {
		return result, err
	}
	if !d.allowMkdir {
		return result, core.Fail(core.UnsupportedOperation, nil)
	}
	if address.Value() == "/" {
		result.Before = core.TargetPresent
		return result, core.Fail(core.AlreadyExists, nil)
	}
	parent, err := d.directoryID(ctx, path.Dir(address.Value()))
	if err != nil {
		return result, err
	}
	fields := struct {
		Name   string      `json:"name"`
		Parent json.Number `json:"parentID"`
	}{path.Base(address.Value()), json.Number(parent)}
	req, err := controlPost(ctx, "/upload/v1/file/mkdir", fields)
	if err != nil {
		return result, err
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	if d.closed.Load() {
		return result, core.Fail(core.MountUnavailable, nil)
	}
	result.Effect = core.Unknown
	var receipt struct {
		ID nativeID `json:"dirID"`
	}
	decoded, err := d.request(&d.api, req, &receipt)
	if !decoded {
		return result, err
	}
	if receipt.ID == "" || receipt.ID == "0" || string(receipt.ID) == parent {
		return result, errors.Join(core.Fail(core.InvalidUpstreamResponse, errors.New("123 did not confirm a new directory ID")), err)
	}
	return core.Change{Effect: core.Applied, Before: core.TargetMissing}, err
}

func (d *Driver) directoryID(ctx context.Context, name string) (string, error) {
	entry, err := d.resolve(ctx, name)
	if err != nil {
		return "", err
	}
	if entry.meta.Kind != core.Directory {
		return "", core.Fail(core.NotDirectory, nil)
	}
	return string(entry.ID), nil
}

// Remove moves the selected file ID to the native recycle bin.
func (d *Driver) Remove(ctx context.Context, address core.NativeAddress, policies ...core.RemoveOptions) (core.Change, error) {
	result := core.Change{Effect: core.Unchanged}
	if len(policies) > 1 {
		return result, core.Fail(core.InvalidRequest, nil)
	}
	if !d.allowRemove || len(policies) == 1 && (!policies[0].When.Empty() || policies[0].Target != "") {
		return result, core.Fail(core.UnsupportedOperation, nil)
	}
	if err := d.check(ctx, address); err != nil {
		return result, err
	}
	file, err := d.resolve(ctx, address.Value())
	if err != nil {
		return result, err
	}
	if file.meta.Kind != core.File {
		return result, core.Fail(core.UnsupportedOperation, nil)
	}
	fields := struct {
		IDs [1]json.Number `json:"fileIDs"`
	}{[1]json.Number{json.Number(file.ID)}}
	req, err := controlPost(ctx, "/api/v1/file/trash", fields)
	if err != nil {
		return result, err
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	if d.closed.Load() {
		return result, core.Fail(core.MountUnavailable, nil)
	}
	result.Effect = core.Unknown
	decoded, err := d.request(&d.api, req, nil)
	if decoded {
		result.Effect = core.Applied
	}
	return result, err
}

var _ core.DirectoryMaker = (*Driver)(nil)
var _ core.Remover = (*Driver)(nil)
