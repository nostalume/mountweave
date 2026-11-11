package wps365

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"path"

	core "github.com/nostalume/mountweave"
)

// Mkdir creates one directory with the native fail-on-conflict policy.
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
	parent, err := d.resolve(ctx, path.Dir(address.Value()))
	if err != nil {
		return result, err
	}
	if parent.Type != "folder" {
		return result, core.Fail(core.NotDirectory, nil)
	}
	fields := struct {
		Name     string `json:"name"`
		Type     string `json:"file_type"`
		Conflict string `json:"on_name_conflict"`
	}{path.Base(address.Value()), "folder", "fail"}
	data, err := json.Marshal(fields)
	if err != nil {
		return result, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, d.fileURL(parent.ID)+"/create", bytes.NewReader(data))
	if err != nil {
		return result, core.Fail(core.InvalidRequest, nil)
	}
	req.GetBody = nil
	if err := d.active(ctx); err != nil {
		return result, err
	}

	var created item
	confirmed, err := d.request(req, &created)
	if !confirmed {
		if core.CodeOf(err) == core.AlreadyExists {
			result.Before = core.TargetPresent
			return result, err
		}
		return core.Change{Effect: core.Unknown}, err
	}
	if created.Drive != d.drive || created.Parent != parent.ID || created.Name != fields.Name ||
		created.Type != "folder" || !core.ValidName(created.ID) || created.ID == "0" ||
		created.ID == parent.ID || created.ID == d.root {
		return core.Change{Effect: core.Unknown}, errors.Join(
			core.Fail(core.InvalidUpstreamResponse, errors.New("WPS did not confirm the requested directory")), err,
		)
	}
	return core.Change{Effect: core.Applied, Before: core.TargetMissing}, err
}

// Remove uses the selected ordinary file's native ID.
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
	if file.Type != "file" {
		return result, core.Fail(core.UnsupportedOperation, nil)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, d.fileURL(file.ID)+"/delete", nil)
	if err != nil {
		return result, core.Fail(core.InvalidRequest, nil)
	}
	if err := d.active(ctx); err != nil {
		return result, err
	}
	result.Effect = core.Unknown
	confirmed, err := d.request(req, nil)
	if confirmed {
		result.Effect = core.Applied
	}
	return result, err
}

var _ core.DirectoryMaker = (*Driver)(nil)
var _ core.Remover = (*Driver)(nil)
