package graphdrive

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"path"

	core "github.com/nostalume/mountweave"
)

// Mkdir asks Graph to create a folder under the admitted parent without replacing an entry.
func (d *Driver) Mkdir(ctx context.Context, target core.NativeAddress) (core.Change, error) {
	unchanged := core.Change{Effect: core.Unchanged}
	if d.readOnly || target.Namespace() != core.TreeNamespace {
		return unchanged, core.Fail(core.UnsupportedOperation, nil)
	}
	full := target.Value()
	if err := d.admit(ctx, full); err != nil {
		return unchanged, err
	}
	if full == "/" {
		unchanged.Before = core.TargetPresent
		return unchanged, core.Fail(core.AlreadyExists, nil)
	}
	parent, err := d.resolve(ctx, path.Dir(full))
	if err != nil {
		return unchanged, err
	}
	if parent.Folder == nil {
		return unchanged, core.Fail(core.NotDirectory, nil)
	}
	name := path.Base(full)
	body, _ := json.Marshal(struct {
		Name     string   `json:"name"`
		Folder   struct{} `json:"folder"`
		Conflict string   `json:"@microsoft.graph.conflictBehavior"`
	}{Name: name, Conflict: "fail"})
	route := "/drives/" + url.PathEscape(d.opts.DriveID) + "/items/" + url.PathEscape(parent.ID) + "/children"
	if err := ctx.Err(); err != nil {
		return unchanged, err
	}
	unknown := core.Change{Effect: core.Unknown}
	response, err := d.apiRequest(ctx, http.MethodPost, d.apiURL(route), bytes.NewReader(body), "application/json", int64(len(body)))
	if err != nil {
		return unknown, err
	}
	if response.StatusCode != http.StatusCreated {
		failure, cleanup := d.responseFailure(response)
		if response.StatusCode == http.StatusConflict {
			var native graphError
			if errors.As(failure, &native) && native.Code == "nameAlreadyExists" {
				unchanged.Before = core.TargetPresent
			} else {
				failure = core.Fail(core.UpstreamFailure, failure)
			}
		}
		err = errors.Join(failure, cleanup)
		if response.StatusCode < 400 || response.StatusCode == http.StatusRequestTimeout || response.StatusCode >= 500 {
			return unknown, err
		}
		return unchanged, err
	}
	var created item
	decoded, err := d.decode(response, &created)
	if !decoded || created.ID == "" || created.ID == parent.ID || created.Name != name || created.Folder == nil ||
		created.File != nil || created.Remote != nil || created.Package != nil || created.Parent == nil ||
		created.Parent.ID != parent.ID || created.Parent.DriveID != d.opts.DriveID {
		return unknown, core.Fail(core.InvalidUpstreamResponse, err)
	}
	return core.Change{Effect: core.Applied, Before: core.TargetMissing}, err
}

var _ core.DirectoryMaker = (*Driver)(nil)
