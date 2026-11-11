package alipan

import (
	"context"
	"errors"
	"path"

	core "github.com/nostalume/mountweave"
)

func (d *Driver) destination(ctx context.Context, p string) (parent nativeFile, name string, err error) {
	if err = d.admit(ctx, p); err != nil {
		return
	}
	if p == "/" {
		err = core.Fail(core.InvalidRequest, nil)
		return
	}
	parent, err = d.resolve(ctx, path.Dir(p))
	if err == nil && parent.Type != "folder" {
		err = core.Fail(core.NotDirectory, nil)
	}
	name = path.Base(p)
	return
}
func (d *Driver) mutate(ctx context.Context, route string, input, out any) (core.Change, error) {
	if err := ctx.Err(); err != nil {
		return core.Change{Effect: core.Unchanged}, err
	}
	d.mu.Lock()
	cleanup := d.closeErr
	d.mu.Unlock()
	if cleanup != nil {
		return core.Change{Effect: core.Unchanged}, core.Fail(core.UpstreamFailure, cleanup)
	}
	effect, err := d.call(ctx, route, input, out, false)
	return core.Change{Effect: effect}, errors.Join(err, ctx.Err())
}

type createdFile struct {
	Drive     string       `json:"drive_id"`
	Parent    string       `json:"parent_file_id"`
	ID        string       `json:"file_id"`
	Name      string       `json:"file_name"`
	UploadID  string       `json:"upload_id"`
	Exists    bool         `json:"exist"`
	Available bool         `json:"available"`
	Rapid     bool         `json:"rapid_upload"`
	Parts     []uploadPart `json:"part_info_list"`
}
type uploadPart struct {
	Number int    `json:"part_number"`
	URL    string `json:"upload_url,omitempty"`
}

func (d *Driver) createEntry(ctx context.Context, parent, name, kind string, parts []uploadPart, length *int64) (createdFile, core.Change, error) {
	var out createdFile
	request := map[string]any{"drive_id": d.opts.DriveID, "parent_file_id": parent, "name": name, "type": kind, "check_name_mode": "refuse"}
	if parts != nil {
		request["part_info_list"] = parts
	}
	if length != nil {
		request["size"] = *length
	}
	change, err := d.mutate(ctx, "/adrive/v1.0/openFile/create", request, &out)
	if change.Effect != core.Applied {
		return out, change, err
	}
	if out.Exists {
		return out, core.Change{Effect: core.Unchanged}, errors.Join(err, core.Fail(core.AlreadyExists, nil))
	}
	if out.ID == "" || out.Drive != d.opts.DriveID || out.Parent != parent || (out.Name != "" && out.Name != name) {
		return out, core.Change{Effect: core.Unknown}, errors.Join(err, core.Fail(core.InvalidUpstreamResponse, nil))
	}
	return out, change, err
}
func (d *Driver) Mkdir(ctx context.Context, pAddress core.NativeAddress) (core.Change, error) {
	if pAddress.Namespace() != core.TreeNamespace {
		return core.Change{Effect: core.Unchanged}, core.Fail(core.UnsupportedOperation, nil)
	}
	p := pAddress.Value()

	unchanged := core.Change{Effect: core.Unchanged}
	parent, name, err := d.destination(ctx, p)
	if err != nil {
		return unchanged, err
	}
	if _, exists, err := d.child(ctx, parent.ID, name); err != nil {
		return unchanged, err
	} else if exists {
		return unchanged, core.Fail(core.AlreadyExists, nil)
	}
	_, change, err := d.createEntry(ctx, parent.ID, name, "folder", nil, nil)
	return change, err
}
func (d *Driver) Remove(ctx context.Context, pAddress core.NativeAddress, policies ...core.RemoveOptions) (core.Change, error) {
	if len(policies) > 1 {
		return core.Change{Effect: core.Unchanged}, core.Fail(core.InvalidRequest, nil)
	}
	if len(policies) == 1 && (!policies[0].When.Empty() || policies[0].Target != "") {
		return core.Change{Effect: core.Unchanged}, core.Fail(core.UnsupportedOperation, nil)
	}
	if pAddress.Namespace() != core.TreeNamespace {
		return core.Change{Effect: core.Unchanged}, core.Fail(core.UnsupportedOperation, nil)
	}
	p := pAddress.Value()

	unchanged := core.Change{Effect: core.Unchanged}
	if p == "/" {
		return unchanged, core.Fail(core.InvalidRequest, nil)
	}
	f, err := d.resolve(ctx, p)
	if err != nil {
		return unchanged, err
	}
	// No native empty-directory condition is supplied; a prior listing cannot
	// establish that condition against concurrent child creation.
	if f.Type != "file" {
		return unchanged, core.Fail(core.UnsupportedOperation, nil)
	}
	return d.deleteEntry(ctx, f.ID)
}
func (d *Driver) Relocate(ctx context.Context, sourceAddress, targetAddress core.NativeAddress, policies ...core.MoveOptions) (core.Change, error) {
	if len(policies) > 1 {
		return core.Change{Effect: core.Unchanged}, core.Fail(core.InvalidRequest, nil)
	}
	if len(policies) == 1 && (!policies[0].Source.Empty() || !policies[0].Target.Empty()) {
		return core.Change{Effect: core.Unchanged}, core.Fail(core.UnsupportedOperation, nil)
	}
	if sourceAddress.Namespace() != core.TreeNamespace {
		return core.Change{Effect: core.Unchanged}, core.Fail(core.UnsupportedOperation, nil)
	}
	source := sourceAddress.Value()
	if targetAddress.Namespace() != core.TreeNamespace {
		return core.Change{Effect: core.Unchanged}, core.Fail(core.UnsupportedOperation, nil)
	}
	target := targetAddress.Value()

	unchanged := core.Change{Effect: core.Unchanged}
	if source == "/" || target == "/" || source == target {
		return unchanged, core.Fail(core.InvalidRequest, nil)
	}
	f, err := d.resolve(ctx, source)
	if err != nil {
		return unchanged, err
	}
	parent, name, err := d.destination(ctx, target)
	if err != nil {
		return unchanged, &core.OperationError{Side: core.TargetSide, Phase: core.CreatePhase, Cause: err}
	}
	// Refuse unsupported modes before conflicts: Core may replace a target.
	if parent.ID != f.Parent && (f.Type != "file" || name != f.Name) {
		return unchanged, core.Fail(core.UnsupportedOperation, nil)
	}
	if _, exists, err := d.child(ctx, parent.ID, name); err != nil {
		return unchanged, err
	} else if exists {
		return unchanged, core.Fail(core.AlreadyExists, nil)
	}
	if parent.ID == f.Parent {
		var out nativeFile
		change, err := d.mutate(ctx, "/adrive/v1.0/openFile/update", map[string]string{"drive_id": d.opts.DriveID, "file_id": f.ID, "name": name, "check_name_mode": "refuse"}, &out)
		if change.Effect == core.Applied && (out.ID != f.ID || out.Drive != d.opts.DriveID || out.Parent != parent.ID || out.Name != name || out.Type != f.Type) {
			return core.Change{Effect: core.Unknown}, errors.Join(err, core.Fail(core.InvalidUpstreamResponse, nil))
		}
		return change, err
	}
	var out struct {
		Drive  string `json:"drive_id"`
		ID     string `json:"file_id"`
		Exists *bool  `json:"exist"`
		Async  string `json:"async_task_id"`
	}
	change, err := d.mutate(ctx, "/adrive/v1.0/openFile/move", map[string]string{"drive_id": d.opts.DriveID, "file_id": f.ID, "to_parent_file_id": parent.ID, "check_name_mode": "refuse"}, &out)
	if change.Effect == core.Applied && out.Exists != nil && *out.Exists {
		return unchanged, errors.Join(err, core.Fail(core.AlreadyExists, nil))
	}
	if change.Effect == core.Applied && (out.Drive != d.opts.DriveID || out.ID != f.ID || out.Async != "" || out.Exists == nil) {
		return core.Change{Effect: core.Unknown}, errors.Join(err, core.Fail(core.InvalidUpstreamResponse, nil))
	}
	return change, err
}

var _ core.DirectoryMaker = (*Driver)(nil)
var _ core.Remover = (*Driver)(nil)
var _ core.Relocator = (*Driver)(nil)
