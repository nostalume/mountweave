package dropbox

import (
	"context"
	"errors"
	"net/http"
	"path"
	"strings"

	core "github.com/nostalume/mountweave"
)

// Relocate moves a selected native item to an exact child of an admitted parent.
func (d *Driver) Relocate(ctx context.Context, source, target core.NativeAddress, policies ...core.MoveOptions) (core.Change, error) {
	unchanged := core.Change{Effect: core.Unchanged}
	if len(policies) > 1 {
		return unchanged, core.Fail(core.InvalidRequest, nil)
	}
	if len(policies) == 1 && (!policies[0].Source.Empty() || !policies[0].Target.Empty() ||
		policies[0].Existing != "" && policies[0].Existing != core.CreateNew) {
		return unchanged, core.Fail(core.UnsupportedOperation, nil)
	}
	_, change, err := d.relocate(ctx, source, target, false)
	return change, err
}

// RelocateInto moves a selected item to a directory, letting Dropbox assign
// an available name on collision.
func (d *Driver) RelocateInto(ctx context.Context, source, directory core.NativeAddress) (string, core.Change, error) {
	return d.relocate(ctx, source, directory, true)
}

func (d *Driver) relocate(ctx context.Context, source, target core.NativeAddress, into bool) (actual string, result core.Change, err error) {
	result.Effect = core.Unchanged
	if !d.allowCreate || source.Namespace() != core.TreeNamespace || target.Namespace() != core.TreeNamespace {
		return "", result, core.Fail(core.UnsupportedOperation, nil)
	}
	from, to := source.Value(), target.Value()
	if err := d.admit(ctx, from); err != nil {
		return "", result, err
	}
	if err := core.ValidatePath(to); err != nil {
		return "", result, err
	}
	if from == "/" || core.Within(to, from) || !into && to == "/" {
		return "", result, core.Fail(core.InvalidRequest, nil)
	}
	directory, name := path.Dir(to), path.Base(to)
	if into {
		directory, name = to, path.Base(from)
		if strings.EqualFold(path.Dir(from), directory) {
			return "", result, core.Fail(core.InvalidRequest, nil)
		}
	} else if strings.EqualFold(from, to) {
		return "", result, core.Fail(core.UnsupportedOperation, nil)
	}
	entry, err := d.walk(ctx, from)
	if err != nil {
		return "", result, err
	}
	if entry.symlink {
		return "", result, core.Fail(core.UnsupportedOperation, nil)
	}
	parent, err := d.walk(ctx, directory)
	if err != nil {
		return "", result, err
	}
	if parent.meta.Kind != core.Directory {
		return "", result, core.Fail(core.NotDirectory, nil)
	}
	if parent.ID != "" && core.ValidatePath(parent.pathLower) != nil {
		return "", result, core.Fail(core.InvalidUpstreamResponse, nil)
	}
	req, err := rpcRequest(ctx, "/files/move_v2", map[string]any{
		"from_path": entry.ID, "to_path": childPath(parent, name),
		"autorename": into, "allow_ownership_transfer": false,
	})
	if err != nil {
		return "", result, err
	}
	req.GetBody = nil
	response, entered, err := d.do(req)
	if entered {
		result.Effect = core.Unknown
	}
	if err != nil {
		return "", result, err
	}
	defer func() { err = errors.Join(err, d.closeBody(response.Body, false), ctx.Err()) }()
	if response.StatusCode != http.StatusOK {
		_, failure := d.failure(response, relocationFailure)
		switch core.CodeOf(failure) {
		case core.AlreadyExists:
			return "", core.Change{Effect: core.Unchanged, Before: core.TargetPresent}, failure
		case core.NotDirectory, core.UpstreamAccessDenied:
			result.Effect = core.Unchanged
		}
		return "", result, failure
	}
	var receipt struct {
		Metadata nativeMetadata `json:"metadata"`
	}
	if err := d.readJSON(response, &receipt, false); err != nil {
		return "", result, err
	}
	moved, err := accept(receipt.Metadata, false)
	if err != nil {
		return "", result, err
	}
	actual = moved.meta.Name
	if moved.ID != entry.ID || !into && actual != name || moved.meta.Kind != entry.meta.Kind || moved.symlink ||
		!strings.EqualFold(moved.pathLower, parent.pathLower+"/"+actual) {
		return "", result, core.Fail(core.InvalidUpstreamResponse, errors.New("Dropbox relocation receipt did not match the selected item and destination"))
	}
	return actual, core.Change{Effect: core.Applied, Before: core.TargetMissing}, nil
}

var (
	_ core.Relocator          = (*Driver)(nil)
	_ core.DirectoryRelocator = (*Driver)(nil)
)
