package alipan

import (
	"context"
	"errors"
	"fmt"
	"time"

	core "github.com/nostalume/mountweave"
)

// RemoveSubtree deletes the captured native directory ID and confirms its task.
// This is whole-subtree authority, including names absent from client listings.
func (d *Driver) RemoveSubtree(ctx context.Context, pAddress core.NativeAddress, limits core.TreeOptions) (core.Change, error) {
	if pAddress.Namespace() != core.TreeNamespace {
		return core.Change{Effect: core.Unchanged}, core.Fail(core.UnsupportedOperation, nil)
	}
	p := pAddress.Value()

	unchanged := core.Change{Effect: core.Unchanged}
	if p == "/" || limits.MaxEntries <= 0 || limits.MaxPlanBytes <= 0 || limits.Concurrency < 0 {
		return unchanged, core.Fail(core.InvalidRequest, nil)
	}
	f, err := d.resolve(ctx, p)
	if err != nil {
		return unchanged, err
	}
	if f.Type != "folder" {
		return unchanged, core.Fail(core.NotDirectory, nil)
	}
	return d.deleteEntry(ctx, f.ID)
}

func (d *Driver) deleteEntry(ctx context.Context, id string) (core.Change, error) {
	var out struct {
		Drive string `json:"drive_id"`
		ID    string `json:"file_id"`
		Async string `json:"async_task_id"`
	}
	change, err := d.mutate(ctx, "/adrive/v1.0/openFile/delete", map[string]string{"drive_id": d.opts.DriveID, "file_id": id}, &out)
	if change.Effect != core.Applied {
		return change, err
	}
	if out.Drive != d.opts.DriveID || out.ID != id {
		return core.Change{Effect: core.Unknown}, errors.Join(err, core.Fail(core.InvalidUpstreamResponse, nil))
	}
	if out.Async == "" {
		return change, err
	}
	uncertain := func(cause error) (core.Change, error) {
		return core.Change{Effect: core.Unknown}, fmt.Errorf("deletion task %s: %w", out.Async, cause)
	}
	if err != nil {
		return uncertain(err)
	}
	interval := d.opts.DeletionPollInterval
	if interval == 0 {
		interval = time.Second
	}
	for {
		if err := ctx.Err(); err != nil {
			return uncertain(err)
		}
		var task struct {
			ID    string `json:"async_task_id"`
			State string `json:"state"`
		}
		if err := d.post(ctx, "/adrive/v1.0/openFile/async_task/get", map[string]string{"async_task_id": out.Async}, &task); err != nil {
			return uncertain(err)
		}
		if task.ID != out.Async {
			return uncertain(core.Fail(core.InvalidUpstreamResponse, nil))
		}
		switch task.State {
		case "Succeed":
			return core.Change{Effect: core.Applied}, ctx.Err()
		case "Failed":
			return core.Change{Effect: core.Incomplete}, errors.Join(core.Fail(core.UpstreamFailure, fmt.Errorf("deletion task %s failed", out.Async)), ctx.Err())
		case "Running":
		default:
			return uncertain(core.Fail(core.InvalidUpstreamResponse, nil))
		}
		timer := time.NewTimer(interval)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return uncertain(ctx.Err())
		}
	}
}

var _ core.SubtreeRemover = (*Driver)(nil)
