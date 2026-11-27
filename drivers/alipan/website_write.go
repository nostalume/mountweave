package alipan

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"time"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/internal/upload"
)

// WebsiteWriteOptions selects the private folder and bounded upload memory.
type WebsiteWriteOptions struct {
	StagingRootID    string `json:"staging_root_id"`
	PartSize         int64  `json:"part_size"`
	MaxBufferedBytes int64  `json:"max_buffered_bytes"`
}

// OpenWritable adds exclusive file creation, relocation, trash and directory creation.
// The staging directory must be separate from the mounted tree on the same drive.
func (a WebsiteAccount) OpenWritable(ctx context.Context, rootID string, opts WebsiteWriteOptions, client *http.Client, release func() error) (core.Driver, error) {
	if rootID == "root" || opts.StagingRootID == "" || opts.StagingRootID == "root" || opts.StagingRootID == rootID {
		return nil, core.Fail(core.InvalidRequest, errors.New("Aliyun Drive's staging folder must be outside the mounted tree"))
	}
	if err := upload.Validate(opts.PartSize, opts.MaxBufferedBytes, 1); err != nil || opts.PartSize > 5<<30 {
		return nil, core.Fail(core.InvalidRequest, err)
	}
	d, _, err := a.open(ctx, rootID, "", client, release)
	if err != nil {
		return nil, err
	}
	for _, pair := range [][2]string{{opts.StagingRootID, rootID}, {rootID, opts.StagingRootID}} {
		for next, steps := pair[0], 0; next != "root"; steps++ {
			if next == pair[1] || steps == 100 {
				return nil, core.Fail(core.InvalidRequest, errors.New("Aliyun Drive's staging folder overlaps the mounted tree"))
			}
			f, err := d.websiteFile(ctx, next)
			if err != nil {
				return nil, err
			}
			if f.Type != "folder" || f.Parent == "" {
				return nil, core.Fail(core.InvalidUpstreamResponse, nil)
			}
			next = f.Parent
		}
	}
	d.opts.PartSize = opts.PartSize
	d.buffer, err = upload.New(opts.MaxBufferedBytes, 1)
	if err != nil {
		return nil, core.Fail(core.InvalidRequest, err)
	}
	return &websiteWriter{Driver: d, d: d, staging: opts.StagingRootID}, nil
}

type websiteWriter struct {
	core.Driver
	d       *Driver
	staging string
}

func (d *Driver) websiteFile(ctx context.Context, id string) (nativeFile, error) {
	var f nativeFile
	err := d.website.request(ctx, "file", map[string]string{"drive_id": d.opts.DriveID, "file_id": id}, &f)
	if err == nil && (f.ID != id || f.Drive != d.opts.DriveID) {
		err = core.Fail(core.InvalidUpstreamResponse, nil)
	}
	return f, err
}

func (w *websiteWriter) Mkdir(ctx context.Context, p core.NativeAddress) (core.Change, error) {
	return w.d.Mkdir(ctx, p)
}

func (w *websiteWriter) RemoveWithDisposition(ctx context.Context, target core.NativeAddress, options core.RemoveOptions) (core.Change, error) {
	if options.Disposition != core.RemoveTrash {
		return core.Change{Effect: core.Unchanged}, core.Fail(core.UnsupportedOperation, nil)
	}
	options.Disposition = ""
	return w.Remove(ctx, target, options)
}

func (w *websiteWriter) Remove(ctx context.Context, p core.NativeAddress, opts ...core.RemoveOptions) (core.Change, error) {
	return w.d.Remove(ctx, p, opts...)
}

func (w *websiteWriter) Relocate(ctx context.Context, source, target core.NativeAddress, options ...core.MoveOptions) (core.Change, error) {
	unchanged := core.Change{Effect: core.Unchanged}
	if len(options) > 1 {
		return unchanged, core.Fail(core.InvalidRequest, nil)
	}
	if len(options) == 1 {
		o := options[0]
		if !o.Source.Empty() || !o.Target.Empty() || (o.Existing != "" && o.Existing != core.CreateNew) {
			return unchanged, core.Fail(core.UnsupportedOperation, nil)
		}
	}
	if source.Namespace() != core.TreeNamespace || target.Namespace() != core.TreeNamespace {
		return unchanged, core.Fail(core.UnsupportedOperation, nil)
	}
	if source.Value() == "/" || target.Value() == "/" || source == target {
		return unchanged, core.Fail(core.InvalidRequest, nil)
	}
	f, err := w.d.resolve(ctx, source.Value())
	if err != nil {
		return unchanged, err
	}
	if f.Type != "file" {
		return unchanged, core.Fail(core.UnsupportedOperation, nil)
	}
	parent, name, err := w.d.destination(ctx, target.Value())
	if err != nil {
		return unchanged, &core.OperationError{Side: core.TargetSide, Phase: core.CreatePhase, Cause: err}
	}
	return w.d.websiteMove(ctx, f, parent.ID, name)
}

func (w *websiteWriter) Create(ctx context.Context, p core.NativeAddress, input io.Reader, length *int64) (core.Change, error) {
	return w.Publish(ctx, p, input, length, core.PublicationOptions{When: core.IfAbsent()})
}

func (w *websiteWriter) Publish(ctx context.Context, p core.NativeAddress, input io.Reader, length *int64, options core.PublicationOptions) (core.Change, error) {
	unchanged := core.Change{Effect: core.Unchanged}
	if !options.When.IsAbsent() || p.Namespace() != core.TreeNamespace || length == nil {
		return unchanged, core.Fail(core.UnsupportedOperation, nil)
	}
	if *length < 0 || input == nil {
		return unchanged, core.Fail(core.InvalidRequest, nil)
	}
	d := w.d
	parent, name, err := d.destination(ctx, p.Value())
	if err != nil {
		return unchanged, err
	}
	var nonce [16]byte
	if _, err = rand.Read(nonce[:]); err != nil {
		return unchanged, err
	}
	temporary := "mweave-" + hex.EncodeToString(nonce[:])
	n := *length
	staged, result, err := d.uploadFile(ctx, w.staging, temporary, input, n)
	if err != nil || result.Effect != core.Applied {
		if err == nil {
			err = core.Fail(core.InvalidUpstreamResponse, nil)
		}
		return unchanged, errors.Join(err, w.discard(ctx, staged.ID, temporary))
	}
	change, err := d.websiteMove(ctx, nativeFile{ID: staged.ID, Name: temporary, Size: &n}, parent.ID, name)
	if change.Effect == core.Unchanged {
		err = errors.Join(err, w.discard(ctx, staged.ID, temporary))
	}
	return change, err
}

func (d *Driver) websiteMove(ctx context.Context, source nativeFile, parent, name string) (core.Change, error) {
	if err := ctx.Err(); err != nil {
		return core.Change{Effect: core.Unchanged}, err
	}
	request := map[string]string{"drive_id": d.opts.DriveID, "file_id": source.ID, "type": "file", "file_name": source.Name,
		"to_drive_id": d.opts.DriveID, "to_parent_file_id": parent, "new_name": name, "check_name_mode": "refuse"}
	var reply struct {
		createdFile
		Task string `json:"async_task_id"`
	}
	err := d.website.request(ctx, "move", request, &reply)
	if err == nil && reply.Task == "" {
		if reply.Exists {
			return core.Change{Effect: core.Unchanged, Before: core.TargetPresent}, core.Fail(core.AlreadyExists, nil)
		}
		if reply.ID == source.ID && reply.Drive == d.opts.DriveID && reply.Parent == parent && reply.Name == name {
			return core.Change{Effect: core.Applied, Before: core.TargetMissing}, nil
		}
	}
	// A missing acknowledgement can outlive cancellation. Inspect by ID without
	// replaying the move or deleting an object whose commit can still arrive.
	reconcile, cancel := context.WithTimeout(context.WithoutCancel(ctx), 8*time.Second)
	defer cancel()
	observed, readErr := d.websiteFile(reconcile, source.ID)
	if readErr == nil && observed.Parent == parent && observed.Name == name && observed.Type == "file" &&
		(source.Size == nil || observed.Size != nil && *observed.Size == *source.Size) {
		return core.Change{Effect: core.Applied, Before: core.TargetMissing}, nil
	}
	return core.Change{Effect: core.Unknown}, errors.Join(err, readErr, core.Fail(core.InvalidUpstreamResponse, errors.New("Aliyun Drive move could not be confirmed")))
}

func (w *websiteWriter) discard(ctx context.Context, id, name string) error {
	if id == "" {
		return nil
	}
	cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 8*time.Second)
	defer cancel()
	f, err := w.d.websiteFile(cleanup, id)
	if core.CodeOf(err) == core.NotFound {
		return nil
	}
	if err != nil {
		return err
	}
	if f.Parent != w.staging || f.Name != name || f.Type != "file" {
		return core.Fail(core.InvalidUpstreamResponse, errors.New("staged file changed before cleanup"))
	}
	_, err = w.d.websiteTrash(cleanup, id)
	return err
}

func (d *Driver) websiteTrash(ctx context.Context, id string) (core.Change, error) {
	if err := ctx.Err(); err != nil {
		return core.Change{Effect: core.Unchanged}, err
	}
	var reply struct {
		createdFile
		Task string `json:"async_task_id"`
	}
	err := d.website.request(ctx, "trash", map[string]string{"drive_id": d.opts.DriveID, "file_id": id}, &reply)
	if err == nil && reply.Task == "" && (reply.ID == "" || reply.ID == id) &&
		(reply.Drive == "" || reply.Drive == d.opts.DriveID) {
		return core.Change{Effect: core.Applied}, nil
	}
	reconcile, cancel := context.WithTimeout(context.WithoutCancel(ctx), 8*time.Second)
	defer cancel()
	_, readErr := d.websiteFile(reconcile, id)
	if core.CodeOf(readErr) == core.NotFound {
		return core.Change{Effect: core.Applied}, nil
	}
	return core.Change{Effect: core.Unknown}, errors.Join(err, readErr,
		core.Fail(core.InvalidUpstreamResponse, errors.New("Aliyun Drive trash could not be confirmed")))
}
