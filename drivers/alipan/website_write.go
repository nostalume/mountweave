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

// OpenWritable adds exclusive publication, directory creation and file trash.
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

func (w *websiteWriter) Remove(ctx context.Context, p core.NativeAddress, opts ...core.RemoveOptions) (core.Change, error) {
	return w.d.Remove(ctx, p, opts...)
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
	if err := ctx.Err(); err != nil {
		return unchanged, errors.Join(err, w.discard(ctx, staged.ID, temporary))
	}
	request := map[string]string{"drive_id": d.opts.DriveID, "file_id": staged.ID, "type": "file", "file_name": temporary,
		"to_drive_id": d.opts.DriveID, "to_parent_file_id": parent.ID, "new_name": name, "check_name_mode": "refuse"}
	var reply struct {
		createdFile
		Task string `json:"async_task_id"`
	}
	err = d.website.request(ctx, "move", request, &reply)
	if err == nil && reply.Exists && reply.Task == "" {
		return unchanged, errors.Join(core.Fail(core.AlreadyExists, nil), w.discard(ctx, staged.ID, temporary))
	}
	if err == nil && reply.ID == staged.ID && reply.Drive == d.opts.DriveID && reply.Task == "" &&
		(reply.Parent == "" || reply.Parent == parent.ID) && (reply.Name == "" || reply.Name == name) {
		return core.Change{Effect: core.Applied, Before: core.TargetMissing}, nil
	}
	// A missing acknowledgement can outlive cancellation. Inspect by ID without
	// replaying the move or deleting an object whose commit can still arrive.
	reconcile, cancel := context.WithTimeout(context.WithoutCancel(ctx), 8*time.Second)
	defer cancel()
	observed, readErr := d.websiteFile(reconcile, staged.ID)
	if readErr == nil && observed.Parent == parent.ID && observed.Name == name && observed.Type == "file" && observed.Size != nil && *observed.Size == n {
		return core.Change{Effect: core.Applied, Before: core.TargetMissing}, nil
	}
	return core.Change{Effect: core.Unknown}, errors.Join(err, readErr, core.Fail(core.InvalidUpstreamResponse, errors.New("Aliyun Drive publication could not be confirmed")))
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
