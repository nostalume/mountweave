package alipan

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/internal/httpcontent"
	"github.com/nostalume/mountweave/drivers/internal/upload"
)

func uploadError(err error) error {
	switch {
	case errors.Is(err, upload.ErrCapacity), errors.Is(err, upload.ErrParts):
		return core.Fail(core.CapacityExceeded, err)
	case errors.Is(err, upload.ErrSize):
		return core.Fail(core.InvalidRequest, err)
	case errors.Is(err, upload.ErrRead):
		return core.Fail(core.InvalidUpstreamResponse, err)
	default:
		return err
	}
}

// Create uploads a known-length new file, without automatic rename/overwrite.
// Native creation may expose an incomplete file. Failures after confirmed
// creation retain that logical file; it is never deleted as path compensation.
func (d *Driver) Create(ctx context.Context, pAddress core.NativeAddress, input io.Reader, length *int64) (core.Change, error) {
	if pAddress.Namespace() != core.TreeNamespace {
		return core.Change{Effect: core.Unchanged}, core.Fail(core.UnsupportedOperation, nil)
	}
	p := pAddress.Value()

	unchanged := core.Change{Effect: core.Unchanged}
	if input == nil || (length != nil && *length < 0) {
		return unchanged, core.Fail(core.InvalidRequest, nil)
	}
	if err := d.admit(ctx, p); err != nil {
		return unchanged, err
	}
	if d.buffer == nil || length == nil {
		return unchanged, core.Fail(core.UnsupportedOperation, nil)
	}
	d.mu.Lock()
	cleanup := d.closeErr
	d.mu.Unlock()
	if cleanup != nil {
		return unchanged, core.Fail(core.UpstreamFailure, cleanup)
	}
	parent, name, err := d.destination(ctx, p)
	if err != nil {
		return unchanged, err
	}
	if _, exists, err := d.child(ctx, parent.ID, name); err != nil {
		return unchanged, err
	} else if exists {
		return unchanged, core.Fail(core.AlreadyExists, nil)
	}
	_, change, err := d.uploadFile(ctx, parent.ID, name, input, *length)
	return change, err
}

// uploadFile retains the native identity for the owner of a staging object.
func (d *Driver) uploadFile(ctx context.Context, parent, name string, input io.Reader, n int64) (file createdFile, result core.Change, err error) {
	unchanged := core.Change{Effect: core.Unchanged}
	length := &n
	count := n / d.opts.PartSize
	if n%d.opts.PartSize != 0 {
		count++
	}
	if count > 10000 {
		return file, unchanged, core.Fail(core.UnsupportedOperation, nil)
	}
	lease, err := d.buffer.Acquire(ctx, d.opts.PartSize, max(1, int(count)))
	if err != nil {
		return file, unchanged, uploadError(err)
	}
	defer lease.Release()
	if n <= d.opts.PartSize {
		data := lease.Buffer()[:int(n)]
		if err := upload.ReadExact(ctx, input, data); err != nil {
			return file, unchanged, uploadError(err)
		}
		input = bytes.NewReader(data)
	}
	parts := make([]uploadPart, max(1, int(count)))
	for i := range parts {
		parts[i].Number = i + 1
	}
	requestParts := parts
	if d.website != nil {
		requestParts = parts[:min(20, len(parts))]
	}
	created, change, err := d.createEntry(ctx, parent, name, "file", requestParts, length)
	if change.Effect != core.Applied {
		return file, change, err
	}
	if n == 0 && (created.Available || created.Rapid) {
		return created, change, err
	}
	partial := core.Change{Effect: core.Incomplete}
	if err != nil {
		return created, partial, err
	}
	if created.Rapid || created.UploadID == "" {
		return created, partial, core.Fail(core.InvalidUpstreamResponse, nil)
	}
	urls := make(map[int]string, len(created.Parts))
	for _, part := range created.Parts {
		if part.Number < 1 || part.Number > len(parts) || urls[part.Number] != "" {
			return created, partial, core.Fail(core.InvalidUpstreamResponse, nil)
		}
		urls[part.Number] = part.URL
	}
	_, _, err = lease.Run(ctx, input, length, 10000, func(partCtx context.Context, index int, data []byte) error {
		target := urls[index]
		if target == "" {
			var out struct {
				Drive  string       `json:"drive_id"`
				ID     string       `json:"file_id"`
				Upload string       `json:"upload_id"`
				Parts  []uploadPart `json:"part_info_list"`
			}
			request := map[string]any{"drive_id": d.opts.DriveID, "file_id": created.ID, "upload_id": created.UploadID, "part_info_list": []uploadPart{{Number: index}}}
			var err error
			if d.website != nil {
				err = d.website.request(partCtx, "upload_url", request, &out)
			} else {
				err = d.post(partCtx, "/adrive/v1.0/openFile/getUploadUrl", request, &out)
			}
			if err != nil {
				return err
			}
			if (d.website == nil || out.Drive != "") && out.Drive != d.opts.DriveID ||
				(d.website == nil || out.ID != "") && out.ID != created.ID ||
				(d.website == nil || out.Upload != "") && out.Upload != created.UploadID ||
				len(out.Parts) != 1 || out.Parts[0].Number != index {
				return core.Fail(core.InvalidUpstreamResponse, nil)
			}
			target = out.Parts[0].URL
		}
		return d.sendPart(partCtx, target, data)
	})
	if err != nil {
		return created, partial, uploadError(err)
	}
	var completed nativeFile
	change, err = d.mutate(ctx, "/adrive/v1.0/openFile/complete", map[string]string{"drive_id": d.opts.DriveID, "file_id": created.ID, "upload_id": created.UploadID}, &completed)
	if change.Effect != core.Applied {
		if change.Effect == core.Unchanged {
			return created, partial, err
		}
		return created, change, err
	}
	if completed.ID != created.ID || completed.Drive != d.opts.DriveID || completed.Parent != parent || completed.Name != name || completed.Type != "file" || completed.Size == nil || *completed.Size != n {
		return created, core.Change{Effect: core.Unknown}, errors.Join(err, core.Fail(core.InvalidUpstreamResponse, nil))
	}
	return created, change, err
}

func (d *Driver) sendPart(ctx context.Context, target string, data []byte) error {
	u, err := url.Parse(target)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.Fragment != "" {
		return core.Fail(core.InvalidUpstreamResponse, nil)
	}
	if d.website != nil && (u.Port() != "" || !strings.HasSuffix(u.Hostname(), ".aliyuncs.com") && !strings.HasSuffix(u.Hostname(), ".aliyundrive.net")) {
		return core.Fail(core.InvalidUpstreamResponse, nil)
	}
	// A fresh isolated request never forwards provider bearer credentials.
	body := upload.Borrow(data)
	defer body.Close()
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, u.String(), body)
	if err != nil {
		return core.Fail(core.InvalidUpstreamResponse, nil)
	}
	req.ContentLength = int64(len(data))
	if len(data) == 0 {
		req.Body = http.NoBody
	}
	response, err := d.client.Do(req)
	if errors.Is(err, context.Canceled) {
		return context.Canceled
	}
	if err != nil {
		return httpcontent.UpstreamIOError(err)
	}
	closeErr := d.closeBody(response.Body)
	if response.StatusCode != http.StatusOK {
		return errors.Join(statusError(response.StatusCode, "", nil), closeErr)
	}
	return errors.Join(closeErr, ctx.Err())
}

var _ core.FileCreator = (*Driver)(nil)
