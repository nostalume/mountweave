package alipan

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"
	"sync"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/internal/upload"
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
func (d *Driver) Create(ctx context.Context, p string, input io.Reader, length *int64) (core.Change, error) {
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
	n := *length
	length = &n
	count := n / d.opts.PartSize
	if n%d.opts.PartSize != 0 {
		count++
	}
	if count > 10000 {
		return unchanged, core.Fail(core.UnsupportedOperation, nil)
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
	lease, err := d.buffer.Acquire(ctx, max(1, int(count)))
	if err != nil {
		return unchanged, uploadError(err)
	}
	defer lease.Release()
	if n <= d.opts.PartSize {
		data := lease.Buffer()[:int(n)]
		if err := upload.ReadExact(ctx, input, data); err != nil {
			return unchanged, uploadError(err)
		}
		input = bytes.NewReader(data)
	}
	parts := make([]uploadPart, max(1, int(count)))
	for i := range parts {
		parts[i].Number = i + 1
	}
	created, change, err := d.createEntry(ctx, parent.ID, name, "file", parts, length)
	if change.Effect != core.Applied {
		return change, err
	}
	if n == 0 && (created.Available || created.Rapid) {
		return change, err
	}
	partial := core.Change{Effect: core.Incomplete}
	if err != nil {
		return partial, err
	}
	if created.Rapid || created.UploadID == "" {
		return partial, core.Fail(core.InvalidUpstreamResponse, nil)
	}
	urls := make(map[int]string, len(created.Parts))
	for _, part := range created.Parts {
		if part.Number < 1 || part.Number > len(parts) || urls[part.Number] != "" {
			return partial, core.Fail(core.InvalidUpstreamResponse, nil)
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
			if err := d.post(partCtx, "/adrive/v1.0/openFile/getUploadUrl", map[string]any{"drive_id": d.opts.DriveID, "file_id": created.ID, "upload_id": created.UploadID, "part_info_list": []uploadPart{{Number: index}}}, &out); err != nil {
				return err
			}
			if out.Drive != d.opts.DriveID || out.ID != created.ID || out.Upload != created.UploadID || len(out.Parts) != 1 || out.Parts[0].Number != index {
				return core.Fail(core.InvalidUpstreamResponse, nil)
			}
			target = out.Parts[0].URL
		}
		return d.sendPart(partCtx, target, data)
	})
	if err != nil {
		return partial, uploadError(err)
	}
	var completed nativeFile
	change, err = d.mutate(ctx, "/adrive/v1.0/openFile/complete", map[string]string{"drive_id": d.opts.DriveID, "file_id": created.ID, "upload_id": created.UploadID}, &completed)
	if change.Effect != core.Applied {
		if change.Effect == core.Unchanged {
			return partial, err
		}
		return change, err
	}
	if completed.ID != created.ID || completed.Drive != d.opts.DriveID || completed.Parent != parent.ID || completed.Name != name || completed.Type != "file" || completed.Size == nil || *completed.Size != n {
		return core.Change{Effect: core.Unknown}, errors.Join(err, core.Fail(core.InvalidUpstreamResponse, nil))
	}
	return change, err
}

// partBody revokes late transport access before its payload buffer is reused.
type partBody struct {
	mu     sync.Mutex
	reader *bytes.Reader
}

func (b *partBody) Read(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.reader == nil {
		return 0, io.ErrClosedPipe
	}
	return b.reader.Read(p)
}
func (b *partBody) Close() error {
	b.mu.Lock()
	b.reader = nil
	b.mu.Unlock()
	return nil
}

func (d *Driver) sendPart(ctx context.Context, target string, data []byte) error {
	u, err := url.Parse(target)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.Fragment != "" {
		return core.Fail(core.InvalidUpstreamResponse, nil)
	}
	// A fresh isolated request never forwards provider bearer credentials.
	body := &partBody{reader: bytes.NewReader(data)}
	defer body.Close()
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, u.String(), body)
	if err != nil {
		return core.Fail(core.InvalidUpstreamResponse, err)
	}
	req.ContentLength = int64(len(data))
	if len(data) == 0 {
		req.Body = http.NoBody
	}
	response, err := d.client.Do(req)
	if err != nil {
		return networkError(err)
	}
	closeErr := d.closeBody(response.Body)
	if response.StatusCode != http.StatusOK {
		return errors.Join(statusError(response.StatusCode, "", nil), closeErr)
	}
	return errors.Join(closeErr, ctx.Err())
}

var _ core.FileCreator = (*Driver)(nil)
