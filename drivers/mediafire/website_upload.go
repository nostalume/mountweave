package mediafire

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path"
	"regexp"
	"strings"
	"time"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/internal/upload"
)

const maxMediaFireUnit = 4 << 20

// WebsiteUploadOptions enables exclusive creation with a bounded local spool.
// StagingDirectory must be an existing private directory owned by the host.
type WebsiteUploadOptions struct {
	StagingDirectory string
	MaxStagingBytes  int64
}

func newWebsiteUpload(o *WebsiteUploadOptions) (*upload.Staging, error) {
	if o == nil {
		return nil, nil
	}
	return upload.NewStaging(o.StagingDirectory, o.MaxStagingBytes)
}

type uploadRequest struct {
	Parent, Name, Hash string
	Length             int64
	Key                string `json:",omitempty"`
	Unit               int    `json:",omitempty"`
	Data               []byte `json:",omitempty"`
}

type uploadReceipt struct {
	Key, FileKey, Name, Hash string
	UnitBytes, Size          int64
	Units, Terminal, Failure int
	Bitmap                   []uint16
	Ready, Exists            bool
}

var mediaFireUploadKey = regexp.MustCompile(`^[A-Za-z0-9._-]{1,256}$`)
var mediaFireFileKey = regexp.MustCompile(`^(?:[a-z0-9]{11}|[a-z0-9]{15})$`)

func (r uploadReceipt) geometry(length int64) bool {
	return r.UnitBytes > 0 && r.UnitBytes <= maxMediaFireUnit && r.Units > 0 &&
		int64(r.Units) == (length-1)/r.UnitBytes+1 &&
		len(r.Bitmap) == (r.Units-1)/16+1 && (r.Key == "" || mediaFireUploadKey.MatchString(r.Key))
}

func (r uploadReceipt) has(unit int) bool {
	return r.Bitmap[unit/16]&(1<<(unit%16)) != 0
}

func (w *Website) uploadCall(ctx context.Context, s *BrowserSession, operation string, request uploadRequest) (receipt uploadReceipt, err error) {
	if err := w.active(ctx); err != nil {
		return receipt, err
	}
	reply, err := s.request(ctx, browserRequest{Operation: operation, Account: w.website.account, Upload: &request})
	if err = websiteError(ctx, err); err != nil {
		return receipt, err
	}
	if err := w.website.checkAccount(reply.Account); err != nil {
		return receipt, err
	}
	if json.Unmarshal(reply.Body, &receipt) != nil {
		return receipt, badUploadReceipt()
	}
	return receipt, nil
}

func badUploadReceipt() error {
	return core.Fail(core.InvalidUpstreamResponse, errors.New("MediaFire did not confirm the upload receipt"))
}

// Create borrows input until return and publishes only at the exact absent name.
func (w *Website) Create(ctx context.Context, target core.NativeAddress, input io.Reader, length *int64) (change core.Change, err error) {
	change.Effect = core.Unchanged
	session, usable := w.website.session.(*BrowserSession)
	if w.upload == nil || !usable || target.Namespace() != core.TreeNamespace || target.Value() == "/" {
		return change, core.Fail(core.UnsupportedOperation, nil)
	}
	if err = core.ValidatePath(target.Value()); err != nil {
		return change, err
	}
	if length != nil && *length == 0 {
		return change, core.Fail(core.UnsupportedOperation, errors.New("MediaFire does not upload empty files"))
	}
	parent, err := w.lookup(ctx, core.NativePath(path.Dir(target.Value())))
	if err != nil {
		return change, err
	}
	if parent.meta.Kind != core.Directory {
		return change, core.Fail(core.NotDirectory, nil)
	}

	if err := w.active(ctx); err != nil {
		return change, err
	}
	file, err := w.upload.Stage(ctx, input, length)
	if err != nil {
		return change, err
	}
	defer func() { err = errors.Join(err, file.Close()) }()
	size := file.Size
	if size == 0 {
		return change, core.Fail(core.UnsupportedOperation, errors.New("MediaFire does not upload empty files"))
	}
	request := uploadRequest{Parent: parent.key, Name: path.Base(target.Value()), Length: size, Hash: file.SHA256}
	spec, err := w.uploadCall(ctx, session, "upload-check", request)
	if err != nil {
		return w.uploadRejection(ctx, target, err)
	}
	if spec.Exists {
		return core.Change{Effect: core.Unchanged, Before: core.TargetPresent}, core.Fail(core.AlreadyExists, nil)
	}
	if !spec.geometry(size) || spec.Ready {
		return w.uploadRejection(ctx, target, badUploadReceipt())
	}
	for _, word := range spec.Bitmap {
		if word != 0 {
			return change, core.Fail(core.UpstreamFailure, errors.New("MediaFire already has units for the same upload"))
		}
	}
	request.Key = spec.Key
	unit := make([]byte, min(spec.UnitBytes, size))
	for i, offset := 0, int64(0); i < spec.Units; i, offset = i+1, offset+spec.UnitBytes {
		request.Unit, request.Data = i, unit[:int(min(spec.UnitBytes, size-offset))]
		if _, err = file.ReadAt(request.Data, offset); err != nil {
			return change, err
		}
		if err = w.active(ctx); err != nil {
			return change, err
		}
		final := i == spec.Units-1
		received, uploadErr := w.uploadCall(ctx, session, "upload-unit", request)
		key, uploadErr := unitAcknowledgment(request.Key, spec, i, final, received, uploadErr)
		if final {
			if key != "" {
				request.Key = key
			}
			request.Data = nil
			if request.Key == "" {
				return core.Change{Effect: core.Unknown}, uploadErr
			}
			return w.confirmUpload(ctx, session, target, request, uploadErr)
		}
		if uploadErr != nil {
			key, uploadErr = w.recheckUnit(ctx, session, request, spec, uploadErr)
		}
		if uploadErr != nil {
			return w.uploadRejection(ctx, target, uploadErr)
		}
		request.Key = key
	}
	return change, badUploadReceipt()
}

func (w *Website) uploadRejection(ctx context.Context, target core.NativeAddress, failure error) (core.Change, error) {
	if core.CodeOf(failure) == core.InvalidUpstreamResponse {
		if _, err := w.lookup(ctx, target); err == nil {
			failure = core.Fail(core.AlreadyExists, nil)
		}
	}
	result := core.Change{Effect: core.Unchanged}
	if core.CodeOf(failure) == core.AlreadyExists {
		result.Before = core.TargetPresent
	}
	return result, failure
}

func unitAcknowledgment(key string, spec uploadReceipt, unit int, final bool, got uploadReceipt, failure error) (string, error) {
	if failure != nil {
		return "", failure
	}
	if got.UnitBytes != spec.UnitBytes || got.Units != spec.Units || len(got.Bitmap) != len(spec.Bitmap) ||
		!mediaFireUploadKey.MatchString(got.Key) || key != "" && got.Key != key || !got.has(unit) || got.Ready != final {
		return "", badUploadReceipt()
	}
	return got.Key, nil
}

func (w *Website) recheckUnit(ctx context.Context, s *BrowserSession, req uploadRequest, spec uploadReceipt, prior error) (string, error) {
	unit := req.Data
	req.Data = nil
	got, err := w.uploadCall(ctx, s, "upload-check", req)
	if err != nil {
		return "", errors.Join(prior, err)
	}
	if got.Exists {
		return "", core.Fail(core.AlreadyExists, nil)
	}
	if !got.geometry(req.Length) || got.UnitBytes != spec.UnitBytes || got.Units != spec.Units ||
		req.Key != "" && got.Key != req.Key || got.Ready {
		return "", errors.Join(prior, badUploadReceipt())
	}
	if got.has(req.Unit) {
		if got.Key == "" {
			return "", errors.Join(prior, badUploadReceipt())
		}
		return got.Key, nil
	}
	if got.Key != "" {
		req.Key = got.Key
	}
	req.Data = unit
	received, err := w.uploadCall(ctx, s, "upload-unit", req)
	return unitAcknowledgment(req.Key, spec, req.Unit, false, received, err)
}

func (w *Website) confirmUpload(ctx context.Context, s *BrowserSession, target core.NativeAddress, request uploadRequest, prior error) (core.Change, error) {
	unknown := core.Change{Effect: core.Unknown}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	for {
		got, err := w.uploadCall(ctx, s, "upload-poll", request)
		if err != nil {
			return unknown, errors.Join(prior, err)
		}
		if got.Terminal == 99 {
			if got.Failure == 13 && got.FileKey == "" {
				return core.Change{Effect: core.Unchanged, Before: core.TargetPresent}, core.Fail(core.AlreadyExists, nil)
			}
			if got.Failure != 0 {
				return unknown, core.Fail(core.UpstreamFailure, fmt.Errorf("MediaFire upload failed with code %d", got.Failure))
			}
			if !mediaFireFileKey.MatchString(got.FileKey) || got.Name != request.Name ||
				got.Size != request.Length || !strings.EqualFold(got.Hash, request.Hash) {
				return unknown, badUploadReceipt()
			}
			item, err := w.lookup(ctx, target)
			if err != nil || item.key != got.FileKey || item.meta.Kind != core.File ||
				item.meta.Size == nil || *item.meta.Size != request.Length {
				return unknown, errors.Join(badUploadReceipt(), err)
			}
			return core.Change{Effect: core.Applied, Before: core.TargetMissing}, nil
		}
		timer := time.NewTimer(500 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return unknown, errors.Join(prior, ctx.Err())
		case <-timer.C:
		}
	}
}

var _ core.FileCreator = (*Website)(nil)
