package s3

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"io"
	"sort"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/internal/upload"
)

// PendingUpload is an owned private resource, not a resumable client token.
// ID identifies the local attempt; an empty UploadID means initiation was lost.
// Bytes is a conservative count of submitted payload, not provider quota usage.
type PendingUpload struct {
	ID, Key, UploadID string
	Bytes             int64
	cause             error
}

func (d *Driver) PendingUploads() []PendingUpload {
	d.mu.Lock()
	defer d.mu.Unlock()
	items := make([]PendingUpload, 0, len(d.pending))
	for _, p := range d.pending {
		items = append(items, p)
	}
	sort.Slice(items, func(i, j int) bool { return items[i].ID < items[j].ID })
	return items
}
func (d *Driver) savePending(p PendingUpload) { d.mu.Lock(); d.pending[p.ID] = p; d.mu.Unlock() }
func (d *Driver) forgetPending(id string)     { d.mu.Lock(); delete(d.pending, id); d.mu.Unlock() }

// Cleanup retries only recorded immutable upload identities, never file names.
// Call after active writes finish. Unknown initiation needs external native
// reconciliation; no list/Stat absence is promoted to a confirmed outcome.
func (d *Driver) Cleanup(ctx context.Context) error {
	var err error
	for _, p := range d.PendingUploads() {
		err = errors.Join(err, d.abort(ctx, p))
	}
	return err
}
func (d *Driver) abort(ctx context.Context, p PendingUpload) (err error) {
	defer func() {
		if err != nil {
			if p.cause == nil {
				p.cause = err
			}
			d.savePending(p)
		}
	}()
	if p.UploadID == "" {
		return core.Fail(core.UpstreamFailure, errors.New("multipart initiation requires native reconciliation"))
	}
	_, abortErr := d.client.AbortMultipartUpload(ctx, &sdk.AbortMultipartUploadInput{Bucket: aws.String(d.bucket), Key: aws.String(p.Key), UploadId: aws.String(p.UploadID)}, noReplay)
	if abortErr != nil && core.CodeOf(translate(abortErr)) != core.NotFound {
		return translate(abortErr)
	}
	listed, err := d.client.ListParts(ctx, &sdk.ListPartsInput{Bucket: aws.String(d.bucket), Key: aws.String(p.Key), UploadId: aws.String(p.UploadID), MaxParts: aws.Int32(1)})
	if err == nil && (listed.IsTruncated == nil || aws.ToBool(listed.IsTruncated) || len(listed.Parts) > 0) {
		return core.Fail(core.UpstreamFailure, errors.New("multipart cleanup is not confirmed"))
	}
	if err != nil && core.CodeOf(translate(err)) != core.NotFound {
		return translate(err)
	}
	d.forgetPending(p.ID)
	return nil
}
func inputError(err error) error {
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
func (d *Driver) Publish(ctx context.Context, p core.NativeAddress, input io.Reader, length *int64, options core.PublicationOptions) (core.Change, error) {
	return d.bufferedWrite(ctx, p, input, length, options)
}
func (d *Driver) bufferedWrite(ctx context.Context, p core.NativeAddress, input io.Reader, length *int64, options core.PublicationOptions) (result core.Change, err error) {
	replace := !options.When.IsAbsent()
	match, none, conditionErr := nativeCondition(options.When, false)
	if conditionErr != nil {
		return core.Change{Effect: core.Unchanged}, conditionErr
	}
	before := core.TargetUnknown
	defer func() {
		if !replace && result.Effect == core.Applied {
			before = core.TargetMissing
		}
		if options.When.Match() != "" && result.Effect == core.Applied {
			before = core.TargetPresent
		}
		result.Before = before
	}()
	result.Effect = core.Unchanged
	if input == nil || (length != nil && *length < 0) {
		return result, core.Fail(core.InvalidRequest, nil)
	}
	if err = d.target(ctx, p); err != nil {
		return
	}
	if d.buffer == nil {
		return result, core.Fail(core.UnsupportedOperation, nil)
	}
	if length != nil {
		size := *length
		length = &size
	}
	d.mu.Lock()
	pending := len(d.pending)
	d.mu.Unlock()
	if pending != 0 {
		return result, core.Fail(core.UpstreamFailure, errors.New("multipart cleanup remains unresolved"))
	}
	if directory, e := d.directoryAt(ctx, p); e != nil {
		return result, e
	} else if directory {
		code := core.AlreadyExists
		if replace {
			code = core.NotFile
		}
		return result, core.Fail(code, nil)
	}
	if options.ObserveTarget && options.When.Empty() {
		_, headErr := d.client.HeadObject(ctx, &sdk.HeadObjectInput{Bucket: aws.String(d.bucket), Key: aws.String(d.key(p))})
		before = core.TargetPresent
		if headErr != nil {
			if core.CodeOf(translate(headErr)) != core.NotFound {
				return result, translate(headErr)
			}
			before = core.TargetMissing
		}
	}
	parts := 0
	if length != nil {
		n := *length / d.opts.PartSize
		if *length%d.opts.PartSize != 0 {
			n++
		}
		if n > 10000 {
			return result, core.Fail(core.UnsupportedOperation, nil)
		}
		parts = max(1, int(n))
	}
	lease, e := d.buffer.Acquire(ctx, parts)
	if e != nil {
		return result, inputError(e)
	}
	defer lease.Release()
	if length != nil && *length <= d.opts.PartSize {
		data := lease.Buffer()[:int(*length)]
		if err = upload.ReadExact(ctx, input, data); err != nil {
			return result, inputError(err)
		}
		if err = ctx.Err(); err != nil {
			return
		}
		err = lease.Send(ctx, func() error {
			result, err = d.putBuffer(ctx, d.key(p), data, options.When)
			return err
		})
		return
	}
	intent := PendingUpload{ID: rand.Text(), Key: d.key(p)}
	out, e := d.client.CreateMultipartUpload(ctx, &sdk.CreateMultipartUploadInput{Bucket: aws.String(d.bucket), Key: aws.String(intent.Key)}, noReplay)
	if e != nil {
		var status interface{ HTTPStatusCode() int }
		if !(errors.As(e, &status) && status.HTTPStatusCode() >= 400 && status.HTTPStatusCode() < 500 && status.HTTPStatusCode() != 408) {
			intent.cause = translate(e)
			d.savePending(intent)
		}
		return result, translate(e)
	}
	if out.UploadId == nil || *out.UploadId == "" {
		intent.cause = core.Fail(core.InvalidUpstreamResponse, nil)
		d.savePending(intent)
		return result, intent.cause
	}
	intent.UploadID = *out.UploadId
	complete := false
	defer func() {
		if complete {
			return
		}
		d.savePending(intent)
		cleanupCtx := context.WithoutCancel(ctx)
		if d.opts.CleanupTimeout > 0 {
			var cancel context.CancelFunc
			cleanupCtx, cancel = context.WithTimeout(cleanupCtx, d.opts.CleanupTimeout)
			defer cancel()
		}
		err = errors.Join(err, d.abort(cleanupCtx, intent))
	}()
	// Receipt metadata grows with actual parts, independently of payload buffers.
	var receiptMu sync.Mutex
	receipts := make([]types.CompletedPart, 0, parts)
	total, count, e := lease.Run(ctx, input, length, 10000, func(partCtx context.Context, index int, data []byte) error {
		var last error
		for attempt := 0; attempt < d.opts.PartAttempts; attempt++ {
			if err := partCtx.Err(); err != nil {
				return err
			}
			part, err := d.client.UploadPart(partCtx, &sdk.UploadPartInput{Bucket: aws.String(d.bucket), Key: aws.String(intent.Key), UploadId: aws.String(intent.UploadID), PartNumber: aws.Int32(int32(index)), Body: bytes.NewReader(data), ContentLength: aws.Int64(int64(len(data)))}, noReplay)
			if err == nil {
				if part.ETag == nil || *part.ETag == "" {
					return core.Fail(core.InvalidUpstreamResponse, nil)
				}
				receiptMu.Lock()
				if index > len(receipts) {
					receipts = append(receipts, make([]types.CompletedPart, index-len(receipts))...)
				}
				receipts[index-1] = types.CompletedPart{ETag: part.ETag, PartNumber: aws.Int32(int32(index))}
				receiptMu.Unlock()
				return nil
			}
			last = translate(err)
			code := core.CodeOf(last)
			if code != core.UpstreamFailure && code != core.UpstreamTimeout && code != core.UpstreamThrottled {
				return last
			}
		}
		return last
	})
	intent.Bytes = total
	if e != nil {
		return result, inputError(e)
	}
	if err = ctx.Err(); err != nil {
		return
	}
	request := &sdk.CompleteMultipartUploadInput{Bucket: aws.String(d.bucket), Key: aws.String(intent.Key), UploadId: aws.String(intent.UploadID), MultipartUpload: &types.CompletedMultipartUpload{Parts: receipts[:count]}}
	request.IfMatch, request.IfNoneMatch = match, none
	ack, e := d.client.CompleteMultipartUpload(ctx, request, noReplay)
	result, err = mutationResult(ctx, e, !options.When.Empty())
	if core.CodeOf(err) == core.AlreadyExists && !options.When.IsAbsent() {
		err = core.Fail(core.PreconditionFailed, errors.Join(&core.ConditionFailure{}, err))
	}
	if e == nil {
		result.Evidence = evidence(ack.ETag, ack.VersionId)
		complete = true
		d.forgetPending(intent.ID)
		if ack.ETag == nil || *ack.ETag == "" {
			result.Effect = core.Unknown
			err = errors.Join(err, core.Fail(core.InvalidUpstreamResponse, nil))
		}
	}
	return
}

var _ core.FilePublisher = (*Driver)(nil)
