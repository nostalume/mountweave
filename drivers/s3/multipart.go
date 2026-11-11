package s3

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sort"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/s3"
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
	if err = d.cancelMultipart(ctx, p.Key, p.UploadID, false); err != nil {
		return err
	}
	d.forgetPending(p.ID)
	return nil
}

// Explicit retirement needs session absence; ordinary private cleanup also
// accepts an acknowledged empty part set after its own requests have joined.
func (d *Driver) cancelMultipart(ctx context.Context, key, id string, terminal bool) error {
	_, abortErr := d.client.AbortMultipartUpload(ctx, &sdk.AbortMultipartUploadInput{Bucket: aws.String(d.bucket), Key: aws.String(key), UploadId: aws.String(id)}, noReplay)
	if !terminal && abortErr != nil && core.CodeOf(translate(abortErr)) != core.NotFound {
		return translate(abortErr)
	}
	listed, listErr := d.client.ListParts(ctx, &sdk.ListPartsInput{Bucket: aws.String(d.bucket), Key: aws.String(key), UploadId: aws.String(id), MaxParts: aws.Int32(1)})
	if core.CodeOf(translate(listErr)) == core.NotFound {
		return nil
	}
	if listErr == nil && !terminal && listed.IsTruncated != nil && !aws.ToBool(listed.IsTruncated) && len(listed.Parts) == 0 {
		return nil
	}
	return errors.Join(core.Fail(core.UpstreamFailure, errors.New("multipart retirement is unresolved")), translate(abortErr), translate(listErr))
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
	if length != nil && partsFor(*length, d.opts.PartSize) > 10000 {
		return result, core.Fail(core.UnsupportedOperation, nil)
	}
	if length != nil && *length <= d.opts.PartSize {
		lease, e := d.buffer.Acquire(ctx, max(1, *length), 1)
		if e != nil {
			return result, inputError(e)
		}
		defer lease.Release()
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
	write := multipartWrite{driver: d, key: d.key(p), when: options.When, match: match, none: none}
	return write.run(ctx, p, input, length)
}

func (d *Driver) sendPart(ctx context.Context, key, uploadID string, number int, data []byte) (*string, error) {
	var last error
	for attempt := 0; attempt < d.opts.PartAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		part, err := d.client.UploadPart(ctx, &sdk.UploadPartInput{Bucket: aws.String(d.bucket), Key: aws.String(key), UploadId: aws.String(uploadID), PartNumber: aws.Int32(int32(number)), Body: bytes.NewReader(data), ContentLength: aws.Int64(int64(len(data)))}, noReplay)
		if err == nil {
			if aws.ToString(part.ETag) == "" {
				return nil, core.Fail(core.InvalidUpstreamResponse, nil)
			}
			return part.ETag, nil
		}
		last = translate(err)
		code := core.CodeOf(last)
		if code != core.UpstreamFailure && code != core.UpstreamTimeout && code != core.UpstreamThrottled {
			return nil, last
		}
	}
	return nil, last
}

var _ core.FilePublisher = (*Driver)(nil)
