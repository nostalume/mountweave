package s3

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"io"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/internal/upload"
)

type resumeBinding struct {
	Scope, Bucket, Key, Source string
	Length, PartSize           int64
	Replace                    bool           `json:",omitempty"` // Decode version-2 checkpoints only.
	When                       core.Condition `json:",omitzero"`
}
type uploadState struct {
	Phase, Token, UploadID string
	Parts                  map[int]string
	Evidence               core.Evidence
}

func resumeError(message string) error { return core.Fail(core.UpstreamFailure, errors.New(message)) }

// ContinueWrite retains native uploads on cancellation/failure. The protected
// checkpoint consumer owns them after return; ordinary PendingUploads/Cleanup
// never aborts these independently owned sessions. No credentials/URLs are saved.
func (d *Driver) ContinueWrite(ctx context.Context, p core.NativeAddress, source core.Replayable, length int64, options core.PublicationOptions, c core.WriteContinuation) (result core.Change, err error) {
	result.Effect = core.Unchanged
	// Observation for
	// a new HTTP response is not reconstructed from a durable prior attempt.
	if options.ObserveTarget {
		return result, core.Fail(core.UnsupportedOperation, nil)
	}
	match, none, err := nativeCondition(options.When, false)
	if err != nil {
		return result, err
	}
	if d.buffer == nil || d.opts.ResumeScope == "" {
		return result, core.Fail(core.UnsupportedOperation, nil)
	}
	if length < 0 || source == nil || source.SourceVersion() == "" || c.Append == nil {
		return result, core.Fail(core.InvalidRequest, nil)
	}
	if err = d.target(ctx, p); err != nil {
		return
	}
	count := length / d.opts.PartSize
	if length%d.opts.PartSize != 0 {
		count++
	}
	if count > 10000 {
		return result, core.Fail(core.UnsupportedOperation, nil)
	}
	count = max(count, 1)
	binding := resumeBinding{Scope: d.opts.ResumeScope, Bucket: d.bucket, Key: d.key(p), Source: source.SourceVersion(), Length: length, PartSize: d.opts.PartSize, When: options.When}
	state, err := restoreUpload(c.Checkpoint, binding, int(count))
	if err != nil {
		return result, err
	}
	save := func(saveCtx context.Context) error {
		record := uploadRecord{Version: 4, Kind: "phase", Phase: state.Phase, UploadID: state.UploadID}
		if state.Phase == "done" {
			record.Evidence = &state.Evidence
		}
		if state.Phase == "initializing" {
			record.Kind = "start"
			record.Binding = &binding
			record.Token = state.Token
		}
		return appendUpload(saveCtx, c.Append, record)
	}
	acknowledged := context.WithoutCancel(ctx)
	if state.Phase == "done" {
		return core.Change{Effect: core.Applied, Evidence: state.Evidence}, nil
	}
	if state.Phase == "aborted" {
		if c.Abort {
			return result, nil
		}
		return result, core.Fail(core.InvalidRequest, errors.New("upload intent was aborted"))
	}
	if state.Phase == "committing" {
		// NoSuchUpload or a missing/different object does not distinguish a lost
		// completion from a subsequent replacement/deletion. Never replay blindly.
		out, e := d.client.HeadObject(ctx, &sdk.HeadObjectInput{Bucket: aws.String(d.bucket), Key: aws.String(binding.Key)})
		if e == nil && out.Metadata["mountweave-upload"] == state.Token && out.ContentLength != nil && *out.ContentLength == length {
			if observed := evidence(out.ETag, out.VersionId); observed.Validator.Valid() {
				state.Phase, state.Evidence = "done", observed
				return core.Change{Effect: core.Applied, Evidence: observed}, save(acknowledged)
			}
		}
		return core.Change{Effect: core.Unknown}, errors.Join(resumeError("multipart publication is unresolved"), translate(e))
	}
	if state.Phase == "initializing" {
		return result, resumeError("multipart initiation is unresolved")
	}
	if state.Phase != "" && state.Phase != "uploading" && state.Phase != "aborting" {
		return result, core.Fail(core.InvalidRequest, nil)
	}
	if state.Phase != "" && state.UploadID == "" {
		return result, core.Fail(core.InvalidRequest, nil)
	}
	if state.Phase == "aborting" && !c.Abort {
		return result, resumeError("upload retirement requires Abort")
	}
	if c.Abort || state.Phase == "aborting" {
		if state.Phase == "" {
			return result, nil
		}
		if state.Phase != "aborting" {
			state.Phase = "aborting"
			if err = save(ctx); err != nil {
				return
			}
		}
		if err = ctx.Err(); err != nil {
			return
		}
		_, abortErr := d.client.AbortMultipartUpload(ctx, &sdk.AbortMultipartUploadInput{Bucket: aws.String(d.bucket), Key: aws.String(binding.Key), UploadId: aws.String(state.UploadID)}, noReplay)
		_, listErr := d.client.ListParts(ctx, &sdk.ListPartsInput{Bucket: aws.String(d.bucket), Key: aws.String(binding.Key), UploadId: aws.String(state.UploadID), MaxParts: aws.Int32(1)})
		if listErr == nil || core.CodeOf(translate(listErr)) != core.NotFound {
			return result, errors.Join(resumeError("multipart retirement is unresolved"), translate(abortErr), translate(listErr))
		}
		state.Phase = "aborted"
		return result, save(acknowledged)
	}
	// Recheck the ordinary namespace before creating/sending any new native data.
	if directory, e := d.directoryAt(ctx, p); e != nil {
		return result, e
	} else if directory {
		return result, core.Fail(core.NotFile, nil)
	}
	var lease *upload.Lease
	if missingCount := int(count) - len(state.Parts); missingCount > 0 {
		var acquireErr error
		lease, acquireErr = d.buffer.Acquire(ctx, missingCount)
		if acquireErr != nil {
			return result, inputError(acquireErr)
		}
		defer lease.Release()
	}
	if state.Phase == "" {
		state.Phase = "initializing"
		state.Token = rand.Text()
		if err = save(ctx); err != nil {
			return
		}
		if err = ctx.Err(); err != nil {
			state.Phase = ""
			err = errors.Join(err, save(acknowledged))
			return
		}
		out, e := d.client.CreateMultipartUpload(ctx, &sdk.CreateMultipartUploadInput{Bucket: aws.String(d.bucket), Key: aws.String(binding.Key), Metadata: map[string]string{"mountweave-upload": state.Token}}, noReplay)
		if e != nil {
			var status interface{ HTTPStatusCode() int }
			if errors.As(e, &status) && status.HTTPStatusCode() >= 400 && status.HTTPStatusCode() < 500 && status.HTTPStatusCode() != 408 {
				state.Phase = ""
				return result, errors.Join(translate(e), save(acknowledged))
			}
			return result, translate(e)
		}
		if aws.ToString(out.UploadId) == "" {
			return result, core.Fail(core.InvalidUpstreamResponse, nil)
		}
		state.UploadID = *out.UploadId
		state.Phase = "uploading"
		if err = save(acknowledged); err != nil {
			return
		}
	}
	missing := make([]int, 0, int(count)-len(state.Parts))
	var remaining int64
	for n := 1; n <= int(count); n++ {
		if _, ok := state.Parts[n]; !ok {
			missing = append(missing, n)
			remaining += min(d.opts.PartSize, length-int64(n-1)*d.opts.PartSize)
		}
	}
	if len(missing) > 0 {
		reader := &missingInput{source: source, ctx: ctx, parts: missing, size: length, partSize: d.opts.PartSize}
		var checkpointMu sync.Mutex
		_, _, err = lease.Run(ctx, reader, &remaining, 10000, func(partCtx context.Context, index int, data []byte) error {
			n := missing[index-1]
			var tag string
			var last error
			for attempt := 0; attempt < d.opts.PartAttempts; attempt++ {
				if e := partCtx.Err(); e != nil {
					return e
				}
				out, e := d.client.UploadPart(partCtx, &sdk.UploadPartInput{Bucket: aws.String(d.bucket), Key: aws.String(binding.Key), UploadId: aws.String(state.UploadID), PartNumber: aws.Int32(int32(n)), Body: bytes.NewReader(data), ContentLength: aws.Int64(int64(len(data)))}, noReplay)
				if e == nil {
					tag = aws.ToString(out.ETag)
					break
				}
				last = translate(e)
				code := core.CodeOf(last)
				if code != core.UpstreamFailure && code != core.UpstreamTimeout && code != core.UpstreamThrottled {
					return last
				}
			}
			if tag == "" {
				if last != nil {
					return last
				}
				return core.Fail(core.InvalidUpstreamResponse, nil)
			}
			checkpointMu.Lock()
			defer checkpointMu.Unlock()
			state.Parts[n] = tag
			return appendUpload(context.WithoutCancel(partCtx), c.Append, uploadRecord{Version: 4, Kind: "part", Part: n, ETag: tag})
		})
		err = errors.Join(inputError(err), reader.Close())
		if err != nil {
			return
		}
	}
	if err = ctx.Err(); err != nil {
		return
	}
	receipts := make([]types.CompletedPart, int(count))
	for n := 1; n <= int(count); n++ {
		tag := state.Parts[n]
		if tag == "" {
			return result, core.Fail(core.InvalidUpstreamResponse, nil)
		}
		receipts[n-1] = types.CompletedPart{ETag: aws.String(tag), PartNumber: aws.Int32(int32(n))}
	}
	state.Phase = "committing"
	if err = save(ctx); err != nil {
		return
	}
	if err = ctx.Err(); err != nil {
		state.Phase = "uploading"
		err = errors.Join(err, save(acknowledged))
		return
	}
	request := &sdk.CompleteMultipartUploadInput{Bucket: aws.String(d.bucket), Key: aws.String(binding.Key), UploadId: aws.String(state.UploadID), MultipartUpload: &types.CompletedMultipartUpload{Parts: receipts}}
	request.IfMatch, request.IfNoneMatch = match, none
	ack, e := d.client.CompleteMultipartUpload(ctx, request, noReplay)
	if e != nil {
		var status interface{ HTTPStatusCode() int }
		if errors.As(e, &status) && status.HTTPStatusCode() == 412 {
			state.Phase = "uploading"
			return result, errors.Join(core.Fail(core.PreconditionFailed, errors.Join(&core.ConditionFailure{Negative: options.When.IsAbsent()}, e)), save(acknowledged))
		}
		return core.Change{Effect: core.Unknown}, translate(e)
	}
	state.Evidence = evidence(ack.ETag, ack.VersionId)
	if !state.Evidence.Validator.Valid() {
		return core.Change{Effect: core.Unknown}, core.Fail(core.InvalidUpstreamResponse, nil)
	}
	state.Phase = "done"
	return core.Change{Effect: core.Applied, Evidence: state.Evidence}, save(acknowledged)
}

// missingInput concatenates only missing native ranges for the existing bounded
// upload pipeline. At most one source body is open, while send buffers retain
// their existing concurrency/retry lifetime. Source reads are joined by Run.
type missingInput struct {
	source         core.Replayable
	ctx            context.Context
	parts          []int
	size, partSize int64
	current        io.ReadCloser
}

func (r *missingInput) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	for {
		if err := r.ctx.Err(); err != nil {
			return 0, err
		}
		if r.current == nil {
			if len(r.parts) == 0 {
				return 0, io.EOF
			}
			offset := int64(r.parts[0]-1) * r.partSize
			body, err := r.source.OpenRange(r.ctx, offset, min(r.partSize, r.size-offset))
			if err != nil {
				return 0, err
			}
			r.current = body
			r.parts = r.parts[1:]
		}
		n, err := r.current.Read(p)
		if err == io.EOF {
			closeErr := r.current.Close()
			r.current = nil
			if closeErr != nil {
				return n, closeErr
			}
			if n > 0 {
				return n, nil
			}
			continue
		}
		return n, err
	}
}
func (r *missingInput) Close() error {
	if r.current != nil {
		err := r.current.Close()
		r.current = nil
		return err
	}
	return nil
}

var _ core.ContinuableWriter = (*Driver)(nil)
