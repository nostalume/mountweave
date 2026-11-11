package s3

import (
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
	Replace                    bool              `json:",omitempty"` // Decode version-2 checkpoints only.
	When                       core.Condition    `json:",omitzero"`  // Decode version-3/4 checkpoints only.
	Policy                     *core.WritePolicy `json:"policy,omitempty"`
}

type uploadState struct {
	Phase, Token, UploadID string
	Parts                  []types.CompletedPart
	Confirmed              int
	Evidence               core.Evidence
}

func (s *uploadState) acknowledge(number int, tag *string) {
	if number > len(s.Parts) {
		s.Parts = append(s.Parts, make([]types.CompletedPart, number-len(s.Parts))...)
	}
	if s.Parts[number-1].ETag == nil {
		s.Confirmed++
	}
	s.Parts[number-1] = types.CompletedPart{ETag: tag, PartNumber: aws.Int32(int32(number))}
}

func resumeError(message string) error { return core.Fail(core.UpstreamFailure, errors.New(message)) }

// ContinueWrite uses the same native multipart flow as ordinary buffered writes.
// The protected checkpoint consumer owns retained sessions; ordinary Cleanup does
// not abort them. Direct creation retains its policy and never overwrites.
func (d *Driver) ContinueWrite(ctx context.Context, p core.NativeAddress, source core.Replayable, length int64, options core.WriteOptions) (core.Change, error) {
	unchanged := core.Change{Effect: core.Unchanged}
	if options.ObserveTarget {
		return unchanged, core.Fail(core.UnsupportedOperation, nil)
	}
	if err := options.Policy.Validate(); err != nil {
		return unchanged, err
	}
	when := options.Policy.When()
	if !options.Policy.Complete() {
		when = core.IfAbsent()
	}
	match, none, err := nativeCondition(when, false)
	if err != nil {
		return unchanged, err
	}
	if d.buffer == nil || d.opts.ResumeScope == "" {
		return unchanged, core.Fail(core.UnsupportedOperation, nil)
	}
	c := options.Continuation
	if length < 0 || source == nil || source.SourceVersion() == "" || c == nil || c.Append == nil {
		return unchanged, core.Fail(core.InvalidRequest, nil)
	}
	if err := d.target(ctx, p); err != nil {
		return unchanged, err
	}
	count := partsFor(length, d.opts.PartSize)
	if count > 10000 {
		return unchanged, core.Fail(core.UnsupportedOperation, nil)
	}
	policy := options.Policy
	binding := resumeBinding{Scope: d.opts.ResumeScope, Bucket: d.bucket, Key: d.key(p), Source: source.SourceVersion(), Length: length, PartSize: d.opts.PartSize, Policy: &policy}
	state, err := restoreUpload(c.Checkpoint, binding, count)
	if err != nil {
		return unchanged, err
	}
	write := multipartWrite{driver: d, key: binding.Key, when: when, match: match, none: none, state: state, binding: binding, continuation: c}
	return write.run(ctx, p, source, &length)
}

func partsFor(length, size int64) int {
	count := length / size
	if length%size != 0 {
		count++
	}
	// The native limit is checked before converting a count to int.
	return int(min(max(count, 1), 10001))
}

// multipartWrite owns one native attempt. Its source and buffers remain borrowed;
// only the optional recorder changes who owns the session after an attempt ends.
type multipartWrite struct {
	driver       *Driver
	key          string
	when         core.Condition
	match, none  *string
	state        uploadState
	binding      resumeBinding
	continuation *core.WriteContinuation
}

func (w *multipartWrite) save(ctx context.Context) error {
	if w.continuation == nil {
		return nil
	}
	record := uploadRecord{Version: 5, Kind: "phase", Phase: w.state.Phase, UploadID: w.state.UploadID}
	if w.state.Phase == "done" {
		record.Evidence = &w.state.Evidence
	}
	if w.state.Phase == "initializing" {
		record.Kind, record.Binding, record.Token = "start", &w.binding, w.state.Token
	}
	return appendUpload(ctx, w.continuation.Append, record)
}

func (w *multipartWrite) run(ctx context.Context, p core.NativeAddress, input io.Reader, length *int64) (result core.Change, err error) {
	result.Effect = core.Unchanged
	d := w.driver
	durable := w.continuation != nil
	acknowledged := context.WithoutCancel(ctx)
	count := 0
	if length != nil {
		count = partsFor(*length, d.opts.PartSize)
	}
	if durable {
		switch w.state.Phase {
		case "done":
			return core.Change{Effect: core.Applied, Evidence: w.state.Evidence}, nil
		case "aborted":
			if w.continuation.Abort {
				return result, nil
			}
			return result, core.Fail(core.InvalidRequest, errors.New("upload intent was aborted"))
		case "committing":
			// Missing/different content cannot disprove a prior publication.
			out, e := d.client.HeadObject(ctx, &sdk.HeadObjectInput{Bucket: aws.String(d.bucket), Key: aws.String(w.key)})
			if e == nil && out.Metadata["mountweave-upload"] == w.state.Token && out.ContentLength != nil && *out.ContentLength == *length {
				if observed := evidence(out.ETag, out.VersionId); observed.Validator.Valid() {
					w.state.Phase, w.state.Evidence = "done", observed
					return core.Change{Effect: core.Applied, Evidence: observed}, w.save(acknowledged)
				}
			}
			return core.Change{Effect: core.Unknown}, errors.Join(resumeError("multipart publication is unresolved"), translate(e))
		case "initializing":
			return result, resumeError("multipart initiation is unresolved")
		}
		if w.state.Phase == "aborting" && !w.continuation.Abort {
			return result, resumeError("upload retirement requires Abort")
		}
		if w.continuation.Abort || w.state.Phase == "aborting" {
			if w.state.Phase == "" {
				return result, nil
			}
			if w.state.Phase != "aborting" {
				w.state.Phase = "aborting"
				if err = w.save(ctx); err != nil {
					return
				}
			}
			if err = d.cancelMultipart(ctx, w.key, w.state.UploadID, true); err != nil {
				return
			}
			w.state.Phase = "aborted"
			return result, w.save(acknowledged)
		}
		if directory, e := d.directoryAt(ctx, p); e != nil {
			return result, e
		} else if directory {
			return result, core.Fail(core.NotFile, nil)
		}
	}
	var lease *upload.Lease
	if !durable || w.state.Confirmed < count {
		lease, err = d.buffer.Acquire(ctx, d.opts.PartSize, count-w.state.Confirmed)
		if err != nil {
			return result, inputError(err)
		}
		defer lease.Release()
	}
	if w.state.Phase == "" {
		w.state.Phase, w.state.Token = "initializing", rand.Text()
		if err = w.save(ctx); err != nil {
			return
		}
		if err = ctx.Err(); err != nil {
			w.state.Phase = ""
			return result, errors.Join(err, w.save(acknowledged))
		}
		request := &sdk.CreateMultipartUploadInput{Bucket: aws.String(d.bucket), Key: aws.String(w.key)}
		if durable {
			request.Metadata = map[string]string{"mountweave-upload": w.state.Token}
		}
		out, e := d.client.CreateMultipartUpload(ctx, request, noReplay)
		if e != nil {
			var status interface{ HTTPStatusCode() int }
			refused := errors.As(e, &status) && status.HTTPStatusCode() >= 400 && status.HTTPStatusCode() < 500 && status.HTTPStatusCode() != 408
			if refused {
				w.state.Phase = ""
				return result, errors.Join(translate(e), w.save(acknowledged))
			} else if !durable {
				d.savePending(PendingUpload{ID: w.state.Token, Key: w.key, cause: translate(e)})
			}
			return result, translate(e)
		}
		if aws.ToString(out.UploadId) == "" {
			err = core.Fail(core.InvalidUpstreamResponse, nil)
			if !durable {
				d.savePending(PendingUpload{ID: w.state.Token, Key: w.key, cause: err})
			}
			return
		}
		w.state.Phase, w.state.UploadID = "uploading", *out.UploadId
		if err = w.save(acknowledged); err != nil {
			return
		}
	}
	intent := PendingUpload{ID: w.state.Token, Key: w.key, UploadID: w.state.UploadID}
	complete := false
	defer func() {
		if durable || complete {
			return
		}
		d.savePending(intent)
		cleanup := context.WithoutCancel(ctx)
		if d.opts.CleanupTimeout > 0 {
			var cancel context.CancelFunc
			cleanup, cancel = context.WithTimeout(cleanup, d.opts.CleanupTimeout)
			defer cancel()
		}
		err = errors.Join(err, d.abort(cleanup, intent))
	}()
	if lease != nil {
		var missing []int
		var ranges *missingInput
		runLength := length
		if durable {
			var remaining int64
			for n := 1; n <= count; n++ {
				if n > len(w.state.Parts) || w.state.Parts[n-1].ETag == nil {
					missing = append(missing, n)
					remaining += min(d.opts.PartSize, *length-int64(n-1)*d.opts.PartSize)
				}
			}
			ranges = &missingInput{source: input.(core.Replayable), ctx: ctx, parts: missing, size: *length, partSize: d.opts.PartSize}
			input, runLength = ranges, &remaining
		}
		var receipts sync.Mutex
		total, sent, e := lease.Run(ctx, input, runLength, 10000, func(partCtx context.Context, index int, data []byte) error {
			number := index
			if durable {
				number = missing[index-1]
			}
			tag, e := d.sendPart(partCtx, w.key, w.state.UploadID, number, data)
			if e != nil {
				return e
			}
			receipts.Lock()
			defer receipts.Unlock()
			w.state.acknowledge(number, tag)
			if durable {
				return appendUpload(context.WithoutCancel(partCtx), w.continuation.Append, uploadRecord{Version: 5, Kind: "part", Part: number, ETag: *tag})
			}
			return nil
		})
		intent.Bytes = total
		if ranges != nil {
			e = errors.Join(e, ranges.Close())
		}
		if e != nil {
			return result, inputError(e)
		}
		if !durable {
			count = sent
		}
	}
	if w.state.Confirmed != count {
		return result, core.Fail(core.InvalidUpstreamResponse, errors.New("multipart receipts are incomplete"))
	}
	w.state.Phase = "committing"
	if err = w.save(ctx); err != nil {
		return
	}
	if err = ctx.Err(); err != nil {
		w.state.Phase = "uploading"
		return result, errors.Join(err, w.save(acknowledged))
	}
	request := &sdk.CompleteMultipartUploadInput{Bucket: aws.String(d.bucket), Key: aws.String(w.key), UploadId: aws.String(w.state.UploadID), MultipartUpload: &types.CompletedMultipartUpload{Parts: w.state.Parts}}
	request.IfMatch, request.IfNoneMatch = w.match, w.none
	ack, e := d.client.CompleteMultipartUpload(ctx, request, noReplay)
	result, err = mutationResult(ctx, e, !w.when.Empty())
	if core.CodeOf(err) == core.AlreadyExists && !w.when.IsAbsent() {
		err = core.Fail(core.PreconditionFailed, errors.Join(&core.ConditionFailure{}, err))
	}
	if e != nil {
		if durable {
			var status interface{ HTTPStatusCode() int }
			if errors.As(e, &status) && status.HTTPStatusCode() == 412 {
				w.state.Phase = "uploading"
				return result, errors.Join(err, w.save(acknowledged))
			}
			result.Effect = core.Unknown
		}
		return
	}
	complete = true
	w.state.Evidence = evidence(ack.ETag, ack.VersionId)
	result.Evidence = w.state.Evidence
	if ack.ETag == nil || *ack.ETag == "" || durable && !w.state.Evidence.Validator.Valid() {
		return core.Change{Effect: core.Unknown}, errors.Join(err, core.Fail(core.InvalidUpstreamResponse, nil))
	}
	w.state.Phase = "done"
	if w.when.IsAbsent() {
		result.Before = core.TargetMissing
	}
	return result, errors.Join(err, w.save(acknowledged))
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
