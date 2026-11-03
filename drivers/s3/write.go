package s3

import (
	"context"
	"errors"
	"io"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	sdk "github.com/aws/aws-sdk-go-v2/service/s3"
	core "github.com/nostalume/mountweave"
)

// Create conditionally creates a file. Configured buffering enables multipart;
// otherwise it uses one PUT with an exact, known input length.
func (d *Driver) Create(ctx context.Context, p string, input io.Reader, length *int64) (core.Change, error) {
	if d.buffer != nil {
		return d.bufferedWrite(ctx, p, input, length, false)
	}
	unchanged := core.Change{Effect: core.Unchanged}
	if input == nil || (length != nil && *length < 0) {
		return unchanged, core.Fail(core.InvalidRequest, nil)
	}
	if err := d.admit(ctx, p); err != nil {
		return unchanged, err
	}
	if p == "/" {
		return unchanged, core.Fail(core.InvalidRequest, nil)
	}
	if length == nil || *length > 5<<30 {
		return unchanged, core.Fail(core.UnsupportedOperation, nil)
	}
	if err := d.ancestors(ctx, p); err != nil {
		return unchanged, err
	}
	directory, err := d.hasDirectory(ctx, d.key(p))
	if err != nil {
		return unchanged, err
	}
	if directory {
		return unchanged, core.Fail(core.AlreadyExists, nil)
	}
	body := &uploadBody{ctx: ctx, input: input, remaining: *length}
	if *length == 0 {
		if _, err := body.Read(make([]byte, 1)); err != io.EOF {
			body.finish()
			return unchanged, err
		}
	}
	_, putErr := d.client.PutObject(ctx, &sdk.PutObjectInput{
		Bucket: aws.String(d.bucket), Key: aws.String(d.key(p)), Body: body,
		ContentLength: length, IfNoneMatch: aws.String("*"),
	}, func(o *sdk.Options) {
		o.Retryer = aws.NopRetryer{}
		o.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
		o.APIOptions = append(o.APIOptions, v4.SwapComputePayloadSHA256ForUnsignedPayloadMiddleware)
	})
	complete, inputErr := body.finish()
	err = errors.Join(inputErr, ctx.Err())
	if putErr != nil {
		// A single conditional rejection proves this call did not create the key.
		var status interface{ HTTPStatusCode() int }
		if errors.As(putErr, &status) && status.HTTPStatusCode() == 412 {
			return unchanged, errors.Join(core.Fail(core.AlreadyExists, putErr), err)
		}
		return core.Change{Effect: core.Unknown}, errors.Join(translate(putErr), err)
	}
	if inputErr != nil {
		return core.Change{Effect: core.Incomplete}, err
	}
	if !complete {
		return core.Change{Effect: core.Unknown}, errors.Join(core.Fail(core.InvalidUpstreamResponse, errors.New("upload did not consume the complete input")), err)
	}
	return core.Change{Effect: core.Applied}, err
}

// uploadBody owns request access, not input. finish waits for a current Read and
// prevents late transport reads from retaining or accessing the borrowed input.
type uploadBody struct {
	mu        sync.Mutex
	ctx       context.Context
	input     io.Reader
	remaining int64
	closed    bool
	complete  bool
	err       error
}

func (b *uploadBody) Read(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(p) == 0 {
		return 0, nil
	}
	if b.closed {
		return 0, io.ErrClosedPipe
	}
	if b.err != nil {
		return 0, b.err
	}
	if b.complete {
		return 0, io.EOF
	}
	if err := b.ctx.Err(); err != nil {
		b.err = err
		return 0, err
	}
	if int64(len(p)) > b.remaining {
		p = p[:int(b.remaining)]
	}
	var n int
	var err error
	if b.remaining > 0 {
		n, err = b.input.Read(p)
		b.remaining -= int64(n)
		if err == io.EOF && b.remaining != 0 {
			err = io.ErrUnexpectedEOF
		}
		if err != nil && err != io.EOF {
			b.err = err
			return n, err
		}
	}
	if b.remaining == 0 {
		if err != io.EOF {
			var extra [1]byte
			count, end := io.ReadFull(b.input, extra[:])
			if count != 0 {
				err = core.Fail(core.InvalidRequest, errors.New("input exceeds declared length"))
			} else {
				err = end
			}
		}
		if err == nil || err == io.EOF {
			b.complete = true
		} else {
			b.err = err
		}
	}
	return n, err
}

func (b *uploadBody) finish() (bool, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.closed = true
	b.input = nil
	return b.complete, b.err
}

var _ core.FileCreator = (*Driver)(nil)
