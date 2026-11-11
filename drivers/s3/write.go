package s3

import (
	"context"
	"errors"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/s3"
	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/internal/upload"
)

// Create conditionally creates a file. Configured buffering enables multipart;
// otherwise it uses one PUT with an exact, known input length.
func (d *Driver) Create(ctx context.Context, p core.NativeAddress, input io.Reader, length *int64) (core.Change, error) {
	if d.buffer != nil {
		return d.bufferedWrite(ctx, p, input, length, core.PublicationOptions{When: core.IfAbsent()})
	}
	unchanged := core.Change{Effect: core.Unchanged}
	if input == nil || (length != nil && *length < 0) {
		return unchanged, core.Fail(core.InvalidRequest, nil)
	}
	if err := d.admit(ctx, p); err != nil {
		return unchanged, err
	}
	if p.Namespace() == core.TreeNamespace && p.Value() == "/" {
		return unchanged, core.Fail(core.InvalidRequest, nil)
	}
	if length == nil || *length > 5<<30 {
		return unchanged, core.Fail(core.UnsupportedOperation, nil)
	}
	if err := d.ancestors(ctx, p); err != nil {
		return unchanged, err
	}
	directory, err := d.directoryAt(ctx, p)
	if err != nil {
		return unchanged, err
	}
	if directory {
		return unchanged, core.Fail(core.AlreadyExists, nil)
	}
	body := upload.NewBody(ctx, input, length)
	if *length == 0 {
		if _, err := body.Read(make([]byte, 1)); err != io.EOF {
			body.Finish()
			return unchanged, err
		}
	}
	ack, putErr := d.client.PutObject(ctx, &sdk.PutObjectInput{
		Bucket: aws.String(d.bucket), Key: aws.String(d.key(p)), Body: body,
		ContentLength: length, IfNoneMatch: aws.String("*"),
	}, noReplay)
	complete, inputErr := body.Finish()
	err = errors.Join(inputErr, ctx.Err())
	if putErr != nil {
		result, putErr := mutationResult(ctx, putErr, true)
		return result, errors.Join(putErr, inputErr)
	}
	if inputErr != nil {
		return core.Change{Effect: core.Incomplete}, err
	}
	if !complete {
		return core.Change{Effect: core.Unknown}, errors.Join(core.Fail(core.InvalidUpstreamResponse, errors.New("upload did not consume the complete input")), err)
	}
	return core.Change{Effect: core.Applied, Evidence: evidence(ack.ETag, ack.VersionId)}, err
}

var _ core.FileCreator = (*Driver)(nil)
