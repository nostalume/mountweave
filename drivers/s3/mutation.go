package s3

import (
	"bytes"
	"context"
	"errors"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	sdk "github.com/aws/aws-sdk-go-v2/service/s3"
	core "github.com/nostalume/mountweave"
)

func noReplay(o *sdk.Options) {
	o.Retryer = aws.NopRetryer{}
	o.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
	o.APIOptions = append(o.APIOptions, v4.SwapComputePayloadSHA256ForUnsignedPayloadMiddleware)
}
func mutationResult(ctx context.Context, err error, conditional bool) (core.Change, error) {
	if err == nil {
		return core.Change{Effect: core.Applied}, ctx.Err()
	}
	var status interface{ HTTPStatusCode() int }
	if conditional && errors.As(err, &status) && status.HTTPStatusCode() == 412 {
		return core.Change{Effect: core.Unchanged}, errors.Join(core.Fail(core.AlreadyExists, err), ctx.Err())
	}
	return core.Change{Effect: core.Unknown}, errors.Join(translate(err), ctx.Err())
}
func (d *Driver) target(ctx context.Context, p string) error {
	if err := d.admit(ctx, p); err != nil {
		return err
	}
	if p == "/" {
		return core.Fail(core.InvalidRequest, nil)
	}
	return d.ancestors(ctx, p)
}
func (d *Driver) Mkdir(ctx context.Context, p string) (core.Change, error) {
	u := core.Change{Effect: core.Unchanged}
	if err := d.target(ctx, p); err != nil {
		return u, err
	}
	if _, err := d.statKey(ctx, d.key(p), name(p)); err == nil {
		return u, core.Fail(core.AlreadyExists, nil)
	} else if core.CodeOf(err) != core.NotFound {
		return u, err
	}
	_, err := d.client.PutObject(ctx, &sdk.PutObjectInput{Bucket: aws.String(d.bucket), Key: aws.String(d.key(p) + "/"), Body: bytes.NewReader(nil), ContentLength: aws.Int64(0), IfNoneMatch: aws.String("*")}, noReplay)
	return mutationResult(ctx, err, true)
}
func (d *Driver) Remove(ctx context.Context, p string) (core.Change, error) {
	u := core.Change{Effect: core.Unchanged}
	if err := d.target(ctx, p); err != nil {
		return u, err
	}
	meta, err := d.statKey(ctx, d.key(p), name(p))
	if err != nil {
		return u, err
	}
	// Prefix membership is independent of the marker object: S3 has no native
	// empty-prefix conditional removal. Do not turn it into recursive deletion.
	if meta.Kind != core.File {
		return u, core.Fail(core.UnsupportedOperation, nil)
	}
	if err = ctx.Err(); err != nil {
		return u, err
	}
	_, err = d.client.DeleteObject(ctx, &sdk.DeleteObjectInput{Bucket: aws.String(d.bucket), Key: aws.String(d.key(p))}, noReplay)
	return mutationResult(ctx, err, false)
}

func (d *Driver) putBuffer(ctx context.Context, key string, data []byte, replace bool) (core.Change, error) {
	request := &sdk.PutObjectInput{Bucket: aws.String(d.bucket), Key: aws.String(key), Body: bytes.NewReader(data), ContentLength: aws.Int64(int64(len(data)))}
	if !replace {
		request.IfNoneMatch = aws.String("*")
	}
	_, err := d.client.PutObject(ctx, request, noReplay)
	return mutationResult(ctx, err, !replace)
}

var _ core.DirectoryMaker = (*Driver)(nil)
var _ core.Remover = (*Driver)(nil)
