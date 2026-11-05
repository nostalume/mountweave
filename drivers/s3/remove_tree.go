package s3

import (
	"context"
	"errors"
	"net/url"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	core "github.com/nostalume/mountweave"
)

// RemoveSubtree uses one finite native-key discovery, then batch deletion.
// Concurrent additions may remain; a final prefix observation confirms absence.
func (d *Driver) RemoveSubtree(ctx context.Context, p core.NativeAddress, limits core.TreeOptions) (core.Change, error) {
	change := core.Change{Effect: core.Unchanged}
	if p.Namespace() != core.TreeNamespace {
		return change, core.Fail(core.UnsupportedOperation, nil)
	}
	if limits.MaxEntries <= 0 || limits.MaxPlanBytes <= 0 || limits.Concurrency < 0 {
		return change, core.Fail(core.InvalidRequest, nil)
	}
	meta, err := d.Stat(ctx, p)
	if err != nil {
		return change, err
	}
	if p.Value() == "/" {
		return change, core.Fail(core.InvalidRequest, nil)
	}
	if meta.Kind != core.Directory {
		return change, core.Fail(core.NotDirectory, nil)
	}
	prefix := d.key(p) + "/"
	keys := make([]string, 0)
	seen := make(map[string]bool)
	marker, token := false, ""
	entries, planBytes := 1, 1
	for {
		out, err := d.client.ListObjectsV2(ctx, &sdk.ListObjectsV2Input{Bucket: aws.String(d.bucket), Prefix: aws.String(prefix), MaxKeys: aws.Int32(1000), EncodingType: types.EncodingTypeUrl, ContinuationToken: optionalString(token)})
		if err != nil {
			return change, translate(err)
		}
		next := aws.ToString(out.NextContinuationToken)
		if out.IsTruncated == nil || len(out.CommonPrefixes) != 0 || (aws.ToBool(out.IsTruncated) && (next == "" || next == token)) {
			return change, core.Fail(core.InvalidUpstreamResponse, nil)
		}
		if len(out.Contents) == 0 && aws.ToBool(out.IsTruncated) {
			entries++
		}
		for _, item := range out.Contents {
			key := aws.ToString(item.Key)
			if out.EncodingType == types.EncodingTypeUrl {
				key, err = url.PathUnescape(key)
			} else if out.EncodingType != "" {
				err = errors.New("unsupported key encoding")
			}
			if err != nil || !strings.HasPrefix(key, prefix) || seen[key] {
				return change, core.Fail(core.InvalidUpstreamResponse, err)
			}
			seen[key] = true
			if key == prefix {
				marker = true
				continue
			}
			entries++
			planBytes += len(key) - len(prefix)
			if entries > limits.MaxEntries || planBytes > limits.MaxPlanBytes {
				return change, core.Fail(core.CapacityExceeded, nil)
			}
			keys = append(keys, key)
		}
		if entries > limits.MaxEntries {
			return change, core.Fail(core.CapacityExceeded, nil)
		}
		if !aws.ToBool(out.IsTruncated) {
			break
		}
		token = next
	}
	if len(keys) == 0 && !marker {
		return change, core.Fail(core.NotFound, nil)
	}
	for start := 0; start < len(keys); start += 1000 {
		if err := ctx.Err(); err != nil {
			return change, err
		}
		batch := keys[start:min(start+1000, len(keys))]
		deleted, err := d.deleteKeys(ctx, batch)
		if deleted.Effect == core.Unknown {
			return deleted, err
		}
		if deleted.Effect == core.Applied || deleted.Effect == core.Incomplete {
			change.Effect = core.Incomplete
		}
		if err != nil {
			return change, err
		}
	}
	if marker {
		if err := ctx.Err(); err != nil {
			return change, err
		}
		_, err := d.client.DeleteObject(ctx, &sdk.DeleteObjectInput{Bucket: aws.String(d.bucket), Key: aws.String(prefix)}, noReplay)
		if err != nil {
			return core.Change{Effect: core.Unknown}, errors.Join(translate(err), ctx.Err())
		}
		change.Effect = core.Incomplete
	}
	if exists, err := d.hasDirectory(ctx, d.key(p)); err != nil {
		return core.Change{Effect: core.Unknown}, err
	} else if exists {
		return change, core.Fail(core.DirectoryNotEmpty, nil)
	}
	return core.Change{Effect: core.Applied}, ctx.Err()
}

func optionalString(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func (d *Driver) deleteKeys(ctx context.Context, keys []string) (core.Change, error) {
	request := make([]types.ObjectIdentifier, len(keys))
	waiting := make(map[string]bool, len(keys))
	for i, key := range keys {
		request[i].Key, waiting[key] = aws.String(key), true
	}
	out, err := d.client.DeleteObjects(ctx, &sdk.DeleteObjectsInput{Bucket: aws.String(d.bucket), Delete: &types.Delete{Objects: request, Quiet: aws.Bool(false)}}, noReplay)
	if err != nil {
		return core.Change{Effect: core.Unknown}, errors.Join(translate(err), ctx.Err())
	}
	valid := true
	ack := func(key string) {
		if !waiting[key] {
			valid = false
		}
		delete(waiting, key)
	}
	for _, item := range out.Deleted {
		ack(aws.ToString(item.Key))
	}
	for _, item := range out.Errors {
		ack(aws.ToString(item.Key))
		err = errors.Join(err, translate(&smithy.GenericAPIError{Code: aws.ToString(item.Code), Message: aws.ToString(item.Message)}))
	}
	if !valid || len(waiting) != 0 {
		return core.Change{Effect: core.Unknown}, errors.Join(err, core.Fail(core.InvalidUpstreamResponse, nil), ctx.Err())
	}
	effect := core.Unchanged
	if len(out.Deleted) > 0 {
		effect = core.Applied
		if len(out.Errors) > 0 {
			effect = core.Incomplete
		}
	}
	return core.Change{Effect: effect}, errors.Join(err, ctx.Err())
}

var _ core.SubtreeRemover = (*Driver)(nil)
