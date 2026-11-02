// Package s3 adapts a prepared AWS S3 client to core reads and new-file writes.
package s3

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strings"
	"sync/atomic"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsmiddleware "github.com/aws/aws-sdk-go-v2/aws/middleware"
	sdk "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	core "github.com/nostalume/mountweave"
)

// Driver borrows its SDK client. The host owns transports and credential providers.
type Driver struct {
	client         *sdk.Client
	bucket, prefix string
	closed         atomic.Bool
}

func New(client *sdk.Client, bucket, prefix string) (*Driver, error) {
	if client == nil || bucket == "" || strings.ContainsAny(bucket, "/\\") ||
		(prefix != "" && !strings.HasSuffix(prefix, "/")) {
		return nil, errors.New("a prepared client, bucket, and empty or slash-terminated prefix are required")
	}
	return &Driver{client: client, bucket: bucket, prefix: prefix}, nil
}
func (d *Driver) Close() error { d.closed.Store(true); return nil }
func (d *Driver) admit(ctx context.Context, p string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := core.ValidatePath(p); err != nil {
		return err
	}
	if d.closed.Load() {
		return core.Fail(core.MountUnavailable, nil)
	}
	return nil
}
func name(p string) string            { return p[strings.LastIndexByte(p, '/')+1:] }
func (d *Driver) key(p string) string { return d.prefix + strings.TrimPrefix(p, "/") }

// Ancestors must be directories under the same namespace interpretation as Stat.
// A direct object-key GET alone would bypass an ambiguous ancestor.
func (d *Driver) ancestors(ctx context.Context, p string) error {
	parts := strings.Split(strings.TrimPrefix(p, "/"), "/")
	key := d.prefix
	for _, part := range parts[:len(parts)-1] {
		key += part
		meta, err := d.statKey(ctx, key, part)
		if err != nil {
			return err
		}
		if meta.Kind != core.Directory {
			return core.Fail(core.NotDirectory, nil)
		}
		key += "/"
	}
	return nil
}
func (d *Driver) hasDirectory(ctx context.Context, key string) (bool, error) {
	out, err := d.client.ListObjectsV2(ctx, &sdk.ListObjectsV2Input{
		Bucket: aws.String(d.bucket), Prefix: aws.String(key + "/"), Delimiter: aws.String("/"), MaxKeys: aws.Int32(1),
	})
	if err != nil {
		return false, translate(err)
	}
	if out.IsTruncated == nil || (aws.ToBool(out.IsTruncated) && len(out.Contents)+len(out.CommonPrefixes) == 0) {
		return false, core.Fail(core.InvalidUpstreamResponse, nil)
	}
	for _, item := range out.Contents {
		if !strings.HasPrefix(aws.ToString(item.Key), key+"/") {
			return false, core.Fail(core.InvalidUpstreamResponse, nil)
		}
	}
	for _, item := range out.CommonPrefixes {
		if !strings.HasPrefix(aws.ToString(item.Prefix), key+"/") || !strings.HasSuffix(aws.ToString(item.Prefix), "/") {
			return false, core.Fail(core.InvalidUpstreamResponse, nil)
		}
	}
	return len(out.Contents) > 0 || len(out.CommonPrefixes) > 0, nil
}
func (d *Driver) statKey(ctx context.Context, key, n string) (core.Metadata, error) {
	out, err := d.client.HeadObject(ctx, &sdk.HeadObjectInput{Bucket: aws.String(d.bucket), Key: aws.String(key)})
	if err != nil && core.CodeOf(translate(err)) != core.NotFound {
		return core.Metadata{}, translate(err)
	}
	directory, dirErr := d.hasDirectory(ctx, key)
	if dirErr != nil {
		return core.Metadata{}, dirErr
	}
	if err == nil && directory {
		return core.Metadata{}, core.Fail(core.PathAmbiguous, nil)
	}
	if directory {
		return core.Metadata{Name: n, Kind: core.Directory}, nil
	}
	if err != nil {
		return core.Metadata{}, translate(err)
	}
	return core.Metadata{Name: n, Kind: core.File, Size: out.ContentLength, ModifiedAt: out.LastModified}, nil
}
func (d *Driver) Stat(ctx context.Context, p string) (core.Metadata, error) {
	if err := d.admit(ctx, p); err != nil {
		return core.Metadata{}, err
	}
	if p == "/" {
		return core.Metadata{Kind: core.Directory}, nil
	}
	if err := d.ancestors(ctx, p); err != nil {
		return core.Metadata{}, err
	}
	return d.statKey(ctx, d.key(p), name(p))
}
func (d *Driver) Enumerate(ctx context.Context, p string) (core.Iterator, error) {
	meta, err := d.Stat(ctx, p)
	if err != nil {
		return nil, err
	}
	if meta.Kind != core.Directory {
		return nil, core.Fail(core.NotDirectory, nil)
	}
	prefix := d.prefix
	if p != "/" {
		prefix = d.key(p) + "/"
	}
	return &iterator{driver: d, prefix: prefix}, nil
}

type iterator struct {
	driver         *Driver
	prefix, marker string
	done, closed   bool
	files          map[string]core.Metadata
}

func (i *iterator) Close() error { i.closed = true; i.files = nil; return nil }
func (i *iterator) Next(ctx context.Context, n int) ([]core.Metadata, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if i.closed {
		return nil, io.ErrClosedPipe
	}
	if i.done {
		return nil, io.EOF
	}
	if i.driver.closed.Load() {
		return nil, core.Fail(core.MountUnavailable, nil)
	}
	if n < 1 {
		return nil, core.Fail(core.InvalidRequest, nil)
	}
	if n > 1000 {
		n = 1000
	}
	input := &sdk.ListObjectsV2Input{Bucket: aws.String(i.driver.bucket), Prefix: aws.String(i.prefix), Delimiter: aws.String("/"), MaxKeys: aws.Int32(int32(n)), EncodingType: types.EncodingTypeUrl}
	if i.marker != "" {
		input.ContinuationToken = aws.String(i.marker)
	}
	out, err := i.driver.client.ListObjectsV2(ctx, input)
	if err != nil {
		return nil, translate(err)
	}
	if out.IsTruncated == nil {
		return nil, core.Fail(core.InvalidUpstreamResponse, nil)
	}
	next := aws.ToString(out.NextContinuationToken)
	if aws.ToBool(out.IsTruncated) && (next == "" || next == i.marker) {
		return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("invalid native continuation"))
	}
	items := make([]core.Metadata, 0, len(out.Contents)+len(out.CommonPrefixes))
	decode := func(v string) (string, error) {
		if out.EncodingType == types.EncodingTypeUrl {
			return url.PathUnescape(v)
		}
		if out.EncodingType != "" {
			return "", errors.New("unsupported key encoding")
		}
		return v, nil
	}
	for _, o := range out.Contents {
		key, err := decode(aws.ToString(o.Key))
		if err != nil {
			return nil, core.Fail(core.InvalidUpstreamResponse, err)
		}
		if key == i.prefix {
			continue
		} // The current directory marker is not its own child.
		if !strings.HasPrefix(key, i.prefix) {
			return nil, core.Fail(core.InvalidUpstreamResponse, nil)
		}
		n := strings.TrimPrefix(key, i.prefix)
		if !core.ValidName(n) {
			return nil, core.Fail(core.UnrepresentableName, nil)
		}
		items = append(items, core.Metadata{Name: n, Kind: core.File, Size: o.Size, ModifiedAt: o.LastModified})
	}
	for _, o := range out.CommonPrefixes {
		key, err := decode(aws.ToString(o.Prefix))
		if err != nil || !strings.HasPrefix(key, i.prefix) || !strings.HasSuffix(key, "/") {
			return nil, core.Fail(core.InvalidUpstreamResponse, err)
		}
		n := strings.TrimSuffix(strings.TrimPrefix(key, i.prefix), "/")
		if !core.ValidName(n) {
			return nil, core.Fail(core.UnrepresentableName, nil)
		}
		items = append(items, core.Metadata{Name: n, Kind: core.Directory})
	}
	if i.files != nil {
		for _, meta := range items {
			if _, exists := i.files[meta.Name]; exists {
				return nil, core.Fail(core.PathAmbiguous, nil)
			}
			i.files[meta.Name] = meta
		}
	}
	i.marker = next
	i.done = !aws.ToBool(out.IsTruncated)
	if i.done {
		return items, io.EOF
	}
	return items, nil
}
func rangeHeader(r *core.Range) string {
	if r.Suffix {
		return fmt.Sprintf("bytes=-%d", r.Start)
	}
	if r.End < 0 {
		return fmt.Sprintf("bytes=%d-", r.Start)
	}
	return fmt.Sprintf("bytes=%d-%d", r.Start, r.End)
}
func (d *Driver) Open(ctx context.Context, p string, wanted *core.Range) (core.Opened, error) {
	if err := d.admit(ctx, p); err != nil {
		return core.Opened{}, err
	}
	if wanted != nil && !wanted.Valid() {
		return core.Opened{}, core.Fail(core.InvalidRange, nil)
	}
	if p == "/" {
		return core.Opened{}, core.Fail(core.NotFile, nil)
	}
	if err := d.ancestors(ctx, p); err != nil {
		return core.Opened{}, err
	}
	key := d.key(p)
	directory, err := d.hasDirectory(ctx, key)
	if err != nil {
		return core.Opened{}, err
	}
	if directory {
		_, err = d.client.HeadObject(ctx, &sdk.HeadObjectInput{Bucket: aws.String(d.bucket), Key: aws.String(key)})
		if err == nil {
			return core.Opened{}, core.Fail(core.PathAmbiguous, nil)
		}
		if core.CodeOf(translate(err)) != core.NotFound {
			return core.Opened{}, translate(err)
		}
		return core.Opened{}, core.Fail(core.NotFile, nil)
	}
	return d.get(ctx, key, name(p), wanted)
}

func (d *Driver) get(ctx context.Context, key, leaf string, wanted *core.Range) (core.Opened, error) {
	input := &sdk.GetObjectInput{Bucket: aws.String(d.bucket), Key: aws.String(key)}
	if wanted != nil {
		input.Range = aws.String(rangeHeader(wanted))
	}
	out, err := d.client.GetObject(ctx, input, func(o *sdk.Options) { o.APIOptions = append(o.APIOptions, awsmiddleware.AddRawResponseToMetadata) })
	if err != nil {
		return core.Opened{}, translate(err)
	}
	opened := core.Opened{Metadata: core.Metadata{Name: leaf, Kind: core.File, Size: out.ContentLength, ModifiedAt: out.LastModified}, Length: out.ContentLength, Body: out.Body}
	response, ok := awsmiddleware.GetRawResponse(out.ResultMetadata).(*smithyhttp.Response)
	if !ok || (response.StatusCode != 200 && response.StatusCode != 206) || ((response.StatusCode == 206) != (out.ContentRange != nil)) {
		return opened, core.Fail(core.InvalidUpstreamResponse, nil)
	}
	if out.ContentRange != nil {
		var r core.ContentRange
		text := *out.ContentRange
		_, err = fmt.Sscanf(text, "bytes %d-%d/%d", &r.Start, &r.End, &r.Total)
		if err != nil || text != fmt.Sprintf("bytes %d-%d/%d", r.Start, r.End, r.Total) || wanted == nil {
			return opened, core.Fail(core.InvalidUpstreamResponse, err)
		}
		expected, err := wanted.Resolve(r.Total)
		if err != nil || expected != r || out.ContentLength == nil || *out.ContentLength != r.End-r.Start+1 {
			return opened, core.Fail(core.InvalidUpstreamResponse, err)
		}
		opened.Range = &r
		total := r.Total
		opened.Metadata.Size = &total
	}
	return opened, nil
}
func translate(err error) error {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	code := core.UpstreamFailure
	var malformed *smithy.DeserializationError
	if errors.As(err, &malformed) {
		code = core.InvalidUpstreamResponse
	}
	var api smithy.APIError
	if errors.As(err, &api) {
		switch api.ErrorCode() {
		case "NoSuchKey", "NotFound":
			code = core.NotFound
		case "NoSuchBucket":
			code = core.MountUnavailable
		case "AccessDenied", "InvalidAccessKeyId", "SignatureDoesNotMatch", "ExpiredToken":
			code = core.UpstreamAccessDenied
		case "SlowDown", "Throttling", "ThrottlingException":
			code = core.UpstreamThrottled
		case "RequestTimeout":
			code = core.UpstreamTimeout
		case "InvalidRange", "RequestedRangeNotSatisfiable":
			code = core.RangeNotSatisfiable
		}
	}
	var response *smithyhttp.ResponseError
	if errors.As(err, &response) {
		switch response.HTTPStatusCode() {
		case 403:
			code = core.UpstreamAccessDenied
		case 404:
			if code == core.UpstreamFailure {
				code = core.NotFound
			}
		case 429:
			code = core.UpstreamThrottled
		case 416:
			failure := &core.Error{Code: core.RangeNotSatisfiable, Cause: err}
			var total int64
			if _, scanErr := fmt.Sscanf(response.Response.Header.Get("Content-Range"), "bytes */%d", &total); scanErr == nil && total >= 0 {
				failure.Total = &total
			}
			return failure
		}
	}
	return core.Fail(code, err)
}
