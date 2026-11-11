// Package s3 adapts a prepared AWS S3 client to core reads and native mutations.
package s3

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsmiddleware "github.com/aws/aws-sdk-go-v2/aws/middleware"
	sdk "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/internal/upload"
)

// Driver borrows its SDK client. The host owns transports and credential providers.
type Driver struct {
	client         *sdk.Client
	bucket, prefix string
	closed         atomic.Bool
	opts           Options
	listEncoding   types.EncodingType
	buffer         *upload.Budget
	mu             sync.Mutex
	pending        map[string]PendingUpload
}

// Options configures listings and bounded buffered/multipart writes.
// Set PartSize and MaxBufferedBytes together. PartConcurrency and PartAttempts
// default to one. CleanupTimeout zero adds no deadline beyond the borrowed client.
type Options struct {
	PartSize, MaxBufferedBytes    int64
	PartConcurrency, PartAttempts int
	CleanupTimeout                time.Duration
	// PlainListKeys omits encoding-type for S3 endpoints that only list raw XML keys.
	PlainListKeys bool
	// ListV1 selects ListObjects with explicit, increasing NextMarker values.
	ListV1 bool
	// ResumeScope is a stable host identity for the native account/service. It
	// contains no credentials and must change when that identity changes.
	ResumeScope string
}

func New(client *sdk.Client, bucket, prefix string, options ...Options) (*Driver, error) {
	if client == nil || bucket == "" || strings.ContainsAny(bucket, "/\\") ||
		(prefix != "" && !strings.HasSuffix(prefix, "/")) {
		return nil, errors.New("a prepared client, bucket, and empty or slash-terminated prefix are required")
	}
	if len(options) > 1 {
		return nil, core.Fail(core.InvalidRequest, nil)
	}
	var opts Options
	if len(options) == 1 {
		opts = options[0]
	}
	if opts.PartConcurrency < 0 || opts.PartAttempts < 0 || opts.CleanupTimeout < 0 ||
		(opts.PartSize == 0) != (opts.MaxBufferedBytes == 0) || opts.PartSize < 0 || opts.MaxBufferedBytes < 0 ||
		(opts.PartSize != 0 && (opts.PartSize < 5<<20 || opts.PartSize > 5<<30)) {
		return nil, core.Fail(core.InvalidRequest, nil)
	}
	if opts.PartAttempts == 0 {
		opts.PartAttempts = 1
	}
	d := &Driver{client: client, bucket: bucket, prefix: prefix, opts: opts, pending: make(map[string]PendingUpload)}
	if !opts.PlainListKeys {
		d.listEncoding = types.EncodingTypeUrl
	}
	if opts.PartSize != 0 {
		if err := upload.Validate(opts.PartSize, opts.MaxBufferedBytes, opts.PartConcurrency); err != nil {
			return nil, core.Fail(core.InvalidRequest, err)
		}
		var err error
		d.buffer, err = upload.New(opts.MaxBufferedBytes, opts.PartConcurrency)
		if err != nil {
			return nil, core.Fail(core.InvalidRequest, err)
		}
	}
	return d, nil
}
func (d *Driver) Close() error {
	d.closed.Store(true)
	d.mu.Lock()
	defer d.mu.Unlock()
	if len(d.pending) != 0 {
		err := errors.New("private multipart uploads still require cleanup")
		for _, p := range d.pending {
			err = errors.Join(err, p.cause)
		}
		return err
	}
	return nil
}
func (d *Driver) admit(ctx context.Context, p core.NativeAddress) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if p.Namespace() == core.TreeNamespace {
		if err := core.ValidatePath(p.Value()); err != nil {
			return err
		}
	} else if p.Namespace() != core.KeyNamespace || p.Value() == "" || !utf8.ValidString(p.Value()) {
		return core.Fail(core.InvalidRequest, nil)
	}
	length := len(p.Value())
	if p.Namespace() == core.TreeNamespace {
		length--
	}
	if len(d.prefix)+length > 1024 {
		return core.Fail(core.InvalidRequest, nil)
	}
	if d.closed.Load() {
		return core.Fail(core.MountUnavailable, nil)
	}
	return nil
}
func name(p core.NativeAddress) string {
	if p.Namespace() == core.KeyNamespace {
		return p.Value()
	}
	return p.Value()[strings.LastIndexByte(p.Value(), '/')+1:]
}
func (d *Driver) key(p core.NativeAddress) string {
	if p.Namespace() == core.KeyNamespace {
		return d.prefix + p.Value()
	}
	return d.prefix + strings.TrimPrefix(p.Value(), "/")
}

func (*Driver) SupportsNamespace(n core.Namespace) bool {
	return n == core.TreeNamespace || n == core.KeyNamespace
}
func (*Driver) KeyRoot(p core.NativeAddress) (string, bool) {
	if p.Namespace() != core.TreeNamespace {
		return "", false
	}
	prefix := strings.TrimPrefix(p.Value(), "/")
	if prefix != "" {
		prefix += "/"
	}
	return prefix, true
}
func (*Driver) KeyExtent(p core.NativeAddress) core.KeyExtent {
	key := strings.TrimPrefix(p.Value(), "/")
	if p.Namespace() == core.KeyNamespace {
		return core.KeyExtent{Exact: p.Value(), HasExact: true}
	}
	if key == "" {
		return core.KeyExtent{Prefix: "", HasPrefix: true}
	}
	return core.KeyExtent{Exact: key, HasExact: true, Prefix: key + "/", HasPrefix: true}
}

// Ancestors must be directories under the same namespace interpretation as Stat.
// A direct object-key GET alone would bypass an ambiguous ancestor.
func (d *Driver) ancestors(ctx context.Context, p core.NativeAddress) error {
	if p.Namespace() == core.KeyNamespace {
		return nil
	}
	parts := strings.Split(strings.TrimPrefix(p.Value(), "/"), "/")
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
func decodeListedKey(encoding types.EncodingType, key string) (string, error) {
	switch encoding {
	case types.EncodingTypeUrl:
		return url.PathUnescape(key)
	case "":
		// XML line-ending normalization can erase the original key bytes.
		if strings.ContainsAny(key, "\r\n") {
			return "", errors.New("raw XML listing cannot establish the exact key")
		}
		return key, nil
	default:
		return "", errors.New("unsupported key encoding")
	}
}

// listObjects keeps native marker handling at the SDK boundary.
func (d *Driver) listObjects(ctx context.Context, input *sdk.ListObjectsV2Input) (*sdk.ListObjectsV2Output, error) {
	if !d.opts.ListV1 {
		out, err := d.client.ListObjectsV2(ctx, input)
		if err != nil {
			return nil, translate(err)
		}
		return out, nil
	}
	marker := input.ContinuationToken
	if marker == nil {
		marker = input.StartAfter
	}
	out, err := d.client.ListObjects(ctx, &sdk.ListObjectsInput{
		Bucket: input.Bucket, Prefix: input.Prefix, Delimiter: input.Delimiter, MaxKeys: input.MaxKeys,
		Marker: marker, EncodingType: input.EncodingType,
	})
	if err != nil {
		return nil, translate(err)
	}
	if out.IsTruncated == nil || out.EncodingType != input.EncodingType {
		return nil, core.Fail(core.InvalidUpstreamResponse, nil)
	}
	var next string
	if aws.ToBool(out.IsTruncated) {
		nextKey := out.NextMarker
		if nextKey == nil && aws.ToString(input.Delimiter) == "" && len(out.Contents) > 0 {
			nextKey = out.Contents[len(out.Contents)-1].Key
		}
		next, err = decodeListedKey(out.EncodingType, aws.ToString(nextKey))
		if err != nil || !utf8.ValidString(next) || next <= aws.ToString(marker) || !strings.HasPrefix(next, aws.ToString(input.Prefix)) {
			return nil, core.Fail(core.InvalidUpstreamResponse, errors.Join(errors.New("invalid native listing marker"), err))
		}
	}
	return &sdk.ListObjectsV2Output{IsTruncated: out.IsTruncated, Contents: out.Contents,
		CommonPrefixes: out.CommonPrefixes, EncodingType: out.EncodingType, NextContinuationToken: optionalString(next)}, nil
}

func (d *Driver) hasDirectory(ctx context.Context, key string) (bool, error) {
	prefix := key + "/"
	out, err := d.listObjects(ctx, &sdk.ListObjectsV2Input{
		Bucket: aws.String(d.bucket), Prefix: aws.String(prefix), Delimiter: aws.String("/"), MaxKeys: aws.Int32(1), EncodingType: d.listEncoding,
	})
	if err != nil {
		return false, err
	}
	if out.IsTruncated == nil || (aws.ToBool(out.IsTruncated) && len(out.Contents)+len(out.CommonPrefixes) == 0) {
		return false, core.Fail(core.InvalidUpstreamResponse, nil)
	}
	for _, item := range out.Contents {
		listed, err := decodeListedKey(out.EncodingType, aws.ToString(item.Key))
		if err != nil || !strings.HasPrefix(listed, prefix) {
			return false, core.Fail(core.InvalidUpstreamResponse, err)
		}
	}
	for _, item := range out.CommonPrefixes {
		listed, err := decodeListedKey(out.EncodingType, aws.ToString(item.Prefix))
		if err != nil || !strings.HasPrefix(listed, prefix) || !strings.HasSuffix(listed, "/") {
			return false, core.Fail(core.InvalidUpstreamResponse, err)
		}
	}
	return len(out.Contents) > 0 || len(out.CommonPrefixes) > 0, nil
}
func (d *Driver) directoryAt(ctx context.Context, p core.NativeAddress) (bool, error) {
	if p.Namespace() == core.KeyNamespace {
		return false, nil
	}
	return d.hasDirectory(ctx, d.key(p))
}
func (d *Driver) statKey(ctx context.Context, key, n string, versions ...core.Version) (core.Metadata, error) {
	meta, err := d.head(ctx, key, n, versions...)
	if err != nil && core.CodeOf(err) != core.NotFound {
		return core.Metadata{}, err
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
	return meta, err
}

func (d *Driver) head(ctx context.Context, key, n string, versions ...core.Version) (core.Metadata, error) {
	request := &sdk.HeadObjectInput{Bucket: aws.String(d.bucket), Key: aws.String(key)}
	if len(versions) == 1 && versions[0] != "" {
		request.VersionId = aws.String(string(versions[0]))
	}
	out, err := d.client.HeadObject(ctx, request)
	if err != nil {
		return core.Metadata{}, translate(err)
	}
	return core.Metadata{Name: n, Kind: core.File, Size: out.ContentLength, ModifiedAt: out.LastModified, Evidence: evidence(out.ETag, out.VersionId)}, nil
}
func (d *Driver) Stat(ctx context.Context, p core.NativeAddress, selections ...core.ReadSelection) (meta core.Metadata, err error) {
	if len(selections) > 1 {
		return meta, core.Fail(core.InvalidRequest, nil)
	}
	var selection core.ReadSelection
	if len(selections) == 1 {
		selection = selections[0]
	}
	if selection.Version == "null" {
		return meta, core.Fail(core.UnsupportedOperation, nil)
	}
	defer func() {
		if err == nil {
			if selection.Version != "" && meta.Kind != core.File {
				err = core.Fail(core.UnsupportedOperation, nil)
				return
			}
			if selection.Version != "" && meta.Evidence.Version != selection.Version {
				err = core.Fail(core.InvalidUpstreamResponse, nil)
				return
			}
			err = selection.Check(meta.Evidence)
		}
	}()
	if err := d.admit(ctx, p); err != nil {
		return core.Metadata{}, err
	}
	if p.Namespace() == core.KeyNamespace {
		return d.head(ctx, d.key(p), p.Value(), selection.Version)
	}
	if p.Value() == "/" {
		return core.Metadata{Kind: core.Directory}, nil
	}
	if err := d.ancestors(ctx, p); err != nil {
		return core.Metadata{}, err
	}
	return d.statKey(ctx, d.key(p), name(p), selection.Version)
}

// Lstat uses the same lookup because tree objects and prefixes never redirect.
func (d *Driver) Lstat(ctx context.Context, p core.NativeAddress) (core.Metadata, error) {
	if p.Namespace() != core.TreeNamespace {
		return core.Metadata{}, core.Fail(core.UnsupportedOperation, nil)
	}
	return d.Stat(ctx, p)
}

func (d *Driver) Enumerate(ctx context.Context, query core.NativeQuery) (core.Iterator, error) {
	p := query.Address()
	if p.Namespace() == core.KeyNamespace {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if d.closed.Load() {
			return nil, core.Fail(core.MountUnavailable, nil)
		}
		if !utf8.ValidString(p.Value()) || !utf8.ValidString(query.Delimiter()) || !utf8.ValidString(query.StartAfter()) || len(d.prefix+p.Value()) > 1024 {
			return nil, core.Fail(core.InvalidRequest, nil)
		}
		return &iterator{driver: d, prefix: d.prefix + p.Value(), delimiter: query.Delimiter(), after: query.StartAfter(), keys: true}, nil
	}
	if query.Delimiter() != "" || query.StartAfter() != "" {
		return nil, core.Fail(core.InvalidRequest, nil)
	}
	return d.directoryIterator(ctx, p)
}

func (d *Driver) directoryIterator(ctx context.Context, p core.NativeAddress) (*iterator, error) {
	meta, err := d.Stat(ctx, p)
	if err != nil {
		return nil, err
	}
	if meta.Kind != core.Directory {
		return nil, core.Fail(core.NotDirectory, nil)
	}
	prefix := d.prefix
	if p.Value() != "/" {
		prefix = d.key(p) + "/"
	}
	return &iterator{driver: d, prefix: prefix, delimiter: "/"}, nil
}

type iterator struct {
	driver           *Driver
	prefix, marker   string
	delimiter, after string
	keys             bool
	done, closed     bool
	files            map[string]core.Metadata
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
	input := &sdk.ListObjectsV2Input{Bucket: aws.String(i.driver.bucket), Prefix: aws.String(i.prefix), MaxKeys: aws.Int32(int32(n)), EncodingType: i.driver.listEncoding}
	if i.delimiter != "" {
		input.Delimiter = aws.String(i.delimiter)
	}
	if i.marker == "" && i.after != "" {
		input.StartAfter = aws.String(i.driver.prefix + i.after)
	}
	if i.marker != "" {
		input.ContinuationToken = aws.String(i.marker)
	}
	out, err := i.driver.listObjects(ctx, input)
	if err != nil {
		return nil, err
	}
	if out.IsTruncated == nil {
		return nil, core.Fail(core.InvalidUpstreamResponse, nil)
	}
	next := aws.ToString(out.NextContinuationToken)
	if aws.ToBool(out.IsTruncated) && (next == "" || next == i.marker) {
		return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("invalid native continuation"))
	}
	items := make([]core.Metadata, 0, len(out.Contents)+len(out.CommonPrefixes))
	for _, o := range out.Contents {
		key, err := decodeListedKey(out.EncodingType, aws.ToString(o.Key))
		if err != nil {
			return nil, core.Fail(core.InvalidUpstreamResponse, err)
		}
		if !i.keys && key == i.prefix {
			continue
		} // The current directory marker is not its own child.
		if !strings.HasPrefix(key, i.prefix) {
			return nil, core.Fail(core.InvalidUpstreamResponse, nil)
		}
		n := strings.TrimPrefix(key, i.prefix)
		if i.keys {
			n = strings.TrimPrefix(key, i.driver.prefix)
		}
		if (!i.keys && !core.ValidName(n)) || (i.keys && (n == "" || !utf8.ValidString(n))) {
			return nil, core.Fail(core.UnrepresentableName, nil)
		}
		items = append(items, core.Metadata{Name: n, Kind: core.File, Size: o.Size, ModifiedAt: o.LastModified, Evidence: evidence(o.ETag, nil)})
	}
	for _, o := range out.CommonPrefixes {
		key, err := decodeListedKey(out.EncodingType, aws.ToString(o.Prefix))
		if err != nil || i.delimiter == "" || !strings.HasPrefix(key, i.prefix) || !strings.HasSuffix(key, i.delimiter) {
			return nil, core.Fail(core.InvalidUpstreamResponse, err)
		}
		n := strings.TrimSuffix(strings.TrimPrefix(key, i.prefix), "/")
		kind := core.Directory
		if i.keys {
			n = strings.TrimPrefix(key, i.driver.prefix)
			kind = core.PrefixGroup
		}
		if (!i.keys && !core.ValidName(n)) || (i.keys && (n == "" || !utf8.ValidString(n))) {
			return nil, core.Fail(core.UnrepresentableName, nil)
		}
		items = append(items, core.Metadata{Name: n, Kind: kind})
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
func (d *Driver) Open(ctx context.Context, p core.NativeAddress, options core.OpenOptions) (core.Opened, error) {
	if options.Mode == core.LiveEdit {
		return core.Opened{Change: core.Change{Effect: core.Unchanged}}, core.Fail(core.UnsupportedOperation, nil)
	}
	if err := options.Validate(); err != nil {
		return core.Opened{}, err
	}
	if options.Mode == core.BoundRead {
		return d.bindFile(ctx, p, options.Selection)
	}
	wanted := options.Range
	if err := d.admit(ctx, p); err != nil {
		return core.Opened{}, err
	}
	if p.Namespace() == core.TreeNamespace && p.Value() == "/" {
		return core.Opened{}, core.Fail(core.NotFile, nil)
	}
	if err := d.ancestors(ctx, p); err != nil {
		return core.Opened{}, err
	}
	key := d.key(p)
	if p.Namespace() == core.KeyNamespace {
		return d.get(ctx, key, name(p), wanted, options.Selection)
	}
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
	return d.get(ctx, key, name(p), wanted, options.Selection)
}

func (d *Driver) get(ctx context.Context, key, leaf string, wanted *core.Range, selections ...core.ReadSelection) (core.Opened, error) {
	var selection core.ReadSelection
	if len(selections) == 1 {
		selection = selections[0]
	}
	match, none, err := nativeCondition(selection.When, true)
	if err != nil {
		return core.Opened{}, err
	}
	if selection.Version == "null" {
		return core.Opened{}, core.Fail(core.UnsupportedOperation, nil)
	}
	input := &sdk.GetObjectInput{Bucket: aws.String(d.bucket), Key: aws.String(key)}
	input.IfMatch, input.IfNoneMatch = match, none
	if selection.Version != "" {
		input.VersionId = aws.String(string(selection.Version))
	}
	if wanted != nil {
		input.Range = aws.String(rangeHeader(wanted))
	}
	out, err := d.client.GetObject(ctx, input, func(o *sdk.Options) { o.APIOptions = append(o.APIOptions, awsmiddleware.AddRawResponseToMetadata) })
	if err != nil {
		return core.Opened{}, readFailure(err)
	}
	opened := core.Opened{Metadata: core.Metadata{Name: leaf, Kind: core.File, Size: out.ContentLength, ModifiedAt: out.LastModified, Evidence: evidence(out.ETag, out.VersionId)}, Length: out.ContentLength, Body: out.Body}
	if selection.Version != "" && opened.Metadata.Evidence.Version != selection.Version {
		return opened, core.Fail(core.InvalidUpstreamResponse, nil)
	}
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
	if match != nil || none != nil {
		if failure := selection.When.Check(opened.Metadata.Evidence, true); failure != nil {
			tag := opened.Metadata.Evidence.Validator
			if core.CodeOf(failure) == core.PreconditionFailed || tag.Tag != "" && !tag.Valid() {
				failure = core.Fail(core.InvalidUpstreamResponse, failure)
			}
			return opened, failure
		}
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
		case "NoSuchKey", "NoSuchVersion", "NotFound":
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
			text := response.Response.Header.Get("Content-Range")
			if _, scanErr := fmt.Sscanf(text, "bytes */%d", &total); scanErr == nil && total >= 0 && text == fmt.Sprintf("bytes */%d", total) {
				failure.Total = &total
			}
			return failure
		}
	}
	return core.Fail(code, err)
}
