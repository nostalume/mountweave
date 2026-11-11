// Package azureblob adapts a prepared Azure Blob container to exact-key access.
package azureblob

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"sync/atomic"
	"unicode/utf8"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/internal/httpcontent"
	"github.com/nostalume/mountweave/drivers/internal/upload"
)

// Options borrows Client, including its transport and credential provider.
// Prefix is literal, empty or slash-terminated. PartSize and MaxBufferedBytes
// enable writes together; zero leaves the instance read-only. The byte budget
// and PartConcurrency are shared by all uploads; concurrency defaults to one.
type Options struct {
	Client                     *container.Client
	Prefix                     string
	PartSize, MaxBufferedBytes int64
	PartConcurrency            int
}

type Driver struct {
	client *container.Client
	prefix string
	part   int64
	buffer *upload.Budget
	closed atomic.Bool
}

// Open validates configuration without probing a prefix's existence. Client
// already selects the container; subsequent native calls establish permission.
func Open(ctx context.Context, o Options) (*Driver, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if o.Client == nil || !utf8.ValidString(o.Prefix) || utf8.RuneCountInString(o.Prefix) > 1024 ||
		(o.Prefix != "" && !strings.HasSuffix(o.Prefix, "/")) ||
		o.PartSize < 0 || o.PartSize > blockblob.MaxStageBlockBytes || o.MaxBufferedBytes < 0 ||
		o.PartConcurrency < 0 || (o.PartSize == 0) != (o.MaxBufferedBytes == 0) ||
		(o.PartSize == 0 && o.PartConcurrency != 0) {
		return nil, core.Fail(core.InvalidRequest, nil)
	}
	d := &Driver{client: o.Client, prefix: o.Prefix, part: o.PartSize}
	if o.PartSize != 0 {
		if err := upload.Validate(o.PartSize, o.MaxBufferedBytes, o.PartConcurrency); err != nil {
			return nil, core.Fail(core.InvalidRequest, err)
		}
		d.buffer, _ = upload.New(o.MaxBufferedBytes, o.PartConcurrency)
	}
	return d, nil
}

func (*Driver) SupportsNamespace(n core.Namespace) bool { return n == core.KeyNamespace }

// Close stops admission. Core has already canceled and joined its active uses;
// neither borrowed clients nor upstream uncommitted blocks are owned cleanup.
func (d *Driver) Close() error { d.closed.Store(true); return nil }

func (d *Driver) admit(ctx context.Context, p core.NativeAddress, prefix bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if d.closed.Load() {
		return core.Fail(core.MountUnavailable, nil)
	}
	if p.Namespace() != core.KeyNamespace {
		return core.Fail(core.UnsupportedOperation, nil)
	}
	if !utf8.ValidString(p.Value()) || (!prefix && p.Value() == "") || utf8.RuneCountInString(d.prefix+p.Value()) > 1024 {
		return core.Fail(core.InvalidRequest, nil)
	}
	return nil
}

func evidence(tag *azcore.ETag, version *string) core.Evidence {
	var e core.Evidence
	if tag != nil {
		e.Validator.Tag = string(*tag)
	}
	if version != nil {
		e.Version = core.Version(*version)
	}
	return e
}

// Native writes accept one condition and one strong tag. Keep that subset
// explicit instead of approximating unsupported predicates with a HEAD.
func conditions(when core.Condition, write bool) (*blob.AccessConditions, error) {
	if err := when.ForDomain(""); err != nil {
		return nil, err
	}
	match, none := when.Match(), when.NotMatch()
	if write {
		if match != "" && none != "" {
			return nil, core.Fail(core.UnsupportedOperation, nil)
		}
		for _, tag := range []string{match, none} {
			if tag != "" && tag != "*" && (!(core.Validator{Tag: tag}).Valid() || strings.HasPrefix(tag, "W/")) {
				return nil, core.Fail(core.UnsupportedOperation, nil)
			}
		}
	}
	if when.Empty() {
		return nil, nil
	}
	native := &blob.ModifiedAccessConditions{}
	if match != "" {
		v := azcore.ETag(match)
		native.IfMatch = &v
	}
	if none != "" {
		v := azcore.ETag(none)
		native.IfNoneMatch = &v
	}
	return &blob.AccessConditions{ModifiedAccessConditions: native}, nil
}

func (d *Driver) selected(p core.NativeAddress, selection core.ReadSelection) (*blob.Client, error) {
	b := d.client.NewBlobClient(d.prefix + p.Value())
	if selection.Version != "" {
		return b.WithVersionID(string(selection.Version))
	}
	return b, nil
}

func (d *Driver) Stat(ctx context.Context, p core.NativeAddress, selections ...core.ReadSelection) (core.Metadata, error) {
	if len(selections) > 1 {
		return core.Metadata{}, core.Fail(core.InvalidRequest, nil)
	}
	if err := d.admit(ctx, p, false); err != nil {
		return core.Metadata{}, err
	}
	var selection core.ReadSelection
	if len(selections) == 1 {
		selection = selections[0]
	}
	if err := selection.When.Validate(); err != nil {
		return core.Metadata{}, err
	}
	b, err := d.selected(p, selection)
	if err != nil {
		return core.Metadata{}, core.Fail(core.InvalidRequest, err)
	}
	var raw *http.Response
	out, err := b.GetProperties(policy.WithCaptureResponse(ctx, &raw), nil)
	if err != nil {
		return core.Metadata{}, translate(err)
	}
	meta := core.Metadata{Name: p.Value(), Kind: core.File, Size: out.ContentLength, ModifiedAt: out.LastModified, Evidence: evidence(out.ETag, out.VersionID)}
	if raw != nil && raw.Header.Get("x-ms-resource-type") == "directory" {
		return meta, core.Fail(core.UnsupportedOperation, errors.New("hierarchical directories are not blob files"))
	}
	if selection.Version != "" && meta.Evidence.Version != selection.Version {
		return meta, core.Fail(core.InvalidUpstreamResponse, nil)
	}
	return meta, selection.Check(meta.Evidence)
}

func (d *Driver) Open(ctx context.Context, p core.NativeAddress, o core.OpenOptions) (core.Opened, error) {
	if err := o.Validate(); err != nil {
		return core.Opened{}, err
	}
	if o.Mode != core.StreamRead || o.NoFollow {
		return core.Opened{Change: core.Change{Effect: core.Unchanged}}, core.Fail(core.UnsupportedOperation, nil)
	}
	if err := d.admit(ctx, p, false); err != nil {
		return core.Opened{}, err
	}
	access, err := conditions(o.Selection.When, false)
	if err != nil {
		return core.Opened{}, err
	}
	b, err := d.selected(p, o.Selection)
	if err != nil {
		return core.Opened{}, core.Fail(core.InvalidRequest, err)
	}
	options := &blob.DownloadStreamOptions{AccessConditions: access}
	if o.Range != nil && !o.Range.Suffix {
		options.Range.Offset = o.Range.Start
		if o.Range.End >= 0 {
			options.Range.Count = o.Range.End - o.Range.Start + 1
		}
	}
	var raw *http.Response
	out, err := b.DownloadStream(policy.WithCaptureResponse(ctx, &raw), options)
	if raw != nil && raw.StatusCode == http.StatusNotModified {
		return core.Opened{}, core.Fail(core.PreconditionFailed, &core.ConditionFailure{Negative: true})
	}
	if err != nil {
		var opened core.Opened
		if raw != nil && (raw.StatusCode == http.StatusOK || raw.StatusCode == http.StatusPartialContent) {
			opened.Body = raw.Body
		}
		failure := translate(err)
		if core.CodeOf(failure) == core.PreconditionFailed && !o.Selection.When.Empty() {
			failure = conditionFailure(o.Selection.When, failure)
		}
		return opened, failure
	}
	if raw == nil {
		return core.Opened{Body: out.Body}, core.Fail(core.InvalidUpstreamResponse, nil)
	}
	raw.Body = out.Body
	opened, err := httpcontent.Open(raw, o.Range)
	opened.Metadata.Name = p.Value()
	opened.Metadata.Evidence = evidence(out.ETag, out.VersionID)
	if raw.Header.Get("x-ms-resource-type") == "directory" {
		err = core.Fail(core.UnsupportedOperation, nil)
	}
	if err == nil && o.Selection.Version != "" && opened.Metadata.Evidence.Version != o.Selection.Version {
		err = core.Fail(core.InvalidUpstreamResponse, nil)
	}
	return opened, err
}

func conditionFailure(when core.Condition, cause error) error {
	return core.Fail(core.PreconditionFailed, errors.Join(&core.ConditionFailure{Negative: when.Match() == ""}, cause))
}

func translate(err error) error {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	code := core.UpstreamFailure
	var response *azcore.ResponseError
	if errors.As(err, &response) {
		switch response.StatusCode {
		case 400:
			code = core.InvalidRequest
		case 401, 403:
			code = core.UpstreamAccessDenied
		case 404:
			code = core.NotFound
		case 412:
			code = core.PreconditionFailed
		case 416:
			if response.RawResponse != nil {
				_, failure := httpcontent.Open(response.RawResponse, nil)
				return errors.Join(failure, err)
			}
			code = core.RangeNotSatisfiable
		case 429, 503:
			code = core.UpstreamThrottled
		}
		if response.ErrorCode == "ContainerNotFound" {
			code = core.MountUnavailable
		}
	}
	return core.Fail(code, err)
}

var _ core.Driver = (*Driver)(nil)
var _ core.NamespaceSupport = (*Driver)(nil)
