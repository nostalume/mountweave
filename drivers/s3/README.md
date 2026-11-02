# S3 adapter

This independent Go module adapts a prepared AWS SDK for Go v2 S3 client to
github.com/nostalume/mountweave.Driver. It does not register itself, load environment or profile
credentials, construct a configuration system, or import OpenList.

New(client, bucket, prefix) borrows the SDK client. The caller prepares endpoint,
region, credentials, transport deadlines, and retry policy. A prefix is empty or
ends in a slash; it is concatenated exactly, without path normalization. Install
transfers the driver to the core. Driver.Close does not close borrowed clients,
transports, or shared SDK credential providers.

The driver uses HeadObject, ListObjectsV2, GetObject and conditional PutObject. A key and a directory
prefix with the same name are ambiguous. Ancestors use the same interpretation;
an object-key GET cannot bypass a conflicting ancestor. Proving a directory's
absence requires a successful prefix query. Missing permission is never absence.
The root is the host's prepared namespace root, not proof that the bucket exists.

Enumeration retains a native continuation and one returned batch; native order
and mutations remain observable. URI-encoded keys are decoded exactly once.
Duplicate file and directory names remain in listings. Invalid native
continuations fail rather than claim completion. No directory snapshot is promised.

Open uses the GET response's content length and Content-Range, with strict
validation of partial responses. A full response to a range request remains full.
An unknown length remains unknown. The core owns any returned body, including on
error, and enforces short-read, cancellation and retirement behavior.

The official SDK owns SigV4, endpoint selection, XML parsing, and its internal
request/response cleanup. The adapter makes one normalized metadata projection.
Its SDK dependencies do not enter the core module or the Aliyun Drive module.
This initial scope targets ordinary bucket operations; directory buckets,
access points, SSE-C, version selection and all vendor extensions are unverified.

## New-file writes

Create implements FileCreator using a single PutObject with If-None-Match: *.
It accepts an exact known length from zero through 5 GiB; unknown lengths and
multipart-sized input are refused before input consumption. Existing parents and
the absence of a target directory prefix are checked using the same namespace
interpretation as reads. These checks are observations, not a directory snapshot;
the native conditional request excludes replacement of the object key only.

The request body validates the complete input and its normal EOF without
whole-file staging. It serializes input reads, waits for an active read before
returning and prevents later transport access to returned input. The driver never
closes borrowed input. A blocked reader can delay cancellation and retirement.

SDK write retries are disabled, even when the borrowed client retries reads.
Host transports must not independently replay writes. A single 412 rejection is
Unchanged/AlreadyExists; uncertain request failures are Unknown, without replay
or path-based cleanup. Acknowledged writes retain their effects when cancellation
is subsequently observed. Confirmed writes with invalid input are Incomplete.

The operation uses the existing SDK unsigned-payload middleware and disables
optional request checksum calculation; hosts supply the transport, including TLS.
This is not a cryptographic content-integrity guarantee or a claim about every
S3-compatible product. No new SDK module or multipart/session API is introduced.

## Directory reads

OpenDirectory supplies core.DirectoryScope. A complete, unambiguous enumeration
retains child names inside that call's directory prefix; OpenFile reuses the
prefix and the existing GET validation without repeating ancestor probes. Close
clears the scoped lookup. This is observational membership, not a snapshot or
safe-delete/version capability. Core.CopyTree can use S3 as a source; directory
creation and removal remain unsupported here.

## Development

The module identity is github.com/nostalume/mountweave/drivers/s3.
Version v0.1.0 depends on Mountweave core v0.1.0. The relative replace is for
checkout development; consuming applications need no local replacements.

~~~sh
cd drivers/s3
go test ./...
go vet ./...
go test -race ./...  # requires a supported C compiler
~~~

Tests drive the actual SDK serializers, signer and deserializers through a
controlled HTTP transport, then exercise the core. They contact no provider
accounts. Real S3 service compatibility and performance remain separate work.

Protocol references:
[objects](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html),
[pagination](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html),
[SDK endpoints](https://docs.aws.amazon.com/sdk-for-go/v2/developer-guide/configure-endpoints.html),
[conditional writes](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html).
