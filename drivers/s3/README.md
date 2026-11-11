# S3 adapter

This independent module adapts a prepared AWS SDK for Go v2 S3 client to
Mountweave. It does not register itself, load credentials or import OpenList.
`New(client, bucket, prefix, options ...Options)` borrows the client. The host
chooses endpoint, region, credentials, deadlines and transport behavior. Prefix
is empty or slash-terminated and is concatenated exactly. Successful Install
transfers Driver ownership; Close never closes the borrowed client or transport.

## Compatible S3 services

For ordinary reads, pass a prepared AWS SDK v2 client to `New`. Set
`BaseEndpoint` and signing `Region` for the bucket, use vendor credentials and
virtual-host addressing (`UsePathStyle: false`), and grant list and read access.
Restrict the mount to reads with the Core Authorizer. These are example regions
from vendor contracts. Live deployments and stronger mutation or version
guarantees have not been verified.

| Service and example region | HTTPS `BaseEndpoint` | Signing `Region` | Adapter settings and references |
| --- | --- | --- | --- |
| Kingsoft Cloud KS3, Beijing | `https://ks3-cn-beijing.ksyuncs.com` | `BEIJING` | Default listing. [Regions](https://docs.ksyun.com/documents/6761) · [SigV4](https://docs.ksyun.com/documents/6890) · [Listing](https://docs.ksyun.com/documents/42287) · [Reads](https://docs.ksyun.com/documents/42320) |
| China Telecom Cloud ZOS, East China 1 | `https://jiangsu-10.zos.ctyun.cn` | `us-east-1` | Default listing. Special-key encoding is unverified. [Endpoints](https://www.ctyun.cn/document/10026735/11093379) · [Go SDK and signing](https://www.ctyun.cn/ctyun-portal/file/d51980d8661d40ec90ba0aadfc206421) · [ListObjectsV2](https://www.ctyun.cn/ctyun-portal/file/816e98029eda4c99b8e988246fe06358) · [HEAD](https://www.ctyun.cn/document/10026735/11070348) · [GET](https://www.ctyun.cn/document/10026735/11070697) |
| Baidu AI Cloud BOS, Beijing | `https://s3.bj.bcebos.com` | `bj` | Default listing. [HTTPS configuration](https://cloud.baidu.com/doc/BOS/s/Im885d7q2) · [SigV4](https://cloud.baidu.com/doc/BOS/s/Hjwvyq84s) · [Compatible operations](https://cloud.baidu.com/doc/BOS/s/Njwvyq8ho) |
| Qiniu Kodo, Zhejiang | `https://s3.cn-east-1.qiniucs.com` | `cn-east-1` | Set `Options{PlainListKeys: true}` and use the bucket's S3 space name. [Endpoints](https://developer.qiniu.com/kodo/4088/s3-access-domainname) · [Go SDK](https://developer.qiniu.com/kodo/12556/aws-sdk-go-examples) · [Compatible operations](https://developer.qiniu.com/kodo/4087/compatible-s3-api) |
| Alibaba Cloud OSS, Hangzhou | `https://s3.oss-cn-hangzhou.aliyuncs.com` | `cn-hangzhou` | Use a general-purpose bucket with regular objects and set `PlainListKeys: true`. OSS documents optional URL encoding of continuation tokens on its native listing. [AWS SDK setup](https://help.aliyun.com/zh/oss/developer-reference/use-aws-sdks-to-access-oss) · [Compatible operations](https://help.aliyun.com/zh/oss/developer-reference/compatibility-with-amazon-s3) · [Listing](https://help.aliyun.com/zh/oss/developer-reference/listobjectsv2/) |
| Tencent Cloud COS, Guangzhou | `https://cos.ap-guangzhou.myqcloud.com` | `ap-guangzhou` | Set `Options{ListV1: true}` and use the full `BucketName-APPID`. The default URL encoding preserves exact keys and markers. [AWS SDK setup](https://cloud.tencent.com/document/product/436/37421) · [Marker listing](https://intl.cloud.tencent.com/document/api/436/30614) |
| UCloud US3, Beijing 2 | `https://s3-cn-bj.ufileos.com` | `cn-bj` | Set `Options{PlainListKeys: true}`. Use the US3 S3 credential pair and keep the default V2 continuation. [Endpoints and signing](https://docs.ucloud.cn/ufile/s3/s3_introduction?id=%E6%A6%82%E8%BF%B0) · [Native listing and reads](https://ufile-release.cn-bj.ufileos.com/s3%2FUS3%20%E5%85%BC%E5%AE%B9S3%20API%20-%20v2.2.pdf) |
| Cloudflare R2 | `https://<ACCOUNT_ID>.r2.cloudflarestorage.com` | `auto` | Default listing. Use bucket-scoped R2 S3 credentials and the endpoint for the bucket jurisdiction. [AWS SDK setup](https://developers.cloudflare.com/r2/examples/aws/aws-sdk-go/) · [Compatible operations](https://developers.cloudflare.com/r2/api/s3/api/) |

`PlainListKeys` omits `encoding-type` from listing, directory probes and
recursive discovery. Without a declared encoding, keys are literal. Listings
that return line breaks in raw XML fail because XML can normalize the original
key bytes. Direct exact-key HEAD and GET use the supplied key.

`ListV1` selects the `ListObjects` operation for listing, directory probes and
discovery. A truncated page uses `NextMarker` when present. A flat listing can
continue from its last object key when the marker is absent. The continuation
must advance within the requested prefix. URL-encoded keys and markers are
decoded once. Use `PlainListKeys` for services that return raw XML keys.
[Amazon S3 ListObjects](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjects.html#AmazonS3-ListObjects-response-NextMarker)

## Namespace and reads

`core.Path(...)` selects the tree view. A key and a directory prefix of the same
name are ambiguous in this view, including ancestors.
Proving absence requires successful HEAD/prefix observations; denied access is
never absence. The root represents the prepared namespace, not proof of a bucket.
Listings preserve native order/continuations and decode URI-encoded keys once.
They retain one batch, not a snapshot. Open validates actual lengths and ranges;
a full response to a range request remains full. The Core owns returned bodies.

`core.Key("/cloud", rawKey)` selects the native key view of the same installed
Driver. `a`, `a/b`, `a/`, `a//b`, leading slashes and dot components remain distinct.
The prepared prefix is concatenated once; no path cleaning, ancestor HEAD or
directory probe occurs. Stat uses one HEAD and StreamRead Open one GET. The native limit
includes the configured prefix (1,024 UTF-8 bytes). Tree-only operations refuse
keys before effects; an object ending in `/` remains a file.

List accepts `core.Keys(space, prefix, delimiter).After(startAfter)`. Prefix and
delimiter are literal; an empty delimiter lists keys without groups. Returned
key addresses are complete keys relative to the prepared prefix, not basenames.
CommonPrefixes become explicit PrefixGroup selectors. Core cursors bind the full
query; native continuations remain opaque. Both views share the same Driver,
buffer budget and retirement owner, with pure exact-key/prefix conflict projection.

OpenDirectory retains enumerated child names in that call's prefix. OpenFile
reuses this membership without repeating ancestor probes. Close clears the scope;
it is neither a snapshot nor a version-bound delete capability.

`OpenOptions{Mode: core.BoundRead}` uses the same Open entry. An initial metadata
observation must return a non-null native VersionId; unversioned objects are
refused even if they have ETags. The returned File has `PinnedVersion` strength.
ReadAt issues one exact Range GET per nonempty in-bounds call and closes its body;
Stat issues HEAD. Both address the captured literal key and VersionId, without
tree/ancestor probes or current-key fallback. A deleted historical version fails.
The opening context does not survive handoff; each method has its own context,
and Close cancels and joins active native requests. Failed body cleanup remains
owned by that File and prevents successful retirement. No entire-file cache or
download is needed. Native permissions must allow access to the selected version;
see [GetObject](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html).

## Mutations and transfer options

Use the existing Core.Write, Copy, CopyTree, Mkdir, Remove and RemoveTree APIs. Capabilities
are native Driver implementations, not a second application API:

| Capability | Behavior |
| --- | --- |
| FileCreator | Conditional new object, with `If-None-Match: *` on final publication |
| FilePublisher | Complete new/replacement content; requires buffered options |
| ContinuableWriter | Explicit Core.Write continuation with immutable range source and protected checkpoints |
| DirectoryMaker | Conditional zero-byte directory marker |
| Remover | Files only; directory removal is unsupported |
| SubtreeRemover | Explicit RemoveTree subtree scope, with bounded key discovery and batch deletion |
| Relocator / scoped ChildRemover | Unsupported; no copy-delete or recursive-delete fallback |

Without options, Create keeps its single-PUT known-length path (0 through 5 GiB).
Unknown-length writes and Publish are refused before input consumption. Enable
bounded buffered/multipart writes explicitly:

```go
storage, err := s3.New(client, bucket, prefix, s3.Options{
    PartSize:         partBytes,
    MaxBufferedBytes: payloadBudget,
    PartConcurrency:  workers,
    PartAttempts:     attempts,
    CleanupTimeout:   cleanupDeadline,
})
```

PartSize and MaxBufferedBytes must be set together; the budget must hold at least
one part. Native parts are 5 MiB through 5 GiB, at most 10,000, with a smaller final
part allowed. PartConcurrency and PartAttempts default to one. CleanupTimeout zero
adds no deadline beyond the host client. No machine-derived budget is supplied.

Known content fitting one part is validated in a buffer sized to the input,
then PUT directly. Empty PUT reserves one byte of the shared budget.
Larger or unknown-length content uses native multipart and ordered completion
receipts. Input is read sequentially, never closed or replayed. The shared Driver
budget includes filling, sending and retained retry buffers across calls; excess
admission returns CapacityExceeded before reading input. Payload requests share
the configured concurrency cap, including buffered single PUT. SDK memory and
receipt metadata (bounded by the part limit) are outside the payload budget.

Only a retained identical part may retry its native upload ID/part number.
Initiation, final publication and delete never automatically replay; host
transports must preserve this boundary. CompleteMultipartUpload is called only
after normal EOF and exact declared length, when supplied. Failed replacement
input leaves old content unchanged. A lost final acknowledgement remains Unknown;
neither Stat nor a missing upload ID proves the publication outcome. Native
completion conditionality excludes an existing object key, not a concurrently
created directory prefix. Mkdir/removal observations are also not namespace locks.
No native empty-prefix condition is available, so Remove(directory) and default
RemoveTree Discovered scope are refused. Explicit DirectoryRemoveOptions{Scope:
core.Subtree} uses Core's separate remove_tree authority for the whole prefix.
It discovers keys once within the client entry/path budgets, then sends serial
DeleteObjects batches of at most 1,000 keys. Per-key errors inside HTTP 200 remain
errors and preserve partial effects. The directory marker is removed after child
batches succeed; prefix absence is then observed. New remaining keys return
DirectoryNotEmpty; they are not chased indefinitely. Native execution returns one
root receipt. Lost acknowledgements remain Unknown without automatic replay.
Deleting names does not reclaim older object versions or defeat native retention.

## Cancellation and private cleanup

Cancellation stops dispatch and waits for actual input/native callbacks before
releasing buffers. Arbitrary blocked I/O can delay return and retirement. Failed
multipart attempts abort only the captured immutable upload ID, then verify
ListParts; cleanup never deletes a logical file by name. Cleanup failure preserves
the file effect and retains private-resource ownership:

```go
pending := storage.PendingUploads()
err = storage.Cleanup(ctx) // call after active writes finish, before retirement
```

PendingUpload exposes attempt ID, key, native UploadID and submitted Bytes. Bytes
is conservative remote payload accounting, not RAM usage or provider quota.
Unresolved cleanup refuses new buffered writes while reads remain available;
Close is terminal and reports pending cleanup. An empty UploadID records uncertain
initiation, which Cleanup cannot resolve: the host must reconcile native resources
before preparing another owner. These records are in-memory, not crash-recovery
checkpoints or client resume tokens. No public live upload session is introduced.

## Durable continuation

Complete ordinary publication uses `WriteOptions{Policy: core.Publish()}` or
`core.Publish(core.IfAbsent())`, reusing the buffered/multipart pipeline above.
IfAbsent is enforced on PUT or CompleteMultipartUpload. Optional ObserveTarget
adds one destination HEAD for unconditional publication and returns Change.Before;
the observation is not a CAS. Ordinary calls do not acquire that extra metadata.
Single strong `IfMatch` publication uses the actual PUT/Complete `If-Match`.
Presence-only replacement, finite negative write tags and multi-tag sets are
refused before input; they are never approximated by a HEAD followed by PUT.
Reads forward supported single tag and presence guards to GET and check the
returned evidence before handing over the body. Native 304 and 412 responses
are precondition failures. A successful response that contradicts the guard is
an invalid upstream response. Tag guards require an ETag on success.
`ReadSelection.Version` sends an explicit non-null VersionId; it does not infer
object identity from an ETag. Remove supports single strong Match and Present
through native DeleteObject.IfMatch; directory removal restrictions still apply.

An explicit `Core.Write(..., WriteOptions{Continuation: ...})` selects native
continuation while preserving the same `Policy`: zero creates exclusively;
`Publish(...)` requests complete publication under its original predicate.
Native `ContinueWrite(ctx, address, source, length, core.WriteOptions)` receives
that policy and continuation together. It requires a known length, `core.Replayable` source, synchronous
checkpoint persistence, buffered options and a nonempty `Options.ResumeScope`.
The host makes ResumeScope a stable account/service identity, without credentials.
Changing that identity, bucket/key, source version, length, part size or replacement
predicate rejects an old checkpoint before data is sent. Copy/tree options currently
refuse continuation; one file's checkpoint cannot govern a composite operation.

The optional `transfer` consumer owns the protected record, source hold and host
configuration ID/revision. It calls the same Core.Write. Direct library consumers
can supply their own durable ownership. This mode always uses multipart, including
small/empty input, so a single native session identity governs recovery.

The version-5 recovery log records the full write policy once at initialization,
individual ETag
receipts and phase transitions. Each acknowledgement appends only its new fact;
normal N-part receipt encoding/retention is O(N), with O(N) replay/state on resume.
Version-2 boolean and version-3/4 predicate checkpoints retain their original
complete-publication meaning and can append version-5 progress records without
rewriting old bytes. Changing direct creation to complete publication, or the
reverse, rejects a checkpoint before native dispatch. The host atomically persists
the intent revision and appended bytes. Incompatible
or malformed records are refused before sending data; no implicit new session is
created from a failed replay. A failed/uncertain append stops further saves in
that execution until a new Run reloads the revision. Full historical checkpoint
snapshots are neither submitted per part nor required of the Store.

Ordinary multipart and continuation use the same initialization, part receipt,
completion and native cleanup implementation. The runtime-only path keeps its
small/empty PUT fast path; durable ownership adds barriers and acknowledgement
records without a second multipart writer or a per-part full snapshot.
Own ETag receipts are persisted after acknowledged parts. Restart reads only
missing native ranges and uses the existing shared buffer/concurrency/retry engine.
It does not construct completion receipts from ListParts. Receipt-only completion
needs no payload-buffer lease. Cancellation pauses and retains the upload/source;
ordinary PendingUploads/Cleanup does not own these separately persisted sessions.

Initialization and completion have persisted barriers before native dispatch.
Unknown initiation preserves the record for native reconciliation. A lost completion
is checked against the attempt's random `mountweave-upload` object metadata, length
and ETag. A positive match confirms publication; a missing/different object or
missing upload ID does not. Unresolved completion is not resubmitted or aborted,
and never causes a path-based delete. This avoids overwriting a later external
object while preserving Unknown. The host/transport must keep the no-replay
boundary, native metadata and single-executor assumptions.

Explicit Abort persists an abort barrier and retires only its UploadID, then
requires ListParts to report that session absent. A lost abort response can thus
be reconciled. Completed content is never undone. Unknown resources stay owned;
manual/native terminal proof remains necessary when no positive proof is available.
The closed continuation tests sync a fixture Store and reconstruct both intent
and retained input from disk; this is process-restart evidence, not a production
persistence engine, live-provider test or physical power-loss certification.

## Development and limits

The SDK owns signing, XML parsing and HTTP response cleanup. Its dependencies stay
in this module; no new dependency is required by buffered uploads. Ordinary bucket
behavior is targeted; directory buckets, access points, SSE-C, version selection
and individual compatible-provider guarantees are unverified.

The module is `github.com/nostalume/mountweave/drivers/s3`. The relative core
replace is for checkout development. This working-tree implementation requires
the matching core revision containing the shared internal chunker; it is not yet
part of the published v0.1.0 modules.

```sh
go test ./...
go vet ./...
go test -race ./... # requires a supported C compiler
```

Closed tests use the actual SDK serializer/signer/deserializer and synthetic HTTP
transport, including cross-part ranges, native part retry, out-of-order completion,
conflicts, cancellation and cleanup failure. No provider accounts are contacted.
Protocol references: [multipart limits](https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html),
[UploadPart](https://docs.aws.amazon.com/AmazonS3/latest/API/API_UploadPart.html),
[CompleteMultipartUpload](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompleteMultipartUpload.html),
[AbortMultipartUpload](https://docs.aws.amazon.com/AmazonS3/latest/API/API_AbortMultipartUpload.html).
