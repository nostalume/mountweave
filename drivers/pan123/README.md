# 123 Open source

This optional Go 1.24 module connects ordinary 123 Open account files to
Mountweave. It uses the standard library, Core and its existing private helpers.

```go
source, err := pan123.Open(ctx, pan123.Options{
    RootPath: "/Archive",
    Client: authenticatedClient,
})
if err != nil {
    if source != nil { _ = source.Close() }
    return err
}
mount, err := storage.Install("/cloud", source)
if err != nil { _ = source.Close(); return err }
_ = mount
```

Choose either an account-root-relative `RootPath` or a native decimal `RootID`.
If neither is specified, the account root is ID `0`. Construction validates settings and resolves a
path once to its actual directory ID. Root `0` is checked with a native list,
not an undocumented `detail(0)` request; another RootID uses its details. A
RootPath uses the directory entry already selected, without repeating details.
A non-nil failed Driver retains cleanup and must be closed. Successful
`Core.Install` transfers ownership. Reconstructing a path later can bind another
ID; the constructor does not establish a directory snapshot or a permanent
permission observation.

## Existing file API and cost

- `Stat`, `List`, ordinary `Open` and Core `Copy` as a source use existing APIs.
  A path component is selected by exact name from its current parent ID using
  `childscan.Unique`. One candidate is retained until the native stream proves
  it unique; different IDs with that name are ambiguous, while a repeated
  matching ID is an invalid upstream response.
- Listing uses `/api/v2/file/list`, at most 100 native entries per page. Only
  `lastFileId=-1` means completion. The returned continuation is used as-is,
  including zero; short or empty pages do not imply completion. An unchanged
  nonterminal continuation is invalid. A repeating native cursor cycle also
  fails. Public cursor replay remains Core-owned.
- The service includes recycle-bin entries. Each page's parent and recycle state
  are checked, then `trashed=1` items are omitted from the ordinary active tree.
  A filtered empty page retains its native continuation. Global `searchData`
  is never used for root-relative lookup because it ignores the parent ID.
- Native fields are admitted once and kept with their ID and prepared metadata.
  `Open` consumes that same lookup result, obtains `downloadUrl` using the ID,
  closes the control response, then opens the private body. It does not call
  `Stat`, repeat leaf details or issue a preliminary `HEAD`.
- `httpcontent` interprets actual HTTP 200/206/416 lengths and ranges. A full
  response remains a full-response fallback; old listing size is not copied
  over actual content observations. The native `etag` is MD5, not a version or
  strong validator. Unzoned native date strings are not guessed as modification
  times.

A path of depth D costs the sum of scanned native pages over those D parents,
plus one capability request and one content GET for opening. A root `Stat`
refreshes the corresponding native observation. Each list page is decoded within
`MaxResponseBytes` (default 4 MiB, configurable independently of file size).
Memory is the current native page, one matching candidate and Core's public
replay state. No full-directory cache or file staging is introduced.
`MaxResolvePages` is an optional per-component page budget, matching Alipan;
zero adds no page limit. Native listings are not snapshots.

## Authentication and resource ownership

`Client` is required and borrowed. Its transport authenticates control requests
only to `https://open-api.123pan.com`. The Driver adds the native
`Platform: open_platform` header. The host owns credentials, expiry, token
rotation and any required persistence; this module does not invent a refresh
state machine or ask for client secrets. Both developer credentials and OAuth
can provide a prepared authenticated client while retaining their own lifetimes.

`ContentClient` is independently borrowed and defaults to a fresh client, never a
copy of the authenticated transport. The constructor rejects passing the same
client for content and authenticated control or upload. Keep credential
injection out of the content transport, including transport shared by distinct
client instances. Both copied clients disable cookie jars. Control redirects are refused. Body
capabilities and their bounded GET redirect chains require HTTPS, no userinfo,
opaque URL or fragment; each hop retains only the original Range and identity
encoding. Account authentication, cookies and Referer are not forwarded. Each
intermediate response is closed before the next request; failed cleanup stops
the chain and remains Driver-owned for retry at `Close`.

Core owns successful returned bodies, including EOF, cancellation and mount
retirement. The Driver owns failed download responses and retries their private
cleanup. Each iterator applies `Next` context to its current native
request; `Close` cancels and joins that request. Short-response close failures
remain Driver-owned. Unexpected connection or cleanup EOF never becomes normal
enumeration EOF. Borrowed clients and transports remain host-owned. Metadata
control failures retain the native code, message and trace. The Driver redacts
private HTTP exchange errors and intermediate download response cleanup errors.
Ordinary download quota exhaustion (`5113`) reports `UpstreamThrottled` and
retains the native code. Core receives the original successful download body.
Hosts must keep secrets out of errors from custom content body implementations.

## Optional direct links

Set `UseDirectLinks: true` for a directory whose direct-link space is already
enabled. Listing and exact path lookup still use the 123 Open file catalog.
`Open` requests the selected file's `url` from
`GET /api/v1/direct-link/url?fileID=...` and opens the returned HTTPS grant
using `ContentClient`. No extra metadata request is needed. A direct-link
refusal is returned as-is through the existing error categories.

The Driver does not enable the feature, alter CDN configuration or fall back
to an ordinary download. The service controls entitlement and traffic quota.
The Driver sends the URL as returned. If a deployment requires additional URL
signing, the host may supply a dedicated content transport restricted to its
trusted direct-link domains. That transport must keep account credentials and
signed URLs out of requests to other origins and out of diagnostics.

[Official direct-link grant](https://123yunpan.yuque.com/org-wiki-123yunpan-muaork/cr6ced/tdxfsmtemp4gu4o2) ·
[123 Cloud Drive CDN](https://www.123pan.com/link/)

## Optional directory creation and file removal

`AllowMkdir` enables `Core.Mkdir` independently of file upload settings. It uses
the fixed authenticated control `Client`, existing parent ID lookup, and the
native no-duplicate directory name rule. One `POST /upload/v1/file/mkdir`
submits the leaf name and parent ID without a target precheck. A new nonzero
directory ID confirms creation. An unconfirmed response leaves the effect
unknown. The official reference does not specify a distinct name-conflict code.

`AllowRemove` enables ordinary single-file `Core.Remove` calls through the
control `Client`. After exact path lookup, one `POST /api/v1/file/trash` submits
only the selected native file ID. This moves the file to the provider's recycle
bin. A successful HTTP 200 envelope with `code: 0` confirms the operation. If
dispatch is followed by a lost reply or cancellation without confirmation, the
effect remains unknown. The supported mode is unconditional file removal with
default `RemoveOptions`.

## Optional file creation

Set `UploadClient` and a positive `MaxUploadBytes` to enable ordinary new-file
`Core.Write` and `Copy` targets. Leave mutation settings unset for a read-only
instance.
`UploadClient` is borrowed and uses the same fixed 123 Open account as `Client`.
Its transport must refuse the whole request outside its trusted 123 Open upload
origins and refrain from retrying POST requests or adding cookies. The Driver
selects one HTTPS origin from the authenticated domain response at the fixed
control API. Cookie jars and redirects are disabled. Control and content
download clients retain their separate authentication rules.

Both upload modes require a known size and an MD5. When the input fits the
configured budget and the native single-request limit, the Driver resolves the
parent before borrowing a payload buffer. It then reads the input once through
exact length and EOF checks and sends that same buffer. Larger files
require `core.Replayable`, such as a fresh full reader from a sealed `input.Store`
record. The native 1 GB and 10 GB limits are interpreted as local decimal byte
guards. Ordinary sequential inputs, including a Core Copy body, must fit the
single-request path. The host retains ownership of the original input.

Multipart first reads the supplied stream through EOF to calculate its native
MD5 and a local SHA-256. A native reuse receipt with a nonzero file ID completes
in one control request and one input pass. Otherwise the control response gives
the native slice size and upload origins. Before sending any slice the Driver
compares a full independent replay against the initial SHA-256. This check
rejects a replay whose offsets describe a different view of the same source.
The immutable Replayable contract then permits a third input pass to send the
native slices, using one reusable buffer. `MaxUploadBytes` covers all active
payload buffers in the instance, with one payload POST in flight. A native slice
that does not fit this budget stops the operation before slice upload. Core
retirement cancels and waits for hashing, transfers and completion polling.
The caller's context governs polling and HTTP requests. A blocked input reader
must return before the write can finish.
V2 multipart is the default. Its official example shows an HTTP upload origin;
the adapter needs a native response with a trusted HTTPS origin to send slices.
Set `PresignedMultipart` to use the separate V1 multipart protocol from the
start of a large-file write. Single-request writes still use V2 and require the
existing `UploadClient` and budget. V1 fetches each slice's HTTPS presigned URL
from the control API and uses it immediately for a credential-free PUT via
`ContentClient`. It sends neither account authorization nor `Platform` and
refuses redirects. No URL expiration or cross-version session compatibility is
assumed.

The official API disallows duplicate names when no optional rename or overwrite
policy is chosen. The Driver omits `duplicate` and does not precheck the leaf.
Apart from native reuse, only `completed: true` with a nonzero file ID confirms
the file. V2 polls its completion endpoint after one second only for an explicit
`completed: false`. V1 sends completion once and uses the separate result query
after `async: true`. A pending result waits one second before polling again.
If the V1 completion receipt is lost, unreadable or masked by a gateway
failure (HTTP 502 or 504), the active write may query once using the same
`preuploadID`. Only a completed file ID confirms success.
A pending or unavailable result leaves the effect unknown. V2 has no documented
separate result query for a lost completion receipt. A V1 non-reuse write needs
one create, two requests per slice, and one completion, plus any result queries.
It uses the service's final file check without listing uploaded slices. An
unfinished session stays upstream. The host's transports must not replay upload
requests.
Upload HTTP status and native code remain available without echoing raw upload
diagnostics that could contain credentials. A response close failure preserves a
confirmed file receipt. With a valid pending receipt, the Driver makes one more
confirmation request for the same upload and reports the close error alongside
the resulting effect. Driver close retries the failed response close.

The adapter has no permanent deletion, move, complete replacement,
version or bound access, Lstat, NoFollow or durable
continuation. Reading does not create shares, enable paid direct-link space or
import files. Native entitlement or audit refusal remains an upstream failure.

## Verification

Three closed HTTP synthesis families cover root modes, precise IDs beyond binary
floating-point precision, exact names, recycle-only and terminal pages, public
replay, ambiguous names, parent/continuation checks, ranges and full fallback,
credential isolation, HTTPS redirects, Copy to Local, native diagnostics, budgets,
cancellation, retirement and retryable cleanup. They use synthetic authentication
and loopback HTTP/TLS services, not real accounts. They do not certify all live
CDN policies, deployments or authorization scopes. One upload synthesis case
shares these fixtures for native routing, exact multipart fields, Core Write and
Copy, failure effects, shared admission, cancellation and response cleanup.
The multipart synthesis case checks native slice sizes and ordering, reuse,
different replay views, resource limits, lost confirmation and canceled polling.
It also checks V1 signed URL binding, credential-free PUT and completion results.
The existing native failure fixture checks optional directory creation and
single-file trash, including authorization, exact native IDs, cancellation and
receipt handling.
The development module uses an adjacent Core checkout replacement.

Native references: [list](https://123yunpan.yuque.com/org-wiki-123yunpan-muaork/cr6ced/zrip9b0ye81zimv4),
[details](https://123yunpan.yuque.com/org-wiki-123yunpan-muaork/cr6ced/owapsz373dzwiqbp),
[download](https://123yunpan.yuque.com/org-wiki-123yunpan-muaork/cr6ced/fnf60phsushn8ip2),
[directory creation](https://123yunpan.yuque.com/org-wiki-123yunpan-muaork/cr6ced/ouyvcxqg3185zzk4),
[file trash](https://123yunpan.yuque.com/org-wiki-123yunpan-muaork/cr6ced/en07662k2kki4bo6),
[upload origin](https://123yunpan.yuque.com/org-wiki-123yunpan-muaork/cr6ced/agn8lolktbqie7p9),
[single-request upload](https://123yunpan.yuque.com/org-wiki-123yunpan-muaork/cr6ced/xhiht1uh3yp92pzc),
[multipart workflow](https://123yunpan.yuque.com/org-wiki-123yunpan-muaork/cr6ced/xogi45g7okqk7svr),
[V1 presigned workflow](https://123yunpan.yuque.com/org-wiki-123yunpan-muaork/cr6ced/il16qi0opiel4889),
[V1 upload result](https://123yunpan.yuque.com/org-wiki-123yunpan-muaork/cr6ced/qgcosr6adkmm51h7),
[developer token](https://123yunpan.yuque.com/org-wiki-123yunpan-muaork/cr6ced/gn1nai4x0v0ry9ki)
and [OAuth rotation](https://123yunpan.yuque.com/org-wiki-123yunpan-muaork/cr6ced/gammzlhe6k4qtwd9).
