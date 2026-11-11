# Cloudreve V4

An optional, standard-library-only Driver for the Cloudreve V4 file-service
contract. It provides an ordinary account tree, native paging, streaming reads
and known-length exclusive creation using OneDrive-backed upload sessions.

```go
d, err := cloudreve.Open(ctx, cloudreve.Options{
    Endpoint: endpoint, RootURI: "cloudreve://my/Documents",
    APIClient: apiClient, ContentClient: contentClient,
    MaxBufferedBytes: uploadBudget,
})
```

`Open` automatically validates settings and checks the actual root directory.
`Validate` is available for resource-free configuration preflight; callers do
not need to call it before `Open`. Install the result with `Core.Install`.
Successful installation transfers ownership; close a non-nil failed construction
result. The existing `control.TypedFactory[cloudreve.Options]` can supply optional
managed activation without adding another constructor or configuration format.

`Endpoint` is an HTTP or HTTPS API origin; the Driver uses `/api/v4`.
`RootURI` selects an ordinary `cloudreve://my` account path, not a pinned object
ID. Replacing that directory can change subsequent path-based observations.
Search, trash, other-user and share views are not implicitly mounted. Native
address responses must match the requested path; symbolic shared redirects are
refused. Root metadata comes from the native list parent because file info rejects
the account root. Other paths use one native info request.

The host lends prepared clients and owns authentication, transports and refresh.
`APIClient` receives account API calls. `ContentClient` must not inject account
credentials: it receives delegated content URLs, OneDrive session URLs and
Cloudreve callback capabilities. Client copies disable cookie jars and automatic
redirects. Authentication middleware must not blindly replay mutations. Download
bodies, including error results, transfer to Core or a direct caller for closing.
Reads support ordinary streams and actual single ranges; bound handles and
unproven versions or conditions are refused through the existing Core contracts.

## Creation and confirmation

Set a positive `MaxBufferedBytes` to enable payload delivery; zero leaves reads
usable. A durable attempt with a confirmed ready body can finish without borrowing
a new payload buffer; current Core authorization still governs that write.
Each native session supplies its own chunk size. All active uploads share one
byte budget and one send slot in this Driver, with one retained chunk per call.
A chunk larger than the available quota is rejected before reading input. Native
preparation may already have created a visible placeholder; private cleanup does
not undo that file effect. There is no whole-file staging or source copy.
`RangeAttempts` defaults to one; retries query native progress before resending.
Requests finish and borrowed buffer reads are revoked before buffers are returned.

Use ordinary `Core.Write`, or `Core.Create`. Creation never requests a new version,
automatic rename or replacement. Empty input is checked through normal EOF before
native file creation with `err_on_conflict`. Nonempty input prepares an exclusive
Cloudreve placeholder, sends OneDrive ranges through the same internal mechanism
as Graph Drive, and confirms normal EOF before the final range. Only a successful
Cloudreve callback (`code: 0`) supplies the ordinary file receipt. There is no extra
path lookup in ordinary writes that could mistake a later replacement for this
operation's receipt.

This Driver implements `FileCreator`, not `FilePublisher`: preparation can expose
an incomplete placeholder. Complete-publication policies are rejected by Core
before input consumption. Only unrelayed, unencrypted OneDrive upload credentials
are currently supported. Other native policies are refused without reading input;
their already prepared resources still have a cleanup owner. Metadata and parsed
response memory are separate from the upload payload budget; `MaxResponseBytes`
optionally bounds parsed native JSON.

## Durable creation

The existing `Core.Write` accepts `WriteOptions.Continuation` with a versioned
`Replayable` source and known length. The existing Transfer service can own the
protected log and Input retention. Only ordinary exclusive creation is supported;
continuation does not turn this Driver into a complete-publication capability.

Recovery binds the API authority, root URI, actual selected root and parent IDs,
target URI, source version, length and policy. The selected root is a real global
FileID in the authenticated account tree; no additional profile request or
configuration selector is needed. Session credentials are saved before querying
extended file info. An exact upload-session metadata value and one version entity
establish the owned FileID/EntityID pair before any source bytes are sent.

The same OneDrive loop reads each new durable payload through an exact verified
range and retains it for retries. Recovery queries native progress instead of
logging every fragment. Separate records preserve body readiness, callback intent
and the file receipt. A ready-body restart does not resend content or repeat its
already successful ownership admission. A callback is never blindly replayed.
After a lost callback acknowledgement, an ID lookup must return the same FileID,
primary EntityID and size to confirm the result, including after a rename. This
reconciliation does not fetch the full version list.
Nonprimary versions, missing entities, names and sizes alone are not proof.

A final body acknowledgement lost before `body_ready` was durably recorded cannot
be reconstructed from a missing OneDrive session. The callback's own size check
is insufficient to manufacture that receipt. Lost initialization cannot be
recreated or adopted by path: preparation may already have committed a logical
placeholder, while the service-generated session identity is unknown. Empty
files retain their single exclusive creation request; a lost reply stays unknown.

Checkpoints contain secret upload and callback capabilities. Keep them protected,
outside public status and untrusted input. Requests, leases, clients, closures and
source readers are not serialized. Cancellation pauses execution; retirement and
ordinary Cleanup do not adopt durable sessions. Explicit Abort is recorded before
private cleanup, so a restart cannot resurrect a canceled upload or callback.

## Failures and ownership

Failed ordinary uploads retire only the admitted OneDrive upload URL. The
Cloudreve `DELETE /file/upload` operation is not used: if its session has expired,
it can modify a replacement at the supplied path, and it has no expected FileID
or EntityID condition. Confirming private-body retirement leaves the logical
`Incomplete` effect and reports the remaining placeholder. That file artifact is
returned to the caller; it does not occupy private admission after the body is
retired. A failed private cleanup retains its owned resource for retry.

A callback failure or lost acknowledgement returns `Unknown`, because file
publication may have committed before the error.
It is not retried or canceled automatically. A successful callback remains
`Applied` even if response closure or cancellation subsequently reports an error.
Native error codes, messages and cleanup causes remain attached to the error.

The shared write lifetime distinguishes a file result from content readiness and
from failure without a terminal receipt. Failed or canceled body delivery never
starts the callback. Each response is consumed and closed at its native boundary;
failed closures remain owned separately from native session cancellation.

`Cleanup(ctx)` retries retained private-body cleanup after active writes have
ended, including after retirement. It does not delete a logical file by path or
claim that a missing session proves publication. Lost initialization and attempted
callbacks without confirmation require native reconciliation; ownership and the
diagnostic remain retained. Unresolved sessions prevent new uploads.
Concurrent unknown initiations keep separate local attempts and their target URIs,
even when neither response supplied a native session ID.
`CleanupTimeout` optionally bounds automatic cancellation; zero adds no deadline
beyond the borrowed client. `Close` retries failed response closures and reports
retained sessions without closing host clients.

Durable Abort can retire the private body, but cannot atomically remove the
logical placeholder through the current Cloudreve API. It retains `Incomplete`
or `Unknown`, a diagnostic and the source hold. A later Run does not resume an
aborting intent. Only a confirmed file outcome or an already definite initial
refusal reaches the existing Transfer terminal rules; partial Abort does not
pretend to be `Unchanged` or release its source.

Native contracts: [uploads](https://docs.cloudreve.org/en/api/upload),
[file URIs](https://docs.cloudreve.org/en/api/file-uri),
[prepare, complete and cancellation](https://github.com/cloudreve/Cloudreve/blob/9f1e0b8333f5c7304a09e1dcb3f9c170dddd63f3/pkg/filemanager/fs/dbfs/upload.go),
[entity publication](https://github.com/cloudreve/Cloudreve/blob/9f1e0b8333f5c7304a09e1dcb3f9c170dddd63f3/inventory/file.go),
[callback](https://github.com/cloudreve/Cloudreve/blob/9f1e0b8333f5c7304a09e1dcb3f9c170dddd63f3/service/callback/upload.go).
The module uses the adjacent Core checkout through its local replacement; this
change does not publish a package version.
