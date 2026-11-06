# Mountweave

Composable file management and storage primitives for Go.

Inspired by OpenList, Mountweave is an independently developed library for
building file-management backends. It is not a refactor of OpenList or a
replacement for its API. Its goal is to provide core functionality as an
embeddable dependency; it does not yet cover every OpenList feature or Driver.

The core uses only the Go standard library. It does not load configuration,
start listeners, or require a frontend, database, cache or task system.
Go 1.24 or newer is required. The package name of the root library is `core`.

## Start with a local mount

```go
package consumer

import (
    "errors"

    core "github.com/nostalume/mountweave"
    "github.com/nostalume/mountweave/drivers/local"
)

func NewLocalReader(directory string, authorize core.Authorizer) (*core.Core, error) {
    files, err := core.New(core.Options{Authorize: authorize})
    if err != nil {
        return nil, err
    }
    storage, err := local.New(directory)
    if err != nil {
        return nil, err
    }
    if _, err := files.Install("/files", storage); err != nil {
        return nil, errors.Join(err, storage.Close())
    }
    return files, nil
}
```

Supply an authorization policy, then call Stat, List, Open and optional mutations
with an explicit principal. Close every returned Body and directory scope.
Successful Install transfers Driver ownership; a rejected Driver stays with its
creator. The application eventually calls Core.Close with its own wait context.

All file operations use `core.Address`. Use `core.Path("/files/a")` for a tree
path or `core.Key("/cloud", "a//b")` for an exact native key in that binding.
Authorizers receive the complete address, including its namespace and key space.
`ListRequest{Query: core.Children(core.Path("/files")), ...}` reads children;
`core.Keys("/cloud", "a", "/")` selects keys and explicit prefix groups.
`Entry.Address` preserves the distinction. Ordinary reads/writes still use the
same Body/Reader contracts; no upload session or task queue is required.

## Compose what the application needs

| Component | Responsibility | Reference |
| --- | --- | --- |
| `core` | Namespace, authorization, file operations and resource lifetime | `go doc -all .` |
| `drivers/local` | Prepared filesystem root and native file operations | `go doc -all ./drivers/local` |
| `control` | Desired mount definitions and explicit activation/restore | `go doc -all ./control` |
| `task` | Arbitrary bounded function execution | `go doc -all ./task` |
| `input` | Durable sequential byte receipt, sealed sources and reference lifetime | `go doc -all ./input` |
| `transfer` | Durable upload intent, source retention and native checkpoint handoff | `go doc -all ./transfer` |
| `protocols/httpapi` | Optional HTTP delivery with injected identity and policies | [OpenAPI](api/openapi.json), `go doc -all ./protocols/httpapi` |
| `protocols/webdav` | Optional basic WebDAV tree endpoint, calling Core directly | `go doc -all ./protocols/webdav` |
| Optional Drivers | Native clients, credentials and storage representation | [S3](drivers/s3/README.md), [Alipan](drivers/alipan/README.md), [Pool](drivers/pool/README.md) |

See [Architecture](docs/architecture.md) for dependency direction and ownership.
Detailed behavioral contracts live with their owning packages in GoDoc.

`WriteOptions{Policy: core.Publish()}` publishes complete input at a name,
creating or replacing a file. `core.Publish(core.IfAbsent())` instead publishes
exclusively. The zero policy retains direct streaming creation. Both publication
policies reuse the Driver's private storage, budgets and cleanup; they require
its optional `FilePublisher` capability. `ObserveTarget` requests `Change.Before`
for response classification, not a conditional version update.

`Entry`, native `Metadata`, and mutation `Change` carry optional `Evidence`:
an opaque `Validator{Domain, Tag}` and a separate immutable `Version` selection.
Use `Publish(core.IfMatch(entry.Evidence.Validator))` for guarded publication,
`OpenOptions{Selection: core.ReadSelection{When: condition}}` for conditional
reading, and `CopyOptions{Source: selection, Target: writeOptions}` to keep both
endpoints distinct. `Stat` accepts the same optional read selection. `Remove`
and `Move` accept conditions through their existing options; capabilities remain
native. Unsupported predicates fail explicitly; a metadata check followed by an
unconditional mutation is never used to claim a guard. No content hash is added.

`OpenOptions{Mode: core.BoundRead}` returns `Content.File` (`*core.BoundFile`)
instead of Body. `File.ReadAt(ctx, buffer, offset)` and `File.Stat(ctx)` operate
on that resource; call `File.Close()` when finished. The opening context ends
at successful handoff. EOF and cancellation of one method do not close the File;
Close and mount retirement cancel and join active work before releasing its use.
Methods renew the original address's permissions and binding generation, without
reopening its current name. Calls on one File are serial and waits are cancelable.
The host bounds the number of retained files and concurrent callers.

`Content.Binding` reports actual strength. Local supplies `BoundObject`: the fd
survives rename/replacement, but in-place edits may remain visible. Pool supplies
`PinnedVersion` through its existing generation pin. S3 requires a non-null native
VersionId; later ranges and Stat use that exact version or fail, without fallback.
Request `Binding: core.PinnedVersion` when immutable contents are required; Local
refuses it. Alipan currently refuses BoundRead. The zero OpenOptions still uses
the original Body/WriterTo path, without a File or extra remote metadata request.

`OpenOptions{Mode: core.LiveEdit}` opens an existing object for visible editing.
Set `Read: true` to request reading too. `Creation: core.OpenOrCreate` permits
creation, while `core.OpenNew` requires native exclusion; `Truncate` acts at open.
`Content.Change` retains opening effects even on error. The same File provides
`WriteAt(ctx, bytes, offset)`, `Append(ctx, bytes)`, `Truncate(ctx, size)` and
`Sync(ctx)`. Byte writes return `(n, Change, error)`; inspect all three. Partial
bytes remain visible and Close does not publish or roll them back. `AppendOnly`
selects native append instead of random writes. Sync requests native file
durability, separately from visible effects. Local implements these operations
on Windows/Linux; S3, Pool and Alipan refuse LiveEdit before effects.

When receive order matters across handles and names, the host creates
`core.NewOrderScope(capacity)` and calls `scope.Reserve(requestCtx)` in receive
order, before dispatching goroutines. Pass `turn.Context()` to the ordinary
Core/File call; `defer turn.Cancel()` abandons any unused reservation. One turn
is one root operation; internal Copy/Tree/Batch calls reuse its admission.
A File opened in a scope requires a fresh turn from that scope on each method.
StreamRead holds its turn through actual Body completion; persistent Open hands
off after opening. Known partial effects permit the next turn; Unknown stops
the scope with `ErrOrderUncertain`. `scope.Stop()` cancels queued/active contexts
without acknowledging native I/O. File.Close and cursor cleanup bypass order.
This orders participants in that scope, not other scopes or external writers;
it uses no task records, worker or global file table.

`webdav.New(files, options)` returns an `http.Handler`. The host supplies the
URL prefix, Core tree root, authentication callback, XML/list and positive tree
planning budgets (`Options.Tree`), and cursor
lifetime. It supports OPTIONS, PROPFIND Depth 0/1, file GET/HEAD with a single
range, complete PUT, MKCOL, COPY, MOVE and DELETE. COPY accepts Depth 0 or infinity
for directories; MOVE/DELETE accept infinity. Overwrite defaults to T and selects
whole-target replacement, never directory merging. Destination stays within this
export and authority. Partial tree failures retain actual failed hrefs in 207;
successful and inferred dependency rows are omitted. JSON HTTP and WebDAV share tag parsing and Core
condition results: strong If-Match, weak If-None-Match, 304 for negative GET/HEAD
failures, and 412 for failed mutation predicates. HTTP tags encode the native
comparison domain; copy returned tags unchanged. S3 supports native single-tag
guards; Pool compares finite tag sets in its manifest transaction. Local and
Alipan do not fabricate validators or strong conditional mutation support.
Unimplemented date predicates and DAV If return explicit errors; tag headers
take precedence over their corresponding date headers. If-Range retains a full
response fallback. This is still a bounded protocol preview.
This endpoint does not advertise a compliance class, locks or user-property writes.
It needs no task queue or second storage implementation. Hosts behind a proxy
must provide the trusted public request host/scheme used for Destination checks.

All protocol constructors use `New(files, Options)`. JSON HTTP now takes
`Authenticate`, `CursorTTL` and `OnStreamError` in Options; the old positional
constructor and mutable Handler callback have been removed in this v0 checkout.

`protocols/sftp` is an optional Go module (Go 1.25+) exposing
`ServeConn(ctx, principal, channel)` for an authenticated SSH subsystem. It uses
bound file handles for reads, writes, append, FSTAT and size-only FSETSTAT, plus
bounded cursor-backed directories, lexical REALPATH, MKDIR and no-replacement
RENAME. The host owns SSH and cancels the session context on disconnect. Requests
execute in receive order; channel termination cancels active I/O and closes
owned files/cursors. Optional `ModeView` presents file and directory type bits
with logical export ceilings. The host derives the view and Core authorization
from the same policy. A compatible creation mask uses native defaults. A
conflicting mask fails before creation or truncation. LSTAT observes the final
entry without following links. Downloads use a separate no-follow bound open.
Local supports this observation on Windows and Linux. Pool and the S3 tree
namespace use their existing link-free lookup. Drivers without the capability
return an unsupported status. REMOVE/RMDIR use Core's non-directory and
empty-directory predicates. Link resolution and creation and general attribute
changes remain outside this profile.

`protocols/ftp` remains in the standard-library root module and exposes
`ServeConn(ctx, control)`. Options supplies authentication, active/passive network
factories, transfer limits and optional explicit TLS. It supports ordinary
file transfers, binary REST, APPE, TYPE A conversion, EPLF LIST, MLST/MLSD/NLST,
lexical navigation, MKD, no-replacement rename, DELE/RMD and ABOR. Ordinary STOR defaults to
complete publication; explicit LiveEdit, APPE and nonzero REST+STOR use native
visible editing and can leave partial effects. RETR preserves Body/WriterTo.
Each connection owns one active transfer, with data and content cleanup before completion.
Both adapters accept `OnFailure` for original causes and effect receipts.
Wire replies carry public reason text and known effects. Native causes remain
with the host.
See the protocol packages' GoDoc for exact configuration and ownership contracts.
The new adapters are checkout-only and have not been released.

## Current scope

The core supports mounted reads, fixed aliases, synchronous file operations,
bounded batches and finite tree operations. Copy belongs to Core; optional
Driver capabilities determine which mutations are available. Cross-mount Move
is currently unsupported. File effects remain observable alongside errors and
cancellation; a tree operation is not a transaction.
Capture can select a virtual hierarchy spanning existing mounts and aliases.
CopyTree uses that fixed membership; new bindings never join queued work, and
retired or rebound members fail admission instead of targeting replacements.

`CopyTree` accepts a file leaf or directory. `DirectoryCopyOptions` selects
`Existing: CreateNew` (default), `Merge` or `Replace`, and `Depth: Recursive`
(default) or `Self`. Whole replacement removes old membership after bounded
source discovery and known admission checks; it is not a tree transaction.
Per-file `WriteOptions` remains independent. `RemoveTree` also accepts file leaves.
Both return `TreeResult{Change, Items}`; items retain side, phase, error side and
confirmed effects. Directory plans require positive caller-selected budgets;
file leaves accept zero. Replacement bounds source and old-target plans separately,
and retains both plans' receipts. `MoveOptions{Existing: Replace, Tree: limits}`
permits target removal before the final exclusive native move. `MoveResult.Removed`
records that separate phase even when relocation fails; a later peer is not removed.

Local provides file and directory mutations. S3 provides reads, directory markers,
file deletion and optional bounded multipart creation/complete replacement. Alipan
provides reads, rotating credentials, directory creation, optional known-length
uploads, file deletion and qualified native rename/move. Native capability limits
and host-selected transfer budgets are described in the Driver references.
RemoveTree defaults to discovered names. Explicit subtree scope uses separate
subtree authority and optional native recursive deletion; it may affect new
descendants and returns the receipts the implementation can actually confirm.
ContentsOnly independently preserves the requested root and selected namespace
anchors while clearing mutable contents, including across a virtual hierarchy.
Preserved structure has no mutation receipt. Permissions remain dynamic; aliases
retain distinct path intents and known overlapping writes execute in order.
The optional Pool Driver provides an erasure-coded logical filesystem, member
management, repair and durable maintenance. These are independently versioned
modules; the core does not import them or their SDK/database dependencies.

Remote adapters have closed protocol tests, not a universal live-service
compatibility claim. The host owns credential persistence and chooses budgets.
Cancellation requests a stop; it does not forcibly interrupt arbitrary native I/O.
There is no product executable, frontend or legacy configuration converter.
An optional input Store supports client reconnect/restart continuation. Sealed
inputs feed Core.Write directly or through HTTP fs.write tasks; ordinary streamed
writes remain available. The optional transfer consumer resumes S3 native multipart
uploads through the same Core.Write, with host-provided protected persistence and
stable configuration binding. It does not make queued closures durable.

Optional `httpapi.Options.JobSources` exposes those existing durable owners through
one job resource. Register `httpapi.TransferJobs(service)` for uploads or the Pool
module's optional `delivery.Jobs` for maintenance. Create/query persisted intent
through `/api/v1/jobs/job` and `/api/v1/jobs`; run its advertised action through
`/api/v1/jobs/actions` or submit the same `kind`/`input` to `/api/v1/tasks`.
`JobRef{Source, ID, Generation}` identifies an intent; a TaskID identifies one
process-local execution attempt. The host restores original owners and chooses
which intents to resume. No HTTP query starts native recovery work.

## Development and availability

Install the core library:

```sh
go get github.com/nostalume/mountweave@v0.1.0
```

Optional Driver modules are published separately from the same repository:

```sh
go get github.com/nostalume/mountweave/drivers/s3@v0.1.0
go get github.com/nostalume/mountweave/drivers/alipan@v0.1.0
go get github.com/nostalume/mountweave/drivers/pool@v0.1.0
```

The published v0.1.0 Driver modules depend on core v0.1.0. Relative replacements
apply only to checkout development. Working-tree transfer additions require the
matching core revision and have not been released; consuming released modules
does not require local replacements.

See [Contributing](CONTRIBUTING.md) for commands covering all five modules.
The API is under development; no v1 compatibility promise is made yet.

## License

Mountweave source, documentation and schemas are available under the [MIT License](LICENSE).
Third-party dependencies retain their own licenses; see [Third-party notices](THIRD_PARTY_NOTICES.md).
