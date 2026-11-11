# Dropbox source

This optional Go 1.24 module connects Dropbox to the existing Core tree API.
It uses only the standard library and Core, including the shared HTTP range
interpreter. There is no Dropbox SDK, directory cache or separate download API.

~~~go
source, err := dropbox.Open(ctx, dropbox.Options{
    Client:   authenticatedClient,
    Root:     dropbox.AccountRoot(),
    RootPath: "/Archive",
    Export:   &dropbox.ExportOptions{}, // Explicitly accept native default export.
})
if err != nil {
    if source != nil {
        _ = source.Close()
    }
    return err
}
mount, err := storage.Install("/cloud", source)
if err != nil {
    _ = source.Close()
    return err
}
_ = mount
~~~

## Construction and credentials

`Root` has three closed selections:

| Selection | Native meaning |
| --- | --- |
| Zero value | Preserve the application's authorized default; no Path-Root header |
| `AccountRoot()` | Query the current account once, then use expected-root mode |
| `NamespaceRoot(id)` | Fix the selected namespace using namespace_id mode |

The default is not universally the user's home: an App Folder app starts in its
app folder. AccountRoot additionally requires `account_info.read`. If the account
root later changes, native `invalid_root` remains an error; the Driver does not
silently rebind or fall back to another namespace.

`RootPath` is relative to that selection and defaults to `/`. Construction verifies
each exact path component using metadata, then retains the selected directory ID.
The complete namespace root has no native folder metadata; construction confirms
it with a one-item listing instead, without caching its page or manufacturing an
object ID. This confirms current access, not a permanent permission grant.

`Client` is borrowed and shallow-copied. Its host owns authentication, refresh,
transport configuration, retries and a fixed user/admin actor. Authentication
must work for both `api.dropboxapi.com` and `content.dropboxapi.com`. The Driver
owns Path-Root, so a supplied transport must not inject or rewrite that header.
Cookie jars and automatic redirects are disabled. Client, credentials and
transports are never closed by this module.

`Export` has three intentional states: nil disables conversion; a non-nil empty
Format accepts the native default; a nonempty Format selects that native format.
Construction copies this value. Format availability remains a native per-file
decision, and invalid formats retain their original failure reason.

## Existing Core operations

The module implements ordinary `Stat`, `List`, `Open` and source `Copy` through the
existing Driver API. Paths remain exact Core paths even though Dropbox lookup
itself ignores case. Each ancestor is looked up directly as parent-ID/name and
its actual `metadata.name` is checked. The Driver does not trust the potentially
stale ancestor spelling of `path_display`, scan siblings or expose native ID,
revision or namespace addressing through public paths.

An Iterator resolves its directory once and retains the native cursor.
Each Next obtains one page; its requested size is a hint, since the native limit
may be exceeded slightly. Empty pages continue while `has_more` is true; the
first false ends this enumeration even though the native cursor remains useful
for later change polling. There is no background polling, snapshot, global
deduplication or cursor-history index. Reset/path errors end the current List
instead of restarting and duplicating its earlier pages. Enumeration excludes
deleted entries and retains mounted and non-downloadable items.

Ordinary Open resolves ancestors, then requests download directly using the
parent ID and final name. The same response's `Dropbox-API-Result` identifies the
actual file; its exact final name is checked before any Body is delivered.
There is no preliminary leaf metadata request, HEAD or temporary-link fetch.

Only a complete HTTP 409 `unsupported_file` result, with Export explicitly
enabled, closes the error response and makes one export request to the same
parent/name. The export receipt performs its own leaf selection; an earlier
failed download did not bind an object. Other errors never trigger conversion,
and export never loops back to download. Names remain unchanged even when the
native exported filename has a different suffix.

Download metadata describes the actual source bytes. Export uses
`export_metadata.size` for the output and `file_metadata` for the original name;
Stat/List do not treat a non-downloadable file's storage size as export length.
Both native methods support actual HTTP range interpretation, including a full
response fallback. Known lengths and totals must agree with the same response's
native size; a 200 response with unknown HTTP length can use that native size.

A 416 does not promise same-response file metadata. This failure path alone
checks the leaf once more under the already selected parent. A wrong spelling
returns NotFound and a directory returns NotFile. A confirmed file returns
RangeNotSatisfiable without the older response's size/total, since the later
observation might describe a replacement. The original Body remains Core-owned.
Ordinary 200/206 paths acquire no extra lookup.

## Optional file creation

Set `AllowCreate: true` and provide the `files.content.write` scope to enable
ordinary `Core.Write` and Copy targets. The driver resolves the parent once and
uses its exact child name with mode `add`, `autorename=false` and
`strict_conflict=true`. Identical existing content is
still a conflict. A conflict may use Dropbox's case-insensitive namespace, so
it confirms a rejected name without establishing the existing entry's spelling.

Known lengths up to 150 MiB use one `files/upload` request with
`Content-Length`. Unknown lengths use the same request and byte limit with
HTTP streaming. A reliable single-request receipt with an input error can
report `Incomplete`.

Known lengths above 150 MiB use one sequential session up to the native
2,199,019,061,248-byte limit. The driver streams segments of at most 150 MiB
and advances the native offset after each acknowledged segment. Once the
last append verifies the input's actual EOF, one empty `finish` request
commits the file. A size N session uses ceil(N / 150 MiB) data requests plus
one commit. It uses no full-segment or full-file buffer. The session ID
stays in the request header and error diagnostics omit it.

The native file receipt must identify the requested file and match its
submitted size. Before session commit, failure leaves the target unchanged.
A lost or contradictory commit receipt returns `Unknown`. A response close
error keeps any confirmed effect. Unfinished sessions expire under Dropbox's
native session policy.

## Cost, ownership and scope

For a depth-D path below the mounted root, Stat uses D direct lookups; ordinary
Open uses D-1 lookups plus one download. Explicit export adds only the one
recognized representation transition; 416 adds only its failure-path lookup.
List resolves its directory once, then uses one request per native page. Core
owns public remainder and replay, so page replay does not contact Dropbox.
There is no full-directory or full-body staging.

`MaxResponseBytes` defaults to 4 MiB for RPC responses, error bodies and result
metadata headers, independently of file size. Client deadlines and call contexts
govern work. Iterator Close cancels and joins current I/O. Core owns every returned
Body on success or failure; the Driver retains failed internal response cleanup
for a later Close. Native reasons, request IDs and Retry-After diagnostics remain
inspectable; no automatic retry or native cursor recovery is introduced.

Conditional/version reads, Lstat, NoFollow, bound handles, durable upload recovery
and synchronization remain outside this module's supported operations.
Ordinary reads do not promise a snapshot or an atomic fixed ancestor chain.

Tests use confined HTTP fixtures, synthetic credentials and Core/Local
composition. No real account or complete deployment compatibility is claimed.
The development module replaces Core with the adjacent checkout; it has not
been published as a new version.

Native references: [path and response formats](https://docs.dropboxapi.com/dropbox-api/docs/technical-reference/request-response-formats),
[root modes](https://docs.dropboxapi.com/dropbox-api/docs/technical-reference/path-root-header-modes),
[metadata](https://docs.dropboxapi.com/dropbox-api/api-reference/user-endpoints/files/get-metadata),
[listing](https://docs.dropboxapi.com/dropbox-api/api-reference/user-endpoints/files/list-folder),
[cursors and changes](https://docs.dropboxapi.com/dropbox-api/docs/detecting-changes),
[download](https://docs.dropboxapi.com/dropbox-api/api-reference/user-endpoints/files/download),
[export](https://docs.dropboxapi.com/dropbox-api/api-reference/user-endpoints/files/export),
[upload](https://www.dropbox.com/developers/documentation/http/documentation#files-upload)
and [official type definitions](https://github.com/dropbox/dropbox-api-spec).
