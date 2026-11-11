# Google Drive source

This optional Go 1.24 module connects Google Drive V3 to the existing Core tree
API. It uses the standard library, Core's native iterator and shared HTTP body
interpreter. It does not require a Google SDK or introduce a public download API.

~~~go
driver, err := googledrive.Open(ctx, googledrive.Options{
    RootPath: "/Documents",
    Client:   authenticatedClient,
    ExportFormats: map[string]string{
        "application/vnd.google-apps.document": "application/pdf",
    },
})
if err != nil {
    if driver != nil {
        _ = driver.Close()
    }
    return err
}
mount, err := storage.Install("/cloud", driver)
if err != nil {
    _ = driver.Close()
    return err
}
_ = mount
~~~

`Client` is a borrowed, already authenticated HTTP client. Its host owns OAuth,
refresh persistence, transport configuration and retry policy. `ContentClient`
is independently borrowed, or defaults to a separate client without account
authentication. A supplied content client must differ from the authenticated
client, and its transport must omit account credentials.
Both clients disable cookie jars and automatic redirects without modifying the
original clients. Driver closing never closes clients, transports or credentials.

## Root and ordinary tree access

`RootID` and `RootPath` are alternatives. With neither, construction resolves the
native `root` alias to its actual directory ID. `RootPath` is relative to that
account root; `RootID` directly selects a folder, including a shared-drive root
or subfolder. Shared-drive query settings come from the root's native `driveId`;
the caller does not also provide a `DriveID`.

`RootResourceKey` is optional for a link-shared root. It is sent with the directly
selected RootID, then remains paired with the actual returned ID. With RootPath,
it is bound only after the selected directory has been resolved. Keys returned
in metadata stay with their respective native items. Only referenced ID/key
pairs enter Google API requests, never public Metadata or a global header cache.

Construction confirms a real directory. Each path component uses a native
parent-ID and name query, excludes trash, then checks exact returned spelling and
parent membership. The existing child scanner confirms uniqueness across all
candidate pages. It does not enumerate unrelated siblings, use global search or
convert IDs into public paths. Same-name distinct IDs are ambiguous; repeated
matching IDs are an invalid native enumeration. Ordinary access is not a
snapshot, an atomic membership guarantee, a NoFollow promise or bound access.

`Stat`, `List`, ordinary `Open` and source `Copy` use existing Core operations.
Only the actual folder MIME becomes Directory. Native documents and shortcuts
remain visible Files, preserving names; their uncertain byte sizes remain unknown.
Shortcuts are not silently followed or converted into folders. Native download
capabilities travel with the selected item; a known `canDownload=false` retains
an upstream access-denied reason without another detail or HEAD request.

The Iterator retains the selected parent and its private page token. Empty or
short pages with a continuation continue, even when the native empty `files`
field is omitted. Only an absent/empty `nextPageToken` ends enumeration. An
incomplete search fails rather than proving uniqueness or absence. Invalidated
tokens fail the current enumeration; the caller can explicitly start a new
List. Core owns public remainder, cursor authorization and page replay.

## Body and explicit export

Ordinary blob `Open` performs a direct authenticated GET to the fixed Drive API,
using the item already found during resolution. It does not obtain a temporary
URL first, repeat Stat, send HEAD or stage a full file.

Metadata requests refuse redirects. The body path allows at most ten requests:
same-API-origin redirects may use the authenticated client and the selected
file's own resource key. After the first origin crossing, all later requests use
the independent content client, including a return to the API origin. External
requests receive no account credentials, resource-key header, cookies or Referer.
Each hop keeps identity encoding and the original blob range. Only valid HTTPS
targets without userinfo or fragments are accepted. Intermediate responses are
closed before continuing. A failed close stops the chain and remains Driver-owned.
Drive API errors retain their native reason. Delegated HTTP failures report the
status without interpreting the body as a Drive API error.

`ExportFormats` is an optional map from native source MIME to desired output MIME.
Construction copies the map and, when nonempty, checks it once against
`about.exportFormats`. A confirmed mapping takes precedence over the MIME's name
or prefix. No format is chosen by default, no filename suffix is appended, and
file calls do not repeat format discovery. The shared read path returns the
actual body length and range facts.

Export uses the same `Open`, sends no Range and returns the actual complete
response as the existing full-body fallback. Stat/List do not use the native
document's storage/billing size as the selected export length. There is no
whole-file buffering to emulate ranges. The native `files.export` output limit
is 10 MB; format-specific losses, such as selecting a sheet when exporting CSV,
remain properties of the caller's chosen representation.

Folders, Google/third-party shortcuts and Google Vids are rejected as export
configuration sources before network access: their byte workflow is not this
module's ordinary export method. Without such configuration they remain visible;
Open explains unsupported representations. Vids and other long-running download
operations, shortcut traversal, writes, versions, Lstat, NoFollow and bound access
are not implemented.

## Resources and verification

Each metadata/error response defaults to a configurable 4 MiB byte budget;
this is independent of file size. `MaxResolvePages` applies per path component;
zero adds no page-count limit. Client deadlines and contexts govern I/O.
Depth D with Q_i matching-candidate pages costs sum(Q_i) requests for resolution,
then one body GET on the ordinary no-redirect path. List costs one request per
native page and Core replay costs none. Memory is bounded metadata pages plus
Core's public remainder/replay, not a full-directory cache or body copy.

Core owns the successful response body. Keep secrets out of late read and close
diagnostics from custom transports. Iterator closing cancels and joins its
current metadata request. The Driver closes failed responses and intermediate
redirect bodies with private I/O diagnostics. It retains failed closes for a
later Close, so a failed construction may return a non-nil Driver. Physical and
cleanup EOF do not signal successful listing completion.

Tests use confined HTTP/TLS fixtures, synthetic authentication and Core/Local
composition. They do not contact real accounts or certify every Google deployment.
The development module currently replaces Core with the adjacent checkout;
no new version or independent-host release has been published.

Native references: [root folders](https://developers.google.com/workspace/drive/api/guides/folder),
[queries](https://developers.google.com/workspace/drive/api/guides/search-files),
[pagination](https://developers.google.com/workspace/drive/api/reference/rest/v3/files/list),
[shared drives](https://developers.google.com/workspace/drive/api/guides/enable-shareddrives),
[resource keys](https://developers.google.com/workspace/drive/api/guides/resource-keys),
[downloads](https://developers.google.com/workspace/drive/api/guides/manage-downloads),
[export formats](https://developers.google.com/workspace/drive/api/guides/ref-export-formats)
and [format discovery](https://developers.google.com/workspace/drive/api/reference/rest/v3/about).