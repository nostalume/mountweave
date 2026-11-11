# Feishu Drive source

This optional Go module mounts a folder already accessible to a Feishu app or
user through the Drive v1 file APIs. It uses the standard library, Core and its
existing private helpers.

```go
source, err := feishudrive.Open(ctx, feishudrive.Options{
    RootToken:     folderToken,
    Client:        authenticatedClient,
    ExportFormats: map[string]string{"docx": "pdf"}, // Optional.
})
if err != nil {
    if source != nil {
        err = errors.Join(err, source.Close())
    }
    return err
}
if _, err := storage.Install("/drive", source); err != nil {
    return errors.Join(err, source.Close())
}
```

`RootToken` is the native folder token. Obtain it from the Feishu folder URL or
the official API using the selected account. The constructor checks that the
folder can be listed. A tenant token uses the app's permissions and a user token
uses the user's permissions. The host owns token acquisition and refresh and
provides a client that authenticates only `https://open.feishu.cn`. The Driver
borrows the transport and disables cookies and redirects on its copy.

The explicit folder token is used directly with the Drive v1 list endpoint.
Explorer v2 folder discovery and Drive v2 permissions and interactions are
separate API families. This module uses the mainland Feishu origin. The
international Lark service has its own `https://open.larksuite.com` origin.

## Read behavior

Use Core `Stat`, `List`, `Open` and `Copy`. The public tree contains folders,
uploaded files and any explicitly enabled document types. Hidden documents
and shortcuts still participate in exact-name ambiguity checks. A direct match
for a hidden item returns an unsupported-operation error.

Each path component scans the selected parent's native pages, retaining one
exact-name candidate. Page entries must refer to that parent. `has_more` and
`next_page_token` govern native pagination. Public replay stays in Core. Pages
are observations, not a snapshot. `MaxResolvePages` can bound each path component
and zero leaves it unrestricted. `MaxResponseBytes` bounds each JSON or error
response, with a 4 MiB default. It does not limit file size.

An ordinary open adds one authenticated GET to the same fixed API origin after
locating the file. It streams the original body, including a JSON file, and uses
actual HTTP length and range information. File listing metadata has no body
size. Core owns the returned body and handles cancellation and mount retirement.
The Driver closes metadata and failed download responses and retains failed
closes for retry. Writes, immutable versions and bound handles are outside this
source's contract.

## Optional document export

`ExportFormats` maps a native document type to one output format. A nil or empty
map exposes only folders and uploaded files. The driver copies and validates the
map before network I/O. Supported whole-document formats are:

| Source | Formats |
| --- | --- |
| `doc` | `docx`, `pdf` |
| `docx` | `docx`, `pdf`, `markdown` |
| `sheet` | `xlsx` |
| `bitable` | `xlsx`, `base` |
| `slides` | `pptx`, `pdf` |

CSV requires a separate native sheet or table selection. Configured documents
retain their original paths and their size is unknown until Open. Neither
browsing nor Stat creates an export.

An Open creates one [Drive v1 export task](https://open.feishu.cn/document/server-docs/docs/drive-v1/export_task/export-user-guide),
queries it with the selected document token, and streams its completed result.
The authenticated client must keep the same user or app across the workflow,
have export access, and avoid replaying the task-creation POST. The driver
checks pending tasks every five seconds, with a local 50-second control deadline
and normal caller cancellation. Canceling stops local work. An existing task
may still finish at Feishu. The result download uses the original call context
and returns the full converted file, including when a range was requested.
An export receipt size remains entry metadata when HTTP has no declared
length. The stream verifies that size through EOF.

## Optional uploaded-file removal

Set `AllowRemove: true` to enable unconditional Core `Remove` for uploaded
files. The driver resolves the exact path, then sends one synchronous native
delete for the selected file token. Feishu moves the file to its recycle bin.
The authenticated client must have delete access and avoid replaying DELETE.
Coordinate deletes through the host when multiple callers share this account,
as the service does not support concurrent calls to its delete endpoint.

A success receipt without a task ID confirms the removal. If Feishu reports the
selected token was already deleted, Core returns `NotFound` with an unchanged
effect. An unexpected task or missing confirmation leaves the effect unknown. Explicit target constraints,
conditions, folders, exported documents and shortcuts are unsupported.

The closed Core integration workflow covers original reads, explicit export
and removal, including task failures, cancellation, and response cleanup. No
live account was used.

Native references: [folder listing](https://open.feishu.cn/document/server-docs/docs/drive-v1/folder/list),
[original file download](https://open.feishu.cn/document/server-docs/docs/drive-v1/download/download),
[file deletion](https://open.feishu.cn/document/server-docs/docs/drive-v1/file/delete),
and [official SDK contract](https://github.com/larksuite/oapi-sdk-go/tree/master/service/drive/v1).
