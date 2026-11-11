# Tencent Docs source

`tencentdocs.Open(ctx, Options{FolderID: id, Client: client})` confirms a known
Tencent Docs OpenAPI folder ID. Install it with the usual Core `Install`.
The host handles [application registration and user OAuth authorization](https://docs.qq.com/open/document/app/oauth2/).
Its control client supplies the matching `Access-Token`, `Client-Id` and
`Open-Id` headers to `docs.qq.com`. Directory browsing requires
`scope.drive.readonly`. The Driver borrows both clients.

The Driver uses [native directory pages](https://docs.qq.com/open/document/app/openapi/v2/file/folders/list.html)
and the service's [next offset](https://docs.qq.com/open/document/app/openapi/v2/file/common/list_info.html).
Path resolution matches the exact native title and reports duplicate titles.
Shortcuts and unexportable entries remain visible without dereferencing their
targets. Document byte sizes remain unknown in listing and metadata.

Set `AllowExport: true` and grant `scope.drive.exportable` to read the default
same-type export of an online document, spreadsheet, presentation or PDF using
Core `Open` or `Copy`. This generates a new representation. The service
allows [nine exports per user per day](https://docs.qq.com/open/document/app/openapi/v2/file/export/async_export.html).
An Open submits one export, polls the same file and operation within the
published 20-minute operation ID lifetime and the caller's deadline, and
streams the completed response. A range request receives the
full export. Uploads, raw original attachments and other export types are
outside this adapter. Browsing does not consume export quota.

An independent `ContentClient` may handle the HTTPS download URL; its transport
must never add account credentials. The control transport must not retry
export submissions. The default control response cap is 4 MiB, configurable
with `MaxResponseBytes`.
