# 360 Fangcloud source

This optional Go module mounts the personal space of a 360 Fangcloud
(360 亿方云) enterprise account through its official user API.

```go
source, err := fangcloud.Open(ctx, fangcloud.Options{
    RootPath: "/Archive",
    Client:   authenticatedUserClient,
})
if err != nil {
    if source != nil { _ = source.Close() }
    return err
}
_, err = storage.Install("/cloud", source)
if err != nil { _ = source.Close() }
return err
```

The zero root selects the user's personal items. Set `RootPath` to resolve
an exact child path or `RootID` to select a positive native folder ID.
The constructor confirms an accessible directory and binds its current ID.
An explicit ID is admitted through folder details in personal space. The
Driver accepts a host-owned control client for `https://open.fangcloud.com`
authenticated with a user-level token from the enterprise integration.
Token acquisition and rotation belong to the host.

`Stat`, `List`, ordinary `Open` and `Copy` as a source use the existing
Core APIs. The native personal-root and folder-children endpoints return
files and folders in a single page. Both arrays are consumed before advancing
the native page number. The total page count determines completion. An exact
path scans native pages per component until its child is unique. Optional
`MaxResolvePages` bounds each scan, with zero adding no limit. Public paging
and cursor replay remain Core-owned. Metadata and control replies have a
`MaxResponseBytes` budget that defaults to 4 MiB. Listings are live observations.

The selected file ID authorizes one `download_v2` request for its current
original. The returned HTTPS capability is used immediately by an independent
`ContentClient`, which defaults to a fresh unauthenticated client and must
omit account credentials. The Driver disables cookie jars, refuses control
redirects and follows bounded HTTPS content redirects with the original
range. It redacts private HTTP exchange and cleanup errors, and owns cleanup
for rejected responses. Core owns an accepted body. Custom content transports
must keep secrets out of errors from an accepted body. Returned range and length are
derived from the actual HTTP response, with a complete-response fallback.

Only the ordinary personal-space read workflow is supported. Department,
collaboration and share roots, mutations, fixed versions and bound access
require their own native contracts. The closed fixture exercises roots,
pagination and replay, exact path admission, a ranged download, Core Copy,
cancellation and retirement. It uses synthetic responses and credentials.

Official references: [current API and user authentication](https://open.fangcloud.com/wiki/v3/),
[pagination and direct download fields](https://open.fangcloud.com/wiki/v2/).
