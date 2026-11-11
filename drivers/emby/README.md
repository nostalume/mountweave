# Emby media source

Mount a folder from an authenticated Emby user's library through the Core
Stat, List, Open and Copy APIs.

```go
source, err := emby.Open(ctx, emby.Options{
    Endpoint: "https://media.example",
    UserID:   userID,
    RootID:   folderID,
    Client:   authenticatedClient,
})
if err != nil {
    if source != nil {
        err = errors.Join(err, source.Close())
    }
    return err
}
if _, err := storage.Install("/media", source); err != nil {
    return errors.Join(err, source.Close())
}
```

Provide the server URL and any reverse proxy prefix. The driver appends
`/emby` and confines authenticated requests to the configured server.
The borrowed client's transport supplies the same user's authentication
for browsing and downloading. The driver clears the cookie jar and
refuses redirects. Use HTTPS for a remote server carrying credentials.

Construction checks the selected directory ID. Enumeration advances
through native pages and verifies the display parent of every item.
The Core path uses exact display names. Equal display names make path
resolution ambiguous. File lengths stay unknown until a download response
describes the actual body.

Only filesystem media items are opened, through the original download
endpoint and under the server's download permissions. Linked or virtual
items have no readable body through this source. Actual response ranges
are preserved. This module exposes a read-only library view. Its single
integration fixture uses a closed HTTP service.

[Emby library browsing](https://dev.emby.media/doc/restapi/Browsing-the-Library.html) ·
[Original download endpoint](https://dev.emby.media/reference/RestAPI/LibraryService/getItemsByIdDownload.html)
