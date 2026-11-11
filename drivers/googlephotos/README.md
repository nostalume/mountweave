# Google Photos album source

Mount an album created by your application in Google Photos. Supply a
prepared OAuth client with the Library API scope
`https://www.googleapis.com/auth/photoslibrary.readonly.appcreateddata`.

```go
source, err := googlephotos.Open(ctx, googlephotos.Options{
    AlbumID: albumID,
    Client:  authenticatedClient,
})
if err != nil {
    if source != nil {
        err = errors.Join(err, source.Close())
    }
    return err
}
if _, err := storage.Install("/album", source); err != nil {
    return errors.Join(err, source.Close())
}
```

The driver confirms the album and sends its ID with every media search.
It advances the native page token, including empty continuation pages.
Core owns public pagination and cursor replay. File names match exactly,
and duplicate names are ambiguous. `MaxResponseBytes` defaults to
4 MiB for each metadata response.

Open obtains a fresh download address from the selected item. Photos use
the `=d` download representation, which omits location EXIF. Ready videos
use the `=dv` high-quality transcoded representation. The actual response
supplies the length and range. Core Copy streams that same representation.

The authenticated client contacts the Library API origin. An independent
credential-free `ContentClient` handles downloads and HTTPS redirects.
Custom transports must keep secrets out of errors from an accepted body.

This is a read-only view of one app-created album. The fixture uses closed
HTTP exchanges, with synthetic credentials and no live Google account.

[App-created album and media scope](https://developers.google.com/photos/library/guides/get-started-library) ·
[Media search](https://developers.google.com/photos/library/reference/rest/v1/mediaItems/search) ·
[Download representations](https://developers.google.com/photos/library/guides/access-media-items)
