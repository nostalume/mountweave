# CloudFlare ImgBed source

Mount one directory from a CloudFlare ImgBed deployment with the standard
Core path API. The management client supplies a token with `list` permission.

```go
source, err := imgbed.Open(ctx, imgbed.Options{
    Endpoint: "https://images.example",
    RootPath: "/Album",
    APIClient: listClient,
})
if err != nil {
    if source != nil {
        err = errors.Join(err, source.Close())
    }
    return err
}
mount, err := storage.Install("/images", source)
if err != nil {
    return errors.Join(err, source.Close())
}
```

The root defaults to `/`. Non-root directories are inferred from native
indexed files and verified on construction. The Driver matches exact child
keys. Names with a comma and directory components containing `..` cannot
round-trip through the native routing rules and are rejected.

The management list returns all direct directories on each native file page.
Directories enter the Core listing once. The file offset uses the direct file
count, independent of the count of descendants. The Driver requires an indexed
response because the database fallback can report an incomplete result as a
successful list. Listings are live. `MaxResponseBytes`
defaults to 4 MiB per control response. The server may perform its own expired
file cleanup when the management list is called.

Open and Copy consume the ordinary `/file/{path}` response without image
transforms or admin preview. The endpoint's access policy applies. The
optional ContentClient is borrowed and must omit credentials. The Driver
does not use cookie jars or follow redirects, including external-link
redirects. File size and modification time are left unknown in Stat and
List. Open reports the actual HTTP length and range. The Driver implements
ordinary read-only access. Verification uses a sealed HTTPS fixture.

[ImgBed List API](https://cfbed.sanyue.de/en/api/list.html)

[ImgBed Read API](https://cfbed.sanyue.de/en/api/file.html)
