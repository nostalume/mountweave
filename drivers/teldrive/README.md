# Teldrive source

Mount an active directory from a Teldrive 2.0.12 deployment through the
standard Core path API.

```go
source, err := teldrive.Open(ctx, teldrive.Options{
    Endpoint: "https://teldrive.example",
    RootPath: "/Documents",
    Client:   authenticatedClient,
})
if err != nil {
    if source != nil {
        err = errors.Join(err, source.Close())
    }
    return err
}
mount, err := storage.Install("/telegram", source)
if err != nil {
    return errors.Join(err, source.Close())
}
```

Use either a root folder UUID or an exact absolute path. Omitting both
selects the authenticated drive root. A path resolves to a folder UUID
at construction and stays bound to that folder. The borrowed client's
transport supplies `X-Api-Key` or bearer credentials for one Teldrive
account. Both metadata and original content requests use the configured
HTTPS origin. The driver disables automatic redirects and cookie jars.

File lists use native folder scope, active status and opaque cursors. Each
path component is matched by exact name under its verified parent UUID.
Listings are live. `MaxResponseBytes` defaults to 4 MiB per metadata
response. Depth and native page count determine the lookup request count.
Stat uses the admitted listing metadata without a separate HEAD.

Open requests original content by the resolved file UUID. Copy uses the
same stream. Teldrive assembles the Telegram parts on the server, and Core
reports the range and length observed on the HTTP response. The driver
exposes ordinary read-only operations. A sealed HTTPS fixture verifies the
adapter without contacting Telegram or a live Teldrive instance.

[Teldrive 2.0.12 OpenAPI](https://github.com/tgdrive/teldrive/blob/2.0.12/openapi/teldrive.openapi.yaml)
