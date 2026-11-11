# KDocs personal source

Mount My Documents or an exact folder through the KDocs Open Platform. Use an
approved application and a valid user grant with `access_personal_files` and
`download_personal_files`. The host manages grant acquisition and refresh.

```go
source, err := kdocs.Open(ctx, kdocs.Options{
    RootPath:    "/Documents",
    AccessToken: tokenForAccount,
})
```

`RootPath` defaults to `/`. A selected subfolder is bound to its application-scoped
ID. The driver resolves exact child names and follows the native offset/filter
pair through the folder and file phases. Listings reflect the live directory.
Install the source into Core for `Stat`, `List`, `Open`, and source-side `Copy`.
If construction returns a non-nil source and an error, call `Close` to retry
response cleanup. Close the source if installation fails.

`AccessToken` supplies a current token for the same user and application on
each control request. The token goes in the query to `developer.kdocs.cn` only.
An optional borrowed `Client` handles control requests. An independent, borrowed
`ContentClient` can fetch the delegated HTTPS URL. Its transport must be free of
account credentials. Both reject redirects and avoid their client's cookie jar.
`MaxResponseBytes` bounds each control reply and defaults to 4 MiB.

Downloads stream the original bytes supplied by the temporary URL. A known HTTP
total must agree with the download receipt. If HTTP omits its length, the
stream verifies the receipt size through EOF. The actual HTTP range is reported.
The driver exposes provider refusal to download an online document as unsupported.
Received shares, app space, export, and mutations are outside this source.

The sealed integration fixture uses synthetic grants and an independent TLS
body endpoint. Live grants and new application enrollment were untested.

[Personal listing](https://developer.kdocs.cn/server/personal/files.html) ·
[Download](https://developer.kdocs.cn/server/personal/download.html) ·
[User authorization](https://developer.kdocs.cn/common/authorization/web.html)
