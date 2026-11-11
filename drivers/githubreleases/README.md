# GitHub release assets source

This optional Go module mounts the uploaded assets of one published GitHub
release. It uses the existing Core Stat, List, Open and Copy APIs.

```go
source, err := githubreleases.Open(ctx, githubreleases.Options{
    Owner:      "organization",
    Repository: "project",
    Tag:        "v1.0.0",
    Client:     authenticatedClient,
})
if err != nil {
    if source != nil { err = errors.Join(err, source.Close()) }
    return err
}
if _, err := storage.Install("/release", source); err != nil {
    return errors.Join(err, source.Close())
}
```

The tag selects the release at construction. The mount keeps that release ID
even if the tag later changes. The mounted directory lists its uploaded
assets using their exact GitHub names. Assets are live observations. The
driver supports published prereleases selected by tag. Repository-wide release
listing, generated source archives and Git trees have distinct contracts.

A nil `Client` reads public releases. For private repositories, supply a
client with a repository Contents read grant. The borrowed client contacts
only `https://api.github.com`. Asset pages advance from GitHub's next-page
signal while retaining the original repository and release route. Core
handles public cursors. Exact asset lookup scans those pages and keeps one
candidate. `MaxResponseBytes` bounds each control reply and defaults to
4 MiB. No file body is staged.

The selected asset ID requests binary content from the API. Direct content
stays on that origin. On a redirect the driver switches permanently to an
independent credential-free HTTPS client, preserving the requested range.
`ContentClient` is optional and defaults to an unauthenticated client.
Its transport must omit account credentials. Cookie jars are disabled.
Ranges and lengths come from the actual HTTP response, including full-body
fallback. Core owns successful streams. The driver closes failed responses
and redacts transport and cleanup errors. Hosts keep secrets out of errors
from custom successful body implementations.

The closed integration fixture covers exact binding, pagination, HTTP
content and redirects, Copy, cancellation and retirement. It requires
neither a GitHub account nor a live release.

[Get a release by tag](https://docs.github.com/en/rest/releases/releases#get-a-release-by-tag-name) ·
[List release assets](https://docs.github.com/en/rest/releases/assets#list-release-assets) ·
[Download a release asset](https://docs.github.com/en/rest/releases/assets#get-a-release-asset) ·
[Pagination](https://docs.github.com/en/rest/using-the-rest-api/using-pagination-in-the-rest-api)
