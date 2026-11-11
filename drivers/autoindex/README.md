# Nginx autoindex source

Mount an explicit Nginx JSON directory as a read-only source. The configured
location must serve autoindex JSON for directory requests. Index files can
take precedence over autoindex in Nginx.

```nginx
location /files/ {
    autoindex on;
    autoindex_format json;
}
```

```go
source, err := autoindex.Open(ctx, autoindex.Options{
    RootURL: "https://storage.example/files/",
    Client:  authenticatedClient, // optional, borrowed
})
if err != nil {
    if source != nil {
        err = errors.Join(err, source.Close())
    }
    return err
}
mount, err := storage.Install("/files", source)
if err != nil {
    return errors.Join(err, source.Close())
}
```

HTTPS requests may use an authenticated client supplied by the host. HTTP
roots use the default unauthenticated client. Redirects are refused, the
client's cookie jar is unused, and requests stay under the explicit root
URL. The root's path escaping is preserved and its trailing slash can be
omitted. The URL bounds HTTP routing. Server-side aliases and symbolic links
remain subject to the upstream configuration.

Directory traversal matches the exact returned name and encodes each child
as one URL segment. Autoindex returns one JSON array per directory.
`MaxResponseBytes` defaults to 4 MiB and bounds each identity-encoded
metadata response. Core can page and replay that in-memory listing without
fetching it again. Nginx hides dotfiles. This driver exposes files and
directories, and rejects direct access to entries of type `other`.

Core supports Stat, List, streaming Open, and Copy from this source. Ranges
reflect the actual HTTP response. Bound access, NoFollow, and changes are
unsupported. Verification uses a sealed HTTPS fixture.

[Nginx autoindex documentation](https://nginx.org/en/docs/http/ngx_http_autoindex_module.html)
