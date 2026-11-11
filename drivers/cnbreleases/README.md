# CNB release assets source

This optional Go module mounts the assets of one published CNB tag with
the existing Core Stat, List, Open and Copy APIs.

```go
source, err := cnbreleases.Open(ctx, cnbreleases.Options{
    Repository: "organization/team/project",
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

`Repository` is the CNB slug without a host or `.git` suffix. Supply a
borrowed control client with a `repo-release:r` grant to `https://api.cnb.cool`.
Each native lookup verifies the configured tag and published state. CNB
downloads by tag and asset name, so the mount tracks the live tag. Moving
or replacing the tag changes subsequent observations and does not establish
a fixed release identity, asset version or snapshot.

The release detail contains one complete asset array. The driver decodes
it once per operation, with a `MaxResponseBytes` budget of 4 MiB by default.
An exact lookup uses the returned names and IDs, and detects ambiguity.
Core list pages and their replays reuse the same fetched array without
another native request. Asset names occupy one directory level.

The driver requests the documented tag and filename download route and
requires its temporary HTTPS redirect. It does not request a share link.
Every redirected request uses the independent credential-free
`ContentClient`, which defaults to an unauthenticated client. Its
transport must omit account credentials. Cookie jars and automatic
client redirects are disabled. Actual response ranges and lengths are
preserved, including full-body fallback. The driver redacts transport
and failed response cleanup errors. Core owns successful streams.
Hosts keep secrets out of errors from custom successful bodies.

The module exposes uploaded release assets. It leaves repository-wide
release lists, generated source archives and mutations to separate
contracts. The closed fixture uses synthetic responses and needs no
CNB account or live release.

[CNB OpenAPI](https://docs.cnb.cool/zh/develops/openapi.html) ·
[Official Swagger](https://api.cnb.cool/swagger.json) ·
[Access tokens](https://docs.cnb.cool/zh/guide/access-token.html)
