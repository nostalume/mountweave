# GitHub repository source

This optional Go module mounts the committed files under a GitHub repository
ref through the existing Core Stat, List, Open and Copy APIs.

```go
source, err := githubrepo.Open(ctx, githubrepo.Options{
    Owner:      "organization",
    Repository: "project",
    Ref:        "heads/main",
    RootPath:   "/docs",
    Client:     authenticatedClient,
})
if err != nil {
    if source != nil {
        err = errors.Join(err, source.Close())
    }
    return err
}
if _, err := storage.Install("/docs", source); err != nil {
    return errors.Join(err, source.Close())
}
```

`Ref` is required. Use a commit SHA, a branch such as `heads/main`, or a tag
such as `tags/v1.0`. The Driver resolves the ref once to a Git tree. Branch
changes are visible after creating and installing a new instance. Empty
`RootPath` selects the entire tree. A supplied Client provides authentication
and is borrowed. A nil Client can read public repositories. Private access
requires repository Contents read permission. The Driver uses only
`https://api.github.com` and disables redirects and its copy's cookie jar.

Each path component reads one nonrecursive native tree and selects an exact
child name. A directory list reads its tree once and Core paginates the result.
The Driver does not keep a repository index. It rejects truncated or malformed
trees. `MaxResponseBytes` bounds a single control response and defaults to
4 MiB. File bodies are streamed separately and GitHub limits this blob API to
100 MB.

Regular and executable Git blobs are visible as files. Git symbolic links and
submodules are omitted from listings and direct access returns unsupported.
Git LFS pointers are returned as the literal Git blob bytes. LFS object
retrieval, writes and bound file handles are outside this module's contract.
Range results reflect the body response, including full-body fallback. The
committed tree supplies the object size. If the response omits its length, the
driver leaves the stream length unknown and checks its size through EOF. Core
owns successful raw streams and their cancellation. The Driver owns metadata
and HTTP error responses and retries failed closes.

[Get a commit](https://docs.github.com/en/rest/commits/commits#get-a-commit) ·
[Get a tree](https://docs.github.com/en/rest/git/trees#get-a-tree) ·
[Get a blob](https://docs.github.com/en/rest/git/blobs#get-a-blob)
