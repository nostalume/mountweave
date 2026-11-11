# AtomGit source

Mount a fixed commit from the official AtomGit v5 API with
`atomgit.Open(ctx, atomgit.Options{Owner: owner, Repository: repo, Ref: ref})`.
The driver resolves the branch or commit once. Set `RootPath` to mount an
exact subdirectory. Public repositories can use the default client. For
private repositories, supply a client with the required AtomGit authorization
for `api.atomgit.com`. The caller owns that client and its credentials. The
driver does not follow redirects.

Install the returned driver with Core `Install`. Core `Stat`, `List`,
`Open` and the source side of `Copy` use the same fixed commit. Native
numbered directory pages continue until an explicit empty tree. Directory
metadata does not report file sizes. Symbolic links and submodules are not
traversed.

The raw endpoint streams the bytes stored in Git at the selected path and
commit. Git LFS pointer files are exposed as stored in Git. HTTP byte ranges
are returned only when the server supplies a matching partial response. The
control response cap defaults to 4 MiB and can be changed with
`MaxResponseBytes`.

[Authentication](https://atomgit-api-doc.atomgit.com/guide/2.auth.html) ·
[Commit](https://atomgit-api-doc.atomgit.com/6.commit/131.a_commit_in_a_repository.html) ·
[Tree](https://atomgit-api-doc.atomgit.com/1.repositories/2.get_repository_directory_tree.html) ·
[Raw file](https://atomgit-api-doc.atomgit.com/1.repositories/35.get_raw_file.html)
