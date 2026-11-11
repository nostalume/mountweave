# Huawei Cloud CodeArts Repo source

`codeartsrepo.Open(ctx, Options{Endpoint: origin, RepositoryID: id,
CommitSHA: sha, Client: client})` confirms one immutable commit. Use the
[regional API origin](https://github.com/huaweicloud/huaweicloud-sdk-go-v3/blob/master/services/codeartsrepo/v4/region/region.go),
such as `https://codehub-ext.cn-north-4.myhuaweicloud.com`, and the numeric
repository ID shown in CodeArts Repo. Set the optional `RootPath` to an
exact subdirectory path to mount only that subtree. Supply a prepared client carrying a
valid IAM token or signing its requests. The host owns the client and its
credentials. The driver does not follow redirects.

Install the returned driver using Core `Install`. Browsing uses the
[nonrecursive tree endpoint](https://support.huaweicloud.com/api-codeartsrepo/ListTrees.html)
with the confirmed commit on every request, an exact parent path and
native offset pagination. Special Git entries are not traversed.
`Stat` and listings leave file sizes unknown because the tree does not
report them.

`Open` and the source side of `Copy` read the selected object's
[raw blob response](https://support.huaweicloud.com/api-codeartsrepo/DownloadBlobsRaw.html)
with the original repository-relative path. The service uses separate
`codeartsrepo:repository:getRepository` and
`codeartsrepo:repository:downloadCode` permissions. The raw body is streamed
without decoding or caching. Range requests return the full body.
The per-response control metadata cap defaults to 4 MiB and can be set via
`MaxResponseBytes`.

[Commit lookup](https://support.huaweicloud.com/api-codeartsrepo/ShowCommit.html)
