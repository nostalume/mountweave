# Baidu Netdisk source

This optional Go 1.24 module uses the official path API with the existing
Core tree API. It requires only the standard library and Core; it has no SDK,
directory index, separate download API or full-file staging.

~~~go
source, err := baidupan.Open(ctx, baidupan.Options{
    RootPath:    "/apps/My Application/Archive",
    AccessToken: currentAccountToken,
    Client:      httpClient,
})
if err != nil {
    if source != nil {
        _ = source.Close()
    }
    return err
}
mount, err := storage.Install("/cloud", source)
if err != nil {
    _ = source.Close()
    return err
}
_ = mount
~~~

`RootPath` is an explicit absolute directory. Construction verifies that directory;
it does not infer an application name, select an account root or create folders.
The host must obtain the user's authorization and the required native permissions.
Native directory restrictions and per-application quotas still apply.

`AccessToken` belongs to the host, including refresh and persistence. Each operation
takes one credential snapshot; an Open uses the same snapshot for its path query
and the returned download URL. Credential rotation must retain the same account.
The callback must be safe for concurrent calls.

`Client` is borrowed and shallow-copied. Its transport must not add account
authentication, cookies or Referer: the Driver puts the current token only in
the native query/form positions. Cookie jars and automatic redirects are disabled.
The module never closes the client, transport or credential provider.

## Read behavior and cost

Stat uses one path query, at any depth. The private request is a single-element
`POST /rest/2.0/xpan/file` with `method=filemetas` and `target=[exactPath]`.
The response must contain one successful `info` result. Its complete path, original
name, type, integer ID and size are admitted together; a conflicting spelling
or path is not silently corrected. Native IDs are not public path shortcuts.

List verifies its directory once, then requests ordinary native `method=list`
windows of 1000 with name ascending order and all item types. It advances by the
requested window, continues after short windows and ends at the first complete,
successful empty window. This is a live enumeration, not a snapshot. There is
no full-directory deduplication, background polling or cursor index. Core owns
public page limits, remainder and replay.

Open performs one path query with `dlink=1`, then one streaming GET without
redirects, or one GET per native redirect. It does not list siblings, repeat Stat,
fetch ID details, issue HEAD or reopen a failed stream.
Presence conditions use the same file lookup; a failed absence condition stops
before fetching the body.

Baidu can return HTTP 200 JSON for download failure as well as legitimate JSON
files. The module does not inspect file bytes to guess which it received.
For a nonempty file it requests a range, including `bytes=0-` for an ordinary
read, and accepts only identity-encoded single-range HTTP 206 whose total agrees
with the path metadata. An ordinary full-file 206 remains an ordinary Core Body.
A CDN that ignores Range and returns 200 is explicitly rejected, even when its
Content-Length matches. An empty file may use 200 with verified zero length;
an unknown HTTP length needs at most a one-byte EOF check.

When the caller requests a range, a native 416 returns RangeNotSatisfiable
without combining the response's total with earlier metadata. A 416 to the
driver's full-file request is an upstream response error. Ordinary access uses
live observations of the file and its ancestors.

The native download URL must use HTTPS. Only its account-token query item is
set to the current snapshot; all other query bytes remain unchanged.
After the first origin change, subsequent Location targets are used without
adding the account token, including a return to the original origin. The driver
rejects delegated targets that repeat the active token in an `access_token`
query value. HTTPS downgrades are refused and the chain is bounded to ten requests.
A native chain
that requires the client to forward the account token to a different origin is
therefore outside this source's supported credential boundary.

## Exclusive creation

The same instance implements Core's existing Mkdir and Create/default Write
operations. Mkdir issues one `method=create` request with `isdir=1` and
`rtype=0`, then validates the returned directory receipt.

The single-step file path takes one token snapshot, discovers an upload host
for this attempt, then makes one native multipart POST with `ondup=fail`.
It issues no preliminary Stat, retry, POST redirect, pre-upload task or commit.

Host discovery uses the task-free `locateupload` variant used by the official
browser client: anonymous GET returns `server[]` hostnames, which are used over
HTTPS. No host is hardcoded or cached. This differs from the task-specific
`uploadid` variant documented for upload tasks. The routing behavior and
request composition have confined-fixture coverage; OAuth upload has not been
verified with a real account.

Multipart framing is small; input streams directly through the shared input
checker without complete staging or a producer goroutine. Known lengths produce
an exact HTTP Content-Length; a nil length remains streamed with unknown length.
The caller keeps ownership of input. The single-step payload guard is 2 GiB. This
is the module's interpretation of the documented “2 GB”, whose exact byte unit
is not specified. A stricter native limit still applies. Native empty-file
upload is unsupported.

A complete, error-free native receipt must confirm the exact target and size.
Normal input EOF and matching byte counts establish Applied; a reliable receipt
with an input failure can establish Incomplete. Missing or contradictory receipts
remain Unknown. Explicit native name conflict is Unchanged/AlreadyExists without
draining input. Later cancellation or response cleanup errors retain any already
confirmed effect. Unknown results never trigger deletion by path.

### Large files

Set `MaxBufferedBytes` to at least one configured part to enable the large-file
path. Set `PartSize` in bytes to select the native limits for the account. Zero
defaults to 4 MiB. The host selects a size its account supports.

| Part size | File limit |
| --- | --- |
| 4 MiB | 4 GiB |
| 16 MiB | 10 GiB |
| 32 MiB | 20 GiB |

A known length above the 2 GiB single-step guard and within the configured
limit uses the large-file path with a versioned, immutable `core.Replayable`
input. The limits use binary units locally. The server enforces the actual
account limits. The driver shares its configured byte budget across writes.
Unknown-length input uses the single-step guard.

The driver reads the input once to compute ordered part digests. It
pre-creates a native upload task, discovers its upload host, sends the missing
parts, and commits the complete digest manifest. Reopened bytes must match the
local SHA-256 before sending. The server must acknowledge each part with its
expected MD5. After a transport failure, the driver can refresh the task's
HTTPS route and retry the same part once. With M missing parts, the normal
path uses M+3 requests and one leased part buffer plus the digest manifest.

After a valid pre-create task receipt, a part failure leaves the destination
unchanged. The temporary task is subject to server cleanup. A lost or
unverifiable pre-create or commit receipt is Unknown. The commit is sent once.
The task and input are scoped to the call.

## Resources and scope

`MaxResponseBytes` defaults to 4 MiB for control responses and error bodies,
independently of file size. The caller's contexts and client timeouts govern I/O.
Each Iterator Next has its own request context; Close cancels and joins current
I/O. Core owns admitted download bodies. The Driver closes rejected, intermediate
and control responses, redacts private cleanup errors and retains failed cleanup
for a later Close. Custom transports must keep credentials out of errors from
admitted download bodies.

Ordinary Stat, List, Open, Mkdir, exclusive Create and Core Copy composition are
implemented. Complete publication, overwrite, resumable uploads,
bound handles, NoFollow, revision and entity-tag reads, shared-space selection and
automatic download restart are not declared. No real-account or
all-CDN compatibility claim follows from the confined HTTP fixtures.

The development module uses the adjacent Core checkout; no new version is
published.

Official references: [authorization](https://pan.baidu.com/union/doc/使用入门/接入授权/授权介绍/),
[permissions](https://pan.baidu.com/union/doc/使用入门/权限与配额/),
[path metadata](https://pan.baidu.com/union/doc/基础网盘服务/获取文件信息/查询文件信息/),
[directory listing](https://pan.baidu.com/union/doc/基础网盘服务/获取文件信息/获取文件列表/),
[download](https://pan.baidu.com/union/doc/基础网盘服务/下载/),
[single-step upload](https://pan.baidu.com/union/doc/基础网盘服务/上传/单步上传/),
[directory creation](https://pan.baidu.com/union/doc/基础网盘服务/创建文件夹/),
[pre-create](https://pan.baidu.com/union/doc/基础网盘服务/上传/预上传/),
[upload routing](https://pan.baidu.com/union/doc/基础网盘服务/上传/获取上传域名/),
[part upload](https://pan.baidu.com/union/doc/基础网盘服务/上传/分片上传/),
[file commit](https://pan.baidu.com/union/doc/基础网盘服务/上传/创建文件/),
[upload FAQ](https://pan.baidu.com/union/doc/faq/上传/) and the
[official browser implementation](https://nd-static.bdstatic.com/open-static/open-official-website/js/devtool.08641d34.js).
