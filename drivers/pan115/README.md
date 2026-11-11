# 115 Open source

This optional Go 1.24 module connects ordinary, unencrypted 115 Open account
files to Mountweave. It uses the standard library and existing Core APIs.

```go
source, err := pan115.Open(ctx, pan115.Options{
    RootPath: "/Archive",
    Client: authenticatedClient,
})
if err != nil {
    if source != nil { _ = source.Close() }
    return err
}
mount, err := storage.Install("/cloud", source)
if err != nil { _ = source.Close(); return err }
_ = mount
```

Choose either an account-root-relative `RootPath` or a native `RootID`. If neither
is specified, the account root is ID `0`. Construction validates settings, resolves
the selected root and confirms a readable directory. A non-nil failed result
retains cleanup ownership and must be closed. Successful `Core.Install` transfers
ownership. Paths resolved during construction bind the resulting native root ID;
reconstructing the path later may choose a different directory.

## File access and cost

- `Stat`, `List`, ordinary `Open` and Core `Copy` as a source use the existing API.
  Each path component is selected by exact name from the native parent listing.
  The shared `childscan.Unique` retains one candidate until the native scan ends;
  different IDs with one name are ambiguous, and repeated matching IDs are invalid.
- Native `cid/offset/count/limit` remain private. Requests explicitly include
  directories and the current directory scope, with native name ordering.
  Offset advances by actual item count; short pages continue and a nonterminal
  empty page fails rather than skipping entries. Native paging is not a snapshot.
- Listing fields are admitted once and retained with their native ID and
  pick code. Metadata is projected directly; `Open` does not repeat `Stat`, query
  leaf details or issue `HEAD` after locating the file.
- `Open` uses the selected pick code to obtain a private URL, checks the reply's
  file-ID key and then reads the body through `httpcontent`. A changed reply name
  or pick code does not change the selected ID. Neither names nor old sizes are
  version evidence. When the download receipt reports a size, it must agree with
  the HTTP total or with the bytes through EOF on an unframed response. Actual
  HTTP framing determines length and range, and a full response remains a
  full-response fallback.
- A path of depth D costs the sum of the scanned native pages over those D
  parents, followed by one capability request and one content GET for opening.
  Root ID construction uses details plus one readable-directory observation;
  account-root construction needs one list request. A root `Stat` refreshes the
  native observation. Nothing caches whole directories or stages file bodies.

`MaxResolvePages` is an optional per-component page budget, matching the existing
Alipan policy; zero adds no page limit. `MaxResponseBytes` defaults to 4 MiB per
metadata/error response and is configurable independently of file size. Memory is
bounded by the current native page, one matching item and Core's public replay
state. Public cursor replay does not fetch another native page.

Encrypted directory entries may be listed as directories, but entering their
contents is refused. An explicit encryption marker is never ignored. A nonempty
native directory page proves contents were returned; an empty non-root page needs
a readable-directory observation from its native path or the same traversal's
parent item. If that evidence is absent, the response is insufficient, not proof
of an empty directory. Constructor observations do not become a permanent
permission or encryption cache. Hidden/invalid parent state and malformed page
bounds are reported without pretending that enumeration ended normally.

## Credentials and resource ownership

`Client` is required and borrowed. Its transport authenticates only requests to
`https://proapi.115.com`; the host owns authorization, token rotation and atomic
credential persistence. The official refresh protocol rotates access and refresh
tokens together and requires one refresh owner per authorization. This Driver
does not introduce another token store, login flow or automatic retry loop.

`ContentClient` is independently borrowed and defaults to a fresh client, not a
copy of the authenticated transport. The constructor rejects using the control
client as the content client. Keep credential injection out of the content
transport, including transport shared by distinct client instances. Both client
copies disable cookie jars. Control redirects are refused; private
content redirects use a bounded HTTPS GET chain, retaining only the original
Range and identity encoding. Account authentication, cookies and Referer are not
forwarded. Each intermediate response is closed before the next request; failed
cleanup stops the chain and remains Driver-owned for `Close` retry.

Core owns every returned body. Rejected download responses are closed by the
Driver. Each iterator owns cancellation
until `Close`; a canceled `Next` interrupts and joins its own I/O. Core retains its
normal authorization, cancellation and mount-retirement boundaries. Short-response
close failures stay with the Driver; unexpected connection or close EOF is not
reported as successful enumeration EOF. Borrowed clients and transports remain
host-owned. Metadata failures retain the native code, errno and reason.
Download grant failures keep the HTTP status and numeric codes. The Driver
redacts grant messages, private I/O errors and rejected response cleanup errors.
Core receives the original successful download body. Hosts must keep secrets
out of errors from custom content body implementations.

## Current scope and evidence

The Driver does not declare creation, directory mutation, complete publication,
Move, Lstat, NoFollow, bound access, sharing, encrypted-directory access, immutable
versions or durable continuation. Ordinary reads do not claim these guarantees.
Three closed integration/synthesis families use synthetic authorization, two
local HTTP origins and Local as a Copy target. They cover root choices, exact
Unicode/percent names, cross-page ambiguity, replay, native range/full fallback,
credentials, receipts, malformed/hidden pages, budgets, cleanup, cancellation and
retirement. They do not certify live accounts or every service deployment.
Development currently replaces Core with the adjacent checkout; no new version
or external-host acceptance is claimed.

Official references: [listing](https://open.115.com/doc/115开放平台/API列表/文件管理/获取文件列表.md?raw=true),
[ID details](https://open.115.com/doc/115开放平台/API列表/文件管理/获取文件(夹)详情/按ID获取.md?raw=true),
[path details](https://open.115.com/doc/115开放平台/API列表/文件管理/获取文件(夹)详情/按路径获取.md?raw=true),
[download](https://open.115.com/doc/115开放平台/API列表/文件管理/获取文件下载地址.md?raw=true),
[token rotation](https://open.115.com/doc/115开放平台/接入指南/接入授权/刷新access_token.md?raw=true).
The native path endpoint is not substituted for ID-rooted exact traversal without
proof of equivalent root selection and ambiguity behavior.
