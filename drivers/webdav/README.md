# WebDAV source

This optional module connects a remote WebDAV collection to Mountweave. It is a
source Driver; `protocols/webdav` independently exports an existing Core over HTTP.
The implementation uses the standard library and the Core module only.

```go
storage, err := webdav.Open(ctx, webdav.Options{
    RootURL:  "https://storage.example/dav/project/",
    Username: "account",
    Password: applicationPassword,
})
if err != nil {
    if storage != nil {
        err = errors.Join(err, storage.Close())
    }
    return err
}
if _, err := files.Install("/remote", storage); err != nil {
    return errors.Join(err, storage.Close())
}
```

Use the existing Core `Stat`, `List`, `Open`, `Write`, `Mkdir` and `Copy` APIs.
Successful installation transfers ownership; otherwise the creator closes the
Driver. A failed constructor can return a non-nil Driver that still owns failed
response cleanup. `Close` retries that cleanup, never a deletion by resource name.

## Configuration and authentication

`RootURL` is the actual remote collection, not the Core mount prefix. It remains
a path-based namespace selection, not a pinned directory identity. The constructor
checks static settings and verifies the current collection with depth-zero
PROPFIND. Explicit percent escapes in the root URI are preserved on requests. `Options.Validate` is available for optional Control preflight; callers
do not need to call it before `Open`. Existing `control.TypedFactory` can construct
the instance without a new registry or configuration format.

Username/password use Basic authentication, including service application
passwords. Leave both empty when a prepared `Client` supplies authentication.
Clients and transports are borrowed; the host owns proxy, TLS, timeouts and any
credential provider. A supplied transport must not blindly replay mutations.
The Driver disables automatic redirects and only retries a PROPFIND once for an
exact same-origin collection URL with an added trailing slash. PUT is not replayed.
Cross-origin downloads and continuations are not followed with account credentials.

For 123 Cloud Drive, add an app under **Third-party Mounting** and select its
authorized folders and permissions. Use the HTTPS URL shown for the app as
`RootURL`. The [123 Cloud Drive setup guide](https://www.weibo.com/ttarticle/p/show?id=2309405214103219339373)
shows `https://webdav.123pan.cn/webdav`. Set `Username` to the account shown in the
app settings and `Password` to the generated application password. Mount the
authorized collection at a local path with the same `Core.Install` call.

For USTC Cloud Drive, use the credentials from its [WebDAV settings](https://pan.ustc.edu.cn/my/webDav)
as `Username` and `Password`. Set `RootURL` to the HTTPS collection, such as
`https://pan.ustc.edu.cn/seafdav/` for the library list or the documented
[Zotero collection](https://pan.ustc.edu.cn/doc/zotero/).

For Nanjing University Box, use `https://box.nju.edu.cn/seafdav/` or a library
or folder below it as `RootURL`. Use the WebDAV username and dedicated password
from the university's **WebDAV Access** settings. For a read-only mount, deny
mutations in the Core Authorizer. The endpoint advertises Basic authentication.
Authenticated browsing and downloads have yet to be verified on this service.
[Nanjing University Box WebDAV setup](https://doc.nju.edu.cn/books/17e62/page/webdav-api)

For PikPak Premium, create a WebDAV credential under **Settings > Access &
Integrations > WebDAV**. Use the HTTPS collection, username, and password shown
for that credential as `RootURL`, `Username`, and `Password`. The existing
read workflow supports standard collection responses and direct GETs. Deny
mutations with the Core Authorizer for a read-only mount. The vendor account
endpoint has not been exercised by the sealed protocol fixture.
[PikPak WebDAV setup](https://mypikpak.com/en-US/help-center/connected_apps/webdav/how_to)

For MEGA, use `webdav /path/to/folder` in an authenticated MEGAcmd shell.
Keep MEGAcmd Server running on the same host and set `RootURL` to the loopback
collection URL it prints, typically on port 4443. Leave `Username` and
`Password` empty. Deny mutations in the Core Authorizer for a read-only mount.
MEGAcmd decrypts file data and manages its streaming cache. Whole-file checksum
verification requires a separate MEGAcmd `get` transfer. This configuration has
not been tested with a live MEGA account.
[MEGAcmd WebDAV guide](https://github.com/meganz/MEGAcmd/blob/master/contrib/docs/WEBDAV.md)

For UGREEN UGOS Pro and fnOS, enable the NAS WebDAV service and set `RootURL`
to its HTTPS collection. UGOS Pro uses port 5006 by default. Use the HTTPS
WebDAV address shown by fnOS for its service. Set `Username` and `Password`
to the NAS account credentials, and grant that account access to the selected
share. Deny mutations in the Core Authorizer for a read-only mount. This setup
uses standard collection responses and direct GETs.
[UGOS Pro WebDAV](https://support.ugnas.com/detail/article/zh-CN/490) · [fnOS WebDAV](https://help.fnnas.com/articles/v1/file-service/webdav)

For TerraMaster TOS and ZSpace NAS, use the device's configured HTTPS WebDAV
service and a NAS account authorized for the selected share. For TOS, set
`RootURL` to `https://nas.example:<configured-port>/webdav/` or a collection
below it. For ZSpace, use the collection served by the enabled WebDAV service.
Set `Username` and `Password` to the device credentials. If the service uses a
private certificate, configure a trusted certificate in the host's `Client`.
For a read-only mount, deny mutations in the Core Authorizer. These setups
have not been exercised on vendor devices.
[TOS WebDAV setup](https://help.terra-master.com/cn/faq/TNAS/file-manager/shared_folder_access/#五webdav) · [ZSpace file services](https://www.zspace.cn/software/index.html)

For U-NAS 6, enable the WebDAV service and grant a NAS account access to the
selected share under its WebDAV permissions. Set `RootURL` to
`https://nas.example/webdav/` or an authorized collection below it, using the
configured HTTPS port. Set `Username` and `Password` to that NAS account's
credentials. For a read-only mount, deny mutations in the Core Authorizer.
Authenticated listing and downloads have yet to be verified on a U-NAS device.
[U-NAS WebDAV setup](https://www.u-nas.cn/doc/6.0/network.html#webdav)

For Uniview DX NAS, enable HTTPS under **Share Management > Service
Configuration > WebDAV**. Set `RootURL` to the authorized collection on the
configured NAS HTTPS address, for example `https://nas.example:8081/`. Grant
the NAS account read access to the share and deny mutations in the Core
Authorizer. Use the NAS credentials with Basic authentication if offered, or
provide a `Client` that authenticates to the NAS. Live listing and downloads
have yet to be verified.
[Uniview NAS manual, sections 5.1 and 5.2.4](https://cn.uniview.com/res/202308/01/20230801_1637790_NAS%E7%AE%A1%E7%90%86%E7%B3%BB%E7%BB%9F%E7%94%A8%E6%88%B7%E6%89%8B%E5%86%8C-V1.04_814700_194214_0.pdf)

For XanhyTech XOS 4.5.x, enable WebDAV under **Control Panel > Applications >
Web Server > WebDAV** and grant read access through the selected permission
mode. Use the NAS account for `Username` and `Password`. Set `RootURL` to the
shared-folder collection on the configured HTTPS port, for example
`https://nas.example:8443/Projects/`. Deny mutations in the Core Authorizer
for a read-only mount. Authenticated browsing and downloads have yet to be
verified on an XOS device.
[XOS 4.5.x user guide, WebDAV](https://www.xanhytech.com/static/upload/file/20241213/1734076761133009.pdf)

For YDisk Enterprise 7.3.3 and later, set `RootURL` to the server's HTTPS
`/api/dav/` collection or to a selected space or folder below it. Set `Username`
and `Password` to the YDisk account credentials. For a read-only source, grant
list and download access on the server and deny mutations in the Core Authorizer.
This setup uses standard collection responses and direct file bodies. It has not
been tested against a running YDisk deployment.
[YDisk WebDAV guide](https://www.ydisk.cn/docs/dev/webdav/) · [WebDAV release](https://www.ydisk.cn/blog/product/release/7.3.3/)

For FEB, create a dedicated account under **Console > Settings > WebDAV** and
limit it to the selected directory with read-only access. Set `RootURL` to
`https://webdav.febbox.com/` and use that account for `Username` and `Password`.
The endpoint advertises HTTP Basic authentication. This configuration requires
standard directory responses and direct file bodies. Authenticated browsing and
reading have not been verified against FEB.
[FEB WebDAV settings](https://www.febbox.com/faq) · [Directory permissions](https://www.febbox.com/article/detail/id/46)

For KodBox, enable its WebDAV plugin and copy the HTTPS collection URL shown in
the account's personal center as `RootURL`, for example
`https://storage.example/index.php/dav/my/`. Supply the KodBox account credentials
as `Username` and `Password`. Use a server-side read-only account and deny
mutations on the mount in the Core Authorizer. The upstream PUT can replace
existing files despite the exclusive-create request, and MKCOL can return
success for an existing path.
[KodBox setup guide](https://bbs.kodcloud.com/d/693-webdav-windows)
[KodBox WebDAV server](https://github.com/kalcaddle/kodbox/blob/main/plugins/webdav/php/webdavServer.class.php#L295-L339)
[KodBox PUT implementation](https://github.com/kalcaddle/kodbox/blob/main/plugins/webdav/php/webdavServerKod.class.php#L284-L327)

This read-only setup covers standard 207 PROPFIND, direct GET and use as a
Copy source. Use a collection that serves file bodies directly. KodBox can
redirect downloads from object storage, and the Driver refuses those redirects.
KodBox's special HTTP 200 PROPFIND response for `.xbel` files is outside this
setup.
[KodBox download implementation](https://github.com/kalcaddle/kodbox/blob/main/plugins/webdav/php/webdavServerKod.class.php#L249-L273)
[KodBox PROPFIND implementation](https://github.com/kalcaddle/kodbox/blob/main/plugins/webdav/php/webdavServer.class.php#L188-L235)

`MaxResponseBytes` bounds each PROPFIND response, including the optional
trailing `DAV:responsedescription` and XML trailing data. Zero selects 4 MiB.
This setting is independent of public page size and file size. Exceeding it is an error, never successful enumeration EOF.

## Listing, bodies and effects

Listings stream XML through the existing Iterator. The response href determines
identity, not display name or response order. Only the selected collection and
its direct children are admitted; dot segments, encoded separators and references
outside the root are refused. Literal percent characters are encoded once.
Missing or denied optional properties stay unavailable. A missing
`DAV:resourcetype` prevents type inference without implying resource absence.
Only valid entity tags from `DAV:getetag` are retained as validator evidence.
[SeafDAV can return a raw object ID](https://github.com/haiwen/seafdav/blob/master/wsgidav/dav_provider.py#L748)
in that property. Ordinary access remains available without that validator. A valid HTTP
`ETag` on the GET response remains separate evidence.
`Link: <...>; rel="next"` advances the query on the selected collection URI.
The relation is case-insensitive. Links with an explicit anchor are ignored.
The [official Nutstore client](https://github.com/nutstore/obsidian-nutstore-sync/blob/7ccfe320d620a5d6c8453ca0bd305cdf348f4819/src/api/webdav.ts) uses this form of pagination.
Empty pages may continue. The Iterator retains one response and next URL, not a
directory cache or full page history. Listings are not snapshots.

Each Next context applies only to that call. Close cancels an active request and
waits for its response owner; a failed Close keeps ownership for a later retry.
Public cursor expiry, replay and retirement remain owned by Core.

Ordinary Open observes the resource type with PROPFIND, then GETs its body. This
does not pin the object between requests. Actual GET length/range replace earlier
size observations. HTTP 200 range fallback remains an entire body; 206 and 416
use the existing HTTP body interpreter. Core adopts the returned response Body,
including on failure, and owns its cancellation and close.

Write defaults to native exclusive PUT (`If-None-Match: *`). Known and unknown
length inputs stream through the same synchronous borrowing mechanism used by S3;
declared length includes normal EOF. No full-file buffering, implicit parent
creation or post-failure name deletion occurs. Applied requires a creation receipt
and complete input. A lost response or ambiguous server failure remains Unknown;
a confirmed creation with invalid input can report Incomplete. MKCOL creates one
directory and reports its native receipt. Errors retain native HTTP status.

## Capability boundary

A directory `Core.RemoveTree` with explicit `Scope: core.Subtree` sends one
recursive DELETE. The mount root is protected. HTTP 200 or 204 confirms
completion. HTTP 202, 207 or a lost response leaves the effect unknown. A 207
reports failed members and can omit successful deletions. The driver returns
the native HTTP status without inferring per-member effects.
[RFC 4918](https://www.rfc-editor.org/rfc/rfc4918.html#section-9.6.1)

The Driver provides subtree deletion alongside `Driver`, `FileCreator` and
`DirectoryMaker`. Single-entry type-limited deletion, atomic relocation, complete
publication, bound handles, LSTAT, NoFollow and resumable uploads are unsupported.
Collection MOVE can partially succeed, and complete publication still needs a
native staged commit.
[Nextcloud chunked upload](https://docs.nextcloud.com/server/stable/developer_manual/client_apis/WebDAV/chunking.html)

Tests use the existing WebDAV export and Local Driver plus closed HTTP fixtures.
They verify protocol behavior and ownership; they do not certify a live vendor
account or every service version. Local module replacement is for checkout use;
this module has not been published as a versioned release.
