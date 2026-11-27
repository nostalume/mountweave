# MediaFire source

`mediafire.Open(ctx, mediafire.Options{RootKey: key, SessionToken: token})`
mounts one authorized folder through the MediaFire 1.5 Core API. The token
provider returns a v1 session token for the same account throughout the mount.
The host owns login and renewal. Both HTTP clients are optional and borrowed.
If construction returns a driver and an error, call `Close` on that driver.

Core provides Stat, List, Open, and Copy from this source. Each folder is read
in separate native folder and file chunks. Paths select exact names across
both streams and reject ambiguity. The response limit defaults to 4 MiB.

Reading bytes requires MediaFire to grant a direct-download link for the
selected file. Account permissions and available bandwidth govern that grant.
The driver accepts HTTPS capabilities and HTTPS redirects. It keeps the
session token on the fixed control API and uses a credential-free
`ContentClient` for the body. The returned range follows the actual response.

The sealed fixture exercises metadata and file transfer without an account.
MediaFire's public examples include HTTP direct links. Compatibility with an
account that receives only those links is outside the implemented read scope.

[Folder API](https://www.mediafire.com/developers/core_api/1.5/folder/#get_content) ·
[Direct downloads](https://www.mediafire.com/developers/core_api/1.5/download/#direct_download_link) ·
[Session tokens](https://www.mediafire.com/developers/core_api/1.5/user/#get_session_token)

## Website session

`OpenWebsite` resolves a path under My Files and checks the selected account
and folder before the mount takes ownership of the session. The host provides
the authenticated website session and an opaque account binding. The Driver
checks that binding and the requested folder on each native listing.

The website adapter uses the official app page to recover the MediaFire
session in the browser. It sends the API token only to MediaFire's fixed
control origin. Ordinary reads open the exact listed public download page in
a separate anonymous browser context. A credential-free HTTP client reads the
attachment. The Driver checks the file identity, name, and complete body.

With `BrowserSession`, `Core.MoveInto` moves one regular file to an existing
folder in the same mount. MediaFire keeps existing entries and may allocate
an unused name. The driver confirms the same file's quickkey and the actual
name in the target folder.

`Core.Remove` sends one file to Trash by quickkey,
including explicit `RemoveOptions{Disposition: core.RemoveTrash}`.
`Core.RemoveTree` with `Scope: core.Subtree` sends a selected non-root directory
and its contents to Trash by folder key. The driver confirms the same item and
its deletion date. Asynchronous acceptance and unavailable confirmation report
an unknown effect. Ordinary directory removal and conditional removal remain
unavailable.

[File deletion](https://www.mediafire.com/developers/core_api/1.5/file/#delete) ·
[Folder deletion](https://www.mediafire.com/developers/core_api/1.5/folder/#delete)

With WebsiteOptions.Upload configured, Core.Write can exclusively create a
nonempty file through a BrowserSession. The host supplies an existing private
staging directory and a per-mount byte budget. The driver validates the input to
EOF and computes SHA-256 before sending data. One upload per mount holds the
spool, and the browser receives at most one 4 MiB unit at a time. Native skip
rejects existing file or directory names.

The driver reconciles an uncertain nonfinal unit against the native bitmap.
After dispatching the final unit it confirms the same upload key, exact name,
size and digest, and checks its location. Unknown effects remain unknown when
the confirmation is unavailable. The local spool is removed on return. MediaFire
owns the cleanup of unpublished remote units.

[Native upload and polling](https://www.mediafire.com/developers/core_api/1.5/upload/#resumable)
