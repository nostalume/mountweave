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
