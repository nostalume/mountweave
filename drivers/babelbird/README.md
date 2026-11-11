# BabelBird Enterprise Drive source

Use `babelbird.Open` with the enterprise deployment's HTTPS origin, a
host-owned client carrying the developer access token, and the selected
workspace ID. Use `RootID: "1"` for enterprise files or `RootID: "2"` for
personal files. Set the optional `RootPath` to an exact subdirectory.
The host obtains and refreshes the token using the deployment's authorized
application flow.

Install the driver with Core `Install`. `Stat`, `List`, `Open` and the
source side of `Copy` use the same selected workspace. Directory and
file pages use native type filters because the published list entry does
not specify its kind. Each filter uses zero-based page numbers and the
server's reported page count. Listings do not report file size.

The driver requests a download grant for the selected file ID. Its absolute
HTTPS address and any HTTPS redirects use an independent client without
account credentials, even on the deployment's own origin. Provide an
independent `ContentClient` if that transport needs custom TLS settings.
Valid partial HTTP responses supply actual ranges. The read-only driver
expects the published list and grant response shape. Actual deployments may
vary with version and authorization.

[Authorization](https://www.babel.cc/public/babelwiki/zh/developer-api-overview.html) ·
[Directory pages](https://www.babel.cc/public/babelwiki/zh/api-get-nd-api-file-list-dir.html) ·
[Download grant](https://www.babel.cc/public/babelwiki/zh/api-get-api-file-getfiledownloadurl-do.html)
