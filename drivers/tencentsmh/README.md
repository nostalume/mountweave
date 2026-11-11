# Tencent Cloud Smart Media Hosting source

`tencentsmh.Open(ctx, Options{LibraryID: library, SpaceID: space, AccessToken: token})`
opens a Tencent Cloud Smart Media Hosting (SMH) space. Empty `SpaceID` selects
the single-tenant space. `RootPath` optionally selects an absolute path in the
space. Set `Endpoint` to the library's HTTPS origin if it has a dedicated
endpoint. The host supplies a [business access token](https://cloud.tencent.com/document/product/1339/71159)
for the selected library and space. A nil token provider selects anonymous
access for a public-read space. Install the driver with Core's `Install`.

`Stat` verifies the entire returned native path in one metadata request.
`List` checks the directory path and follows the native marker. `Open` and the
source side of `Copy` verify the entire ordinary file path, then request a
signed HTTPS download conditional on its native content CAS. A changed file
fails before any download starts. Symbolic links and virtual entries remain visible as names
without readable bodies. The driver streams the actual response and projects
a range only when the content server returns one.

Control requests go to one HTTPS origin and do not follow redirects. The
optional `ContentClient` must never add account credentials. Both clients
remain host-owned. `MaxResponseBytes` bounds each control response and defaults
to 4 MiB.

## Recipient shares

Use `tencentsmh.OpenShare(ctx, ShareOptions{Endpoint: origin, ShareCode: code,
ExtractionCode: password})` for an extraction-code-protected share that allows
anonymous recipients. Use the provider's HTTPS API origin. Install the
returned read-only source with the same Core API. An unprotected or
account-required share needs a separate access contract.

The driver renews the share-scoped token using its native expiration, follows
the native listing markers, and locates exact names by inode chain. An open
uses the selected inode chain to request the original. The share token is sent
only to the control origin. The independent content client follows the issued
HTTPS address and receives no account or share headers. Download requests may
count against the share's download limit. The driver does not transfer shared
files into another account.

[Tencent SMH Go SDK share access](https://cloud.tencent.com/document/product/1339/138982)
