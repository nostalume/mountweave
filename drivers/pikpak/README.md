# PikPak source

PikPak mounts a folder for browsing, ordinary file reads, and single byte
ranges. Create a Personal Access Token with **Read files** permission in
**Account & Security > Connected Apps**.

Use `pikpak.Open(ctx, pikpak.Options{Client: api, DeviceID: deviceID})`.
Supply a stable 32-character hexadecimal device ID. The API client sends the
token to `pikpak.ControlHost` over HTTPS. Set `RootID` to pin a directory by its
native ID. An empty ID selects the supplied credential's current account root.

The API and content clients can use the same host-configured network route.
The optional `ContentClient` must be free of account credentials. PikPak may
sign an HTTP URL for the file body. The driver reads the original signed URL
and validates the returned file length and range. The host owns both clients.

Directory listings use PikPak's native page token. Exact path lookup checks
for ambiguous names. The driver is read-only.

[PikPak permissions and network guidance](https://mypikpak.com/en-US/faq) ·
[Folder and file operations](https://github.com/pikcloud/skills/blob/main/skills/pikpak-cli/SKILL.md#file-browsing)
