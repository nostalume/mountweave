# LanZou Cloud

This read-only source mounts a folder from an authenticated LanZou website.
It supports metadata, folder and file listing, and original-file downloads
through the file's existing share. A download can report a precise range
or return the full original when the server ignores the range.

Open the browser at [the LanZou website](https://up.woozooo.com/mydisk.php) and
supply its CDP context, page evaluator and release callback to `OpenWebsite`.
Use `RootFolderID` for the top-level folder or supply the saved decimal root ID.
`FolderID` verifies a selected path for mounting. Save its result and
the verified `Account` when available.

A successful `OpenWebsite` adopts the release callback. Before success,
the caller owns the page. The Driver binds each directory response to the
account and uses a separate browser context for the original download.
