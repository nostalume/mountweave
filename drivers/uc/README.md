# UC Cloud Drive

This source mounts a folder from an authenticated UC website session.
It supports metadata, paged directory listing, original-file streaming and
exact single byte ranges. It can serve as a Core Copy source.

The host opens a browser tab on [the UC website](https://drive.uc.cn/) and
provides its CDP context and a page call that evaluates a function with one JSON
argument. Use `WebsiteAccount` to check the signed-in account, then call
`OpenWebsite` with the account and folder path. Save the returned `RootID` and
supply it when reopening the mount.

`OpenWebsite` verifies the account, folder and first page before adopting the
release callback. Until it succeeds, the host owns the tab and its cleanup.
The driver correlates the selected file, the website's download grant and
the attachment response before opening the stream. The browser must stay
available until the mount closes. Empty files that the website downloads
locally and downloads without a usable website folder route are unsupported.
