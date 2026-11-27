# FEB Box

This read-only Driver mounts folders and original files on an authorized user's
own disk. It supports directory pagination, metadata, and streaming reads with
the byte range actually returned by FEB Box.

Call `febbox.Open(ctx, options)` with an HTTP client that supplies the user's
authorization to `https://api.febbox.com`. Choose `RootID` or `RootPath`, or
leave both empty for the account root. The constructor verifies the selected
directory. An optional `ContentClient` must use a transport free of account
credentials. The Driver obtains a fresh original download URL when opening a
file and checks the file and parent IDs before using it.

The host owns authorization, token rotation, and the HTTP transports. Close
the Driver after active operations and streams have drained. The first
integration is checked against a sealed two-endpoint fixture. Application
registration, original download allowances, and private-root acceptance
await live verification.

[Authorization](https://www.febbox.com/open/docs?id=local-oauth-authorize) ·
[Directory listing](https://www.febbox.com/open/docs?id=api-146718079) ·
[Original downloads](https://www.febbox.com/open/docs?id=api-146674774)
