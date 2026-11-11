# Box source

This optional Go module mounts an authorized Box folder through the existing
Core Stat, List, Open and Copy APIs. Use `box.Open(ctx, box.Options{Client:
authenticatedClient})` for the account root or supply a Box folder `RootID`.
The constructor checks the selected folder. Close a non-nil failed result to
complete any cleanup it still owns.

The supplied client authenticates the fixed Box API origin. The optional
`ContentClient` is borrowed and must be credential-free. It defaults to an
unauthenticated client. Download redirects use only that client, even when a
redirect targets the API host. Only HTTPS redirects are followed, with no cookie
jar, referrer, or transfer of the account request's headers. Custom content
transports must keep capability URLs out of final body errors.

Exact path lookup reads native folder pages and rejects ambiguous names. Box's
opaque markers control native progress. A listing retains the markers it has
seen to reject cycles. Core owns the public cursor. `MaxResponseBytes` limits
each metadata response and defaults to 4 MiB. A web link is visible without a
file size and has no readable body.

Original file bytes stream directly. Actual HTTP body and range results are
reported, including full-response fallback. A pending download preserves a
numeric `Retry-After` as an error cause. Writes, version selection, and bound
file access are outside this module's contract.

[Folder listing](https://developer.box.com/reference/get-folders-id-items) ·
[Marker pagination](https://developer.box.com/guides/api-calls/pagination/marker-based) ·
[File download](https://developer.box.com/reference/get-files-id-content)
