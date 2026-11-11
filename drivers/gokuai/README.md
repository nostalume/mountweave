# Gokuai enterprise library

This optional module mounts one Gokuai enterprise library as a read-only tree.
Enable [library developer authorization](https://developer.gokuai.com/grant/library.html)
for file listing, information, and download. Pass the issued library ID and
secret to `gokuai.Open(ctx, gokuai.Options{...})` and install the Driver in
Core. The host supplies the library secret at runtime. The Options JSON
excludes the secret and both HTTP clients. An empty `RootPath` mounts the
whole library. A scoped root is an absolute path, such as `/Team docs`.

The Driver uses the [library file API](https://developer.gokuai.com/yk3/file.html).
It signs control requests for `yk3.gokuai.com`, lists by native offset, and
requests the original download for the exact path and observed content hash.
Downloads require an HTTPS URL issued by the server. The separate
`ContentClient` must be credential-free, including for redirects back to the
control host. Both HTTP clients are optional and remain host-owned. The
control response limit defaults to 4 MiB. Requested ranges are reported only
when actually served upstream.

The current contract covers Core Stat, List, streaming Open, and Copy as a
source. The sealed fixture validates the HTTPS flow and rejects insecure
grants. A live tenant's HTTPS grant availability has not been verified.
