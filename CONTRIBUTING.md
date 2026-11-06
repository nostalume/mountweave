# Contributing

Use Go 1.24 or newer. Run the following in the repository root and separately
in drivers/s3, drivers/alipan, drivers/pool and protocols/sftp; root commands do
not traverse nested modules. The optional SFTP codec/client dependencies require
Go 1.25 or newer in that module:

```sh
go test ./...
go vet ./...
# Where a supported C compiler is available:
go test -race ./...
```

The root checks include drivers/local, protocols/ftp, protocols/httpapi, protocols/webdav
and their shared internal packages. Local module replacements are for checkout
development. Keep optional dependencies
in their owning modules. Do not add a global registry or require every consumer
to import every Driver. A root-only change should not need a remote account.

Tests use temporary storage, loopback services and synthetic credentials. Do not
provide real accounts or contact cloud services in functional tests. Prefer
existing synthesis/integration scenarios for cancellation, conflict and partial
effects; do not grow a test matrix that mirrors every implementation detail.
Do not use a local benchmark result as a universal resource default or gate.

Keep names, code, comments, schemas and public documentation in English. Put
behavioral contracts with the owning package, native options in Driver references,
and wire changes in OpenAPI. Explain API changes and their compatibility impact.
Never infer success from a progress update, timeout or missing response.

Mountweave is independently developed and inspired by OpenList. Submit only
material you have the right to contribute under MIT. Contributions are accepted
under the project MIT license unless explicitly agreed otherwise. Third-party
code and dependencies retain their original notices and licenses; do not remove
attribution or relabel copied code as project-owned MIT code. Include relevant
source and license information when proposing an external adaptation.

Keep private plans, work directories, generated caches and credentials outside
the public change. The API remains in v0 development; see the repository releases.
