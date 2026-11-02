# Mountweave

Composable file management and storage primitives for Go.

Inspired by OpenList, Mountweave is an independently developed library for
building file-management backends. It is not a refactor of OpenList or a
replacement for its API. Its goal is to provide core functionality as an
embeddable dependency; it does not yet cover every OpenList feature or Driver.

The core uses only the Go standard library. It does not load configuration,
start listeners, or require a frontend, database, cache or task system.
Go 1.24 or newer is required. The package name of the root library is `core`.

## Start with a local mount

```go
package consumer

import (
    "errors"

    core "github.com/nostalume/mountweave"
    "github.com/nostalume/mountweave/local"
)

func NewLocalReader(directory string, authorize core.Authorizer) (*core.Core, error) {
    files, err := core.New(core.Options{Authorize: authorize})
    if err != nil {
        return nil, err
    }
    storage, err := local.New(directory)
    if err != nil {
        return nil, err
    }
    if _, err := files.Install("/files", storage); err != nil {
        return nil, errors.Join(err, storage.Close())
    }
    return files, nil
}
```

Supply an authorization policy, then call Stat, List, Open and optional mutations
with an explicit principal. Close every returned Body and directory scope.
Successful Install transfers Driver ownership; a rejected Driver stays with its
creator. The application eventually calls Core.Close with its own wait context.

## Compose what the application needs

| Component | Responsibility | Reference |
| --- | --- | --- |
| `core` | Namespace, authorization, file operations and resource lifetime | `go doc -all .` |
| `local` | Prepared filesystem root and native file operations | `go doc -all ./local` |
| `control` | Desired mount definitions and explicit activation/restore | `go doc -all ./control` |
| `task` | Arbitrary bounded function execution | `go doc -all ./task` |
| `httpapi` | Optional HTTP delivery with injected identity and policies | [OpenAPI](api/openapi.json), `go doc -all ./httpapi` |
| Optional Drivers | Native clients, credentials and storage representation | [S3](drivers/s3/README.md), [Alipan](drivers/alipan/README.md), [Pool](drivers/pool/README.md) |

See [Architecture](docs/architecture.md) for dependency direction and ownership.
Detailed behavioral contracts live with their owning packages in GoDoc.

## Current scope

The core supports mounted reads, fixed aliases, synchronous file operations,
bounded batches and finite tree operations. Copy belongs to Core; optional
Driver capabilities determine which mutations are available. Cross-mount Move
is currently unsupported. File effects remain observable alongside errors and
cancellation; a tree operation is not a transaction.

Local provides file and directory mutations. S3 currently provides reads and
known-length conditional creation; Alipan provides reads and rotating credentials.
The optional Pool Driver provides an erasure-coded logical filesystem, member
management, repair and durable maintenance. These are independently versioned
modules; the core does not import them or their SDK/database dependencies.

Remote adapters have closed protocol tests, not a universal live-service
compatibility claim. The host owns credential persistence and chooses budgets.
Cancellation requests a stop; it does not forcibly interrupt arbitrary native I/O.
There is no product executable, frontend or legacy configuration converter.

## Development and availability

Install the core library:

```sh
go get github.com/nostalume/mountweave@v0.1.0
```

Optional Driver modules are published separately from the same repository:

```sh
go get github.com/nostalume/mountweave/drivers/s3@v0.1.0
go get github.com/nostalume/mountweave/drivers/alipan@v0.1.0
go get github.com/nostalume/mountweave/drivers/pool@v0.1.0
```

The Driver modules depend on core v0.1.0. Relative replacements apply only to
checkout development; consuming applications do not need local replacements.

See [Contributing](CONTRIBUTING.md) for commands covering all four modules.
The API is under development; no v1 compatibility promise is made yet.

## License

Mountweave source, documentation and schemas are available under the [MIT License](LICENSE).
Third-party dependencies retain their own licenses; see [Third-party notices](THIRD_PARTY_NOTICES.md).
