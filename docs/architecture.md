# Architecture

Mountweave gives applications a common file service over installed storage.
Hosts choose the storage, visible mounts and access policy. Drivers interpret
native storage; protocol adapters serve the same Core to clients. The Core
module uses only the Go standard library. Drivers and adapters that need
external dependencies have separate Go modules.

## A request through the library

```text
Go caller or protocol adapter
  -> Core: authorize, resolve the mount, borrow its Driver
  -> Driver: resolve a name under its native root, perform the operation
  -> Core: expose a page, stream, bound file or change receipt
```

The host creates the Driver and installs it under a visible prefix. Successful
installation hands lifetime ownership to Core. A Driver resolves native paths,
object keys or provider IDs under its root. An adapter maps wire requests to
Core operations, so the same authorization and storage guarantees apply to Go
callers and protocol clients. A Driver can decline a guarantee the native
operation cannot prove; Core preserves that limit in its result.

## Where to change behavior

| Question | Owner and starting point |
| --- | --- |
| Which paths, operations or effects should mean the same on every mount? | Core: [file service specification](specification.md) and [Go API contract](../doc.go). |
| How does a service bind a root, page a listing or read the original body? | The [specific Driver](drivers.md), following [Driver development](driver-development.md). |
| How does a wire request become a file operation, and how is the result reported? | The [protocol adapter](../protocols/). Start with its package contract and client fixtures. JSON HTTP also has an [OpenAPI schema](../api/openapi.json). |
| How are identities checked, secrets stored, listeners started or mounts configured? | The embedding host. Its deployment settings and credential store stay outside Core. |

WebDAV, FTP, SFTP and S3 have two directions. A source Driver talks to remote
storage. A server adapter handles incoming clients and can serve any mounted
source that satisfies the operation. An outbound request header belongs to the
Driver; a status delivered to a connected client belongs to the server adapter.

## Boundaries that keep the library small

- **Names and access.** Core keeps the caller's address for authorization and
  projects it into a rooted Driver address. Aliases share the original mount,
  its lifetime and its limits. Provider IDs remain inside the Driver.
- **Guarantees and effects.** Optional Driver interfaces express additional
  guarantees. Core composes file operations and carries evidence through the
  result. Only the native operation can confirm a conditional write or safe
  removal at the point of change.
- **Bytes and resources.** Ordinary reads stream the native body. Drivers own
  native buffering, connections and temporary upload state. The host decides
  deployment budgets and how long to wait for shutdown.

## Optional components

- [Control](../control/doc.go) saves desired mount definitions and activates
  an explicit revision. The host supplies persistence and Driver factories.
- [Task](../task/doc.go) schedules functions. A file task calls the same Core
  operation as a synchronous caller.
- [Input](../input/doc.go) seals bytes for reuse. [Transfer](../transfer/doc.go)
  pairs sealed input with durable upload intent and native checkpoints, using
  host-supplied persistence and binding checks.
- [Pool](../drivers/pool/README.md) is a Driver. It serves one tree while owning
  shard placement, publication, degraded reads and repair. Its private members
  stay outside the visible mount namespace.
