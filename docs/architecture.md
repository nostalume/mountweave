# Architecture

Mountweave gives Go applications one file-management model for local storage,
cloud drives and object stores. An application mounts the storage it needs and
chooses how to expose it to users.

## Domains and dependencies

| Component | Responsibility |
| --- | --- |
| Core | Mounts, visible addresses, authorization, file operations and resource lifetime |
| Driver | Native roots, identifiers, credentials, connections and evidence of storage effects |
| Protocol adapter | Wire commands, client sessions and response mapping |
| Host | Identities, policies, storage configuration, listeners and shutdown |

The Core module uses the Go standard library. It includes Local and the JSON
HTTP, WebDAV and FTP servers. Drivers and servers that need extra dependencies
use separate Go modules. The host constructs and mounts each Driver explicitly.

WebDAV, SFTP, FTP and S3 can be used to connect to storage or to serve clients.
Each side has its own capabilities. A server uses the same Core operations as a
Go application and exposes the behavior the mounted Driver can support.

## Core API

| Task | Core entry points |
| --- | --- |
| Set up storage | `New` takes an authorizer. `Install` adopts a prepared Driver. `BindAlias` gives an existing directory another visible path. |
| Browse | `Stat` observes an entry. `List` pages through children or object keys. |
| Access content | `Open` returns a stream by default. Supported mounts can return a bound file for random reads or editing. |
| Change files | `Write`, `Mkdir`, `Remove`, `Copy` and `Move` share the same addressing and authorization. Tree and batch operations compose these semantics. |
| Release resources | Close opened bodies and files. Retire a mount when removing it. Close Core when the host shuts down. |

## Locating and changing files

An address selects either a tree path or a literal object key. Tree paths model
directory relationships. Object keys preserve their exact spelling, so keys
such as `a` and `a/b` can coexist. Key listings can filter by prefix and group by
delimiter. Each Driver resolves addresses relative to its configured root and
keeps native IDs and temporary download URLs private.

A mount or alias resolves to one storage binding. Core authorizes both the
visible address and its primary address when an alias is used. Work rechecks
access and binding identity at execution. An already opened body or file stays
with the resource it acquired. Listings have a time limit and a cursor tied to
the caller, query and binding. Changes to a mount can invalidate its cursors.

A Driver supplies metadata, enumeration and reading. It can add native
capabilities for stronger guarantees such as observing a link itself, opening
without following links, editing a bound file or enforcing a write condition.
Core rejects guarantees the mount cannot provide. A range result describes the
bytes actually returned. A Driver may return the full stream for a range
request.

Ordinary writes create a new name and may leave partial content after failure.
Complete publication receives the whole input before changing the target and
enforces any condition at the native commit. Cross-mount moves publish a new
target before conditionally removing the matching source. Source and target
effects are recorded separately. Batch operations run serially by default.
Bounded parallel execution preserves the order of conflicting work.

Check the reported effect along with the error. A confirmed change can come
with a cancellation or cleanup error. An uncertain effect needs reconciliation
before retrying.

## Resources, configuration and work

A stream keeps its mount in use through completion or close. A bound file keeps
its opened object through EOF and releases it on close or retirement. Retirement
blocks new work, requests cancellation and waits for native cleanup. The wait
can time out while the original owner still has cleanup to finish.

Streams carry bytes directly from their native source. Drivers own their native
staging, connections and upload buffers. The host supplies deployment-specific
limits and decides how long to wait for shutdown.

Optional packages fit around the same Core operations.

- **Control** stores desired mount definitions and manages activation separately.
  The host supplies storage and Driver factories.
- **Task** runs functions with bounded scheduling and cancellation. File tasks
  call the same operations as synchronous callers.
- **Input** receives bytes and seals a stable source for later use.
- **Transfer** persists upload intent, holds the sealed source and records
  checkpoints. It resumes with a host-verified target binding and the same write
  policy. The Driver provides native progress and publication evidence.

## Distributed storage

Pool presents one file namespace while placing each file's shards across
private members. It owns placement, publication, degraded reads and repair.
Core still owns the visible mount and access policy. See the
[Pool reference](../drivers/pool/README.md) for storage and recovery details.

## Reference

- [Core package documentation](../doc.go)
- [Driver capabilities](drivers.md)
- [JSON HTTP API](../api/openapi.json)
