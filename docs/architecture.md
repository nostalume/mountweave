# Architecture

Mountweave exposes file-management semantics as an embeddable Go library.
Applications compose the services they need; these responsibilities are not a
mandatory stack of forwarding layers.

## Domains and dependencies

| Owner | Facts and behavior it owns | Dependencies |
| --- | --- | --- |
| Core | Live bindings, logical namespace, authorization, synchronous file effects and operation resources | Standard library; narrow Driver contracts |
| Driver | Native root/IDs, protocol, connection, credentials and native effects | Core contracts; optional SDKs in separate modules |
| Control | Desired definitions, revisions and explicit prepare/activate/restore | Core plus host Store and Factories |
| Task | Admission, bounded scheduling, cancellation and finalization of arbitrary functions | Standard library; no file/configuration dependency |
| Input | Durable received bytes, confirmed offsets, immutable versions and pins | Standard library; independent of Core and Task |
| Transfer | Durable upload intent, source retention and protected native checkpoints | Core, Input and host Store/binding policy; no SDK or scheduler |
| HTTP | Wire representation, request lifetime and safe error projection | Core and optional injected control/task consumers |
| Host | Identity sources, storage, factories, clients, listeners and shutdown | Selected components |

The root package is Core. Storage implementations live under drivers/; protocol
exports live under protocols/. Local, JSON HTTP and WebDAV remain packages in the
standard-library-only root module. S3, Alipan and Pool keep separate modules.
control, task, input and transfer remain independently composable packages.
Core imports no implementation, protocol, configuration system, database or frontend.
Driver imports do not register global instances.

drivers/internal/upload is private shared upload machinery for Driver implementations;
protocols/internal/httpwire owns shared HTTP condition/range translation. Pool's
optional delivery package combines Pool with protocols/httpapi and task. The base
Pool package has no HTTP dependency, and the root module never imports Pool.

The Local, JSON HTTP and WebDAV import paths are now
github.com/nostalume/mountweave/drivers/local,
github.com/nostalume/mountweave/protocols/httpapi and
github.com/nostalume/mountweave/protocols/webdav. Update former /local, /httpapi
and /webdav imports to these paths. Their package names and public methods, the
root Core import path, and HTTP URLs/wire schema are unchanged.

## Finding Core behavior

| Responsibility | Source owners |
| --- | --- |
| Runtime construction, install and shutdown | core.go; mount.go owns use/retirement |
| Addresses and binding projection | address.go, binding.go |
| Captured selections, conflicts and execution admission | selection.go, admission.go |
| Metadata observations and projection | metadata.go |
| Opening policy, results and resource adoption | open.go |
| Stream body / persistent bound file / parent directory lifetime | content.go / file.go / directory.go |
| Basic name changes, effects and result confirmation | change.go |
| Body writes, complete publication and continuation | write.go, publication.go, continuation.go |
| Shared tree discovery | tree.go |
| Copy, Move and batch composition | copy.go, move.go, batch.go |
| Tree copy / removal and shared receipts | copy_tree.go / remove_tree.go; tree_result.go |
| Finite bounded execution / cross-call receive order | execution.go / order.go |

These are file owners within one Core package. Captured locations, mount uses and
native resource scopes retain their existing ownership; organizing them adds no
runtime dispatch or check. Stream EOF cleanup and persistent File lifetime remain
separate. New protocol handlers consume these same public operations and resources.

## Locating and changing files

Core maps typed tree/key addresses onto one live binding table, including fixed
aliases. Drivers receive root-relative NativeAddress/NativeQuery values. Tree
paths and literal keys have different invariants; provider IDs stay native.
Space identifies an existing binding, without a second resource registry.

Internally one captured location carries the binding generation and rooted address.
Visible and primary authorization addresses project from it. Selection retains these
locations; use adds execution lifetime, and an owned directory binds the parent
location to its native scope. Children are derived from that parent. Prepared
operations consume locations directly instead of pairing raw paths with optional
expected locations/scopes or resolving the same selection again. Dynamic grants,
retirement and native-object conditions are still checked at actual use. This
address is not a persistent object ID or a content version.

List uses one query/cursor owner for children or prefix/delimiter results. A
prefix group is not a directory. Optional pure key projections let tree/key views
share conflict scheduling without metadata probes; unknown overlap is conservative
for that owner. Ordinary stream bodies have no per-buffer namespace dispatch.

Optional mutation interfaces express native capability without expanding the
mandatory read interface. Core owns Copy, Move, batch and tree composition;
protocols and queued work use those same operations. A capability advertisement
does not grant permission or reserve resources: execution revalidates admission.
See the core package documentation for exact selection and partial-effect rules.

## Resources, configuration and work

An operation owns its actual mount use; returned bodies and scopes retain it
until cleanup finishes. Retirement blocks new use, requests cancellation and
waits for real acknowledgement. Timeouts bound waiting, not arbitrary native I/O.
Known effects and cleanup errors survive cancellation. Failed cleanup retains an
owner instead of claiming that replacement or shutdown finished.

Configuration save and live activation are separate facts. A replacement factory
loads the latest credential checkpoint after the old refresh owner retires.
Credentials remain native; Core neither parses them nor persists configuration.

The task package runs any function, including file work. It does not serialize
closures or decide retry semantics. A durable consumer owns versioned intent,
receipts and safe continuation rules. Pool maintenance is one such consumer:
its bbolt ledger owns manifests and private shard receipts while task only runs
the work. This does not make Pool a prerequisite for other durable jobs.

The input Store is another independent consumer. Its journal owns receipt and
sealed-source facts; acquiring a reference retains no mount use or open file.
HTTP prepares fs.write by pinning that source and capturing the target binding,
then invokes Core.Write at execution. Release waits for accepted references and
bodies and durable holds. Durable input does not serialize queue closures or
automatically replay file publication after restart. Transfer is the separate
checkpoint consumer: it persists an idempotent preparation before retaining a
sealed source, resolves a fresh verified binding for execution, then passes an
explicit continuation through Core.Write. Terminal intent precedes releasing its
source hold. Store commits metadata revisions with incremental native records;
queries load metadata, while recovery reads the committed log at a specified
revision. S3 owns native part receipts and commit/abort proof; a missing native
upload does not prove a lost publication outcome. Ordinary writes need neither
input nor transfer, and task itself remains process-local.

## Pool is storage, not a mount view

Pool stores shards of one logical file across private member Drivers and exposes
a single ordinary filesystem. It owns coding, placement, quota, publication and
repair facts; Core owns the outer user namespace and authorization. Its local
metadata writer is not a distributed metadata service. Its optional native
upload reconciliation requires terminal evidence, not just a successful Stat.
The [Pool reference](../drivers/pool/README.md) owns its format and recovery contract.

## Evolution

The library is inspired by OpenList workflows, independently implemented, and
does not promise compatibility with OpenList API routes or configuration.
Public API details belong in GoDoc, the HTTP wire contract in OpenAPI, and
native configuration in each Driver reference. Benchmarks inform cost choices;
machine-specific measurements do not become universal design limits.
