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

The root module contains core, local, control, task, input, transfer and httpapi. S3, Alipan and
Pool each have their own module. Core imports no SDK, configuration system,
database or frontend. Driver package imports do not register global instances.

## Locating and changing files

Core maps visible paths onto one live binding table, including fixed aliases.
Drivers receive strict root-relative logical paths. Local paths, S3 prefixes
and provider root IDs share a responsibility but not a universal representation.

Internally one captured location carries the binding generation and rooted address.
Visible and primary authorization paths project from it. Selection retains these
locations; use adds execution lifetime, and an owned directory binds the parent
location to its native scope. Children are derived from that parent. Prepared
operations consume locations directly instead of pairing raw paths with optional
expected locations/scopes or resolving the same selection again. Dynamic grants,
retirement and native-object conditions are still checked at actual use. This
address is not a persistent object ID or a content version.

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
