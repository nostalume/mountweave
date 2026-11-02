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
| HTTP | Wire representation, request lifetime and safe error projection | Core and optional injected control/task consumers |
| Host | Identity sources, storage, factories, clients, listeners and shutdown | Selected components |

The root module contains core, local, control, task and httpapi. S3, Alipan and
Pool each have their own module. Core imports no SDK, configuration system,
database or frontend. Driver package imports do not register global instances.

## Locating and changing files

Core maps visible paths onto one live binding table, including fixed aliases.
Drivers receive strict root-relative logical paths. Local paths, S3 prefixes
and provider root IDs share a responsibility but not a universal representation.

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
