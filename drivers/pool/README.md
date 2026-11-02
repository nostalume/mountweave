# Erasure-coded storage pool

An independent, optional Go module providing one logical filesystem over private
member Drivers. The core, HTTP adapter, and generic task runtime do not import
this module. Encoding and bbolt dependencies stay here.

## Construct and mount

Prepare exclusively owned members, then provide explicit options:

```go
storage, err := pool.New(pool.Options{
    MetadataPath:     privateMetadataPath,
    LockTimeout:      time.Second,
    DataShards:       2,
    ParityShards:     1,
    ShardBytes:       1 << 20,
    MaxBufferedBytes: 6 << 20,
    Members: []pool.Member{
        {ID: "a", Identity: "account-a/root", FailureDomain: "account-a", Driver: a},
        {ID: "b", Identity: "account-b/root", FailureDomain: "account-b", Driver: b},
        {ID: "c", Identity: "account-c/root", FailureDomain: "account-c", Driver: c},
    },
})
if err != nil {
    return err // The caller still owns a, b, and c.
}
if _, err := files.Install("/pool", storage); err != nil {
    return errors.Join(err, storage.Close())
}
```

These numbers illustrate configuration, not default allowances. `Concurrency`
defaults to one and bounds per-operation member I/O; encoding uses one worker.
The metadata parent must exist on a supported local filesystem. Keep metadata
and member roots private and stable; do not expose member objects independently.
Identity/failure-domain strings are host assertions without credentials. Aliases
of one account/root cannot provide independent capacity or redundancy.

`New` acquires the bbolt writer lock; `LockTimeout` zero waits without a timeout.
Only success transfers member ownership. Credentials remain with native Drivers
and their existing refresh owners. Reopen with matching stored layout/identities;
omitted or nil Drivers are offline and read-only members can serve old data.
Stored quota/placement policy is preserved on reopen. `Member.MaxBytes` supplies
the initial private-byte cap; use `ConfigureMember` to change existing policy.

## File API

Pool implements the existing `Driver`, `DirectoryMaker`, `FileCreator`,
`FilePublisher`, `Remover`, `Relocator`, and `DirectoryReader` capabilities.
Directory scopes also implement `ChildRemover`. Use ordinary Core APIs:

```go
files.Mkdir(ctx, principal, "/pool/videos")
files.Write(ctx, principal, "/pool/videos/movie", input, length)
files.Write(ctx, principal, "/pool/videos/movie", input, length,
    core.WriteOptions{Replace: true})
files.Move(ctx, principal, "/pool/videos", "/pool/archive")
```

`CopyTree`, `RemoveTree`, batches, and directory-merge options reuse their existing
Core implementation and budgets. There is no alternate Pool file API or HTTP
route. File authorization remains in Core; native management belongs to the host.

Directories and files have stable IDs, with names indexed by parent directory
ID. A move changes metadata, not file bytes or descendant path keys. Scope reads
remain bound to their directory across rename. Create refuses existing targets;
Publish creates or replaces a regular file only after complete input and full
redundancy. It captures its parent ID before reading input, preserving that parent
across rename and refusing publication into a deleted/recreated parent.

An open pins its immutable content generation. Replacement atomically switches
the name to a new generation; deletion unlinks the name. Both leave old readers
usable until Close. Logical success does not claim physical garbage collection.
Remove is nonrecursive and refuses nonempty directories; Move does not overwrite.

## Members, capacity, and maintenance

| Host API | Behavior |
| --- | --- |
| `Members(ctx)` | Stable identity, policy, private-byte usage and runtime attachment; attachment is not network health |
| `AddMember(ctx, member)` | Persist membership, then attach; only Applied transfers ownership, including Applied with cancellation error |
| `ReconnectMember(ctx, id, identity, build)` | Retire and close the old native owner, then construct its same-identity replacement; nil build leaves it offline |
| `ConfigureMember(ctx, id, MemberPolicy)` | Change MaxBytes and Draining; existing reservations/reads remain valid |
| `Drain(ctx, id, maxRecords)` | Stop new placement and copy/verify bounded existing shards to eligible members |
| `Collect(ctx, maxRecords)` | Reclaim proven unused private objects and release their reservations |
| `Repair(ctx, maxRecords)` | Verify committed stripes and reconstruct/verify missing or corrupt data/parity shards |
| `RemoveMember(ctx, id)` | Require draining and zero reservations, retain an identity tombstone, then close the owned instance once |

Any AddMember result other than Applied leaves its Driver with the caller.
Metadata confirmation failure freezes writes; retry/query cannot silently adopt
a second instance. A removed ID cannot represent another root. Configuration
storage remains a separate host responsibility; saving configuration is not
membership activation. These are trusted management calls, not new public routes.

Reconnect stops new native uses, cancels existing ones and waits for their calls
and returned bodies to acknowledge completion before closing the old Driver.
Only then does `build(ctx)` load the latest credential checkpoint and create the
replacement. Identity is the host's account/root assertion, checked against the
stored mapping; Pool cannot discover credentials or verify that assertion through
the generic Driver interface. Other members and namespace operations remain usable.
`Members` exposes `Retiring` separately from attachment; neither is a health probe.

Reconnect's Applied receipt means retirement began. Cancelled waiting or failed
construction can leave the member offline/retiring; invoke it again to continue.
It never starts a competing credential writer after failed native cleanup. A
non-nil Driver returned by build transfers even with an error and is then closed;
failed cleanup remains owned. Cancellation does not forcibly interrupt native
Close or a blocked build callback. Native refresh/retry policy stays in the member.

MaxBytes zero adds no Pool cap; it does not assert unlimited provider capacity.
Positive limits count padded private shard bytes, including pending/uncertain
uploads and old generations, not provider physical blocks or other applications.
Reservations are atomic with durable intents before native I/O. Placement favors
less-charged eligible members while separating failure domains; it does not infer
unknown account quotas. Shrinking a limit preserves existing content/reservations
and can stop additional placement. New membership does not alter old manifests
or coding parameters, nor start an implicit whole-pool rebalance.

Maintenance returns `Visited`, `Changed`, `Bytes`, and `More`. Limits count stripe
records and generation boundaries. Changed/Bytes describe confirmed placement
switches or reclamation; errors can accompany partial or unconfirmed effects.
More can remain true with no progress while readers or unresolved intents remain.
Calls keep a runtime scan cursor; restart can rescan durable receipts safely.
One bounded maintenance call runs per Pool at a time, without holding locks or
transactions across member I/O. Submit these functions to the existing generic
task queue when background lifetime is desired; no separate runner is required.

Drain reads and verifies the source shard, writes a fresh immutable destination,
verifies it, then publishes the new location. Old readers protect the abandoned
source until Collect. Cancellation leaves partial confirmed work; policy changes
invalidate stale scan progress, and unrelated uploads do not prevent completion.
If the source is lost, Drain fetches enough verified other shards to reconstruct
it. Repair scrubs all slots, requires at least DataShards valid inputs, reconstructs
data and parity, and verifies stored hashes before using the same destination
publication path. Both reuse one stripe payload buffer and native concurrency.
Insufficient valid inputs or destination capacity fails without discarding the
old manifest. Repair never publishes a partially received user file.
RemoveMember verifies zero reservations rather than trusting a completed scan.

Collect requires confirmed upload effects and no active upload/old reader. It
uses native Remover receipts for immutable, never-reused private keys. A missing
confirmed object is already reclaimed; lost delete confirmation can be retried.
For Unknown/Incomplete uploads, Collect optionally calls the member's
`UploadResolver.ResolveUpload(ctx, PrivateUpload{Path, Size, SHA256})`. Applied
must prove exact complete content and terminal creation; Unchanged must prove
terminal absence. Both exclude delayed creation at that immutable attempt key.
Only then is its receipt updated and the object reclaimed or reservation released.
Confirmed terminal receipts survive accompanying errors, which are still returned.
The member adapter owns any provider-specific operation IDs/checkpoints needed
for that proof. This capability belongs to Pool integration, not to every Core
Driver. Without it, or while native outcome is unresolved, `ErrUncertain` and the
charge remain. Stat/hash alone cannot fence a delayed remote upload. Missing
deletion capability also retains ownership/usage. Existing S3/alipan adapters do
not yet provide native reconciliation; they do not acquire that promise implicitly.

## Durable maintenance and tasks

Direct Repair/Drain/Collect calls use runtime scan cursors and durable per-object
receipts. For restart continuation, the host can save a finite maintenance pass:

```go
change, err := storage.PlanMaintenance(ctx, "repair-pass-1",
    pool.MaintenanceRequest{Kind: pool.RepairWork})
// Check change/error. Repeating this ID/request does not replay a finished pass.

handle, err := queue.Submit(ctx, task.Work{
    Run: func(ctx context.Context, report func(any)) (any, error) {
        for {
            job, err := storage.RunMaintenance(ctx, "repair-pass-1", 32)
            report(job)
            if err != nil || job.Done {
                return job, err
            }
        }
    },
})
```

`DrainWork` additionally takes Member; `CollectWork` needs no member. Intent,
checkpoint and latest batch receipt live in the same Pool database and Backup.
`MaintenanceJobs(ctx, afterID, limit)` lists them in ID order for the host to
query/resubmit unfinished work after reopen. RunMaintenance saves native effects
before scan progress, so a crash may safely rescan a batch. `Last` is the last
acknowledged batch, not an exactly-once cumulative count. Done means that finite
pass ended, not that later uploads remain perpetually healthy. Replacement/deletion
during a repair cannot resurrect a generation that no longer has a logical name.

Cancellation/error leaves a resumable intent and all confirmed partial effects.
The host chooses scheduling/backoff when no progress is possible, rather than
spinning on More. `ForgetMaintenance` removes an inactive intent/receipt without
undoing file effects; private resources remain owned by the shard ledger. A
running batch must return first; its task Handle can request cancellation.
The generic queue still accepts arbitrary functions, without serializing closures,
credentials, bodies or a second Pool scheduler. No new HTTP route is introduced.

## Format, buffers, and recovery

The format uses systematic Vandermonde GF(256) Reed-Solomon with explicit positive
data/parity counts totaling at most 256. Each stripe holds DataShards*ShardBytes
logical bytes; tail padding stays private. Manifests record exact length, codec,
membership revision, ordered locations, SHA-256 hashes, attempts and receipts.
Normal EOF, declared length and all configured Applied receipts precede name
publication. Original uncertain receipts survive spare placement. Reads validate
needed shards before output and reconstruct on failure; insufficient verified
shards produce an error after any emitted prefix. Healthy Range reads fetch only
relevant data shards, with whole-shard checksum amplification.

Shared MaxBufferedBytes counts payload buffers, excluding metadata/DB mappings,
encoding matrices and SDK storage. Buffered reads, writes, drains and repairs reserve
(DataShards+ParityShards)*ShardBytes; admission refuses CapacityExceeded instead
of waiting on a borrowed Reader that may require the same budget. Same-pool Copy
needs reader and writer space. Close every Body/scope. A failed native Body.Close
retains ownership and stops new buffered I/O; metadata/backup stay available.
Failed member retirement keeps that member unavailable and prevents its replacement.

Metadata uses synchronized bbolt transactions and one local writer instance;
filesystem/hardware durability belongs to the host. Format one is imported into
format two atomically, preserving content and deriving conservative usage from
its original receipts. `Backup(ctx, writer)` streams a consistent snapshot; the
host durably saves output and separately preserves member objects/credentials.
Slow output can delay database growth; do not write a backup into the same Pool.
There is no implicit periodic repair or persistent scheduler: the host resumes
maintenance IDs through the existing runtime. Logical name publication is one
database transaction; reopening reads its committed result. An interrupted user
stream is garbage, not automatically resumed or published. This is not an
exactly-once retry promise for arbitrary client writes without a correlation ID.
`Close` cancels, waits for actual acknowledgement, then closes owned members and
metadata, preserving cleanup failures.

## Verification

Run `go test ./...` in this module. Nine synthesis scenarios reuse disposable Local
members with quotas/faults: distribution/Range, restore, partial effects, cleanup,
full file management, pinned generations, expansion/drain, schema import, parent
competition, cancellation and concurrent quota admission. Recovery coverage adds
member/body handoff, data/parity repair, durable pass continuation and a real child
process exit between native upload and metadata receipt, followed by a lost delete
acknowledgement, and another exit after a repair upload but before placement commit.
The local resolver fixture uses the acknowledged process exit as
its no-late-writer proof; it establishes no equivalent remote-provider guarantee.
No live account or disk power-loss test is used.
