// Durable transfer is an optional consumer of input and Core. It owns intent and
// native checkpoints; task remains an arbitrary function runner. Core never
// imports transfer/input, and this package imports no Driver SDK or database.
//
// # Host assembly
//
// New requires Core, Inputs, a protected Store, a stable Namespace, a positive
// MaxCheckpointBytes, Authorize and Bind. Store owns durable CAS, total retained
// record/byte budgets, listing/retention and backup. This package does not ship a
// persistence engine or enforce a cross-process lease. The host must stop the old
// executor/native activity before starting another service on the same records.
// Separate namespaces must identify separate intent stores sharing an input Store.
//
// Bind verifies the persisted Target.DefinitionID/Revision against the current
// native account/root and returns a freshly captured Core.Selection. It must
// coordinate configuration activation and capture rather than inspect a definition
// and later capture an unrelated replacement. Selection then protects execution
// against subsequent rebinding. Credentials and refresh revisions are not stored
// in intent; native factories/providers still load their current secret state.
// Target.Address retains tree/key identity. Old JSON Target.Path records decode
// as tree addresses; their legacy generation hash uses the original field order.
// Store every new field and stop old writers before switching versions.
//
// Authorize protects intent creation/observation/execution/abort and use of the
// referenced input; input IDs alone grant no byte permission. It receives a copy
// of the private record and may consult the input owner/policy. Core separately
// reauthorizes target mutation as the fixed initiating Owner on every execution.
// Status returns intent metadata, including the accumulated CheckpointBytes;
// it neither loads nor exposes native recovery bytes. Store.Checkpoint reads those
// protected bytes only for the exact revision being resumed. Existing HTTP fs.write
// JSON does not carry native tokens or expose durable transfer resources.
//
// # Intent and resource lifetime
//
// Create(ctx, principal, id, Request) uses a caller-chosen idempotency ID. It first
// persists Preparing with the sealed source version, establishes an input Hold,
// then persists Ready. A failed/uncertain response may leave a visible preparation;
// retry the same ID and request, or Abort it. It never creates a replacement ID
// implicitly. Run can finish this handoff after a restart. Input Release cannot
// delete an established durable hold; a race before Hold can instead fail source
// admission without publishing any target. The preparation is still discoverable.
// Request.Policy follows Core.Write: zero creates exclusively and may leave
// incomplete content, while explicit Publish requires complete publication.
// The exact policy is persisted, never strengthened on retry. Existing stored
// records without Policy or with legacy Replace keep their original complete
// publication meaning; resubmitting an ID requires the same stored policy.
//
// Run and Abort load protected intent and acquire a process-local per-ID owner.
// Duplicate execution returns ErrBusy. They retain no worker, Mount/use, native
// request or open source between calls. Submit an ordinary task.Work whose Run
// calls this Run/Abort to compose the existing bounded queue. Accepted closures
// themselves do not survive process death; the host selects pending durable IDs
// and submits fresh work after restoring configuration.
//
// Core.Write with WriteOptions.Continuation performs the actual upload/cleanup.
// Its source is immutable and replayable. Continuation.Append atomically persists
// new native records with the next Store revision, without cloning/rewriting the
// accumulated recovery log. Load returns metadata; Checkpoint returns the log only
// during recovery. MaxCheckpointBytes bounds total retained log bytes per intent,
// not a single append. Storage quota exhaustion preserves the intent/source for
// explicit capacity adjustment and continuation. An uncertain append stops further
// appends in that execution; a new Run reloads the committed revision. Cancellation pauses; it does not remove native
// sessions or durable source holds. Abort explicitly coordinates native cleanup
// and does not undo committed content. Completed/Aborted is persisted before
// Unhold. A retry of terminal Run/Abort finishes retention cleanup idempotently.
// Source readers and runtime references close before the durable hold is released.
//
// Errors preserve file effects. Ready is nonterminal, not proof that publication
// never happened: inspect Change and the Driver's protected checkpoint when an
// attempt is unresolved. Raw causes are not persisted as public diagnostic text.
// Unknown initiation/publication/cleanup remains owned until actual native proof
// permits a transition; there is no automatic path-based compensation, reset or
// invented exactly-once guarantee. A store failure can itself be uncertain; reload
// before retry rather than inferring rollback. Native checkpoint proof and manual
// recovery tools remain specific to the selected Driver/host.
package transfer
