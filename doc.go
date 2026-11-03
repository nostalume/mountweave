// Package core provides authorized file management over prepared storage mounts.
//
// Call Stat, List, or Open with an explicit Principal. Close every returned content
// Body. The creator closes a rejected Driver; Install transfers ownership only on
// success. The embedding application eventually calls Core.Close with its chosen
// wait context. Drivers keep physical paths, provider IDs and temporary URLs private.
//
// ListRequest.CursorTTL supplies an absolute lifetime on the first enumeration
// request. A continuation inherits it when zero, or must match it when supplied.
// Stat and Open need no enumeration policy. MaxCursors and OpenTimeout are optional
// runtime policies. Hosts choose values; there are no benchmark defaults.
// Continuations bind principal, path, mount generation and limit. The latest
// continuation response is replayable until advancement; competing advancement
// returns 429 instead of queuing. ReleaseCursor explicitly releases one enumeration.
// Cursors do not survive process restart.
//
// Mount.Retire rejects new uses, invalidates enumerations and requests cancellation.
// Its context limits the wait, not unfinished work. Cleanup failures preserve the
// cause and owned resource; no automatic replacement or retry is claimed.
//
// # Directory aliases
//
// BindAlias(ctx, prefix, mount, base) exposes an existing mount-relative directory
// under a fixed prefix. Ordinary mount entries and aliases share namespace lookup.
// All entry prefixes must be unique and non-nested. Targets are real installed
// Mount handles, so aliases cannot chain or merge storage. The base is a path
// mapping, not a pinned directory-object version.
//
// An alias owns no Driver, connection or credentials. Every action checks both its
// visible path and corresponding primary path through the same Authorizer.
// Operations reject alias roots just like mount roots. Explicit aliases share
// mount revisions and retirement; equivalent resolved Copy/Move endpoints are
// rejected. No implicit physical-alias discovery across independent mounts is
// promised.
//
// UnbindAlias removes the entry and invalidates its cursors, waiting for their
// cleanup within the supplied context. Already opened bodies retain their original
// mount use. A replacement mount requires explicit rebinding; queued work captured
// on an old binding cannot silently switch to the replacement. Hosts own alias
// persistence and control-plane authorization, just as they own prepared Install.
//
// # Optional mutations
//
// Core.Mkdir and default Core.Write/Core.Create require CreateAction on the target;
// explicit replacement requires WriteAction, which permits creation or replacement
// at that name. Core.Remove requires RemoveAction. Policies granting only StatAction, ListAction and
// ReadAction remain read-only. An authorizer that ignores the action and returns
// nil for everything is a wildcard grant.
//
// DirectoryMaker, Remover, FileCreator and FilePublisher are independent optional interfaces.
// The required Driver and Iterator interfaces are unchanged. Unsupported mounts
// return UnsupportedOperation with Unchanged before target effects or input reads.
// There is no second writer runtime, permission table or provider registry.
//
// Create borrows an io.Reader until it returns. The caller retains ownership and
// closes owned input afterward. A nil length permits unknown input length; a
// nonnegative length describes the entire input, including normal EOF. Short,
// overlong and failed input cannot report successful completion. The operation
// does not seek, replay or close the input. A blocking reader may keep retirement
// unfinished even after cancellation.
//
// Change.Effect is independent of error: Unknown (the zero value), Unchanged,
// Applied or Incomplete. Check both. Confirmed effects survive cancellation and
// cleanup failure; do not automatically retry or delete a failed target by path.
//
// Local creates one directory with requested mode 0700, removes one entry without
// recursion, and creates new files with O_CREATE|O_EXCL and requested mode 0600.
// The operating system's permission rules still apply. Existing contents are never
// truncated. A final symbolic link is removed itself, not its target.
//
// Default new-file creation may be visible while being written, and failed input
// can leave an incomplete file. This mode has no complete-publication or append
// guarantee. Explicit complete publication is described below. Failed native cleanup
// remains owned by the Driver and prevents successful retirement.
//
// A possible mutation invalidates existing cursors for that mount before its
// result returns, including EOF replay. Other mounts and opened content are not
// invalidated by this coordination. Already-admitted reads may finish with an
// earlier observation; enumeration is not a snapshot.
//
// The HTTP mutation routes are:
//
// | Method and path | Result |
// | --- | --- |
// | POST /api/v1/fs/directories?path=... | Create one directory; no request body |
// | DELETE /api/v1/fs/entry?path=... | Remove one entry without recursion; no body |
// | PUT /api/v1/fs/content?path=... | Create a file from raw request bytes |
//
// Success returns JSON with effect applied (201 for creation, 200 for removal).
// Handled mutation errors add effect to redacted Problem Details. Unsupported
// mounts return 501; existing targets and nonempty directories return 409.
// PUT optionally accepts If-None-Match: *; a proven unchanged conflict then returns
// 412. The condition uses native exclusion, not a metadata preflight.
// Other mutation conditions/range headers and non-identity Content-Encoding are
// rejected. Known Content-Length is exact; chunked input runs to normal EOF.
// A disconnected client may receive no receipt and must treat its outcome as
// uncertain. A later conflict does not prove an earlier lost request had no effect.
//
// The info response lists server operations, not per-mount support or permission.
// There is no capability-discovery or frontend product in this module.
//
// # Complete file publication
//
// Core.Write(ctx, principal, target, input, length, options...) is the common body
// write entry. Core.Create remains its default new-file shorthand. Supply at most
// one WriteOptions; Replace defaults to false. Explicit Replace creates a missing
// name or replaces an existing file only after complete input, including normal
// EOF and any declared length, has been validated. Input is never replayed or
// closed by the operation. Copy accepts the same options; each CopyRequest embeds
// WriteOptions for CopyBatch. CopyTree accepts it through DirectoryCopyOptions;
// Move retains its no-replacement rule.
// The HTTP PUT route continues to use default creation.
//
// FilePublisher is a narrow optional Driver capability, separate from FileCreator.
// Unsupported capability or known Local configuration/volume mismatch is refused
// before reading input. Preparation and input failures leave the target Unchanged.
// A confirmed native publication reports Applied even with a subsequent cancellation
// or cleanup error. Unconfirmed publication stays Unknown; partial-publication
// receipts violate the capability and are reported conservatively. There is no
// automatic retry, rollback, conditional version update or power-loss durability.
//
// Local enables publication with local.New(directory, local.Options{
// StagingDirectory: staging, MaxStagingBytes: budget}). Both settings are required
// together; the byte budget must be positive. The host supplies an existing staging
// directory outside the served root, keeps it private from other mounts, and does
// not move or manipulate its owned objects while the Driver is active. No user name
// is reserved or filtered. Each target's actual parent must share the staging
// filesystem; a mismatch refuses publication without a cross-device copy fallback.
// Missing configuration leaves default creation available and refuses replacement.
//
// Optional local.Options.MaxConcurrentCommits limits concurrent native complete-file
// publication commits within one Local instance. Zero adds no limit; negative values
// are invalid. It is independent of batch concurrency and staging configuration.
// Transfers remain parallel, and waiting calls retain their staging reservations,
// captured parents and runtime uses. Waiting honors cancellation and retirement;
// the permit is released after the native call, before temporary-file cleanup.
// The limit does not cover default creation, Move or Remove, and supplies no FIFO
// ordering, cross-instance limit or transaction isolation. Choose it for the host's
// filesystem workload; no platform-specific limit is imposed.
//
// The budget belongs to one Local instance across all its calls. Known lengths
// reserve their full logical byte count before reading input; unknown lengths
// reserve each write before growing the temporary file. Exhaustion fails with
// CapacityExceeded rather than queueing. Cleanup of unpublished data must finish
// before its reservation is released; failed cleanup remains owned and charged.
// Published data leaves this temporary budget, while cleanup errors still prevent
// successful retirement. This is not a physical-block, total-storage, file-count or
// cross-instance quota. It excludes old opened objects, foreign files and crash
// leftovers; restart cleanup and recovery belong to the host.
//
// Local performs one streaming write to private storage, then a parent-relative
// native rename: renameat on Linux and FileRenameInformationEx with replacement and
// POSIX semantics on Windows. It does not copy the temporary body back into target.
// See [Windows rename semantics](https://learn.microsoft.com/en-us/windows-hardware/drivers/ddi/ntifs/ns-ntifs-_file_rename_information).
// Observed directories, links and spelling mismatches are refused. Final links
// introduced by a race are never followed; compatible name replacement may race.
// There is no content snapshot, object-ID/ACL preservation or external-mutation
// isolation. Already opened readers retain their old object.
//
// A write retains its captured mount and parent through transfer, publication and
// cleanup. If the parent is moved meanwhile, the bound original directory remains
// the destination; new requests resolve the current namespace independently.
// Alias rebinding never redirects started work, and queued work rechecks its binding.
// Cancellation requests stop; the call still waits for borrowed input and cleanup
// acknowledgments. Cancellation can race with native publication, so inspect both
// Change and error.
//
// Existing batch conflict ranges cover source reads and target writes for the whole
// operation. They preserve call-local order and the started result prefix. Separate
// calls have no shared path lock: writes, removes and parent moves may compete.
// A confirmed effect does not promise the current path still holds that result.
// Concurrency defaults to one. With W in-flight publications, stream buffers grow
// with W, and unpublished bytes are additionally bounded by the configured instance
// budget. Native filesystem work can reject publication after transfer; there is
// no truncation, copy/delete or delayed-success fallback.
//
// # Copy files
//
// Core.Copy(ctx, principal, source, target, options...) writes the complete source
// body to a new target by default. It uses ReadAction and CreateAction, without
// requiring Stat or List grants. Explicit WriteOptions{Replace: true} instead uses
// WriteAction and the same complete-publication path as Core.Write. The target
// parent must exist; source and target paths must differ. Distinct paths within one
// mount or across mounts are supported. Copy does not remove source or copy metadata.
//
// Copy borrows the opened Body and Length to the common write path, waits for that
// call to return, then closes the source and preserves observed cancellation.
// Check both Change.Effect and error. Default copies may leave incomplete targets;
// explicit replacement follows the complete-publication contract above. Source
// opening can independently refresh credentials. Neither mode promises a source
// snapshot, hard native-I/O cancellation deadline or durable job. Copy is included
// in the root test command and needs no additional module.
//
// # Rename and move entries
//
// Core.Move(ctx, principal, source, target) unifies renaming, moving and changing
// both parent and name. It requires RemoveAction at source and CreateAction at
// target. MoveResult contains Source and Target changes: confirmed source-name
// removal and complete target creation. Both must be Applied and error must be nil
// for completion without errors. Applied can coexist with cancellation or cleanup
// errors; inspect receipts and error together and do not automatically replay.
//
// Currently both paths must select the same live mount with the optional Relocator
// port. Its native no-replace receipt maps to both endpoints; a possible effect
// invalidates that mount's cursors once. Unchanged means this call did not modify
// the endpoint, not that another caller could not have changed it. Unknown is not
// resolved by checking the current path. Cross-mount movement is refused before
// creating a target: ordinary Open and Remove do not bind deletion to the object
// and content state read by this call. Move never falls back to copying and then
// deleting an unbound path.
//
// Directory moves relocate one entry together with its children, without subtree
// enumeration or content copying. Source RemoveAction and target CreateAction
// authorize that whole relocation; descendant grants are not checked individually.
// Mount and alias roots, equivalent resolved endpoints and targets inside the
// source path are rejected. Aliases retain their configured paths after a move.
//
// Local admits regular files and directories with existing target parents, refusing
// observed final symbolic links and special objects. Name and type checks are
// observations, not object/version/type conditions against external replacement.
// Windows opens the source with DELETE access relative to its rooted parent and
// uses the target directory handle; Linux amd64 uses renameat2 with
// RENAME_NOREPLACE and rooted parent descriptors. Other native platform/filesystem
// combinations return UnsupportedOperation when this guarantee is unavailable.
// Failed owned-handle cleanup remains with the Driver. Cross-volume fallback and
// power-loss durability are not promised.
//
// # Batch file operations
//
// Core.RemoveBatch(ctx, principal, targets, options...), Core.CopyBatch(ctx,
// principal, requests, options...) and Core.MoveBatch(ctx, principal, requests,
// options...) retain input order and duplicates. MoveBatch takes []MoveRequest
// with Source and Target paths. Supply at most one ExecutionOptions. Concurrency
// zero or one is serial; negative values are invalid. Existing calls without options preserve serial behavior.
//
// Explicit concurrency uses a shared, call-scoped executor. It resolves known
// aliases to mount generations and relative paths, and preserves read/write
// conflicts on equal or ancestor paths. Copy includes its source read and target
// write; Remove includes its target write; Move includes both endpoint writes.
// A blocked next item is not bypassed. Unknown locations act as conservative
// barriers. This orders known namespace
// conflicts within the call, without providing cross-request locks, physical-alias
// discovery or protection from external writers.
//
// RemoveBatch and CopyBatch return []ItemResult with Change and Err; MoveBatch
// returns []MoveItemResult embedding MoveResult (Source and Target) plus Err.
// Each result slice is the started input prefix, in input order. Captured endpoint
// bindings are checked again at admission, so queued work cannot switch to a
// recreated alias or replacement mount. The first observed item error stops
// dispatch, including Applied with an error. With concurrency, later items may already have started;
// all in-flight items finish under their original contexts and keep their actual
// receipts. User cancellation requests cancellation of native work. The call waits
// for its resource handoffs before returning, without a hard native-I/O deadline.
// The overall error also retains cancellation observed between items or after the
// last success, without rewriting recorded item errors.
//
// Empty input performs no authorization or native work, but reports observed
// cancellation. Results grow with K attempted items, and executor state is bounded
// by the chosen concurrency. Calls do not retain input lists, retry, roll back or
// create durable jobs.
//
// # Directory copy
//
// Core.CopyTree(ctx, principal, source, target, TreeOptions, directoryOptions...)
// copies one real source directory into its corresponding target directory, whose
// parent must exist. It uses ListAction on source directories and ReadAction on
// copied files. Target directories and default file creation use CreateAction;
// explicit file publication uses WriteAction. The default requires new target names.
// It preserves empty directories without metadata preservation or source deletion.
//
// Supply at most one DirectoryCopyOptions. Explicit MergeDirectories: true reuses
// existing directories and creates missing ones. Only an unchanged existence
// conflict followed by successful no-follow directory confirmation and cleanup is
// accepted. Reused directories report Unchanged with no error; this receipt does
// not summarize their descendants. File names still conflict by default, and type
// collisions fail; extra target entries are retained. Ordinary Mkdir keeps its rule.
// Merge requires DirectoryReader on the target, checked before mutation; native
// confirmation can still fail later after earlier directory effects. Confirmation
// uses CreateAction without additional Stat/List grants or consuming the returned
// target iterator. Native path resolution remains driver-specific.
//
// DirectoryCopyOptions embeds WriteOptions independently of MergeDirectories.
// Explicit WriteOptions{Replace: true} publishes each complete file at its target
// name, creating missing files or replacing existing ones. It uses WriteAction on
// files and requires FilePublisher rather than FileCreator. Directories still use
// CreateAction and do not merge unless MergeDirectories is explicit. A new tree
// can also select complete file publication without directory merging.
//
// The default retains ordinary Create semantics, including possible Incomplete
// effects on failure. Explicit publication reuses the single-file contract and
// Local staging budget/commit capacity. Failed input preserves that file's old
// target; confirmed earlier files and directories remain. This is not a whole-tree
// transaction. Known missing interfaces are refused before target mutation;
// driver configuration, per-file authorization and native submission failures can
// still occur after earlier effects. Input is not replayed, and publication never
// falls back to default creation. Inspect per-item Change and error on failure.
// The target tree is resolved by name per operation, without a physical-tree pin or
// transaction. Observed target links/redirects are refused during confirmation;
// external replacements afterward can change later path resolution. Confirmation
// does not supply parent-relative writes or a lasting link-free guarantee.
//
// TreeOptions embeds ExecutionOptions and requires positive MaxEntries and
// MaxPlanBytes chosen by the caller. The root counts as an entry; the byte budget
// counts relative-path UTF-8 bytes in the plan, not content or native responses.
// Source scans also bound empty nonterminal advances with MaxEntries. Budget
// exhaustion is an error, never successful truncation. TreeCopyOptions remains a
// type alias for existing callers.
//
// Discovery completes and closes all enumerations before target mutation.
// Directories are created parent-first and serially. File groups prepare bounded
// batches of at most W source directory scopes, each with a completed membership
// scan, then share one executor budget of at most W file operations. W is the
// requested concurrency, defaulting to one. Dispatch and results retain contiguous
// group order; adjacent groups can overlap without a worker pool per directory.
// Scopes close only after all started bodies finish. Membership scans retain their
// per-directory budgets; explicit concurrency can retain up to W directories'
// lookup state. A preparation failure leaves that batch unstarted; earlier effects
// remain. File bodies retain the existing Open/write ownership, length and cleanup
// contracts. Core adds no extra content staging or shared directory cache; explicit
// Local publication uses the private storage described above.
//
// DirectoryReader is optional and leaves the required Driver unchanged.
// DirectoryScope reuses Iterator and adds OpenFile for an observed child name,
// available after normal enumeration EOF. OpenFile may run concurrently; Next has
// one caller, and all returned bodies must finish before Close. Source scopes
// reject native links/redirects at actual opens. Local supports this on Windows
// and Linux; Windows reparse points are refused, and unsupported platforms refuse
// before target mutation. S3 and alipan also provide source scopes. Destination
// capabilities remain independent: Local and Pool supply directory creation; see each Driver for its capabilities.
//
// TreeItemResult reports RelativePath ("/" for the copied root), Change and Err
// in plan order: directories first, then file groups. Discovery, budget or
// discovery-close failure leaves the target untouched. Execution or later source
// preparation failure can leave a partial target; inspect both returned rows and
// the overall error. Confirmed effects survive cancellation or cleanup faults.
// Only discovered entries are included, without a directory/content snapshot,
// stable versions or a promise about undeclared physical overlap. Resolved
// same-mount source/target containment is refused.
//
// # Directory removal
//
// Core.RemoveTree(ctx, principal, target, TreeOptions, DirectoryRemoveOptions...)
// defaults to Discovered: remove one real directory using a finite discovery plan.
// Core.Remove remains nonrecursive. Mount, alias
// and virtual roots are refused. The positive entry/path budgets have the same
// meaning as CopyTree. Discovery, name validation, known capability checks,
// authorization preflight and enumeration cleanup finish before any deletion.
//
// Discovery requires ListAction on directories and RemoveAction on every planned
// entry, including the root. Per-entry discovery authorization does not register
// a runtime mount use and does not reserve future permission. Each deletion is
// admitted again against the captured binding and borrows its own runtime use.
// It requires no content ReadAction and does not enumerate the target's
// outside parent. Discovery accepts ordinary files and directories; observed links,
// redirects or special objects are refused before effects.
//
// Selection means names, not discovered object versions. A compatible same-name
// replacement can be removed, including a file whose content has changed. Unlisted
// new names are not chased. Nonempty directories return DirectoryNotEmpty; missing
// planned entries return Unchanged with NotFound and stop execution. An execution
// parent is opened without following redirects and then remains bound for its
// group. Final links are never followed; a non-directory unlink can remove the
// link itself. External renames are not transactionally isolated.
//
// Parent groups execute deepest-first, retaining discovery order within each depth
// and group. Children finish and their parent scope closes before that directory
// can be removed; the target root is last. Within each depth, batches prepare at
// most W parent scopes and share one executor budget of W in-flight deletions,
// using the same ExecutionOptions and defaulting to serial execution. Dispatch and
// results keep contiguous group order; adjacent independent parents can overlap.
// A preparation failure leaves that batch unstarted and closes its prepared scopes.
// All started items finish and all batch scopes close before another batch starts.
// There is no worker pool per directory or global lock.
//
// The optional ChildRemover capability belongs to DirectoryScope. RemoveChild takes
// one child name and File (non-directory unlink) or Directory (empty-directory
// removal only). It needs no enumeration first. Calls can overlap each other, but
// not Next, OpenFile or Close. Local implements it with parent-relative unlinkat
// on Linux and parent-relative DELETE handles with POSIX disposition on Windows.
// A successful Windows delete-handle close confirms namespace removal; pending
// deletion alone is not reported as Applied. Unsupported native modes have no
// recursive or delayed-success fallback. S3 and Alipan read scopes remain read-only;
// their default Discovered removal is unsupported. Pool supplies ChildRemover.
//
// Explicit DirectoryRemoveOptions{Scope: Subtree} requires RemoveTreeAction on
// the target, including its physical alias target. That grant authorizes subtree
// traversal and deletion, including newly added descendants; an entry's ordinary
// RemoveAction alone is insufficient. The optional Driver SubtreeRemover supplies
// native deletion with one root receipt. Without it, Core reuses the finite planner
// and executor, renewing the root subtree grant for client steps instead of
// requiring child ListAction/RemoveAction. Native execution need not enumerate
// descendants or return invented per-child results. TreeOptions bounds client
// discovery/planning, not total native affected entries or provider concurrency.
// S3 uses finite key discovery and batches of at most 1,000, removes its marker
// after acknowledged child batches, then observes prefix absence. New remaining
// keys return DirectoryNotEmpty with retained effects. Alipan captures a directory
// ID and waits for synchronous confirmation or native deletion-task success.
// Task acceptance alone is not Applied. Canceling a native wait may leave remote
// deletion running and reports Unknown; it does not undo deletion. No strategy
// automatically broadens scope, continually chases additions or prevents later
// recreation. Frontend confirmation belongs to the caller.
//
// RemoveTree returns []TreeItemResult in started execution order, with "/" for the
// root. Every item keeps its Change and Err; cancellation, preparation and cleanup
// errors are also retained by the overall error. Already confirmed effects are not
// rolled back or rewritten. Applied describes this operation's name removal, not
// storage reclamation or protection against later recreation. Failed cleanup stays
// owned by its original scope/Driver. There are no retries, durable jobs or
// cross-mount Move guarantees.
//
// Captured access routes preserve the alias used by the caller and the installed
// generation's rooted address together. Read/write composition and owned directory
// scopes retain that same location; a retired route never resolves to a replacement.
// Each child operation renews its current grant, including the primary path grant
// for aliases. Returned entry paths project from the admitted access route. These
// captured addresses do not identify immutable file content or persistent objects.
package core
