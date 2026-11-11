// Package core provides authorized file management over prepared storage mounts.
//
// Call Stat, List, or Open with an explicit Principal. Close every returned content
// Body. The creator closes a rejected Driver; Install transfers ownership only on
// success. The embedding application eventually calls Core.Close with its chosen
// wait context. Drivers keep physical paths, provider IDs and temporary URLs private.
//
// # Addresses and queries
//
// All file operations accept Address: Path("/files/a") selects the tree view;
// Key("/cloud", "a//b") selects an exact key in that installed binding. Space is
// an existing mount/alias prefix, not a second registry or a credential owner.
// Constructors preserve bytes; admission validates their namespace. Authorization
// receives the full Address and must distinguish tree paths, keys and prefixes.
// Entry.Address retains that distinction; a key Entry.Name is the visible key.
//
// ListRequest.Query is Children(Path(...)) or Keys(space, prefix, delimiter).
// Query.After is a literal native start-after key, not a Core cursor. PrefixGroup
// entries carry Prefix addresses and are listing selectors, never directories or
// file mutation targets. Delimiter/ordering semantics belong to the Driver.
// Native keys do not pass through path normalization or tree ancestor checks.
// Tree-only drivers reject key addresses before consuming input or making effects.
//
// Drivers receive NativeAddress/NativeQuery after root projection. NamespaceSupport
// advertises key support; optional KeyProjection conservatively describes overlap
// between tree and key views without native I/O. Both views retain the same mount,
// budget, mutation revision, authorization and retirement owner. These addresses
// name execution-time resources, not immutable content or persistent object IDs.
//
// ListRequest.CursorTTL supplies an absolute lifetime on the first enumeration
// request. A continuation inherits it when zero, or must match it when supplied.
// Stat and Open need no enumeration policy. MaxCursors and OpenTimeout are optional
// runtime policies. Hosts choose values; there are no benchmark defaults.
// Continuations bind principal, the complete Query, mount generation and limit. The latest
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
// Tree mutations reject alias roots just like mount roots. Explicit aliases share
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
// Default new-file creation may be visible while being written, and failed input
// can leave an incomplete file. This mode has no complete-publication or append
// guarantee. Explicit complete publication is described below. Failed native cleanup
// remains owned by the Driver and prevents successful retirement.
//
// WriteOptions.Continuation optionally supplies protected native recovery records
// for an immutable Replayable input. It preserves the selected write policy:
// ordinary creation may report Incomplete; explicit Publish retains complete
// publication guarantees. Cancellation pauses the attempt, while Abort requests
// native cleanup without undoing published content. Durable input retention and
// intent persistence remain with the caller or optional transfer package.
//
// A possible mutation invalidates existing cursors for that mount before its
// result returns, including EOF replay. Other mounts and opened content are not
// invalidated by this coordination. Already-admitted reads may finish with an
// earlier observation; enumeration is not a snapshot.
//
// Protocol delivery belongs to the packages under protocols/. The JSON HTTP
// wire contract is maintained by protocols/httpapi and api/openapi.json.
//
// # Bound file reading
//
// OpenOptions{Mode: BoundRead} uses the same Core/Driver.Open and returns
// Content.File (*BoundFile), exclusively of Body. Its native port is ReadFile:
// ReadAt(context.Context, []byte, int64), Stat(context.Context) and Close(). Core
// Stat returns Entry; native Stat returns Metadata for the original opened name.
// Each logical call renews ReadAction or StatAction and checks its original
// binding generation, without resolving that name or borrowing another use.
//
// The opening context bounds construction only. Once adopted, File owns one use
// until Close or retirement. ReadAt follows io.ReaderAt byte/EOF rules; EOF and
// per-call cancellation do not close File. Calls on one File are serial; waits
// are cancelable, and different files are independent. Close needs no permission,
// stops admission, cancels active work and joins both native Close and calls.
// A failed Close keeps the original cleanup owner and prevents successful retirement.
// The host bounds retained files and concurrent callers; no global handle table
// or task queue is introduced. StreamRead/zero OpenOptions keeps ordinary Body
// and WriterTo, including its request lifetime and EOF cleanup.
//
// OpenOptions.Binding requests minimum strength; zero means BoundObject.
// Content.Binding reports the supplied strength. BoundObject fixes resource
// identity, allowing in-place content changes; PinnedVersion also fixes contents.
// Local holds its fd and refuses PinnedVersion. Pool holds its immutable generation
// pin and reuses the stripe decoder, returning window credits after every ReadAt.
// S3 requires a non-null VersionId obtained at open; each Range and Stat uses that
// version directly. External deletion fails instead of reopening the current key.
// Alipan refuses BoundRead until equivalent native lifetime can be established.
// Range is a stream selection and cannot be combined with a File mode; offsets
// belong to ReadAt. No HTTP handle route is added here.
//
// # Visible editing and receive order
//
// OpenOptions{Mode: LiveEdit} opens an existing file for WriteAction. Read: true
// additionally requires ReadAction before any opening effect. Creation selects
// OpenExisting (zero), OpenOrCreate or exclusive OpenNew; Truncate acts during
// Open. Content.Change retains known opening effects even when adoption or cleanup
// fails. This mode requires BoundObject and refuses immutable version selection.
//
// The same BoundFile provides WriteAt(ctx, bytes, offset), Append(ctx, bytes),
// Truncate(ctx, size) and Sync(ctx). Native Opened.File implements EditFile,
// selected once at adoption. Byte methods return n, Change and error independently;
// short writes retain their visible prefix. AppendOnly permits Append and refuses
// WriteAt, using native end-of-file writes rather than Stat followed by WriteAt.
// Sync explicitly requests native file durability, not a parent-name transaction.
// Close is cleanup, not publication. Local supplies Windows/Linux editing; object
// upload adapters refuse the mode without attempting a read/reupload emulation.
//
// Optional NewOrderScope(capacity) bounds active plus waiting operations. Reserve
// turns in trusted receive order before concurrent dispatch, then pass each
// turn.Context() to the same Core or File API. Defer turn.Cancel() for abandoned
// reservations. Internal compositions enter once, preserving existing finite-batch
// concurrency. A scoped File requires fresh turns of its opening scope. Stream
// bodies retain the turn through real completion; persistent Open releases it
// after handoff. No goroutine lock race is advertised as receive order.
//
// Unknown effects stop that scope; known partial effects do not. Stop cancels
// contexts but leaves native acknowledgments with their original owners. Close
// and ReleaseCursor bypass ordering so cleanup cannot wait behind itself. Each
// turn covers one Core operation, not an arbitrary protocol transaction; protocol
// request framing and waiting before a requested CLOSE remain host-owned.
// Other scopes, unscoped calls and external writes are independent. There are no
// new task records, worker pools, distributed locks or HTTP routes.
//
// # Complete file publication
//
// Core.Write(ctx, principal, target, input, length, options...) is the common body
// write entry. Core.Create remains its default new-file shorthand. Supply at most
// one WriteOptions. Its zero policy directly creates a new file and may leave
// partial new content on failure. Policy: Publish() creates or replaces a file;
// Publish(IfAbsent()) publishes exclusively. Both validate complete input,
// including normal EOF and any declared length, before publication. Input is never replayed or
// closed by the operation. CopyOptions separates Source ReadSelection from Target
// WriteOptions; each CopyRequest retains these options for CopyBatch.
// CopyTree accepts WriteOptions through DirectoryCopyOptions independently of
// whole-tree target policy. Move defaults to no replacement; explicit Replace
// records target removal separately from its final exclusive native relocation.
// ObserveTarget requests Change.Before for response classification. This is a
// native observation, not a CAS guarantee; ordinary publication need not add a
// HEAD for it. The exclusive predicate itself is enforced by native commit.
//
// FilePublisher is a narrow optional Driver capability, separate from FileCreator.
// Unsupported capability or known Local configuration/volume mismatch is refused
// before reading input. Preparation and input failures leave the target Unchanged.
// A confirmed native publication reports Applied even with a subsequent cancellation
// or cleanup error. Unconfirmed publication stays Unknown; partial-publication
// receipts violate the capability and are reported conservatively. There is no
// automatic retry, rollback or power-loss durability.
//
// Evidence keeps Validator comparison and immutable Version selection distinct.
// IfMatch is a strong comparison; IfNotMatch uses weak comparison. IfPresent and
// IfAbsent test presence. All combines at most one positive and one negative
// test, without an expression tree. Tag sets are canonical comparable values.
// Publish(When) enforces the predicate at the actual native commit. A failure
// reports PreconditionFailed and an Unchanged target; native private cleanup may
// still be owed. Unsupported conditions are not treated as failed comparisons.
// Local has no strong validator; S3 uses native ETags and supported SDK guards;
// Pool compares generations in its transaction. Ordinary reads add no hash/HEAD.
// OpenOptions holds Range and ReadSelection; Stat accepts an optional selection.
// Source copy conditions apply to the actual open, target conditions to commit.
// RemoveOptions and MoveOptions use these same predicates. RemoveOptions.Target
// constrains the native Remove effect to a non-directory name or empty directory.
// Single removal needs only Remover; tree operations reuse ChildRemover on their
// bound parents. Move keeps source and target evidence and receipts distinct.
// Tree policies do not broadcast one file validator over all members.
//
// Native staging configuration, byte budgets and commit limits belong to the
// Driver. See package github.com/nostalume/mountweave/drivers/local for the
// filesystem implementation's publication and cleanup requirements.
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
// requiring Stat or List grants. Explicit WriteOptions{Policy: Publish()} instead uses
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
// MoveOptions.Existing defaults to CreateNew. Replace first tries the same
// exclusive native relocation, then removes an existing target only on a proven
// unchanged conflict. MoveResult.Removed preserves that separately confirmed
// phase; the final relocation stays exclusive and never removes a later peer.
// Tree budgets must be positive if target removal encounters a directory.
//
// Same-mount moves use Relocator and map the native receipt to both endpoints.
// Same-mount directory moves transfer the directory entry without walking its
// children. NoCopy requires this path. Mount and alias roots, equivalent resolved
// endpoints and targets inside the source path are rejected.
//
// A cross-mount move requires an absent target. MoveSource ties the opened file
// version to a native conditional removal. The target publishes complete input
// exclusively before Core reauthorizes and removes that exact source. A failed
// or uncertain publication retains the source. An error can leave a confirmed
// target, so use the two endpoint receipts to decide what remains.
//
// Cross-mount directories require positive Tree budgets and source directory
// identities. MoveDirectoryWriter enforces target parent identities at creation
// and publication. Core discovers bounded names, checks descendant grants, and
// publishes a group of files before removing their source names. Source
// directories are then removed from the bottom up, only if still the same
// directory and empty. MoveResult.Tree retains the attempted phases. These
// receipts describe this operation's effects amid independent mutations.
//
// Native relocation guarantees and platform support belong to each Driver.
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
// copies a file leaf or selected directory into its corresponding target, whose
// parent must exist. It uses ListAction on source directories and ReadAction on
// copied files. Target directories and default file creation use CreateAction;
// explicit file publication uses WriteAction. The default requires new target names.
// It preserves empty directories without metadata preservation or source deletion.
//
// Supply at most one DirectoryCopyOptions. Existing: Merge reuses
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
// Existing: Replace removes all old target membership after bounded discovery
// and known admission checks, then creates the new root exclusively. It is not
// Merge: target-only names are removed, including type conflicts. Failed later
// work retains confirmed removal receipts without rollback. Source and old-target
// plans are bounded separately; both their plans/receipts contribute memory cost.
// Depth defaults to Recursive; Self creates only the directory shell. File leaves
// ignore depth, reuse no-follow Open and accept zero traversal budgets. Root kind
// observation adds no StatAction grant. Actual file reads retain Source conditions;
// directory roots refuse version/finite validator predicates before child effects.
//
// DirectoryCopyOptions embeds WriteOptions independently of Existing.
// Explicit WriteOptions{Policy: Publish()} publishes each complete file at its target
// name, creating missing files or replacing existing ones. It uses WriteAction on
// files and requires FilePublisher rather than FileCreator. Directories still use
// CreateAction and do not merge unless Existing: Merge is explicit. A new tree
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
// publication uses the Driver's configured private storage.
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
// TreeResult reports whole Change and ordered Items. Each TreeItemResult keeps
// RelativePath ("/" for the root), Side, Phase, ErrorSide, Change and Err. Target
// removal precedes creation, followed by file groups. Preparation/read/cleanup
// failures retain their provenance; successful cleanup has no separate row.
// Applied means the selected operation completed, not that every item changed.
// Discovery, budget or
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
// defaults to Discovered: remove a file leaf or directory using a finite name plan.
// Core.Remove remains nonrecursive. Mount, alias and virtual roots require
// ContentsOnly and are preserved by it. Positive directory budgets have the same
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
// one child name and the same RemoveOptions, including type and native conditions.
// It needs no enumeration first. Calls can overlap each other, but
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
// RemoveTree also accepts file leaves with zero traversal budgets, reusing Remove
// authority and its native predicate. DirectoryRemoveOptions.When is a file-root
// predicate; nonempty directory guards without commit proof are refused before
// child effects and never broadcast to descendants.
// RemoveTree returns TreeResult in started execution order, with "/" for the
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
