// Package httpapi projects file, mount-control and task contracts onto HTTP.
//
// # Optional HTTP adapter
//
// httpapi.New(core, options) returns an embeddable
// http.Handler. Existing three-argument construction keeps file delivery available.
// Optional httpapi.Options injects a control.Controller, task.Queue, task kinds,
// task access policy, record capacity, JSON input limit and native-config projection.
// All components, servers, listeners and storage remain host-owned. The root module
// uses only the standard library and provides no executable or configuration loader.
//
// The OpenAPI specification at api/openapi.json in the repository root defines the wire contract. All routes
// share bearer verification, request IDs, no-store responses and redacted problems.
// Identity verification does not grant file, management or task access. Information
// and task-kind advertisements are hints, not mount capabilities or permissions.
//
// | Route | Delivery |
// | --- | --- |
// | GET /api/v1/info | Operations, optional components, kinds and admission limits |
// | GET /api/v1/fs/entry, GET /api/v1/fs/entries | Metadata and bounded cursor pages |
// | GET/HEAD/PUT /api/v1/fs/content | Stream content; PUT defaults to create-only |
// | POST /api/v1/fs/directories, DELETE /api/v1/fs/entry | Existing single directory creation/removal |
// | POST /api/v1/fs/operations | Typed synchronous file operation |
// | GET /api/v1/mounts | Desired summaries filtered by management authority |
// | GET/PUT /api/v1/mounts/definition?id=... | Read/save desired input at an expected revision |
// | GET/POST /api/v1/mounts/activation?id=... | Observe/apply a selected revision |
// | POST /api/v1/mounts/stop?id=..., POST /api/v1/mounts/restore | Explicit stop/restore |
// | GET/POST /api/v1/tasks | Authorized retained summaries / task admission |
// | GET /api/v1/tasks/task?id=..., POST /api/v1/tasks/cancel?id=... | Observe / request cancellation |
// | GET/POST/DELETE /api/v1/inputs | Optional durable input listing/status, creation and release |
// | PUT /api/v1/inputs/content?id=...&offset=... | Length-delimited append or exact block retry |
// | POST /api/v1/inputs/seal?id=...&offset=... | Seal one immutable input version |
//
// Synchronous operations and queued file tasks accept the same application/json
// envelope. For example, send this body to either /fs/operations or /tasks under
// the /api/v1 prefix:
//
//	{
//	  "kind": "fs.copy_tree",
//	  "input": {
//	    "source": "/source/docs",
//	    "target": "/target/docs",
//	    "max_entries": 1000,
//	    "max_plan_bytes": 65536,
//	    "concurrency": 2,
//	    "existing": "merge",
//	    "replace": false
//	  }
//	}
//
// These example budgets are caller choices. The built-in kinds are fs.copy,
// fs.move, fs.remove, fs.mkdir, fs.copy_batch, fs.move_batch, fs.remove_batch,
// fs.copy_tree and fs.remove_tree. Syntax and participating bindings are selected before
// execution; refusals there have no file effects. Both modes execute the same
// prepared function through the ordinary Core API, preserving stop/prefix, links,
// explicit replacement/merge and partial-effect contracts. A stale queued binding
// fails instead of targeting a replacement. Move retains its same-mount restriction.
// fs.remove_tree accepts scope: discovered (default) or subtree. Empty also selects
// discovered. Subtree requires Core RemoveTreeAction, may affect new descendants
// and may return only a root receipt for native execution. Planning budgets limit
// client work rather than native affected entries. Synchronous and queued requests
// carry the same scope; presentation confirmation remains caller-owned.
// copy_tree accepts a virtual source spanning captured mounts and aliases.
// remove_tree accepts contents_only to preserve the requested root and namespace
// anchors while removing their selected mutable contents, across the same visible
// hierarchy. It is independent of scope. New bindings never join queued work;
// execution still renews authority and borrows the captured live generation.
// copy_tree also accepts file leaves, depth self/recursive, source_selection and
// policy for individual file writes. Existing create_new/merge/replace selects
// new names, retained directory membership or whole-target replacement. Legacy
// merge_directories/replace flags are boundary mappings; conflicting explicit
// policies are rejected. Directory traversal needs positive budgets; file leaves
// can use zero. remove_tree accepts when for a file root, never for every child.
// move options.existing=replace uses options.tree to bound target removal.
// Tree FileResult carries effect/before and item side/phase/error_side. Move adds
// target_removed_effect and its removal rows; MoveBatch retains nested removed
// receipts per item. Sync and queued delivery use these same operation results.
//
// Optional Options.Inputs borrows an independent input.Store and requires
// AuthorizeInput for create/list/read/append/seal/release/use. A permitted input
// does not grant target file access. Its host-selected limits appear in /info.
// fs.write accepts {path, input_id, replace?} through the same operation/task
// envelope. Prepare pins a sealed source and captures the target binding; Run
// reauthorizes source use, opens its body under execution lifetime and calls
// Core.Write. Cleanup releases the reference on synchronous completion, queue
// rejection, cancellation-before-run and ordinary task completion. Release may
// precede accepted execution without invalidating its source. File effects and
// cleanup errors remain separate. Handler never closes the borrowed input Store.
// Stop HTTP admission, close this adapter/queue, then the input Store and Core.
// Inputs survive process restart; the process-local task queue does not.
// Synchronous requests call Core directly under request lifetime; their operations
// do not participate in queue conflict ordering. A host needing shared scheduling
// can submit FileTaskKinds work and wait, explicitly linking request cancellation
// when execution should remain request-bound.
//
// Synchronous success returns FileResult; a failure problem retains any known
// receipt in result. Move has source_effect and target_effect; batch/tree receipts
// have ordered items, stable error codes and relative paths where applicable.
// Unstarted suffix items are absent; empty item lists are omitted. Effects do not
// disappear when an error accompanies them. Structured mutations reject conditional
// and range headers rather than evaluating a racy metadata precondition.
//
// PUT content?path=...&replace=true streams into Core.Write's complete-publication
// mode, requiring write authority and native FilePublisher support. It creates if
// absent; ordinary PUT still conflicts with existing content. Combining replacement
// with If-None-Match selects the same complete conditional policy. If-None-Match:*
// without replace=true retains streaming exclusive creation. Other accepted tag
// conditions select complete publication; explicit replace=false rejects that
// conflicting policy. If-Match/If-None-Match parsing and native condition results
// are shared with WebDAV. Unsupported predicates return 501; actual failures use
// 412, or 304 for negative GET/HEAD. Date predicates remain unsupported except
// when superseded by the corresponding tag header. No guessed validators or
// metadata preflight precede GET. File content is not buffered into task requests.
// Copy input optionally carries CopyOptions; Move options and Remove when feed
// the same Core operations in synchronous and queued delivery. Tree requests do
// not broadcast a file condition. Structured operation headers remain refused.
// Optional MaxJSONBytes limits structured requests only; zero adds no adapter limit.
//
// When configuring task delivery, provide a positive MaxTasks and AuthorizeTask.
// Queue concurrency/capacity and Core per-call W remain separate. File keys can use
// httpapi.FileTaskConflicts; compose it with host policies for other key types.
// Generic task runtime remains independent of this adapter:
//
//	handler, err := httpapi.New(files, httpapi.Options{
//	    Authenticate: verify,
//	    CursorTTL: cursorTTL,
//	    Control: manager,
//	    Queue: jobs,
//	    MaxTasks: retainedTaskLimit,
//	    MaxJSONBytes: requestLimit,
//	    AuthorizeTask: taskPolicy,
//	    TaskKinds: hostKinds,
//	})
//
// POST tasks reserves record capacity before preparing/admitting work. Accepted
// work returns 202 plus TaskInfo and Location, independently of request disconnect.
// GET task returns TaskView: state, latest progress, result, error, cleanup_error
// and completion_error. A failed task is still a successful observation response.
// Cancel returns an acknowledgement of the request, not an absence-of-effects claim.
// Task access checks distinguish submit/list/read/cancel and retain the original
// principal; file calls still reauthorize through Core at execution.
//
// The HTTP index is bounded by MaxTasks, including preparation reservations and
// active/completed records. New admission may evict the oldest accepted completed
// record; active records are never evicted. Missing/evicted IDs return 404. Lists
// return authorized summaries without copying large results. This is process-local
// result retention, not a durable queue or automatic replay policy. If the client
// misses an acceptance response, it must not infer that no work was accepted.
//
// Host TaskKinds supply Prepare and Project. Prepare decodes its own native input,
// defers business effects to Run and owns any partial preparation failure. After
// successful Submit, the queue owns Work.Cleanup; after rejected admission this
// adapter calls it once. Failed cleanup stays owned, blocks further task admission
// and is reported by Handler.Close. Project supplies safe JSON progress/results;
// projection failure does not replay or change the task. Built-in file kinds cannot
// be replaced by a host kind, keeping synchronous and queued file behavior aligned.
//
// Control.Load/List enforce management policy before definitions reach HTTP. Save
// returns safe confirmation fields; listing returns safe summaries. Individual
// reads omit native config and secret references unless ProjectConfig deliberately
// provides a safe native-config view. No generic field-name heuristic is used for
// opaque driver config. Activation serializes observations and stable errors, never
// Mount pointers or raw provider causes. Saving is separate from activation, and
// partial activation reports its actual root/alias facts.
//
// Optional JobSources exposes existing durable transfer/Pool intents, not another
// queue or database. TransferJobs requires a bounded metadata Lister; Pool's
// optional delivery package supplies its own authorized resource adapter.
// PUT /api/v1/jobs/job?source=uploads&id=one with {version:1,input:...} durably
// creates an intent without starting native work. GET the same URL for status;
// GET /api/v1/jobs?source=uploads&after=...&limit=... for a bounded metadata page.
// The page limit counts scanned entries, including hidden ones; empty items can
// carry next. Queries never load native checkpoints or probe member health.
//
// A JobRef contains source/id/generation, independent of checkpoint revisions and
// temporary TaskIDs. POST /api/v1/jobs/actions or POST /api/v1/tasks with the same
// {kind:"uploads.run",input:{id:"one",generation:"..."}} uses the same TaskKind
// and domain command. Upload also supports abort; Pool supports run (with positive
// limit) and forget. Generation is checked by the original owner before effects.
// Prepare only decodes; it neither loads the record nor borrows a body or Mount.
// Compose JobTaskConflicts and FileTaskConflicts in the existing queue policy.
// Transfer target accepts exactly one path or tagged address. Existing v1 file
// routes and tree target responses retain path; native key targets use address.
// They share the same Core.Write; prefix selectors are not upload targets.
// Transfer input accepts Policy or the legacy replace flag, never both. New
// durable intents retain the exact publication condition across Run and restart;
// checkpoints do not refresh it from current destination metadata.
// Sync actions remain request-bound and outside queue ordering, with original
// domain mutual exclusion. Pool run performs one bounded batch; task finished
// does not imply pass done. Host backoff/resubmission owns further scheduling.
//
// Job GET reports committed state; action/task result is an invocation receipt,
// including effects accompanying save errors. Transfer cleanup_pending means a
// terminal input-release confirmation remains owed; retry run/abort to finish it.
// Task cancellation requests this attempt stop; it does not abort/forget intent.
// Abort cannot undo published content; Pool forget cannot undo file effects or
// Drain policy. Source actions advertise syntax, never future authorization.
// After restart, restore original owners before choosing new execution attempts;
// accepted 202 does not promise durable automatic queue replay.
//
// For shutdown, stop server admission and call Handler.Close to drain HTTP handling;
// then close the queue, control, Core and finally host clients/storage. Handler.Close
// does not close injected components. A timeout only bounds waiting; retain owners
// and resolve outstanding work before declaring shutdown complete. There is no
// login UI, session issuer, frontend or additional protocol server in this module.
package httpapi
