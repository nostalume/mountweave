// Package httpapi projects file, mount-control and task contracts onto HTTP.
//
// # Optional HTTP adapter
//
// httpapi.New(core, verifier, cursorTTL, options...) returns an embeddable
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
//	    "merge_directories": true,
//	    "replace": false
//	  }
//	}
//
// These example budgets are caller choices. The built-in kinds are fs.copy,
// fs.move, fs.remove, fs.mkdir, fs.copy_batch, fs.move_batch, fs.remove_batch,
// fs.copy_tree and fs.remove_tree. Syntax and all real bindings are selected before
// execution; refusals there have no file effects. Both modes execute the same
// prepared function through the ordinary Core API, preserving stop/prefix, links,
// explicit replacement/merge and partial-effect contracts. A stale queued binding
// fails instead of targeting a replacement. Move retains its same-mount restriction.
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
// with If-None-Match is invalid. File content is not buffered into a task request.
// Optional MaxJSONBytes limits structured requests only; zero adds no adapter limit.
//
// When configuring task delivery, provide a positive MaxTasks and AuthorizeTask.
// Queue concurrency/capacity and Core per-call W remain separate. File keys can use
// httpapi.FileTaskConflicts; compose it with host policies for other key types.
// Generic task runtime remains independent of this adapter:
//
//	handler, err := httpapi.New(files, verify, cursorTTL, httpapi.Options{
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
// For shutdown, stop server admission and call Handler.Close to drain HTTP handling;
// then close the queue, control, Core and finally host clients/storage. Handler.Close
// does not close injected components. A timeout only bounds waiting; retain owners
// and resolve outstanding work before declaring shutdown complete. There is no
// login UI, session issuer, frontend or additional protocol server in this module.
package httpapi
