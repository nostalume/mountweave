// Package task runs arbitrary process-local work with bounded admission and concurrency.
//
// # Optional generic tasks
//
// task.New takes positive Concurrency and Capacity. Work.Run is an arbitrary
// function/closure returning its own result and error. The task package imports
// only the standard library. File operations, configuration and other host work
// can use the same scheduler; the queue neither interprets paths nor chooses retry
// semantics. Submit's context controls admission only. Accepted tasks get their own
// context; Handle.Cancel and Queue.Close request cooperative cancellation. Wait's
// error bounds waiting; the Snapshot separately retains the task's Result and Err.
// Cancellation never proves absence of effects or rewrites a completed result.
//
// An optional pure Conflicts policy compares Work.Key values. Conflicting jobs keep
// submission order while independent jobs can overlap. Without a policy, jobs are
// independent. File tasks can use Core.Capture plus Selection.Context to constrain
// the ordinary API, with Selection.Conflicts as their policy:
//
//	jobs, err := task.New(task.Options{
//	    Concurrency: jobLimit, Capacity: admittedLimit, // Host-selected budgets.
//	    Conflicts: func(a, b any) bool {
//	        left, _ := a.(*core.Selection)
//	        right, _ := b.(*core.Selection)
//	        return left.Conflicts(right)
//	    },
//	})
//	if err != nil { return err }
//	selection, err := files.Capture(ctx,
//	    core.PathAccess{Path: source},
//	    core.PathAccess{Path: target, Write: true})
//	if err != nil { return err }
//	handle, err := jobs.Submit(ctx, task.Work{
//	    Key: selection,
//	    Run: func(run context.Context, report func(any)) (any, error) {
//	        report("copying")
//	        return files.Copy(selection.Context(run), principal, source, target)
//	    },
//	})
//
// Capture holds no runtime use and grants no permission. Normal Core admission
// reauthorizes, checks the captured binding and borrows resources atomically.
// Replacement or alias rebinding refuses old work instead of targeting a new
// instance. Descendant calls derive constraints from captured roots. Selection is
// process-local, does not freeze file content, and is not a durable request.
// Other task types need none of these file-specific facts. File job concurrency J
// and per-call Core concurrency W are separate budgets; account for J times W.
// Declared conflicts do not isolate direct calls, other queues/processes or unknown
// physical aliases. Core's existing default serial execution remains unchanged.
//
// Progress retains only the latest value. Result and progress values must be
// immutable after publication. A completed Handle owns its result; the queue does
// not index or retain completed history. The host chooses handle retention and
// durable codecs when needed; closures are not serialized. Optional Cleanup runs
// once after accepted work, including queued cancellation. A rejected Submit leaves
// ownership with the caller. Unresolved cleanup retains its owner and capacity slot.
// Optional Complete can save a final receipt; its failure is CompletionErr, separate
// from the operation's result, and does not replay the operation. It receives an
// uncancelled context and owns its storage timeout. Capacity includes finalization,
// so slow completion cannot create an unbounded save queue. Queue.Close cancels and
// waits for actual returns/cleanup/completion, reports cleanup failures, and leaves
// business/completion errors in their Handles. Run/cleanup/completion panics become
// task errors. Stop admission and finish tasks before retiring mounts and closing
// host-owned clients/storage. Process-crash recovery and retries require the actual
// task's input/effect contract; there is no universal automatic replay guarantee.
package task
