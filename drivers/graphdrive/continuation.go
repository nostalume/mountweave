package graphdrive

import (
	"context"
	"errors"
	"net/http"
	"strings"

	core "github.com/nostalume/mountweave"
)

// ContinueWrite uses the same native write and upload loop as Publish. The
// protected checkpoint consumer owns durable sessions; Cleanup never adopts them.
func (d *Driver) ContinueWrite(ctx context.Context, target core.NativeAddress, source core.Replayable, length int64, options core.WriteOptions) (core.Change, error) {
	unchanged := core.Change{Effect: core.Unchanged}
	if d.readOnly {
		return unchanged, core.Fail(core.UnsupportedOperation, nil)
	}
	if err := options.Policy.Validate(); err != nil {
		return unchanged, err
	}
	if target.Namespace() != core.TreeNamespace || options.Policy.Complete() && options.Policy.When() != core.IfAbsent() {
		return unchanged, core.Fail(core.UnsupportedOperation, nil)
	}
	c := options.Continuation
	if source == nil || length < 0 || c == nil || c.Append == nil {
		return unchanged, core.Fail(core.InvalidRequest, nil)
	}
	version := source.SourceVersion()
	if version == "" {
		return unchanged, core.Fail(core.InvalidRequest, nil)
	}
	if err := d.admit(ctx, target.Value()); err != nil {
		return unchanged, err
	}
	if target.Value() == "/" {
		return unchanged, core.Fail(core.InvalidRequest, nil)
	}
	cut := strings.LastIndexByte(target.Value(), '/')
	binding := writeBinding{Endpoint: d.opts.Endpoint, Drive: d.opts.DriveID, Root: d.root.ID, Name: target.Value()[cut+1:], Target: target.Value(), Source: version, Length: length, Policy: options.Policy}
	state, err := restoreWrite(c.Checkpoint, binding)
	if err != nil {
		return unchanged, err
	}
	// Historical receipts survive later renaming, deletion and resource tuning.
	// The Core has already performed current authorization and mount admission.
	if state.Done != nil {
		return *state.Done, nil
	}
	if state.Aborted {
		if c.Abort {
			return unchanged, nil
		}
		return unchanged, recoveryError("native write was aborted")
	}
	if state.uncertain() {
		unchanged.Effect = core.Unknown
	}
	if c.Abort {
		if state.Mode == "" {
			return unchanged, nil
		}
		a := &writeAttempt{driver: d, continuation: c, state: state}
		return a.abort(ctx)
	}
	if state.Refused != nil {
		return unchanged, core.Fail(state.Refused.Code, errors.New(state.Refused.Reason))
	}
	if state.Aborting {
		return unchanged, recoveryError("native cancellation must be resumed with Abort")
	}
	if state.Mode == "content" && state.Publishing {
		return core.Change{Effect: core.Unknown}, recoveryError("content publication requires native reconciliation")
	}
	if d.buffer == nil {
		return unchanged, core.Fail(core.UnsupportedOperation, nil)
	}
	a, err := d.newWrite(ctx, target, source, length)
	if err != nil {
		return unchanged, err
	}
	if state.Mode != "" && state.Parent != a.parent {
		return unchanged, core.Fail(core.InvalidRequest, errors.New("native target parent changed"))
	}
	a.continuation, a.state = c, state
	if state.Mode == "" {
		mode := "session"
		if length <= d.opts.ChunkSize {
			mode = "content"
		}
		binding.Parent = a.parent
		if err := a.save(ctx, writeRecord{Kind: "start", Binding: &binding, Mode: mode}); err != nil {
			return unchanged, err
		}
		a.state.Mode, a.state.Parent = mode, a.parent
	}
	a.session.URL, a.resume = state.Session, state.Session != ""
	result, err := a.run(ctx)
	if result.Effect != core.Applied && state.Publishing {
		result.Effect = core.Unknown
	}
	if result.Effect == core.Unchanged && a.state.Publishing && !state.Publishing && err != nil {
		// A definite native refusal or locally unsent request closes this
		// publication attempt. Progress observations alone never do so.
		failure := &writeFailure{Code: core.CodeOf(err), Reason: err.Error()}
		err = errors.Join(err, a.save(context.WithoutCancel(ctx), writeRecord{Kind: "refused", Failure: failure}))
	}
	return result, err
}

func (a *writeAttempt) abort(ctx context.Context) (core.Change, error) {
	result := core.Change{Effect: core.Unchanged}
	if a.state.uncertain() {
		result.Effect = core.Unknown
	}
	if !a.state.Aborting {
		if err := a.save(ctx, writeRecord{Kind: "aborting"}); err != nil {
			return result, err
		}
	}
	if a.state.Retired {
		return result, recoveryError("private session was retired; file publication still requires reconciliation")
	}
	if a.state.Session == "" {
		if a.state.uncertain() {
			return result, recoveryError("content publication cannot be undone by name")
		}
		// No body was sent. Lost initialization URLs remain orphan diagnostics
		// in the log and expire natively; they cannot publish this source.
		return result, a.save(context.WithoutCancel(ctx), writeRecord{Kind: "aborted"})
	}
	status, err := a.driver.abortSession(ctx, a.state.Session)
	if status != http.StatusNoContent && status != http.StatusNotFound {
		return result, err
	}
	if a.state.uncertain() {
		// DELETE acknowledges temporary cleanup, not a historical file effect.
		// Neither its 204 nor an absent session proves this write never published.
		saved := a.save(context.WithoutCancel(ctx), writeRecord{Kind: "retired"})
		return result, errors.Join(err, saved, recoveryError("private session retired without a file publication receipt"))
	}
	return result, errors.Join(err, a.save(context.WithoutCancel(ctx), writeRecord{Kind: "aborted"}))
}

var _ core.ContinuableWriter = (*Driver)(nil)
