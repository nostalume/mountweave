package graphdrive

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/internal/onedrive"
	"github.com/nostalume/mountweave/drivers/internal/upload"
)

func inputError(err error) error {
	switch {
	case errors.Is(err, upload.ErrCapacity):
		return core.Fail(core.CapacityExceeded, err)
	case errors.Is(err, upload.ErrSize):
		return core.Fail(core.InvalidRequest, err)
	case errors.Is(err, upload.ErrRead):
		return core.Fail(core.InvalidUpstreamResponse, err)
	default:
		return err
	}
}

// Create borrows known-length input and creates exclusively. Small files use
// one content PUT; large files publish through the final session request.
func (d *Driver) Create(ctx context.Context, target core.NativeAddress, input io.Reader, length *int64) (core.Change, error) {
	return d.Publish(ctx, target, input, length, core.PublicationOptions{When: core.IfAbsent()})
}

// Publish currently supports IfAbsent only. Other policies are rejected before
// input is consumed; initialization conditions are not final publication guards.
func (d *Driver) Publish(ctx context.Context, target core.NativeAddress, input io.Reader, length *int64, options core.PublicationOptions) (result core.Change, err error) {
	result.Effect = core.Unchanged
	if d.readOnly || options.When != core.IfAbsent() || target.Namespace() != core.TreeNamespace {
		return result, core.Fail(core.UnsupportedOperation, nil)
	}
	if input == nil || length != nil && *length < 0 {
		return result, core.Fail(core.InvalidRequest, nil)
	}
	if length == nil || d.buffer == nil {
		return result, core.Fail(core.UnsupportedOperation, nil)
	}
	attempt, err := d.newWrite(ctx, target, input, *length)
	if err != nil {
		return result, err
	}
	return attempt.run(ctx)
}

func (d *Driver) newWrite(ctx context.Context, target core.NativeAddress, input io.Reader, size int64) (*writeAttempt, error) {
	if err := d.admit(ctx, target.Value()); err != nil {
		return nil, err
	}
	if target.Value() == "/" {
		return nil, core.Fail(core.InvalidRequest, nil)
	}
	d.mu.Lock()
	blocked := len(d.pending) != 0
	d.mu.Unlock()
	if blocked {
		return nil, core.Fail(core.UpstreamFailure, errors.New("upload cleanup remains unresolved"))
	}
	cut := strings.LastIndexByte(target.Value(), '/')
	parentPath, name := target.Value()[:cut], target.Value()[cut+1:]
	if parentPath == "" {
		parentPath = "/"
	}
	parent, e := d.resolve(ctx, parentPath)
	if e != nil {
		return nil, e
	}
	if parent.Folder == nil {
		return nil, core.Fail(core.NotDirectory, nil)
	}
	route := "/drives/" + url.PathEscape(d.opts.DriveID) + "/items/" + url.PathEscape(parent.ID) + ":/" + url.PathEscape(name)
	return &writeAttempt{driver: d, id: d.nextUpload.Add(1), target: target.Value(), route: route, parent: parent.ID, name: name, input: input, size: size}, nil
}

func (a *writeAttempt) run(ctx context.Context) (core.Change, error) {
	d := a.driver
	chunk := d.opts.ChunkSize
	if a.state.Mode == "content" {
		chunk = max(chunk, a.size)
	}
	lease, e := d.buffer.Acquire(ctx, chunk, 1)
	if e != nil {
		return core.Change{Effect: core.Unchanged}, inputError(e)
	}
	defer lease.Release()
	a.lease = lease
	if a.state.Mode == "content" || a.state.Mode == "" && a.size <= int64(len(lease.Buffer())) {
		return a.content(ctx)
	}
	return upload.Write(ctx, a, nil, d.opts.CleanupTimeout)
}

func (a *writeAttempt) content(ctx context.Context) (result core.Change, err error) {
	d := a.driver
	result.Effect = core.Unchanged
	// A recovered content PUT has no native session to query. Repeating it
	// cannot attribute a previous publication, even when the name now exists.
	if a.state.Publishing {
		return core.Change{Effect: core.Unknown}, recoveryError("content publication requires native reconciliation")
	}
	data := a.lease.Buffer()[:int(a.size)]
	input := a.input
	if a.continuation != nil {
		err = upload.ReadRange(ctx, input.(core.Replayable), 0, data)
	} else {
		err = upload.ReadExact(ctx, input, data)
	}
	if err != nil {
		return result, inputError(err)
	}
	err = a.lease.Send(ctx, func() error {
		if err := a.beforeComplete(ctx); err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		body := upload.Borrow(data)
		defer body.Close()
		response, failure := d.apiRequest(ctx, http.MethodPut, d.apiURL(a.route+":/content?@microsoft.graph.conflictBehavior=fail"), body, "application/octet-stream", a.size)
		if failure != nil {
			result.Effect = core.Unknown
			return failure
		}
		result, failure = d.receipt(response, a.parent, a.name, a.size)
		return failure
	})
	return a.confirmed(ctx, result, errors.Join(err, ctx.Err()))
}

type writeAttempt struct {
	driver                      *Driver
	id                          uint64
	target, route, parent, name string
	input                       io.Reader
	size                        int64
	lease                       *upload.Lease
	session                     onedrive.Session
	unresolved                  bool
	continuation                *core.WriteContinuation
	state                       writeState
	resume                      bool
}

func (a *writeAttempt) Prepare(ctx context.Context) (core.Change, error) {
	d := a.driver
	unchanged := core.Change{Effect: core.Unchanged}
	if a.session.URL != "" {
		return unchanged, nil
	}
	if err := a.save(ctx, writeRecord{Kind: "init"}); err != nil {
		return unchanged, err
	}
	payload, _ := json.Marshal(map[string]any{"item": map[string]string{"@microsoft.graph.conflictBehavior": "fail", "name": a.name}})
	response, e := d.apiRequest(ctx, http.MethodPost, d.apiURL(a.route+":/createUploadSession"), bytes.NewReader(payload), "application/json", int64(len(payload)))
	if e != nil {
		a.unresolved = true
		return unchanged, e
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		a.unresolved = response.StatusCode >= 500 || response.StatusCode == http.StatusRequestTimeout
		return unchanged, errors.Join(d.responseFailure(response))
	}
	var session onedrive.Session
	decoded, e := d.decode(response, &session)
	if !decoded || onedrive.ValidateURL(session.URL) != nil {
		a.unresolved = true
		return unchanged, core.Fail(core.InvalidUpstreamResponse, e)
	}
	a.session = session
	saved := a.save(context.WithoutCancel(ctx), writeRecord{Kind: "session", Session: session.URL})
	if saved == nil {
		a.state.Session = session.URL
	}
	return unchanged, errors.Join(e, saved)
}

func (a *writeAttempt) Deliver(ctx context.Context) (upload.Delivery, error) {
	d := a.driver
	result := core.Change{Effect: core.Unchanged}
	response, e := onedrive.Upload(ctx, &a.session, a.input, a.size, a.lease, onedrive.Options{Client: d.content, Attempts: d.opts.RangeAttempts, MaxResponseBytes: d.opts.MaxResponseBytes, CloseBody: d.closeBody, Replayable: a.continuation != nil, Resume: a.resume, BeforeComplete: a.beforeComplete})
	var err error
	if response != nil {
		// Always decode an already received file result, including cancellation
		// or closure failure. No response Body crosses into the shared lifetime.
		if response.Request != nil && response.Request.Method == http.MethodGet {
			err = errors.Join(d.responseFailure(response))
		} else {
			result, err = d.receipt(response, a.parent, a.name, a.size)
		}
		if e != nil && result.Effect == core.Applied || result.Effect != core.Applied && a.session.FinalAttempted {
			result.Effect = core.Unknown
		}
		err = errors.Join(err, e)
	} else {
		if a.session.FinalAttempted {
			result.Effect = core.Unknown
		} else {
			return upload.Delivery{}, inputError(e)
		}
		err = inputError(e)
	}
	if a.continuation != nil && a.session.Gone && !a.state.Publishing {
		err = errors.Join(err, a.save(context.WithoutCancel(ctx), writeRecord{Kind: "expired"}))
	}
	result, err = a.confirmed(ctx, result, err)
	return upload.Delivery{Kind: upload.FileResult, Change: result}, err
}

func (a *writeAttempt) Retire(ctx context.Context, change core.Change) (bool, core.Change, error) {
	if a.continuation != nil {
		// Pausing returns runtime resources, not the durable session. The
		// checkpoint consumer remains its sole owner, including failed appends.
		return false, change, nil
	}
	if a.unresolved {
		return false, change, core.Fail(core.UpstreamFailure, errors.New("upload initiation requires native reconciliation"))
	}
	if a.session.URL == "" || a.session.Gone {
		return true, change, nil
	}
	status, err := a.driver.abortSession(ctx, a.session.URL)
	return status == http.StatusNoContent || status == http.StatusNotFound, change, err
}

func (a *writeAttempt) Retain(cause error) {
	if a.continuation != nil {
		return
	}
	a.driver.keepUpload(a.id, pendingUpload{Target: a.target, Session: a.session.URL}, cause)
}

func (d *Driver) receipt(response *http.Response, parent, name string, size int64) (core.Change, error) {
	unchanged := core.Change{Effect: core.Unchanged}
	if response.StatusCode != http.StatusOK && response.StatusCode != http.StatusCreated {
		failure := errors.Join(d.responseFailure(response))
		if response.StatusCode >= 500 || response.StatusCode == http.StatusRequestTimeout || response.StatusCode < 400 {
			return core.Change{Effect: core.Unknown}, failure
		}
		return unchanged, failure
	}
	var f item
	decoded, err := d.decode(response, &f)
	if !decoded || f.ID == "" || f.Name != name || f.File == nil || f.Folder != nil || f.Remote != nil || f.Package != nil || f.Size == nil || *f.Size != size ||
		f.Parent != nil && (f.Parent.ID != "" && f.Parent.ID != parent || f.Parent.DriveID != "" && f.Parent.DriveID != d.opts.DriveID) {
		return core.Change{Effect: core.Unknown}, core.Fail(core.InvalidUpstreamResponse, err)
	}
	return core.Change{Effect: core.Applied, Before: core.TargetMissing}, err
}

var _ core.FileCreator = (*Driver)(nil)
var _ core.FilePublisher = (*Driver)(nil)
