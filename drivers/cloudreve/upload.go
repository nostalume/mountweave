package cloudreve

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

// Create borrows known-length input synchronously. Native preparation creates
// a placeholder; only Cloudreve's successful callback confirms publication.
// It is not FilePublisher: incomplete input can already have a visible effect.
func (d *Driver) Create(ctx context.Context, target core.NativeAddress, input io.Reader, length *int64) (result core.Change, err error) {
	result.Effect = core.Unchanged
	if target.Namespace() != core.TreeNamespace {
		return result, core.Fail(core.UnsupportedOperation, nil)
	}
	if input == nil || length != nil && *length < 0 {
		return result, core.Fail(core.InvalidRequest, nil)
	}
	if length == nil || d.buffer == nil {
		return result, core.Fail(core.UnsupportedOperation, nil)
	}
	attempt, err := d.newWrite(ctx, target, input, *length, nil)
	if err != nil {
		return result, err
	}
	return attempt.run(ctx)
}

func (d *Driver) newWrite(ctx context.Context, target core.NativeAddress, input io.Reader, size int64, root *item) (*writeAttempt, error) {
	p := target.Value()
	if err := d.admit(ctx, p); err != nil {
		return nil, err
	}
	if p == "/" {
		return nil, core.Fail(core.InvalidRequest, nil)
	}
	d.mu.Lock()
	blocked := len(d.pending) != 0
	d.mu.Unlock()
	if blocked {
		return nil, core.Fail(core.UpstreamFailure, errors.New("upload cleanup remains unresolved"))
	}
	cut := strings.LastIndexByte(p, '/')
	parent := p[:cut]
	if parent == "" {
		parent = "/"
	}
	var f item
	if root != nil && parent == "/" {
		f = *root
	} else {
		var err error
		f, err = d.resolve(ctx, parent)
		if err != nil {
			return nil, err
		}
	}
	if *f.Type != 1 {
		return nil, core.Fail(core.NotDirectory, nil)
	}
	return &writeAttempt{driver: d, id: d.nextUpload.Add(1), owned: pendingUpload{URI: d.nativeURI(p)}, input: input, size: size, parent: f.ID}, nil
}

func (a *writeAttempt) run(ctx context.Context) (core.Change, error) {
	if a.size == 0 {
		return a.empty(ctx)
	}
	return upload.Write(ctx, a, a.confirm, a.driver.opts.CleanupTimeout)
}

func (a *writeAttempt) empty(ctx context.Context) (core.Change, error) {
	result := core.Change{Effect: core.Unchanged}
	d := a.driver
	var err error
	if a.continuation != nil {
		err = upload.ReadRange(ctx, a.input.(core.Replayable), 0, nil)
	} else {
		err = upload.ReadExact(ctx, a.input, nil)
	}
	if err != nil {
		return result, inputError(err)
	}
	if err := a.begin(ctx); err != nil {
		return result, err
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	data, _ := json.Marshal(map[string]any{"type": "file", "uri": a.owned.URI, "err_on_conflict": true})
	response, failure := d.apiRequest(ctx, http.MethodPost, d.apiURL("/file/create"), bytes.NewReader(data), "application/json", int64(len(data)))
	if failure != nil {
		return core.Change{Effect: core.Unknown}, failure
	}
	var created item
	decoded, failure := d.decode(response, &created)
	if !decoded {
		return a.refused(ctx, mutationFailure(failure), failure)
	}
	if e := created.at(a.owned.URI); e != nil || *created.Type != 0 || created.Size == nil || *created.Size != 0 {
		return core.Change{Effect: core.Unknown}, errors.Join(failure, core.Fail(core.InvalidUpstreamResponse, e))
	}
	return a.confirmed(ctx, core.Change{Effect: core.Applied, Before: core.TargetMissing}, errors.Join(failure, ctx.Err()))
}

type uploadCredential struct {
	ID     string   `json:"session_id"`
	URI    string   `json:"uri"`
	Size   int64    `json:"chunk_size"`
	Secret string   `json:"callback_secret"`
	URLs   []string `json:"upload_urls"`
	Policy struct {
		Type  string `json:"type"`
		Relay bool   `json:"relay"`
	} `json:"storage_policy"`
	Encryption json.RawMessage `json:"encrypt_metadata"`
}

type writeAttempt struct {
	driver       *Driver
	id           uint64
	owned        pendingUpload
	native       uploadCredential
	input        io.Reader
	size         int64
	parent       string
	session      onedrive.Session
	resume       bool
	ownerChecked bool
	continuation *core.WriteContinuation
	state        writeState
}

func (a *writeAttempt) Prepare(ctx context.Context) (core.Change, error) {
	d := a.driver
	result := core.Change{Effect: core.Incomplete, Before: core.TargetMissing}
	if a.owned.ID != "" {
		return result, a.prepareOwned(ctx)
	}
	if e := a.begin(ctx); e != nil {
		return core.Change{Effect: core.Unchanged}, e
	}
	if e := ctx.Err(); e != nil {
		return core.Change{Effect: core.Unchanged}, e
	}
	data, _ := json.Marshal(map[string]any{"uri": a.owned.URI, "size": a.size})
	response, e := d.apiRequest(ctx, http.MethodPut, d.apiURL("/file/upload"), bytes.NewReader(data), "application/json", int64(len(data)))
	if e != nil {
		a.owned.reconcile = true
		return core.Change{Effect: core.Unknown}, e
	}
	decoded, e := d.decode(response, &a.native)
	if !decoded {
		result := mutationFailure(e)
		a.owned.reconcile = result.Effect == core.Unknown
		return a.refused(ctx, result, e)
	}
	returned, parseErr := parseURI(a.native.URI)
	expected, _ := parseURI(a.owned.URI)
	if a.native.ID == "" || parseErr != nil || returned.Path != expected.Path {
		a.owned.reconcile = true
		return core.Change{Effect: core.Unknown}, errors.Join(e, core.Fail(core.InvalidUpstreamResponse, errors.New("invalid upload session identity")))
	}
	a.owned.ID = a.native.ID
	saved := a.save(context.WithoutCancel(ctx), writeRecord{Kind: "session", Session: &a.native})
	if saved == nil && a.continuation != nil {
		a.state.Session = &a.native
	}
	if e != nil || saved != nil {
		credentialErr := a.native.bodyCredential()
		if credentialErr == nil {
			a.owned.BodyURL = a.native.URLs[0]
		}
		return result, errors.Join(e, saved, credentialErr)
	}
	return result, a.prepareOwned(ctx)
}

func (a *writeAttempt) prepareOwned(ctx context.Context) error {
	if e := a.native.bodyCredential(); e != nil {
		return e
	}
	a.owned.BodyURL = a.native.URLs[0]
	a.session.URL = a.owned.BodyURL
	if a.continuation != nil && a.state.Owner == nil {
		owner, e := a.captureOwner(ctx)
		if owner != nil {
			saved := a.save(context.WithoutCancel(ctx), writeRecord{Kind: "owned", Owner: owner})
			if saved == nil {
				a.state.Owner = owner
			}
			e = errors.Join(e, saved)
		}
		return e
	}
	return nil
}

func (c uploadCredential) bodyCredential() error {
	if c.Policy.Type != "onedrive" || c.Policy.Relay || len(c.Encryption) != 0 && string(c.Encryption) != "null" {
		return core.Fail(core.UnsupportedOperation, errors.New("unsupported Cloudreve upload policy"))
	}
	if len(c.URLs) != 1 || !core.ValidName(c.ID) || !core.ValidName(c.Secret) || onedrive.ValidateURL(c.URLs[0]) != nil || c.Size < 1 || c.Size%(320<<10) != 0 || c.Size >= 60<<20 {
		return core.Fail(core.InvalidUpstreamResponse, errors.New("invalid OneDrive upload credential"))
	}
	return nil
}

func (a *writeAttempt) Deliver(ctx context.Context) (upload.Delivery, error) {
	d := a.driver
	var delivery upload.Delivery
	if a.state.BodyReady {
		return upload.Delivery{Kind: upload.ContentReady}, nil
	}
	lease, e := d.buffer.Acquire(ctx, a.native.Size, 1)
	if e != nil {
		return delivery, inputError(e)
	}
	defer lease.Release()
	options := onedrive.Options{Client: d.content, Attempts: d.opts.RangeAttempts, MaxResponseBytes: d.opts.MaxResponseBytes, CloseBody: d.closeBody, Replayable: a.continuation != nil, Resume: a.resume}
	response, e := onedrive.Upload(ctx, &a.session, a.input, a.size, lease, options)
	if response != nil {
		if response.Request != nil && response.Request.Method == http.MethodGet || response.StatusCode != http.StatusOK && response.StatusCode != http.StatusCreated {
			failure, cleanup := options.Failure(response)
			e = errors.Join(e, failure, cleanup)
		} else {
			if e == nil {
				delivery.Kind = upload.ContentReady
				saved := a.save(context.WithoutCancel(ctx), writeRecord{Kind: "body_ready"})
				if saved == nil && a.continuation != nil {
					a.state.BodyReady = true
				}
				e = saved
			}
			e = errors.Join(e, d.closeBody(response.Body))
		}
	}
	return delivery, inputError(e)
}

func (a *writeAttempt) confirm(ctx context.Context) (core.Change, error) {
	d := a.driver
	result := core.Change{Effect: core.Incomplete, Before: core.TargetMissing}
	if a.continuation != nil {
		if a.state.Confirming {
			return core.Change{Effect: core.Unknown, Before: core.TargetMissing}, recoveryError("Cloudreve callback requires native reconciliation")
		}
		// A restored ready body has already passed ownership admission in this
		// execution. No transfer follows that observation; do not query it twice.
		if !a.ownerChecked {
			f, e := d.uploadInfo(ctx, url.Values{"uri": {a.owned.URI}, "extended": {"true"}})
			if f != nil {
				if a.fileConfirmed(*f) {
					return a.confirmed(ctx, core.Change{Effect: core.Applied, Before: core.TargetMissing}, e)
				}
				e = errors.Join(e, a.matchOwner(*f))
			}
			if e != nil {
				return result, e
			}
		}
		if e := a.save(ctx, writeRecord{Kind: "confirming"}); e != nil {
			return result, e
		}
		a.state.Confirming = true
	}
	// The native body receipt is not a Cloudreve file receipt. The callback
	// capability has a different recipient from account authentication.
	callback := d.apiURL("/callback/onedrive/" + url.PathEscape(a.native.ID) + "/" + url.PathEscape(a.native.Secret))
	req, e := http.NewRequestWithContext(ctx, http.MethodPost, callback, strings.NewReader("{}"))
	if e != nil {
		return result, e
	}
	req.Header.Set("Content-Type", "application/json")
	if e := ctx.Err(); e != nil {
		return result, e
	}
	result.Effect = core.Unknown
	a.owned.reconcile = true
	response, e := d.content.Do(req)
	if e != nil {
		return result, networkError(e)
	}
	decoded, e := d.decode(response, nil)
	if decoded {
		result.Effect = core.Applied
	}
	return a.confirmed(ctx, result, e)
}

func (a *writeAttempt) Retire(ctx context.Context, change core.Change) (bool, core.Change, error) {
	if a.continuation != nil {
		return false, change, nil
	}
	if a.owned.reconcile {
		return false, change, core.Fail(core.UpstreamFailure, errors.New("upload requires native reconciliation"))
	}
	if a.owned.ID == "" {
		return true, change, nil
	}
	confirmed, err := a.driver.retireBody(ctx, a.owned)
	return confirmed, change, errors.Join(err, recoveryError("Cloudreve placeholder remains incomplete"))
}

func (a *writeAttempt) Retain(cause error) {
	if a.continuation != nil {
		return
	}
	a.driver.keepUpload(a.id, a.owned, cause)
}

func mutationFailure(err error) core.Change {
	switch core.CodeOf(err) {
	case core.AlreadyExists:
		return core.Change{Effect: core.Unchanged}
	}
	return core.Change{Effect: core.Unknown}
}

var _ core.FileCreator = (*Driver)(nil)
