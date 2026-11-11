package cloudreve

import (
	"context"
	"errors"
	"net/http"
	"net/url"

	core "github.com/nostalume/mountweave"
)

// ContinueWrite keeps ordinary exclusive creation and its visible placeholder
// semantics. The protected checkpoint consumer owns paused native sessions.
func (d *Driver) ContinueWrite(ctx context.Context, target core.NativeAddress, source core.Replayable, length int64, options core.WriteOptions) (core.Change, error) {
	unchanged := core.Change{Effect: core.Unchanged}
	if err := options.Policy.Validate(); err != nil {
		return unchanged, err
	}
	if target.Namespace() != core.TreeNamespace || options.Policy.Complete() {
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
	if err := core.ValidatePath(target.Value()); err != nil || target.Value() == "/" {
		return unchanged, core.Fail(core.InvalidRequest, err)
	}
	binding := writeBinding{Endpoint: d.opts.Endpoint, RootURI: d.opts.RootURI, Target: d.nativeURI(target.Value()), Source: version, Length: length, Policy: options.Policy}
	state, err := restoreWrite(c.Checkpoint, binding)
	if err != nil {
		return unchanged, err
	}
	current := state.change()
	if c.Abort && !state.Started {
		return unchanged, nil
	}
	root, err := d.resolve(ctx, "/")
	if err != nil {
		return current, err
	}
	if *root.Type != 1 {
		return current, core.Fail(core.NotDirectory, nil)
	}
	if state.Started && state.Binding.Root != root.ID {
		return current, core.Fail(core.InvalidRequest, errors.New("native Cloudreve root changed"))
	}
	if state.Done != nil {
		return current, nil
	}
	if state.Refused != nil {
		if c.Abort {
			return unchanged, nil
		}
		return current, core.Fail(state.Refused.Code, errors.New(state.Refused.Reason))
	}
	if state.Started && state.Session == nil {
		return current, recoveryError("Cloudreve initialization requires native reconciliation")
	}
	a := &writeAttempt{driver: d, owned: pendingUpload{URI: binding.Target}, input: source, size: length, continuation: c, state: state}
	if state.Session != nil {
		a.native, a.owned.ID = *state.Session, state.Session.ID
		if a.native.bodyCredential() == nil {
			a.owned.BodyURL, a.session.URL = a.native.URLs[0], a.native.URLs[0]
		}
	}
	var observation error
	checkOwner := !state.Confirming && !state.Aborting && !c.Abort
	if state.Owner != nil && (checkOwner || state.BodyReady) {
		query := url.Values{"id": {state.Owner.File}}
		if checkOwner {
			query.Set("extended", "true")
		}
		f, failure := d.uploadInfo(ctx, query)
		observation = failure
		if f != nil {
			if a.fileConfirmed(*f) {
				return a.confirmed(ctx, core.Change{Effect: core.Applied, Before: core.TargetMissing}, failure)
			}
			if checkOwner {
				observation = errors.Join(observation, a.matchOwner(*f))
				a.ownerChecked = state.BodyReady && observation == nil
			}
		}
	}
	if c.Abort {
		return a.abort(ctx, observation)
	}
	if state.Confirming {
		return current, errors.Join(observation, recoveryError("Cloudreve callback requires native reconciliation"))
	}
	if state.Retired {
		return current, errors.Join(observation, recoveryError("native body retired; Cloudreve placeholder requires reconciliation"))
	}
	if state.Aborting {
		return current, errors.Join(observation, recoveryError("Cloudreve cancellation must be resumed with Abort"))
	}
	if observation != nil {
		return current, observation
	}
	if d.buffer == nil && !state.BodyReady {
		return current, core.Fail(core.UnsupportedOperation, nil)
	}
	fresh, err := d.newWrite(ctx, target, source, length, &root)
	if err != nil {
		return current, err
	}
	if state.Started && state.Binding.Parent != fresh.parent {
		return current, core.Fail(core.InvalidRequest, errors.New("native Cloudreve target parent changed"))
	}
	a.id, a.parent = fresh.id, fresh.parent
	if !state.Started {
		binding.Root, binding.Parent = root.ID, a.parent
		a.state.Binding = binding
	}
	a.resume = state.Session != nil
	result, err := a.run(ctx)
	if result.Effect == core.Unchanged && state.Started {
		result = current
	}
	return result, err
}

type uploadFile struct {
	item
	Primary  string `json:"primary_entity"`
	Extended *struct {
		Entities []struct {
			ID   string `json:"id"`
			Type *int   `json:"type"`
		} `json:"entities"`
	} `json:"extended_info"`
}

// Preserve a decoded native fact even if its response body fails to close.
// URI observations bind a future callback; ID observations reconcile an old one.
func (d *Driver) uploadInfo(ctx context.Context, query url.Values) (*uploadFile, error) {
	response, err := d.apiRequest(ctx, http.MethodGet, d.apiURL("/file/info")+"?"+query.Encode(), nil, "", 0)
	if err != nil {
		return nil, err
	}
	var f uploadFile
	decoded, err := d.decode(response, &f)
	if !decoded {
		return nil, err
	}
	if _, failure := f.metadata(); failure != nil || *f.Type != 0 {
		return nil, errors.Join(err, core.Fail(core.InvalidUpstreamResponse, failure))
	}
	if id := query.Get("id"); id != "" && f.ID != id {
		return nil, errors.Join(err, core.Fail(core.InvalidUpstreamResponse, errors.New("Cloudreve returned a different file identity")))
	}
	if uri := query.Get("uri"); uri != "" {
		if failure := f.at(uri); failure != nil {
			return nil, errors.Join(err, failure)
		}
	}
	return &f, err
}

func (a *writeAttempt) ownerOf(f uploadFile) (*nativeOwner, error) {
	if f.Primary == "" {
		return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("missing Cloudreve primary entity identity"))
	}
	if f.Metadata["sys:upload_session_id"] != a.native.ID || f.Extended == nil {
		return nil, recoveryError("Cloudreve placeholder no longer belongs to this upload session")
	}
	var entity string
	for _, e := range f.Extended.Entities {
		if e.ID == "" || e.Type == nil {
			return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("invalid Cloudreve entity identity"))
		}
		if *e.Type == 0 {
			if entity != "" {
				return nil, recoveryError("Cloudreve placeholder has ambiguous version ownership")
			}
			entity = e.ID
		}
	}
	if entity == "" || f.Primary == entity {
		return nil, recoveryError("Cloudreve placeholder lacks an uncommitted owned version")
	}
	return &nativeOwner{File: f.ID, Entity: entity}, nil
}

func (a *writeAttempt) captureOwner(ctx context.Context) (*nativeOwner, error) {
	f, err := a.driver.uploadInfo(ctx, url.Values{"uri": {a.owned.URI}, "extended": {"true"}})
	if f == nil {
		return nil, err
	}
	owner, failure := a.ownerOf(*f)
	return owner, errors.Join(err, failure)
}

func (a *writeAttempt) matchOwner(f uploadFile) error {
	if err := f.at(a.owned.URI); err != nil {
		return err
	}
	owner, err := a.ownerOf(f)
	if err != nil {
		return err
	}
	if a.state.Owner == nil || *owner != *a.state.Owner {
		return recoveryError("Cloudreve file or entity ownership changed")
	}
	return nil
}

func (a *writeAttempt) fileConfirmed(f uploadFile) bool {
	return a.state.BodyReady && a.state.Owner != nil && f.ID == a.state.Owner.File && f.Primary == a.state.Owner.Entity && f.Size != nil && *f.Size == a.size
}

func (a *writeAttempt) abort(ctx context.Context, observation error) (core.Change, error) {
	change := a.state.change()
	residue := recoveryError("Cloudreve placeholder requires native reconciliation")
	if a.state.Retired {
		return change, errors.Join(observation, residue)
	}
	if !a.state.Aborting {
		if err := a.save(ctx, writeRecord{Kind: "aborting"}); err != nil {
			return change, errors.Join(observation, err, residue)
		}
		a.state.Aborting = true
	}
	if a.owned.BodyURL == "" {
		return change, errors.Join(observation, residue, a.native.bodyCredential())
	}
	retired, err := a.driver.retireBody(ctx, a.owned)
	if retired {
		err = errors.Join(err, a.save(context.WithoutCancel(ctx), writeRecord{Kind: "body_retired"}))
	}
	// Private body retirement does not remove the logical placeholder or undo
	// a possibly acknowledged callback. Its effect and source hold survive.
	return change, errors.Join(observation, err, residue)
}

var _ core.ContinuableWriter = (*Driver)(nil)
