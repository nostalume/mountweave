package cloudreve

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"

	core "github.com/nostalume/mountweave"
)

type writeBinding struct {
	Endpoint string           `json:"endpoint"`
	RootURI  string           `json:"root_uri"`
	Root     string           `json:"root_id"`
	Parent   string           `json:"parent_id"`
	Target   string           `json:"target_uri"`
	Source   string           `json:"source_version"`
	Length   int64            `json:"length"`
	Policy   core.WritePolicy `json:"policy"`
}

type nativeOwner struct {
	File   string `json:"file_id"`
	Entity string `json:"entity_id"`
}

// Cloudreve creates a visible placeholder before delivering content. Native
// byte ranges stay with OneDrive; these records preserve ownership and the
// separate content and logical-file receipts.
type writeRecord struct {
	Version int               `json:"version"`
	Kind    string            `json:"kind"`
	Binding *writeBinding     `json:"binding,omitempty"`
	Session *uploadCredential `json:"session,omitempty"`
	Owner   *nativeOwner      `json:"owner,omitempty"`
	Change  *core.Change      `json:"change,omitempty"`
	Failure *writeFailure     `json:"failure,omitempty"`
}

type writeFailure struct {
	Code   core.Code `json:"code"`
	Reason string    `json:"reason"`
}

type writeState struct {
	Binding                                           writeBinding
	Started, BodyReady, Confirming, Aborting, Retired bool
	Session                                           *uploadCredential
	Owner                                             *nativeOwner
	Done                                              *core.Change
	Refused                                           *writeFailure
}

func (s writeState) change() core.Change {
	switch {
	case s.Done != nil:
		return *s.Done
	case s.Refused != nil || !s.Started:
		return core.Change{Effect: core.Unchanged}
	case s.Confirming:
		return core.Change{Effect: core.Unknown, Before: core.TargetMissing}
	case s.Session == nil:
		return core.Change{Effect: core.Unknown}
	default:
		return core.Change{Effect: core.Incomplete, Before: core.TargetMissing}
	}
}

func recoveryError(message string) error {
	return core.Fail(core.UpstreamFailure, errors.New(message))
}

func restoreWrite(data []byte, expected writeBinding) (writeState, error) {
	state := writeState{Binding: expected}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	invalid := func(err error) (writeState, error) {
		return writeState{}, core.Fail(core.InvalidRequest, errors.Join(errors.New("invalid Cloudreve recovery log or binding"), err))
	}
	for {
		var r writeRecord
		err := decoder.Decode(&r)
		if err == io.EOF {
			if len(data) != 0 && !state.Started {
				return invalid(nil)
			}
			return state, nil
		}
		if err != nil || r.Version != 1 || state.Done != nil || state.Refused != nil {
			return invalid(err)
		}
		allowed := writeRecord{Version: 1, Kind: r.Kind}
		switch r.Kind {
		case "start":
			if state.Started || r.Binding == nil || r.Binding.Root == "" || r.Binding.Parent == "" {
				return invalid(nil)
			}
			observed := *r.Binding
			observed.Root, observed.Parent = "", ""
			if observed != expected {
				return invalid(nil)
			}
			allowed.Binding = r.Binding
			state.Binding, state.Started = *r.Binding, true
		case "session":
			if !state.Started || state.Binding.Length == 0 || state.Session != nil || r.Session == nil || r.Session.ID == "" {
				return invalid(nil)
			}
			got, err := parseURI(r.Session.URI)
			want, _ := parseURI(state.Binding.Target)
			if err != nil || got.Path != want.Path {
				return invalid(err)
			}
			allowed.Session, state.Session = r.Session, r.Session
		case "owned":
			if state.Session == nil || state.Owner != nil || state.Aborting || r.Owner == nil || r.Owner.File == "" || r.Owner.Entity == "" {
				return invalid(nil)
			}
			allowed.Owner, state.Owner = r.Owner, r.Owner
		case "body_ready":
			if state.Owner == nil || state.BodyReady || state.Aborting {
				return invalid(nil)
			}
			state.BodyReady = true
		case "confirming":
			if !state.BodyReady || state.Confirming || state.Aborting {
				return invalid(nil)
			}
			state.Confirming = true
		case "done":
			if !state.Started || state.Binding.Length != 0 && !state.BodyReady || r.Change == nil || r.Change.Effect != core.Applied || r.Change.Before != core.TargetMissing {
				return invalid(nil)
			}
			allowed.Change, state.Done = r.Change, r.Change
		case "aborting":
			if state.Session == nil || state.Aborting {
				return invalid(nil)
			}
			state.Aborting = true
		case "body_retired":
			if !state.Aborting || state.Retired {
				return invalid(nil)
			}
			state.Retired = true
		case "refused":
			if !state.Started || state.Session != nil || r.Failure == nil || r.Failure.Code != core.AlreadyExists || r.Failure.Reason == "" {
				return invalid(nil)
			}
			allowed.Failure, state.Refused = r.Failure, r.Failure
		default:
			return invalid(nil)
		}
		if r != allowed {
			return invalid(nil)
		}
	}
}

// An append error is not a rollback. Stop native work and let the consumer
// reload its committed log; never continue from an assumed in-memory revision.
func (a *writeAttempt) save(ctx context.Context, record writeRecord) error {
	if a.continuation == nil {
		return nil
	}
	record.Version = 1
	data, err := json.Marshal(record)
	if err != nil {
		return err
	}
	return a.continuation.Append(ctx, append(data, '\n'))
}

func (a *writeAttempt) begin(ctx context.Context) error {
	if a.continuation == nil || a.state.Started {
		return nil
	}
	if err := a.save(ctx, writeRecord{Kind: "start", Binding: &a.state.Binding}); err != nil {
		return err
	}
	a.state.Started = true
	return nil
}

func (a *writeAttempt) refused(ctx context.Context, change core.Change, err error) (core.Change, error) {
	if a.continuation != nil && change.Effect == core.Unchanged && err != nil {
		failure := &writeFailure{Code: core.CodeOf(err), Reason: err.Error()}
		err = errors.Join(err, a.save(context.WithoutCancel(ctx), writeRecord{Kind: "refused", Failure: failure}))
	}
	return change, err
}

func (a *writeAttempt) confirmed(ctx context.Context, change core.Change, err error) (core.Change, error) {
	if a.continuation != nil && change.Effect == core.Applied {
		err = errors.Join(err, a.save(context.WithoutCancel(ctx), writeRecord{Kind: "done", Change: &change}))
	}
	return change, err
}
