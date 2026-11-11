package graphdrive

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/internal/onedrive"
)

type writeBinding struct {
	Endpoint, Drive, Root, Parent, Name, Target, Source string
	Length                                              int64
	Policy                                              core.WritePolicy
}

// Progress belongs to the native session. The log stores identity and effect
// boundaries, not a second copy of the server's received-byte map.
type writeRecord struct {
	Version int           `json:"version"`
	Kind    string        `json:"kind"`
	Binding *writeBinding `json:"binding,omitempty"`
	Mode    string        `json:"mode,omitempty"`
	Session string        `json:"session,omitempty"`
	Change  *core.Change  `json:"change,omitempty"`
	Failure *writeFailure `json:"failure,omitempty"`
}

type writeFailure struct {
	Code   core.Code `json:"code"`
	Reason string    `json:"reason"`
}

type writeState struct {
	Mode, Session, Parent                  string
	Initializing                           bool
	Publishing, Aborting, Aborted, Retired bool
	Done                                   *core.Change
	Refused                                *writeFailure
}

func (s writeState) uncertain() bool { return s.Publishing && s.Refused == nil }

func recoveryError(message string) error {
	return core.Fail(core.UpstreamFailure, errors.New(message))
}

func restoreWrite(data []byte, binding writeBinding) (writeState, error) {
	state := writeState{}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	invalid := func(err error) (writeState, error) {
		return writeState{}, core.Fail(core.InvalidRequest, errors.Join(errors.New("invalid Graph recovery log or binding"), err))
	}
	for {
		var r writeRecord
		err := decoder.Decode(&r)
		if err == io.EOF {
			if len(data) != 0 && state.Mode == "" {
				return invalid(nil)
			}
			return state, nil
		}
		if err != nil || r.Version != 1 || state.Done != nil || state.Aborted {
			return invalid(err)
		}
		if state.Refused != nil && r.Kind != "aborting" && r.Kind != "aborted" {
			return invalid(nil)
		}
		allowed := writeRecord{Version: 1, Kind: r.Kind}
		switch r.Kind {
		case "start":
			if r.Binding == nil {
				return invalid(nil)
			}
			observed := *r.Binding
			observed.Parent = ""
			if state.Mode != "" || observed != binding || r.Binding.Parent == "" || (r.Mode != "content" && r.Mode != "session") || (r.Mode == "session" && binding.Length == 0) {
				return invalid(nil)
			}
			allowed.Binding, allowed.Mode = r.Binding, r.Mode
			state.Mode, state.Parent = r.Mode, r.Binding.Parent
		case "init":
			if state.Mode != "session" || state.Session != "" || state.Publishing || state.Aborting {
				return invalid(nil)
			}
			state.Initializing = true
		case "session":
			if !state.Initializing || state.Session != "" || state.Publishing || state.Aborting || onedrive.ValidateURL(r.Session) != nil {
				return invalid(nil)
			}
			allowed.Session = r.Session
			state.Session = r.Session
			state.Initializing = false
		case "expired":
			if state.Session == "" || state.Publishing || state.Aborting {
				return invalid(nil)
			}
			state.Session = ""
		case "publishing":
			if state.Mode == "" || state.Publishing || state.Aborting || (state.Mode == "session" && state.Session == "") {
				return invalid(nil)
			}
			state.Publishing = true
		case "done":
			if !state.Publishing || state.Aborting || r.Change == nil || r.Change.Effect != core.Applied || r.Change.Before != core.TargetMissing {
				return invalid(nil)
			}
			allowed.Change = r.Change
			state.Done = r.Change
		case "refused":
			if !state.Publishing || state.Aborting || r.Failure == nil || r.Failure.Code == "" || r.Failure.Reason == "" {
				return invalid(nil)
			}
			allowed.Failure = r.Failure
			state.Refused = r.Failure
		case "aborting":
			if state.Mode == "" || state.Aborting {
				return invalid(nil)
			}
			state.Aborting = true
		case "aborted":
			if !state.Aborting || state.uncertain() {
				return invalid(nil)
			}
			state.Aborted = true
		case "retired":
			if !state.Aborting || !state.Publishing || state.Session == "" || state.Retired {
				return invalid(nil)
			}
			state.Retired = true
		default:
			return invalid(nil)
		}
		if r != allowed {
			return invalid(nil)
		}
	}
}

// A failed append is not a rollback. The executor stops; its next invocation
// reloads the consumer's committed log before doing any more native work.
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

func (a *writeAttempt) beforeComplete(ctx context.Context) error {
	if a.continuation == nil || a.state.Publishing {
		return nil
	}
	if err := a.save(ctx, writeRecord{Kind: "publishing"}); err != nil {
		return err
	}
	a.state.Publishing = true
	return nil
}

func (a *writeAttempt) confirmed(ctx context.Context, change core.Change, err error) (core.Change, error) {
	if a.continuation != nil && change.Effect == core.Applied {
		err = errors.Join(err, a.save(context.WithoutCancel(ctx), writeRecord{Kind: "done", Change: &change}))
	}
	return change, err
}
