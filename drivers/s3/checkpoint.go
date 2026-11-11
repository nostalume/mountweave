package s3

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	core "github.com/nostalume/mountweave"
)

// Each record carries only the fact acknowledged at this boundary. Source and
// destination identity are recorded at initiation, never repeated per part.
type uploadRecord struct {
	Version  int            `json:"version"`
	Kind     string         `json:"kind"`
	Binding  *resumeBinding `json:"binding,omitempty"`
	Token    string         `json:"token,omitempty"`
	Phase    string         `json:"phase,omitempty"`
	UploadID string         `json:"upload_id,omitempty"`
	Part     int            `json:"part,omitempty"`
	ETag     string         `json:"etag,omitempty"`
	Evidence *core.Evidence `json:"evidence,omitempty"`
}

func appendUpload(ctx context.Context, appendRecord func(context.Context, []byte) error, r uploadRecord) error {
	data, err := json.Marshal(r)
	if err != nil {
		return err
	}
	return appendRecord(ctx, append(data, '\n'))
}
func restoreUpload(data []byte, binding resumeBinding, count int) (uploadState, error) {
	state := uploadState{}
	invalid := func(err error) (uploadState, error) {
		return state, core.Fail(core.InvalidRequest, errors.Join(errors.New("invalid native recovery log"), err))
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	startVersion := 0
	for {
		var r uploadRecord
		err := decoder.Decode(&r)
		if err == io.EOF {
			if len(data) > 0 && state.Token == "" {
				return invalid(nil)
			}
			return state, nil
		}
		if err != nil {
			return invalid(err)
		}
		if r.Version < 2 || r.Version > 5 {
			return invalid(nil)
		}
		done := r.Kind == "phase" && r.Phase == "done"
		if r.Evidence != nil {
			if !done || r.Version < 4 || !r.Evidence.Validator.Valid() {
				return invalid(nil)
			}
			state.Evidence = *r.Evidence
		} else if done && (r.Version >= 4 || startVersion >= 4) {
			return invalid(nil)
		}
		switch r.Kind {
		case "start":
			if r.Binding == nil {
				return invalid(nil)
			}
			if r.Version < 5 {
				if r.Binding.Policy != nil || r.Version == 2 && !r.Binding.When.Empty() || r.Version > 2 && r.Binding.Replace {
					return invalid(nil)
				}
				when := r.Binding.When
				if r.Version == 2 && !r.Binding.Replace {
					when = core.IfAbsent()
				}
				policy := core.Publish(when)
				r.Binding.Policy = &policy
				r.Binding.When, r.Binding.Replace = core.Condition{}, false
			} else if r.Binding.Policy == nil || r.Binding.Policy.Validate() != nil || !r.Binding.When.Empty() || r.Binding.Replace {
				return invalid(nil)
			}
			if !sameBinding(*r.Binding, binding) || r.Token == "" || state.Phase != "" || len(state.Parts) != 0 || r.Phase != "initializing" || r.UploadID != "" || r.Part != 0 || r.ETag != "" {
				return invalid(nil)
			}
			state.Token = r.Token
			state.Phase = r.Phase
			startVersion = r.Version
		case "part":
			if state.Phase != "uploading" || r.Part < 1 || r.Part > count || r.ETag == "" || r.Binding != nil || r.Phase != "" || r.Token != "" || r.UploadID != "" {
				return invalid(nil)
			}
			if r.Part <= len(state.Parts) && state.Parts[r.Part-1].ETag != nil && *state.Parts[r.Part-1].ETag != r.ETag {
				return invalid(nil)
			}
			state.acknowledge(r.Part, aws.String(r.ETag))
		case "phase":
			if r.Part != 0 || r.ETag != "" || r.Token != "" || r.Binding != nil || state.Token == "" || (state.UploadID != "" && r.UploadID != state.UploadID) {
				return invalid(nil)
			}
			valid := false
			switch r.Phase {
			case "":
				valid = state.Phase == "initializing" && state.UploadID == "" && r.UploadID == ""
			case "uploading":
				valid = (state.Phase == "initializing" || state.Phase == "committing") && r.UploadID != ""
			case "committing":
				valid = state.Phase == "uploading" && state.Confirmed == count
			case "done":
				valid = state.Phase == "committing"
			case "aborting":
				valid = state.Phase == "uploading"
			case "aborted":
				valid = state.Phase == "aborting"
			}
			if !valid {
				return invalid(nil)
			}
			state.Phase = r.Phase
			state.UploadID = r.UploadID
		default:
			return invalid(nil)
		}
	}
}

func sameBinding(a, b resumeBinding) bool {
	if a.Policy == nil || b.Policy == nil || *a.Policy != *b.Policy {
		return false
	}
	a.Policy, b.Policy = nil, nil
	return a == b
}
