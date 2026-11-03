package s3

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"

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
}

func appendUpload(ctx context.Context, appendRecord func(context.Context, []byte) error, r uploadRecord) error {
	data, err := json.Marshal(r)
	if err != nil {
		return err
	}
	return appendRecord(ctx, append(data, '\n'))
}
func restoreUpload(data []byte, binding resumeBinding, count int) (uploadState, error) {
	state := uploadState{Parts: make(map[int]string)}
	invalid := func(err error) (uploadState, error) {
		return state, core.Fail(core.InvalidRequest, errors.Join(errors.New("invalid native recovery log"), err))
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
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
		if r.Version != 2 {
			return invalid(nil)
		}
		switch r.Kind {
		case "start":
			if r.Binding == nil || *r.Binding != binding || r.Token == "" || state.Phase != "" || len(state.Parts) != 0 || r.Phase != "initializing" || r.UploadID != "" || r.Part != 0 || r.ETag != "" {
				return invalid(nil)
			}
			state.Token = r.Token
			state.Phase = r.Phase
		case "part":
			if state.Phase != "uploading" || r.Part < 1 || r.Part > count || r.ETag == "" || r.Binding != nil || r.Phase != "" || r.Token != "" || r.UploadID != "" {
				return invalid(nil)
			}
			if previous, ok := state.Parts[r.Part]; ok && previous != r.ETag {
				return invalid(nil)
			}
			state.Parts[r.Part] = r.ETag
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
				valid = state.Phase == "uploading" && len(state.Parts) == count
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
