package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strconv"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/input"
	"github.com/nostalume/mountweave/task"
)

type InputAction string

const (
	CreateInput  InputAction = "create"
	ListInputs   InputAction = "list"
	ReadInput    InputAction = "read"
	AppendInput  InputAction = "append"
	SealInput    InputAction = "seal"
	ReleaseInput InputAction = "release"
	UseInput     InputAction = "use"
)

type InputInfo struct {
	ID    string
	Owner core.Principal
}
type inputValue struct {
	ID       string `json:"id"`
	Owner    string `json:"owner"`
	Offset   int64  `json:"offset"`
	Sealed   bool   `json:"sealed"`
	Released bool   `json:"released"`
	Blocked  bool   `json:"blocked"`
	Version  string `json:"version,omitempty"`
}

func inputView(r input.Record) inputValue {
	return inputValue{ID: r.ID, Owner: r.Owner, Offset: r.Offset, Sealed: r.Sealed, Released: r.Released, Blocked: r.Blocked, Version: r.Version}
}
func (h *Handler) inputAccess(ctx context.Context, p core.Principal, a InputAction, r input.Record) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := h.options.AuthorizeInput(ctx, p, a, InputInfo{ID: r.ID, Owner: core.Principal(r.Owner)}); err != nil {
		return core.Fail(core.PermissionDenied, err)
	}
	return nil
}
func (h *Handler) inputRoute(w http.ResponseWriter, r *http.Request, requestID string, p core.Principal) {
	s := h.options.Inputs
	if s == nil {
		writeProblem(w, r, requestID, core.Fail(core.UnsupportedOperation, nil), nil)
		return
	}
	q, err := singleQuery(r, "id", "offset")
	fail := func(err error, record input.Record) {
		var result any
		if record.ID != "" {
			result = inputView(record)
		}
		writeDomainProblem(w, r, requestID, err, result)
	}
	if err != nil {
		fail(err, input.Record{})
		return
	}
	id := q.Get("id")
	if r.URL.Path == "/api/v1/inputs" {
		if q.Has("offset") {
			fail(core.Fail(core.InvalidRequest, nil), input.Record{})
			return
		}
		if r.Method == http.MethodPost {
			if len(q) != 0 {
				fail(core.Fail(core.InvalidRequest, nil), input.Record{})
				return
			}
			if err = noBody(r); err != nil {
				fail(err, input.Record{})
				return
			}
			if err = h.inputAccess(r.Context(), p, CreateInput, input.Record{Owner: string(p)}); err != nil {
				fail(err, input.Record{})
				return
			}
			created, err := s.Create(r.Context(), string(p))
			if err != nil {
				fail(err, created)
				return
			}
			w.Header().Set("Location", "/api/v1/inputs?id="+url.QueryEscape(created.ID))
			respond(w, r, requestID, 201, inputView(created))
			return
		}
		if r.Method == http.MethodGet && len(q) == 0 {
			if err = h.inputAccess(r.Context(), p, ListInputs, input.Record{}); err != nil {
				fail(err, input.Record{})
				return
			}
			records, err := s.List(r.Context())
			if err != nil {
				fail(err, input.Record{})
				return
			}
			rows := make([]inputValue, 0, len(records))
			for _, record := range records {
				if h.inputAccess(r.Context(), p, ReadInput, record) == nil {
					rows = append(rows, inputView(record))
				}
			}
			respond(w, r, requestID, 200, rows)
			return
		}
	}
	if id == "" {
		fail(core.Fail(core.InvalidRequest, nil), input.Record{})
		return
	}
	record, statusErr := s.Status(r.Context(), id)
	if record.ID == "" {
		fail(statusErr, record)
		return
	}
	action := ReadInput
	if r.Method == http.MethodDelete {
		action = ReleaseInput
	} else if r.URL.Path == "/api/v1/inputs/content" {
		action = AppendInput
	} else if r.URL.Path == "/api/v1/inputs/seal" {
		action = SealInput
	}
	if err = h.inputAccess(r.Context(), p, action, record); err != nil {
		fail(err, input.Record{})
		return
	}
	if statusErr != nil {
		fail(statusErr, record)
		return
	}
	if action == ReadInput {
		respond(w, r, requestID, 200, inputView(record))
		return
	}
	if action == ReleaseInput {
		if err = noBody(r); err != nil {
			fail(err, record)
			return
		}
		record, err = s.Release(r.Context(), id)
	} else {
		offset, parseErr := strconv.ParseInt(q.Get("offset"), 10, 64)
		if parseErr != nil || offset < 0 {
			fail(core.Fail(core.InvalidRequest, parseErr), record)
			return
		}
		if action == SealInput {
			if err = noBody(r); err != nil {
				fail(err, record)
				return
			}
			record, err = s.Seal(r.Context(), id, offset)
		} else {
			if r.ContentLength < 0 || r.ContentLength == 0 || (r.Header.Get("Content-Encoding") != "" && r.Header.Get("Content-Encoding") != "identity") {
				fail(core.Fail(core.InvalidRequest, nil), record)
				return
			}
			record, err = s.Append(r.Context(), id, offset, r.Body, r.ContentLength)
		}
	}
	if err != nil {
		fail(err, record)
		return
	}
	respond(w, r, requestID, 200, inputView(record))
}

// The input consumer holds the source pin at admission; Core remains the sole
// file operation owner. Queuing retains no body, open source file or mount use.
func (h *Handler) inputWriteKind() TaskKind {
	return TaskKind{
		Prepare: func(ctx context.Context, p core.Principal, raw json.RawMessage) (task.Work, error) {
			var i struct {
				Path    string            `json:"path"`
				InputID string            `json:"input_id"`
				Replace *bool             `json:"replace,omitempty"`
				Policy  *core.WritePolicy `json:"policy,omitempty"`
			}
			if err := decode(raw, &i); err != nil {
				return task.Work{}, err
			}
			if i.Policy != nil && i.Replace != nil {
				return task.Work{}, core.Fail(core.InvalidRequest, nil)
			}
			var options core.WriteOptions
			if i.Policy != nil {
				options.Policy = *i.Policy
			} else if i.Replace != nil && *i.Replace {
				options.Policy = core.Publish()
			}
			if err := options.Policy.Validate(); err != nil {
				return task.Work{}, err
			}
			if i.InputID == "" {
				return task.Work{}, core.Fail(core.InvalidRequest, nil)
			}
			selected, err := h.core.Capture(ctx, core.AddressAccess{Address: core.Path(i.Path), Write: true})
			if err != nil {
				return task.Work{}, err
			}
			record, err := h.options.Inputs.Status(ctx, i.InputID)
			if err != nil {
				return task.Work{}, err
			}
			if err = h.inputAccess(ctx, p, UseInput, record); err != nil {
				return task.Work{}, err
			}
			ref, err := h.options.Inputs.Acquire(ctx, i.InputID)
			if err != nil {
				return task.Work{}, err
			}
			return task.Work{Key: selected, Cleanup: ref.Close, Run: func(ctx context.Context, report func(any)) (result any, runErr error) {
				unchanged := changeResult(core.Change{Effect: core.Unchanged})
				if err := h.inputAccess(ctx, p, UseInput, record); err != nil {
					return unchanged, err
				}
				body, err := ref.Open(ctx)
				if err != nil {
					return unchanged, err
				}
				defer func() {
					if closeErr := body.Close(); closeErr != nil {
						runErr = errors.Join(runErr, closeErr)
						if file, ok := result.(FileResult); ok {
							file.CleanupError = errorCode(closeErr)
							result = file
						}
					}
				}()
				length := record.Offset
				change, err := h.core.Write(selected.Context(ctx), p, core.Path(i.Path), body, &length, options)
				return changeResult(change), err
			}}, nil
		},
		Project: func(s task.Snapshot) (TaskValues, error) {
			return TaskValues{Progress: s.Progress, Result: s.Result}, nil
		},
	}
}
