package httpapi

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"net/url"
	"strings"
	"unicode/utf8"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/control"
	"github.com/nostalume/mountweave/input"
	"github.com/nostalume/mountweave/task"
	"github.com/nostalume/mountweave/transfer"
)

func fileWriteOptions(replace bool) core.WriteOptions {
	if replace {
		return core.WriteOptions{Policy: core.Publish()}
	}
	return core.WriteOptions{}
}

// decode admits one structured value and rejects unknown fields and trailing
// values. Native driver configuration remains opaque to this decoder.
func decode(raw []byte, value any) error {
	if !utf8.Valid(raw) {
		return core.Fail(core.InvalidRequest, nil)
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if err := d.Decode(value); err != nil {
		return core.Fail(core.InvalidRequest, err)
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return core.Fail(core.InvalidRequest, err)
	}
	return nil
}
func (h *Handler) readJSON(w http.ResponseWriter, r *http.Request, value any) error {
	if err := noConditions(r); err != nil {
		return err
	}
	media, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || media != "application/json" {
		return core.Fail(core.InvalidRequest, err)
	}
	if encoding := r.Header.Get("Content-Encoding"); encoding != "" && !strings.EqualFold(encoding, "identity") {
		return core.Fail(core.InvalidRequest, nil)
	}
	var input io.Reader = r.Body
	if h.options.MaxJSONBytes > 0 {
		input = http.MaxBytesReader(w, r.Body, h.options.MaxJSONBytes)
	}
	data, err := io.ReadAll(input)
	if err != nil {
		return core.Fail(core.InvalidRequest, err)
	}
	return decode(data, value)
}
func singleQuery(r *http.Request, allowed ...string) (url.Values, error) {
	query, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		return nil, core.Fail(core.InvalidRequest, err)
	}
	for name, values := range query {
		ok := false
		for _, a := range allowed {
			if a == name {
				ok = true
				break
			}
		}
		if !ok || len(values) != 1 {
			return nil, core.Fail(core.InvalidRequest, nil)
		}
	}
	return query, nil
}
func noBody(r *http.Request) error {
	if err := noConditions(r); err != nil {
		return err
	}
	if r.ContentLength != 0 {
		return core.Fail(core.InvalidRequest, nil)
	}
	return nil
}
func noConditions(r *http.Request) error {
	for _, name := range []string{"If-Match", "If-None-Match", "If-Range", "If-Modified-Since", "If-Unmodified-Since", "Range", "Content-Range"} {
		if _, present := r.Header[http.CanonicalHeaderKey(name)]; present {
			return core.Fail(core.InvalidRequest, nil)
		}
	}
	return nil
}
func respond(w http.ResponseWriter, r *http.Request, id string, status int, value any) {
	data, err := json.Marshal(value)
	if err != nil {
		writeProblem(w, r, id, core.Fail(core.InternalError, err), nil)
		return
	}
	if r.Context().Err() != nil {
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(data)
}
func domainStatus(err error) (int, string) {
	switch {
	case errors.Is(err, transfer.ErrInvalid):
		return 400, "invalid_request"
	case errors.Is(err, transfer.ErrNotFound):
		return 404, "not_found"
	case errors.Is(err, transfer.ErrConflict):
		return 409, "revision_conflict"
	case errors.Is(err, transfer.ErrBusy):
		return 409, "transfer_busy"
	case errors.Is(err, input.ErrInvalid):
		return 400, "invalid_request"
	case errors.Is(err, input.ErrNotFound):
		return 404, "not_found"
	case errors.Is(err, input.ErrCapacity):
		return 429, "capacity_exceeded"
	case errors.Is(err, input.ErrConflict):
		return 409, "input_conflict"
	case errors.Is(err, input.ErrBusy):
		return 409, "input_busy"
	case errors.Is(err, input.ErrState):
		return 409, "input_state"
	case errors.Is(err, input.ErrClosed), errors.Is(err, input.ErrCorrupt), errors.Is(err, input.ErrUncertain):
		return 503, "unavailable"
	case errors.Is(err, control.ErrConflict):
		return 409, "revision_conflict"
	case errors.Is(err, control.ErrNotFound):
		return 404, string(core.NotFound)
	case errors.Is(err, control.ErrClosed), errors.Is(err, task.ErrClosed), errors.Is(err, errTaskCleanup):
		return 503, "unavailable"
	case errors.Is(err, task.ErrFull):
		return 429, string(core.CapacityExceeded)
	}
	// Preserve the established file error/status mapping in one place.
	status, code := problemStatus(err)
	return status, string(code)
}
func writeDomainProblem(w http.ResponseWriter, r *http.Request, id string, err error, result any) {
	if r.Context().Err() != nil {
		return
	}
	status, code := domainStatus(err)
	problem := map[string]any{"type": "about:blank", "title": http.StatusText(status), "status": status, "code": code, "request_id": id}
	if result != nil {
		problem["result"] = result
	}
	data, _ := json.Marshal(problem)
	if status == 401 {
		w.Header().Set("WWW-Authenticate", `Bearer realm="files"`)
	}
	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(status)
	_, _ = w.Write(data)
}
func (h *Handler) operation(w http.ResponseWriter, r *http.Request, id string, p core.Principal) {
	if _, err := singleQuery(r); err != nil {
		writeProblem(w, r, id, err, nil)
		return
	}
	var request OperationRequest
	if err := h.readJSON(w, r, &request); err != nil {
		writeProblem(w, r, id, err, nil)
		return
	}
	kind, ok := h.files[request.Kind]
	if r.URL.Path == "/api/v1/jobs/actions" {
		kind, ok = h.jobKinds[request.Kind]
	}
	if !ok || len(request.Input) == 0 {
		writeProblem(w, r, id, core.Fail(core.InvalidRequest, nil), nil)
		return
	}
	work, err := kind.Prepare(r.Context(), p, request.Input)
	if err != nil {
		var result any
		if r.URL.Path == "/api/v1/fs/operations" {
			result = FileResult{Effect: core.Unchanged.String()}
		}
		writeDomainProblem(w, r, id, err, result)
		return
	}
	result, err := func() (result any, err error) {
		defer func() {
			if cleanupErr := h.finishWork(work.Cleanup); cleanupErr != nil {
				err = errors.Join(err, cleanupErr)
				if file, ok := result.(FileResult); ok {
					file.CleanupError = errorCode(cleanupErr)
					result = file
				}
			}
		}()
		return work.Run(r.Context(), func(any) {})
	}()
	values, projectionErr := kind.Project(task.Snapshot{Result: result, Err: err})
	err = errors.Join(err, projectionErr)
	result = values.Result
	if err != nil {
		writeDomainProblem(w, r, id, err, result)
		return
	}
	respond(w, r, id, 200, result)
}
