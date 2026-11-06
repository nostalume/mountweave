package httpapi

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/protocols/internal/httpwire"
)

type Verifier func(context.Context, string) (core.Principal, error)

// v1 HTTP paths are a tree view; raw keys are never coerced into this field.
type entryView struct {
	Path       string        `json:"path"`
	Name       string        `json:"name"`
	Kind       core.Kind     `json:"kind"`
	Size       *int64        `json:"size"`
	ModifiedAt *time.Time    `json:"modified_at"`
	Virtual    bool          `json:"virtual"`
	Evidence   core.Evidence `json:"evidence,omitzero"`
}

func treeEntry(e core.Entry) entryView {
	return entryView{e.Address.Value(), e.Name, e.Kind, e.Size, e.ModifiedAt, e.Virtual, e.Evidence}
}

type Handler struct {
	core         *core.Core
	verify       Verifier
	cursorTTL    time.Duration
	options      Options
	files, kinds map[string]TaskKind
	jobKinds     map[string]TaskKind
	jobs         map[string]JobSource
	tasks        taskIndex
	failed       []failedTask
	life         sync.Mutex
	closed       bool
	requests     sync.WaitGroup
	closeOnce    sync.Once
	closeDone    chan struct{}
}

func New(c *core.Core, options Options) (*Handler, error) {
	if c == nil || options.Authenticate == nil || options.CursorTTL <= 0 {
		return nil, core.Fail(core.InvalidRequest, nil)
	}
	h := &Handler{core: c, verify: options.Authenticate, cursorTTL: options.CursorTTL, options: options}
	if err := h.configure(); err != nil {
		return nil, core.Fail(core.InvalidRequest, err)
	}
	return h, nil
}
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	id := rand.Text()
	w.Header().Set("X-Request-ID", id)
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	h.life.Lock()
	if h.closed {
		h.life.Unlock()
		writeStatusProblem(w, r, id, 503, "unavailable", nil)
		return
	}
	h.requests.Add(1)
	h.life.Unlock()
	defer h.requests.Done()
	var allow string
	switch r.URL.Path {
	case "/api/v1/info", "/api/v1/fs/entries":
		allow = "GET"
	case "/api/v1/fs/entry":
		allow = "GET, DELETE"
	case "/api/v1/fs/content":
		allow = "GET, HEAD, PUT"
	case "/api/v1/fs/directories", "/api/v1/fs/operations", "/api/v1/mounts/stop", "/api/v1/mounts/restore", "/api/v1/tasks/cancel", "/api/v1/jobs/actions":
		allow = "POST"
	case "/api/v1/mounts", "/api/v1/tasks/task", "/api/v1/jobs":
		allow = "GET"
	case "/api/v1/mounts/definition", "/api/v1/jobs/job":
		allow = "GET, PUT"
	case "/api/v1/mounts/activation", "/api/v1/tasks":
		allow = "GET, POST"
	case "/api/v1/inputs":
		allow = "GET, POST, DELETE"
	case "/api/v1/inputs/content":
		allow = "PUT"
	case "/api/v1/inputs/seal":
		allow = "POST"
	}
	permitted := slices.Contains(strings.Split(allow, ", "), r.Method)
	mutation := permitted && strings.HasPrefix(r.URL.Path, "/api/v1/fs/") && r.Method != http.MethodGet && r.Method != http.MethodHead
	var effect *core.Effect
	if mutation {
		unchanged := core.Unchanged
		effect = &unchanged
	}
	fail := func(err error) { writeProblem(w, r, id, err, effect) }
	values := r.Header.Values("Authorization")
	if len(values) != 1 {
		fail(core.Fail(core.Unauthenticated, nil))
		return
	}
	fields := strings.Fields(values[0])
	if len(fields) != 2 || !strings.EqualFold(fields[0], "Bearer") {
		fail(core.Fail(core.Unauthenticated, nil))
		return
	}
	principal, err := h.verify(r.Context(), fields[1])
	if err != nil || principal == "" {
		fail(core.Fail(core.Unauthenticated, nil))
		return
	}
	if allow == "" {
		writeStatusProblem(w, r, id, 404, "not_found", nil)
		return
	}
	if !permitted {
		w.Header().Set("Allow", allow)
		writeStatusProblem(w, r, id, 405, "invalid_request", nil)
		return
	}
	if r.URL.Path == "/api/v1/fs/operations" {
		h.operation(w, r, id, principal)
		return
	}
	if strings.HasPrefix(r.URL.Path, "/api/v1/jobs") {
		if r.URL.Path == "/api/v1/jobs/actions" {
			h.operation(w, r, id, principal)
		} else {
			h.jobRoute(w, r, id, principal)
		}
		return
	}
	if strings.HasPrefix(r.URL.Path, "/api/v1/mounts") {
		h.controlRoute(w, r, id, principal)
		return
	}
	if strings.HasPrefix(r.URL.Path, "/api/v1/tasks") {
		h.taskRoute(w, r, id, principal)
		return
	}
	if strings.HasPrefix(r.URL.Path, "/api/v1/inputs") {
		h.inputRoute(w, r, id, principal)
		return
	}
	query, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		fail(core.Fail(core.InvalidRequest, err))
		return
	}
	allowed := map[string]bool{}
	if r.URL.Path != "/api/v1/info" {
		allowed["path"] = true
	}
	if r.URL.Path == "/api/v1/fs/entries" {
		allowed["limit"] = true
		allowed["cursor"] = true
	}
	if r.URL.Path == "/api/v1/fs/content" && r.Method == http.MethodPut {
		allowed["replace"] = true
	}
	for key, v := range query {
		if !allowed[key] || len(v) != 1 {
			fail(core.Fail(core.InvalidRequest, nil))
			return
		}
	}
	target := query.Get("path")
	if mutation {
		result, changeErr := h.change(r, principal, target)
		httpwire.EvidenceHeaders(w, result.Evidence)
		*effect = result.Effect
		if changeErr != nil {
			fail(changeErr)
			return
		}
		status := http.StatusCreated
		if r.Method == http.MethodDelete {
			status = http.StatusOK
		}
		writeJSON(w, status, map[string]string{"effect": result.Effect.String()})
		return
	}
	conditions, err := httpwire.ParseConditions(r.Header)
	if err != nil {
		fail(err)
		return
	}
	if r.URL.Path == "/api/v1/info" {
		if err := conditions.When.Check(core.Evidence{}, true); err != nil {
			fail(err)
			return
		}
		var inputBytes, inputAppend int64
		var inputCount, inputBlocks int
		if h.options.Inputs != nil {
			limits := h.options.Inputs.Limits()
			inputBytes, inputAppend, inputCount, inputBlocks = limits.MaxBytes, limits.MaxAppendBytes, limits.MaxInputs, limits.MaxBlocks
		}
		writeJSON(w, http.StatusOK, map[string]any{"api_major": 1,
			"operations":                []string{"stat", "list", "read", "mkdir", "create", "write", "remove", "copy", "move", "copy_batch", "move_batch", "remove_batch", "copy_tree", "remove_tree"},
			"file_operations_read_only": false,
			"components":                map[string]bool{"control": h.options.Control != nil, "tasks": h.options.Queue != nil, "inputs": h.options.Inputs != nil, "jobs": len(h.jobs) > 0},
			"job_sources":               h.jobDescriptions(),
			"task_kinds":                h.taskNames(),
			"limits":                    map[string]any{"page_default": core.DefaultPage, "page_max": core.MaxPage, "path_max_utf8_bytes": core.MaxPathBytes, "path_max_segments": core.MaxPathSegments, "task_records_max": h.options.MaxTasks, "json_request_max_bytes": h.options.MaxJSONBytes, "input_journal_max_bytes": inputBytes, "input_append_max_bytes": inputAppend, "input_records_max": inputCount, "input_blocks_max": inputBlocks}})
		return
	}
	switch r.URL.Path {
	case "/api/v1/fs/entry":
		err = h.entry(w, r, id, principal, target, conditions)
	case "/api/v1/fs/entries":
		err = h.entries(w, r, id, principal, target, query, conditions)
	default:
		err = h.content(w, r, id, principal, target, conditions)
	}
	if err != nil {
		fail(err)
	}
}

// change projects only supported synchronous mutations. Conditions are never
// evaluated by a racy metadata preflight before an irreversible operation.
func (h *Handler) change(r *http.Request, p core.Principal, target string) (core.Change, error) {
	refused := core.Change{Effect: core.Unchanged}
	conditions, conditionErr := httpwire.ParseConditions(r.Header)
	if conditionErr != nil {
		return refused, conditionErr
	}
	for _, name := range []string{"If-Range", "Range", "Content-Range"} {
		if _, present := r.Header[http.CanonicalHeaderKey(name)]; present {
			return refused, core.Fail(core.InvalidRequest, nil)
		}
	}
	if encoding := strings.Join(r.Header.Values("Content-Encoding"), ","); encoding != "" && !strings.EqualFold(encoding, "identity") {
		return refused, core.Fail(core.InvalidRequest, nil)
	}
	switch r.Method {
	case http.MethodPut:
		var length *int64
		if r.ContentLength >= 0 {
			n := r.ContentLength
			length = &n
		}
		input := r.Body
		if input == nil {
			input = http.NoBody
		}
		replace := r.URL.Query().Get("replace")
		if _, present := r.URL.Query()["replace"]; present && replace != "true" && replace != "false" {
			return refused, core.Fail(core.InvalidRequest, nil)
		}
		options := fileWriteOptions(replace == "true")
		if !conditions.When.Empty() && (!conditions.When.IsAbsent() || replace == "true") {
			if replace == "false" {
				return refused, core.Fail(core.InvalidRequest, nil)
			}
			options.Policy = core.Publish(conditions.When)
		}
		change, err := h.core.Write(r.Context(), p, core.Path(target), input, length, options)
		if conditions.When.IsAbsent() && change.Effect == core.Unchanged && core.CodeOf(err) == core.AlreadyExists {
			err = core.Fail(core.PreconditionFailed, errors.Join(&core.ConditionFailure{Negative: true}, err))
		}
		return change, err
	case http.MethodPost:
		if !conditions.When.Empty() {
			return refused, core.Fail(core.UnsupportedOperation, nil)
		}
		if r.ContentLength != 0 {
			return refused, core.Fail(core.InvalidRequest, nil)
		}
		return h.core.Mkdir(r.Context(), p, core.Path(target))
	case http.MethodDelete:
		if r.ContentLength != 0 {
			return refused, core.Fail(core.InvalidRequest, nil)
		}
		return h.core.Remove(r.Context(), p, core.Path(target), core.RemoveOptions{When: conditions.When})
	default:
		return refused, core.Fail(core.UnsupportedOperation, nil)
	}
}

func (h *Handler) entry(w http.ResponseWriter, r *http.Request, id string, principal core.Principal, target string, conditions httpwire.Conditions) error {
	entry, err := h.core.Stat(r.Context(), principal, core.Path(target), core.ReadSelection{When: conditions.When})
	if err != nil {
		return err
	}
	httpwire.EvidenceHeaders(w, entry.Evidence)
	writeJSON(w, 200, treeEntry(entry))
	return nil
}
func (h *Handler) entries(w http.ResponseWriter, r *http.Request, id string, principal core.Principal, target string, query url.Values, conditions httpwire.Conditions) error {
	req := core.ListRequest{Query: core.Children(core.Path(target)), Cursor: query.Get("cursor"), When: conditions.When}
	if req.Cursor == "" {
		req.CursorTTL = h.cursorTTL
	}
	if _, ok := query["cursor"]; ok && (req.Cursor == "" || len(req.Cursor) > 256) {
		return core.Fail(core.InvalidRequest, nil)
	}
	if v, ok := query["limit"]; ok {
		n, err := strconv.Atoi(v[0])
		if err != nil || n < 1 {
			return core.Fail(core.InvalidRequest, nil)
		}
		req.Limit = n
	}
	page, err := h.core.List(r.Context(), principal, req)
	if err != nil {
		return err
	}
	items := make([]entryView, len(page.Items))
	for i, e := range page.Items {
		items[i] = treeEntry(e)
	}
	writeJSON(w, 200, struct {
		Items           []entryView `json:"items"`
		NextCursor      *string     `json:"next_cursor"`
		CursorExpiresAt *time.Time  `json:"cursor_expires_at"`
	}{items, page.NextCursor, page.CursorExpiresAt})
	return nil
}
func (h *Handler) content(w http.ResponseWriter, r *http.Request, id string, principal core.Principal, target string, conditions httpwire.Conditions) error {
	if r.Method == http.MethodHead {
		entry, err := h.core.Inspect(r.Context(), principal, core.ReadAction, core.Path(target), core.ReadSelection{When: conditions.When})
		if err != nil {
			return err
		}
		if entry.Kind != core.File {
			return core.Fail(core.NotFile, nil)
		}
		contentHeaders(w, entry.Name, entry.Size)
		httpwire.EvidenceHeaders(w, entry.Evidence)
		w.WriteHeader(200)
		return nil
	}
	var wanted *core.Range
	var err error
	if r.Header.Get("If-Range") == "" {
		wanted, err = httpwire.ParseRange(r.Header)
		if err != nil {
			return err
		}
	}
	opened, err := h.core.Open(r.Context(), principal, core.Path(target), core.OpenOptions{Range: wanted, Selection: core.ReadSelection{When: conditions.When}})
	if err != nil {
		return err
	}
	contentHeaders(w, opened.Entry.Name, opened.Length)
	httpwire.EvidenceHeaders(w, opened.Entry.Evidence)
	status := http.StatusOK
	if opened.Range != nil {
		status = http.StatusPartialContent
		actual := opened.Range
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", actual.Start, actual.End, actual.Total))
	}
	w.WriteHeader(status)
	_, copyErr := io.Copy(w, opened.Body)
	closeErr := opened.Body.Close()
	err = errors.Join(copyErr, closeErr)
	if err != nil {
		if h.options.OnStreamError != nil {
			h.options.OnStreamError(id, err)
		}
		panic(http.ErrAbortHandler)
	}
	return nil
}
func contentHeaders(w http.ResponseWriter, name string, length *int64) {
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Disposition", mime.FormatMediaType("attachment", map[string]string{"filename": name}))
	if length != nil {
		w.Header().Set("Content-Length", strconv.FormatInt(*length, 10))
	}
}
func writeJSON(w http.ResponseWriter, status int, value any) {
	data, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(data)
}
func writeProblem(w http.ResponseWriter, r *http.Request, id string, err error, effect *core.Effect) {
	if r.Context().Err() != nil {
		return
	}
	if status, ok := httpwire.ConditionStatus(w, r, err); ok {
		if status == 304 {
			w.WriteHeader(status)
		} else {
			writeStatusProblem(w, r, id, status, string(core.CodeOf(err)), effect)
		}
		return
	}
	status, code := problemStatus(err)
	var e *core.Error
	if status == 416 && errors.As(err, &e) && e.Total != nil {
		w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", *e.Total))
	}
	writeStatusProblem(w, r, id, status, string(code), effect)
}
func writeStatusProblem(w http.ResponseWriter, r *http.Request, id string, status int, code string, effect *core.Effect) {
	if status == 401 {
		w.Header().Set("WWW-Authenticate", `Bearer realm="files"`)
	}
	problem := map[string]any{"type": "about:blank", "title": http.StatusText(status), "status": status, "code": code, "request_id": id}
	if effect != nil {
		problem["effect"] = effect.String()
	}
	data, _ := json.Marshal(problem)
	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(status)
	if r.Method != http.MethodHead {
		_, _ = w.Write(data)
	}
}

func problemStatus(err error) (int, core.Code) {
	code := core.CodeOf(err)
	status := 500
	switch code {
	case core.InvalidRequest, core.InvalidPath, core.InvalidRange:
		status = 400
	case core.Unauthenticated:
		status = 401
	case core.PermissionDenied:
		status = 403
	case core.NotFound:
		status = 404
	case core.PreconditionFailed:
		status = 412
	case core.PathAmbiguous, core.NotDirectory, core.NotFile, core.UnrepresentableName, core.AlreadyExists, core.DirectoryNotEmpty:
		status = 409
	case core.CursorUnavailable:
		status = 410
	case core.RangeNotSatisfiable:
		status = 416
	case core.CapacityExceeded:
		status = 429
	case core.UnsupportedOperation:
		status = 501
	case core.UpstreamFailure, core.UpstreamAccessDenied, core.InvalidUpstreamResponse:
		status = 502
	case core.MountUnavailable, core.ResolutionBudgetExceeded, core.ResponseBudgetExceeded, core.UpstreamThrottled:
		status = 503
	case core.UpstreamTimeout:
		status = 504
	default:
		code = core.InternalError
	}
	return status, code
}
