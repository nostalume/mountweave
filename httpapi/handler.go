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
)

type Verifier func(context.Context, string) (core.Principal, error)
type Handler struct {
	core          *core.Core
	verify        Verifier
	cursorTTL     time.Duration
	OnStreamError func(string, error)
	options       Options
	files, kinds  map[string]TaskKind
	tasks         taskIndex
	failed        []failedTask
	life          sync.Mutex
	closed        bool
	requests      sync.WaitGroup
	closeOnce     sync.Once
	closeDone     chan struct{}
}

func New(c *core.Core, verify Verifier, cursorTTL time.Duration, options ...Options) (*Handler, error) {
	if c == nil || verify == nil || cursorTTL <= 0 {
		return nil, errors.New("a core, bearer verifier, and positive cursor lifetime are required")
	}
	h := &Handler{core: c, verify: verify, cursorTTL: cursorTTL}
	if err := h.configure(options); err != nil {
		return nil, err
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
	case "/api/v1/fs/directories", "/api/v1/fs/operations", "/api/v1/mounts/stop", "/api/v1/mounts/restore", "/api/v1/tasks/cancel":
		allow = "POST"
	case "/api/v1/mounts", "/api/v1/tasks/task":
		allow = "GET"
	case "/api/v1/mounts/definition":
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
	if r.URL.Path != "/api/v1/info" {
		if err := core.ValidatePath(target); err != nil {
			fail(err)
			return
		}
	}
	if mutation {
		result, changeErr := h.change(r, principal, target)
		*effect = result.Effect
		if changeErr != nil {
			if r.Method == http.MethodPut && r.Header.Get("If-None-Match") == "*" && result.Effect == core.Unchanged && core.CodeOf(changeErr) == core.AlreadyExists {
				writeStatusProblem(w, r, id, 412, "precondition_failed", effect)
			} else {
				fail(changeErr)
			}
			return
		}
		status := http.StatusCreated
		if r.Method == http.MethodDelete {
			status = http.StatusOK
		}
		writeJSON(w, status, map[string]string{"effect": result.Effect.String()})
		return
	}
	conditions, err := parseConditions(r.Header)
	if err != nil {
		fail(err)
		return
	}
	if r.URL.Path == "/api/v1/info" {
		if status := conditions.status(); status != 0 {
			writeConditional(w, r, id, status)
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
			"components":                map[string]bool{"control": h.options.Control != nil, "tasks": h.options.Queue != nil, "inputs": h.options.Inputs != nil},
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
	for _, name := range []string{"If-Match", "If-Range", "If-Modified-Since", "If-Unmodified-Since", "Range", "Content-Range"} {
		if _, present := r.Header[http.CanonicalHeaderKey(name)]; present {
			return refused, core.Fail(core.InvalidRequest, nil)
		}
	}
	if values, present := r.Header["If-None-Match"]; present {
		if r.Method != http.MethodPut || len(values) != 1 || values[0] != "*" {
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
		if replace == "true" && r.Header.Get("If-None-Match") != "" {
			return refused, core.Fail(core.InvalidRequest, nil)
		}
		return h.core.Write(r.Context(), p, target, input, length, core.WriteOptions{Replace: replace == "true"})
	case http.MethodPost:
		if r.ContentLength != 0 {
			return refused, core.Fail(core.InvalidRequest, nil)
		}
		return h.core.Mkdir(r.Context(), p, target)
	case http.MethodDelete:
		if r.ContentLength != 0 {
			return refused, core.Fail(core.InvalidRequest, nil)
		}
		return h.core.Remove(r.Context(), p, target)
	default:
		return refused, core.Fail(core.UnsupportedOperation, nil)
	}
}

func (h *Handler) entry(w http.ResponseWriter, r *http.Request, id string, principal core.Principal, target string, conditions conditions) error {
	entry, err := h.core.Stat(r.Context(), principal, target)
	if err != nil {
		return err
	}
	if status := conditions.status(); status != 0 {
		writeConditional(w, r, id, status)
		return nil
	}
	writeJSON(w, 200, entry)
	return nil
}
func (h *Handler) entries(w http.ResponseWriter, r *http.Request, id string, principal core.Principal, target string, query url.Values, conditions conditions) error {
	req := core.ListRequest{Path: target, Cursor: query.Get("cursor")}
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
	if status := conditions.status(); status != 0 {
		if req.Cursor != "" {
			if err := h.core.CheckCursor(r.Context(), principal, req); err != nil {
				return err
			}
		} else if req.Limit > core.MaxPage {
			return core.Fail(core.InvalidRequest, nil)
		}
		entry, err := h.core.Inspect(r.Context(), principal, core.ListAction, target)
		if err != nil {
			return err
		}
		if entry.Kind != core.Directory {
			return core.Fail(core.NotDirectory, nil)
		}
		writeConditional(w, r, id, status)
		return nil
	}
	page, err := h.core.List(r.Context(), principal, req)
	if err != nil {
		return err
	}
	writeJSON(w, 200, page)
	return nil
}
func (h *Handler) content(w http.ResponseWriter, r *http.Request, id string, principal core.Principal, target string, conditions conditions) error {
	if r.Method == http.MethodHead || conditions.status() != 0 {
		entry, err := h.core.Inspect(r.Context(), principal, core.ReadAction, target)
		if err != nil {
			return err
		}
		if entry.Kind != core.File {
			return core.Fail(core.NotFile, nil)
		}
		if status := conditions.status(); status != 0 {
			writeConditional(w, r, id, status)
			return nil
		}
		contentHeaders(w, entry.Name, entry.Size)
		w.WriteHeader(200)
		return nil
	}
	var err error
	var wanted *core.Range
	if r.Header.Get("If-Range") == "" {
		wanted, err = parseRange(r.Header)
		if err != nil {
			return err
		}
	}
	opened, err := h.core.Open(r.Context(), principal, target, wanted)
	if err != nil {
		return err
	}
	contentHeaders(w, opened.Entry.Name, opened.Length)
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
		if h.OnStreamError != nil {
			h.OnStreamError(id, err)
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
func writeConditional(w http.ResponseWriter, r *http.Request, id string, status int) {
	if status == http.StatusNotModified {
		w.WriteHeader(status)
	} else {
		writeStatusProblem(w, r, id, status, "precondition_failed", nil)
	}
}
func writeProblem(w http.ResponseWriter, r *http.Request, id string, err error, effect *core.Effect) {
	if r.Context().Err() != nil {
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

type conditions struct {
	ifMatch, ifNone     bool
	matchStar, noneStar bool
}

func (c conditions) status() int {
	if c.ifMatch && !c.matchStar {
		return http.StatusPreconditionFailed
	}
	if c.ifNone && c.noneStar {
		return http.StatusNotModified
	}
	return 0
}
func parseConditions(h http.Header) (conditions, error) {
	var c conditions
	for _, name := range []string{"If-Match", "If-None-Match"} {
		v, ok := h[http.CanonicalHeaderKey(name)]
		if !ok {
			continue
		}
		star, err := validTags(strings.Join(v, ","))
		if err != nil {
			return c, err
		}
		if name == "If-Match" {
			c.ifMatch = true
			c.matchStar = star
		} else {
			c.ifNone = true
			c.noneStar = star
		}
	}
	return c, nil
}
func validTags(value string) (bool, error) {
	s := strings.TrimSpace(value)
	if s == "*" {
		return true, nil
	}
	if s == "" {
		return false, core.Fail(core.InvalidRequest, nil)
	}
	for {
		if strings.HasPrefix(s, "W/") {
			s = s[2:]
		}
		if len(s) < 2 || s[0] != '"' {
			return false, core.Fail(core.InvalidRequest, nil)
		}
		end := strings.IndexByte(s[1:], '"')
		if end < 0 {
			return false, core.Fail(core.InvalidRequest, nil)
		}
		end++
		for _, b := range []byte(s[1:end]) {
			if b < 0x21 || b == 0x7f {
				return false, core.Fail(core.InvalidRequest, nil)
			}
		}
		s = strings.TrimSpace(s[end+1:])
		if s == "" {
			return false, nil
		}
		if s[0] != ',' {
			return false, core.Fail(core.InvalidRequest, nil)
		}
		s = strings.TrimSpace(s[1:])
		if s == "" {
			return false, core.Fail(core.InvalidRequest, nil)
		}
	}
}
func parseRange(h http.Header) (*core.Range, error) {
	values := h.Values("Range")
	if len(values) == 0 {
		return nil, nil
	}
	if len(values) != 1 {
		return nil, core.Fail(core.InvalidRange, nil)
	}
	raw := strings.TrimSpace(values[0])
	unit, spec, ok := strings.Cut(raw, "=")
	if !ok {
		return nil, core.Fail(core.InvalidRange, nil)
	}
	if !strings.EqualFold(strings.TrimSpace(unit), "bytes") {
		return nil, nil
	}
	if strings.Contains(spec, ",") {
		return nil, nil
	}
	first, last, ok := strings.Cut(strings.TrimSpace(spec), "-")
	if !ok {
		return nil, core.Fail(core.InvalidRange, nil)
	}
	number := func(s string) (int64, error) {
		if s == "" {
			return 0, errors.New("missing integer")
		}
		for _, r := range s {
			if r < '0' || r > '9' {
				return 0, errors.New("invalid integer")
			}
		}
		return strconv.ParseInt(s, 10, 64)
	}
	r := core.Range{End: -1}
	if first == "" {
		n, err := number(last)
		if err != nil {
			return nil, core.Fail(core.InvalidRange, err)
		}
		r.Start = n
		r.Suffix = true
	} else {
		n, err := number(first)
		if err != nil {
			return nil, core.Fail(core.InvalidRange, err)
		}
		r.Start = n
		if last != "" {
			n, err = number(last)
			if err != nil {
				return nil, core.Fail(core.InvalidRange, err)
			}
			r.End = n
		}
	}
	if !r.Valid() {
		return nil, core.Fail(core.InvalidRange, nil)
	}
	return &r, nil
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
