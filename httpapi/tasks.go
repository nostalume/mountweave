package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"sync"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/control"
	"github.com/nostalume/mountweave/input"
	"github.com/nostalume/mountweave/task"
)

type TaskValues struct {
	Progress any `json:"progress"`
	Result   any `json:"result"`
}

// TaskKind owns wire decoding and redacted progress/result projection. Prepare
// defers business effects to Run and transfers resources only after successful
// queue admission; on rejection the HTTP adapter calls Work.Cleanup. A failed
// Prepare owns its partial resources. Run must use its task lifetime; borrowed
// request resources require explicit ownership transfer before acceptance.
type TaskKind struct {
	Prepare func(context.Context, core.Principal, json.RawMessage) (task.Work, error)
	Project func(task.Snapshot) (TaskValues, error)
}
type TaskAction string

const (
	SubmitTask TaskAction = "submit"
	ReadTask   TaskAction = "read"
	CancelTask TaskAction = "cancel"
	ListTasks  TaskAction = "list"
)

type TaskInfo struct {
	ID    string         `json:"id"`
	Kind  string         `json:"kind"`
	Owner core.Principal `json:"owner"`
}

// Options composes optional consumers; it does not configure Core or Driver.
// Zero MaxJSONBytes imposes no additional adapter limit. MaxTasks bounds active,
// reserved and completed HTTP records; completed records evict oldest-first by
// acceptance order. The queue and all injected components remain host-owned.
type Options struct {
	Control        *control.Controller
	ProjectConfig  func(context.Context, core.Principal, control.Definition) (any, error)
	Queue          *task.Queue
	TaskKinds      map[string]TaskKind
	AuthorizeTask  func(context.Context, core.Principal, TaskAction, TaskInfo) error
	MaxTasks       int
	MaxJSONBytes   int64
	Inputs         *input.Store
	AuthorizeInput func(context.Context, core.Principal, InputAction, InputInfo) error
}
type taskRecord struct {
	info   TaskInfo
	kind   TaskKind
	handle *task.Handle
	order  uint64
}
type taskIndex struct {
	mu       sync.Mutex
	records  map[string]*taskRecord
	reserved int
	sequence uint64
}

var errTaskCleanup = errors.New("task adapter retains failed cleanup")

func (h *Handler) configure(options []Options) error {
	if len(options) > 1 {
		return errors.New("at most one HTTP options value is accepted")
	}
	if len(options) == 1 {
		h.options = options[0]
	}
	o := h.options
	if o.MaxJSONBytes < 0 || o.MaxTasks < 0 {
		return errors.New("HTTP limits must be nonnegative")
	}
	if o.Queue != nil && (o.MaxTasks == 0 || o.AuthorizeTask == nil) {
		return errors.New("task HTTP delivery requires a record budget and authorizer")
	}
	h.files = FileTaskKinds(h.core)
	if o.Inputs != nil {
		if o.AuthorizeInput == nil {
			return errors.New("input HTTP delivery requires an authorizer")
		}
		h.files["fs.write"] = h.inputWriteKind()
	}
	h.kinds = make(map[string]TaskKind, len(h.files)+len(o.TaskKinds))
	for name, kind := range h.files {
		h.kinds[name] = kind
	}
	for name, kind := range o.TaskKinds {
		if name == "" || kind.Prepare == nil || kind.Project == nil {
			return errors.New("task kind, prepare and projection are required")
		}
		if _, exists := h.files[name]; exists || name == "fs.write" {
			return errors.New("host task kinds cannot replace built-in file semantics")
		}
		h.kinds[name] = kind
	}
	h.tasks.records = make(map[string]*taskRecord)
	h.closeDone = make(chan struct{})
	return nil
}

func (h *Handler) taskNames() []string {
	names := make([]string, 0)
	if h.options.Queue != nil {
		for name := range h.kinds {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	return names
}

func (h *Handler) taskAccess(ctx context.Context, p core.Principal, action TaskAction, info TaskInfo) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := h.options.AuthorizeTask(ctx, p, action, info); err != nil {
		return core.Fail(core.PermissionDenied, err)
	}
	return nil
}

func (h *Handler) reserve() error {
	h.tasks.mu.Lock()
	defer h.tasks.mu.Unlock()
	if len(h.failed) > 0 {
		return errTaskCleanup
	}
	if len(h.tasks.records)+h.tasks.reserved >= h.options.MaxTasks {
		var oldest *taskRecord
		for _, r := range h.tasks.records {
			if r.handle.Snapshot().State == task.Finished && (oldest == nil || r.order < oldest.order) {
				oldest = r
			}
		}
		if oldest == nil {
			return core.Fail(core.CapacityExceeded, nil)
		}
		delete(h.tasks.records, oldest.info.ID)
	}
	h.tasks.reserved++
	return nil
}
func (h *Handler) releaseReservation() { h.tasks.mu.Lock(); h.tasks.reserved--; h.tasks.mu.Unlock() }

type TaskView struct {
	TaskInfo
	State           task.State `json:"state"`
	CancelRequested bool       `json:"cancel_requested"`
	TaskValues
	Error           string `json:"error,omitempty"`
	CleanupError    string `json:"cleanup_error,omitempty"`
	CompletionError string `json:"completion_error,omitempty"`
}

func viewTask(r *taskRecord) (TaskView, error) {
	s := r.handle.Snapshot()
	values, err := r.kind.Project(s)
	return TaskView{TaskInfo: r.info, State: s.State, CancelRequested: s.CancelRequested, TaskValues: values, Error: errorCode(s.Err), CleanupError: errorCode(s.CleanupErr), CompletionError: errorCode(s.CompletionErr)}, err
}

func (h *Handler) submit(w http.ResponseWriter, r *http.Request, id string, p core.Principal) {
	var input OperationRequest
	if err := h.readJSON(w, r, &input); err != nil {
		writeProblem(w, r, id, err, nil)
		return
	}
	kind, ok := h.kinds[input.Kind]
	if !ok || len(input.Input) == 0 {
		writeProblem(w, r, id, core.Fail(core.InvalidRequest, nil), nil)
		return
	}
	info := TaskInfo{Kind: input.Kind, Owner: p}
	if err := h.taskAccess(r.Context(), p, SubmitTask, info); err != nil {
		writeProblem(w, r, id, err, nil)
		return
	}
	if err := h.reserve(); err != nil {
		writeDomainProblem(w, r, id, err, nil)
		return
	}
	reserved := true
	defer func() {
		if reserved {
			h.releaseReservation()
		}
	}()
	work, err := kind.Prepare(r.Context(), p, input.Input)
	if err != nil {
		writeProblem(w, r, id, err, nil)
		return
	}
	handle, err := h.options.Queue.Submit(r.Context(), work)
	if err != nil {
		var rejection any
		// Retain failed cleanup ownership and report it through Close.
		// Run is never invoked on rejected admission.
		if work.Cleanup != nil {
			cleanupErr := h.finishWork(work.Cleanup)
			if cleanupErr != nil {
				err = errors.Join(err, cleanupErr)
				rejection = map[string]string{"cleanup_error": errorCode(cleanupErr)}
			}
		}
		writeDomainProblem(w, r, id, err, rejection)
		return
	}
	info.ID = handle.Snapshot().ID
	h.tasks.mu.Lock()
	h.tasks.sequence++
	record := &taskRecord{info: info, kind: kind, handle: handle, order: h.tasks.sequence}
	h.tasks.records[info.ID] = record
	h.tasks.reserved--
	reserved = false
	h.tasks.mu.Unlock()
	w.Header().Set("Location", "/api/v1/tasks/task?id="+url.QueryEscape(info.ID))
	// An accepted response does not depend on task result/projection success.
	respond(w, r, id, http.StatusAccepted, info)
}

func (h *Handler) taskRoute(w http.ResponseWriter, r *http.Request, id string, p core.Principal) {
	if h.options.Queue == nil {
		writeProblem(w, r, id, core.Fail(core.UnsupportedOperation, nil), nil)
		return
	}
	query, err := singleQuery(r, "id")
	if err != nil {
		writeProblem(w, r, id, err, nil)
		return
	}
	if r.URL.Path == "/api/v1/tasks" {
		if len(query) != 0 {
			writeProblem(w, r, id, core.Fail(core.InvalidRequest, nil), nil)
			return
		}
		if r.Method == http.MethodPost {
			h.submit(w, r, id, p)
			return
		}
		if err = h.taskAccess(r.Context(), p, ListTasks, TaskInfo{}); err != nil {
			writeProblem(w, r, id, err, nil)
			return
		}
		h.tasks.mu.Lock()
		records := make([]*taskRecord, 0, len(h.tasks.records))
		for _, record := range h.tasks.records {
			records = append(records, record)
		}
		h.tasks.mu.Unlock()
		sort.Slice(records, func(i, j int) bool { return records[i].order < records[j].order })
		// Listing is a bounded summary; large results are fetched individually.
		views := make([]TaskInfo, 0, len(records))
		for _, record := range records {
			if h.taskAccess(r.Context(), p, ReadTask, record.info) == nil {
				views = append(views, record.info)
			}
		}
		respond(w, r, id, 200, views)
		return
	}
	taskID := query.Get("id")
	if taskID == "" {
		writeProblem(w, r, id, core.Fail(core.InvalidRequest, nil), nil)
		return
	}
	h.tasks.mu.Lock()
	record := h.tasks.records[taskID]
	h.tasks.mu.Unlock()
	if record == nil {
		writeProblem(w, r, id, core.Fail(core.NotFound, nil), nil)
		return
	}
	action := ReadTask
	if r.URL.Path == "/api/v1/tasks/cancel" {
		action = CancelTask
	}
	if err = h.taskAccess(r.Context(), p, action, record.info); err != nil {
		writeProblem(w, r, id, err, nil)
		return
	}
	if action == CancelTask {
		if err = noBody(r); err != nil {
			writeProblem(w, r, id, err, nil)
			return
		}
		record.handle.Cancel()
		respond(w, r, id, 202, record.info)
		return
	}
	view, err := viewTask(record)
	if err != nil {
		writeProblem(w, r, id, err, nil)
		return
	}
	respond(w, r, id, 200, view)
}

// failedTask retains only cleanup ownership, not a rejected executable closure.
type failedTask struct {
	cleanup func() error
	err     error
}

// Close stops adapter admission and waits for in-flight HTTP handling before
// reporting failed unadopted cleanup. It never closes host-owned components.
// Stop the HTTP server, then close this adapter, queue, control and Core in order.
func (h *Handler) Close(ctx context.Context) error {
	h.life.Lock()
	h.closed = true
	h.life.Unlock()
	h.closeOnce.Do(func() { go func() { h.requests.Wait(); close(h.closeDone) }() })
	select {
	case <-h.closeDone:
	default:
		select {
		case <-h.closeDone:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	h.tasks.mu.Lock()
	defer h.tasks.mu.Unlock()
	var err error
	for _, failed := range h.failed {
		err = errors.Join(err, failed.err)
	}
	return err
}

func cleanupWork(cleanup func() error) (err error) {
	defer func() {
		if value := recover(); value != nil {
			err = fmt.Errorf("task cleanup panic: %v", value)
		}
	}()
	return cleanup()
}

func (h *Handler) finishWork(cleanup func() error) error {
	if cleanup == nil {
		return nil
	}
	err := cleanupWork(cleanup)
	if err != nil {
		h.tasks.mu.Lock()
		h.failed = append(h.failed, failedTask{cleanup: cleanup, err: err})
		h.tasks.mu.Unlock()
	}
	return err
}
