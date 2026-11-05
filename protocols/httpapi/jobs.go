package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sort"
	"strconv"
	"unicode/utf8"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/task"
)

// JobRef names a durable domain intent. Generation fences ID reuse; it grants
// no authority and is independent of task IDs and checkpoint revisions.
type JobRef struct {
	Source     string `json:"source"`
	ID         string `json:"id"`
	Generation string `json:"generation"`
}
type JobView struct {
	JobRef
	Kind    string            `json:"kind"`
	Version int               `json:"version"`
	State   string            `json:"state"`
	Owner   core.Principal    `json:"owner,omitempty"`
	Actions map[string]string `json:"actions"`
	Details any               `json:"details"`
}
type JobPage struct {
	Items []JobView `json:"items"`
	Next  string    `json:"next,omitempty"`
}

// JobResult is an invocation receipt, not a fresh read of committed state.
// For example a forget receipt does not invent a stored "forgotten" job.
type JobResult struct {
	JobRef
	Action  string `json:"action"`
	Receipt any    `json:"receipt"`
}

// JobSource adapts an existing durable owner. Callbacks own authorization and
// safe wire projection. List bounds scanned metadata, including hidden entries;
// it may return an empty page with Next. No callback closes host-owned services.
// Actions defer domain effects and generation checks to Work.Run, never Prepare.
// Returned action results use JobResult; native checkpoints never enter views.
type JobSource struct {
	Kind    string
	Version int
	Create  func(context.Context, core.Principal, string, json.RawMessage) (JobView, error)
	List    func(context.Context, core.Principal, string, int) (JobPage, error)
	Get     func(context.Context, core.Principal, string) (JobView, error)
	Actions map[string]TaskKind
}

// JobKey declares only conflicts the domain can enforce. Resource is a stable
// comparable identity (for example a service pointer or configured Pool scope).
// An empty ID serializes the entire resource. Compose JobTaskConflicts with
// FileTaskConflicts in the host's existing queue policy.
type JobKey struct {
	Resource any
	ID       string
}

func JobTaskConflicts(a, b any) bool {
	left, ok := a.(JobKey)
	if !ok {
		return false
	}
	right, ok := b.(JobKey)
	return ok && left.Resource == right.Resource && (left.ID == "" || right.ID == "" || left.ID == right.ID)
}

func (h *Handler) configureJobs(sources map[string]JobSource) error {
	h.jobs = make(map[string]JobSource, len(sources))
	h.jobKinds = make(map[string]TaskKind)
	for name, source := range sources {
		if name == "" || !utf8.ValidString(name) || source.Kind == "" || source.Version < 1 || source.Create == nil || source.List == nil || source.Get == nil || len(source.Actions) == 0 {
			return errors.New("job source requires a name, format, discovery and actions")
		}
		copySource := source
		copySource.Actions = make(map[string]TaskKind, len(source.Actions))
		for action, kind := range source.Actions {
			full := name + "." + action
			if action == "" || !utf8.ValidString(action) || kind.Prepare == nil || kind.Project == nil {
				return errors.New("invalid job action")
			}
			if _, exists := h.kinds[full]; exists {
				return errors.New("job action conflicts with a task kind")
			}
			project := kind.Project
			kind.Project = func(s task.Snapshot) (TaskValues, error) {
				values, err := project(s)
				if result, ok := values.Result.(JobResult); ok {
					result.Source = name
					values.Result = result
				}
				return values, err
			}
			copySource.Actions[action] = kind
			h.jobKinds[full] = kind
			h.kinds[full] = kind
		}
		h.jobs[name] = copySource
	}
	return nil
}
func jobView(source string, s JobSource, v JobView) JobView {
	v.Source = source
	v.Kind = s.Kind
	v.Version = s.Version
	v.Actions = make(map[string]string, len(s.Actions))
	for action := range s.Actions {
		v.Actions[action] = source + "." + action
	}
	return v
}
func (h *Handler) jobDescriptions() []map[string]any {
	names := make([]string, 0, len(h.jobs))
	for name := range h.jobs {
		names = append(names, name)
	}
	sort.Strings(names)
	views := make([]map[string]any, 0, len(names))
	for _, name := range names {
		s := h.jobs[name]
		views = append(views, map[string]any{"source": name, "kind": s.Kind, "version": s.Version, "actions": jobView(name, s, JobView{}).Actions})
	}
	return views
}
func (h *Handler) jobRoute(w http.ResponseWriter, r *http.Request, id string, p core.Principal) {
	var result any
	query, err := singleQuery(r, "source", "id", "generation", "after", "limit")
	if err != nil {
		writeDomainProblem(w, r, id, err, nil)
		return
	}
	name := query.Get("source")
	source, ok := h.jobs[name]
	if !ok {
		writeDomainProblem(w, r, id, core.Fail(core.NotFound, nil), nil)
		return
	}
	if r.URL.Path == "/api/v1/jobs" {
		limit, e := strconv.Atoi(query.Get("limit"))
		if e != nil || limit < 1 || limit > core.MaxPage || query.Has("id") || query.Has("generation") {
			err = core.Fail(core.InvalidRequest, nil)
		} else if err = noBody(r); err == nil {
			var page JobPage
			page, err = source.List(r.Context(), p, query.Get("after"), limit)
			for i := range page.Items {
				page.Items[i] = jobView(name, source, page.Items[i])
			}
			if page.Items == nil {
				page.Items = []JobView{}
			}
			result = page
		}
	} else {
		jobID := query.Get("id")
		if jobID == "" || query.Has("after") || query.Has("limit") || (r.Method == http.MethodPut && query.Has("generation")) {
			err = core.Fail(core.InvalidRequest, nil)
		} else {
			var view JobView
			if r.Method == http.MethodPut {
				var request struct {
					Version int             `json:"version"`
					Input   json.RawMessage `json:"input"`
				}
				err = h.readJSON(w, r, &request)
				if err == nil && (request.Version != source.Version || len(request.Input) == 0) {
					err = core.Fail(core.InvalidRequest, nil)
				}
				if err == nil {
					view, err = source.Create(r.Context(), p, jobID, request.Input)
				}
			} else if err = noBody(r); err == nil {
				view, err = source.Get(r.Context(), p, jobID)
			}
			if err == nil && query.Has("generation") && query.Get("generation") != view.Generation {
				err = core.Fail(core.AlreadyExists, nil)
				view = JobView{}
			}
			if view.ID != "" {
				result = jobView(name, source, view)
			}
		}
	}
	if err != nil {
		writeDomainProblem(w, r, id, err, result)
		return
	}
	respond(w, r, id, 200, result)
}
