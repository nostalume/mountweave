package httpapi_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/iotest"
	"time"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/control"
	"github.com/nostalume/mountweave/drivers/local"
	"github.com/nostalume/mountweave/protocols/httpapi"
	"github.com/nostalume/mountweave/task"
)

func identity(_ context.Context, token string) (core.Principal, error) {
	switch token {
	case "write-token":
		return "writer", nil
	case "read-token":
		return "reader", nil
	case "manage-token":
		return "manager", nil
	case "view-token":
		return "viewer", nil
	}
	return "", errors.New("private identity failure")
}
func taskPolicy(_ context.Context, p core.Principal, action httpapi.TaskAction, info httpapi.TaskInfo) error {
	if action == httpapi.SubmitTask || action == httpapi.ListTasks || p == info.Owner {
		return nil
	}
	return errors.New("private task policy")
}
func jsonCall(t *testing.T, h http.Handler, method, path, token string, value any) *httptest.ResponseRecorder {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return request(t, h, method, path, token, map[string]string{"Content-Type": "application/json"}, bytes.NewReader(data))
}
func wire(t *testing.T, w *httptest.ResponseRecorder, status int) map[string]any {
	t.Helper()
	if w.Code != status {
		t.Fatalf("HTTP %d, want %d: %s", w.Code, status, w.Body.String())
	}
	var out map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
		t.Fatal(err, w.Body.String())
	}
	return out
}
func operation(kind string, input any) map[string]any {
	return map[string]any{"kind": kind, "input": input}
}
func untilTask(t *testing.T, h http.Handler, id string) map[string]any {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		v := wire(t, request(t, h, "GET", "/api/v1/tasks/task?id="+id, "write-token", nil), 200)
		if v["state"] == "finished" {
			return v
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("task did not finish")
	return nil
}
func awaitSignal(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatal("boundary was not reached")
	}
}

func TestVisibleNamespaceDelivery(t *testing.T) {
	c, _, _ := setup(t)
	q, err := task.New(task.Options{Concurrency: 2, Capacity: 4, Conflicts: httpapi.FileTaskConflicts})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := q.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	h, err := httpapi.New(c, identity, time.Minute, httpapi.Options{Queue: q, MaxTasks: 4, MaxJSONBytes: 4096, AuthorizeTask: taskPolicy})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := h.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	roots := []string{t.TempDir(), t.TempDir()}
	for i, prefix := range []string{"/sources/a", "/sources/b"} {
		d, err := local.New(roots[i])
		if err != nil {
			t.Fatal(err)
		}
		if _, err = c.Install(prefix, d); err != nil {
			_ = d.Close()
			t.Fatal(err)
		}
		if err = os.WriteFile(filepath.Join(roots[i], "same"), []byte(prefix), 0600); err != nil {
			t.Fatal(err)
		}
	}
	copyInput := func(target string) map[string]any {
		return operation("fs.copy_tree", map[string]any{"source": "/sources", "target": target, "max_entries": 20, "max_plan_bytes": 1024, "concurrency": 2})
	}
	syncResult := wire(t, jsonCall(t, h, "POST", "/api/v1/fs/operations", "write-token", copyInput("/files/visible-sync")), 200)
	id := wire(t, jsonCall(t, h, "POST", "/api/v1/tasks", "write-token", copyInput("/files/visible-queued")), 202)["id"].(string)
	queued := untilTask(t, h, id)
	if queued["error"] != nil || !reflect.DeepEqual(syncResult, queued["result"]) {
		t.Fatal(syncResult, queued)
	}
	clear := operation("fs.remove_tree", map[string]any{"path": "/sources", "contents_only": true, "max_entries": 20, "max_plan_bytes": 1024, "concurrency": 2})
	contractExample(t, "FileOperationRequest", clear)
	syncResult = wire(t, jsonCall(t, h, "POST", "/api/v1/fs/operations", "write-token", clear), 200)
	for _, root := range roots {
		if err := os.WriteFile(filepath.Join(root, "same"), []byte("again"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	contractExample(t, "TaskRequest", clear)
	id = wire(t, jsonCall(t, h, "POST", "/api/v1/tasks", "write-token", clear), 202)["id"].(string)
	queued = untilTask(t, h, id)
	if queued["error"] != nil || !reflect.DeepEqual(syncResult, queued["result"]) || len(syncResult["items"].([]any)) != 2 {
		t.Fatal(syncResult, queued)
	}
	for _, root := range roots {
		entries, err := os.ReadDir(root)
		if err != nil || len(entries) != 0 {
			t.Fatal(entries, err)
		}
	}
}

func TestSharedFileDeliveryAndPublication(t *testing.T) {
	c, _, root := setup(t)
	q, err := task.New(task.Options{Concurrency: 2, Capacity: 4, Conflicts: httpapi.FileTaskConflicts})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := q.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	kinds := httpapi.FileTaskKinds(c)
	saved := kinds["fs.copy"]
	var executions atomic.Int32
	saved.Prepare = func(ctx context.Context, p core.Principal, raw json.RawMessage) (task.Work, error) {
		work, err := kinds["fs.copy"].Prepare(ctx, p, raw)
		if err != nil {
			return work, err
		}
		run := work.Run
		work.Run = func(ctx context.Context, report func(any)) (any, error) { executions.Add(1); return run(ctx, report) }
		work.Complete = func(context.Context, task.Snapshot) error { return errors.New("private save failure") }
		return work, nil
	}
	h, err := httpapi.New(c, identity, time.Minute, httpapi.Options{Queue: q, MaxTasks: 4, MaxJSONBytes: 4096, AuthorizeTask: taskPolicy, TaskKinds: map[string]httpapi.TaskKind{"host.saved-copy": saved}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := h.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	batch := func(name string) map[string]any {
		return operation("fs.copy_batch", map[string]any{"items": []any{
			map[string]any{"source": "/files/data.bin", "target": "/files/" + name},
			map[string]any{"source": "/files/data.bin", "target": "/files/" + name},
			map[string]any{"source": "/files/data.bin", "target": "/files/unstarted"}}, "concurrency": 1})
	}
	synchronous := wire(t, jsonCall(t, h, "POST", "/api/v1/fs/operations", "write-token", batch("sync")), 409)
	contractExample(t, "FileOperationRequest", batch("sync"))
	contractExample(t, "TaskRequest", batch("async"))
	contractExample(t, "Problem", synchronous)
	contractExample(t, "FileResult", synchronous["result"])
	id := wire(t, jsonCall(t, h, "POST", "/api/v1/tasks", "write-token", batch("async")), 202)["id"].(string)
	queued := untilTask(t, h, id)
	contractExample(t, "TaskView", queued)
	if synchronous["code"] != "already_exists" || queued["error"] != "already_exists" || !reflect.DeepEqual(synchronous["result"], queued["result"]) {
		t.Fatalf("delivery changed receipts: %v / %v", synchronous, queued)
	}
	if _, err := os.Stat(filepath.Join(root, "unstarted")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("started suffix", err)
	}
	// Other operations use the same prepared behavior and preserve endpoint facts.
	if err := os.WriteFile(filepath.Join(root, "moved"), []byte("old"), 0600); err != nil {
		t.Fatal(err)
	}
	moved := wire(t, jsonCall(t, h, "POST", "/api/v1/fs/operations", "write-token", operation("fs.move_batch", map[string]any{"items": []any{map[string]any{"source": "/files/sync", "target": "/files/moved", "options": core.MoveOptions{Existing: core.Replace}}}})), 200)
	moveItem := moved["items"].([]any)[0].(map[string]any)
	if moveItem["source_effect"] != "applied" || moveItem["removed"].(map[string]any)["effect"] != "applied" {
		t.Fatal(moved)
	}
	contractExample(t, "FileResult", moved)
	wire(t, jsonCall(t, h, "POST", "/api/v1/fs/operations", "write-token", operation("fs.mkdir", map[string]any{"path": "/files/tree"})), 200)
	if err := os.WriteFile(filepath.Join(root, "tree/a"), []byte("tree"), 0600); err != nil {
		t.Fatal(err)
	}
	copyTree := operation("fs.copy_tree", map[string]any{"source": "/files/tree", "target": "/files/copied", "max_entries": 10, "max_plan_bytes": 100, "concurrency": 2, "existing": "replace", "policy": core.WritePolicy{}})
	contractExample(t, "TaskRequest", copyTree)
	id = wire(t, jsonCall(t, h, "POST", "/api/v1/tasks", "write-token", copyTree), 202)["id"].(string)
	if result := untilTask(t, h, id); result["error"] != nil || len(result["result"].(map[string]any)["items"].([]any)) != 2 {
		t.Fatal(result)
	} else {
		contractExample(t, "TaskView", result)
		contractExample(t, "FileResult", result["result"])
	}
	removal := operation("fs.remove_tree", map[string]any{"path": "/files/copied", "max_entries": 10, "max_plan_bytes": 100, "scope": "subtree"})
	contractExample(t, "FileOperationRequest", removal)
	wire(t, jsonCall(t, h, "POST", "/api/v1/fs/operations", "write-token", removal), 200)
	removal["input"].(map[string]any)["path"] = "/files/tree"
	contractExample(t, "TaskRequest", removal)
	id = wire(t, jsonCall(t, h, "POST", "/api/v1/tasks", "write-token", removal), 202)["id"].(string)
	if result := untilTask(t, h, id); result["error"] != nil {
		t.Fatal(result)
	}
	removal["input"].(map[string]any)["scope"] = "auto"
	wire(t, jsonCall(t, h, "POST", "/api/v1/tasks", "write-token", removal), 400)
	id = wire(t, jsonCall(t, h, "POST", "/api/v1/tasks", "write-token", operation("host.saved-copy", map[string]any{"source": "/files/data.bin", "target": "/files/saved"})), 202)["id"].(string)
	result := untilTask(t, h, id)
	if result["error"] != nil || result["completion_error"] != "internal_error" || result["result"].(map[string]any)["effect"] != "applied" || executions.Load() != 1 {
		t.Fatal(result)
	}
	if strings.Contains(fmtJSON(result), "private") {
		t.Fatal("private error escaped")
	}
	contractExample(t, "ApiInfo", wire(t, request(t, h, "GET", "/api/v1/info", "write-token", nil), 200))
	// An explicit replacement retains old bytes when receiving the body fails.
	pub, stage := t.TempDir(), t.TempDir()
	d, err := local.New(pub, local.Options{StagingDirectory: stage, MaxStagingBytes: 1024})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = c.Install("/publish", d); err != nil {
		_ = d.Close()
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(pub, "a"), []byte("old"), 0600); err != nil {
		t.Fatal(err)
	}
	bad := io.MultiReader(strings.NewReader("prefix"), iotest.ErrReader(io.ErrNoProgress))
	w := request(t, h, "PUT", "/api/v1/fs/content?path=/publish/a&replace=true", "write-token", nil, bad)
	if v := wire(t, w, 502); v["effect"] != "unchanged" {
		t.Fatal(v)
	}
	if b, err := os.ReadFile(filepath.Join(pub, "a")); err != nil || string(b) != "old" {
		t.Fatal(string(b), err)
	}
	if v := wire(t, request(t, h, "PUT", "/api/v1/fs/content?path=/publish/a&replace=true", "write-token", nil, strings.NewReader("new")), 201); v["effect"] != "applied" {
		t.Fatal(v)
	}
	input := strings.NewReader("unread")
	wire(t, request(t, h, "PUT", "/api/v1/fs/content?path=/publish/a&replace=true", "write-token", map[string]string{"If-None-Match": "*"}, input), 412)
	if input.Len() != 6 {
		t.Fatal("failed absence predicate consumed input")
	}
	entries, err := os.ReadDir(stage)
	if err != nil || len(entries) != 0 {
		t.Fatal("staging cleanup", entries, err)
	}
	// Invalid structured input is refused before any file mutation.
	wire(t, jsonCall(t, h, "POST", "/api/v1/fs/operations", "write-token", operation("fs.remove", map[string]any{"path": "/files/data.bin", "unexpected": true})), 400)
}
func fmtJSON(v any) string { b, _ := json.Marshal(v); return string(b) }
func contractExample(t *testing.T, name string, value any) {
	t.Helper()
	t.Logf("CONTRACT %s %s", name, fmtJSON(value))
}

// Test storage owns independent records; its physical format is not a product
// backend or a crash-durability claim.
type mountStore struct {
	mu     sync.Mutex
	values map[string]json.RawMessage
}

func (s *mountStore) Load(_ context.Context, id string) (control.Definition, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.load(id)
}
func (s *mountStore) load(id string) (control.Definition, error) {
	var d control.Definition
	b := s.values[id]
	if b == nil {
		return d, control.ErrNotFound
	}
	err := json.Unmarshal(b, &d)
	return d, err
}
func (s *mountStore) List(context.Context) ([]control.Definition, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ids := make([]string, 0, len(s.values))
	for id := range s.values {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	out := make([]control.Definition, 0, len(ids))
	for _, id := range ids {
		d, err := s.load(id)
		if err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, nil
}
func (s *mountStore) Commit(_ context.Context, d control.Definition, expected uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	old, err := s.load(d.ID)
	if err != nil && !errors.Is(err, control.ErrNotFound) {
		return err
	}
	if old.Revision != expected {
		return control.ErrConflict
	}
	b, err := json.Marshal(d)
	if err == nil {
		s.values[d.ID] = b
	}
	return err
}

func TestManagedHTTPTasksAndBindingHandoff(t *testing.T) {
	c, _, root := setup(t)
	source1, source2 := t.TempDir(), t.TempDir()
	for _, dir := range []string{source1, source2} {
		if err := os.Mkdir(filepath.Join(dir, "tree"), 0700); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(source1, "tree/a"), []byte("old"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(source2, "tree/a"), []byte("new"), 0600); err != nil {
		t.Fatal(err)
	}
	store := &mountStore{values: make(map[string]json.RawMessage)}
	factory := control.Factory{Build: func(_ context.Context, d control.Definition) (core.Driver, error) {
		var input struct {
			Root string `json:"root"`
		}
		if err := json.Unmarshal(d.Config, &input); err != nil {
			return nil, err
		}
		return local.New(input.Root)
	}}
	m, err := control.New(control.Options{Core: c, Store: store, Factories: map[string]control.Factory{"local": factory}, Authorize: func(_ context.Context, p core.Principal, a control.Action, id string) error {
		if p == "manager" || p == "viewer" && (a == control.ListAction || a == control.LoadAction && id == "source") {
			return nil
		}
		return errors.New("private management policy")
	}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := m.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	q, err := task.New(task.Options{Concurrency: 1, Capacity: 4, Conflicts: httpapi.FileTaskConflicts})
	if err != nil {
		t.Fatal(err)
	}
	entered, cleaning, ack := make(chan struct{}), make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(ack) })
	t.Cleanup(func() {
		unblock()
		if err := q.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	var prepares atomic.Int32
	gate := httpapi.TaskKind{Prepare: func(context.Context, core.Principal, json.RawMessage) (task.Work, error) {
		prepares.Add(1)
		return task.Work{
			Run: func(ctx context.Context, report func(any)) (any, error) {
				report("waiting")
				close(entered)
				<-ctx.Done()
				return "partial", ctx.Err()
			},
			Cleanup: func() error { close(cleaning); <-ack; return nil },
		}, nil
	}, Project: func(s task.Snapshot) (httpapi.TaskValues, error) {
		return httpapi.TaskValues{Progress: s.Progress, Result: s.Result}, nil
	}}
	h, err := httpapi.New(c, identity, time.Minute, httpapi.Options{Control: m, Queue: q, MaxTasks: 2, MaxJSONBytes: 4096, AuthorizeTask: taskPolicy, TaskKinds: map[string]httpapi.TaskKind{"host.gate": gate}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		unblock()
		if err := h.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	server := httptest.NewServer(h)
	t.Cleanup(server.Close)
	call := func(method, path, token string, value any) *httptest.ResponseRecorder {
		var body io.Reader
		if value != nil {
			b, _ := json.Marshal(value)
			body = bytes.NewReader(b)
		}
		req, err := http.NewRequest(method, server.URL+path, body)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Authorization", "Bearer "+token)
		if value != nil {
			req.Header.Set("Content-Type", "application/json")
		}
		response, err := server.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		b, err := io.ReadAll(response.Body)
		if err != nil {
			t.Fatal(err)
		}
		w := httptest.NewRecorder()
		w.Code = response.StatusCode
		w.Body.Write(b)
		return w
	}
	save := func(dir string, revision uint64) map[string]any {
		return map[string]any{"expected_revision": revision, "prefix": "/managed", "driver": "local", "enabled": true, "config": map[string]any{"root": dir, "private": "private-native-secret"}, "secrets": map[string]string{"credential": "private-reference"}, "aliases": []any{map[string]any{"prefix": "/view", "base": "/tree"}}}
	}
	wire(t, call("PUT", "/api/v1/mounts/definition?id=source", "write-token", save(source1, 0)), 403)
	wire(t, call("PUT", "/api/v1/mounts/definition?id=source", "manage-token", save(source1, 0)), 200)
	definition := wire(t, call("GET", "/api/v1/mounts/definition?id=source", "manage-token", nil), 200)
	contractExample(t, "Definition", definition)
	if definition["config"] != nil || strings.Contains(fmtJSON(definition), "private") {
		t.Fatal("opaque config escaped", definition)
	}
	wire(t, call("PUT", "/api/v1/mounts/definition?id=source", "manage-token", save(source1, 0)), 409)
	wire(t, call("POST", "/api/v1/mounts/activation?id=source", "manage-token", map[string]any{"revision": 1}), 200)
	// List and individual load authority are separate; hidden desired input stays hidden.
	hidden := save(source1, 0)
	hidden["prefix"] = "/hidden"
	hidden["aliases"] = []any{}
	hidden["enabled"] = false
	wire(t, call("PUT", "/api/v1/mounts/definition?id=hidden", "manage-token", hidden), 200)
	listed := call("GET", "/api/v1/mounts", "view-token", nil)
	var defs []map[string]any
	if err = json.Unmarshal(listed.Body.Bytes(), &defs); err != nil || listed.Code != 200 || len(defs) != 1 || defs[0]["id"] != "source" {
		t.Fatal(listed.Code, defs, err)
	}
	wire(t, call("GET", "/api/v1/mounts/definition?id=hidden", "view-token", nil), 403)
	// Once accepted, disconnecting the submitting HTTP request does not cancel work.
	requestCtx, cancel := context.WithCancel(context.Background())
	data, _ := json.Marshal(operation("host.gate", map[string]any{}))
	req, _ := http.NewRequestWithContext(requestCtx, "POST", server.URL+"/api/v1/tasks", bytes.NewReader(data))
	req.Header.Set("Authorization", "Bearer write-token")
	req.Header.Set("Content-Type", "application/json")
	response, err := server.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	var accepted map[string]any
	if err = json.NewDecoder(response.Body).Decode(&accepted); err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	cancel()
	gateID := accepted["id"].(string)
	awaitSignal(t, entered)
	contractExample(t, "TaskInfo", accepted)
	select {
	case <-cleaning:
		t.Fatal("request cancellation stopped accepted work")
	default:
	}
	copyID := wire(t, call("POST", "/api/v1/tasks", "write-token", operation("fs.copy", map[string]any{"source": "/view/a", "target": "/files/stale"})), 202)["id"].(string)
	wire(t, call("POST", "/api/v1/tasks", "write-token", operation("host.gate", map[string]any{})), 429)
	if prepares.Load() != 1 {
		t.Fatal("record exhaustion prepared another resource")
	}
	wire(t, call("PUT", "/api/v1/mounts/definition?id=source", "manage-token", save(source2, 1)), 200)
	before := wire(t, call("GET", "/api/v1/mounts/activation?id=source", "manage-token", nil), 200)
	contractExample(t, "Activation", before)
	if before["revision"] != float64(1) || before["active"] != true {
		t.Fatal("save altered activation", before)
	}
	wire(t, call("POST", "/api/v1/mounts/activation?id=source", "manage-token", map[string]any{"revision": 2}), 200)
	wire(t, call("GET", "/api/v1/tasks/task?id="+gateID, "read-token", nil), 403)
	wire(t, call("POST", "/api/v1/tasks/cancel?id="+gateID, "read-token", nil), 403)
	wire(t, call("POST", "/api/v1/tasks/cancel?id="+gateID, "write-token", nil), 202)
	awaitSignal(t, cleaning)
	finishing := wire(t, call("GET", "/api/v1/tasks/task?id="+gateID, "write-token", nil), 200)
	if finishing["state"] != "finishing" || finishing["result"] != "partial" || finishing["error"] != "cancelled" {
		t.Fatal(finishing)
	}
	unblock()
	untilTask(t, h, gateID)
	stale := untilTask(t, h, copyID)
	if stale["error"] != "mount_unavailable" || stale["result"].(map[string]any)["effect"] != "unchanged" {
		t.Fatal(stale)
	}
	if _, err = os.Stat(filepath.Join(root, "stale")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("queued work retargeted", err)
	}
	wire(t, call("POST", "/api/v1/mounts/stop?id=source", "manage-token", nil), 200)
	restored := call("POST", "/api/v1/mounts/restore", "manage-token", nil)
	if restored.Code != 200 || strings.Contains(restored.Body.String(), "private") {
		t.Fatal(restored.Code, restored.Body.String())
	}
	body := call("GET", "/api/v1/fs/content?path=/view/a", "write-token", nil)
	if body.Code != 200 || body.Body.String() != "new" {
		t.Fatal(body.Code, body.Body.String())
	}
	// A completed result can be evicted for new admission; active records cannot.
	next := wire(t, call("POST", "/api/v1/tasks", "write-token", operation("fs.remove", map[string]any{"path": "/files/async-missing"})), 202)["id"].(string)
	untilTask(t, h, next)
	wire(t, call("GET", "/api/v1/tasks/task?id="+gateID, "write-token", nil), 404)
}

func TestRejectedTaskCleanupAndAdapterShutdown(t *testing.T) {
	c, _, _ := setup(t)
	q, _ := task.New(task.Options{Concurrency: 1, Capacity: 1})
	entered := make(chan struct{})
	_, err := q.Submit(context.Background(), task.Work{Run: func(ctx context.Context, _ func(any)) (any, error) {
		close(entered)
		<-ctx.Done()
		return nil, ctx.Err()
	}})
	if err != nil {
		t.Fatal(err)
	}
	awaitSignal(t, entered)
	t.Cleanup(func() {
		if err := q.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	cleaning, ack := make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(ack) })
	t.Cleanup(unblock)
	var runs, closes atomic.Int32
	failure := errors.New("private unadopted cleanup failure")
	kind := httpapi.TaskKind{Prepare: func(context.Context, core.Principal, json.RawMessage) (task.Work, error) {
		return task.Work{Run: func(context.Context, func(any)) (any, error) { runs.Add(1); return nil, nil }, Cleanup: func() error { closes.Add(1); close(cleaning); <-ack; return failure }}, nil
	}, Project: func(task.Snapshot) (httpapi.TaskValues, error) { return httpapi.TaskValues{}, nil }}
	h, err := httpapi.New(c, identity, time.Minute, httpapi.Options{Queue: q, MaxTasks: 1, AuthorizeTask: taskPolicy, TaskKinds: map[string]httpapi.TaskKind{"host.owned": kind}})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		done <- jsonCall(t, h, "POST", "/api/v1/tasks", "write-token", operation("host.owned", map[string]any{}))
	}()
	awaitSignal(t, cleaning)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err = h.Close(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal("close acknowledged pending cleanup", err)
	}
	unblock()
	response := <-done
	if v := wire(t, response, 429); v["code"] != "capacity_exceeded" || v["result"].(map[string]any)["cleanup_error"] != "internal_error" || strings.Contains(response.Body.String(), "private") {
		t.Fatal(v)
	}
	if err = h.Close(context.Background()); !errors.Is(err, failure) || runs.Load() != 0 || closes.Load() != 1 {
		t.Fatal("rejected ownership lost/retried", err, runs.Load(), closes.Load())
	}
	wire(t, request(t, h, "GET", "/api/v1/info", "write-token", nil), 503)
}
