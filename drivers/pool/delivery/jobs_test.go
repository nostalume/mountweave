package delivery_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/local"
	pool "github.com/nostalume/mountweave/drivers/pool"
	"github.com/nostalume/mountweave/drivers/pool/delivery"
	"github.com/nostalume/mountweave/protocols/httpapi"
	"github.com/nostalume/mountweave/task"
)

func TestMaintenanceProtocolRecovery(t *testing.T) {
	ctx := context.Background()
	opts := pool.Options{MetadataPath: filepath.Join(t.TempDir(), "pool.db"), DataShards: 1, ParityShards: 1, ShardBytes: 8, MaxBufferedBytes: 32, LockTimeout: time.Second}
	roots := []string{t.TempDir(), t.TempDir()}
	open := func() *pool.Driver {
		t.Helper()
		opts.Members = nil
		for n, root := range roots {
			d, err := local.New(root)
			if err != nil {
				t.Fatal(err)
			}
			id := string(rune('a' + n))
			opts.Members = append(opts.Members, pool.Member{ID: id, Identity: root, FailureDomain: id, Driver: d})
		}
		d, err := pool.New(opts)
		if err != nil {
			t.Fatal(err)
		}
		return d
	}
	d := open()
	defer func() {
		if err := d.Close(); err != nil {
			t.Error(err)
		}
	}()
	c, err := core.New(core.Options{Authorize: func(context.Context, core.Principal, core.Action, core.Address) error {
		return nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close(ctx)
	resolves := 0
	hidden := ""
	offline := false
	denyObserve := false
	source, err := delivery.Jobs(delivery.Options{Scope: "archive-database", Resolve: func(context.Context) (*pool.Driver, error) {
		resolves++
		if offline {
			return nil, core.Fail(core.MountUnavailable, nil)
		}
		return d, nil
	}, Authorize: func(_ context.Context, p core.Principal, a delivery.Action, id string) error {
		if p != "admin" || (a == delivery.Observe && (id == hidden || denyObserve)) {
			return core.Fail(core.PermissionDenied, nil)
		}
		return nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	q, err := task.New(task.Options{Concurrency: 1, Capacity: 2, Conflicts: httpapi.JobTaskConflicts})
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close(ctx)
	h, err := httpapi.New(c, func(_ context.Context, token string) (core.Principal, error) { return core.Principal(token), nil }, time.Minute, httpapi.Options{JobSources: map[string]httpapi.JobSource{"archive": source}, Queue: q, MaxTasks: 4, MaxJSONBytes: 4096, AuthorizeTask: func(context.Context, core.Principal, httpapi.TaskAction, httpapi.TaskInfo) error { return nil }})
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close(ctx)
	call := func(method, path, actor string, body any, status int) *httptest.ResponseRecorder {
		t.Helper()
		var b io.Reader
		if body != nil {
			raw, e := json.Marshal(body)
			if e != nil {
				t.Fatal(e)
			}
			b = bytes.NewReader(raw)
		}
		r := httptest.NewRequest(method, path, b)
		r.Header.Set("Authorization", "Bearer "+actor)
		if body != nil {
			r.Header.Set("Content-Type", "application/json")
		}
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != status {
			t.Fatalf("%s %d want %d: %s", path, w.Code, status, w.Body.String())
		}
		return w
	}
	create := map[string]any{"version": 1, "input": map[string]any{"kind": "repair"}}
	w := call("PUT", "/api/v1/jobs/job?source=archive&id=first", "admin", create, 200)
	var old httpapi.JobView
	if err = json.Unmarshal(w.Body.Bytes(), &old); err != nil || old.Generation == "" || old.Owner != "" {
		t.Fatal(old, err)
	}
	call("PUT", "/api/v1/jobs/job?source=archive&id=second", "admin", create, 200)
	before := resolves
	call("GET", "/api/v1/jobs?source=archive&limit=1", "guest", nil, 403)
	if resolves != before {
		t.Fatal("unauthorized query resolved owner")
	}
	hidden = "first"
	w = call("GET", "/api/v1/jobs?source=archive&limit=1", "admin", nil, 200)
	var page httpapi.JobPage
	_ = json.Unmarshal(w.Body.Bytes(), &page)
	if len(page.Items) != 0 || page.Next != "first" {
		t.Fatal(page)
	}
	hidden = ""
	w = call("GET", "/api/v1/jobs?source=archive&after=first&limit=1", "admin", nil, 200)
	_ = json.Unmarshal(w.Body.Bytes(), &page)
	if len(page.Items) != 1 || page.Items[0].ID != "second" {
		t.Fatal(page)
	}
	// Reopening the original owner preserves the intent independently of HTTP
	// and queue lifetime. Missing owner reports unavailable, never an empty page.
	if err = d.Close(); err != nil {
		t.Fatal(err)
	}
	d = open()
	call("GET", "/api/v1/jobs/job?source=archive&id=first&generation="+old.Generation, "admin", nil, 200)
	offline = true
	call("GET", "/api/v1/jobs?source=archive&limit=1", "admin", nil, 503)
	offline = false
	input := map[string]any{"id": "first", "generation": old.Generation, "limit": 1}
	action := map[string]any{"kind": "archive.run", "input": input}
	// Prepare captures no DB record, owner or Observe permission. A command
	// decoded before Forget must still reject a newly created incarnation.
	raw, _ := json.Marshal(input)
	before = resolves
	work, err := source.Actions["run"].Prepare(ctx, "admin", raw)
	if err != nil || resolves != before {
		t.Fatal(err, resolves, before)
	}
	other, err := source.Actions["run"].Prepare(ctx, "admin", []byte(`{"id":"second","generation":"other","limit":1}`))
	if err != nil || !httpapi.JobTaskConflicts(work.Key, other.Key) {
		t.Fatal("same-Pool keys did not conflict", err)
	}
	if _, err = d.ForgetMaintenance(ctx, "first", pool.MaintenanceOptions{ExpectedGeneration: old.Generation}); err != nil {
		t.Fatal(err)
	}
	call("PUT", "/api/v1/jobs/job?source=archive&id=first", "admin", create, 200)
	if _, err = work.Run(ctx, func(any) {}); core.CodeOf(err) != core.AlreadyExists {
		t.Fatal("stale prepared command passed", err)
	}
	call("POST", "/api/v1/jobs/actions", "admin", action, 409)
	current, err := d.MaintenanceStatus(ctx, "first")
	if err != nil || current.Generation == old.Generation {
		t.Fatal(current, err)
	}
	input["generation"] = current.Generation
	denyObserve = true
	w = call("POST", "/api/v1/jobs/actions", "admin", action, 200)
	if !bytes.Contains(w.Body.Bytes(), []byte(`"done":true`)) {
		t.Fatal(w.Body.String())
	}
	t.Logf("CONTRACT JobResult %s", w.Body.String())
	w = call("POST", "/api/v1/tasks", "admin", action, 202)
	var info httpapi.TaskInfo
	_ = json.Unmarshal(w.Body.Bytes(), &info)
	deadline := time.Now().Add(5 * time.Second)
	for {
		w = call("GET", "/api/v1/tasks/task?id="+info.ID, "admin", nil, 200)
		var v httpapi.TaskView
		_ = json.Unmarshal(w.Body.Bytes(), &v)
		if v.State == task.Finished {
			if v.Error != "" || !bytes.Contains(w.Body.Bytes(), []byte(`"source":"archive"`)) {
				t.Fatal(w.Body.String())
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("task did not finish")
		}
		time.Sleep(time.Millisecond)
	}
	delete(input, "limit")
	action["kind"] = "archive.forget"
	w = call("POST", "/api/v1/jobs/actions", "admin", action, 200)
	if !bytes.Contains(w.Body.Bytes(), []byte(`"record_effect":"applied"`)) {
		t.Fatal(w.Body.String())
	}
	denyObserve = false
	call("GET", "/api/v1/jobs/job?source=archive&id=first", "admin", nil, 404)
}
