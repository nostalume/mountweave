package httpapi_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"testing/iotest"
	"time"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/input"
	"github.com/nostalume/mountweave/protocols/httpapi"
	"github.com/nostalume/mountweave/task"
)

func TestDurableInputAndSharedFileExecution(t *testing.T) {
	ctx := context.Background()
	c, _, targetRoot := setup(t)
	directory := t.TempDir()
	opts := input.Options{MaxBytes: 16384, MaxInputs: 8, MaxAppendBytes: 1024, MaxBlocks: 32}
	s, err := input.Open(ctx, directory, opts)
	if err != nil {
		t.Fatal(err)
	}
	q, err := task.New(task.Options{Concurrency: 1, Capacity: 2, Conflicts: httpapi.FileTaskConflicts})
	if err != nil {
		t.Fatal(err)
	}
	policy := func(_ context.Context, p core.Principal, a httpapi.InputAction, info httpapi.InputInfo) error {
		if p == "writer" && (a == httpapi.CreateInput || a == httpapi.ListInputs || p == info.Owner) {
			return nil
		}
		if p == "reader" && a == httpapi.UseInput {
			return nil
		} // Input permission alone grants no file write.
		return errors.New("private input policy")
	}
	makeHandler := func() *httpapi.Handler {
		h, err := httpapi.New(c, identity, time.Minute, httpapi.Options{Inputs: s, AuthorizeInput: policy, Queue: q, AuthorizeTask: taskPolicy, MaxTasks: 8})
		if err != nil {
			t.Fatal(err)
		}
		return h
	}
	h := makeHandler()
	release := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	t.Cleanup(func() {
		unblock()
		wait, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		if err := errors.Join(h.Close(wait), q.Close(wait), s.Close(wait)); err != nil {
			t.Error(err)
		}
	})
	created := wire(t, request(t, h, "POST", "/api/v1/inputs", "write-token", nil), 201)
	id := created["id"].(string)
	contractExample(t, "InputRecord", created)
	put := func(offset string, data string) *httptest.ResponseRecorder {
		return request(t, h, "PUT", "/api/v1/inputs/content?id="+id+"&offset="+offset, "write-token", nil, strings.NewReader(data))
	}
	wire(t, put("0", "hello "), 200)
	broken := httptest.NewRequest(http.MethodPut, "/api/v1/inputs/content?id="+id+"&offset=6", io.MultiReader(strings.NewReader("bad"), iotest.ErrReader(io.ErrUnexpectedEOF)))
	broken.ContentLength = 5
	broken.Header.Set("Authorization", "Bearer write-token")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, broken)
	if v := wire(t, w, 400); v["result"].(map[string]any)["offset"] != float64(6) {
		t.Fatal(v)
	}
	wire(t, request(t, h, "GET", "/api/v1/inputs?id="+id, "read-token", nil), 403)
	if err = errors.Join(h.Close(ctx), s.Close(ctx)); err != nil {
		t.Fatal(err)
	}
	s, err = input.Open(ctx, directory, opts)
	if err != nil {
		t.Fatal(err)
	}
	h = makeHandler()
	if v := wire(t, request(t, h, "GET", "/api/v1/inputs?id="+id, "write-token", nil), 200); v["offset"] != float64(6) {
		t.Fatal(v)
	}
	if v := wire(t, put("0", "hello "), 200); v["offset"] != float64(6) {
		t.Fatal(v)
	}
	conflict := wire(t, put("0", "wrong!"), 409)
	contractExample(t, "Problem", conflict)
	wire(t, put("6", "world"), 200)
	wire(t, request(t, h, "POST", "/api/v1/inputs/seal?id="+id+"&offset=10", "write-token", nil), 409)
	sealed := wire(t, request(t, h, "POST", "/api/v1/inputs/seal?id="+id+"&offset=11", "write-token", nil), 200)
	if sealed["version"] == nil {
		t.Fatal(sealed)
	}
	contractExample(t, "InputRecord", sealed)
	listed := request(t, h, "GET", "/api/v1/inputs", "write-token", nil)
	if listed.Code != 200 {
		t.Fatal(listed.Code, listed.Body.String())
	}
	var summaries any
	if err = json.Unmarshal(listed.Body.Bytes(), &summaries); err != nil {
		t.Fatal(err)
	}
	contractExample(t, "InputList", summaries)
	contractExample(t, "ApiInfo", wire(t, request(t, h, "GET", "/api/v1/info", "write-token", nil), 200))
	writeRequest := func(path string) map[string]any {
		return operation("fs.write", map[string]any{"path": path, "input_id": id})
	}
	contractExample(t, "FileOperationRequest", writeRequest("/files/from-input"))
	wire(t, jsonCall(t, h, "POST", "/api/v1/fs/operations", "write-token", writeRequest("/files/from-input")), 200)
	if data, err := os.ReadFile(filepath.Join(targetRoot, "from-input")); err != nil || string(data) != "hello world" {
		t.Fatal(string(data), err)
	}
	if v := wire(t, jsonCall(t, h, "POST", "/api/v1/fs/operations", "read-token", writeRequest("/files/denied-input")), 403); v["result"].(map[string]any)["effect"] != "unchanged" {
		t.Fatal(v)
	}
	started := make(chan struct{})
	if _, err = q.Submit(ctx, task.Work{Run: func(context.Context, func(any)) (any, error) { close(started); <-release; return nil, nil }}); err != nil {
		t.Fatal(err)
	}
	awaitSignal(t, started)
	cancelledID := wire(t, jsonCall(t, h, "POST", "/api/v1/tasks", "write-token", writeRequest("/files/cancelled-input")), 202)["id"].(string)
	wire(t, request(t, h, "POST", "/api/v1/tasks/cancel?id="+cancelledID, "write-token", nil), 202)
	if v := untilTask(t, h, cancelledID); v["error"] != "cancelled" || v["cleanup_error"] != nil {
		t.Fatal(v)
	}
	contractExample(t, "TaskRequest", writeRequest("/files/queued-input"))
	submitCtx, cancelSubmit := context.WithCancel(ctx)
	raw, _ := json.Marshal(writeRequest("/files/queued-input"))
	submission := httptest.NewRequest(http.MethodPost, "/api/v1/tasks", strings.NewReader(string(raw))).WithContext(submitCtx)
	submission.Header.Set("Content-Type", "application/json")
	submission.Header.Set("Authorization", "Bearer write-token")
	accepted := httptest.NewRecorder()
	h.ServeHTTP(accepted, submission)
	taskID := wire(t, accepted, 202)["id"].(string)
	cancelSubmit()
	wire(t, jsonCall(t, h, "POST", "/api/v1/tasks", "write-token", writeRequest("/files/rejected-input")), 429)
	if v := wire(t, request(t, h, "DELETE", "/api/v1/inputs?id="+id, "write-token", nil), 200); v["released"] != true {
		t.Fatal(v)
	}
	wire(t, jsonCall(t, h, "POST", "/api/v1/fs/operations", "write-token", writeRequest("/files/after-release")), 409)
	unblock()
	if v := untilTask(t, h, taskID); v["error"] != nil || v["cleanup_error"] != nil || v["result"].(map[string]any)["effect"] != "applied" {
		t.Fatal(v)
	}
	wire(t, request(t, h, "GET", "/api/v1/inputs?id="+id, "write-token", nil), 404)
	if data, err := os.ReadFile(filepath.Join(targetRoot, "queued-input")); err != nil || string(data) != "hello world" {
		t.Fatal(string(data), err)
	}
	for _, name := range []string{"cancelled-input", "rejected-input", "after-release", "denied-input"} {
		if _, err = os.Stat(filepath.Join(targetRoot, name)); !errors.Is(err, os.ErrNotExist) {
			t.Fatal(name, err)
		}
	}
}
