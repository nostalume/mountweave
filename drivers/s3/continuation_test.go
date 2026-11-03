package s3_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	core "github.com/nostalume/mountweave"
	driver "github.com/nostalume/mountweave/drivers/s3"
	"github.com/nostalume/mountweave/input"
	"github.com/nostalume/mountweave/task"
	"github.com/nostalume/mountweave/transfer"
)

// This closed fixture actually syncs records. Reopening it reconstructs intent
// from disk, not a surviving map. It is not a production Store implementation.
type fileIntentStore struct {
	mu      sync.Mutex
	path    string
	fail    func(transfer.Record, []byte) bool
	after   func(transfer.Record, []byte) bool
	records map[string]transfer.Record
	logs    map[string][]byte
}
type intentEntry struct {
	Record transfer.Record
	Delta  []byte
}

var storageFault = errors.New("injected intent storage failure")

func (s *fileIntentStore) recover() error {
	if s.records != nil {
		return nil
	}
	s.records = make(map[string]transfer.Record)
	s.logs = make(map[string][]byte)
	f, err := os.Open(s.path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	defer f.Close()
	decoder := json.NewDecoder(f)
	for {
		var e intentEntry
		err = decoder.Decode(&e)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		s.records[e.Record.ID] = e.Record
		s.logs[e.Record.ID] = append(s.logs[e.Record.ID], e.Delta...)
	}
}
func (s *fileIntentStore) Load(ctx context.Context, id string) (transfer.Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return transfer.Record{}, err
	}
	if err := s.recover(); err != nil {
		return transfer.Record{}, err
	}
	r, ok := s.records[id]
	if !ok {
		return r, transfer.ErrNotFound
	}
	return r, nil
}
func (s *fileIntentStore) Checkpoint(ctx context.Context, id string, revision uint64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := s.recover(); err != nil {
		return nil, err
	}
	if s.records[id].Revision != revision {
		return nil, transfer.ErrConflict
	}
	return bytes.Clone(s.logs[id]), nil
}
func (s *fileIntentStore) Commit(ctx context.Context, r transfer.Record, expected uint64, delta []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := s.recover(); err != nil {
		return err
	}
	previous := s.records[r.ID]
	if previous.Revision != expected || r.Revision != expected+1 {
		return transfer.ErrConflict
	}
	if r.CheckpointBytes != len(s.logs[r.ID])+len(delta) {
		return transfer.ErrInvalid
	}
	if s.fail != nil && s.fail(r, delta) {
		return storageFault
	}
	f, err := os.OpenFile(s.path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0600)
	if err != nil {
		return err
	}
	err = json.NewEncoder(f).Encode(intentEntry{Record: r, Delta: delta})
	if err == nil {
		err = f.Sync()
	}
	err = errors.Join(err, f.Close())
	if err != nil {
		s.records = nil
		return err
	}
	s.records[r.ID] = r
	s.logs[r.ID] = append(s.logs[r.ID], delta...)
	if s.after != nil && s.after(r, delta) {
		return storageFault
	}
	return nil
}
func nativeState(t *testing.T, store *fileIntentStore, r transfer.Record) (phase, id string, parts int) {
	t.Helper()
	data, err := store.Checkpoint(context.Background(), r.ID, r.Revision)
	if err != nil {
		t.Fatal(err)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	tags := make(map[int]string)
	for {
		var event struct {
			Kind, Phase string
			UploadID    string `json:"upload_id"`
			Part        int
			ETag        string
		}
		err = decoder.Decode(&event)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		switch event.Kind {
		case "start":
			phase = event.Phase
		case "phase":
			phase = event.Phase
			if event.UploadID != "" {
				id = event.UploadID
			}
		case "part":
			tags[event.Part] = event.ETag
		}
	}
	return phase, id, len(tags)
}

func TestDurableNativeTransferWorkflow(t *testing.T) {
	ctx := context.Background()
	server := newProtocol()
	opts := driver.Options{PartSize: 5 << 20, MaxBufferedBytes: 10 << 20, PartConcurrency: 1, ResumeScope: "account/service-one"}
	_, c := preparedDriver(t, server.roundTrip, []driver.Options{opts})
	directory := t.TempDir()
	inputOpts := input.Options{MaxBytes: 64 << 20, MaxInputs: 16, MaxAppendBytes: 5 << 20, MaxBlocks: 64}
	inputs, err := input.Open(ctx, directory, inputOpts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := inputs.Close(ctx); err != nil {
			t.Error(err)
		}
	}()
	storePath := filepath.Join(t.TempDir(), "intent.log")
	store := &fileIntentStore{path: storePath}
	revision := uint64(1)
	checkpointBudget := 1 << 20
	serviceFor := func() *transfer.Service {
		service, err := transfer.New(transfer.Options{Core: c, Inputs: inputs, Store: store, Namespace: "transfers", MaxCheckpointBytes: checkpointBudget,
			Authorize: func(_ context.Context, p core.Principal, _ transfer.Action, r transfer.Record) error {
				if p != "writer" || (r.Owner != "" && r.Owner != p) {
					return core.Fail(core.PermissionDenied, nil)
				}
				return nil
			},
			Bind: func(bindCtx context.Context, _ core.Principal, target transfer.Target) (*core.Selection, error) {
				if target.DefinitionID != "cloud" || target.Revision != revision {
					return nil, core.Fail(core.MountUnavailable, nil)
				}
				return c.Capture(bindCtx, core.PathAccess{Path: target.Path, Write: true})
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		return service
	}
	service := serviceFor()
	create := func(id, target string, data []byte) transfer.Record {
		t.Helper()
		source, err := inputs.Create(ctx, "writer")
		if err != nil {
			t.Fatal(err)
		}
		for offset := 0; offset < len(data); {
			end := min(offset+5<<20, len(data))
			if _, err = inputs.Append(ctx, source.ID, int64(offset), bytes.NewReader(data[offset:end]), int64(end-offset)); err != nil {
				t.Fatal(err)
			}
			offset = end
		}
		if _, err = inputs.Seal(ctx, source.ID, int64(len(data))); err != nil {
			t.Fatal(err)
		}
		request := transfer.Request{InputID: source.ID, Target: transfer.Target{DefinitionID: "cloud", Revision: 1, Path: "/cloud/" + target}}
		r, err := service.Create(ctx, "writer", id, request)
		if errors.Is(err, storageFault) {
			r, err = service.Create(ctx, "writer", id, request)
		}
		if err != nil {
			t.Fatal(err)
		}
		if _, err = inputs.Release(ctx, source.ID); err != nil {
			t.Fatal(err)
		}
		return r
	}
	restart := func() {
		t.Helper()
		if err = inputs.Close(ctx); err != nil {
			t.Fatal(err)
		}
		inputs, err = input.Open(ctx, directory, inputOpts)
		if err != nil {
			t.Fatal(err)
		}
		store = &fileIntentStore{path: storePath}
		service = serviceFor()
	}
	payload := append(bytes.Repeat([]byte("a"), 5<<20), bytes.Repeat([]byte("b"), 5<<20)...)
	payload = append(payload, []byte("final")...)
	lostCreation := true
	store.after = func(r transfer.Record, _ []byte) bool {
		if lostCreation && r.State == transfer.Preparing {
			lostCreation = false
			return true
		}
		return false
	}
	intent := create("resume", "large", payload)
	if lostCreation {
		t.Fatal("creation uncertainty was not exercised")
	}
	restart() // Source Release cannot erase the durable transfer's retention.
	server.failPartNumber = 2
	r, err := service.Run(ctx, "writer", intent.ID)
	if err == nil || r.State != transfer.Ready || r.Change.Effect != core.Unchanged {
		t.Fatalf("pause: %+v %v", r, err)
	}
	phase, uploadID, parts := nativeState(t, store, r)
	if phase != "uploading" || parts != 1 {
		t.Fatalf("checkpoint %s %d", phase, parts)
	}
	if server.calls["PUT 1 "+uploadID] != 1 {
		t.Fatal("first part not confirmed once")
	}
	if err = c.Close(ctx); err != nil {
		t.Fatal(err)
	}
	opts.PartConcurrency = 2
	opts.ResumeScope = "account/service-other"
	_, c = preparedDriver(t, server.roundTrip, []driver.Options{opts})
	restart()
	server.failPartNumber = 0
	if _, err = service.Run(ctx, "writer", intent.ID); core.CodeOf(err) != core.InvalidRequest {
		t.Fatalf("native scope change admitted: %v", err)
	}
	if server.calls["PUT 1 "+uploadID] != 1 {
		t.Fatal("identity mismatch sent native data")
	}
	if err = c.Close(ctx); err != nil {
		t.Fatal(err)
	}
	opts.ResumeScope = "account/service-one"
	_, c = preparedDriver(t, server.roundTrip, []driver.Options{opts})
	service = serviceFor()
	revision = 2
	if _, err = service.Run(ctx, "writer", intent.ID); core.CodeOf(err) != core.MountUnavailable {
		t.Fatalf("changed activation admitted: %v", err)
	}
	revision = 1
	q, err := task.New(task.Options{Concurrency: 1, Capacity: 1})
	if err != nil {
		t.Fatal(err)
	}
	handle, err := q.Submit(ctx, task.Work{Run: func(runCtx context.Context, _ func(any)) (any, error) {
		return service.Run(runCtx, "writer", intent.ID)
	}})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := handle.Wait(ctx)
	if err != nil || snapshot.Err != nil {
		t.Fatalf("queued resume: %+v %v", snapshot, err)
	}
	if err = q.Close(ctx); err != nil {
		t.Fatal(err)
	}
	r = snapshot.Result.(transfer.Record)
	if r.State != transfer.Completed || server.calls["PUT 1 "+uploadID] != 1 || !bytes.Equal(server.objects["vault/large"], payload) {
		t.Fatalf("resume did not reuse confirmed parts: %+v", r)
	}
	if _, err = inputs.Status(ctx, intent.Request.InputID); !errors.Is(err, input.ErrNotFound) {
		t.Fatalf("terminal source leaked: %v", err)
	}
	if _, err = service.Status(ctx, "reader", intent.ID); core.CodeOf(err) != core.PermissionDenied {
		t.Fatalf("intent authorization: %v", err)
	}
	if _, err = c.Copy(ctx, "writer", "/cloud/large", "/cloud/copy", core.WriteOptions{Continuation: &core.WriteContinuation{}}); core.CodeOf(err) != core.UnsupportedOperation {
		t.Fatalf("undefined copy continuation admitted: %v", err)
	}
	if _, err = c.CopyTree(ctx, "writer", "/cloud/source", "/cloud/tree", core.TreeOptions{MaxEntries: 8, MaxPlanBytes: 1024}, core.DirectoryCopyOptions{WriteOptions: core.WriteOptions{Continuation: &core.WriteContinuation{}}}); core.CodeOf(err) != core.UnsupportedOperation {
		t.Fatalf("tree reused a file checkpoint: %v", err)
	}

	// A lost receipt save retransmits only that immutable part in the same session.
	intent = create("save-failure", "save", []byte("receipt"))
	failOnce := true
	store.fail = func(r transfer.Record, delta []byte) bool {
		var v struct {
			Kind, Phase string
		}
		_ = json.Unmarshal(delta, &v)
		if failOnce && v.Kind == "part" {
			failOnce = false
			return true
		}
		return false
	}
	r, err = service.Run(ctx, "writer", intent.ID)
	if !errors.Is(err, storageFault) {
		t.Fatalf("lost checkpoint: %v", err)
	}
	_, uploadID, _ = nativeState(t, store, r)
	restart()
	r, err = service.Run(ctx, "writer", intent.ID)
	if err != nil || r.State != transfer.Completed || server.calls["PUT 1 "+uploadID] != 2 {
		t.Fatalf("receipt retry: %+v %v", r, err)
	}

	// The quota counts all retained records, not just one append. Stop before
	// publication, then increase the host budget and finish without resending data.
	log, e := store.Checkpoint(ctx, r.ID, r.Revision)
	if e != nil {
		t.Fatal(e)
	}
	decoder := json.NewDecoder(bytes.NewReader(log))
	prefix := 0
	for range 3 {
		var raw json.RawMessage
		if e = decoder.Decode(&raw); e != nil {
			t.Fatal(e)
		}
		prefix += len(raw) + 1
	}
	checkpointBudget = prefix + 16
	service = serviceFor()
	intent = create("budget", "caps", []byte("receipt"))
	r, err = service.Run(ctx, "writer", intent.ID)
	if core.CodeOf(err) != core.CapacityExceeded || r.CheckpointBytes == 0 {
		t.Fatalf("cumulative quota: %+v %v", r, err)
	}
	phase, uploadID, parts = nativeState(t, store, r)
	if phase != "uploading" || parts != 1 || server.calls["POST  "+uploadID] != 0 {
		t.Fatalf("quota crossed publication barrier: %s %d", phase, parts)
	}
	checkpointBudget = 1 << 20
	restart()
	r, err = service.Run(ctx, "writer", intent.ID)
	if err != nil || r.State != transfer.Completed || server.calls["PUT 1 "+uploadID] != 1 {
		t.Fatalf("quota continuation: %+v %v", r, err)
	}

	// A receipt may be durable even though Append returns an error. Poison this
	// execution's appender, reload its revision, and keep the confirmed native part.
	intent = create("uncertain-append", "uncertain", payload)
	recordedPart := 0
	store.after = func(_ transfer.Record, delta []byte) bool {
		var event struct {
			Kind string
			Part int
		}
		_ = json.Unmarshal(delta, &event)
		if recordedPart == 0 && event.Kind == "part" {
			recordedPart = event.Part
			return true
		}
		return false
	}
	_, err = service.Run(ctx, "writer", intent.ID)
	if !errors.Is(err, storageFault) || recordedPart == 0 {
		t.Fatalf("uncertain append: %v", err)
	}
	r, e = store.Load(ctx, intent.ID)
	if e != nil {
		t.Fatal(e)
	}
	_, uploadID, parts = nativeState(t, store, r)
	if parts != 1 {
		t.Fatalf("appender continued after uncertain revision: %d", parts)
	}
	partCall := "PUT " + strconv.Itoa(recordedPart) + " " + uploadID
	beforePart := server.calls[partCall]
	restart()
	r, err = service.Run(ctx, "writer", intent.ID)
	if err != nil || r.State != transfer.Completed || server.calls[partCall] != beforePart {
		t.Fatalf("durable receipt replayed: %+v %v", r, err)
	}

	// A failed commit barrier prevents publication. Already confirmed parts survive.
	intent = create("commit-barrier", "barrier", []byte("barrier"))
	failOnce = true
	store.fail = func(r transfer.Record, delta []byte) bool {
		var v struct{ Phase string }
		_ = json.Unmarshal(delta, &v)
		if failOnce && v.Phase == "committing" {
			failOnce = false
			return true
		}
		return false
	}
	r, err = service.Run(ctx, "writer", intent.ID)
	if !errors.Is(err, storageFault) {
		t.Fatalf("commit barrier: %v", err)
	}
	_, uploadID, _ = nativeState(t, store, r)
	if server.calls["POST  "+uploadID] != 0 {
		t.Fatal("publication preceded durable barrier")
	}
	restart()
	r, err = service.Run(ctx, "writer", intent.ID)
	if err != nil || r.State != transfer.Completed || server.calls["PUT 1 "+uploadID] != 1 {
		t.Fatalf("commit barrier recovery: %+v %v", r, err)
	}

	// Completion loss reconciles the native attempt marker; no second completion.
	intent = create("complete-loss", "lost", nil)
	server.lostComplete = true
	r, err = service.Run(ctx, "writer", intent.ID)
	if err == nil || r.Change.Effect != core.Unknown {
		t.Fatalf("lost complete: %+v %v", r, err)
	}
	status, e := service.Status(ctx, "writer", intent.ID)
	if e != nil || status.Change.Effect != core.Unknown {
		t.Fatalf("uncertain effect not persisted: %+v %v", status, e)
	}
	_, uploadID, _ = nativeState(t, store, r)
	before := server.calls["POST  "+uploadID]
	restart()
	r, err = service.Run(ctx, "writer", intent.ID)
	if err != nil || r.State != transfer.Completed || server.calls["POST  "+uploadID] != before {
		t.Fatalf("complete reconciliation: %+v %v", r, err)
	}
	server.lostComplete = false

	// Conditional publication remains the same create-only file semantics.
	intent = create("conflict", "large", []byte("new"))
	r, err = service.Run(ctx, "writer", intent.ID)
	if core.CodeOf(err) != core.AlreadyExists || r.Change.Effect != core.Unchanged || !bytes.Equal(server.objects["vault/large"], payload) {
		t.Fatalf("conditional publication: %+v %v", r, err)
	}
	if r, err = service.Abort(ctx, "writer", intent.ID); err != nil || r.State != transfer.Aborted {
		t.Fatalf("conflict cleanup: %+v %v", r, err)
	}

	// Explicit abort joins native cleanup, including a lost abort response.
	intent = create("abort-loss", "abort", []byte("abort"))
	server.failPartNumber = 1
	r, err = service.Run(ctx, "writer", intent.ID)
	if err == nil {
		t.Fatal("part failure absent")
	}
	_, uploadID, _ = nativeState(t, store, r)
	server.failPartNumber = 0
	server.lostAbort = true
	r, err = service.Abort(ctx, "writer", intent.ID)
	if err != nil || r.State != transfer.Aborted {
		t.Fatalf("abort reconciliation: %+v %v", r, err)
	}
	if _, exists := server.uploads[uploadID]; exists {
		t.Fatal("aborted native session survived")
	}

	// Cancellation is a pause, and a concurrent invocation cannot race that owner.
	intent = create("cancel", "cancel", []byte("cancel"))
	server.hold = make(chan struct{})
	server.started = make(chan struct{})
	server.held = sync.Once{}
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { _, e := service.Run(runCtx, "writer", intent.ID); done <- e }()
	select {
	case <-server.started:
	case <-time.After(5 * time.Second):
		t.Fatal("native request not started")
	}
	if _, err = service.Run(ctx, "writer", intent.ID); !errors.Is(err, transfer.ErrBusy) {
		t.Fatalf("duplicate executor: %v", err)
	}
	cancel()
	close(server.hold)
	if err = <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel lost: %v", err)
	}
	server.hold = nil
	r, err = service.Run(ctx, "writer", intent.ID)
	if err != nil || r.State != transfer.Completed {
		t.Fatalf("resume cancellation: %+v %v", r, err)
	}

	// An externally replaced object cannot prove a lost commit. Neither Run nor
	// Abort may repeat publication/delete by name or release its retained source.
	intent = create("unresolved", "unresolved", []byte("original"))
	server.lostComplete = true
	r, err = service.Run(ctx, "writer", intent.ID)
	if err == nil {
		t.Fatal("lost completion absent")
	}
	_, uploadID, _ = nativeState(t, store, r)
	before = server.calls["POST  "+uploadID]
	server.objects["vault/unresolved"] = []byte("external")
	delete(server.metadata, "vault/unresolved")
	restart()
	for _, run := range []func(context.Context, core.Principal, string) (transfer.Record, error){service.Run, service.Abort} {
		r, err = run(ctx, "writer", intent.ID)
		if err == nil || r.Change.Effect != core.Unknown || r.State != transfer.Ready {
			t.Fatalf("unresolved commit guessed: %+v %v", r, err)
		}
	}
	if server.calls["POST  "+uploadID] != before || server.calls["DELETE  "+uploadID] != 0 || string(server.objects["vault/unresolved"]) != "external" {
		t.Fatal("uncertain commit replayed or compensated")
	}
	if record, e := inputs.Status(ctx, intent.Request.InputID); e != nil || !record.Released {
		t.Fatalf("unresolved source lost: %+v %v", record, e)
	}

	// Losing initiation cannot authorize another upload or erase retained input.
	intent = create("initiation-loss", "initiation", []byte("initiation"))
	server.lostComplete = false
	server.lostInitiate = true
	r, err = service.Run(ctx, "writer", intent.ID)
	if err == nil {
		t.Fatal("lost initiation absent")
	}
	phase, uploadID, _ = nativeState(t, store, r)
	before = server.next
	if phase != "initializing" || uploadID != "" {
		t.Fatalf("lost initiation checkpoint: %s %s", phase, uploadID)
	}
	restart()
	for _, run := range []func(context.Context, core.Principal, string) (transfer.Record, error){service.Run, service.Abort} {
		if _, e := run(ctx, "writer", intent.ID); e == nil {
			t.Fatal("unknown initiation guessed")
		}
	}
	if server.next != before {
		t.Fatal("unknown initiation duplicated native resource")
	}
}
