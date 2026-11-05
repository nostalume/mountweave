package core_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/control"
	"github.com/nostalume/mountweave/drivers/local"
	"github.com/nostalume/mountweave/task"
)

// This fixture is host storage, not a prescribed production format/backend.
type definitionStore struct {
	mu   sync.Mutex
	path string
}

func (s *definitionStore) Load(ctx context.Context, id string) (control.Definition, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.load(id)
}
func (s *definitionStore) load(id string) (control.Definition, error) {
	b, err := os.ReadFile(s.path)
	if errors.Is(err, os.ErrNotExist) {
		return control.Definition{}, control.ErrNotFound
	}
	if err != nil {
		return control.Definition{}, err
	}
	var d control.Definition
	err = json.Unmarshal(b, &d)
	if err == nil && d.ID != id {
		err = control.ErrNotFound
	}
	return d, err
}
func (s *definitionStore) List(ctx context.Context) ([]control.Definition, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	d, err := s.load("source")
	if errors.Is(err, control.ErrNotFound) {
		return nil, nil
	}
	return []control.Definition{d}, err
}
func (s *definitionStore) Commit(ctx context.Context, d control.Definition, expected uint64) error {
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
	if err != nil {
		return err
	}
	return os.WriteFile(s.path, b, 0600)
}
func management(_ context.Context, p core.Principal, _ control.Action, _ string) error {
	if p != "alice" {
		return errors.New("management denied")
	}
	return nil
}
func taskResult(t *testing.T, h *task.Handle) task.Snapshot {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s, err := h.Wait(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return s
}
func addTask(t *testing.T, q *task.Queue, w task.Work) *task.Handle {
	t.Helper()
	h, err := q.Submit(context.Background(), w)
	if err != nil {
		t.Fatal(err)
	}
	return h
}

func TestManagedBindingsTasksAndRestore(t *testing.T) {
	ctx := context.Background()
	root1, root2, target := t.TempDir(), t.TempDir(), t.TempDir()
	for _, root := range []string{root1, root2} {
		if err := os.Mkdir(filepath.Join(root, "tree"), 0700); err != nil {
			t.Fatal(err)
		}
	}
	write(t, root1, "tree/a", []byte("old"))
	write(t, root2, "tree/a", []byte("new"))
	c := newCore(t, core.Options{})
	defer closeCore(t, c)
	dst, err := local.New(target)
	if err != nil {
		t.Fatal(err)
	}
	installCopyMount(t, c, "/target", dst)
	store := &definitionStore{path: filepath.Join(t.TempDir(), "definitions.json")}
	var builds atomic.Int32
	factory := control.Factory{
		Validate: func(_ context.Context, d control.Definition) error {
			var root string
			return json.Unmarshal(d.Config, &root)
		},
		Build: func(_ context.Context, d control.Definition) (core.Driver, error) {
			var root string
			if err := json.Unmarshal(d.Config, &root); err != nil {
				return nil, err
			}
			builds.Add(1)
			return local.New(root)
		},
	}
	manager, err := control.New(control.Options{Core: c, Store: store, Factories: map[string]control.Factory{"local": factory}, Authorize: management})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := manager.Close(ctx); err != nil {
			t.Error(err)
		}
	}()
	config, _ := json.Marshal(root1)
	d, err := manager.Save(ctx, "alice", control.Definition{ID: "source", Prefix: "/source", Driver: "local", Config: config, Enabled: true, Aliases: []control.Alias{{Prefix: "/view", Base: "/tree"}}}, 0)
	if err != nil {
		t.Fatal(err)
	}
	a, err := manager.Apply(ctx, "alice", d.ID, d.Revision)
	if err != nil || !a.Runtime.Active || len(a.Aliases) != 1 {
		t.Fatalf("activation: %+v %v", a, err)
	}
	if _, err = manager.Apply(ctx, "alice", d.ID, d.Revision); err != nil || builds.Load() != 1 {
		t.Fatal("same revision rebuilt", err)
	}
	if _, err = manager.Save(ctx, "alice", d, 0); !errors.Is(err, control.ErrConflict) {
		t.Fatal("stale save", err)
	}
	// Optional conflict policy understands only keys supplied by file adapters.
	q, err := task.New(task.Options{Concurrency: 1, Capacity: 4, Conflicts: func(a, b any) bool { x, _ := a.(*core.Selection); y, _ := b.(*core.Selection); return x.Conflicts(y) }})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := q.Close(ctx); err != nil {
			t.Error(err)
		}
	}()
	entered, release := make(chan struct{}), make(chan struct{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	block := addTask(t, q, task.Work{Run: func(context.Context, func(any)) (any, error) { close(entered); <-release; return nil, nil }})
	await(t, entered)
	selected, err := c.Capture(ctx, core.AddressAccess{Address: core.Path("/view/a")}, core.AddressAccess{Address: core.Path("/target/stale"), Write: true})
	if err != nil {
		t.Fatal(err)
	}
	overlap, err := c.Capture(ctx, core.AddressAccess{Address: core.Path("/source/tree/a"), Write: true})
	if err != nil {
		t.Fatal(err)
	}
	readOnly, err := c.Capture(ctx, core.AddressAccess{Address: core.Path("/source/tree/a")})
	if err != nil {
		t.Fatal(err)
	}
	if !selected.Conflicts(overlap) || selected.Conflicts(readOnly) {
		t.Fatal("alias conflict coordinates are inconsistent")
	}
	oldTask := addTask(t, q, task.Work{Key: selected, Run: func(ctx context.Context, _ func(any)) (any, error) {
		return c.Copy(selected.Context(ctx), "alice", core.Path("/view/a"), core.Path("/target/stale"))
	}})
	config, _ = json.Marshal(root2)
	d.Config = config
	d, err = manager.Save(ctx, "alice", d, d.Revision)
	if err != nil {
		t.Fatal(err)
	}
	if !a.Mount.Status().Active {
		t.Fatal("save changed runtime")
	}
	b, err := manager.Apply(ctx, "alice", d.ID, d.Revision)
	if err != nil || !b.Runtime.Active || !a.Mount.Status().Done || b.Mount == a.Mount {
		t.Fatalf("replacement: %+v %v", b, err)
	}
	close(release)
	taskResult(t, block)
	s := taskResult(t, oldTask)
	if core.CodeOf(s.Err) != core.MountUnavailable || s.Result.(core.Change).Effect != core.Unchanged {
		t.Fatalf("old task retargeted: %+v", s)
	}
	if _, err = os.Stat(filepath.Join(target, "stale")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("unexpected target: %v", err)
	}
	// A current selection covers all discovered descendants without new APIs.
	current, err := c.Capture(ctx, core.AddressAccess{Address: core.Path("/view")}, core.AddressAccess{Address: core.Path("/target/tree"), Write: true})
	if err != nil {
		t.Fatal(err)
	}
	publishErr := errors.New("host receipt save failed")
	tree := addTask(t, q, task.Work{Key: current, Run: func(ctx context.Context, report func(any)) (any, error) {
		report("copying")
		return c.CopyTree(current.Context(ctx), "alice", core.Path("/view"), core.Path("/target/tree"), core.TreeOptions{MaxEntries: 10, MaxPlanBytes: 100})
	}, Complete: func(context.Context, task.Snapshot) error { return publishErr }})
	s = taskResult(t, tree)
	if s.Err != nil || !errors.Is(s.CompletionErr, publishErr) || len(s.Result.(core.TreeResult).Items) != 2 {
		t.Fatalf("tree receipt: %+v", s)
	}
	if b, err := os.ReadFile(filepath.Join(target, "tree/a")); err != nil || string(b) != "new" {
		t.Fatalf("tree body: %q %v", b, err)
	}
	// Alias identity is stronger than its mount/path coordinates for admission.
	aliasSelection, err := c.Capture(ctx, core.AddressAccess{Address: core.Path("/view")})
	if err != nil {
		t.Fatal(err)
	}
	if err = c.UnbindAlias(ctx, "/view"); err != nil {
		t.Fatal(err)
	}
	if err = c.BindAlias(ctx, "/view", b.Mount, "/tree"); err != nil {
		t.Fatal(err)
	}
	_, err = c.Stat(aliasSelection.Context(ctx), "alice", core.Path("/view/a"))
	code(t, err, core.MountUnavailable)
	if _, err = manager.Apply(ctx, "alice", d.ID, d.Revision); err != nil || builds.Load() != 2 {
		t.Fatal("alias reconciliation rebuilt", err)
	}
	if _, err = manager.Stop(ctx, "alice", d.ID); err != nil {
		t.Fatal(err)
	}
	// A new controller/core reloads desired data, not old Mount pointers.
	c2 := newCore(t, core.Options{})
	defer closeCore(t, c2)
	m2, err := control.New(control.Options{Core: c2, Store: &definitionStore{path: store.path}, Factories: map[string]control.Factory{"local": factory}, Authorize: management})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := m2.Close(ctx); err != nil {
			t.Error(err)
		}
	}()
	restored, err := m2.Restore(ctx, "alice")
	if err != nil || len(restored) != 1 || restored[0].Err != nil || !restored[0].Runtime.Active || restored[0].Mount == b.Mount {
		t.Fatalf("restore: %+v %v", restored, err)
	}
	if _, err = c2.Stat(ctx, "alice", core.Path("/view/a")); err != nil {
		t.Fatal(err)
	}
}

func TestActivationFailurePreservesOwnersAndPartialState(t *testing.T) {
	ctx := context.Background()
	c := newCore(t, core.Options{})
	defer closeCore(t, c)
	store := &definitionStore{path: filepath.Join(t.TempDir(), "definition.json")}
	closeEntered, closeAck := make(chan struct{}), make(chan struct{})
	defer func() {
		select {
		case <-closeAck:
		default:
			close(closeAck)
		}
	}()
	failure := errors.New("unadopted close failed")
	bad := &fakeDriver{close: func() error { close(closeEntered); <-closeAck; return failure }}
	m, err := control.New(control.Options{Core: c, Store: store, Authorize: management, Factories: map[string]control.Factory{"broken": {Build: func(context.Context, control.Definition) (core.Driver, error) {
		return bad, errors.New("construction failed")
	}}}})
	if err != nil {
		t.Fatal(err)
	}
	d, err := m.Save(ctx, "alice", control.Definition{ID: "source", Prefix: "/source", Driver: "broken", Enabled: true}, 0)
	if err != nil {
		t.Fatal(err)
	}
	applyCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { _, err := m.Apply(applyCtx, "alice", d.ID, d.Revision); done <- err }()
	await(t, closeEntered)
	cancel()
	if err = <-done; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	a, err := m.Observe(ctx, "alice", d.ID)
	if err != nil || !a.PendingCleanup {
		t.Fatalf("lost unadopted owner: %+v %v", a, err)
	}
	close(closeAck)
	if err = m.Close(ctx); !errors.Is(err, failure) || bad.closes.Load() != 1 {
		t.Fatalf("cleanup acknowledged/retried: %v", err)
	}
	// Successful installation remains visible if the alias stage fails.
	m2, err := control.New(control.Options{Core: c, Store: store, Authorize: management, Factories: map[string]control.Factory{"broken": {Build: func(context.Context, control.Definition) (core.Driver, error) { return &fakeDriver{}, nil }}}})
	if err != nil {
		t.Fatal(err)
	}
	d.Aliases = []control.Alias{{Prefix: "/alias", Base: "/missing"}}
	d, err = m2.Save(ctx, "alice", d, d.Revision)
	if err != nil {
		t.Fatal(err)
	}
	a, err = m2.Apply(ctx, "alice", d.ID, d.Revision)
	if err == nil || !a.Runtime.Active || a.Mount == nil {
		t.Fatalf("partial activation hidden: %+v %v", a, err)
	}
	if err = m2.Close(ctx); err != nil {
		t.Fatal(err)
	}
}
