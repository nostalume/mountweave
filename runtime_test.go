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
	definitions, err := s.load()
	if err != nil {
		return control.Definition{}, err
	}
	d, ok := definitions[id]
	if !ok {
		return control.Definition{}, control.ErrNotFound
	}
	return d, nil
}
func (s *definitionStore) load() (map[string]control.Definition, error) {
	definitions := make(map[string]control.Definition)
	b, err := os.ReadFile(s.path)
	if errors.Is(err, os.ErrNotExist) {
		return definitions, nil
	}
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(b, &definitions)
	return definitions, err
}
func (s *definitionStore) List(ctx context.Context) ([]control.Definition, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	definitions, err := s.load()
	if err != nil {
		return nil, err
	}
	list := make([]control.Definition, 0, len(definitions))
	for _, d := range definitions {
		list = append(list, d)
	}
	return list, nil
}
func (s *definitionStore) Commit(ctx context.Context, d control.Definition, expected uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	definitions, err := s.load()
	if err != nil {
		return err
	}
	if definitions[d.ID].Revision != expected {
		return control.ErrConflict
	}
	definitions[d.ID] = d
	b, err := json.Marshal(definitions)
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
	write(t, target, "target-only", []byte("target"))
	store := &definitionStore{path: filepath.Join(t.TempDir(), "definitions.json")}
	var builds atomic.Int32
	type settings struct {
		Directory string `json:"directory"`
	}
	factory := control.TypedFactory(
		func(_ control.Definition, cfg settings) error {
			if cfg.Directory == "" {
				return errors.New("directory is required")
			}
			return nil
		},
		func(_ context.Context, _ control.Definition, cfg settings) (core.Driver, error) {
			builds.Add(1)
			return local.New(cfg.Directory)
		},
	)
	manager, err := control.New(control.Options{Core: c, Store: store, Factories: map[string]control.Factory{"local": factory}, Authorize: management})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := manager.Close(ctx); err != nil {
			t.Error(err)
		}
	}()
	config, _ := json.Marshal(settings{Directory: target})
	targetDef, err := manager.Save(ctx, "alice", control.Definition{ID: "target", Prefix: "/target", Driver: "local", Config: config, Enabled: true}, 0)
	if err != nil {
		t.Fatal(err)
	}
	targetActive, err := manager.Apply(ctx, "alice", targetDef.ID, targetDef.Revision)
	if err != nil || !targetActive.Runtime.Active {
		t.Fatalf("target activation: %+v %v", targetActive, err)
	}
	config, _ = json.Marshal(settings{Directory: root1})
	d, err := manager.Save(ctx, "alice", control.Definition{ID: "source", Prefix: "/source", Driver: "local", Config: config, Enabled: true, Aliases: []control.Alias{{Prefix: "/view", Base: "/tree"}}}, 0)
	if err != nil {
		t.Fatal(err)
	}
	a, err := manager.Apply(ctx, "alice", d.ID, d.Revision)
	if err != nil || !a.Runtime.Active || len(a.Aliases) != 1 {
		t.Fatalf("activation: %+v %v", a, err)
	}
	if a.Mount == targetActive.Mount {
		t.Fatal("one factory shared a mount across definitions")
	}
	if _, err = c.Stat(ctx, "alice", core.Path("/target/target-only")); err != nil {
		t.Fatal(err)
	}
	if _, err = c.Stat(ctx, "alice", core.Path("/source/target-only")); core.CodeOf(err) != core.NotFound {
		t.Fatal("source reached target storage", err)
	}
	if _, err = manager.Apply(ctx, "alice", d.ID, d.Revision); err != nil || builds.Load() != 2 {
		t.Fatal("same revision rebuilt", err)
	}
	invalid := d
	invalid.Config, _ = json.Marshal(map[string]string{"directory": root1, "unexpected": "field"})
	if _, err = manager.Save(ctx, "alice", invalid, d.Revision); err == nil {
		t.Fatal("saved settings with an unknown field")
	}
	// Exercise Apply's pre-retirement validation even for a definition from the store.
	invalid.Revision++
	if err = store.Commit(ctx, invalid, d.Revision); err != nil {
		t.Fatal(err)
	}
	if observed, err := manager.Apply(ctx, "alice", invalid.ID, invalid.Revision); err == nil ||
		observed.Mount != a.Mount || !observed.Runtime.Active || builds.Load() != 2 {
		t.Fatalf("invalid settings disturbed the active mount: %+v %v", observed, err)
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
	config, _ = json.Marshal(settings{Directory: root2})
	d.Config = config
	d, err = manager.Save(ctx, "alice", d, invalid.Revision)
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
	if _, err = manager.Apply(ctx, "alice", d.ID, d.Revision); err != nil || builds.Load() != 3 {
		t.Fatal("alias reconciliation rebuilt", err)
	}
	if _, err = manager.Stop(ctx, "alice", d.ID); err != nil {
		t.Fatal(err)
	}
	if !targetActive.Mount.Status().Active {
		t.Fatal("stopping source also stopped target")
	}
	if err = manager.Close(ctx); err != nil || !targetActive.Mount.Status().Done {
		t.Fatal("target did not close", err)
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
	if err != nil || len(restored) != 2 {
		t.Fatalf("restore: %+v %v", restored, err)
	}
	for _, result := range restored {
		if result.Err != nil || !result.Runtime.Active || result.Mount == b.Mount || result.Mount == targetActive.Mount {
			t.Fatalf("restored instance: %+v", result)
		}
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
