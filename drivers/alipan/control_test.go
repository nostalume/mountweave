package alipan_test

import (
	"context"
	"errors"
	"maps"
	"net/http"
	"slices"
	"sync"
	"testing"
	"time"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/control"
	driver "github.com/nostalume/mountweave/drivers/alipan"
	"github.com/nostalume/mountweave/drivers/alipan/credential"
	"github.com/nostalume/mountweave/task"
)

type mountStore struct{ d control.Definition }

func (s *mountStore) Load(context.Context, string) (control.Definition, error) {
	d := s.d
	d.Config = slices.Clone(d.Config)
	d.Aliases = slices.Clone(d.Aliases)
	d.Secrets = maps.Clone(d.Secrets)
	return d, nil
}
func (s *mountStore) List(ctx context.Context) ([]control.Definition, error) {
	d, err := s.Load(ctx, "")
	return []control.Definition{d}, err
}
func (s *mountStore) Commit(_ context.Context, d control.Definition, expected uint64) error {
	if s.d.Revision != expected {
		return control.ErrConflict
	}
	s.d = d
	return nil
}

func TestControlledRefreshHandoffWithRunningTask(t *testing.T) {
	ctx := context.Background()
	c, err := core.New(core.Options{Authorize: func(context.Context, core.Principal, core.Action, core.Address) error {
		return nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := c.Close(ctx); err != nil {
			t.Error(err)
		}
	}()
	var mu sync.Mutex
	record := credential.Record{Token: credential.Token{Refresh: "synthetic-r1"}}
	started, stopping, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	builds := 0
	client := &http.Client{Transport: transport(func(r *http.Request) (*http.Response, error) {
		return jsonResponse(r, map[string]any{"items": []any{file("id-a", "a")}, "next_marker": ""}), nil
	})}
	factory := control.Factory{Build: func(_ context.Context, definition control.Definition) (core.Driver, error) {
		if definition.Secrets["credential"] != "host-checkpoint" {
			t.Error("secret reference did not reach its native factory")
		}
		mu.Lock()
		initial := record
		builds++
		n := builds
		mu.Unlock()
		if n > 1 && (initial.Pending || initial.Token.Refresh != "synthetic-r2") {
			t.Errorf("restored stale checkpoint: %+v", initial)
		}
		tokens, err := credential.New(initial, func(refreshCtx context.Context, old string) (credential.Token, error) {
			if old != "synthetic-r1" {
				t.Error("unexpected rotation input")
			}
			close(started)
			<-refreshCtx.Done()
			close(stopping)
			<-release
			return credential.Token{Access: "synthetic-access", Refresh: "synthetic-r2", ExpiresAt: time.Now().Add(time.Hour)}, nil
		}, func(_ context.Context, next credential.Record) error {
			mu.Lock()
			record = next
			mu.Unlock()
			return nil
		})
		if err != nil {
			return nil, err
		}
		d, err := driver.New(driver.Options{Client: client, DriveID: "drive", RootID: "root", Credentials: tokens})
		if err != nil {
			return nil, errors.Join(err, tokens.Close())
		}
		return d, nil
	}}
	m, err := control.New(control.Options{Core: c, Store: &mountStore{}, Factories: map[string]control.Factory{"id": factory}, Authorize: func(context.Context, core.Principal, control.Action, string) error { return nil }})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := m.Close(ctx); err != nil {
			t.Error(err)
		}
	}()
	d, err := m.Save(ctx, "operator", control.Definition{ID: "mount", Driver: "id", Prefix: "/source", Enabled: true, Secrets: map[string]string{"credential": "host-checkpoint"}}, 0)
	if err != nil {
		t.Fatal(err)
	}
	first, err := m.Apply(ctx, "operator", d.ID, d.Revision)
	if err != nil {
		t.Fatal(err)
	}
	q, err := task.New(task.Options{Concurrency: 1, Capacity: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
		if err := q.Close(ctx); err != nil {
			t.Error(err)
		}
	}()
	selection, err := c.Capture(ctx, core.AddressAccess{Address: core.Path("/source/a")})
	if err != nil {
		t.Fatal(err)
	}
	h, err := q.Submit(ctx, task.Work{Run: func(ctx context.Context, _ func(any)) (any, error) {
		return c.Stat(selection.Context(ctx), "reader", core.Path("/source/a"))
	}})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("refresh did not start")
	}
	d, err = m.Save(ctx, "operator", d, d.Revision)
	if err != nil {
		t.Fatal(err)
	}
	applyCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { _, err := m.Apply(applyCtx, "operator", d.ID, d.Revision); done <- err }()
	select {
	case <-stopping:
	case <-time.After(5 * time.Second):
		t.Fatal("retirement did not stop refresh owner")
	}
	cancel()
	if err = <-done; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	mu.Lock()
	n := builds
	mu.Unlock()
	if n != 1 || first.Mount.Status().Done {
		t.Fatal("replacement raced pending refresh acknowledgement")
	}
	close(release)
	waitCtx, stop := context.WithTimeout(ctx, 5*time.Second)
	defer stop()
	s, err := h.Wait(waitCtx)
	if err != nil || !errors.Is(s.Err, context.Canceled) {
		t.Fatalf("running task: %+v %v", s, err)
	}
	second, err := m.Apply(waitCtx, "operator", d.ID, d.Revision)
	if err != nil || !second.Runtime.Active || second.Mount == first.Mount {
		t.Fatalf("handoff: %+v %v", second, err)
	}
}
