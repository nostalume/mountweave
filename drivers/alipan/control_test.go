package alipan_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
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
	c := testCore(t)
	var mu sync.Mutex
	record := credential.Record{Token: credential.Token{Refresh: "synthetic-r1"}}
	started, stopping, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	releaseRefresh := sync.OnceFunc(func() { close(release) })
	builds := 0
	client := &http.Client{Transport: transport(func(r *http.Request) (*http.Response, error) {
		return jsonResponse(r, map[string]any{"items": []any{file("id-a", "a")}, "next_marker": ""}), nil
	})}
	factory := control.TypedFactory(
		func(definition control.Definition, cfg driver.Options) error {
			if definition.Secrets["credential"] != "host-checkpoint" {
				return errors.New("credential reference is required")
			}
			return cfg.Validate()
		},
		func(ctx context.Context, definition control.Definition, cfg driver.Options) (core.Driver, error) {
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
			cfg.Client, cfg.Credentials = client, tokens
			d, err := driver.Open(ctx, cfg)
			if d == nil {
				return nil, errors.Join(err, tokens.Close())
			}
			return d, err
		},
	)
	m, err := control.New(control.Options{Core: c, Store: &mountStore{}, Factories: map[string]control.Factory{"id": factory}, Authorize: func(context.Context, core.Principal, control.Action, string) error { return nil }})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		releaseRefresh()
		if err := m.Close(ctx); err != nil {
			t.Error(err)
		}
	}()
	d, err := m.Save(ctx, "operator", control.Definition{ID: "mount", Driver: "id", Prefix: "/source", Enabled: true, Config: []byte(`{"drive_id":"drive","root_id":"root"}`), Secrets: map[string]string{"credential": "host-checkpoint"}}, 0)
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
		releaseRefresh()
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
	releaseRefresh()
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
	if err = m.Close(waitCtx); err != nil || !second.Mount.Status().Done {
		t.Fatal("replacement did not close", err)
	}
}

func TestRootPathSettingsThroughControl(t *testing.T) {
	ctx := context.Background()
	c := testCore(t)
	rootID, rootPages := "original", 0
	rt := transport(func(r *http.Request) (*http.Response, error) {
		if r.URL.Host == "download.example.test" {
			if r.Header.Get("Authorization") != "" {
				t.Error("API credential leaked to content")
			}
			return response(r, 200, r.URL.Path[1:]), nil
		}
		if r.Header.Get("Authorization") != "Bearer provider-secret" {
			t.Error("wrong root authority")
		}
		body := requestBody(t, r)
		if body["drive_id"] != "drive" {
			t.Error("wrong drive")
		}
		switch r.URL.Path {
		case "/adrive/v1.0/openFile/list":
			parent := body["parent_file_id"].(string)
			if parent == "root" {
				rootPages++
				if body["marker"] != "" {
					return jsonResponse(r, map[string]any{"items": []any{}}), nil
				}
				folder := file(rootID, "docs")
				folder["type"] = "folder"
				return jsonResponse(r, map[string]any{"items": []any{folder, file("outside-id", "outside")}, "next_marker": "last"}), nil
			}
			if parent != "original" && parent != "replacement" {
				t.Error("unexpected bound root", parent)
			}
			entry := file(parent, "song")
			entry["parent_file_id"] = parent
			return jsonResponse(r, map[string]any{"items": []any{entry}}), nil
		case "/adrive/v1.0/openFile/getDownloadUrl":
			return jsonResponse(r, map[string]any{"url": "https://download.example.test/" + body["file_id"].(string), "method": "GET"}), nil
		}
		t.Error("unexpected native request", r.URL.Path)
		return response(r, 500, ""), nil
	})
	var sources []*credential.Source
	factory := control.TypedFactory(
		func(d control.Definition, opts driver.Options) error {
			if d.Secrets["credential"] != "checkpoint" {
				return errors.New("credential reference required")
			}
			return opts.Validate()
		},
		func(ctx context.Context, _ control.Definition, opts driver.Options) (core.Driver, error) {
			runtime := nativeOptions(t, rt)
			opts.Client, opts.Credentials = runtime.Client, runtime.Credentials
			sources = append(sources, opts.Credentials)
			d, err := driver.Open(ctx, opts)
			if d == nil {
				return nil, errors.Join(err, opts.Credentials.Close())
			}
			return d, err
		},
	)
	manager, err := control.New(control.Options{Core: c, Store: &mountStore{}, Factories: map[string]control.Factory{"access": factory}, Authorize: func(context.Context, core.Principal, control.Action, string) error { return nil }})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := manager.Close(ctx); err != nil {
			t.Error(err)
		}
	}()
	opts := driver.Options{DriveID: "drive", RootPath: "/docs"}
	settings, err := json.Marshal(opts)
	if err != nil {
		t.Fatal(err)
	}
	definition, err := manager.Save(ctx, "operator", control.Definition{ID: "library", Prefix: "/cloud", Driver: "access", Config: settings, Secrets: map[string]string{"credential": "checkpoint"}, Enabled: true}, 0)
	if err != nil || len(sources) != 0 || rootPages != 0 {
		t.Fatal("static save acquired resources", err)
	}
	first, err := manager.Apply(ctx, "operator", definition.ID, definition.Revision)
	if err != nil || !first.Runtime.Active || rootPages != 2 {
		t.Fatal("root construction", first, err, rootPages)
	}
	read := func(want string) {
		t.Helper()
		opened, err := c.Open(ctx, "reader", core.Path("/cloud/song"), core.OpenOptions{})
		if err != nil {
			t.Fatal(err)
		}
		data, err := io.ReadAll(opened.Body)
		if err != nil || string(data) != want {
			t.Fatal("bound content", string(data), err)
		}
	}
	rootID = "replacement"
	read("original")
	if rootPages != 2 {
		t.Fatal("live root path was resolved again")
	}
	_, err = c.Stat(ctx, "reader", core.Path("/cloud/outside"))
	requireCode(t, err, core.NotFound)
	for _, invalid := range []string{
		`{"drive_id":"drive","root_id":"root","root_path":"/docs"}`,
		`{"drive_id":"drive","root_path":"/../docs"}`,
		`{"root_path":"/docs"}`,
		`{"drive_id":"drive","root_path":"/docs","endpoint":"http://api.invalid"}`,
		`{"drive_id":"drive","root_path":"/docs","part_size":8,"max_buffered_bytes":4}`,
		`{"drive_id":"drive","root_path":"/docs","client":{}}`,
		`{"drive_id":"drive","root_path":"/docs","credentials":{}}`,
	} {
		bad := definition
		bad.Config = []byte(invalid)
		if _, err := manager.Save(ctx, "operator", bad, definition.Revision); err == nil {
			t.Fatal("invalid native setting accepted", invalid)
		}
	}
	if !first.Mount.Status().Active || len(sources) != 1 || rootPages != 2 {
		t.Fatal("static rejection changed active resources")
	}
	definition, err = manager.Save(ctx, "operator", definition, definition.Revision)
	if err != nil {
		t.Fatal(err)
	}
	second, err := manager.Apply(ctx, "operator", definition.ID, definition.Revision)
	if err != nil || second.Mount == first.Mount || rootPages != 4 {
		t.Fatal("root reconstruction", second, err)
	}
	read("replacement")
	if _, err := sources[0].Access(ctx); !errors.Is(err, credential.ErrClosed) {
		t.Fatal("old credentials survived replacement", err)
	}
	definition.Config = []byte(`{"drive_id":"drive","root_path":"/outside"}`)
	definition, err = manager.Save(ctx, "operator", definition, definition.Revision)
	if err != nil {
		t.Fatal(err)
	}
	failed, err := manager.Apply(ctx, "operator", definition.ID, definition.Revision)
	requireCode(t, err, core.NotDirectory)
	if failed.Runtime.Active || failed.PendingCleanup || len(sources) != 3 {
		t.Fatal("failed root ownership", failed)
	}
	if _, err := sources[2].Access(ctx); !errors.Is(err, credential.ErrClosed) {
		t.Fatal("failed root lost credential cleanup", err)
	}
}
