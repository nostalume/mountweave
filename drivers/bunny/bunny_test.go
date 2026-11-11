package bunny_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	core "github.com/nostalume/mountweave"
	driver "github.com/nostalume/mountweave/drivers/bunny"
	"github.com/nostalume/mountweave/drivers/local"
)

type fixture struct {
	mu             sync.Mutex
	files          map[string]string
	dirs           map[string]bool
	calls          []string
	block, release chan struct{}
}

func (s *fixture) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("AccessKey") != "zone-password" {
		http.Error(w, "wrong authorization", 403)
		return
	}
	s.mu.Lock()
	s.calls = append(s.calls, r.Method+" "+r.URL.EscapedPath())
	s.mu.Unlock()
	switch r.URL.Path {
	case "/zone/archive/block/":
		fmt.Fprint(w, "[")
		w.(http.Flusher).Flush()
		select {
		case s.block <- struct{}{}:
		default:
		}
		select {
		case <-r.Context().Done():
		case <-s.release:
		}
		return
	case "/zone/archive/throttle/":
		http.Error(w, "Unable to process request", 400)
		return
	case "/zone/archive/truncated/":
		fmt.Fprint(w, "[")
		return
	case "/zone/archive/junk/":
		fmt.Fprint(w, "[] false")
		return
	case "/zone/archive/escape/":
		fmt.Fprint(w, `[{"Path":"/another-zone/","ObjectName":"file","Length":1,"IsDirectory":false}]`)
		return
	case "/zone/archive/redirect":
		w.Header().Set("Location", "/elsewhere")
		w.WriteHeader(307)
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if r.Method == "DELETE" {
		if strings.HasSuffix(r.URL.Path, "/lost") {
			conn, _, _ := w.(http.Hijacker).Hijack()
			conn.Close()
			return
		}
		if strings.HasSuffix(r.URL.Path, "/accepted") {
			w.WriteHeader(201)
			return
		}
		if r.URL.Path == "/zone/archive/tree" {
			delete(s.dirs, r.URL.Path)
			for k := range s.files {
				if strings.HasPrefix(k, r.URL.Path+"/") {
					delete(s.files, k)
				}
			}
			w.WriteHeader(200)
			return
		}
		http.NotFound(w, r)
		return
	}
	if r.Method != "GET" {
		http.Error(w, r.Method, 405)
		return
	}
	if strings.HasSuffix(r.URL.Path, "/") {
		if !s.dirs[strings.TrimSuffix(r.URL.Path, "/")] {
			http.NotFound(w, r)
			return
		}
		items := []map[string]any{}
		add := func(key string, dir bool, size int) {
			pos := strings.LastIndexByte(key, '/')
			if key[:pos+1] == r.URL.Path {
				items = append(items, map[string]any{"ObjectName": key[pos+1:], "Path": r.URL.Path, "IsDirectory": dir, "Length": size, "LastChanged": "2026-01-01T00:00:00"})
			}
		}
		for k, v := range s.files {
			add(k, false, len(v))
		}
		for k := range s.dirs {
			add(k, true, 0)
		}
		sort.Slice(items, func(i, j int) bool { return items[i]["ObjectName"].(string) < items[j]["ObjectName"].(string) })
		json.NewEncoder(w).Encode(items)
		return
	}
	text, ok := s.files[r.URL.Path]
	if !ok {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("ETag", `"native-tag"`)
	if r.Header.Get("Range") == "bytes=1-3" {
		w.Header().Set("Content-Range", fmt.Sprintf("bytes 1-3/%d", len(text)))
		w.Header().Set("Content-Length", "3")
		w.WriteHeader(206)
		fmt.Fprint(w, text[1:4])
		return
	}
	w.Header().Set("Content-Length", fmt.Sprint(len(text)))
	fmt.Fprint(w, text)
}
func setup(t *testing.T) (*fixture, *driver.Driver, *core.Core, *core.Mount) {
	t.Helper()
	s := &fixture{files: map[string]string{"/zone/archive/a%+中": "abcdef", "/zone/archive/z": "last", "/zone/archive/tree/file": "nested", "/zone/archive/redirect": "placeholder"}, dirs: map[string]bool{"/zone/archive": true, "/zone/archive/tree": true, "/zone/archive/lost": true, "/zone/archive/accepted": true}, block: make(chan struct{}, 1), release: make(chan struct{})}
	server := httptest.NewServer(s)
	t.Cleanup(server.Close)
	t.Cleanup(func() { close(s.release) })
	d, err := driver.Open(context.Background(), driver.Options{RootURL: server.URL + "/zone/archive/", AccessKey: "zone-password", Client: server.Client()})
	if err != nil {
		t.Fatal(err)
	}
	c, err := core.New(core.Options{Authorize: func(context.Context, core.Principal, core.Action, core.Address) error { return nil }})
	if err != nil {
		t.Fatal(err)
	}
	m, err := c.Install("/cloud", d)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := c.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	return s, d, c, m
}
func (s *fixture) count(prefix string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, v := range s.calls {
		if strings.HasPrefix(v, prefix) {
			n++
		}
	}
	return n
}

func TestBrowseBodyAndCopy(t *testing.T) {
	localRoot := t.TempDir()
	s, _, c, m := setup(t)
	ctx := context.Background()
	if err := c.BindAlias(ctx, "/alias", m, "/"); err != nil {
		t.Fatal(err)
	}
	q := core.Children(core.Path("/cloud"))
	request := core.ListRequest{Query: q, Limit: 1, CursorTTL: time.Minute}
	before := s.count("GET /zone/archive/")
	first, err := c.List(ctx, "alice", request)
	if err != nil || len(first.Items) != 1 || first.NextCursor == nil {
		t.Fatalf("first page: %+v %v", first, err)
	}
	request.Cursor = *first.NextCursor
	request.CursorTTL = 0
	page, err := c.List(ctx, "alice", request)
	if err != nil {
		t.Fatal(err)
	}
	calls := s.count("GET /zone/archive/")
	if calls != before+1 {
		t.Fatalf("native listing repeated: %d ->%d", before, calls)
	}
	for page.NextCursor != nil {
		request.Cursor = *page.NextCursor
		page, err = c.List(ctx, "alice", request)
		if err != nil {
			t.Fatal(err)
		}
	}
	if s.count("GET /zone/archive/") != calls {
		t.Fatal("public pages became native pages")
	}
	entry, err := c.Stat(ctx, "alice", core.Path("/cloud/a%+中"))
	if err != nil || entry.Name != "a%+中" || entry.ModifiedAt != nil {
		t.Fatalf("exact Stat: %+v %v", entry, err)
	}
	opened, err := c.Open(ctx, "alice", core.Path("/alias/a%+中"), core.OpenOptions{Range: &core.Range{Start: 1, End: 3}})
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(opened.Body)
	err = errors.Join(err, opened.Body.Close())
	if err != nil || string(data) != "bcd" || opened.Range == nil {
		t.Fatalf("range: %q %+v %v", data, opened.Range, err)
	}
	s.mu.Lock()
	encoded := false
	for _, v := range s.calls {
		if strings.Contains(v, "a%25+") {
			encoded = true
		}
	}
	s.mu.Unlock()
	if !encoded {
		t.Fatal("literal percent encoded incorrectly")
	}
	out, err := local.New(localRoot)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = c.Install("/out", out); err != nil {
		t.Fatal(err)
	}
	change, err := c.Copy(ctx, "alice", core.Path("/cloud/a%+中"), core.Path("/out/file"))
	if err != nil || change.Effect != core.Applied {
		t.Fatalf("Copy: %+v %v", change, err)
	}
	if data, err = os.ReadFile(filepath.Join(localRoot, "file")); err != nil || string(data) != "abcdef" {
		t.Fatal(string(data), err)
	}
	before = s.count("")
	change, err = c.Write(ctx, "alice", core.Path("/cloud/new"), strings.NewReader("new"), nil)
	if core.CodeOf(err) != core.UnsupportedOperation || change.Effect != core.Unchanged || s.count("") != before {
		t.Fatal("unsupported write sent I/O", change, err)
	}
	if _, err = c.Open(ctx, "alice", core.Path("/cloud/redirect"), core.OpenOptions{}); err == nil || s.count("GET /elsewhere") != 0 {
		t.Fatal("followed credential redirect", err)
	}
}

func TestCancellationLimitsAndExplicitSubtree(t *testing.T) {
	s, d, c, m := setup(t)
	ctx, stop := context.WithTimeout(context.Background(), 5*time.Second)
	defer stop()
	iter, err := d.Enumerate(ctx, core.NativeChildren(core.NativePath("/block")))
	if err != nil {
		t.Fatal(err)
	}
	call, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { _, err := iter.Next(call, 1); done <- err }()
	select {
	case <-s.block:
	case <-ctx.Done():
		t.Fatal("listing never blocked")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("listing cancel did not join")
	}
	if err = iter.Close(); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		path string
		code core.Code
	}{{"/junk", core.InvalidUpstreamResponse}, {"/escape", core.InvalidUpstreamResponse}, {"/throttle", core.UpstreamThrottled}} {
		iter, err = d.Enumerate(ctx, core.NativeChildren(core.NativePath(test.path)))
		if err != nil {
			t.Fatal(err)
		}
		_, err = iter.Next(ctx, 3)
		if core.CodeOf(err) != test.code {
			t.Fatalf("%s: %v", test.path, err)
		}
		if err = iter.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if _, e := c.List(ctx, "alice", core.ListRequest{Query: core.Children(core.Path("/cloud/truncated")), Limit: 1, CursorTTL: time.Minute}); core.CodeOf(e) != core.InvalidUpstreamResponse || errors.Is(e, io.EOF) {
		t.Fatal("truncated JSON became normal EOF", e)
	}
	before := s.count("DELETE")
	if change, err := d.RemoveSubtree(ctx, core.NativePath("/"), core.TreeOptions{}); core.CodeOf(err) != core.UnsupportedOperation || change.Effect != core.Unchanged || s.count("DELETE") != before {
		t.Fatal("root deleted", change, err)
	}
	tree, err := c.RemoveTree(ctx, "alice", core.Path("/cloud/tree"), core.TreeOptions{MaxEntries: 20, MaxPlanBytes: 2000}, core.DirectoryRemoveOptions{Scope: core.Subtree})
	if err != nil || tree.Change.Effect != core.Applied {
		t.Fatal(tree, err)
	}
	s.mu.Lock()
	_, exists := s.files["/zone/archive/tree/file"]
	s.mu.Unlock()
	if exists {
		t.Fatal("confirmed subtree deletion retained nested content")
	}
	before = s.count("DELETE")
	if change, err := d.RemoveSubtree(ctx, core.NativePath("/lost"), core.TreeOptions{}); err == nil || change.Effect != core.Unknown {
		t.Fatal("lost deletion confirmation", change, err)
	}
	if s.count("DELETE") != before+1 {
		t.Fatal("uncertain deletion was replayed")
	}
	if change, err := d.RemoveSubtree(ctx, core.NativePath("/accepted"), core.TreeOptions{}); err == nil || change.Effect == core.Applied {
		t.Fatal("accepted status treated as completed", change, err)
	}
	pending := make(chan error, 1)
	go func() {
		_, e := c.List(ctx, "alice", core.ListRequest{Query: core.Children(core.Path("/cloud/block")), Limit: 1, CursorTTL: time.Minute})
		pending <- e
	}()
	select {
	case <-s.block:
	case <-ctx.Done():
		t.Fatal("retirement listing never started")
	}
	if err = m.Retire(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case e := <-pending:
		if e == nil {
			t.Fatal("retired listing succeeded")
		}
	case <-ctx.Done():
		t.Fatal("retirement did not join the listing")
	}
	before = s.count("")
	if _, err = d.Stat(ctx, core.NativePath("/z")); core.CodeOf(err) != core.MountUnavailable || s.count("") != before {
		t.Fatal("retired admitted source", err)
	}
}

type transport func(*http.Request) (*http.Response, error)

func (f transport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type failingClose struct {
	io.Reader
	fail  atomic.Bool
	calls atomic.Int32
}

func (b *failingClose) Close() error {
	b.calls.Add(1)
	if b.fail.Load() {
		return errors.New("close receipt lost")
	}
	return nil
}
func TestBudgetAndFailedCleanup(t *testing.T) {
	body := &failingClose{Reader: strings.NewReader("[]")}
	body.fail.Store(true)
	client := &http.Client{Transport: transport(func(r *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: body, Request: r}, nil
	})}
	d, err := driver.Open(context.Background(), driver.Options{RootURL: "https://fixture.invalid/zone/root/", Client: client})
	if err == nil || d == nil {
		t.Fatal("failed close lost its owner", d, err)
	}
	before := body.calls.Load()
	if err = d.Close(); err == nil || body.calls.Load() <= before {
		t.Fatal("cleanup not retried", err)
	}
	body.fail.Store(false)
	if err = d.Close(); err != nil {
		t.Fatal(err)
	}
	client.Transport = transport(func(r *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("[]         ")), Request: r}, nil
	})
	d, err = driver.Open(context.Background(), driver.Options{RootURL: "https://fixture.invalid/zone/root/", Client: client, MaxResponseBytes: 3})
	if core.CodeOf(err) != core.ResponseBudgetExceeded {
		t.Fatal("metadata limit became EOF", err)
	}
	if d != nil {
		if err = d.Close(); err != nil {
			t.Fatal(err)
		}
	}
}
