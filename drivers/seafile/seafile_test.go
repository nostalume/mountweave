package seafile

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	core "github.com/nostalume/mountweave"
)

const libraryID = "01234567-89ab-cdef-0123-456789abcdef"

type service struct {
	mu               sync.Mutex
	calls            []string
	content          string
	entered, release chan struct{}
	encrypted        atomic.Bool
	bodyCalls        atomic.Int32
}

func (s *service) count(suffix string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, v := range s.calls {
		if strings.Contains(v, suffix) {
			n++
		}
	}
	return n
}
func (s *service) api(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("Authorization") != "Token synthetic" || r.Header.Get("X-Control-Only") != "yes" {
		http.Error(w, "control auth required", 401)
		return
	}
	s.mu.Lock()
	s.calls = append(s.calls, r.URL.EscapedPath()+"?"+r.URL.RawQuery)
	s.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	p := r.URL.Query().Get("p")
	switch {
	case strings.HasSuffix(r.URL.Path, "/file/detail/"):
		if p == "/archive" || p == "/archive/sub" {
			http.NotFound(w, r)
			return
		}
		json.NewEncoder(w).Encode(map[string]any{"name": path.Base(p), "type": "file", "size": 6, "mtime": 1445412480})
	case strings.HasSuffix(r.URL.Path, "/dir/detail/"):
		p = r.URL.Query().Get("path")
		json.NewEncoder(w).Encode(map[string]any{"repo_id": libraryID, "path": p + "/", "name": path.Base(p), "mtime": "2015-10-21T07:28:00Z"})
	case strings.HasSuffix(r.URL.Path, "/dir/"):
		switch p {
		case "/archive/waiting":
			fmt.Fprint(w, "[")
			w.(http.Flusher).Flush()
			select {
			case s.entered <- struct{}{}:
			default:
			}
			select {
			case <-r.Context().Done():
			case <-s.release:
			}
		case "/archive/native-error":
			w.WriteHeader(http.StatusForbidden)
			fmt.Fprint(w, `{"error_msg":"library access revoked"}`)
		case "/archive/truncated":
			fmt.Fprint(w, "[")
		case "/archive/trailing":
			fmt.Fprint(w, "[] false")
		case "/archive/foreign":
			fmt.Fprint(w, `[{"name":"x","type":"file","size":1,"parent_dir":"/foreign"}]`)
		case "/archive/huge":
			fmt.Fprintf(w, `[{"name":"x","type":"file","size":1,"padding":"%s"}]`, strings.Repeat("x", 1025))
		default:
			fmt.Fprint(w, `[{"name":"a%+中","type":"file","size":6,"mtime":1445412480,"parent_dir":"/archive"},{"name":"sub","type":"dir","parent_dir":"/archive"}]`)
		}
	case strings.HasSuffix(r.URL.Path, "/file/"):
		if p == "/archive/missing" {
			http.NotFound(w, r)
			return
		}
		target := s.content + "/files/cap/" + url.PathEscape(path.Base(p))
		if strings.HasSuffix(p, "/invalid") {
			target = "file:///secret"
		}
		json.NewEncoder(w).Encode(target)
	default:
		json.NewEncoder(w).Encode(map[string]any{"id": libraryID, "encrypted": s.encrypted.Load(), "mtime": 1445412480})
	}
}
func (s *service) body(w http.ResponseWriter, r *http.Request) {
	s.bodyCalls.Add(1)
	if r.Header.Get("Authorization") != "" || r.Header.Get("Cookie") != "" || r.Header.Get("X-Control-Only") != "" {
		http.Error(w, "account credentials leaked", 500)
		return
	}
	if strings.HasSuffix(r.URL.Path, "/expired") {
		http.Error(w, "expired capability", 403)
		return
	}
	if strings.HasSuffix(r.URL.Path, "/redirect") {
		w.Header().Set("Location", "http://outside.invalid/secret")
		w.WriteHeader(307)
		return
	}
	data := "abcdef"
	w.Header().Set("ETag", "bare-file-id")
	w.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
	if raw := r.Header.Get("Range"); raw != "" && !strings.HasSuffix(r.URL.Path, "/fallback") {
		left, right, _ := strings.Cut(strings.TrimPrefix(raw, "bytes="), "-")
		start, _ := strconv.Atoi(left)
		end := 5
		if left == "" {
			n, _ := strconv.Atoi(right)
			start = max(0, 6-n)
		} else if right != "" {
			end, _ = strconv.Atoi(right)
			end = min(5, end)
		}
		if start > end {
			w.Header().Set("Content-Range", "bytes */6")
			w.WriteHeader(416)
			return
		}
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/6", start, end))
		data = data[start : end+1]
		w.Header().Set("Content-Length", strconv.Itoa(len(data)))
		w.WriteHeader(206)
	} else {
		w.Header().Set("Content-Length", strconv.Itoa(len(data)))
	}
	fmt.Fprint(w, data)
}

type transport func(*http.Request) (*http.Response, error)

func (f transport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
func setup(t *testing.T) (*service, *Driver, *core.Core, *core.Mount) {
	t.Helper()
	s := &service{entered: make(chan struct{}, 2), release: make(chan struct{})}
	body := httptest.NewServer(http.HandlerFunc(s.body))
	t.Cleanup(body.Close)
	s.content = body.URL
	api := httptest.NewServer(http.HandlerFunc(s.api))
	t.Cleanup(api.Close)
	t.Cleanup(func() { close(s.release) })
	client := api.Client()
	wire := client.Transport
	client.Transport = transport(func(r *http.Request) (*http.Response, error) {
		r.Header.Set("X-Control-Only", "yes")
		return wire.RoundTrip(r)
	})
	d, err := Open(context.Background(), Options{ServerURL: api.URL + "/base", LibraryID: libraryID, RootPath: "/archive", Token: "synthetic", Client: client})
	if err != nil {
		t.Fatal(err)
	}
	c, err := core.New(core.Options{Authorize: func(_ context.Context, _ core.Principal, _ core.Action, a core.Address) error {
		if a.Value() == "/remote/denied" {
			return core.Fail(core.PermissionDenied, nil)
		}
		return nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	m, err := c.Install("/remote", d)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := c.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	return s, d, c, m
}

func TestLibraryBrowseAndBody(t *testing.T) {
	s, _, c, m := setup(t)
	ctx := context.Background()
	if err := c.BindAlias(ctx, "/alias", m, "/"); err != nil {
		t.Fatal(err)
	}
	q := core.Children(core.Path("/remote"))
	before := s.count("/dir/")
	first, err := c.List(ctx, "user", core.ListRequest{Query: q, Limit: 1, CursorTTL: time.Minute})
	if err != nil || len(first.Items) != 1 || first.NextCursor == nil {
		t.Fatal(first, err)
	}
	request := core.ListRequest{Query: q, Limit: 1, Cursor: *first.NextCursor}
	page, err := c.List(ctx, "user", request)
	if err != nil || len(page.Items) != 1 || page.Items[0].Kind != core.Directory || s.count("/dir/") != before+1 {
		t.Fatal("public page did not reuse the native listing", page, err)
	}
	for _, name := range []string{"a%+中", "e\u0301"} {
		entry, err := c.Stat(ctx, "user", core.Path("/remote/"+name))
		if err != nil || entry.Name != name || entry.ModifiedAt == nil {
			t.Fatal(entry, err)
		}
	}
	for _, name := range []string{"a%+中", "fallback"} {
		before := s.count("/file/detail/")
		opened, err := c.Open(ctx, "user", core.Path("/alias/"+name), core.OpenOptions{Range: &core.Range{Start: 1, End: 3}})
		if err != nil {
			t.Fatal(err)
		}
		data, err := io.ReadAll(opened.Body)
		err = errors.Join(err, opened.Body.Close())
		want := "bcd"
		if name == "fallback" {
			want = "abcdef"
		}
		if err != nil || string(data) != want || opened.Entry.ModifiedAt != nil || opened.Entry.Evidence.Validator != (core.Validator{}) || s.count("/file/detail/") != before {
			t.Fatalf("body: %q %+v %v", data, opened, err)
		}
	}
	before = s.count("")
	input := strings.NewReader("data")
	if change, err := c.Write(ctx, "user", core.Path("/remote/new"), input, nil); core.CodeOf(err) != core.UnsupportedOperation || change.Effect != core.Unchanged || input.Len() != 4 {
		t.Fatal(change, err)
	}
	if _, err = c.Open(ctx, "user", core.Path("/remote/denied"), core.OpenOptions{}); core.CodeOf(err) != core.PermissionDenied || s.count("") != before {
		t.Fatal("refused request reached native control", err)
	}
}

func TestCapabilitiesAndMetadataFailures(t *testing.T) {
	s, d, c, _ := setup(t)
	ctx := context.Background()
	for _, name := range []string{"truncated", "trailing", "foreign", "huge"} {
		old := d.limit
		d.limit = 512
		_, err := c.List(ctx, "user", core.ListRequest{Query: core.Children(core.Path("/remote/" + name)), Limit: 1, CursorTTL: time.Minute})
		d.limit = old
		if err == nil || name == "huge" && core.CodeOf(err) != core.ResponseBudgetExceeded {
			t.Fatalf("%s: %v", name, err)
		}
	}
	if _, err := c.List(ctx, "user", core.ListRequest{Query: core.Children(core.Path("/remote/native-error")), Limit: 1, CursorTTL: time.Minute}); core.CodeOf(err) != core.UpstreamAccessDenied || !strings.Contains(err.Error(), "library access revoked") {
		t.Fatal("native failure reason lost", err)
	}
	before := s.bodyCalls.Load()
	absent := core.OpenOptions{Selection: core.ReadSelection{When: core.IfAbsent()}}
	if _, err := c.Open(ctx, "user", core.Path("/remote/missing"), absent); core.CodeOf(err) != core.NotFound {
		t.Fatalf("condition invented a file: %v", err)
	}
	if _, err := c.Open(ctx, "user", core.Path("/remote/a%+中"), absent); core.CodeOf(err) != core.PreconditionFailed || s.bodyCalls.Load() != before {
		t.Fatalf("existing file did not fail the absence condition: %v", err)
	}
	if _, err := c.Open(ctx, "user", core.Path("/remote/invalid"), core.OpenOptions{}); core.CodeOf(err) != core.InvalidUpstreamResponse || s.bodyCalls.Load() != before {
		t.Fatal("invalid capability reached body client", err)
	}
	for _, name := range []string{"expired", "redirect"} {
		if _, err := c.Open(ctx, "user", core.Path("/remote/"+name), core.OpenOptions{}); err == nil {
			t.Fatal("accepted invalid body outcome", name)
		}
	}
	s.encrypted.Store(true)
	if _, err := d.libraryInfo(ctx); core.CodeOf(err) != core.UnsupportedOperation {
		t.Fatal("encrypted library accepted", err)
	}
	s.encrypted.Store(false)
	// An EOF before HTTP response headers is a failure, not an empty native list.
	original := d.api.Transport
	d.api.Transport = transport(func(r *http.Request) (*http.Response, error) {
		if strings.HasSuffix(r.URL.Path, "/dir/") {
			return nil, io.EOF
		}
		return original.RoundTrip(r)
	})
	if _, err := c.List(ctx, "user", core.ListRequest{Query: core.Children(core.Path("/remote")), Limit: 1, CursorTTL: time.Minute}); err == nil || !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatal("transport EOF became directory EOF", err)
	}
	d.api.Transport = original
}

type failedClose struct {
	io.Reader
	fail atomic.Bool
}

func (b *failedClose) Close() error {
	if b.fail.Load() {
		return io.EOF
	}
	return nil
}
func TestCancellationRetirementAndCleanup(t *testing.T) {
	s, d, c, m := setup(t)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := c.List(ctx, "user", core.ListRequest{Query: core.Children(core.Path("/remote/waiting")), Limit: 1, CursorTTL: time.Minute})
		done <- err
	}()
	select {
	case <-s.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("listing not started")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("cancel did not join")
	}
	go func() {
		_, err := c.List(context.Background(), "user", core.ListRequest{Query: core.Children(core.Path("/remote/waiting")), Limit: 1, CursorTTL: time.Minute})
		done <- err
	}()
	select {
	case <-s.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("second listing not started")
	}
	wait, stop := context.WithTimeout(context.Background(), 3*time.Second)
	defer stop()
	if err := m.Retire(wait); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("retirement reported success")
		}
	case <-wait.Done():
		t.Fatal("retirement did not join")
	}
	if _, err := d.Stat(context.Background(), core.NativePath("/")); core.CodeOf(err) != core.MountUnavailable {
		t.Fatal(err)
	}
	body := &failedClose{Reader: bytes.NewBufferString(`{"id":"` + libraryID + `","encrypted":false}`)}
	body.fail.Store(true)
	client := &http.Client{Transport: transport(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: body}, nil
	})}
	bad, err := Open(context.Background(), Options{ServerURL: "https://fixture.invalid", LibraryID: libraryID, Client: client})
	if err == nil || bad == nil || errors.Is(err, io.EOF) {
		t.Fatal("failed cleanup has no owner", err)
	}
	body.fail.Store(false)
	if err = bad.Close(); err != nil {
		t.Fatal(err)
	}
}
