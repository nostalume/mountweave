package pan115

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	core "github.com/nostalume/mountweave"
)

type wire func(*http.Request) (*http.Response, error)

func (f wire) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type fixture struct {
	mu        sync.Mutex
	mode      string
	calls     []string
	content   string
	entered   chan struct{}
	bodyCalls atomic.Int32
	broken    *badClose
}

func (s *fixture) set(mode string) { s.mu.Lock(); s.mode = mode; s.mu.Unlock() }
func (s *fixture) state() string   { s.mu.Lock(); defer s.mu.Unlock(); return s.mode }
func (s *fixture) count(part string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, v := range s.calls {
		if strings.Contains(v, part) {
			n++
		}
	}
	return n
}
func (s *fixture) control(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("Authorization") != "Bearer synthetic" {
		http.Error(w, "control authentication missing", 401)
		return
	}
	s.mu.Lock()
	s.calls = append(s.calls, r.Method+" "+r.URL.RequestURI())
	mode := s.mode
	s.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	if mode == "native-error" {
		fmt.Fprint(w, `{"state":false,"code":40140125,"errno":77,"message":"credential was superseded"}`)
		return
	}
	if strings.HasSuffix(r.URL.Path, "get_info") {
		id := r.URL.Query().Get("file_id")
		typ := "0"
		if id == "10" {
			typ = "1"
		}
		json.NewEncoder(w).Encode(map[string]any{"state": true, "data": map[string]any{"file_id": id, "file_name": "archive", "file_category": typ, "size_byte": 0}})
		return
	}
	if strings.HasSuffix(r.URL.Path, "downurl") {
		if err := r.ParseMultipartForm(1024); err != nil || r.FormValue("pick_code") != "pick10" {
			http.Error(w, "wrong native reference", 400)
			return
		}
		if mode == "grant-error" {
			w.WriteHeader(http.StatusForbidden)
			fmt.Fprint(w, `{"state":false,"code":40140125,"errno":77,"message":"grant rejected at https://body.example/blob?cap=private-capability"}`)
			return
		}
		id := "10"
		if mode == "wrong-receipt" {
			id = "99"
		}
		suffix := "/blob"
		if mode == "fallback" {
			suffix = "/fallback"
		}
		if mode == "body-wait" {
			suffix = "/waiting"
		}
		if mode == "redirect" {
			suffix = "/redirect"
		}
		size := 6
		if mode == "wrong-grant-size" {
			size = 5
		}
		json.NewEncoder(w).Encode(map[string]any{"state": true, "data": map[string]any{id: map[string]any{"file_name": "renamed-on-server", "file_size": size, "pick_code": "new-native-code", "url": map[string]any{"url": s.content + suffix + "?cap=private-capability"}}}})
		return
	}
	parent := r.URL.Query().Get("cid")
	offset, _ := strconv.Atoi(r.URL.Query().Get("offset"))
	if r.URL.Query().Get("cur") != "1" || r.URL.Query().Get("show_dir") != "1" {
		http.Error(w, "incomplete directory query", 400)
		return
	}
	if mode == "wait" {
		select {
		case s.entered <- struct{}{}:
		default:
		}
		<-r.Context().Done()
		return
	}
	if mode == "truncated" {
		fmt.Fprint(w, `{"state":true,"data":[`)
		return
	}
	f := map[string]any{"fid": "10", "pid": parent, "fn": "p%+中", "fc": "1", "pc": "pick10", "fs": 6, "isp": 0, "aid": "1", "fta": "1"}
	dir := map[string]any{"fid": "2", "pid": parent, "fn": "sub", "fc": "0", "isp": 0}
	all := []any{f, dir}
	if parent == "0" {
		all = []any{map[string]any{"fid": "1", "pid": "0", "fn": "archive", "fc": "0", "isp": 0}}
	}
	if parent == "2" {
		all = []any{}
	}
	if mode == "duplicate" {
		dir = map[string]any{"fid": "11", "pid": parent, "fn": "p%+中", "fc": "1", "pc": "pick11", "fs": 6}
		all = []any{f, dir}
	}
	if mode == "repeated" {
		all = []any{f, f}
	}
	if mode == "wrong-parent" {
		f["pid"] = "900"
	}
	if mode == "wrong-type" {
		f["fc"] = "9"
	}
	if mode == "negative" {
		f["fs"] = -1
	}
	if mode == "name" {
		f["fn"] = "a/b"
	}
	count := len(all)
	items := []any{}
	if offset < count {
		items = all[offset : offset+1]
	}
	if mode == "stalled" {
		items = []any{}
	}
	path := []any{map[string]any{"cid": parent, "isp": 0}}
	if mode == "no-current" {
		path = []any{}
	}
	if mode == "encrypted" {
		path = []any{map[string]any{"cid": parent, "isp": 1}}
	}
	if mode == "hidden" {
		path = []any{}
		items = []any{}
		count = 0
	}
	json.NewEncoder(w).Encode(map[string]any{"state": true, "data": items, "count": count, "cid": parent, "offset": offset, "limit": 1, "aid": "1", "path": path})
}
func (s *fixture) body(w http.ResponseWriter, r *http.Request) {
	s.bodyCalls.Add(1)
	if r.Header.Get("Authorization") != "" || r.Header.Get("Cookie") != "" || r.Header.Get("Referer") != "" {
		http.Error(w, "account information leaked", 403)
		return
	}
	if r.URL.Path == "/redirect" {
		w.Header().Set("Location", s.content+"/blob")
		w.WriteHeader(302)
		return
	}
	if r.URL.Path == "/waiting" {
		w.Header().Set("Content-Length", "6")
		w.WriteHeader(200)
		w.(http.Flusher).Flush()
		select {
		case s.entered <- struct{}{}:
		default:
		}
		<-r.Context().Done()
		return
	}
	data := "abcdef"
	status := 200
	if r.Header.Get("Range") != "" && r.URL.Path != "/fallback" {
		data = "bcd"
		status = 206
		w.Header().Set("Content-Range", "bytes 1-3/6")
	}
	w.Header().Set("Content-Length", strconv.Itoa(len(data)))
	w.WriteHeader(status)
	io.WriteString(w, data)
}

type badClose struct {
	io.ReadCloser
	fail  error
	calls int
}

func (b *badClose) Close() error {
	b.calls++
	if b.fail != nil {
		return b.fail
	}
	return b.ReadCloser.Close()
}
func setup(t *testing.T) (*fixture, *Driver, *core.Core, *core.Mount, Options) {
	t.Helper()
	s := &fixture{entered: make(chan struct{}, 4)}
	body := httptest.NewTLSServer(http.HandlerFunc(s.body))
	s.content = body.URL
	t.Cleanup(body.Close)
	api := httptest.NewServer(http.HandlerFunc(s.control))
	t.Cleanup(api.Close)
	base, _ := url.Parse(api.URL)
	client := &http.Client{Transport: wire(func(r *http.Request) (*http.Response, error) {
		if r.URL.Host != "proapi.115.com" {
			return nil, errors.New("control credential recipient changed")
		}
		call := r.Clone(r.Context())
		u := *r.URL
		u.Scheme, u.Host = base.Scheme, base.Host
		call.URL = &u
		call.Host = ""
		call.Header.Set("Authorization", "Bearer synthetic")
		response, err := http.DefaultTransport.RoundTrip(call)
		if err == nil && s.state() == "close" {
			s.broken = &badClose{ReadCloser: response.Body, fail: io.EOF}
			response.Body = s.broken
		}
		return response, err
	})}
	opts := Options{RootID: "1", Client: client, ContentClient: body.Client(), MaxResponseBytes: 4096}
	d, err := Open(context.Background(), opts)
	if err != nil {
		t.Fatal(err)
	}
	c, err := core.New(core.Options{Authorize: func(_ context.Context, _ core.Principal, _ core.Action, a core.Address) error {
		if strings.HasSuffix(a.Value(), "/denied") {
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
	return s, d, c, m, opts
}
func TestRootsPagesAndAuthorizedBody(t *testing.T) {
	s, d, c, m, opts := setup(t)
	ctx := context.Background()
	if err := c.BindAlias(ctx, "/alias", m, "/"); err != nil {
		t.Fatal(err)
	}
	invalid := opts
	invalid.ContentClient = invalid.Client
	infosBefore := s.count("get_info")
	rejected, err := Open(ctx, invalid)
	if rejected != nil {
		_ = rejected.Close()
	}
	if rejected != nil || core.CodeOf(err) != core.InvalidRequest || s.count("get_info") != infosBefore {
		t.Fatal("shared authenticated client passed constructor admission", rejected != nil, err)
	}
	opts.RootID, opts.RootPath = "", "/archive"
	second, err := Open(ctx, opts)
	if err != nil {
		t.Fatal(err)
	}
	if second.root.ID != d.root.ID || second == d {
		t.Fatal("independent root construction")
	}
	if err = second.Close(); err != nil {
		t.Fatal(err)
	}
	opts.RootID, opts.RootPath, opts.ContentClient = "", "", nil
	account, err := Open(ctx, opts)
	if err != nil {
		t.Fatal(err)
	}
	if account.content.Transport != nil {
		t.Fatal("default content client inherited the authenticated transport")
	}
	if meta, e := account.Stat(ctx, core.NativePath("/")); e != nil || meta.Kind != core.Directory {
		t.Fatal(meta, e)
	}
	if err = account.Close(); err != nil {
		t.Fatal(err)
	}
	opts.RootID = "1"
	s.set("no-current")
	readable, err := Open(ctx, opts)
	if err != nil {
		t.Fatal("nonempty native response was rejected", err)
	}
	if err = readable.Close(); err != nil {
		t.Fatal(err)
	}
	s.set("")
	opts.RootID, opts.RootPath = "1", "/archive"
	if invalid, err := Open(ctx, opts); invalid != nil || core.CodeOf(err) != core.InvalidRequest {
		t.Fatal("ambiguous root", invalid, err)
	}
	q := core.Children(core.Path("/alias"))
	p, err := c.List(ctx, "u", core.ListRequest{Query: q, Limit: 2, CursorTTL: time.Minute})
	if err != nil || len(p.Items) != 1 || p.NextCursor == nil {
		t.Fatal(p, err)
	}
	request := core.ListRequest{Query: q, Limit: 2, Cursor: *p.NextCursor}
	p, err = c.List(ctx, "u", request)
	if err != nil || len(p.Items) != 1 || p.Items[0].Name != "sub" {
		t.Fatal(p, err)
	}
	absent := core.OpenOptions{Selection: core.ReadSelection{When: core.IfAbsent()}}
	if _, err := c.Open(ctx, "u", core.Path("/remote/missing"), absent); core.CodeOf(err) != core.NotFound {
		t.Fatalf("condition invented a file: %v", err)
	}
	if _, err := c.Open(ctx, "u", core.Path("/remote/p%+中"), absent); core.CodeOf(err) != core.PreconditionFailed {
		t.Fatalf("existing file did not fail the absence condition: %v", err)
	}
	infos := s.count("get_info")
	opened, err := c.Open(ctx, "u", core.Path("/alias/p%+中"), core.OpenOptions{Range: &core.Range{Start: 1, End: 3}})
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(opened.Body)
	err = errors.Join(err, opened.Body.Close())
	if err != nil || string(data) != "bcd" || opened.Range == nil {
		t.Fatal(string(data), opened.Range, err)
	}
	if s.count("get_info") != infos {
		t.Fatal("located file was resolved again")
	}
	before := s.count("files")
	if _, err = c.Open(ctx, "u", core.Path("/alias/denied"), core.OpenOptions{}); core.CodeOf(err) != core.PermissionDenied || s.count("files") != before {
		t.Fatal("unauthorized native request", err)
	}
	opened, err = c.Open(ctx, "u", core.Path("/remote/p%+中"), core.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	data, err = io.ReadAll(opened.Body)
	if err = errors.Join(err, opened.Body.Close()); err != nil || string(data) != "abcdef" || opened.Range != nil {
		t.Fatal("complete original", string(data), err)
	}
	for _, mode := range []string{"fallback", "redirect"} {
		s.set(mode)
		opened, err = c.Open(ctx, "u", core.Path("/remote/p%+中"), core.OpenOptions{Range: &core.Range{Start: 1, End: 3}})
		if err != nil {
			t.Fatal(mode, err)
		}
		data, err = io.ReadAll(opened.Body)
		err = errors.Join(err, opened.Body.Close())
		if err != nil || mode == "fallback" && (string(data) != "abcdef" || opened.Range != nil) || mode == "redirect" && string(data) != "bcd" {
			t.Fatal(mode, string(data), err)
		}
	}
}
func TestNativeFailuresDoNotBecomeMissingOrEOF(t *testing.T) {
	s, d, c, _, opts := setup(t)
	ctx := context.Background()
	for mode, want := range map[string]core.Code{"duplicate": core.PathAmbiguous, "repeated": core.InvalidUpstreamResponse, "wrong-parent": core.InvalidUpstreamResponse, "wrong-type": core.InvalidUpstreamResponse, "negative": core.InvalidUpstreamResponse, "name": core.InvalidUpstreamResponse, "stalled": core.InvalidUpstreamResponse, "truncated": core.InvalidUpstreamResponse, "encrypted": core.UnsupportedOperation, "hidden": core.InvalidUpstreamResponse} {
		s.set(mode)
		if _, err := d.Stat(ctx, core.NativePath("/p%+中")); core.CodeOf(err) != want || errors.Is(err, io.EOF) {
			t.Fatal(mode, want, err)
		}
	}
	s.set("native-error")
	if _, err := d.Stat(ctx, core.NativePath("/p%+中")); err == nil || (!strings.Contains(err.Error(), "credential was superseded") || !strings.Contains(err.Error(), "errno 77")) {
		t.Fatal("lost native failure", err)
	}
	s.set("hidden")
	failed, err := Open(ctx, opts)
	if failed == nil || core.CodeOf(err) != core.InvalidUpstreamResponse {
		t.Fatal("hidden root became empty", err)
	}
	if err = failed.Close(); err != nil {
		t.Fatal(err)
	}
	before := s.bodyCalls.Load()
	s.set("grant-error")
	if _, err = c.Open(ctx, "u", core.Path("/remote/p%+中"), core.OpenOptions{}); core.CodeOf(err) != core.UpstreamAccessDenied ||
		!strings.Contains(err.Error(), "HTTP 403, code 40140125, errno 77") || strings.Contains(err.Error(), "private-capability") || s.bodyCalls.Load() != before {
		t.Fatal("private grant failure escaped or dispatched content", err)
	}
	s.set("wrong-receipt")
	if _, err = c.Open(ctx, "u", core.Path("/remote/p%+中"), core.OpenOptions{}); core.CodeOf(err) != core.InvalidUpstreamResponse || s.bodyCalls.Load() != before {
		t.Fatal("wrong file receipt consumed", err)
	}
	s.set("wrong-grant-size")
	opened, err := c.Open(ctx, "u", core.Path("/remote/p%+中"), core.OpenOptions{Range: &core.Range{Start: 1, End: 3}})
	if opened.Body != nil || core.CodeOf(err) != core.InvalidUpstreamResponse {
		t.Fatal("body total contradicted the same download receipt", err)
	}
	s.set("")
	d.resolvePages = 1
	if _, err = d.Stat(ctx, core.NativePath("/p%+中")); core.CodeOf(err) != core.ResolutionBudgetExceeded {
		t.Fatal(err)
	}
	d.resolvePages = 0
	d.limit = 64
	if _, err = d.Stat(ctx, core.NativePath("/p%+中")); core.CodeOf(err) != core.ResponseBudgetExceeded {
		t.Fatal(err)
	}
	d.limit = 4096
	s.set("close")
	if _, err = d.Stat(ctx, core.NativePath("/p%+中")); err == nil || errors.Is(err, io.EOF) {
		t.Fatal("false terminal EOF or lost cleanup", err)
	}
	broken := s.broken
	broken.fail = nil
	s.set("")
	if err = d.Close(); err != nil || broken.calls != 2 {
		t.Fatal("cleanup ownership", broken.calls, err)
	}
}
func TestCancellationRetirementAndOwnership(t *testing.T) {
	s, d, c, _, _ := setup(t)
	ctx := context.Background()
	s.set("wait")
	call, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		_, err := c.List(call, "u", core.ListRequest{Query: core.Children(core.Path("/remote")), Limit: 2, CursorTTL: time.Minute})
		done <- err
	}()
	select {
	case <-s.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("native request did not start")
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
	path := core.Path("/remote/p%+中")
	transport := d.content.Transport
	var broken []*badClose
	d.content.Transport = wire(func(r *http.Request) (*http.Response, error) {
		cause := fmt.Errorf("content request %s: %w", r.URL, io.EOF)
		mode := s.state()
		if mode == "signed-failure" {
			return nil, cause
		}
		reply, err := transport.RoundTrip(r)
		if err == nil && (mode == "redirect" || mode == "unsolicited-range") {
			if mode == "unsolicited-range" {
				reply.StatusCode = http.StatusRequestedRangeNotSatisfiable
			}
			body := &badClose{ReadCloser: reply.Body, fail: cause}
			broken = append(broken, body)
			reply.Body = body
		}
		return reply, err
	})
	before := s.bodyCalls.Load()
	for _, mode := range []string{"signed-failure", "redirect", "unsolicited-range"} {
		s.set(mode)
		opened, err := c.Open(ctx, "u", path, core.OpenOptions{})
		if opened.Body != nil || !errors.Is(err, io.ErrUnexpectedEOF) || strings.Contains(err.Error(), "private-capability") ||
			mode == "unsolicited-range" && core.CodeOf(err) != core.InvalidUpstreamResponse {
			t.Fatal("private response failed without a safe cause", mode, err)
		}
	}
	if s.bodyCalls.Load() != before+2 || len(broken) != 2 {
		t.Fatal("failed response was followed or lost")
	}
	for i, body := range broken {
		if body.calls != 1 {
			t.Fatal("failed response was closed twice or lost", i)
		}
		body.fail = nil
	}
	s.set("body-wait")
	opened, err := c.Open(ctx, "u", path, core.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-s.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("body did not start")
	}
	go func() { _, err := io.ReadAll(opened.Body); done <- err }()
	closeCtx, stop := context.WithTimeout(ctx, 3*time.Second)
	defer stop()
	if err = c.Close(closeCtx); err != nil {
		t.Fatal(err)
	}
	for i, body := range broken {
		if body.calls != 2 {
			t.Fatal("retirement did not retry private response cleanup", i, body.calls)
		}
	}
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("retired body silently completed")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("retirement did not join")
	}
	if err = opened.Body.Close(); err != nil {
		t.Fatal(err)
	}
}
