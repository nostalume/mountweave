package baidupan

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/local"
)

const nativeName = "p%+中"
const firstToken = "synthetic first token+/"
const secondToken = "synthetic next token+/"
const jsonFile = `{"errno":31360}`
const signedPrefix = "sign=raw%2f+%2B&part=%26A%3d&file="

// This synthetic boundary is not a claim about Baidu service path limits.
var syntheticRoot = strings.Repeat("/r", core.MaxPathSegments)

type wire func(*http.Request) (*http.Response, error)

func (f wire) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type fixture struct {
	mu         sync.Mutex
	mode       string
	calls      []string
	tokens     map[string]string
	current    atomic.Value
	tokenCalls atomic.Int32
	unexpected atomic.Int32
	entered    chan struct{}
	broken     *badClose
	uploaded   map[string]string
	postToken  string
	uploadRead chan struct{}
	afterClose func()
	ackSize    int
}

func (s *fixture) set(mode string) { s.mu.Lock(); s.mode = mode; s.mu.Unlock() }
func (s *fixture) state() string   { s.mu.Lock(); defer s.mu.Unlock(); return s.mode }
func (s *fixture) count(part string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, c := range s.calls {
		if strings.Contains(c, part) {
			n++
		}
	}
	return n
}
func (s *fixture) bytes(p string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if data, ok := s.uploaded[p]; ok {
		return data
	}
	switch path.Base(p) {
	case "Json":
		return jsonFile
	case "Empty":
		return ""
	default:
		return "abcdef"
	}
}
func (s *fixture) sampleItem(p string) map[string]any {
	folder := p == "/Archive" || p == "/Archive/Dir" || p == syntheticRoot
	ids := map[string]uint64{
		"/Archive": 18446744073709551614, "/Archive/Dir": 18446744073709551613,
		"/Archive/" + nativeName: 18446744073709551612, "/Archive/Dir/" + nativeName: 18446744073709551611,
		"/Archive/A.bin": 18446744073709551610, "/Archive/Empty": 18446744073709551609, "/Archive/Json": 18446744073709551608,
		syntheticRoot: 18446744073709551607, syntheticRoot + "/" + nativeName: 18446744073709551606,
	}
	id := ids[p]
	f := map[string]any{"errno": 0, "fs_id": id, "path": p, "server_filename": path.Base(p), "isdir": 0, "size": len(s.bytes(p)), "server_mtime": 1731234567, "isdelete": 0}
	if folder {
		f["isdir"] = 1
		f["size"] = 0
	}
	return f
}
func nativeError(w http.ResponseWriter, status, code int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]any{"errno": code, "errmsg": message, "request_id": "12345678901234567890"})
}
func (s *fixture) serve(w http.ResponseWriter, r *http.Request) {
	mode := s.state()
	logicalHost := r.Header.Get("X-Fixture-Host")
	if r.Header.Get("Authorization") != "" || r.Header.Get("Cookie") != "" || r.Header.Get("Referer") != "" {
		nativeError(w, 400, -1, "unexpected account headers")
		return
	}
	control := logicalHost == "pan.baidu.com"
	values := r.URL.Query()
	p, method, token := "", values.Get("method"), values.Get("access_token")
	if control && r.Method == "POST" {
		if r.URL.Path != "/rest/2.0/xpan/file" || r.URL.RawQuery != "" {
			nativeError(w, 400, -1, "wrong metadata route")
			return
		}
		if err := r.ParseForm(); err != nil {
			nativeError(w, 400, -1, "bad metadata form")
			return
		}
		values = r.PostForm
		method, token = values.Get("method"), values.Get("access_token")
		var targets []string
		if values.Get("blocks") != "0" || values.Get("media") != "0" || json.Unmarshal([]byte(values.Get("target")), &targets) != nil || len(targets) != 1 {
			nativeError(w, 400, -1, "wrong metadata arguments")
			return
		}
		p = targets[0]
	} else if control {
		p = values.Get("dir")
	} else if logicalHost == "upload.fixture" {
		p = values.Get("path")
	} else {
		p = values.Get("file")
	}
	s.mu.Lock()
	s.calls = append(s.calls, method+" "+logicalHost+r.URL.Path+" "+p+" start="+values.Get("start"))
	s.mu.Unlock()
	if logicalHost == "d.pcs.baidu.com" {
		if r.Method != "GET" || r.URL.Path != "/rest/2.0/pcs/file" || r.URL.RawQuery != "method=locateupload" {
			nativeError(w, 400, -1, "discovery carried account or task state")
			return
		}
		if mode == "upload-locate-fail" {
			io.WriteString(w, `{"error_code":31024,"error_msg":"discovery refused"}`)
			return
		}
		s.mu.Lock()
		s.postToken = s.current.Load().(string)
		s.mu.Unlock()
		if mode == "upload-rotate" {
			s.current.Store(secondToken)
		}
		io.WriteString(w, `{"error_code":0,"server":["invalid/path","upload.fixture"]}`)
		return
	}
	if logicalHost == "upload.fixture" {
		s.mu.Lock()
		expected := s.postToken
		s.mu.Unlock()
		if r.Method != "POST" || r.URL.Path != "/rest/2.0/pcs/file" || method != "upload" || values.Get("ondup") != "fail" || token != expected || len(values["access_token"]) != 1 || !strings.HasPrefix(p, "/Archive/") {
			nativeError(w, 400, -1, "invalid exclusive upload or credential recipient")
			return
		}
		if mode == "upload-conflict" || mode == "upload-inflight" {
			nativeError(w, 200, 31061, "native conflict "+token)
			return
		}
		if mode == "upload-redirect" {
			w.Header().Set("Location", "https://upload.fixture/replay")
			w.WriteHeader(302)
			return
		}
		if mode == "upload-early" {
			json.NewEncoder(w).Encode(map[string]any{"fs_id": 0, "path": p, "size": 0})
			return
		}
		parts, err := r.MultipartReader()
		if err != nil {
			nativeError(w, 400, -1, "missing multipart body")
			return
		}
		part, err := parts.NextPart()
		if err != nil || part.FormName() != "file" || part.FileName() == "" {
			nativeError(w, 400, -1, "upload did not contain one file")
			return
		}
		data, readErr := io.ReadAll(part)
		if readErr != nil && mode != "upload-input-failure" {
			nativeError(w, 400, -1, "incomplete multipart body")
			return
		}
		if readErr == nil {
			if _, err := parts.NextPart(); err != io.EOF {
				nativeError(w, 400, -1, "unexpected upload fields")
				return
			}
		}
		s.mu.Lock()
		if s.ackSize >= 0 && s.ackSize <= len(data) {
			data = data[:s.ackSize]
		}
		s.uploaded[p] = string(data)
		s.mu.Unlock()
		if mode == "upload-lost" {
			connection, _, _ := w.(http.Hijacker).Hijack()
			_ = connection.Close()
			return
		}
		if mode == "upload-truncated" {
			io.WriteString(w, `{"errno":0,"fs_id":0,"path":`)
			return
		}
		receipt := map[string]any{"fs_id": 0, "path": p, "size": len(data)}
		if mode == "upload-wrong-path" {
			receipt["path"] = "/Outside/" + path.Base(p)
		}
		if mode == "upload-wrong-size" {
			receipt["size"] = len(data) + 1
		}
		json.NewEncoder(w).Encode(receipt)
		return
	}
	if control {
		if token != s.current.Load().(string) {
			nativeError(w, 401, 31045, "wrong token snapshot")
			return
		}
		if mode == "wait" {
			s.wait(r)
			return
		}
		if mode == "truncated" {
			io.WriteString(w, `{"errno":0,"info":[`)
			return
		}
		if method == "filemetas" && r.Method == "POST" {
			if p != "/Archive" && !strings.HasPrefix(p, "/Archive/") && p != syntheticRoot && p != syntheticRoot+"/"+nativeName {
				nativeError(w, 400, -1, "path escaped root")
				return
			}
			f := s.sampleItem(p)
			switch mode {
			case "wrong-parent":
				f["path"] = "/Outside/" + path.Base(p)
			case "wrong-type":
				f["isdir"] = 2
			case "overflow-id":
				f["fs_id"] = json.RawMessage("18446744073709551616")
			case "item-error":
				f["errno"] = -9
			case "meta-size-mismatch":
				f["size"] = 7
			case "root-replaced":
				if p == "/Archive" {
					f["isdir"], f["size"], f["fs_id"] = 0, len(s.bytes(p)), uint64(42)
				}
			}
			if values.Get("dlink") == "1" {
				f["dlink"] = "https://download.fixture/content?" + signedPrefix + url.QueryEscape(p)
				s.mu.Lock()
				s.tokens[p] = token
				s.mu.Unlock()
				if mode == "rotate" {
					s.current.Store(secondToken)
				}
			} else if values.Get("dlink") != "0" && values.Get("dlink") != "" {
				nativeError(w, 400, -1, "invalid dlink selection")
				return
			}
			json.NewEncoder(w).Encode(map[string]any{"errno": 0, "request_id": "12345678901234567890", "info": []any{f}})
			return
		}
		if method == "list" && r.Method == "GET" {
			if values.Get("order") != "name" || values.Get("desc") != "0" || values.Get("folder") != "0" || p != "/Archive" {
				nativeError(w, 400, -1, "wrong listing policy")
				return
			}
			start, err := strconv.Atoi(values.Get("start"))
			if err != nil {
				nativeError(w, 400, -1, "missing start")
				return
			}
			if mode == "outer-list-error" || mode == "nested-list-error" {
				outer, inner := 0, 0
				if mode == "outer-list-error" {
					outer = 31045
				} else {
					inner = 31045
				}
				json.NewEncoder(w).Encode(map[string]any{"errno": outer, "data": map[string]any{"errno": inner, "list": []any{}}})
				return
			}
			var entries []any
			switch start {
			case 0:
				entries = []any{s.sampleItem("/Archive/A.bin")}
			case 1000:
				entries = []any{s.sampleItem("/Archive/Dir"), s.sampleItem("/Archive/Empty"), s.sampleItem("/Archive/" + nativeName)}
			case 2000:
				entries = []any{}
			default:
				nativeError(w, 400, -1, "wrong native progress")
				return
			}
			if mode == "dot-list-parent" && len(entries) > 0 {
				entries[0].(map[string]any)["path"] = "/Archive/Dir/../A.bin"
			}
			var result any = map[string]any{"errno": 0, "request_id": 1234567890, "list": entries}
			if start == 1000 {
				result = map[string]any{"errno": 0, "data": map[string]any{"errno": 0, "list": entries}}
			}
			json.NewEncoder(w).Encode(result)
			return
		}
		nativeError(w, 400, -1, "unexpected control request")
		return
	}
	if r.Method != "GET" || r.Header.Get("User-Agent") != "pan.baidu.com" {
		nativeError(w, 400, -1, "wrong native content request")
		return
	}
	s.mu.Lock()
	expected := s.tokens[p]
	s.mu.Unlock()
	if r.URL.Path == "/cross" || r.URL.Path == "/final" {
		expected = ""
	}
	if token != expected || expected != "" && len(r.URL.Query()["access_token"]) != 1 {
		nativeError(w, 401, 31045, "content token recipient changed")
		return
	}
	if r.URL.Path == "/same" {
		var retained []string
		for _, field := range strings.Split(r.URL.RawQuery, "&") {
			if !strings.HasPrefix(field, "access_token=") {
				retained = append(retained, field)
			}
		}
		if strings.Join(retained, "&") != "sign=hop%2f+%2B&file="+url.QueryEscape(p)+"&tail=%26B%3d" {
			nativeError(w, 400, 31362, "same-origin signature was rewritten")
			return
		}
	}
	if r.URL.Path == "/cross" && r.URL.RawQuery != "file="+url.QueryEscape(p)+"&cap=delegated%2b" || r.URL.Path == "/final" && r.URL.RawQuery != "file="+url.QueryEscape(p)+"&cap=returned%2b" {
		nativeError(w, 400, 31362, "native Location was rewritten")
		return
	}
	if r.URL.Path == "/content" && !strings.HasPrefix(r.URL.RawQuery, signedPrefix+url.QueryEscape(p)+"&access_token=") {
		nativeError(w, 400, 31362, "signed query was rewritten")
		return
	}
	if mode == "redirect" || mode == "redirect-close" || mode == "redirect-echo" {
		target := "https://download.fixture/same?sign=hop%2f+%2B&access_token=old&file=" + url.QueryEscape(p) + "&tail=%26B%3d&access_token=older"
		switch r.URL.Path {
		case "/same":
			target = "https://cdn.fixture/cross?file=" + url.QueryEscape(p) + "&cap=delegated%2b"
			if mode == "redirect-echo" {
				target += "&access_token=delegated&access%5ftoken=" + url.QueryEscape(expected)
			}
		case "/cross":
			target = "https://download.fixture/final?file=" + url.QueryEscape(p) + "&cap=returned%2b"
		case "/final":
			target = ""
		}
		if target != "" {
			w.Header().Set("Location", target)
			w.WriteHeader(302)
			return
		}
	}
	if mode == "downgrade" {
		w.Header().Set("Location", "http://download.fixture/final?file="+url.QueryEscape(p))
		w.WriteHeader(302)
		return
	}
	if mode == "loop" {
		w.Header().Set("Location", "https://download.fixture/content?"+r.URL.RawQuery)
		w.WriteHeader(302)
		return
	}
	data := s.bytes(p)
	if path.Base(p) == "Json" {
		w.Header().Set("Content-Type", "application/json")
	}
	if data == "" {
		if r.Header.Get("Range") != "" {
			nativeError(w, 400, -1, "empty file range")
			return
		}
		if mode == "empty-error" {
			data = jsonFile
		}
		if mode == "empty-unknown" || mode == "empty-error" {
			w.WriteHeader(200)
			w.(http.Flusher).Flush()
			io.WriteString(w, data)
		} else {
			w.Header().Set("Content-Length", "0")
			w.WriteHeader(200)
		}
		return
	}
	if mode == "unconfirmed" || mode == "unconfirmed-close" {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Content-Length", strconv.Itoa(len(data)))
		io.WriteString(w, data)
		return
	}
	raw := r.Header.Get("Range")
	var start, end int
	end = len(data) - 1
	if raw == "bytes=0-" {
		start = 0
	} else if _, err := fmt.Sscanf(raw, "bytes=%d-%d", &start, &end); err != nil {
		nativeError(w, 400, -1, "missing native range receipt")
		return
	}
	if start >= len(data) || mode == "range-rejected" {
		w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", len(data)))
		w.WriteHeader(416)
		return
	}
	end = min(end, len(data)-1)
	part := data[start : end+1]
	w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(data)))
	length := len(part)
	if mode == "length-mismatch" {
		length++
	}
	w.Header().Set("Content-Length", strconv.Itoa(length))
	w.WriteHeader(206)
	if mode == "body-wait" {
		w.(http.Flusher).Flush()
		s.wait(r)
		return
	}
	io.WriteString(w, part)
}
func (s *fixture) wait(r *http.Request) {
	select {
	case s.entered <- struct{}{}:
	default:
	}
	<-r.Context().Done()
}

type badClose struct {
	io.ReadCloser
	calls int
	after func()
	cause error
}

func (b *badClose) Close() error {
	b.calls++
	if b.calls == 1 {
		if b.after != nil {
			b.after()
		}
		if b.cause != nil {
			return b.cause
		}
		return io.EOF
	}
	return b.ReadCloser.Close()
}
func setup(t *testing.T) (*fixture, *Driver, *core.Core, *core.Mount, Options) {
	t.Helper()
	s := &fixture{tokens: make(map[string]string), uploaded: make(map[string]string), entered: make(chan struct{}, 2), ackSize: -1}
	s.current.Store(firstToken)
	server := httptest.NewServer(http.HandlerFunc(s.serve))
	t.Cleanup(server.Close)
	base, _ := url.Parse(server.URL)
	jar, _ := cookiejar.New(nil)
	hosts := []string{"pan.baidu.com", "download.fixture", "cdn.fixture", "d.pcs.baidu.com", "upload.fixture"}
	for _, host := range hosts {
		u, _ := url.Parse("https://" + host)
		jar.SetCookies(u, []*http.Cookie{{Name: "session", Value: "must-not-send"}})
	}
	client := &http.Client{Jar: jar, Transport: wire(func(r *http.Request) (*http.Response, error) {
		if r.URL.Scheme != "https" || !slices.Contains(hosts, r.URL.Host) {
			s.unexpected.Add(1)
			return nil, errors.New("unexpected HTTP recipient")
		}
		if s.state() == "connection-eof" && r.URL.Host != "pan.baidu.com" {
			return nil, io.EOF
		}
		call := r.Clone(r.Context())
		u := *r.URL
		u.Scheme, u.Host = base.Scheme, base.Host
		call.URL = &u
		call.Host = ""
		call.Header.Set("X-Fixture-Host", r.URL.Host)
		if r.URL.Host == "upload.fixture" {
			if r.GetBody != nil {
				s.unexpected.Add(1)
			}
			mode := s.state()
			if mode == "upload-early" || mode == "upload-conflict" || mode == "upload-input-failure" || mode == "upload-inflight" {
				if mode == "upload-inflight" {
					go func() { defer close(s.uploadRead); defer r.Body.Close(); _, _ = io.Copy(io.Discard, r.Body) }()
					<-s.entered
				} else {
					defer r.Body.Close()
				}
				recorder := httptest.NewRecorder()
				s.serve(recorder, call)
				return recorder.Result(), nil
			}
		}
		response, err := http.DefaultTransport.RoundTrip(call)
		mode := s.state()
		if err == nil && (mode == "redirect-close" && response.StatusCode == 302 || mode == "upload-close" && r.URL.Host == "upload.fixture" ||
			mode == "unconfirmed-close" && r.URL.Host == "download.fixture") {
			s.broken = &badClose{ReadCloser: response.Body, after: s.afterClose}
			if mode == "unconfirmed-close" {
				s.broken.cause = fmt.Errorf("closing %s with credential %s: %w", r.URL, r.URL.Query().Get("access_token"), io.EOF)
			}
			response.Body = s.broken
		}
		return response, err
	})}
	opts := Options{RootPath: "/Archive", Client: client, AccessToken: func(ctx context.Context) (string, error) {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		s.tokenCalls.Add(1)
		return s.current.Load().(string), nil
	}, MaxResponseBytes: 4096}
	d, err := Open(context.Background(), opts)
	if err != nil {
		t.Fatal(err)
	}
	c, err := core.New(core.Options{Authorize: func(context.Context, core.Principal, core.Action, core.Address) error { return nil }})
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
func readOpened(t *testing.T, c *core.Core, p string, o core.OpenOptions) ([]byte, core.Content) {
	t.Helper()
	opened, err := c.Open(context.Background(), "u", core.Path(p), o)
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(opened.Body)
	err = errors.Join(err, opened.Body.Close())
	if err != nil {
		t.Fatal(err)
	}
	return data, opened
}

type borrowedInput struct {
	read          func([]byte) (int, error)
	reads, closes atomic.Int32
}

func (b *borrowedInput) Read(p []byte) (int, error) { b.reads.Add(1); return b.read(p) }
func (b *borrowedInput) Close() error               { b.closes.Add(1); return nil }

func TestExactPathsPagingSnapshotsAndCopy(t *testing.T) {
	target := t.TempDir() // Remove it after setup's Core cleanup closes the Local mount.
	s, _, c, m, opts := setup(t)
	ctx := context.Background()
	if err := c.BindAlias(ctx, "/alias", m, "/"); err != nil {
		t.Fatal(err)
	}
	calls, tokens := s.count("pan.baidu.com"), s.tokenCalls.Load()
	invalid := opts
	invalid.RootPath = ""
	if source, err := Open(ctx, invalid); err == nil || source != nil || calls != s.count("pan.baidu.com") || tokens != s.tokenCalls.Load() {
		t.Fatal("missing explicit root performed I/O", source, err)
	}
	invalid = opts
	invalid.RootPath = "/Archive/A.bin"
	failed, err := Open(ctx, invalid)
	if core.CodeOf(err) != core.NotDirectory {
		t.Fatal("file was accepted as mount root", err)
	}
	if failed != nil {
		if err := failed.Close(); err != nil {
			t.Fatal(err)
		}
	}
	visible := "/" + nativeName
	if core.ValidatePath(syntheticRoot) != nil || core.ValidatePath(visible) != nil || core.ValidatePath(syntheticRoot+visible) == nil {
		t.Fatal("synthetic paths did not isolate the representation budgets")
	}
	deepOptions := opts
	deepOptions.RootPath = syntheticRoot
	deep, err := Open(ctx, deepOptions)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Install("/deep", deep); err != nil {
		t.Fatal(err)
	}
	deepData, _ := readOpened(t, c, "/deep"+visible, core.OpenOptions{})
	if string(deepData) != "abcdef" {
		t.Fatal("native prefix consumed the visible path budget", string(deepData))
	}
	before := s.count("filemetas ")
	f, err := c.Stat(ctx, "u", core.Path("/alias/Dir/"+nativeName))
	if err != nil || f.Name != nativeName || s.count("filemetas ") != before+1 || s.count("list ") != 0 {
		t.Fatal("Stat repeated native lookup or lost path/ID precision", f, err)
	}
	req := core.ListRequest{Query: core.Children(core.Path("/alias")), Limit: 2, CursorTTL: time.Minute}
	page, err := c.List(ctx, "u", req)
	if err != nil || len(page.Items) != 1 || page.Items[0].Name != "A.bin" || page.NextCursor == nil {
		t.Fatal("short native page ended listing", page, err)
	}
	req.Cursor = *page.NextCursor
	page, err = c.List(ctx, "u", req)
	if err != nil || len(page.Items) != 2 || page.Items[0].Name != "Dir" || page.Items[1].Name != "Empty" || page.NextCursor == nil {
		t.Fatal(page, err)
	}
	req.Cursor = *page.NextCursor
	before = s.count("list ")
	page, err = c.List(ctx, "u", req)
	if err != nil || len(page.Items) != 1 || page.Items[0].Name != nativeName || page.NextCursor == nil || before != s.count("list ") {
		t.Fatal("native page remainder", page, err)
	}
	req.Cursor = *page.NextCursor
	page, err = c.List(ctx, "u", req)
	if err != nil || len(page.Items) != 0 || page.NextCursor != nil || s.count(" start=2000") != 1 {
		t.Fatal("native window progress or empty terminal", page, err)
	}
	before = s.count("filemetas ")
	bodyCalls := s.count("download.fixture/content")
	data, opened := readOpened(t, c, "/alias/Dir/"+nativeName, core.OpenOptions{Selection: core.ReadSelection{When: core.IfPresent()}})
	if string(data) != "abcdef" || opened.Range != nil || opened.Length == nil || *opened.Length != 6 || s.count("filemetas ") != before+1 || s.count("download.fixture/content") != bodyCalls+1 {
		t.Fatal("Open added lookup or failed full projection", string(data), opened)
	}
	for _, check := range []struct {
		mode      string
		selection core.ReadSelection
		code      core.Code
		lookups   int
	}{
		{"", core.ReadSelection{When: core.IfAbsent()}, core.PreconditionFailed, 1},
		{"item-error", core.ReadSelection{When: core.IfAbsent()}, core.NotFound, 1},
		{"", core.ReadSelection{When: core.IfMatch(core.Validator{Tag: `"tag"`})}, core.UnsupportedOperation, 0},
		{"", core.ReadSelection{Version: "revision"}, core.UnsupportedOperation, 0},
	} {
		s.set(check.mode)
		before, bodyCalls = s.count("filemetas "), s.count("download.fixture/content")
		out, err := c.Open(ctx, "u", core.Path("/remote/"+nativeName), core.OpenOptions{Selection: check.selection})
		if core.CodeOf(err) != check.code || out.Body != nil || s.count("filemetas ") != before+check.lookups || s.count("download.fixture/content") != bodyCalls {
			t.Fatal("conditional Open changed lookup or body admission", check.mode, check.selection, err)
		}
		var condition *core.ConditionFailure
		if check.code == core.PreconditionFailed && (!errors.As(err, &condition) || !condition.Negative) {
			t.Fatal("absence condition lost its negative result", err)
		}
	}
	data, opened = readOpened(t, c, "/remote/"+nativeName, core.OpenOptions{Range: &core.Range{Start: 1, End: 3}})
	if string(data) != "bcd" || opened.Range == nil || opened.Range.Total != 6 {
		t.Fatal(string(data), opened.Range)
	}
	data, _ = readOpened(t, c, "/remote/Json", core.OpenOptions{})
	if string(data) != jsonFile {
		t.Fatal("legitimate JSON was sniffed", string(data))
	}
	s.set("rotate")
	tokens = s.tokenCalls.Load()
	data, _ = readOpened(t, c, "/remote/"+nativeName, core.OpenOptions{})
	if string(data) != "abcdef" || s.tokenCalls.Load() != tokens+1 {
		t.Fatal("token was not one operation snapshot", string(data))
	}
	s.set("")
	if _, err := c.Stat(ctx, "u", core.Path("/remote/"+nativeName)); err != nil || s.tokenCalls.Load() != tokens+2 {
		t.Fatal("next operation did not use replacement credential", err)
	}
	disk, err := local.New(target)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Install("/local", disk); err != nil {
		t.Fatal(err)
	}
	if change, err := c.Copy(ctx, "u", core.Path("/remote/"+nativeName), core.Path("/local/copied"), core.CopyOptions{}); err != nil || change.Effect != core.Applied {
		t.Fatal(change, err)
	}
	data, err = os.ReadFile(filepath.Join(target, "copied"))
	if err != nil || string(data) != "abcdef" {
		t.Fatal(string(data), err)
	}
	s.current.Store(firstToken)
	s.set("upload-rotate")
	posts, discoveries, metadata := s.count("upload upload.fixture"), s.count("locateupload "), s.count("filemetas ")
	tokens = s.tokenCalls.Load()
	if change, err := c.Copy(ctx, "u", core.Path("/local/copied"), core.Path("/alias/Uploaded"), core.CopyOptions{}); err != nil || change.Effect != core.Applied || change.Before != core.TargetMissing {
		t.Fatal("Local to native Copy", change, err)
	}
	if s.count("upload upload.fixture") != posts+1 || s.count("locateupload ") != discoveries+1 || s.count("filemetas ") != metadata || s.tokenCalls.Load() != tokens+1 {
		t.Fatal("exclusive Copy added metadata, task or credential requests")
	}
	s.set("")
	data, _ = readOpened(t, c, "/remote/Uploaded", core.OpenOptions{})
	if string(data) != "abcdef" {
		t.Fatal("uploaded representation did not round trip", string(data))
	}
	input := &borrowedInput{read: strings.NewReader("streamed").Read}
	if change, err := c.Write(ctx, "u", core.Path("/alias/up%+中"), input, nil); err != nil || change.Effect != core.Applied || input.closes.Load() != 0 || s.bytes("/Archive/up%+中") != "streamed" || s.count("locateupload ") != discoveries+2 {
		t.Fatal("unknown-length input was not borrowed once", change, err, input.closes.Load())
	}
	discoveries, tokens = s.count("locateupload "), s.tokenCalls.Load()
	for _, length := range []int64{0, singleUploadBytes + 1} {
		unused := &borrowedInput{read: strings.NewReader("unused").Read}
		if change, err := c.Write(ctx, "u", core.Path("/remote/no-request"), unused, &length); err == nil || change.Effect != core.Unchanged || unused.reads.Load() != 0 || unused.closes.Load() != 0 {
			t.Fatal("native limit rejection consumed input", change, err)
		}
	}
	if s.count("locateupload ") != discoveries || s.tokenCalls.Load() != tokens || s.unexpected.Load() != 0 {
		t.Fatal("rejection or upload used extra discovery, replay or recipient")
	}
	s.set("unconfirmed")
	if change, err := c.Copy(ctx, "u", core.Path("/remote/Json"), core.Path("/local/not-published"), core.CopyOptions{}); err == nil || change.Effect != core.Unchanged {
		t.Fatal("equal-length business JSON published", change, err)
	}
	if _, err := os.Stat(filepath.Join(target, "not-published")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("failed source changed target", err)
	}
}

func TestNativeReceiptsRangesAndHandoffs(t *testing.T) {
	s, d, c, _, opts := setup(t)
	ctx := context.Background()
	s.set("root-replaced")
	mountRoot := core.Path("/remote")
	before := s.count("download.fixture/content")
	replaced, err := c.Open(ctx, "u", mountRoot, core.OpenOptions{})
	if replaced.Body != nil {
		_ = replaced.Body.Close()
	}
	if core.CodeOf(err) != core.NotDirectory || replaced.Body != nil || s.count("download.fixture/content") != before {
		t.Fatal("file replacing the mount directory reached the body", err)
	}
	if _, err := c.Stat(ctx, "u", mountRoot); core.CodeOf(err) != core.NotDirectory {
		t.Fatal("file replacing the mount directory became the root entry", err)
	}
	for _, mode := range []string{"wrong-parent", "wrong-type", "overflow-id", "item-error", "truncated"} {
		s.set(mode)
		before := s.count("download.fixture/content")
		if _, err := c.Open(ctx, "u", core.Path("/remote/"+nativeName), core.OpenOptions{}); err == nil || errors.Is(err, io.EOF) || before != s.count("download.fixture/content") {
			t.Fatal("invalid receipt reached body", mode, err)
		}
	}
	for _, mode := range []string{"dot-list-parent", "outer-list-error", "nested-list-error"} {
		s.set(mode)
		stream, err := d.Enumerate(ctx, core.NativeChildren(core.NativePath("/")))
		if err != nil {
			t.Fatal(err)
		}
		want := core.UpstreamAccessDenied
		if mode == "dot-list-parent" {
			want = core.InvalidUpstreamResponse
		}
		if entries, err := stream.Next(ctx, 2); len(entries) != 0 || core.CodeOf(err) != want || errors.Is(err, io.EOF) {
			t.Fatal("invalid list receipt became entries or EOF", mode, entries, err)
		}
		if err := stream.Close(); err != nil {
			t.Fatal(err)
		}
	}
	for _, mode := range []string{"meta-size-mismatch", "length-mismatch", "connection-eof", "downgrade", "loop"} {
		s.set(mode)
		before := s.count("download.fixture/content")
		if _, err := c.Open(ctx, "u", core.Path("/remote/"+nativeName), core.OpenOptions{}); err == nil || errors.Is(err, io.EOF) || strings.Contains(err.Error(), firstToken) || strings.Contains(err.Error(), "sign=raw") {
			t.Fatal("body failure lost boundary or exposed credentials", mode, err)
		}
		if mode == "loop" && s.count("download.fixture/content")-before > 10 {
			t.Fatal("unbounded redirect loop")
		}
	}
	if s.unexpected.Load() != 0 {
		t.Fatal("downgrade/redirect reached unapproved transport")
	}
	s.set("redirect-echo")
	if _, err := c.Open(ctx, "u", core.Path("/remote/"+nativeName), core.OpenOptions{}); core.CodeOf(err) != core.InvalidUpstreamResponse ||
		strings.Contains(err.Error(), firstToken) || s.count("cdn.fixture/cross") != 0 {
		t.Fatal("redirect carrying the account token reached the delegated origin", err, s.count("cdn.fixture/cross"))
	}
	s.set("redirect")
	data, opened := readOpened(t, c, "/remote/"+nativeName, core.OpenOptions{})
	if string(data) != "abcdef" || opened.Range != nil || s.count("cdn.fixture/cross") != 1 || s.count("download.fixture/final") != 1 {
		t.Fatal("native Location/auth handoff", string(data), opened)
	}
	for _, mode := range []string{"", "empty-unknown"} {
		s.set(mode)
		data, opened = readOpened(t, c, "/remote/Empty", core.OpenOptions{})
		if len(data) != 0 || opened.Length == nil || *opened.Length != 0 {
			t.Fatal("empty body/EOF confirmation", opened)
		}
	}
	s.set("empty-error")
	if _, err := c.Open(ctx, "u", core.Path("/remote/Empty"), core.OpenOptions{}); err == nil {
		t.Fatal("empty metadata hid nonempty error body")
	}
	s.set("range-rejected")
	if _, err := c.Open(ctx, "u", core.Path("/remote/"+nativeName), core.OpenOptions{}); core.CodeOf(err) != core.InvalidUpstreamResponse {
		t.Fatal("native 416 for ordinary Open escaped as a caller range error", err)
	}
	s.set("")
	if _, err := c.Open(ctx, "u", core.Path("/remote/"+nativeName), core.OpenOptions{Range: &core.Range{Start: 9, End: 10}}); core.CodeOf(err) != core.RangeNotSatisfiable {
		t.Fatal(err)
	}
	limited := opts
	limited.MaxResponseBytes = 32
	source, err := Open(ctx, limited)
	if core.CodeOf(err) != core.ResponseBudgetExceeded {
		t.Fatal(source, err)
	}
	if source != nil {
		if err := source.Close(); err != nil {
			t.Fatal(err)
		}
	}
	for _, test := range []struct {
		mode   string
		effect core.Effect
	}{
		{"upload-locate-fail", core.Unchanged}, {"upload-conflict", core.Unchanged},
		{"upload-early", core.Unknown}, {"upload-wrong-path", core.Unknown},
		{"upload-wrong-size", core.Unknown}, {"upload-truncated", core.Unknown},
		{"upload-lost", core.Unknown}, {"upload-redirect", core.Unknown},
	} {
		s.set(test.mode)
		input := &borrowedInput{read: strings.NewReader("abc").Read}
		n := int64(3)
		posts := s.count("upload upload.fixture")
		change, err := c.Write(ctx, "u", core.Path("/remote/"+test.mode), input, &n)
		if err == nil || change.Effect != test.effect || input.closes.Load() != 0 || strings.Contains(err.Error(), firstToken) || strings.Contains(err.Error(), secondToken) {
			t.Fatal("native rejection or uncertain receipt", test.mode, change, err)
		}
		if test.mode == "upload-conflict" && (core.CodeOf(err) != core.AlreadyExists || input.reads.Load() != 0) || test.mode == "upload-early" && (input.reads.Load() != 0 || strings.Contains(err.Error(), "empty")) {
			t.Fatal("early response drained or misclassified unobserved input", test.mode, change, err)
		}
		wantPosts := 1
		if test.mode == "upload-locate-fail" {
			wantPosts = 0
		}
		if s.count("upload upload.fixture") != posts+wantPosts {
			t.Fatal("uncertain write was replayed", test.mode)
		}
	}
	tail := errors.New("source tail failed")
	s.set("upload-input-failure")
	for _, test := range []struct {
		data          string
		declared, ack int
		failure       error
	}{
		{"", 3, 0, tail}, {"abc", 5, 3, io.ErrUnexpectedEOF},
		{"abcde", 5, 5, tail}, {"abcde", 5, 3, tail},
	} {
		s.mu.Lock()
		s.ackSize = test.ack
		s.mu.Unlock()
		reader := strings.NewReader(test.data)
		input := &borrowedInput{read: func(p []byte) (int, error) {
			n, err := reader.Read(p)
			if err == io.EOF && test.failure == tail {
				err = tail
			}
			return n, err
		}}
		n := int64(test.declared)
		change, err := c.Write(ctx, "u", core.Path("/remote/partial"), input, &n)
		if change.Effect != core.Incomplete || change.Before != core.TargetMissing || !errors.Is(err, test.failure) || input.closes.Load() != 0 || len(s.bytes("/Archive/partial")) != test.ack {
			t.Fatal("failed input lost confirmed partial effect", test, change, err)
		}
	}
	s.mu.Lock()
	s.ackSize = -1
	s.mu.Unlock()
	s.set("")
	empty := &borrowedInput{read: strings.NewReader("").Read}
	if change, err := c.Write(ctx, "u", core.Path("/remote/unknown-empty"), empty, nil); err == nil || change.Effect == core.Applied || empty.closes.Load() != 0 || s.unexpected.Load() != 0 {
		t.Fatal("unknown empty input became successful creation", change, err)
	}
	checkParts(t, s, c, opts)
}

func waitFor[T any](t *testing.T, ctx context.Context, signal <-chan T, operation string) T {
	t.Helper()
	select {
	case result := <-signal:
		return result
	case <-ctx.Done():
		t.Fatalf("%s: %v", operation, ctx.Err())
		var zero T
		return zero
	}
}

func TestCancellationRetirementAndCloseOwnership(t *testing.T) {
	s, d, c, m, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	s.set("upload-inflight")
	s.uploadRead = make(chan struct{})
	entered, release := make(chan struct{}), make(chan struct{})
	input := &borrowedInput{read: func(p []byte) (int, error) {
		close(entered)
		s.entered <- struct{}{}
		<-release
		return copy(p, "abc"), io.EOF
	}}
	type result struct {
		change core.Change
		err    error
	}
	writeDone := make(chan result, 1)
	n := int64(3)
	go func() {
		change, err := c.Write(ctx, "u", core.Path("/remote/inflight"), input, &n)
		writeDone <- result{change, err}
	}()
	waitFor(t, ctx, entered, "upload did not borrow input")
	select {
	case got := <-writeDone:
		t.Fatal("early response outlived borrowed input", got)
	case <-time.After(10 * time.Millisecond):
	}
	close(release)
	got := waitFor(t, ctx, writeDone, "upload did not finish after input returned")
	if got.change.Effect != core.Unchanged || core.CodeOf(got.err) != core.AlreadyExists || input.closes.Load() != 0 {
		t.Fatal("early native refusal did not join input", got)
	}
	waitFor(t, ctx, s.uploadRead, "transport did not finish")
	s.set("upload-close")
	completed, stopUpload := context.WithCancel(ctx)
	s.afterClose = stopUpload
	change, uploadErr := c.Write(completed, "u", core.Path("/remote/committed"), strings.NewReader("abc"), &n)
	if change.Effect != core.Applied || !errors.Is(uploadErr, context.Canceled) || errors.Is(uploadErr, io.EOF) || s.broken == nil || s.broken.calls != 1 {
		t.Fatal("confirmed publication was lost to cancellation/cleanup", change, uploadErr)
	}
	committedClose := s.broken
	s.afterClose = nil
	s.set("")
	stream, err := d.Enumerate(ctx, core.NativeChildren(core.NativePath("/")))
	if err != nil {
		t.Fatal(err)
	}
	s.set("wait")
	call, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { _, err := stream.Next(call, 2); done <- err }()
	waitFor(t, ctx, s.entered, "listing did not start")
	stop()
	if err := waitFor(t, ctx, done, "cancellation did not join"); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
	s.set("redirect-close")
	before := s.count("download.fixture/content")
	if _, err := c.Open(ctx, "u", core.Path("/remote/"+nativeName), core.OpenOptions{}); err == nil || errors.Is(err, io.EOF) || s.count("download.fixture/content") != before+1 || s.count("download.fixture/same") != 0 || s.broken == nil || s.broken.calls != 1 {
		t.Fatal("failed intermediate close advanced handoff", err)
	}
	failed := s.broken
	s.set("unconfirmed-close")
	if _, err := c.Open(ctx, "u", core.Path("/remote/"+nativeName), core.OpenOptions{}); core.CodeOf(err) != core.InvalidUpstreamResponse ||
		!errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) || strings.Contains(err.Error(), "download.fixture") ||
		strings.Contains(err.Error(), s.current.Load().(string)) || s.broken == nil || s.broken == failed || s.broken.calls != 1 {
		t.Fatal("rejected final response did not preserve safe cleanup diagnostics", err)
	}
	rejected := s.broken
	s.set("body-wait")
	opened, err := c.Open(ctx, "u", core.Path("/remote/"+nativeName), core.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, ctx, s.entered, "body did not begin")
	go func() { _, err := io.ReadAll(opened.Body); done <- err }()
	if err := m.Retire(ctx); err != nil {
		t.Fatal(err)
	}
	if err := waitFor(t, ctx, done, "retirement did not join body"); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := opened.Body.Close(); err != nil {
		t.Fatal(err)
	}
	if failed.calls != 2 || rejected.calls != 2 || committedClose.calls != 2 {
		t.Fatal("failed close retried with multiple owners", failed.calls, rejected.calls, committedClose.calls)
	}
	s.set("")
}
