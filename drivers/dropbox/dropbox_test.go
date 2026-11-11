package dropbox

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
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/iotest"
	"time"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/local"
)

const nativeName = "p%+中"
const nativeCursor = "opaque %+中"

type wire func(*http.Request) (*http.Response, error)

func (f wire) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type fixture struct {
	mu           sync.Mutex
	mode         string
	calls        []string
	uploaded     map[string]string
	session      int64
	entered      chan struct{}
	unexpected   atomic.Int32
	broken       *badClose
	sessionClose *badClose
}

func (s *fixture) set(mode string) { s.mu.Lock(); s.mode = mode; s.mu.Unlock() }
func (s *fixture) state() string   { s.mu.Lock(); defer s.mu.Unlock(); return s.mode }
func (s *fixture) count(part string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, call := range s.calls {
		if strings.Contains(call, part) {
			n++
		}
	}
	return n
}
func metadata(id, name string, folder bool) map[string]any {
	display := "/aRcHiVe/" + name
	if id == "id:archive" {
		display = "/Archive"
	} else if id == "id:nested-blob" {
		display = "/aRcHiVe/Dir/" + name
	}
	f := map[string]any{"id": id, "name": name, "path_lower": strings.ToLower(display), "path_display": display}
	if folder {
		f[".tag"] = "folder"
		return f
	}
	f[".tag"] = "file"
	f["size"] = 6
	f["rev"] = "015a01044acb99900000001aa8954d0"
	f["client_modified"] = "2026-11-08T01:02:03Z"
	f["server_modified"] = "2026-11-08T01:02:03Z"
	f["is_downloadable"] = true
	if name == "Doc.paper" {
		f["size"] = 0
		f["is_downloadable"] = false
		f["export_info"] = map[string]any{"export_as": "html", "export_options": []string{"html", "markdown"}}
	}
	return f
}
func nativeError(w http.ResponseWriter, status int, tag string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	value := map[string]any{".tag": tag}
	if outer, inner, nested := strings.Cut(tag, "/"); nested {
		value = map[string]any{".tag": outer, outer: map[string]string{".tag": inner}}
	}
	json.NewEncoder(w).Encode(map[string]any{"error_summary": tag + "/..", "error": value})
}
func (s *fixture) serve(w http.ResponseWriter, r *http.Request) {
	path := ""
	var arg map[string]any
	uploading := r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/2/files/upload")
	if uploading && strings.HasPrefix(r.URL.Path, "/2/files/upload_session/") {
		header := r.Header.Get("Dropbox-API-Arg")
		if r.URL.RawQuery != "" || strings.ContainsFunc(header, func(r rune) bool { return r >= 0x7f }) || json.Unmarshal([]byte(header), &arg) != nil {
			nativeError(w, 400, "session arguments must use ASCII JSON in the header")
			return
		}
	} else if r.Method == "POST" && !uploading {
		if r.Body != nil {
			json.NewDecoder(r.Body).Decode(&arg)
		}
	} else if r.Method == "GET" || uploading {
		if r.Header.Get("Dropbox-API-Arg") != "" || json.Unmarshal([]byte(r.URL.Query().Get("arg")), &arg) != nil {
			nativeError(w, 400, "invalid arguments")
			return
		}
	} else {
		nativeError(w, 400, "unexpected method")
		return
	}
	if arg != nil {
		path, _ = arg["path"].(string)
	}
	scope := r.Header.Get("Dropbox-API-Path-Root")
	var root map[string]string
	if scope != "" && json.Unmarshal([]byte(scope), &root) != nil {
		nativeError(w, 400, "bad path root")
		return
	}
	selection := "default"
	if scope != "" {
		selection = root[".tag"] + ":" + root[root[".tag"]]
	}
	s.mu.Lock()
	s.calls = append(s.calls, r.Method+" "+r.URL.Path+" "+path+" "+selection)
	mode := s.mode
	uploaded, exists := s.uploaded[path]
	s.mu.Unlock()
	if r.Header.Get("Authorization") != "Bearer synthetic" || r.Header.Get("Dropbox-API-Select-User") != "dbmid:member" || r.Header.Get("Dropbox-API-Select-Admin") != "" || r.Header.Get("Cookie") != "" {
		nativeError(w, 401, "wrong account recipient")
		return
	}
	if mode == "throttled" {
		w.Header().Set("Retry-After", "17")
		w.WriteHeader(429)
		io.WriteString(w, "temporary native throttle")
		return
	}
	if mode == "root-drift" && strings.Contains(r.URL.Path, "/files/") {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(422)
		io.WriteString(w, `{"error_summary":"invalid_root/..","error":{".tag":"invalid_root","invalid_root":{".tag":"team","root_namespace_id":"new-root","home_namespace_id":"home-ns","home_path":"/Member"}}}`)
		return
	}
	switch r.URL.Path {
	case "/2/users/get_current_account":
		if scope != "" {
			nativeError(w, 400, "account call was already rooted")
			return
		}
		json.NewEncoder(w).Encode(map[string]any{"account_id": "dbid:account", "root_info": map[string]string{".tag": "team", "root_namespace_id": "team-ns", "home_namespace_id": "home-ns", "home_path": "/Member"}})
	case "/2/files/get_metadata":
		if mode == "wait" {
			s.wait(w, r)
			return
		}
		if mode == "truncated" {
			io.WriteString(w, `{".tag":"folder",`)
			return
		}
		var f map[string]any
		switch path {
		case "/Archive":
			f = metadata("id:archive", "Archive", true)
		case "id:archive/Dir", "id:archive/dir":
			f = metadata("id:dir", "Dir", true)
		case "id:archive/" + nativeName, "id:archive/P%+中":
			f = metadata("id:blob", nativeName, false)
		case "id:dir/" + nativeName:
			f = metadata("id:nested-blob", nativeName, false)
		case "id:archive/Doc.paper":
			f = metadata("id:doc", "Doc.paper", false)
		default:
			if !exists {
				nativeError(w, 409, "path/not_found")
				return
			}
			name := path[strings.LastIndexByte(path, '/')+1:]
			f = metadata("id:uploaded-"+name, name, false)
			f["size"] = len(uploaded)
		}
		json.NewEncoder(w).Encode(f)
	case "/2/files/list_folder", "/2/files/list_folder/continue":
		if mode == "wait" {
			s.wait(w, r)
			return
		}
		if strings.HasSuffix(r.URL.Path, "/continue") {
			if arg["cursor"] != nativeCursor {
				nativeError(w, 400, "fabricated cursor")
				return
			}
			if mode == "reset" {
				nativeError(w, 409, "reset")
				return
			}
			entries := []any{metadata("id:blob", nativeName, false), metadata("id:dir", "Dir", true)}
			if mode == "special" {
				entries = []any{metadata("id:doc", "Doc.paper", false)}
			}
			json.NewEncoder(w).Encode(map[string]any{"entries": entries, "cursor": nativeCursor, "has_more": false})
			return
		}
		if arg["recursive"] != false || arg["include_deleted"] != false || arg["include_mounted_folders"] != true || arg["include_non_downloadable_files"] != true {
			nativeError(w, 400, "wrong listing policy")
			return
		}
		if path == "" {
			if arg["limit"] != float64(1) {
				nativeError(w, 400, "unbounded root confirmation")
				return
			}
			json.NewEncoder(w).Encode(map[string]any{"entries": []any{}, "cursor": "confirmed", "has_more": false})
			return
		}
		if path != "id:archive" && path != "id:dir" {
			nativeError(w, 400, "wrong listing parent")
			return
		}
		json.NewEncoder(w).Encode(map[string]any{"entries": []any{}, "cursor": nativeCursor, "has_more": true})
	case "/2/files/download", "/2/files/export":
		if !exists && path != "id:archive/"+nativeName && path != "id:archive/P%+中" && path != "id:dir/"+nativeName && path != "id:archive/Doc.paper" {
			nativeError(w, 400, "wrong direct content path")
			return
		}
		if strings.HasSuffix(r.URL.Path, "/export") {
			if path != "id:archive/Doc.paper" {
				nativeError(w, 400, "wrong export request")
				return
			}
			if format, _ := arg["export_format"].(string); format != "" {
				nativeError(w, 409, "invalid_export_format")
				return
			}
			f := metadata("id:new-doc", "Doc.paper", false)
			delete(f, ".tag")
			header, _ := json.Marshal(map[string]any{"file_metadata": f, "export_metadata": map[string]any{"name": "Doc.html", "size": 8}})
			w.Header().Set("Dropbox-API-Result", string(header))
			serveBytes(w, r, "exported", mode == "export-range")
			return
		}
		if path == "id:archive/Doc.paper" {
			status, tag := 409, "unsupported_file"
			if mode == "forbidden" {
				status, tag = 403, "nativeDownloadForbidden"
			}
			if mode == "other-conflict" {
				tag = "path/not_found"
			}
			nativeError(w, status, tag)
			return
		}
		if mode == "redirect" {
			w.Header().Set("Location", "https://outside.invalid/private")
			w.WriteHeader(302)
			return
		}
		id, name, data := "id:blob", nativeName, "abcdef"
		if exists {
			name = path[strings.LastIndexByte(path, '/')+1:]
			id, data = "id:uploaded-"+name, uploaded
		} else if path == "id:dir/"+nativeName {
			id = "id:nested-blob"
		}
		f := metadata(id, name, false)
		f["size"] = len(data)
		delete(f, ".tag")
		if mode == "size-mismatch" {
			f["size"] = 7
		}
		header, _ := json.Marshal(f)
		w.Header().Set("Dropbox-API-Result", string(header))
		if mode == "body-wait" {
			w.Header().Set("Content-Length", "6")
			w.WriteHeader(200)
			w.(http.Flusher).Flush()
			s.wait(w, r)
			return
		}
		serveBytes(w, r, data, mode != "fallback")
	case "/2/files/upload":
		name, bound := strings.CutPrefix(path, "id:dir/")
		if r.Method != http.MethodPost || !bound || !core.ValidName(name) || selection != "namespace_id:shared-ns" ||
			arg["mode"] != "add" || arg["autorename"] != false || arg["strict_conflict"] != true ||
			r.Header.Get("Content-Type") != "application/octet-stream" || mode == "upload-unknown" && r.ContentLength != -1 {
			nativeError(w, 400, "wrong upload route, policy or body")
			return
		}
		data, err := io.ReadAll(r.Body)
		if err != nil {
			nativeError(w, 400, "incomplete upload body")
			return
		}
		if exists {
			w.WriteHeader(409)
			io.WriteString(w, `{"error":{".tag":"path","path":{"reason":{".tag":"conflict","conflict":{".tag":"file"}},"upload_session_id":"private-session"}}}`)
			return
		}
		s.mu.Lock()
		s.uploaded[path] = string(data)
		s.mu.Unlock()
		f := metadata("id:uploaded-"+name, name, false)
		delete(f, ".tag")
		f["size"] = len(data)
		if mode == "upload-wrong-receipt" {
			f["name"] = "wrong name"
		}
		json.NewEncoder(w).Encode(f)
	case "/2/files/upload_session/start", "/2/files/upload_session/append_v2", "/2/files/upload_session/finish":
		s.serveSession(w, r, arg, mode, selection)
	default:
		nativeError(w, 400, "unexpected endpoint")
	}
}

func (s *fixture) serveSession(w http.ResponseWriter, r *http.Request, arg map[string]any, mode, selection string) {
	const sessionID = "private-upload-session\x7f"
	if r.Method != http.MethodPost || selection != "namespace_id:shared-ns" || r.Header.Get("Content-Type") != "application/octet-stream" {
		nativeError(w, 400, "wrong upload session recipient")
		return
	}
	switch {
	case strings.HasSuffix(r.URL.Path, "/start"):
		n, err := io.Copy(io.Discard, r.Body)
		if err != nil || len(arg) != 0 || n != maxSingleUpload || n != r.ContentLength {
			nativeError(w, 400, "wrong initial segment")
			return
		}
		s.mu.Lock()
		s.session = n
		s.mu.Unlock()
		json.NewEncoder(w).Encode(map[string]string{"session_id": sessionID})
	default:
		s.mu.Lock()
		offset := s.session
		s.mu.Unlock()
		cursor, _ := arg["cursor"].(map[string]any)
		if cursor["session_id"] != sessionID || cursor["offset"] != float64(offset) {
			nativeError(w, 400, "wrong staged cursor")
			return
		}
		if strings.HasSuffix(r.URL.Path, "/append_v2") {
			data, err := io.ReadAll(io.LimitReader(r.Body, 5))
			if err != nil || offset != maxSingleUpload || string(data) != "tail" || r.ContentLength != int64(len(data)) {
				nativeError(w, 400, "wrong trailing segment")
				return
			}
			s.mu.Lock()
			s.session += int64(len(data))
			s.mu.Unlock()
			io.WriteString(w, "null")
			return
		}
		commit, _ := arg["commit"].(map[string]any)
		path, _ := commit["path"].(string)
		name, bound := strings.CutPrefix(path, "id:dir/")
		if !bound || !core.ValidName(name) || commit["mode"] != "add" || commit["autorename"] != false ||
			commit["strict_conflict"] != true || r.ContentLength != 0 || offset != maxSingleUpload+4 {
			nativeError(w, 400, "wrong final commit")
			return
		}
		if mode == "session-conflict" {
			w.WriteHeader(409)
			io.WriteString(w, `{"error":{".tag":"path","path":{".tag":"conflict","conflict":{".tag":"folder"}}}}`)
			return
		}
		f := metadata("id:session-result", name, false)
		delete(f, ".tag")
		f["size"] = offset
		json.NewEncoder(w).Encode(f)
	}
}

type zeros struct{}

func (zeros) Read(p []byte) (int, error) {
	clear(p)
	return len(p), nil
}
func serveBytes(w http.ResponseWriter, r *http.Request, data string, ranged bool) {
	status, total := 200, len(data)
	if raw := r.Header.Get("Range"); raw != "" && ranged {
		var start, end int
		if _, err := fmt.Sscanf(raw, "bytes=%d-%d", &start, &end); err != nil {
			nativeError(w, 400, "bad range")
			return
		}
		if start >= total {
			w.Header().Del("Dropbox-API-Result")
			w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", total))
			w.WriteHeader(416)
			return
		}
		end = min(end, total-1)
		data, status = data[start:end+1], 206
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, total))
	}
	w.Header().Set("Content-Length", strconv.Itoa(len(data)))
	w.WriteHeader(status)
	io.WriteString(w, data)
}
func (s *fixture) wait(_ http.ResponseWriter, r *http.Request) {
	select {
	case s.entered <- struct{}{}:
	default:
	}
	<-r.Context().Done()
}

type badClose struct {
	io.ReadCloser
	calls int
	err   error
}

func (b *badClose) Close() error {
	b.calls++
	if b.calls == 1 {
		if b.err != nil {
			return b.err
		}
		return io.EOF
	}
	return b.ReadCloser.Close()
}
func setup(t *testing.T) (*fixture, *Driver, *core.Core, *core.Mount, Options) {
	t.Helper()
	s := &fixture{entered: make(chan struct{}, 2), uploaded: make(map[string]string)}
	server := httptest.NewServer(http.HandlerFunc(s.serve))
	t.Cleanup(server.Close)
	base, _ := url.Parse(server.URL)
	jar, _ := cookiejar.New(nil)
	for _, host := range []string{"api.dropboxapi.com", "content.dropboxapi.com"} {
		u, _ := url.Parse("https://" + host)
		jar.SetCookies(u, []*http.Cookie{{Name: "session", Value: "must-not-be-sent"}})
	}
	client := &http.Client{Jar: jar, Transport: wire(func(r *http.Request) (*http.Response, error) {
		if r.URL.Scheme != "https" || (r.URL.Host != "api.dropboxapi.com" && r.URL.Host != "content.dropboxapi.com") {
			s.unexpected.Add(1)
			return nil, errors.New("authenticated recipient changed")
		}
		uploading := strings.HasPrefix(r.URL.Path, "/2/files/upload")
		if (uploading || strings.Contains(r.URL.Path, "/files/download") || strings.Contains(r.URL.Path, "/files/export")) != (r.URL.Host == "content.dropboxapi.com") {
			return nil, errors.New("wrong native API host")
		}
		if uploading && r.GetBody != nil {
			return nil, errors.New("upload request enabled body replay")
		}
		call := r.Clone(r.Context())
		u := *r.URL
		u.Scheme, u.Host = base.Scheme, base.Host
		call.URL = &u
		call.Host = ""
		call.Header.Set("Authorization", "Bearer synthetic")
		call.Header.Set("Dropbox-API-Select-User", "dbmid:member")
		if s.state() == "connection-eof" {
			return nil, io.EOF
		}
		if uploading && s.state() == "upload-partial" {
			data, readErr := io.ReadAll(call.Body)
			_ = call.Body.Close()
			if readErr == nil {
				return nil, errors.New("expected the source to fail")
			}
			call.Body = io.NopCloser(strings.NewReader(string(data)))
			call.ContentLength = int64(len(data))
		}
		response, err := http.DefaultTransport.RoundTrip(call)
		mode := s.state()
		if err == nil && (uploading && mode == "upload-lost" ||
			mode == "session-lost" && strings.HasSuffix(r.URL.Path, "/upload_session/finish")) {
			_ = response.Body.Close()
			if mode == "session-lost" {
				return nil, fmt.Errorf("private-upload-session: %w", io.ErrUnexpectedEOF)
			}
			return nil, io.ErrUnexpectedEOF
		}
		if err == nil && mode == "session-ok" && strings.HasSuffix(r.URL.Path, "/upload_session/finish") {
			s.sessionClose = &badClose{ReadCloser: response.Body, err: fmt.Errorf("private-upload-session: %w", io.ErrUnexpectedEOF)}
			response.Body = s.sessionClose
		}
		if err == nil && (mode == "close-failure" && strings.HasSuffix(r.URL.Path, "/download") || mode == "upload-close" && uploading) {
			s.broken = &badClose{ReadCloser: response.Body}
			response.Body = s.broken
		}
		return response, err
	})}
	opts := Options{Client: client, RootPath: "/Archive", MaxResponseBytes: 4096}
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
func readOpened(t *testing.T, c *core.Core, path string, o core.OpenOptions) ([]byte, core.Content) {
	t.Helper()
	opened, err := c.Open(context.Background(), "u", core.Path(path), o)
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

func TestRootsExactPathsPagingAndCopy(t *testing.T) {
	target := t.TempDir() // Core closes the Local mount before this directory is removed.
	s, _, c, m, opts := setup(t)
	ctx := context.Background()
	if err := c.BindAlias(ctx, "/alias", m, "/"); err != nil {
		t.Fatal(err)
	}
	for _, root := range []Root{{}, AccountRoot(), NamespaceRoot("shared-ns")} {
		native := opts
		native.RootPath = ""
		native.Root = root
		d, err := Open(ctx, native)
		if err != nil {
			t.Fatal(err)
		}
		if root == AccountRoot() {
			if _, err := c.Install("/team", d); err != nil {
				t.Fatal(err)
			}
		} else if err := d.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if s.count("/get_current_account") != 1 || s.count(" default") == 0 || s.count(" root:team-ns") != 1 || s.count(" namespace_id:shared-ns") != 1 {
		t.Fatal("native root selection or redundant account discovery")
	}
	before := s.count("/get_metadata")
	if f, err := c.Stat(ctx, "u", core.Path("/remote/Dir/"+nativeName)); err != nil || f.Name != nativeName || s.count("/get_metadata") != before+2 {
		t.Fatal("exact component lookup or display ancestor casing", f, err)
	}
	before = s.count("/get_metadata")
	if _, err := c.Stat(ctx, "u", core.Path("/remote/dir/"+nativeName)); core.CodeOf(err) != core.NotFound || s.count("/get_metadata") != before+1 {
		t.Fatal("case mismatch reached child", err)
	}
	req := core.ListRequest{Query: core.Children(core.Path("/alias")), Limit: 1, CursorTTL: time.Minute}
	page, err := c.List(ctx, "u", req)
	if err != nil || len(page.Items) != 0 || page.NextCursor == nil {
		t.Fatal(page, err)
	}
	req.Cursor = *page.NextCursor
	page, err = c.List(ctx, "u", req)
	if err != nil || len(page.Items) != 1 || page.Items[0].Name != nativeName || page.NextCursor == nil {
		t.Fatal(page, err)
	}
	req.Cursor = *page.NextCursor
	before = s.count("/files/list_folder")
	page, err = c.List(ctx, "u", req)
	if err != nil || len(page.Items) != 1 || page.Items[0].Name != "Dir" || page.NextCursor != nil || before != s.count("/files/list_folder") {
		t.Fatal("public remainder did not finish without another native page", page, err)
	}
	before = s.count("/get_metadata")
	calls := s.count("/files/download")
	wanted := core.OpenOptions{Range: &core.Range{Start: 1, End: 3}}
	data, opened := readOpened(t, c, "/alias/Dir/"+nativeName, wanted)
	if string(data) != "bcd" || opened.Range == nil || s.count("/get_metadata") != before+1 || s.count("/files/download") != calls+1 {
		t.Fatal("Open did leaf preflight or lost range", string(data), opened.Range)
	}
	s.set("fallback")
	data, opened = readOpened(t, c, "/remote/"+nativeName, wanted)
	if string(data) != "abcdef" || opened.Range != nil {
		t.Fatal(string(data), opened.Range)
	}
	s.set("")
	if _, err := c.Open(ctx, "u", core.Path("/remote/"+nativeName), core.OpenOptions{Range: &core.Range{Start: 8, End: 9}}); core.CodeOf(err) != core.RangeNotSatisfiable {
		t.Fatal(err)
	}
	_, err = c.Open(ctx, "u", core.Path("/remote/P%+中"), core.OpenOptions{Range: &core.Range{Start: 8, End: 9}})
	var hidden *core.Error
	if core.CodeOf(err) != core.NotFound || !errors.As(err, &hidden) || hidden.Total != nil {
		t.Fatal("case mismatch exposed the native range total", err)
	}
	disk, err := local.New(target)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Install("/local", disk); err != nil {
		t.Fatal(err)
	}
	exportOptions := opts
	exportOptions.Export = &ExportOptions{}
	exportDriver, err := Open(ctx, exportOptions)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Install("/export-copy", exportDriver); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Copy(ctx, "u", core.Path("/export-copy/Doc.paper"), core.Path("/local/Doc.paper"), core.CopyOptions{}); err != nil {
		t.Fatal("unknown-size export Copy", err)
	}
	checkUpload(t, s, c, opts)
	s.set("root-drift")
	before = s.count("/get_current_account")
	if _, err := c.Stat(ctx, "u", core.Path("/team/Archive")); err == nil || !strings.Contains(err.Error(), "invalid_root") || before != s.count("/get_current_account") {
		t.Fatal("root silently rebound", err)
	}
	if s.unexpected.Load() != 0 {
		t.Fatal("authentication reached other origin")
	}
}

func checkUpload(t *testing.T, s *fixture, c *core.Core, opts Options) {
	t.Helper()
	ctx := context.Background()
	const exported = "exported"
	length := int64(len(exported))
	input := &badClose{ReadCloser: io.NopCloser(strings.NewReader(exported))}
	if change, err := c.Write(ctx, "u", core.Path("/remote/Dir/new"), input, &length); change.Effect != core.Unchanged ||
		core.CodeOf(err) != core.UnsupportedOperation || s.count("/files/upload") != 0 {
		t.Fatal("read-only mount accepted upload", change, err)
	}
	opts.AllowCreate, opts.Root = true, NamespaceRoot("shared-ns")
	d, err := Open(ctx, opts)
	if err != nil {
		t.Fatal(err)
	}
	m, err := c.Install("/write", d)
	if err != nil {
		t.Fatal(err)
	}
	before := s.count("/get_metadata")
	const created = "/write/Dir/new%+中"
	if change, err := c.Copy(ctx, "u", core.Path("/local/Doc.paper"), core.Path(created), core.CopyOptions{}); err != nil ||
		change.Effect != core.Applied || change.Before != core.TargetMissing ||
		s.count("/get_metadata") != before+1 || s.count("/files/upload") != 1 {
		t.Fatal("Copy to Dropbox failed or performed a leaf precheck", change, err)
	}
	if got, _ := readOpened(t, c, created, core.OpenOptions{}); string(got) != exported {
		t.Fatal("exported bytes did not survive both copies", string(got))
	}
	if change, err := c.Write(ctx, "u", core.Path(created), input, &length); change.Effect != core.Unchanged ||
		change.Before != core.TargetUnknown || core.CodeOf(err) != core.AlreadyExists ||
		strings.Contains(err.Error(), "private-session") || input.calls != 0 {
		t.Fatal("identical content bypassed exclusive creation or exposed a session", change, err)
	}
	before = s.count("/2/")
	tooLarge := maxSessionUpload + 1
	if change, err := c.Write(ctx, "u", core.Path("/write/Dir/over"), strings.NewReader("x"), &tooLarge); change.Effect != core.Unchanged ||
		core.CodeOf(err) != core.CapacityExceeded || s.count("/2/") != before {
		t.Fatal("declared oversize upload reached the service", change, err)
	}
	empty, n := int64(0), int64(6)
	for _, tc := range []struct {
		mode, name, content string
		length              *int64
		effect              core.Effect
		code                core.Code
	}{
		{"upload-unknown", "stream", "x", nil, core.Applied, ""},
		{"", "empty", "", &empty, core.Applied, ""},
		{"upload-wrong-receipt", "bad", "abcdef", &n, core.Unknown, core.InvalidUpstreamResponse},
		{"upload-lost", "lost", "abcdef", &n, core.Unknown, core.UpstreamFailure},
		{"upload-close", "closed", "abcdef", &n, core.Applied, core.InternalError},
	} {
		s.set(tc.mode)
		before := s.count("/files/upload")
		change, err := c.Write(ctx, "u", core.Path("/write/Dir/"+tc.name), strings.NewReader(tc.content), tc.length)
		var code core.Code
		if err != nil {
			code = core.CodeOf(err)
		}
		if change.Effect != tc.effect || code != tc.code || s.count("/files/upload") != before+1 {
			t.Fatal(tc.mode, change, err)
		}
	}
	s.set("upload-partial")
	sourceErr := errors.New("input failed after a prefix")
	partial := io.MultiReader(strings.NewReader("abc"), iotest.ErrReader(sourceErr))
	if change, err := c.Write(ctx, "u", core.Path("/write/Dir/partial"), partial, &n); change.Effect != core.Incomplete || !errors.Is(err, sourceErr) {
		t.Fatal("confirmed partial receipt lost the input error or effect", change, err)
	}
	size := maxSingleUpload + 4
	for _, tc := range []struct {
		mode   string
		tail   error
		effect core.Effect
		code   core.Code
	}{
		{"session-ok", nil, core.Applied, core.InternalError},
		{"session-conflict", nil, core.Unchanged, core.AlreadyExists},
		{"session-lost", nil, core.Unknown, core.UpstreamFailure},
		{"session-source-error", sourceErr, core.Unchanged, ""},
	} {
		s.set(tc.mode)
		starts, appends, finishes := s.count("/upload_session/start"), s.count("/upload_session/append_v2"), s.count("/upload_session/finish")
		var tail io.Reader = strings.NewReader("tail")
		wantFinish := 1
		if tc.tail != nil {
			tail = io.MultiReader(tail, iotest.ErrReader(tc.tail))
			wantFinish = 0
		}
		stream := io.MultiReader(io.LimitReader(zeros{}, maxSingleUpload), tail)
		change, err := c.Write(ctx, "u", core.Path("/write/Dir/"+tc.mode+"%+中🚀"), stream, &size)
		var code core.Code
		if err != nil {
			code = core.CodeOf(err)
			if strings.Contains(err.Error(), "private-upload-session") {
				t.Fatal("upload error exposed the session ID", err)
			}
		}
		if change.Effect != tc.effect || tc.tail == nil && code != tc.code || tc.tail != nil && !errors.Is(err, tc.tail) ||
			s.count("/upload_session/start") != starts+1 || s.count("/upload_session/append_v2") > appends+1 ||
			tc.tail == nil && s.count("/upload_session/append_v2") != appends+1 || s.count("/upload_session/finish") != finishes+wantFinish {
			t.Fatal(tc.mode, change, err)
		}
	}
	s.set("")
	if s.broken == nil || s.broken.calls != 1 {
		t.Fatal("upload did not retain its failed response close")
	}
	if s.sessionClose == nil || s.sessionClose.calls != 1 {
		t.Fatal("session did not retain its failed response close")
	}
	if err := m.Retire(ctx); err != nil || s.broken.calls != 2 || s.sessionClose.calls != 2 {
		t.Fatal("upload response cleanup was not retried once", err)
	}
}

func TestNativeBodyReceiptsAndExplicitExports(t *testing.T) {
	s, d, c, _, opts := setup(t)
	ctx := context.Background()
	for _, mode := range []string{"size-mismatch", "redirect", "connection-eof", "throttled"} {
		s.set(mode)
		if _, err := c.Open(ctx, "u", core.Path("/remote/"+nativeName), core.OpenOptions{}); err == nil || errors.Is(err, io.EOF) {
			t.Fatal(mode, err)
		} else if mode == "throttled" && (core.CodeOf(err) != core.UpstreamThrottled || !strings.Contains(err.Error(), "temporary native throttle") || !strings.Contains(err.Error(), "17")) {
			t.Fatal("plain throttle lost native reason or delay", err)
		}
	}
	if s.unexpected.Load() != 0 {
		t.Fatal("redirect carried account authentication")
	}
	s.set("")
	if _, err := c.Open(ctx, "u", core.Path("/remote/Doc.paper"), core.OpenOptions{}); core.CodeOf(err) != core.UnsupportedOperation || s.count("/files/export") != 0 {
		t.Fatal("unselected export", err)
	}
	native := opts
	native.Export = &ExportOptions{}
	exporter, err := Open(ctx, native)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Install("/export", exporter); err != nil {
		t.Fatal(err)
	}
	native.Export.Format = "markdown"
	if f, err := c.Stat(ctx, "u", core.Path("/export/Doc.paper")); err != nil || f.Size != nil {
		t.Fatal("non-downloadable metadata became byte length", f, err)
	}
	before := s.count("/get_metadata")
	data, opened := readOpened(t, c, "/export/Doc.paper", core.OpenOptions{Range: &core.Range{Start: 1, End: 3}})
	if string(data) != "exported" || opened.Range != nil || opened.Length == nil || *opened.Length != 8 || opened.Entry.Name != "Doc.paper" || before != s.count("/get_metadata") {
		t.Fatal("export receipt/default/size/name or leaf preflight", string(data), opened)
	}
	s.set("export-range")
	data, opened = readOpened(t, c, "/export/Doc.paper", core.OpenOptions{Range: &core.Range{Start: 1, End: 3}})
	if string(data) != "xpo" || opened.Range == nil || opened.Range.Total != 8 || opened.Length == nil || *opened.Length != 3 || opened.Entry.Name != "Doc.paper" {
		t.Fatal("export actual range or total", string(data), opened)
	}
	for _, mode := range []string{"special", "reset"} {
		s.set(mode)
		stream, err := d.Enumerate(ctx, core.NativeChildren(core.NativePath("/")))
		if err != nil {
			t.Fatal(err)
		}
		if _, err = stream.Next(ctx, 1); err != nil {
			t.Fatal(err)
		}
		before = s.count("/files/list_folder ")
		items, err := stream.Next(ctx, 1)
		if mode == "reset" {
			if err == nil || !strings.Contains(err.Error(), "reset") || len(items) != 0 || before != s.count("/files/list_folder ") {
				t.Fatal("native cursor reset restarted enumeration", items, err)
			}
		} else if !errors.Is(err, io.EOF) || len(items) != 1 || items[0].Size != nil {
			t.Fatal("listing hid exportable entry or invented size", items, err)
		}
		if err := stream.Close(); err != nil {
			t.Fatal(err)
		}
	}
	for _, mode := range []string{"forbidden", "other-conflict"} {
		s.set(mode)
		before = s.count("/files/export")
		if _, err := c.Open(ctx, "u", core.Path("/export/Doc.paper"), core.OpenOptions{}); err == nil || before != s.count("/files/export") {
			t.Fatal("non-matching native error enabled export", mode, err)
		}
	}
	s.set("")
	custom, err := Open(ctx, native)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Install("/custom", custom); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Open(ctx, "u", core.Path("/custom/Doc.paper"), core.OpenOptions{}); err == nil || !strings.Contains(err.Error(), "invalid_export_format") {
		t.Fatal("explicit native format validation lost", err)
	}
	limited := opts
	limited.MaxResponseBytes = 32
	failedSource, err := Open(ctx, limited)
	if core.CodeOf(err) != core.ResponseBudgetExceeded {
		t.Fatal(failedSource, err)
	}
	if failedSource != nil {
		if err := failedSource.Close(); err != nil {
			t.Fatal(err)
		}
	}
	s.set("truncated")
	if _, err := c.Stat(ctx, "u", core.Path("/remote/Dir")); err == nil || errors.Is(err, io.EOF) {
		t.Fatal("partial metadata escaped", err)
	}
}

func TestCancellationRetirementAndFailedClose(t *testing.T) {
	s, d, c, _, opts := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	stream, err := d.Enumerate(ctx, core.NativeChildren(core.NativePath("/")))
	if err != nil {
		t.Fatal(err)
	}
	s.set("wait")
	call, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { _, err := stream.Next(call, 1); done <- err }()
	select {
	case <-s.entered:
	case <-ctx.Done():
		t.Fatal("native listing never began")
	}
	stop()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("cancellation did not join")
	}
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
	s.set("")
	opts.Export = &ExportOptions{}
	exporter, err := Open(ctx, opts)
	if err != nil {
		t.Fatal(err)
	}
	mount, err := c.Install("/export", exporter)
	if err != nil {
		t.Fatal(err)
	}
	s.set("close-failure")
	before := s.count("/files/export")
	if _, err := c.Open(ctx, "u", core.Path("/export/Doc.paper"), core.OpenOptions{}); err == nil || errors.Is(err, io.EOF) || before != s.count("/files/export") || s.broken == nil || s.broken.calls != 1 {
		t.Fatal("failed download close allowed export or lost cleanup", err)
	}
	failed := s.broken
	s.set("body-wait")
	opened, err := c.Open(ctx, "u", core.Path("/export/"+nativeName), core.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-s.entered:
	case <-ctx.Done():
		t.Fatal("native body never began")
	}
	go func() { _, err := io.ReadAll(opened.Body); done <- err }()
	if err := mount.Retire(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("retirement did not join body")
	}
	if err := opened.Body.Close(); err != nil {
		t.Fatal(err)
	}
	if failed.calls != 2 {
		t.Fatal("failed close was not retried exactly once", failed.calls)
	}
	s.set("")
}
