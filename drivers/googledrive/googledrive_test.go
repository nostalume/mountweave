package googledrive

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/local"
)

const folderMIME = "application/vnd.google-apps.folder"
const documentMIME = "application/vnd.google-apps.document"

// The synthetic MIME tests an about-confirmed mapping, not a product claim.
const alternateMIME = "application/vnd.fixture.document"
const firstToken = "opaque %+中"
const quotedName = "q' or trashed = true"

type wire func(*http.Request) (*http.Response, error)

func (f wire) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type fixture struct {
	mu       sync.Mutex
	mode     string
	calls    []string
	bodyURL  string
	entered  chan struct{}
	external atomic.Int32
	broken   *badClose
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
func (s *fixture) record(r *http.Request) {
	s.mu.Lock()
	s.calls = append(s.calls, r.Method+" "+r.URL.RequestURI())
	s.mu.Unlock()
}
func nativeFile(id, name, mime, parent string) map[string]any {
	f := map[string]any{"id": id, "name": name, "mimeType": mime, "trashed": false, "capabilities": map[string]bool{"canDownload": true}, "modifiedTime": "2026-11-08T01:02:03Z"}
	if parent == "shared-root" {
		f["driveId"] = "team-drive"
	}
	if parent != "" {
		f["parents"] = []string{parent}
	}
	if mime != "application/vnd.google-apps.folder" {
		f["size"] = "6"
	}
	return f
}
func rootFile(id string) map[string]any {
	name, parent, key := "Archive", "root-id", "archive-key"
	switch id {
	case "root", "root-id":
		id, name, parent, key = "root-id", "My Drive", "", ""
	case "shared-root":
		name, parent, key = "Shared", "", "shared-key"
	}
	f := nativeFile(id, name, folderMIME, parent)
	if key != "" {
		f["resourceKey"] = key
	}
	if id == "shared-root" {
		f["driveId"] = "team-drive"
	}
	return f
}

var literal = regexp.MustCompile(`'((?:\\.|[^'\\])*)'`)

func queryParts(q string) (string, string, bool) {
	parts := literal.FindAllStringSubmatch(q, -1)
	if len(parts) < 1 || len(parts) > 2 || !strings.Contains(q, " in parents") || (!strings.Contains(q, "trashed = false") && !strings.Contains(q, "trashed=false")) {
		return "", "", false
	}
	decode := func(v string) string { return strings.NewReplacer(`\\`, `\`, `\'`, `'`).Replace(v) }
	name := ""
	if len(parts) == 2 {
		name = decode(parts[1][1])
	}
	return decode(parts[0][1]), name, true
}
func (s *fixture) control(w http.ResponseWriter, r *http.Request) {
	s.record(r)
	mode := s.state()
	q := r.URL.Query()
	fail := func(status int, reason string) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		json.NewEncoder(w).Encode(map[string]any{"error": map[string]any{"code": status, "message": "native " + reason, "errors": []any{map[string]string{"reason": reason}}}})
	}
	if r.Method != "GET" || r.Header.Get("Authorization") != "Bearer synthetic" {
		fail(401, "missing authentication")
		return
	}
	if mode == "native-error" {
		fail(403, "downloadQuotaExceeded")
		return
	}
	if mode == "error-close" {
		fail(403, "nativeAccessRefused")
		return
	}
	if r.URL.Path == "/drive/v3/about" {
		if q.Get("fields") != "exportFormats" || r.Header.Get("X-Goog-Drive-Resource-Keys") != "" {
			fail(400, "format discovery scope")
			return
		}
		json.NewEncoder(w).Encode(map[string]any{"exportFormats": map[string][]string{documentMIME: {"text/plain", "application/pdf"}, alternateMIME: {"text/plain"}}})
		return
	}
	if r.URL.Path == "/drive/v3/files" {
		parent, name, ok := queryParts(q.Get("q"))
		if !ok {
			fail(400, "invalid scoped query")
			return
		}
		expectedKey := ""
		if parent == "archive" {
			expectedKey = "archive/archive-key"
		}
		if parent == "shared-root" {
			expectedKey = "shared-root/shared-key"
		}
		if r.Header.Get("X-Goog-Drive-Resource-Keys") != expectedKey || q.Get("supportsAllDrives") != "true" {
			fail(400, "wrong parent resource key")
			return
		}
		if parent == "shared-root" {
			if q.Get("corpora") != "drive" || q.Get("driveId") != "team-drive" || q.Get("includeItemsFromAllDrives") != "true" {
				fail(400, "missing shared-drive scope")
				return
			}
		} else if q.Get("corpora") != "user" {
			fail(400, "wrong personal corpus")
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
			fmt.Fprint(w, `{"files":[`)
			return
		}
		if mode == "incomplete" {
			json.NewEncoder(w).Encode(map[string]any{"incompleteSearch": true})
			return
		}
		token := q.Get("pageToken")
		if token == "" {
			json.NewEncoder(w).Encode(map[string]any{"nextPageToken": firstToken})
			return
		}
		if token != firstToken && token != "last" {
			fail(400, "fabricatedPageToken")
			return
		}
		if mode == "expired-token" {
			fail(400, "invalidPageToken")
			return
		}
		f := nativeFile("blob-id", "p%+中", "application/octet-stream", parent)
		f["resourceKey"] = "blob-key"
		children := []any{f, nativeFile("sub-id", "sub", folderMIME, parent)}
		if name != "" {
			children = []any{}
			switch name {
			case "Archive":
				if parent == "root-id" {
					children = append(children, rootFile("archive"))
				}
			case "p%+中":
				children = append(children, f)
			case quotedName:
				children = append(children, nativeFile("quote-id", name, "text/plain", parent))
			case "Document", "Alternate":
				id, mime, key := "doc-id", documentMIME, "doc-key"
				if name == "Alternate" {
					id, mime, key = "alternate-id", alternateMIME, "alternate-key"
				}
				doc := nativeFile(id, name, mime, parent)
				doc["size"] = "999999"
				doc["resourceKey"] = key
				children = append(children, doc)
			case "shortcut":
				link := nativeFile("shortcut-id", name, "application/vnd.google-apps.shortcut", parent)
				link["shortcutDetails"] = map[string]string{"targetId": "outside", "targetMimeType": folderMIME}
				children = append(children, link)
			case "thirdparty":
				children = append(children, nativeFile("thirdparty-id", name, "application/vnd.google-apps.drive-sdk", parent))
			case "video":
				children = append(children, nativeFile("video-id", name, "application/vnd.google-apps.vid", parent))
			}
		}
		if mode == "special" && name == "" {
			children = []any{}
			for _, v := range []struct{ id, name, mime string }{{"doc-id", "Document", documentMIME}, {"shortcut-id", "shortcut", "application/vnd.google-apps.shortcut"}, {"thirdparty-id", "thirdparty", "application/vnd.google-apps.drive-sdk"}, {"video-id", "video", "application/vnd.google-apps.vid"}} {
				children = append(children, nativeFile(v.id, v.name, v.mime, parent))
			}
		}
		if mode == "wrong-parent" {
			f["parents"] = []string{"outside"}
		}
		if mode == "wrong-name" {
			f["name"] = "not-the-requested-name"
		}
		if mode == "negative" {
			f["size"] = "-1"
		}
		if mode == "not-downloadable" {
			f["capabilities"] = map[string]bool{"canDownload": false}
		}
		result := map[string]any{"files": children}
		if mode == "duplicate" || mode == "repeated" {
			if token == firstToken {
				result["nextPageToken"] = "last"
			} else {
				copy := nativeFile("another-id", "p%+中", "application/octet-stream", parent)
				if mode == "repeated" {
					copy["id"] = "blob-id"
				}
				result["files"] = []any{copy}
			}
		}
		if mode == "stalled" {
			result["nextPageToken"] = token
		}
		json.NewEncoder(w).Encode(result)
		return
	}
	id := strings.TrimPrefix(r.URL.Path, "/drive/v3/files/")
	if q.Get("alt") == "media" || strings.HasSuffix(id, "/export") {
		expectedKey := "blob-id/blob-key"
		if id == "doc-id/export" || id == "alternate-id/export" {
			expectedKey = "doc-id/doc-key"
			if id == "alternate-id/export" {
				expectedKey = "alternate-id/alternate-key"
			}
		}
		if r.Header.Get("X-Goog-Drive-Resource-Keys") != expectedKey {
			fail(400, "wrong body resource key")
			return
		}
		if mode == "redirect" || mode == "redirect-close" || strings.HasPrefix(mode, "delegated-") {
			target := "https://www.googleapis.com/drive/v3/files/blob-id?alt=media&stage=next"
			if q.Get("stage") == "next" {
				target = s.bodyURL + "/blob?cap=private-capability"
			}
			w.Header().Set("Location", target)
			w.WriteHeader(302)
			return
		}
		if id == "doc-id/export" || id == "alternate-id/export" {
			if q.Get("mimeType") != "text/plain" || r.Header.Get("Range") != "" {
				fail(400, "wrong export representation")
				return
			}
			if mode == "export-error" {
				fail(403, "exportSizeLimitExceeded")
				return
			}
			w.Header().Set("Content-Length", "8")
			io.WriteString(w, "exported")
			return
		}
		if id != "blob-id" {
			fail(400, "unexpected content target")
			return
		}
		if mode == "body-wait" {
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
		writeBody(w, r, mode)
		return
	}
	if id != "root" && id != "root-id" && id != "archive" && id != "shared-root" {
		fail(400, "repeated leaf details")
		return
	}
	key := ""
	if id == "archive" {
		key = "archive/archive-key"
	}
	if id == "shared-root" {
		key = "shared-root/shared-key"
	}
	if r.Header.Get("X-Goog-Drive-Resource-Keys") != key {
		fail(400, "wrong root resource key")
		return
	}
	if mode == "meta-redirect" {
		w.Header().Set("Location", s.bodyURL)
		w.WriteHeader(302)
		return
	}
	json.NewEncoder(w).Encode(rootFile(id))
}
func writeBody(w http.ResponseWriter, r *http.Request, mode string) {
	data, status := "abcdef", 200
	if raw := r.Header.Get("Range"); raw != "" && mode != "fallback" {
		var start, end int
		if _, err := fmt.Sscanf(raw, "bytes=%d-%d", &start, &end); err != nil {
			http.Error(w, "bad range", 500)
			return
		}
		if start >= len(data) {
			w.Header().Set("Content-Range", "bytes */6")
			w.WriteHeader(416)
			return
		}
		end = min(end, len(data)-1)
		data = data[start : end+1]
		status = 206
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/6", start, end))
	}
	w.Header().Set("Content-Length", strconv.Itoa(len(data)))
	w.WriteHeader(status)
	io.WriteString(w, data)
}
func (s *fixture) externalBody(w http.ResponseWriter, r *http.Request) {
	s.external.Add(1)
	if r.Header.Get("Authorization") != "" || r.Header.Get("Cookie") != "" || r.Header.Get("Referer") != "" || r.Header.Get("X-Goog-Drive-Resource-Keys") != "" {
		http.Error(w, "account data leaked", 500)
		return
	}
	if s.state() == "delegated-error" {
		w.WriteHeader(http.StatusForbidden)
		fmt.Fprintf(w, `{"error":{"message":%q,"errors":[{"reason":"rateLimitExceeded"}]}}`, r.URL.RequestURI())
		return
	}
	writeBody(w, r, s.state())
}

type badClose struct {
	io.ReadCloser
	remaining, calls int
	detail           string
}

func (b *badClose) Close() error {
	b.calls++
	if b.remaining > 0 {
		b.remaining--
		return fmt.Errorf("%s: %w", b.detail, io.EOF)
	}
	return b.ReadCloser.Close()
}
func setup(t *testing.T) (*fixture, *Driver, *core.Core, *core.Mount, Options) {
	t.Helper()
	s := &fixture{entered: make(chan struct{}, 2)}
	body := httptest.NewTLSServer(http.HandlerFunc(s.externalBody))
	s.bodyURL = body.URL
	t.Cleanup(body.Close)
	api := httptest.NewServer(http.HandlerFunc(s.control))
	t.Cleanup(api.Close)
	base, _ := url.Parse(api.URL)
	client := &http.Client{Transport: wire(func(r *http.Request) (*http.Response, error) {
		if r.URL.Scheme != "https" || r.URL.Host != "www.googleapis.com" {
			return nil, errors.New("authenticated recipient changed")
		}
		call := r.Clone(r.Context())
		u := *r.URL
		u.Scheme, u.Host = base.Scheme, base.Host
		call.URL = &u
		call.Host = ""
		call.Header.Set("Authorization", "Bearer synthetic")
		if s.state() == "connection-eof" {
			return nil, io.EOF
		}
		response, err := http.DefaultTransport.RoundTrip(call)
		if err == nil && ((s.state() == "redirect-close" && response.StatusCode == 302) || s.state() == "error-close") {
			s.broken = &badClose{ReadCloser: response.Body, remaining: 1, detail: response.Header.Get("Location") + "?cap=private-capability"}
			response.Body = s.broken
		}
		return response, err
	})}
	content := body.Client()
	transport := content.Transport
	content.Transport = wire(func(r *http.Request) (*http.Response, error) {
		mode := s.state()
		if mode == "delegated-transport" {
			return nil, fmt.Errorf("%s: %w", r.URL, io.EOF)
		}
		response, err := transport.RoundTrip(r)
		if err == nil && mode == "delegated-error" {
			s.broken = &badClose{ReadCloser: response.Body, remaining: 1, detail: r.URL.String()}
			response.Body = s.broken
		}
		return response, err
	})
	opts := Options{RootID: "archive", RootResourceKey: "archive-key", Client: client, ContentClient: content, MaxResponseBytes: 4096}
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
func readOpened(t *testing.T, c *core.Core, name string, o core.OpenOptions) ([]byte, core.Content) {
	t.Helper()
	opened, err := c.Open(context.Background(), "u", core.Path(name), o)
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

func TestRootsPagingDirectBodiesAndCopy(t *testing.T) {
	s, _, c, m, opts := setup(t)
	ctx := context.Background()
	defer func() {
		if err := c.Close(ctx); err != nil {
			t.Error(err)
		}
	}()
	if err := c.BindAlias(ctx, "/alias", m, "/"); err != nil {
		t.Fatal(err)
	}
	calls := s.count("GET ")
	invalid := opts
	invalid.RootPath = "/Archive"
	if failed, err := Open(ctx, invalid); err == nil || failed != nil || calls != s.count("GET ") {
		t.Fatal("mutually exclusive roots performed I/O", failed, err)
	}
	invalid = opts
	invalid.ExportFormats = map[string]string{"application/vnd.google-apps.vid": "video/mp4"}
	if failed, err := Open(ctx, invalid); core.CodeOf(err) != core.InvalidRequest || failed != nil || calls != s.count("GET ") {
		t.Fatal("unsupported Vids branch performed I/O", failed, err)
	}
	before := s.count("/files/archive?")
	opts.RootID, opts.RootPath = "", "/Archive"
	second, err := Open(ctx, opts)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := second.Stat(ctx, core.NativePath("/p%+中")); err != nil {
		t.Fatal(err)
	}
	if s.count("/files/archive?") != before {
		t.Fatal("path construction repeated root details")
	}
	if err := second.Close(); err != nil {
		t.Fatal(err)
	}
	opts.RootPath, opts.RootResourceKey = "", ""
	account, err := Open(ctx, opts)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := account.Stat(ctx, core.NativePath("/Archive/p%+中")); err != nil {
		t.Fatal(err)
	}
	if err := account.Close(); err != nil {
		t.Fatal(err)
	}
	opts.RootID, opts.RootResourceKey = "shared-root", "shared-key"
	shared, err := Open(ctx, opts)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := shared.Stat(ctx, core.NativePath("/p%+中")); err != nil {
		t.Fatal(err)
	}
	if err := shared.Close(); err != nil {
		t.Fatal(err)
	}
	req := core.ListRequest{Query: core.Children(core.Path("/alias")), Limit: 1, CursorTTL: time.Minute}
	empty, err := c.List(ctx, "u", req)
	if err != nil || len(empty.Items) != 0 || empty.NextCursor == nil {
		t.Fatal(empty, err)
	}
	req.Cursor = *empty.NextCursor
	first, err := c.List(ctx, "u", req)
	if err != nil || len(first.Items) != 1 || first.Items[0].Name != "p%+中" || first.NextCursor == nil {
		t.Fatal(first, err)
	}
	req.Cursor = *first.NextCursor
	calls = s.count("/files?")
	last, err := c.List(ctx, "u", req)
	if err != nil || len(last.Items) != 1 || last.Items[0].Name != "sub" || calls != s.count("/files?") {
		t.Fatal("public leftover fetched native page", last, err)
	}
	replay, err := c.List(ctx, "u", req)
	if err != nil || len(replay.Items) != 1 || calls != s.count("/files?") {
		t.Fatal("cursor replay fetched native page", replay, err)
	}
	if meta, err := c.Stat(ctx, "u", core.Path("/remote/"+quotedName)); err != nil || meta.Name != quotedName {
		t.Fatal("query escape or scope", meta, err)
	}
	before = s.count("/files/blob-id?")
	ranges := core.OpenOptions{Range: &core.Range{Start: 1, End: 3}}
	data, opened := readOpened(t, c, "/alias/p%+中", ranges)
	if string(data) != "bcd" || opened.Range == nil || s.count("/files/blob-id?") != before+1 || s.external.Load() != 0 {
		t.Fatal("direct API body added request or lost range", string(data), opened.Range)
	}
	s.set("fallback")
	data, opened = readOpened(t, c, "/remote/p%+中", ranges)
	if string(data) != "abcdef" || opened.Range != nil {
		t.Fatal(string(data), opened.Range)
	}
	s.set("")
	if _, err := c.Open(ctx, "u", core.Path("/remote/p%+中"), core.OpenOptions{Range: &core.Range{Start: 9, End: 11}}); core.CodeOf(err) != core.RangeNotSatisfiable {
		t.Fatal(err)
	}
	s.set("redirect")
	data, opened = readOpened(t, c, "/remote/p%+中", ranges)
	if string(data) != "bcd" || opened.Range == nil || s.external.Load() != 1 {
		t.Fatal("authenticated-to-private redirect", string(data), opened.Range)
	}
	s.set("")
	target := t.TempDir()
	localDriver, err := local.New(target)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Install("/local", localDriver); err != nil {
		t.Fatal(err)
	}
	calls = s.count("GET ")
	if _, err := c.Stat(ctx, "u", core.Path("/remote/denied")); core.CodeOf(err) != core.PermissionDenied || calls != s.count("GET ") {
		t.Fatal("unauthorized native request", err)
	}
	if s.count("/about?") != 0 {
		t.Fatal("ordinary source queried export formats")
	}
	formats := map[string]string{documentMIME: "text/plain", alternateMIME: "text/plain"}
	opts.RootID, opts.RootPath, opts.RootResourceKey = "archive", "", "archive-key"
	opts.ExportFormats = formats
	exportSource, err := Open(ctx, opts)
	if err != nil {
		t.Fatal(err)
	}
	if s.count("/about?") != 1 {
		t.Fatal("export formats were not discovered once")
	}
	formats[documentMIME] = "application/pdf"
	if _, err := c.Install("/export", exportSource); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"Document", "Alternate"} {
		address := "/export/" + name
		if meta, err := c.Stat(ctx, "u", core.Path(address)); err != nil || meta.Size != nil || meta.Name != name {
			t.Fatal("native editor size became export length", meta, err)
		}
		data, opened = readOpened(t, c, address, ranges)
		if string(data) != "exported" || opened.Range != nil || opened.Length == nil || *opened.Length != 8 || opened.Entry.Name != name {
			t.Fatal("export format, name, size or full fallback", string(data), opened, err)
		}
	}
	if _, err := c.Copy(ctx, "u", core.Path("/export/Document"), core.Path("/local/Document"), core.CopyOptions{}); err != nil {
		t.Fatal(err)
	}
	data, err = os.ReadFile(filepath.Join(target, "Document"))
	if err != nil || string(data) != "exported" || s.count("/about?") != 1 {
		t.Fatal("export copy or repeated format discovery", string(data), err)
	}
	s.set("export-error")
	if _, err := c.Open(ctx, "u", core.Path("/export/Document"), core.OpenOptions{}); err == nil || !strings.Contains(err.Error(), "exportSizeLimitExceeded") {
		t.Fatal("native export refusal lost", err)
	}
	s.set("")
}

func TestNativeDifferencesAndVisibleSpecialFiles(t *testing.T) {
	s, _, c, _, opts := setup(t)
	ctx := context.Background()
	for _, test := range []struct {
		mode string
		code core.Code
	}{{"duplicate", core.PathAmbiguous}, {"repeated", core.InvalidUpstreamResponse}, {"wrong-parent", core.InvalidUpstreamResponse}, {"wrong-name", core.NotFound}, {"negative", core.InvalidUpstreamResponse}, {"stalled", core.InvalidUpstreamResponse}, {"incomplete", core.UpstreamFailure}, {"truncated", core.InvalidUpstreamResponse}, {"connection-eof", core.UpstreamFailure}} {
		s.set(test.mode)
		_, err := c.Stat(ctx, "u", core.Path("/remote/p%+中"))
		if core.CodeOf(err) != test.code || errors.Is(err, io.EOF) {
			t.Fatal(test.mode, err)
		}
	}
	s.set("expired-token")
	before := s.count("/files?")
	_, err := c.Stat(ctx, "u", core.Path("/remote/p%+中"))
	if err == nil || !strings.Contains(err.Error(), "invalidPageToken") || s.count("/files?") != before+2 {
		t.Fatal("expired native cursor restarted", err)
	}
	s.set("not-downloadable")
	before = s.count("/files/blob-id?")
	if _, err := c.Open(ctx, "u", core.Path("/remote/p%+中"), core.OpenOptions{}); core.CodeOf(err) != core.UpstreamAccessDenied || s.count("/files/blob-id?") != before {
		t.Fatal(err)
	}
	s.set("native-error")
	if _, err := c.Stat(ctx, "u", core.Path("/remote/p%+中")); err == nil || !strings.Contains(err.Error(), "downloadQuotaExceeded") {
		t.Fatal(err)
	}
	s.set("special")
	req := core.ListRequest{Query: core.Children(core.Path("/remote")), Limit: 100, CursorTTL: time.Minute}
	page, err := c.List(ctx, "u", req)
	if err != nil || page.NextCursor == nil {
		t.Fatal(page, err)
	}
	req.Cursor = *page.NextCursor
	page, err = c.List(ctx, "u", req)
	if err != nil || len(page.Items) != 4 {
		t.Fatal(page, err)
	}
	for _, f := range page.Items {
		if f.Kind != core.File || f.Size != nil {
			t.Fatal("special file was hidden or a directory", f)
		}
		if _, err := c.Open(ctx, "u", f.Address, core.OpenOptions{}); core.CodeOf(err) != core.UnsupportedOperation {
			t.Fatal(f.Name, err)
		}
	}
	if s.count("outside") != 0 || s.count("/download?") != 0 {
		t.Fatal("special content was followed")
	}
	s.set("")
	limited := opts
	limited.MaxResponseBytes = 32
	failed, err := Open(ctx, limited)
	if core.CodeOf(err) != core.ResponseBudgetExceeded {
		t.Fatal(failed, err)
	}
	if failed != nil {
		if err := failed.Close(); err != nil {
			t.Fatal(err)
		}
	}
	limited = opts
	limited.MaxResolvePages = 1
	source, err := Open(ctx, limited)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := source.Stat(ctx, core.NativePath("/p%+中")); core.CodeOf(err) != core.ResolutionBudgetExceeded {
		t.Fatal(err)
	}
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}
	s.set("meta-redirect")
	failed, err = Open(ctx, opts)
	if err == nil || s.external.Load() != 0 {
		t.Fatal("metadata redirect followed", err)
	}
	if failed != nil {
		if err := failed.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestCancellationRetirementAndCloseOwnership(t *testing.T) {
	s, d, c, m, opts := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	before := s.count("/files/archive?")
	opts.ContentClient = opts.Client
	if invalid, err := Open(ctx, opts); invalid != nil || core.CodeOf(err) != core.InvalidRequest || s.count("/files/archive?") != before {
		t.Fatal("credential-bearing client reused for delegated content", err)
	}
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
		t.Fatal("list never started")
	}
	stop()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("list cancellation did not join")
	}
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
	s.set("redirect-close")
	before = s.count("/files/blob-id?")
	path := core.Path("/remote/p%+中")
	if _, err := c.Open(ctx, "u", path, core.OpenOptions{}); !errors.Is(err, io.ErrUnexpectedEOF) || strings.Contains(err.Error(), "private-capability") ||
		s.count("/files/blob-id?") != before+1 || s.external.Load() != 0 || s.broken == nil || s.broken.calls != 1 {
		t.Fatal("intermediate close allowed another request", err)
	}
	redirectClose := s.broken
	s.set("delegated-transport")
	if _, err := c.Open(ctx, "u", path, core.OpenOptions{}); core.CodeOf(err) != core.UpstreamFailure ||
		!errors.Is(err, io.ErrUnexpectedEOF) || strings.Contains(err.Error(), "private-capability") {
		t.Fatal("delegated transport failure exposed its capability or lost its cause", err)
	}
	s.set("delegated-error")
	if _, err := c.Open(ctx, "u", path, core.OpenOptions{}); core.CodeOf(err) != core.UpstreamAccessDenied ||
		!errors.Is(err, io.ErrUnexpectedEOF) || strings.Contains(err.Error(), "private-capability") ||
		strings.Contains(err.Error(), "rateLimitExceeded") || s.external.Load() != 1 || s.broken.calls != 1 {
		t.Fatal("delegated response spoofed Drive or exposed its capability", err)
	}
	delegatedClose := s.broken
	s.set("error-close")
	if _, err := c.Stat(ctx, "u", core.Path("/remote/p%+中")); err == nil || errors.Is(err, io.EOF) || !strings.Contains(err.Error(), "nativeAccessRefused") {
		t.Fatal("error response cleanup or reason lost", err)
	}
	errorClose := s.broken
	s.set("body-wait")
	opened, err := c.Open(ctx, "u", core.Path("/remote/p%+中"), core.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-s.entered:
	case <-ctx.Done():
		t.Fatal("body never started")
	}
	go func() { _, err := io.ReadAll(opened.Body); done <- err }()
	if err := m.Retire(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("retirement did not join")
	}
	if err := opened.Body.Close(); err != nil {
		t.Fatal(err)
	}
	if redirectClose.calls != 2 || delegatedClose.calls != 2 || errorClose.calls != 2 {
		t.Fatal("failed resources were not retried once", redirectClose.calls, delegatedClose.calls, errorClose.calls)
	}
}
