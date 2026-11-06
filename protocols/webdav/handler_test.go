package webdav_test

import (
	"bytes"
	"context"
	"encoding/xml"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"testing/iotest"
	"time"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/local"
	"github.com/nostalume/mountweave/protocols/webdav"
)

func TestBasicFileWorkflow(t *testing.T) {
	root, staging := t.TempDir(), t.TempDir()
	failRebuild := false
	d, err := local.New(root, local.Options{StagingDirectory: staging, MaxStagingBytes: 32})
	if err != nil {
		t.Fatal(err)
	}
	c, err := core.New(core.Options{MaxCursors: 1, Authorize: func(_ context.Context, p core.Principal, a core.Action, address core.Address) error {
		if failRebuild && a == core.CreateAction && address == core.Path("/alias/partial") {
			if _, err := os.Stat(filepath.Join(root, "partial")); errors.Is(err, os.ErrNotExist) {
				return errors.New("injected rebuild denial")
			}
		}
		if p == "creator" && a == core.WriteAction {
			return errors.New("replacement denied")
		}
		if core.Within(address.Value(), "/files/private") || address == core.Path("/files/protected/Draft") {
			return errors.New("private entry")
		}
		return nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := c.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	m, err := c.Install("/files", d)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.BindAlias(context.Background(), "/alias", m, "/"); err != nil {
		t.Fatal(err)
	}
	opts := webdav.Options{Tree: core.TreeOptions{MaxEntries: 64, MaxPlanBytes: 8192}, Prefix: "/dav/", Root: core.Path("/alias"), CursorTTL: time.Minute,
		Limits: webdav.Limits{XMLBytes: 1024, ListEntries: 8, ListNameBytes: 4096},
		Authenticate: func(w http.ResponseWriter, r *http.Request) (core.Principal, bool) {
			if p := r.Header.Get("X-Actor"); p != "" {
				return core.Principal(p), true
			}
			w.Header().Set("WWW-Authenticate", `Basic realm="fixture"`)
			w.WriteHeader(401)
			return "", false
		}}
	h, err := webdav.New(c, opts)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(h)
	t.Cleanup(server.Close)
	request := func(method, path, body string, headers map[string]string, want int) []byte {
		t.Helper()
		r, err := http.NewRequest(method, server.URL+path, strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		r.Header.Set("X-Actor", "writer")
		for key, value := range headers {
			r.Header.Set(key, value)
		}
		response, err := server.Client().Do(r)
		if err != nil {
			t.Fatal(err)
		}
		data, readErr := io.ReadAll(response.Body)
		closeErr := response.Body.Close()
		if response.StatusCode != want || readErr != nil || closeErr != nil {
			t.Fatalf("%s %s: %d %s %v %v", method, path, response.StatusCode, data, readErr, closeErr)
		}
		if response.Header.Get("DAV") != "" {
			t.Fatal("unimplemented compliance advertised")
		}
		return data
	}
	request("OPTIONS", "/dav/", "", nil, 204)
	request("MKCOL", "/dav/folder/", "", nil, 201)
	request("HEAD", "/dav/folder/", "", map[string]string{"If-None-Match": "*"}, 405)
	path := "/dav/folder/space%20%25%20%26"
	request("PUT", path, "first", nil, 201)
	request("PUT", path, "second", nil, 204)
	if got := request("GET", path, "", map[string]string{"Range": "bytes=1-3"}, 206); string(got) != "eco" {
		t.Fatal(string(got))
	}
	request("HEAD", path, "", nil, 200)
	request("GET", path, "", map[string]string{"If-None-Match": "*"}, 304)
	request("PUT", path, "wrong", map[string]string{"If-None-Match": "*"}, 412)
	request("PUT", "/dav/folder/exclusive", "yes", map[string]string{"X-Actor": "creator", "If-None-Match": "*"}, 201)
	request("PUT", "/dav/folder/denied", "no", map[string]string{"X-Actor": "creator"}, 403)
	request("PUT", path, "wrong", map[string]string{"If-Match": `"unknown"`}, 501)
	request("PUT", "/dav/missing/file", "no", nil, 409)
	data := request("PROPFIND", "/dav/folder/", "", map[string]string{"Depth": "1"}, 207)
	var listing struct {
		Responses []struct {
			Href string `xml:"href"`
		} `xml:"response"`
	}
	if err := xml.Unmarshal(data, &listing); err != nil || len(listing.Responses) != 3 {
		t.Fatal(string(data), err)
	}
	if !bytes.Contains(data, []byte("/dav/folder/space%20%25%20&amp;")) {
		t.Fatal("href not escaped once", string(data))
	}
	data = request("PROPFIND", path, `<propfind xmlns="DAV:"><prop><getcontentlength/><missing xmlns="urn:fixture"/></prop></propfind>`, map[string]string{"Depth": "0"}, 207)
	if !bytes.Contains(data, []byte("200 OK")) || !bytes.Contains(data, []byte("404 Not Found")) {
		t.Fatal(string(data))
	}
	request("PROPFIND", "/dav/", "", nil, 403)
	request("PROPFIND", path, `<propfind xmlns="DAV:"><prop>`, map[string]string{"Depth": "0"}, 400)
	request("PROPFIND", path, strings.Repeat(" ", 1025), map[string]string{"Depth": "0"}, 413)
	request("GET", "/dav/folder/%2foutside", "", nil, 400)
	request("PUT", "/dav/%2e%2e/outside", "no", nil, 400)
	request("PUT", "/dav/folder/literal%252f", "once", nil, 201)
	request("MKCOL", "/dav/folder/", "", nil, 405)
	request("MKCOL", "/dav/with-body", "x", nil, 415)
	request("PUT", "/dav/original", "one", nil, 201)
	request("COPY", "/dav/original", "", map[string]string{"Destination": server.URL + "/dav/copied", "Overwrite": "F", "Depth": "1"}, 201)
	request("COPY", "/dav/original", "", map[string]string{"Destination": "/dav/copied", "Overwrite": "F"}, 412)
	request("PUT", "/dav/moved", "old", nil, 201)
	request("MOVE", "/dav/copied", "", map[string]string{"Destination": "/dav/moved"}, 204)
	request("GET", "/dav/copied", "", nil, 404)
	if got := request("GET", "/dav/moved", "", nil, 200); string(got) != "one" {
		t.Fatal(string(got))
	}
	request("DELETE", "/dav/moved", "", nil, 204)
	request("GET", "/dav/moved", "", nil, 404)
	request("MKCOL", "/dav/collection/", "", nil, 201)
	request("PUT", "/dav/collection/a", "a", nil, 201)
	request("MKCOL", "/dav/collection/empty/", "", nil, 201)
	request("MKCOL", "/dav/other/", "", nil, 201)
	request("PUT", "/dav/other/obsolete", "old", nil, 201)
	request("COPY", "/dav/collection/", "", map[string]string{"Destination": "/dav/other/"}, 204)
	request("GET", "/dav/other/obsolete", "", nil, 404)
	request("GET", "/dav/other/a", "", nil, 200)
	request("COPY", "/dav/collection/", "", map[string]string{"Destination": "/dav/shallow/", "Depth": "0"}, 201)
	request("GET", "/dav/shallow/a", "", nil, 404)
	request("COPY", "/dav/collection/", "", map[string]string{"Destination": "/dav/invalid/", "Depth": "1"}, 400)
	request("COPY", "/dav/collection/", "", map[string]string{"Destination": "/dav/missing/child/"}, 409)
	request("COPY", "/dav/original", "", map[string]string{"Destination": "/dav/missing/file"}, 409)
	request("COPY", "/dav/missing/", "", map[string]string{"Destination": "/dav/untouched/"}, 404)
	request("COPY", "/dav/collection/", "", map[string]string{"Destination": "https://outside.invalid/dav/no"}, 502)
	request("COPY", "/dav/original", "", map[string]string{"Destination": "/dav/original"}, 403)
	request("DELETE", "/dav/", "", nil, 403)
	request("MKCOL", "/dav/partial/", "", nil, 201)
	request("PUT", "/dav/partial/old", "old", nil, 201)
	failRebuild = true
	partial := request("COPY", "/dav/collection/", "", map[string]string{"Destination": "/dav/partial/"}, 207)
	failRebuild = false
	if !strings.Contains(string(partial), "/dav/partial") || !strings.Contains(string(partial), "403 Forbidden") || strings.Contains(string(partial), "424") {
		t.Fatal("incorrect phase projection", string(partial))
	}
	if _, err := os.Stat(filepath.Join(root, "partial")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("pre-delete was hidden", err)
	}
	request("COPY", "/dav/collection/", "", map[string]string{"Destination": "/dav/partial/"}, 201)
	request("DELETE", "/dav/partial/", "", nil, 204)

	// Direct Core and HTTP share aliases, content, revision and cleanup owners.
	page, err := c.List(context.Background(), "writer", core.ListRequest{Query: core.Children(core.Path("/files/folder")), Limit: 1, CursorTTL: time.Minute})
	if err != nil || page.NextCursor == nil {
		t.Fatal(page, err)
	}
	request("PUT", path, "shared", nil, 204)
	if err := c.CheckCursor(context.Background(), "writer", core.ListRequest{Query: core.Children(core.Path("/files/folder")), Cursor: *page.NextCursor}); core.CodeOf(err) != core.CursorUnavailable {
		t.Fatal("HTTP did not invalidate Core observation", err)
	}
	r := httptest.NewRequest("PUT", path, io.MultiReader(strings.NewReader("bad"), iotest.ErrReader(io.ErrNoProgress)))
	r.Header.Set("X-Actor", "writer")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code < 400 {
		t.Fatal("failed input acknowledged", w.Code)
	}
	if got, err := os.ReadFile(filepath.Join(root, "folder", "space % &")); err != nil || string(got) != "shared" {
		t.Fatal("failed input replaced old data", string(got), err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	input := &cancelInput{ctx: ctx, started: make(chan struct{}), first: strings.NewReader("bad")}
	r = httptest.NewRequestWithContext(ctx, "PUT", path, input)
	r.Header.Set("X-Actor", "writer")
	done := make(chan struct{})
	go func() { defer close(done); h.ServeHTTP(httptest.NewRecorder(), r) }()
	select {
	case <-input.started:
	case <-time.After(time.Second):
		cancel()
		t.Fatal("input did not start")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("HTTP cancellation did not drain")
	}
	if got, err := os.ReadFile(filepath.Join(root, "folder", "space % &")); err != nil || string(got) != "shared" {
		t.Fatal("cancellation published incomplete data", string(got), err)
	}
	opts.Limits.ListEntries = 1
	opts.Prefix = "/limited/"
	limited, err := webdav.New(c, opts)
	if err != nil {
		t.Fatal(err)
	}
	r = httptest.NewRequest("PROPFIND", "/limited/folder/", nil)
	r.Header.Set("X-Actor", "writer")
	r.Header.Set("Depth", "1")
	w = httptest.NewRecorder()
	limited.ServeHTTP(w, r)
	if w.Code != 507 {
		t.Fatal("truncated listing acknowledged", w.Code, w.Body.String())
	}
	if _, err := c.List(context.Background(), "writer", core.ListRequest{Query: core.Children(core.Path("/files/folder")), Limit: core.MaxPage, CursorTTL: time.Minute}); err != nil {
		t.Fatal("failed PROPFIND retained cursor capacity", err)
	}
	if entries, err := os.ReadDir(staging); err != nil || len(entries) != 0 {
		t.Fatal("HTTP leaked staging", entries, err)
	}
	t.Run("native tree authority", func(t *testing.T) {
		for _, name := range []string{"private", "export", "protected"} {
			if err := os.Mkdir(filepath.Join(root, name), 0700); err != nil {
				t.Fatal(err)
			}
		}
		for _, name := range []string{"private/item", "protected/Draft"} {
			if err := os.WriteFile(filepath.Join(root, name), []byte("hidden"), 0600); err != nil {
				t.Fatal(err)
			}
		}
		if err := os.Symlink(filepath.Join("..", "private"), filepath.Join(root, "export", "bridge")); err != nil {
			if runtime.GOOS == "windows" && (errors.Is(err, os.ErrPermission) || errors.Is(err, syscall.Errno(1314))) {
				t.Skipf("symlink unavailable: %v", err)
			}
			t.Fatal(err)
		}
		ctx := context.Background()
		if _, err := c.Stat(ctx, "writer", core.Path("/files/private/item")); core.CodeOf(err) != core.PermissionDenied {
			t.Fatal("fixture did not deny the direct target", err)
		}
		for _, name := range []string{"/files/export/bridge/item", "/alias/export/bridge/item", "/alias/protected/DRAFT"} {
			address := core.Path(name)
			if _, err := c.Stat(ctx, "writer", address); err == nil {
				t.Errorf("stat exposed %s", name)
			}
			if body, err := c.Open(ctx, "writer", address, core.OpenOptions{}); err == nil {
				_ = body.Body.Close()
				t.Errorf("open exposed %s", name)
			}
		}
		link := core.Path("/alias/export/bridge")
		if _, err := c.List(ctx, "writer", core.ListRequest{Query: core.Children(link), Limit: core.MaxPage, CursorTTL: time.Minute}); err == nil {
			t.Error("list followed the link")
		}
		if change, err := c.Mkdir(ctx, "writer", core.Path(link.Value()+"/added")); err == nil || change.Effect != core.Unchanged {
			t.Error("mkdir crossed the link", change, err)
		}
		if result, err := c.Move(ctx, "writer", core.Path(link.Value()+"/item"), core.Path("/alias/export/moved")); err == nil || result.Source.Effect != core.Unchanged || result.Target.Effect != core.Unchanged {
			t.Error("move crossed the link", result, err)
		}
		for _, tc := range []struct {
			path   string
			target core.RemoveTarget
			code   core.Code
		}{
			{"/alias/protected/DRAFT", core.RemoveNonDirectory, core.NotFound},
			{"/files/export", core.RemoveNonDirectory, core.NotFile},
			{"/alias/protected", core.RemoveEmptyDirectory, core.DirectoryNotEmpty},
		} {
			change, err := c.Remove(ctx, "writer", core.Path(tc.path), core.RemoveOptions{Target: tc.target})
			if core.CodeOf(err) != tc.code || change.Effect != core.Unchanged {
				t.Error("unsafe removal", tc.path, change, err)
			}
		}
		for _, endpoint := range []string{"/dav/export/bridge/item", "/dav/protected/DRAFT"} {
			r := httptest.NewRequest("GET", endpoint, nil)
			r.Header.Set("X-Actor", "writer")
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			if w.Code < 400 || strings.Contains(w.Body.String(), "hidden") {
				t.Error("WebDAV escaped tree authority", endpoint, w.Code, w.Body.String())
			}
		}
		if data, err := os.ReadFile(filepath.Join(root, "private", "item")); err != nil || string(data) != "hidden" {
			t.Fatal("protected source changed", err)
		}
	})
}

type cancelInput struct {
	ctx     context.Context
	started chan struct{}
	first   *strings.Reader
}

func (r *cancelInput) Read(p []byte) (int, error) {
	if r.first.Len() != 0 {
		return r.first.Read(p)
	}
	close(r.started)
	<-r.ctx.Done()
	return 0, r.ctx.Err()
}
