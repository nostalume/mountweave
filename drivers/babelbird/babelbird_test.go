package babelbird

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	core "github.com/nostalume/mountweave"
)

type fixtureTransport func(*http.Request) (*http.Response, error)

func (f fixtureTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestWorkspaceThroughCore(t *testing.T) {
	const token = "test-account-token"
	const name = "report #.json"
	const data = `{"status":"original"}`
	var grants, bodies atomic.Int32
	var scenario atomic.Int32
	content := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		bodies.Add(1)
		if r.Header.Get("Authorization") != "" || r.Header.Get("Cookie") != "" ||
			r.URL.Query().Get("signature") != "test+grant" {
			t.Errorf("invalid download grant or account credentials on the body request")
			w.WriteHeader(http.StatusForbidden)
			return
		}
		if r.URL.Path == "/grant" {
			http.Redirect(w, r, "/object?signature=test%2Bgrant", http.StatusFound)
			return
		}
		if r.URL.Path != "/object" {
			t.Errorf("unexpected body path %s", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if r.Header.Get("Range") == "bytes=1-3" {
			w.Header().Set("Content-Range", fmt.Sprintf("bytes 1-3/%d", len(data)))
			w.Header().Set("Content-Length", "3")
			w.WriteHeader(http.StatusPartialContent)
			_, _ = io.WriteString(w, data[1:4])
			return
		}
		_, _ = io.WriteString(w, data)
	}))
	defer content.Close()
	api := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.Header.Get("Authorization") != "Bearer "+token {
			t.Errorf("invalid control request")
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		q := r.URL.Query()
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/nd/api/file/list_dir":
			page, total, pages := q.Get("page"), 0, 0
			entries := make([]map[string]string, 0)
			add := func(id, name string) {
				entries = append(entries, map[string]string{"fileId": id, "name": name})
			}
			switch q.Get("parent_id") + "/" + q.Get("type") + "/" + page {
			case "2/2/0":
				add("10", "docs")
				total, pages = 1, 1
			case "2/1/0":
				total, pages = 2, 2
				add("20", "outside")
			case "2/1/1":
				total, pages = 2, 2
				switch scenario.Load() {
				case 1:
					add("21", "docs")
				case 2:
					// The final page omits a counted child.
				default:
					add("21", "other")
				}
			case "10/2/0":
			case "10/1/0":
				add("30", name)
				total, pages = 1, 1
			default:
				t.Errorf("unexpected list selector: %v", q)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			if q.Get("page_size") != strconv.Itoa(pageSize) {
				t.Errorf("invalid native page size")
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"success": true, "code": 0, "total": total, "pages": pages, "files": entries,
			})
		case "/api/file/getFileDownloadUrl.do":
			grants.Add(1)
			if q.Get("fileId") != "30" {
				t.Errorf("download was issued for the wrong identity: %v", q)
				w.WriteHeader(http.StatusNotFound)
				return
			}
			address := content.URL + "/grant?signature=test%2Bgrant"
			if scenario.Load() == 3 {
				address = "http://untrusted.example/object"
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"success": true, "code": 0, "url": address})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer api.Close()
	client := api.Client()
	transport := client.Transport
	client.Transport = fixtureTransport(func(r *http.Request) (*http.Response, error) {
		copy := r.Clone(r.Context())
		copy.Header.Set("Authorization", "Bearer "+token)
		return transport.RoundTrip(copy)
	})
	options := Options{Endpoint: api.URL, RootID: "2", Client: client, ContentClient: content.Client()}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	root, err := Open(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	c, err := core.New(core.Options{Authorize: func(context.Context, core.Principal, core.Action, core.Address) error { return nil }})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close(context.Background())
	mount, err := c.Install("/drive", root)
	if err != nil {
		t.Fatal(err)
	}
	q := core.ListRequest{Query: core.Children(core.Path("/drive")), Limit: 1, CursorTTL: time.Minute}
	for i, expected := range []string{"docs", "outside", "other"} {
		page, err := c.List(ctx, "reader", q)
		if err != nil || (page.NextCursor == nil) != (i == 2) ||
			len(page.Items) != 1 || page.Items[0].Name != expected ||
			(page.Items[0].Kind == core.Directory) != (i == 0) {
			t.Fatalf("page %d: %v %v", i, page, err)
		}
		if page.NextCursor != nil {
			q.Cursor = *page.NextCursor
		}
	}
	address := core.Path("/drive/docs/" + name)
	for _, mode := range []struct {
		scenario int32
		code     core.Code
	}{{1, core.PathAmbiguous}, {2, core.InvalidUpstreamResponse}, {3, core.InvalidUpstreamResponse}} {
		scenario.Store(mode.scenario)
		if _, err := c.Open(ctx, "reader", address, core.OpenOptions{}); core.CodeOf(err) != mode.code || bodies.Load() != 0 {
			t.Fatalf("invalid lookup or grant %d: %v", mode.scenario, err)
		}
	}
	if grants.Load() != 1 {
		t.Fatalf("invalid listing reached download authorization: %d", grants.Load())
	}
	scenario.Store(0)
	options.RootPath = "/docs"
	sub, err := Open(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Install("/sub", sub); err != nil {
		t.Fatal(err)
	}
	source := core.Path("/sub/" + name)
	opened, err := c.Open(ctx, "reader", source, core.OpenOptions{Range: &core.Range{Start: 1, End: 3}})
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(opened.Body)
	closeErr := opened.Body.Close()
	if err != nil || closeErr != nil || string(got) != data[1:4] || opened.Range == nil {
		t.Fatalf("range: %q %v %v %v", got, opened.Range, err, closeErr)
	}
	opened, err = c.Open(ctx, "reader", source, core.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	got, err = io.ReadAll(opened.Body)
	closeErr = opened.Body.Close()
	if err != nil || closeErr != nil || string(got) != data || opened.Range != nil {
		t.Fatalf("full body: %q %v %v %v", got, opened.Range, err, closeErr)
	}
	if err := mount.Retire(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := root.Stat(ctx, core.NativePath("/")); core.CodeOf(err) != core.MountUnavailable {
		t.Fatalf("retired driver remained active: %v", err)
	}
}
