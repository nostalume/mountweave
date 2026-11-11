package misskey_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/misskey"
)

type transport func(*http.Request) (*http.Response, error)

func (f transport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestOwnDriveAndAllowedBody(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var contentCalls, deniedCalls, foldersContinued, filesContinued atomic.Int32
	var wait, badParent, cycle atomic.Bool
	denied := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		deniedCalls.Add(1)
		w.WriteHeader(http.StatusForbidden)
	}))
	defer denied.Close()
	content := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		contentCalls.Add(1)
		if r.Method != http.MethodGet || r.Header.Get("Authorization") != "" || r.Header.Get("Cookie") != "" ||
			r.Header.Get("Referer") != "" || r.Header.Get("Accept-Encoding") != "identity" {
			t.Error("content request used control credentials or transport policy")
		}
		switch r.URL.Path {
		case "/redirect":
			w.Header().Set("Location", "/bytes?grant=cap%2B")
			w.WriteHeader(http.StatusFound)
		case "/cross":
			w.Header().Set("Location", denied.URL+"/bytes?grant=private")
			w.WriteHeader(http.StatusTemporaryRedirect)
		case "/bytes":
			if r.URL.RawQuery != "grant=cap%2B" {
				t.Error("content capability was rewritten")
			}
			if wait.Load() {
				w.Header().Set("Content-Length", "6")
				w.WriteHeader(http.StatusOK)
				w.(http.Flusher).Flush()
				<-r.Context().Done()
				return
			}
			if r.Header.Get("Range") != "" {
				if r.Header.Get("Range") != "bytes=1-3" {
					t.Error("native range changed across redirect")
				}
				w.Header().Set("Content-Range", "bytes 1-3/6")
				w.Header().Set("Content-Length", "3")
				w.WriteHeader(http.StatusPartialContent)
				_, _ = io.WriteString(w, "bcd")
				return
			}
			_, _ = io.WriteString(w, "abcdef")
		default:
			t.Error("unexpected content route")
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer content.Close()
	api := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var q struct {
			FolderID *string
			UntilID  string
			Limit    int
		}
		if r.Method != http.MethodPost || r.Header.Get("Authorization") != "Bearer fixture" ||
			r.Header.Get("Content-Type") != "application/json" || json.NewDecoder(r.Body).Decode(&q) != nil ||
			q.Limit < 1 || q.Limit > 100 {
			t.Error("invalid Drive control request")
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		items := []map[string]any{}
		folder := func(id, name string, parent any) map[string]any {
			return map[string]any{"id": id, "name": name, "parentId": parent}
		}
		file := func(id, name, url string) map[string]any {
			parent := "album"
			if badParent.Load() {
				parent = "unrelated"
			}
			return map[string]any{"id": id, "name": name, "folderId": parent, "url": url,
				"thumbnailUrl": denied.URL + "/preview", "size": 0}
		}
		switch {
		case r.URL.Path == "/api/drive/folders" && q.FolderID == nil:
			items = append(items, folder("album", "Album", nil))
		case r.URL.Path == "/api/drive/files" && q.FolderID == nil:
		case r.URL.Path == "/api/drive/folders" && *q.FolderID == "album":
			if cycle.Load() {
				id := "f9"
				if q.UntilID == id {
					id = "f8"
				}
				_ = json.NewEncoder(w).Encode([]any{folder(id, "Folder", "album")})
				return
			}
			if q.UntilID != "" {
				foldersContinued.Add(1)
			}
			items = append(items, folder("f9", "Folder", "album"))
		case r.URL.Path == "/api/drive/files" && *q.FolderID == "album":
			if q.UntilID != "" {
				filesContinued.Add(1)
			}
			items = append(items, file("d9", "a & %", content.URL+"/redirect"),
				file("d8", "Folder", content.URL+"/bytes"),
				file("d7", "untrusted", denied.URL+"/bytes?grant=private"),
				file("d6", "cross", content.URL+"/cross"))
		default:
			t.Error("Drive request left the selected account path")
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		batch := []map[string]any{}
		for _, item := range items {
			if q.UntilID == "" || item["id"].(string) < q.UntilID {
				batch = append(batch, item)
			}
		}
		_ = json.NewEncoder(w).Encode(batch[:min(len(batch), q.Limit)])
	}))
	defer api.Close()
	control := api.Client()
	apiTransport := control.Transport
	control.Transport = transport(func(r *http.Request) (*http.Response, error) {
		if !strings.HasPrefix(r.URL.String(), api.URL+"/api/drive/") {
			return nil, errors.New("control request escaped the API")
		}
		r = r.Clone(r.Context())
		r.Header.Set("Authorization", "Bearer fixture")
		return apiTransport.RoundTrip(r)
	})
	opts := misskey.Options{Endpoint: api.URL, RootPath: "/Album", Client: control, ContentClient: content.Client(), ContentOrigins: []string{content.URL}}
	d, err := misskey.Open(ctx, opts)
	if err != nil {
		t.Fatal(err)
	}
	c, err := core.New(core.Options{MaxCursors: 2, Authorize: func(context.Context, core.Principal, core.Action, core.Address) error { return nil }})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close(ctx)
	mount, err := c.Install("/remote", d)
	if err != nil {
		t.Fatal(err)
	}
	list := core.ListRequest{Query: core.Children(core.Path("/remote")), Limit: 1, CursorTTL: time.Minute}
	var names []string
	for range 6 {
		p, err := c.List(ctx, "u", list)
		if err != nil {
			t.Fatal(err)
		}
		for _, entry := range p.Items {
			names = append(names, entry.Name)
		}
		if p.NextCursor == nil {
			break
		}
		list.Cursor = *p.NextCursor
	}
	if strings.Join(names, "|") != "Folder|a & %|Folder|untrusted|cross" || foldersContinued.Load() == 0 || filesContinued.Load() == 0 {
		t.Fatal("native folder and file pagination lost entries", names)
	}
	cycle.Store(true)
	list.Cursor = ""
	var cycleErr error
	for range 3 {
		p, err := c.List(ctx, "u", list)
		if err != nil || p.NextCursor == nil {
			cycleErr = err
			break
		}
		list.Cursor = *p.NextCursor
	}
	cycle.Store(false)
	if core.CodeOf(cycleErr) != core.InvalidUpstreamResponse {
		t.Fatal("cyclic native continuation accepted", cycleErr)
	}
	for name, code := range map[string]core.Code{"Folder": core.PathAmbiguous, "A & %": core.NotFound} {
		if _, err := c.Stat(ctx, "u", core.Path("/remote/"+name)); core.CodeOf(err) != code {
			t.Fatal("path selection accepted ambiguity or changed case", name, err)
		}
	}
	target := core.Path("/remote/a & %")
	stat, err := c.Stat(ctx, "u", target)
	if err != nil || stat.Size != nil || contentCalls.Load() != 0 {
		t.Fatal("metadata access fetched or inferred a representation", stat, err)
	}
	badParent.Store(true)
	if _, err := c.Stat(ctx, "u", target); core.CodeOf(err) != core.InvalidUpstreamResponse {
		t.Fatal("native parent mismatch accepted", err)
	}
	badParent.Store(false)
	opened, err := c.Open(ctx, "u", target, core.OpenOptions{Range: &core.Range{Start: 1, End: 3}})
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(opened.Body)
	if err = errors.Join(err, opened.Body.Close()); err != nil || string(data) != "bcd" ||
		opened.Range == nil || *opened.Range != (core.ContentRange{Start: 1, End: 3, Total: 6}) {
		t.Fatal("delegated bytes or actual range lost", string(data), opened.Range, err)
	}
	for _, name := range []string{"untrusted", "cross"} {
		if _, err := c.Open(ctx, "u", core.Path("/remote/"+name), core.OpenOptions{}); core.CodeOf(err) != core.UpstreamAccessDenied {
			t.Fatal("untrusted recipient accepted", name, err)
		}
	}
	if deniedCalls.Load() != 0 {
		t.Fatal("non-allowed origin contacted")
	}
	opened, err = c.Open(ctx, "u", target, core.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	data, err = io.ReadAll(opened.Body)
	if err = errors.Join(err, opened.Body.Close()); err != nil || string(data) != "abcdef" || opened.Range != nil {
		t.Fatal("complete original", string(data), err)
	}
	wait.Store(true)
	opened, err = c.Open(ctx, "u", target, core.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { _, err := io.ReadAll(opened.Body); done <- err }()
	if err := mount.Retire(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal("retirement did not cancel body", err)
	}
}
