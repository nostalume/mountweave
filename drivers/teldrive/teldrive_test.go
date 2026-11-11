package teldrive

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	core "github.com/nostalume/mountweave"
)

type wire func(*http.Request) (*http.Response, error)

func (w wire) RoundTrip(r *http.Request) (*http.Response, error) { return w(r) }

type failedClose struct {
	io.ReadCloser
	calls int
}

func (b *failedClose) Close() error {
	b.calls++
	if b.calls == 1 {
		return fmt.Errorf("private transport diagnostic: %w", io.EOF)
	}
	return b.ReadCloser.Close()
}

func TestNativeFolderAndContentThroughCore(t *testing.T) {
	const base = "/api/v1/files"
	const root = "01900000-0000-7000-8000-000000000001"
	const folder = "01900000-0000-7000-8000-000000000002"
	const file = "01900000-0000-7000-8000-000000000003"
	const extra = "01900000-0000-7000-8000-000000000004"
	const name = "文%2F#?.txt"
	const route = base + "/" + file + "/content/%E6%96%87%252F%23%3F.txt"
	const date = "2026-01-02T03:04:05Z"
	const firstCursor, finalCursor = "cursor+first/==", "cursor+last/=="
	type native map[string]any
	entry := func(id, parent, name, kind string) native {
		f := native{"id": id, "name": name, "kind": kind, "status": "active", "modTime": date}
		if parent != "" {
			f["parentId"] = parent
		}
		if kind == "file" {
			f["size"] = 6
		}
		return f
	}
	var mode atomic.Value
	mode.Store("")
	var lists, details, bodies, unexpected atomic.Int32
	started := make(chan struct{})
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.Header.Get("X-Api-Key") != "fixture-key" || r.Header.Get("Cookie") != "" ||
			r.Header.Get("Referer") != "" || r.Header.Get("Accept-Encoding") != "identity" {
			t.Error("unexpected native receiver or credentials")
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		state := mode.Load().(string)
		writeJSON := func(body any) {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(body)
		}
		switch r.URL.EscapedPath() {
		case base + "/" + root:
			details.Add(1)
			id := root
			if state == "wrong-root" {
				id = folder
			}
			writeJSON(entry(id, "", "Album", "folder"))
		case base:
			lists.Add(1)
			q := r.URL.Query()
			n, err := strconv.Atoi(q.Get("limit"))
			if err != nil || n < 1 || n > 500 || q.Get("scope") != "folder" || q.Get("status") != "active" ||
				q.Has("path") || q.Has("search") || r.Header.Get("Accept") != "application/json" || r.Header.Get("Range") != "" {
				t.Error("wrong folder list request", r.URL)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			page := []native{}
			var next string
			switch q.Get("parentId") {
			case "":
				page = append(page, entry(root, "", "Album", "folder"))
			case root:
				folderEntry, other := entry(folder, root, "Team", "folder"), entry(extra, root, "readme.txt", "file")
				switch q.Get("cursor") {
				case "":
					page = append(page, folderEntry, other)
				case firstCursor:
					page = append(page, other)
				case finalCursor:
					if state == "cycle" {
						page = append(page, entry("01900000-0000-7000-8000-000000000005", root, "x.txt", "file"))
					}
				default:
					t.Error("cursor was changed", r.URL.RawQuery)
				}
			case folder:
				f := entry(file, folder, name, "file")
				switch state {
				case "wrong-parent":
					f["parentId"] = root
				case "trashed":
					f["status"] = "trashed"
				}
				page = append(page, f)
				if state == "duplicate" {
					page = append(page, entry(extra, folder, name, "file"))
				}
			default:
				t.Error("unexpected parent ID", r.URL)
			}
			page = page[:min(n, len(page))]
			if len(page) == n {
				next = firstCursor
				if q.Get("cursor") == firstCursor {
					next = finalCursor
				}
			}
			body := native{"items": page}
			if next != "" {
				body["nextCursor"] = next
			}
			writeJSON(body)
		case route:
			bodies.Add(1)
			if r.URL.RawQuery != "" {
				t.Error("unexpected content query", r.URL.RawQuery)
			}
			switch state {
			case "redirect":
				w.Header().Set("Location", "/private?value=secret")
				w.WriteHeader(http.StatusFound)
				return
			case "wait":
				w.Header().Set("Content-Length", "6")
				w.WriteHeader(http.StatusOK)
				w.(http.Flusher).Flush()
				close(started)
				<-r.Context().Done()
				return
			}
			switch r.Header.Get("Range") {
			case "":
				w.Header().Set("Content-Length", "6")
				fmt.Fprint(w, "abcdef")
			case "bytes=1-3":
				w.Header().Set("Content-Length", "3")
				w.Header().Set("Content-Range", "bytes 1-3/6")
				w.WriteHeader(http.StatusPartialContent)
				fmt.Fprint(w, "bcd")
			case "bytes=7-":
				w.Header().Set("Content-Range", "bytes */6")
				w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
			default:
				t.Error("unexpected range", r.Header.Get("Range"))
				w.WriteHeader(http.StatusBadRequest)
			}
		default:
			unexpected.Add(1)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	transport := server.Client().Transport
	var broken *failedClose
	client := &http.Client{Transport: wire(func(r *http.Request) (*http.Response, error) {
		r = r.Clone(r.Context())
		r.Header.Set("X-Api-Key", "fixture-key")
		response, err := transport.RoundTrip(r)
		if err == nil && response.StatusCode == http.StatusFound {
			broken = &failedClose{ReadCloser: response.Body}
			response.Body = broken
		}
		return response, err
	})}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	options := Options{Endpoint: server.URL, Client: client, RootPath: "/Album"}
	byID := Options{Endpoint: server.URL, Client: client, RootID: root}
	mode.Store("wrong-root")
	if driver, err := Open(ctx, byID); driver == nil || core.CodeOf(err) != core.InvalidUpstreamResponse {
		t.Fatal("different root identity accepted", err)
	} else if err := driver.Close(); err != nil {
		t.Fatal(err)
	}
	mode.Store("")
	driver, err := Open(ctx, options)
	if err != nil || details.Load() != 1 || lists.Load() != 1 {
		t.Fatal("path root did not bind the listed folder directly", err, details.Load(), lists.Load())
	}
	storage, err := core.New(core.Options{Authorize: func(_ context.Context, _ core.Principal, _ core.Action, a core.Address) error {
		if a.Value() == "/telegram/denied" {
			return core.Fail(core.PermissionDenied, nil)
		}
		return nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = storage.Close(ctx) }()
	mount, err := storage.Install("/telegram", driver)
	if err != nil {
		t.Fatal(err)
	}
	req := core.ListRequest{Query: core.Children(core.Path("/telegram")), Limit: 1, CursorTTL: time.Minute}
	for _, state := range []string{"", "cycle"} {
		mode.Store(state)
		r := req
		for i, expected := range []string{"Team", "readme.txt", ""} {
			page, err := storage.List(ctx, "u", r)
			if state == "cycle" && i == 2 {
				if core.CodeOf(err) != core.InvalidUpstreamResponse {
					t.Fatal("repeated native cursor accepted", page, err)
				}
				break
			}
			if expected == "" {
				if err != nil || len(page.Items) != 0 || page.NextCursor != nil {
					t.Fatal("invalid terminal empty page", page, err)
				}
				before := lists.Load()
				if replay, err := storage.List(ctx, "u", r); err != nil || replay.NextCursor != nil || lists.Load() != before {
					t.Fatal("public replay fetched another native page", replay, err)
				}
				break
			}
			if err != nil || len(page.Items) != 1 || page.Items[0].Name != expected || page.NextCursor == nil {
				t.Fatal(page, err)
			}
			r.Cursor = *page.NextCursor
		}
	}
	target := core.Path("/telegram/Team/" + name)
	for _, tc := range []struct {
		state string
		path  core.Address
		code  core.Code
	}{{"", core.Path("/telegram/denied"), core.PermissionDenied}, {"", core.Path("/telegram/team/" + name), core.NotFound},
		{"wrong-parent", target, core.InvalidUpstreamResponse}, {"trashed", target, core.InvalidUpstreamResponse},
		{"duplicate", target, core.PathAmbiguous}} {
		mode.Store(tc.state)
		if _, err := storage.Open(ctx, "u", tc.path, core.OpenOptions{}); core.CodeOf(err) != tc.code || bodies.Load() != 0 {
			t.Fatal(tc, err, bodies.Load())
		}
	}
	mode.Store("")
	before := lists.Load()
	if item, err := storage.Stat(ctx, "u", target); err != nil || item.Size == nil || *item.Size != 6 || item.ModifiedAt == nil ||
		lists.Load()-before != 2 || details.Load() != 1 {
		t.Fatal(item, err, lists.Load()-before, details.Load())
	}
	opened, err := storage.Open(ctx, "u", target, core.OpenOptions{Range: &core.Range{Start: 1, End: 3}})
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(opened.Body)
	if err := errors.Join(err, opened.Body.Close()); err != nil || string(data) != "bcd" || opened.Range == nil || opened.Range.Total != 6 {
		t.Fatal(string(data), opened.Range, err)
	}
	if _, err := storage.Open(ctx, "u", target, core.OpenOptions{Range: &core.Range{Start: 7, End: -1}}); core.CodeOf(err) != core.RangeNotSatisfiable {
		t.Fatal(err)
	}
	opened, err = storage.Open(ctx, "u", target, core.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	data, err = io.ReadAll(opened.Body)
	if err = errors.Join(err, opened.Body.Close()); err != nil || string(data) != "abcdef" || opened.Range != nil || bodies.Load() != 3 {
		t.Fatal("complete original", string(data), err)
	}
	mode.Store("redirect")
	if _, err := storage.Open(ctx, "u", target, core.OpenOptions{}); core.CodeOf(err) != core.InvalidUpstreamResponse ||
		!errors.Is(err, io.ErrUnexpectedEOF) || strings.Contains(err.Error(), "private") || broken == nil || broken.calls != 1 || unexpected.Load() != 0 {
		t.Fatal("redirect followed or failed close lost ownership", err, unexpected.Load())
	}
	mode.Store("wait")
	opened, err = storage.Open(ctx, "u", target, core.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("body did not start")
	}
	done := make(chan error, 1)
	go func() { _, err := io.ReadAll(opened.Body); done <- err }()
	if err := mount.Retire(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-done; !errors.Is(err, context.Canceled) || broken.calls != 2 {
		t.Fatal("retirement failed to cancel body and retry close", err, broken.calls)
	}
}
