package imgbed

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
		return fmt.Errorf("private URL: %w", io.EOF)
	}
	return b.ReadCloser.Close()
}

func TestImgBedIndexedDirectoryAndOriginalBody(t *testing.T) {
	const name = "文%2F#?.bin"
	const album, team = "Album/", "Album/Team/"
	const escaped = "/file/Album/Team/%E6%96%87%252F%23%3F.bin"
	var mode atomic.Value
	mode.Store("")
	var lists, bodies, outside atomic.Int32
	started := make(chan struct{})
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.Header.Get("Cookie") != "" || r.Header.Get("Referer") != "" ||
			r.Header.Get("Accept-Encoding") != "identity" {
			t.Error("unexpected request headers", r.Header)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		state := mode.Load().(string)
		switch r.URL.EscapedPath() {
		case "/api/manage/list":
			lists.Add(1)
			q := r.URL.Query()
			n, countErr := strconv.Atoi(q.Get("count"))
			start, startErr := strconv.Atoi(q.Get("start"))
			if r.Header.Get("Authorization") != "Bearer list-only" || r.Header.Get("Accept") != "application/json" ||
				r.Header.Get("Range") != "" || len(q) != 4 || q.Get("recursive") != "false" || n <= 0 ||
				countErr != nil || startErr != nil || start < 0 {
				t.Error("wrong list request", r.URL, r.Header)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			keys, directories := []string{}, []string{}
			switch q.Get("dir") {
			case album:
				keys, directories = []string{album + "readme.txt", album + "more.txt"}, []string{team[:len(team)-1]}
				if state == "collision" {
					keys[0] = album + "Team"
				}
			case team:
				keys = []string{team + name}
				if state == "outside-parent" {
					keys[0] = "Album/Other/" + name
				} else if state == "comma" {
					keys[0] = team + "literal,name"
				}
			case "Missing/":
			default:
				t.Error("wrong list directory", r.URL)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			direct, indexed := len(keys), state != "fallback" && !(state == "late-fallback" && start > 0)
			if indexed {
				keys = keys[min(start, len(keys)):min(start+n, len(keys))]
			} else if start == 0 {
				keys, directories, direct = nil, []string{}, 0
			}
			files := make([]map[string]any, 0, len(keys))
			for _, key := range keys {
				files = append(files, map[string]any{"name": key, "metadata": map[string]any{"FileSize": "0.01"}})
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"files": files, "directories": directories, "directFileCount": direct, "directFolderCount": len(directories),
				"totalCount": direct + len(directories)*10, "returnedCount": len(files), "isIndexedResponse": indexed,
			})
		case escaped:
			bodies.Add(1)
			if r.URL.RawQuery != "" || r.Header.Get("Authorization") != "" {
				t.Error("non-original or authenticated content request", r.URL, r.Header)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			switch state {
			case "redirect":
				w.Header().Set("Location", "/outside?private")
				w.WriteHeader(http.StatusFound)
				return
			case "blocked":
				w.WriteHeader(http.StatusForbidden)
				fmt.Fprint(w, "private block response")
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
				fmt.Fprint(w, "abcdef")
			case "bytes=1-3":
				w.Header().Set("Content-Range", "bytes 1-3/6")
				w.WriteHeader(http.StatusPartialContent)
				fmt.Fprint(w, "bcd")
			default:
				t.Error("wrong content range request", r.Header)
				w.WriteHeader(http.StatusBadRequest)
			}
		default:
			outside.Add(1)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	var broken *failedClose
	transport := server.Client().Transport
	api := &http.Client{Transport: wire(func(r *http.Request) (*http.Response, error) {
		r = r.Clone(r.Context())
		r.Header.Set("Authorization", "Bearer list-only")
		return transport.RoundTrip(r)
	})}
	content := &http.Client{Transport: wire(func(r *http.Request) (*http.Response, error) {
		response, err := transport.RoundTrip(r)
		if err == nil && response.StatusCode == http.StatusFound {
			broken = &failedClose{ReadCloser: response.Body}
			response.Body = broken
		}
		return response, err
	})}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	options := Options{Endpoint: server.URL, RootPath: "/Album", APIClient: api, ContentClient: content}
	missing := options
	missing.RootPath = "/Missing"
	if d, err := Open(ctx, missing); d == nil || core.CodeOf(err) != core.NotFound {
		t.Fatal("nonexistent root", err)
	} else if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	d, err := Open(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	storage, err := core.New(core.Options{Authorize: func(_ context.Context, _ core.Principal, _ core.Action, a core.Address) error {
		if a.Value() == "/images/denied" {
			return core.Fail(core.PermissionDenied, nil)
		}
		return nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = storage.Close(ctx) }()
	mount, err := storage.Install("/images", d)
	if err != nil {
		t.Fatal(err)
	}
	list := core.ListRequest{Query: core.Children(core.Path("/images")), Limit: 1, CursorTTL: time.Minute}
	for _, state := range []string{"", "fallback", "late-fallback"} {
		mode.Store(state)
		req, before := list, lists.Load()
		for i, expected := range []string{"Team", "readme.txt", "more.txt"} {
			page, err := storage.List(ctx, "u", req)
			if state == "fallback" || state == "late-fallback" && i == 2 {
				if core.CodeOf(err) != core.InvalidUpstreamResponse {
					t.Fatal("native database fallback was consumed", state, page, err)
				}
				break
			}
			if err != nil || len(page.Items) != 1 || page.Items[0].Name != expected || (page.NextCursor == nil) != (i == 2) {
				t.Fatal(state, i, page, err)
			}
			if i == 2 {
				replayed, err := storage.List(ctx, "u", req)
				if err != nil || len(replayed.Items) != 1 || replayed.Items[0].Name != expected {
					t.Fatal("cursor replay", replayed, err)
				}
			} else {
				req.Cursor = *page.NextCursor
			}
		}
		expected := int32(2)
		if state == "fallback" {
			expected = 1
		}
		if lists.Load() != before+expected {
			t.Fatal("directory repeats or replay caused extra requests", state, lists.Load()-before)
		}
	}
	target := core.Path("/images/Team/" + name)
	for _, tc := range []struct {
		state string
		path  core.Address
		code  core.Code
	}{{"", core.Path("/images/denied"), core.PermissionDenied}, {"", core.Path("/images/team/" + name), core.NotFound},
		{"outside-parent", target, core.InvalidUpstreamResponse}, {"collision", target, core.PathAmbiguous},
		{"comma", target, core.UnrepresentableName}} {
		mode.Store(tc.state)
		if _, err := storage.Open(ctx, "u", tc.path, core.OpenOptions{}); core.CodeOf(err) != tc.code || bodies.Load() != 0 {
			t.Fatal(tc, err, bodies.Load())
		}
	}
	mode.Store("")
	before := lists.Load()
	if stat, err := storage.Stat(ctx, "u", target); err != nil || stat.Size != nil || stat.Kind != core.File || lists.Load()-before != 2 {
		t.Fatal(stat, err, lists.Load()-before)
	}
	opened, err := storage.Open(ctx, "u", target, core.OpenOptions{Range: &core.Range{Start: 1, End: 3}})
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(opened.Body)
	if err := errors.Join(err, opened.Body.Close()); err != nil || string(data) != "bcd" || opened.Range == nil || opened.Range.Total != 6 {
		t.Fatal(string(data), opened.Range, err)
	}
	opened, err = storage.Open(ctx, "u", target, core.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	data, err = io.ReadAll(opened.Body)
	if err = errors.Join(err, opened.Body.Close()); err != nil || string(data) != "abcdef" || opened.Range != nil || bodies.Load() != 2 {
		t.Fatalf("full original: %q, range=%v, error=%v", data, opened.Range, err)
	}
	mode.Store("blocked")
	if _, err := storage.Open(ctx, "u", target, core.OpenOptions{}); core.CodeOf(err) != core.UpstreamAccessDenied ||
		strings.Contains(err.Error(), "private") {
		t.Fatal("blocked placeholder was admitted as original content", err)
	}
	mode.Store("redirect")
	if _, err := storage.Open(ctx, "u", target, core.OpenOptions{}); core.CodeOf(err) != core.InvalidUpstreamResponse ||
		!errors.Is(err, io.ErrUnexpectedEOF) || strings.Contains(err.Error(), "private") || broken == nil || broken.calls != 1 || outside.Load() != 0 {
		t.Fatal("redirect followed or close failure lost ownership", err, outside.Load())
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
