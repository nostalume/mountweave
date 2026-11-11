package kdocs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"testing/iotest"
	"time"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/local"
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
		return fmt.Errorf("private capability: %w", io.EOF)
	}
	return b.ReadCloser.Close()
}

func TestPersonalFolderAndDownloadThroughCore(t *testing.T) {
	const base = "/api/v1/openapi/personal/files"
	const token = "private token+/%"
	const signature = "sig=private%2fvalue+here"
	const name = "文件 #.pptx"
	var mode atomic.Value
	mode.Store("")
	var bodies, outside atomic.Int32
	started := make(chan struct{})
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/original" {
			outside.Add(1)
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if r.Method != http.MethodGet || r.URL.RawQuery != signature || r.Header.Get("Authorization") != "" ||
			r.Header.Get("Cookie") != "" || r.Header.Get("Referer") != "" || r.Header.Get("Accept-Encoding") != "identity" {
			t.Error("incorrect signed body request")
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		bodies.Add(1)
		switch mode.Load() {
		case "redirect":
			w.Header().Set("Location", "/outside?private")
			w.WriteHeader(http.StatusFound)
			return
		case "short", "extra":
			w.(http.Flusher).Flush()
			payload := "abc"
			if mode.Load() == "extra" {
				payload = "abcdefg"
			}
			fmt.Fprint(w, payload)
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
		case "bytes=7-":
			w.Header().Set("Content-Range", "bytes */6")
			w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
		default:
			t.Error("incorrect original range", r.Header.Get("Range"))
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	defer server.Close()
	type object map[string]any
	entry := func(id, parent, name, kind string) object {
		return object{
			"id": object{"open_id": id, "union_id": "other-" + id}, "parent": object{"open_id": parent},
			"fname": name, "ftype": kind, "mtime": 1655900501, "fsize": 6,
		}
	}
	reply := func(data object) *http.Response {
		raw, _ := json.Marshal(data)
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(string(raw)))}
	}
	lists, downloads, tokens := 0, 0, 0
	timeout := fmt.Errorf("private transport details: %w", context.DeadlineExceeded)
	api := &http.Client{Transport: wire(func(r *http.Request) (*http.Response, error) {
		q := r.URL.Query()
		if r.Method != http.MethodGet || r.URL.Scheme != "https" || r.URL.Host != "developer.kdocs.cn" ||
			q.Get("access_token") != token || r.Header.Get("Authorization") != "" || r.Header.Get("Cookie") != "" ||
			r.Header.Get("Accept") != "application/json" || r.Header.Get("Range") != "" {
			return nil, errors.New("incorrect KDocs control request")
		}
		switch r.URL.Path {
		case base:
			lists++
			switch mode.Load() {
			case "request-timeout":
				return nil, timeout
			case "body-timeout":
				return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(iotest.ErrReader(timeout))}, nil
			}
			items, offset, filter := []object{}, -1, "file"
			switch q.Get("parent_id") {
			case "":
				items = append(items, entry("vault", "mydocs", "Folder", "folder"))
			case "vault":
				switch q.Get("offset") + "/" + q.Get("filter") {
				case "/":
					items, offset, filter = append(items, entry("team", "vault", "Team", "folder")), 1, "folder"
				case "1/folder":
					offset, filter = 0, "file"
				case "0/file":
					items = append(items, entry("readme", "vault", "readme.txt", "file"))
					if mode.Load() == "cycle" {
						offset, filter = 1, "folder"
					}
				default:
					return nil, errors.New("KDocs cursor pair was altered")
				}
			case "team":
				parent := "team"
				if mode.Load() == "outside-parent" {
					parent = "elsewhere"
				}
				items = append(items, entry("file-id", parent, name, "file"))
				if mode.Load() == "duplicate" {
					items = append(items, entry("other-id", "team", name, "file"))
				}
			default:
				return nil, errors.New("wrong KDocs parent ID")
			}
			return reply(object{"code": 0, "data": object{"files": items, "next_offset": offset, "next_filter": filter}}), nil
		case base + "/file-id/download":
			downloads++
			switch mode.Load() {
			case "online":
				return reply(object{"code": 30006, "message": token}), nil
			case "expired":
				return reply(object{"code": 20008, "message": token}), nil
			}
			size := 6
			if mode.Load() == "size" {
				size = 7
			}
			return reply(object{"code": 0, "data": object{
				"fsize": size, "url": server.URL + "/original?" + signature, "checksums": object{"sha1": "provider-checksum"},
			}}), nil
		default:
			return nil, errors.New("unexpected KDocs endpoint")
		}
	})}
	transport := server.Client().Transport
	var broken *failedClose
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
	opts := Options{RootPath: "/Folder", Client: api, ContentClient: api,
		AccessToken: func(context.Context) (string, error) { tokens++; return token, nil }}
	if d, err := Open(ctx, opts); d != nil || core.CodeOf(err) != core.InvalidRequest || tokens != 0 {
		t.Fatal("control client was accepted for delegated content", err, tokens)
	}
	opts.ContentClient = content
	d, err := Open(ctx, opts)
	if err != nil || lists != 1 || tokens != 1 {
		t.Fatal("root did not bind the listed folder directly", err, lists, tokens)
	}
	storage, err := core.New(core.Options{Authorize: func(_ context.Context, _ core.Principal, _ core.Action, a core.Address) error {
		if a.Value() == "/docs/denied" {
			return core.Fail(core.PermissionDenied, nil)
		}
		return nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = storage.Close(ctx) }()
	mount, err := storage.Install("/docs", d)
	if err != nil {
		t.Fatal(err)
	}
	for _, state := range []string{"request-timeout", "body-timeout"} {
		mode.Store(state)
		_, err := storage.Stat(ctx, "u", core.Path("/docs/Team"))
		if core.CodeOf(err) != core.UpstreamTimeout || !errors.Is(err, context.DeadlineExceeded) ||
			strings.Contains(err.Error(), token) || strings.Contains(err.Error(), "private transport details") {
			t.Fatal("HTTP timeout was misclassified or exposed private details", state, err)
		}
	}
	for _, state := range []string{"", "cycle"} {
		mode.Store(state)
		req := core.ListRequest{Query: core.Children(core.Path("/docs")), Limit: 1, CursorTTL: time.Minute}
		for i, expected := range []string{"Team", "", "readme.txt"} {
			page, err := storage.List(ctx, "u", req)
			if state == "cycle" && i == 2 {
				if core.CodeOf(err) != core.InvalidUpstreamResponse {
					t.Fatal("cyclic KDocs cursor was accepted", page, err)
				}
				break
			}
			if err != nil || (page.NextCursor == nil) != (i == 2) ||
				expected == "" && len(page.Items) != 0 || expected != "" && (len(page.Items) != 1 || page.Items[0].Name != expected) {
				t.Fatal(state, i, page, err)
			}
			if i == 1 {
				before := lists
				if replay, err := storage.List(ctx, "u", req); err != nil || len(replay.Items) != 0 || lists != before {
					t.Fatal("cursor replay triggered native I/O", replay, err, lists)
				}
			}
			if page.NextCursor != nil {
				req.Cursor = *page.NextCursor
			}
		}
	}
	target := core.Path("/docs/Team/" + name)
	for _, tc := range []struct {
		state string
		path  core.Address
		code  core.Code
	}{{"", core.Path("/docs/denied"), core.PermissionDenied}, {"", core.Path("/docs/team/" + name), core.NotFound},
		{"outside-parent", target, core.InvalidUpstreamResponse}, {"duplicate", target, core.PathAmbiguous},
		{"online", target, core.UnsupportedOperation}, {"expired", target, core.UpstreamAccessDenied}} {
		mode.Store(tc.state)
		if _, err := storage.Open(ctx, "u", tc.path, core.OpenOptions{}); core.CodeOf(err) != tc.code ||
			strings.Contains(err.Error(), token) || bodies.Load() != 0 {
			t.Fatal(tc, err, bodies.Load())
		}
	}
	mode.Store("")
	before := lists
	if stat, err := storage.Stat(ctx, "u", target); err != nil || stat.Size == nil || *stat.Size != 6 || stat.ModifiedAt == nil ||
		lists-before != 4 || downloads != 2 {
		t.Fatal(stat, err, lists-before, downloads)
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
	destination := t.TempDir()
	localDriver, err := local.New(destination)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := storage.Install("/local", localDriver); err != nil {
		t.Fatal(err)
	}
	if change, err := storage.Copy(ctx, "u", target, core.Path("/local/copied")); err != nil || change.Effect != core.Applied {
		t.Fatal(change, err)
	}
	if data, err := os.ReadFile(filepath.Join(destination, "copied")); err != nil || string(data) != "abcdef" || bodies.Load() != 3 {
		t.Fatal(string(data), err, bodies.Load())
	}
	mode.Store("size")
	if _, err := storage.Open(ctx, "u", target, core.OpenOptions{}); core.CodeOf(err) != core.InvalidUpstreamResponse {
		t.Fatal("inconsistent download size admitted", err)
	}
	for _, state := range []string{"short", "extra"} {
		mode.Store(state)
		change, err := storage.Copy(ctx, "u", target, core.Path("/local/"+state))
		if err == nil || change.Effect == core.Applied ||
			state == "short" && !errors.Is(err, io.ErrUnexpectedEOF) ||
			state == "extra" && core.CodeOf(err) != core.InvalidUpstreamResponse {
			t.Fatal("unframed download disagreed with its receipt", state, change, err)
		}
	}
	mode.Store("redirect")
	if _, err := storage.Open(ctx, "u", target, core.OpenOptions{}); core.CodeOf(err) != core.InvalidUpstreamResponse ||
		!errors.Is(err, io.ErrUnexpectedEOF) || strings.Contains(err.Error(), "private") || broken == nil || broken.calls != 1 || outside.Load() != 0 {
		t.Fatal("redirect followed or failed close lost ownership", err, outside.Load())
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
	if err := <-done; !errors.Is(err, context.Canceled) || broken.calls != 2 || tokens != lists+downloads {
		t.Fatal("retirement failed to cancel body and retry close", err, broken.calls, tokens, lists+downloads)
	}
}
