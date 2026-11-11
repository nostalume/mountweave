package urltree

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
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
		return fmt.Errorf("private signed query: %w", io.EOF)
	}
	return b.ReadCloser.Close()
}

func TestConfiguredHTTPSFilesThroughCore(t *testing.T) {
	const rawQuery = "sig=private%2fvalue+here&sig=second"
	var mode atomic.Value
	mode.Store("")
	var requests, outside atomic.Int32
	started := make(chan struct{})
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.EscapedPath() != "/%6fbject" || r.URL.RawQuery != rawQuery ||
			r.Header.Get("Authorization") != "" || r.Header.Get("Cookie") != "" ||
			r.Header.Get("Referer") != "" || r.Header.Get("Accept-Encoding") != "identity" {
			outside.Add(1)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		requests.Add(1)
		switch mode.Load() {
		case "redirect":
			w.Header().Set("Location", "/outside?secret=private")
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
			fmt.Fprint(w, "abcdef")
		case "bytes=1-3":
			w.Header().Set("Content-Range", "bytes 1-3/6")
			w.WriteHeader(http.StatusPartialContent)
			fmt.Fprint(w, "bcd")
		case "bytes=7-":
			w.Header().Set("Content-Range", "bytes */6")
			w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
		default:
			t.Error("incorrect wire range", r.Header.Get("Range"))
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	defer server.Close()
	targetURL := server.URL + "/%6fbject?" + rawQuery
	firstFile := File{Path: "/Team/report.txt", URL: targetURL}
	for _, files := range [][]File{
		{firstFile, firstFile},
		{firstFile, {Path: "/Team", URL: targetURL}},
		{{Path: "/Team", URL: targetURL}, firstFile},
		{{Path: "/wrong", URL: "http://host/path"}},
		{{Path: "/wrong", URL: "https://account:private@host/path"}},
		{{Path: "/wrong", URL: targetURL + "#"}},
	} {
		if source, err := New(Options{Files: files}); source != nil || core.CodeOf(err) != core.InvalidRequest ||
			strings.Contains(err.Error(), "private") {
			t.Fatal("unsafe mapping was admitted or its URL was exposed", err)
		}
	}
	transport := server.Client().Transport
	var broken *failedClose
	files := []File{firstFile, {Path: "/Team/Report.txt", URL: targetURL}}
	source, err := New(Options{Files: files, Transport: wire(func(r *http.Request) (*http.Response, error) {
		if mode.Load() == "transport" {
			return nil, fmt.Errorf("cannot fetch %s", r.URL)
		}
		response, err := transport.RoundTrip(r)
		if err == nil && response.StatusCode == http.StatusFound {
			broken = &failedClose{ReadCloser: response.Body}
			response.Body = broken
		}
		return response, err
	})})
	if err != nil {
		t.Fatal(err)
	}
	files[0] = File{}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	iter, err := source.Enumerate(ctx, core.NativeChildren(core.NativePath("/Team")))
	if err != nil {
		t.Fatal(err)
	}
	batch, err := iter.Next(ctx, 1)
	if err != nil || len(batch) != 1 {
		t.Fatal(batch, err)
	}
	batch[0].Name = "changed"
	if err := iter.Close(); err != nil {
		t.Fatal(err)
	}
	storage, err := core.New(core.Options{Authorize: func(_ context.Context, _ core.Principal, action core.Action, a core.Address) error {
		if action == core.ReadAction && a.Value() == "/urls/Team/Report.txt" {
			return core.Fail(core.PermissionDenied, nil)
		}
		return nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = storage.Close(ctx) }()
	mount, err := storage.Install("/urls", source)
	if err != nil {
		t.Fatal(err)
	}
	target := core.Path("/urls/Team/report.txt")
	if item, err := storage.Stat(ctx, "u", target); err != nil || item.Kind != core.File || item.Size != nil || item.ModifiedAt != nil {
		t.Fatal(item, err)
	}
	if _, err := storage.Stat(ctx, "u", core.Path("/urls/Team/report.txt/deeper")); core.CodeOf(err) != core.NotDirectory {
		t.Fatal("regular file accepted as a parent", err)
	}
	req := core.ListRequest{Query: core.Children(core.Path("/urls/Team")), Limit: 1, CursorTTL: time.Minute}
	first, err := storage.List(ctx, "u", req)
	if err != nil || len(first.Items) != 1 || first.Items[0].Name != "Report.txt" || first.NextCursor == nil {
		t.Fatal(first, err)
	}
	req.Cursor = *first.NextCursor
	if page, err := storage.List(ctx, "u", req); err != nil || len(page.Items) != 1 ||
		page.Items[0].Name != "report.txt" || page.NextCursor != nil {
		t.Fatal(page, err)
	}
	if _, err := storage.Open(ctx, "u", core.Path("/urls/team/report.txt"), core.OpenOptions{}); core.CodeOf(err) != core.NotFound {
		t.Fatal("case-insensitive path admitted", err)
	}
	if _, err := storage.Open(ctx, "u", core.Path("/urls/Team/Report.txt"), core.OpenOptions{}); core.CodeOf(err) != core.PermissionDenied ||
		requests.Load() != 0 || outside.Load() != 0 {
		t.Fatal("metadata or rejected open sent a request", err, requests.Load(), outside.Load())
	}
	opened, err := storage.Open(ctx, "u", target, core.OpenOptions{NoFollow: true, Range: &core.Range{Start: 1, End: 3}})
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
	if err = errors.Join(err, opened.Body.Close()); err != nil || string(data) != "abcdef" || opened.Range != nil || requests.Load() != 3 {
		t.Fatal("complete original", string(data), err)
	}
	mode.Store("transport")
	if _, err := storage.Open(ctx, "u", target, core.OpenOptions{}); core.CodeOf(err) != core.UpstreamFailure ||
		strings.Contains(err.Error(), "private") || requests.Load() != 3 {
		t.Fatal("transport diagnostic leaked the signed query", err, requests.Load())
	}
	mode.Store("redirect")
	if _, err := storage.Open(ctx, "u", target, core.OpenOptions{}); core.CodeOf(err) != core.InvalidUpstreamResponse ||
		!errors.Is(err, io.ErrUnexpectedEOF) || strings.Contains(err.Error(), "private") || broken == nil || broken.calls != 1 || outside.Load() != 0 {
		t.Fatal("redirect followed or close failed without retaining ownership", err, outside.Load())
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
	if err := <-done; !errors.Is(err, context.Canceled) || broken.calls != 2 || requests.Load() != 5 {
		t.Fatal("retirement failed to cancel the body and retry cleanup", err, broken.calls, requests.Load())
	}
}
