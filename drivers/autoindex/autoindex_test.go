package autoindex

import (
	"context"
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
	"time"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/local"
)

type wire func(*http.Request) (*http.Response, error)

func (w wire) RoundTrip(req *http.Request) (*http.Response, error) { return w(req) }

type failedClose struct {
	io.ReadCloser
	calls int
}

func (b *failedClose) Close() error {
	b.calls++
	if b.calls == 1 {
		return fmt.Errorf("private path: %w", io.EOF)
	}
	return b.ReadCloser.Close()
}

func TestRootedJSONDirectory(t *testing.T) {
	const rootPath = "/%72oot/"
	const directory = rootPath + "dir:one/"
	const name = "%2F+#中?.txt"
	const file = directory + "%252F+%23%E4%B8%AD%3F.txt"
	const entry = `{"name":"%2F+#中?.txt","type":"file","mtime":"Sat, 03 Nov 2001 12:00:00 GMT","size":6}`
	const rootIndex = `[{"name":"dir:one","type":"directory"},{"name":"extra","type":"file","size":0},{"name":"socket","type":"other"}]`
	var mode atomic.Value
	mode.Store("")
	var metadata, contents, outside atomic.Int32
	started := make(chan struct{})
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.RawQuery != "" || r.Header.Get("Authorization") != "Bearer fixture" ||
			r.Header.Get("Accept-Encoding") != "identity" || r.Header.Get("Cookie") != "" {
			t.Error("unexpected request", r.URL, r.Header)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		switch r.URL.EscapedPath() {
		case rootPath, directory:
			metadata.Add(1)
			if r.Header.Get("Accept") != "application/json" || r.Header.Get("Range") != "" {
				t.Error("incorrect directory request", r.Header)
			}
			w.Header().Set("Content-Type", "application/json")
			if r.URL.EscapedPath() == rootPath {
				fmt.Fprint(w, rootIndex)
				return
			}
			switch mode.Load() {
			case "wait":
				close(started)
				<-r.Context().Done()
				return
			case "bad-mime":
				w.Header().Set("Content-Type", "text/html")
			case "bad-name":
				fmt.Fprint(w, `[{"name":"../outside","type":"file","size":1}]`)
				return
			case "non-utf8":
				fmt.Fprint(w, "[{\"name\":\"\xff\",\"type\":\"file\",\"size\":1}]")
				return
			case "duplicate":
				fmt.Fprint(w, "["+entry+","+entry+"]")
				return
			}
			fmt.Fprint(w, "["+entry+"]")
		case file:
			contents.Add(1)
			switch mode.Load() {
			case "redirect":
				w.Header().Set("Location", "/outside")
				w.WriteHeader(http.StatusFound)
				return
			case "unsolicited-range":
				w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
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
				t.Error("incorrect range", r.Header)
				w.WriteHeader(http.StatusBadRequest)
			}
		default:
			outside.Add(1)
			t.Error("unexpected wire path", r.URL.EscapedPath())
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	client := server.Client()
	transport := client.Transport
	var broken *failedClose
	client.Transport = wire(func(r *http.Request) (*http.Response, error) {
		r = r.Clone(r.Context())
		r.Header.Set("Authorization", "Bearer fixture")
		response, err := transport.RoundTrip(r)
		if err == nil && response.StatusCode == http.StatusFound {
			broken = &failedClose{ReadCloser: response.Body}
			response.Body = broken
		}
		return response, err
	})
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	for _, invalid := range []string{server.URL + "/root%2Fother/", server.URL + "/root/%2e%2e/",
		server.URL + "/root/?query", "http://" + strings.TrimPrefix(server.URL, "https://")} {
		if d, err := Open(ctx, Options{RootURL: invalid, Client: client}); d != nil || core.CodeOf(err) != core.InvalidRequest {
			t.Fatal(invalid, d, err)
		}
	}
	options := Options{RootURL: server.URL + strings.TrimSuffix(rootPath, "/"), Client: client}
	bounded := options
	bounded.MaxResponseBytes = 16
	if d, err := Open(ctx, bounded); core.CodeOf(err) != core.ResponseBudgetExceeded || d == nil {
		t.Fatal("response budget", err)
	} else if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	driver, err := Open(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	storage, err := core.New(core.Options{Authorize: func(context.Context, core.Principal, core.Action, core.Address) error { return nil }})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = storage.Close(ctx) }()
	mount, err := storage.Install("/remote", driver)
	if err != nil {
		t.Fatal(err)
	}
	req := core.ListRequest{Query: core.Children(core.Path("/remote")), Limit: 1, CursorTTL: time.Minute}
	first, err := storage.List(ctx, "u", req)
	if err != nil || len(first.Items) != 1 || first.Items[0].Name != "dir:one" || first.Items[0].Size != nil || first.NextCursor == nil {
		t.Fatal(first, err)
	}
	req.Cursor = *first.NextCursor
	if last, err := storage.List(ctx, "u", req); err != nil || len(last.Items) != 1 ||
		last.Items[0].Name != "extra" || last.NextCursor != nil || metadata.Load() != 3 {
		t.Fatal("public page", last, err, metadata.Load())
	}
	target := core.Path("/remote/dir:one/" + name)
	if found, err := storage.Stat(ctx, "u", target); err != nil || found.Size == nil || *found.Size != 6 || found.ModifiedAt == nil ||
		metadata.Load() != 5 || contents.Load() != 0 {
		t.Fatal("native directory metadata", found, err, metadata.Load(), contents.Load())
	}
	if _, err := storage.Open(ctx, "u", target, core.OpenOptions{NoFollow: true}); core.CodeOf(err) != core.UnsupportedOperation ||
		metadata.Load() != 5 {
		t.Fatal("NoFollow was not refused before native access", err)
	}
	for _, tc := range []struct {
		mode string
		path core.Address
		code core.Code
	}{{"", core.Path("/remote/dir:one/" + strings.ToUpper(name)), core.NotFound},
		{"", core.Path("/remote/socket"), core.UnsupportedOperation},
		{"bad-mime", target, core.InvalidUpstreamResponse}, {"bad-name", target, core.UnrepresentableName},
		{"non-utf8", target, core.InvalidUpstreamResponse}, {"duplicate", target, core.InvalidUpstreamResponse}} {
		mode.Store(tc.mode)
		if _, err := storage.Open(ctx, "u", tc.path, core.OpenOptions{}); core.CodeOf(err) != tc.code || contents.Load() != 0 {
			t.Fatal(tc.mode, tc.path, err, contents.Load())
		}
	}
	mode.Store("")
	opened, err := storage.Open(ctx, "u", target, core.OpenOptions{Range: &core.Range{Start: 1, End: 3}})
	if err != nil {
		t.Fatal(err)
	}
	raw, err := io.ReadAll(opened.Body)
	if err := errors.Join(err, opened.Body.Close()); err != nil || string(raw) != "bcd" ||
		opened.Range == nil || opened.Range.Total != 6 {
		t.Fatal(string(raw), opened.Range, err)
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
	if raw, err := os.ReadFile(filepath.Join(destination, "copied")); err != nil || string(raw) != "abcdef" || contents.Load() != 3 {
		t.Fatal(string(raw), err, contents.Load())
	}
	mode.Store("unsolicited-range")
	if _, err := storage.Open(ctx, "u", target, core.OpenOptions{}); core.CodeOf(err) != core.InvalidUpstreamResponse {
		t.Fatal("ordinary read received an unsolicited range failure", err)
	}
	mode.Store("redirect")
	if _, err := storage.Open(ctx, "u", target, core.OpenOptions{}); core.CodeOf(err) != core.InvalidUpstreamResponse ||
		!errors.Is(err, io.ErrUnexpectedEOF) || strings.Contains(err.Error(), "private") || broken == nil || broken.calls != 1 || outside.Load() != 0 {
		t.Fatal("redirect escaped the root or lost cleanup", err, outside.Load())
	}
	mode.Store("wait")
	done := make(chan error, 1)
	go func() { _, err := storage.Stat(ctx, "u", target); done <- err }()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("nested directory request did not start")
	}
	if err := mount.Retire(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-done; !errors.Is(err, context.Canceled) || broken.calls != 2 {
		t.Fatal("retirement did not join and retry cleanup", err, broken.calls)
	}
}
