package ipfs

import (
	"context"
	"encoding/json"
	"errors"
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

func TestImmutableUnixFSThroughCore(t *testing.T) {
	const root, dir, file, empty, symlink = "QmRoot", "bafyTeam", "bafyReport", "bafyEmpty", "bafyLink"
	const configuredRoot = "zAlternateRootEncoding"
	const original = "abcdef"
	var mode atomic.Value
	mode.Store("")
	var lists, cats, unexpected atomic.Int32
	started := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.Header.Get("Accept-Encoding") != "identity" {
			unexpected.Add(1)
			http.Error(w, "wrong RPC request", http.StatusBadRequest)
			return
		}
		q, state := r.URL.Query(), mode.Load().(string)
		arg := q.Get("arg")
		switch r.URL.Path {
		case "/proxy/api/v0/files/stat":
			hash, kind := root, "directory"
			if arg != "/ipfs/"+configuredRoot {
				hash, kind = strings.TrimPrefix(arg, "/ipfs/"), "file"
			}
			_ = json.NewEncoder(w).Encode(map[string]string{"Hash": hash, "Type": kind})
		case "/proxy/api/v0/ls":
			lists.Add(1)
			if q.Get("resolve-type") != "true" || q.Get("size") != "true" || q.Get("stream") != "false" || q.Get("enc") != "json" {
				unexpected.Add(1)
			}
			if state == "budget" {
				_, _ = io.WriteString(w, strings.Repeat(" ", 513))
				return
			}
			entry := func(cid, name string, kind, size int) map[string]any {
				return map[string]any{"Hash": cid, "Name": name, "Type": kind, "Size": size}
			}
			var links []map[string]any
			switch arg {
			case configuredRoot:
				links = append(links, entry(dir, "Team", unixDirectory, 0), entry(symlink, "Link", unixSymlink, 12))
			case dir:
				kind := unixFile
				if state == "unknown-link" {
					kind = 0
				}
				links = append(links, entry(file, "Report 版", kind, len(original)), entry(empty, "Empty", unixFile, 0))
			default:
				unexpected.Add(1)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"Objects": []any{map[string]any{"Hash": arg, "Links": links}}})
		case "/proxy/api/v0/cat":
			cats.Add(1)
			var data string
			switch arg {
			case "/ipfs/" + file:
				data = original
			case "/ipfs/" + empty:
			default:
				unexpected.Add(1)
			}
			if q.Has("offset") || q.Has("length") {
				if q.Get("offset") != "1" || q.Get("length") != "3" {
					unexpected.Add(1)
				}
				data = data[1:4]
			}
			if state == "redirect" {
				w.Header().Set("Location", "/unexpected")
				w.WriteHeader(http.StatusFound)
				return
			}
			w.Header().Set("X-Content-Length", strconv.Itoa(len(data)))
			w.Header().Set("Trailer", "X-Stream-Error")
			w.WriteHeader(http.StatusOK)
			w.(http.Flusher).Flush()
			if state == "wait" {
				started <- struct{}{}
				<-r.Context().Done()
				return
			}
			if state == "overlong" {
				data += "!"
			}
			_, _ = io.WriteString(w, data)
			if state == "late-failure" {
				w.Header().Set("X-Stream-Error", "late UnixFS block failure")
			}
		default:
			unexpected.Add(1)
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	o := Options{Endpoint: server.URL + "/proxy", RootCID: configuredRoot, Client: server.Client(), MaxResponseBytes: 512}
	d, err := Open(ctx, o)
	if err != nil {
		t.Fatal(err)
	}
	o.RootCID = file
	wrong, err := Open(ctx, o)
	if core.CodeOf(err) != core.NotDirectory || wrong == nil {
		t.Fatal("a file was admitted as a directory root", err)
	}
	if err := wrong.Close(); err != nil {
		t.Fatal(err)
	}
	s, err := core.New(core.Options{Authorize: func(context.Context, core.Principal, core.Action, core.Address) error { return nil }})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close(ctx) }()
	mount, err := s.Install("/ipfs", d)
	if err != nil {
		t.Fatal(err)
	}
	q := core.ListRequest{Query: core.Children(core.Path("/ipfs")), Limit: 1, CursorTTL: time.Minute}
	first, err := s.List(ctx, "u", q)
	if err != nil || len(first.Items) != 1 || first.Items[0].Name != "Team" || first.NextCursor == nil {
		t.Fatal(first, err)
	}
	q.Cursor = *first.NextCursor
	before := lists.Load()
	for range 2 {
		last, err := s.List(ctx, "u", q)
		if err != nil || len(last.Items) != 1 || last.Items[0].Name != "Link" || last.Items[0].Size != nil || last.NextCursor != nil || lists.Load() != before {
			t.Fatal("cursor did not reuse the native array", last, err)
		}
	}
	target, emptyPath := core.Path("/ipfs/Team/Report 版"), core.Path("/ipfs/Team/Empty")
	for _, check := range []struct {
		state, path string
		code        core.Code
	}{{"", "/ipfs/Team/report 版", core.NotFound}, {"", "/ipfs/Link", core.UnsupportedOperation},
		{"budget", target.Value(), core.ResponseBudgetExceeded}, {"unknown-link", target.Value(), core.InvalidUpstreamResponse}} {
		mode.Store(check.state)
		if _, err := s.Open(ctx, "u", core.Path(check.path), core.OpenOptions{}); core.CodeOf(err) != check.code || cats.Load() != 0 {
			t.Fatal(check, err)
		}
	}
	mode.Store("")
	if metadata, err := s.Stat(ctx, "u", target); err != nil || metadata.Size == nil || *metadata.Size != 6 {
		t.Fatal(metadata, err)
	}
	opened, err := s.Open(ctx, "u", target, core.OpenOptions{NoFollow: true, Range: &core.Range{Start: 1, End: 3}})
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(opened.Body)
	if err := errors.Join(err, opened.Body.Close()); err != nil || string(data) != "bcd" || opened.Range == nil || opened.Range.Total != 6 {
		t.Fatal(string(data), opened.Range, err)
	}
	opened, err = s.Open(ctx, "u", target, core.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	data, err = io.ReadAll(opened.Body)
	if err = errors.Join(err, opened.Body.Close()); err != nil || string(data) != original || opened.Range != nil {
		t.Fatalf("full original: %q, range=%v, error=%v", data, opened.Range, err)
	}
	opened, err = s.Open(ctx, "u", emptyPath, core.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if data, err := io.ReadAll(opened.Body); len(data) != 0 || err != nil {
		t.Fatal(string(data), err)
	}
	for _, check := range []struct {
		state string
		code  core.Code
	}{{"late-failure", core.UpstreamFailure}, {"overlong", core.InvalidUpstreamResponse}} {
		mode.Store(check.state)
		opened, err := s.Open(ctx, "u", target, core.OpenOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := io.ReadAll(opened.Body); core.CodeOf(err) != check.code {
			t.Fatal("native terminal failure was lost", check, err)
		}
	}
	mode.Store("late-failure")
	if _, err := s.Open(ctx, "u", emptyPath, core.OpenOptions{}); core.CodeOf(err) != core.UpstreamFailure || strings.Contains(err.Error(), "late UnixFS block failure") {
		t.Fatal("empty stream's trailer was not observed", err)
	}
	mode.Store("redirect")
	if _, err := s.Open(ctx, "u", target, core.OpenOptions{}); err == nil || unexpected.Load() != 0 {
		t.Fatal("redirect reached an unexpected receiver", err, unexpected.Load())
	}
	mode.Store("wait")
	opened, err = s.Open(ctx, "u", target, core.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("cat did not begin")
	}
	done := make(chan error, 1)
	go func() { _, err := io.ReadAll(opened.Body); done <- err }()
	if err := mount.Retire(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal("retirement did not cancel native body", err)
	}
}
