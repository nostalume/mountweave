package emby

import (
	"context"
	"encoding/json"
	"errors"
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

func (f wire) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestUserScopedOriginalsThroughCore(t *testing.T) {
	const prefix = "/gateway/emby"
	const user = "/Users/media-user/Items"
	const original = "abcdef"
	const name = "Report 版"
	const token = `MediaBrowser Token="sealed-user-token"`
	var mode atomic.Value
	mode.Store("")
	var pages, downloads, unexpected, foreignCalls atomic.Int32
	started := make(chan struct{}, 1)
	foreign := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		foreignCalls.Add(1)
		w.WriteHeader(http.StatusForbidden)
	}))
	defer foreign.Close()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.Header.Get("Authorization") != token || r.Header.Get("Accept-Encoding") != "identity" {
			unexpected.Add(1)
			http.Error(w, "wrong authenticated request", http.StatusBadRequest)
			return
		}
		state := mode.Load().(string)
		row := func(id, parent, name, location string, folder bool) map[string]any {
			return map[string]any{"Id": id, "ParentId": parent, "Name": name, "LocationType": location, "IsFolder": folder}
		}
		switch r.URL.Path {
		case prefix + user + "/root", prefix + user + "/not-folder":
			id := strings.TrimPrefix(r.URL.Path, prefix+user+"/")
			_ = json.NewEncoder(w).Encode(row(id, "", "Library", "FileSystem", id == "root"))
		case prefix + user:
			pages.Add(1)
			q := r.URL.Query()
			if q.Get("Recursive") != "false" || q.Get("Fields") != "ParentId" || q.Get("Limit") != "100" ||
				q.Get("EnableTotalRecordCount") != "true" || q.Get("SortBy") != "SortName" || q.Get("SortOrder") != "Ascending" {
				unexpected.Add(1)
			}
			parent, start := q.Get("ParentId"), q.Get("StartIndex")
			items, total := []map[string]any{}, 0
			switch parent + ":" + start {
			case "root:0":
				if state == "root-fallback" {
					parent = "user-root"
				}
				items, total = append(items, row("team", parent, "Team", "FileSystem", true), row("virtual", parent, "Virtual", "Virtual", false)), 3
			case "root:2":
				items, total = append(items, row("other", parent, "Other", "FileSystem", true)), 3
			case "team:0":
				items, total = append(items, row("media", parent, name, "FileSystem", false), row("remote", parent, "Remote", "Remote", false)), 2
				if state == "duplicate" || state == "missing-page" {
					total++
				}
			case "team:2":
				total = 3
				if state == "duplicate" {
					items = append(items, row("other-media", parent, name, "FileSystem", false))
				}
			default:
				unexpected.Add(1)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"Items": items, "TotalRecordCount": total})
		case prefix + "/Items/media/Download":
			downloads.Add(1)
			if r.URL.RawQuery != "" || r.Header.Get("Accept") == "application/json" {
				unexpected.Add(1)
			}
			switch state {
			case "redirect":
				w.Header().Set("Location", foreign.URL+"/private")
				w.WriteHeader(http.StatusFound)
				return
			case "denied":
				w.WriteHeader(http.StatusForbidden)
				return
			case "wait":
				w.Header().Set("Content-Length", "6")
				w.WriteHeader(http.StatusOK)
				w.(http.Flusher).Flush()
				started <- struct{}{}
				<-r.Context().Done()
				return
			}
			if r.Header.Get("Range") == "bytes=1-3" {
				w.Header().Set("Content-Range", "bytes 1-3/6")
				w.Header().Set("Content-Length", "3")
				w.WriteHeader(http.StatusPartialContent)
				_, _ = io.WriteString(w, original[1:4])
				return
			}
			w.Header().Set("Content-Length", "6")
			_, _ = io.WriteString(w, original)
		default:
			unexpected.Add(1)
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	client := server.Client()
	base := client.Transport
	client.Transport = wire(func(r *http.Request) (*http.Response, error) {
		req := r.Clone(r.Context())
		req.Header.Set("Authorization", token)
		return base.RoundTrip(req)
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	o := Options{Endpoint: server.URL + "/gateway", UserID: "media-user", RootID: "root", Client: client}
	source, err := Open(ctx, o)
	if err != nil {
		t.Fatal(err)
	}
	o.RootID = "not-folder"
	wrong, err := Open(ctx, o)
	if core.CodeOf(err) != core.NotDirectory || wrong == nil {
		t.Fatal("a media file was accepted as the folder root", err)
	}
	if err := wrong.Close(); err != nil {
		t.Fatal(err)
	}
	s, err := core.New(core.Options{Authorize: func(context.Context, core.Principal, core.Action, core.Address) error { return nil }})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close(ctx) }()
	mount, err := s.Install("/emby", source)
	if err != nil {
		t.Fatal(err)
	}
	q := core.ListRequest{Query: core.Children(core.Path("/emby")), Limit: 1, CursorTTL: time.Minute}
	before := pages.Load()
	for index, expected := range []string{"Team", "Virtual", "Other"} {
		page, err := s.List(ctx, "u", q)
		if err != nil || len(page.Items) != 1 || page.Items[0].Name != expected || (index < 2) != (page.NextCursor != nil) {
			t.Fatal(page, err)
		}
		if index == 1 && pages.Load() != before+1 {
			t.Fatal("page remainder reached Emby", pages.Load()-before)
		}
		if page.NextCursor != nil {
			q.Cursor = *page.NextCursor
		}
	}
	if pages.Load() != before+2 {
		t.Fatal("native paging did not use the returned item count", pages.Load()-before)
	}
	target := core.Path("/emby/Team/" + name)
	for _, check := range []struct {
		state, path string
		code        core.Code
	}{{"root-fallback", target.Value(), core.InvalidUpstreamResponse}, {"duplicate", target.Value(), core.PathAmbiguous},
		{"missing-page", target.Value(), core.InvalidUpstreamResponse}, {"", "/emby/Team/Remote", core.UnsupportedOperation},
		{"", "/emby/Team/report 版", core.NotFound}} {
		mode.Store(check.state)
		if _, err := s.Open(ctx, "u", core.Path(check.path), core.OpenOptions{}); core.CodeOf(err) != check.code || downloads.Load() != 0 {
			t.Fatal(check, err)
		}
	}
	mode.Store("")
	if meta, err := s.Stat(ctx, "u", target); err != nil || meta.Kind != core.File || meta.Size != nil {
		t.Fatal(meta, err)
	}
	opened, err := s.Open(ctx, "u", target, core.OpenOptions{Range: &core.Range{Start: 1, End: 3}})
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(opened.Body)
	if err := errors.Join(err, opened.Body.Close()); err != nil || string(data) != "bcd" || opened.Range == nil || opened.Range.Total != 6 {
		t.Fatal(string(data), opened.Range, err)
	}
	root := t.TempDir()
	dest, err := local.New(root)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Install("/local", dest); err != nil {
		t.Fatal(err)
	}
	if change, err := s.Copy(ctx, "u", target, core.Path("/local/original")); err != nil || change.Effect != core.Applied {
		t.Fatal(change, err)
	}
	if data, err := os.ReadFile(filepath.Join(root, "original")); err != nil || string(data) != original {
		t.Fatal(string(data), err)
	}
	mode.Store("denied")
	if _, err := s.Open(ctx, "u", target, core.OpenOptions{}); core.CodeOf(err) != core.UpstreamAccessDenied {
		t.Fatal("Emby download permission was not preserved", err)
	}
	mode.Store("redirect")
	if _, err := s.Open(ctx, "u", target, core.OpenOptions{}); err == nil || foreignCalls.Load() != 0 || unexpected.Load() != 0 {
		t.Fatal("credentials or requests escaped the selected Emby endpoint", err)
	}
	mode.Store("wait")
	opened, err = s.Open(ctx, "u", target, core.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("native download did not start")
	}
	done := make(chan error, 1)
	go func() { _, err := io.ReadAll(opened.Body); done <- err }()
	if err := mount.Retire(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal("retirement did not cancel the download", err)
	}
}
