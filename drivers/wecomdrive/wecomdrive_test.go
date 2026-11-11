package wecomdrive

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	core "github.com/nostalume/mountweave"
)

type transport func(*http.Request) (*http.Response, error)

func (f transport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestVerifiedFolderThroughCore(t *testing.T) {
	const (
		space  = "native-space"
		folder = "native-folder"
		name   = "Report 版"
		data   = "abcdef"
		token  = "sealed-application-token"
		cookie = "file-grant=sealed-cookie"
		echo   = "&access_token=delegated&access%5ftoken=" + token
	)
	var state atomic.Value
	state.Store("")
	var pages, grants, bodies, unexpected atomic.Int32
	started := make(chan struct{}, 1)
	var contentOrigin, delegatedOrigin string
	download := func(w http.ResponseWriter, r *http.Request) {
		bodies.Add(1)
		cookieExpected := ""
		if r.URL.Path == "/receipt" || r.URL.Path == "/same-origin" {
			cookieExpected = cookie
		}
		if r.Method != http.MethodGet || r.Header.Get("Cookie") != cookieExpected ||
			r.Header.Get("Authorization") != "" || r.Header.Get("Referer") != "" ||
			r.Header.Get("Accept-Encoding") != "identity" || r.URL.RawQuery != "sig=sealed" {
			unexpected.Add(1)
			http.Error(w, "unexpected download request", http.StatusBadRequest)
			return
		}
		switch r.URL.Path {
		case "/receipt":
			w.Header().Set("Location", "/same-origin?sig=sealed")
		case "/same-origin":
			location := delegatedOrigin + "/foreign?sig=sealed"
			if state.Load() == "echo-redirect" {
				location += echo
			}
			w.Header().Set("Location", location)
		case "/foreign":
			w.Header().Set("Location", contentOrigin+"/body?sig=sealed")
		case "/body":
			switch {
			case state.Load() == "wait":
				w.Header().Set("Content-Length", "6")
				w.WriteHeader(http.StatusOK)
				w.(http.Flusher).Flush()
				started <- struct{}{}
				<-r.Context().Done()
			case r.Header.Get("Range") == "bytes=1-3":
				w.Header().Set("Content-Length", "3")
				w.Header().Set("Content-Range", "bytes 1-3/6")
				w.WriteHeader(http.StatusPartialContent)
				_, _ = io.WriteString(w, data[1:4])
			default:
				_, _ = io.WriteString(w, data)
			}
			return
		default:
			unexpected.Add(1)
			http.NotFound(w, r)
			return
		}
		w.WriteHeader(http.StatusFound)
	}
	content := httptest.NewTLSServer(http.HandlerFunc(download))
	defer content.Close()
	delegated := httptest.NewTLSServer(http.HandlerFunc(download))
	defer delegated.Close()
	contentOrigin, delegatedOrigin = content.URL, delegated.URL
	row := func(id, name, parent string, kind int) map[string]any {
		return map[string]any{"fileid": id, "file_name": name, "fatherid": parent, "spaceid": space,
			"file_type": kind, "file_status": 1, "file_size": len(data)}
	}
	api := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			ID     string `json:"fileid"`
			Parent string `json:"fatherid"`
			Space  string `json:"spaceid"`
			Start  uint32 `json:"start"`
			Sort   int    `json:"sort_type"`
			Limit  int    `json:"limit"`
		}
		if r.Method != http.MethodPost || r.URL.Query().Get("access_token") != token ||
			r.Header.Get("Cookie") != "" || r.Header.Get("Content-Type") != "application/json" ||
			json.NewDecoder(r.Body).Decode(&request) != nil {
			unexpected.Add(1)
			http.Error(w, "unexpected control request", http.StatusBadRequest)
			return
		}
		switch r.URL.Path {
		case "/cgi-bin/wedrive/file_info":
			if request.ID != folder {
				unexpected.Add(1)
			}
			root := row(folder, "Projects", space, 1)
			if state.Load() == "wrong-space" {
				root["spaceid"] = "unrelated-space"
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"errcode": 0, "file_info": root})
		case "/cgi-bin/wedrive/file_list":
			if request.Space != space || request.Sort != 1 || request.Limit < 1 || request.Limit > 1000 {
				unexpected.Add(1)
			}
			if request.Parent == folder {
				pages.Add(1)
			}
			files, next := []map[string]any{}, uint32(0)
			switch {
			case request.Parent == folder && request.Start == 0:
				parent := folder
				if state.Load() == "wrong-parent" {
					parent = "unrelated-folder"
				}
				files = append(files, row("team-id", "Team", parent, 1), row("doc-id", "Notes", folder, 3))
				next = 17
			case request.Parent == folder && request.Start == 17:
				next = 99
				if state.Load() == "cycle" {
					next = 17
				}
			case request.Parent == folder && request.Start == 99:
				files = append(files, row("last-id", "Slides", folder, 6))
			case request.Parent == "team-id" && request.Start == 0:
				files = append(files, row("file-id", name, request.Parent, 2))
			default:
				unexpected.Add(1)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"errcode": 0, "has_more": next != 0, "next_start": next,
				"file_list": map[string]any{"item": files}})
		case "/cgi-bin/wedrive/file_download":
			grants.Add(1)
			if request.ID != "file-id" {
				unexpected.Add(1)
			}
			target := content.URL + "/receipt?sig=sealed"
			switch state.Load() {
			case "plain-http":
				target = strings.Replace(target, "https:", "http:", 1)
			case "echo-receipt":
				target += echo
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"errcode": 0, "download_url": target,
				"cookie_name": "file-grant", "cookie_value": "sealed-cookie"})
		default:
			unexpected.Add(1)
			http.NotFound(w, r)
		}
	}))
	defer api.Close()
	apiClient := api.Client()
	apiURL, err := url.Parse(api.URL)
	if err != nil {
		t.Fatal(err)
	}
	native := apiClient.Transport
	apiClient.Transport = transport(func(r *http.Request) (*http.Response, error) {
		if r.URL.Scheme != "https" || r.URL.Host != "qyapi.weixin.qq.com" {
			unexpected.Add(1)
			return nil, errors.New("unexpected control origin")
		}
		req := r.Clone(r.Context())
		req.URL.Host = apiURL.Host
		return native.RoundTrip(req)
	})
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	o := Options{SpaceID: space, FolderID: folder, Client: apiClient, ContentClient: content.Client(),
		AccessToken: func(context.Context) (string, error) {
			if state.Load() == "token-timeout" {
				return "", errors.Join(errors.New("private token "+token), context.DeadlineExceeded)
			}
			return token, nil
		}}
	state.Store("wrong-space")
	rejected, err := Open(ctx, o)
	if rejected == nil || core.CodeOf(err) != core.InvalidUpstreamResponse {
		t.Fatal("a folder from another space was accepted", err)
	}
	if err := rejected.Close(); err != nil {
		t.Fatal(err)
	}
	state.Store("")
	source, err := Open(ctx, o)
	if err != nil {
		t.Fatal(err)
	}
	s, err := core.New(core.Options{Authorize: func(context.Context, core.Principal, core.Action, core.Address) error { return nil }})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close(ctx) }()
	mount, err := s.Install("/work", source)
	if err != nil {
		t.Fatal(err)
	}
	q := core.ListRequest{Query: core.Children(core.Path("/work")), Limit: 1, CursorTTL: time.Minute}
	checks := [][]string{{"Team"}, {"Notes"}, nil, {"Slides"}}
	for index, expected := range checks {
		page, err := s.List(ctx, "u", q)
		if err != nil || len(page.Items) != len(expected) || (index < len(checks)-1) != (page.NextCursor != nil) ||
			len(expected) != 0 && page.Items[0].Name != expected[0] {
			t.Fatal(page, err)
		}
		if index == 1 {
			replay, err := s.List(ctx, "u", q)
			if err != nil || len(replay.Items) != 1 || replay.Items[0].Size != nil || pages.Load() != 1 {
				t.Fatal("Core replay issued another native page", replay, err)
			}
		}
		if page.NextCursor != nil {
			q.Cursor = *page.NextCursor
		}
	}
	if pages.Load() != 3 {
		t.Fatal("opaque continuation did not cross the empty native page", pages.Load())
	}
	target := core.Path("/work/Team/" + name)
	for _, rejected := range []struct {
		mode string
		path core.Address
		code core.Code
	}{{"wrong-parent", target, core.InvalidUpstreamResponse}, {"cycle", target, core.InvalidUpstreamResponse},
		{"", core.Path("/work/Notes"), core.UnsupportedOperation},
		{"", core.Path("/work/Team/" + strings.ToLower(name)), core.NotFound},
		{"plain-http", target, core.InvalidUpstreamResponse}, {"echo-receipt", target, core.InvalidUpstreamResponse},
		{"token-timeout", target, core.UpstreamTimeout}} {
		state.Store(rejected.mode)
		_, err := s.Open(ctx, "u", rejected.path, core.OpenOptions{})
		if core.CodeOf(err) != rejected.code || bodies.Load() != 0 {
			t.Fatal(rejected, err, bodies.Load())
		}
		if strings.Contains(err.Error(), token) || rejected.mode == "token-timeout" && !errors.Is(err, context.DeadlineExceeded) {
			t.Fatal("rejected request exposed the token or lost the timeout", err)
		}
	}
	if grants.Load() != 2 {
		t.Fatal("an unresolved or special file reached download authorization", grants.Load())
	}
	state.Store("")
	if meta, err := s.Stat(ctx, "u", target); err != nil || meta.Name != name || meta.Size == nil || *meta.Size != 6 {
		t.Fatal(meta, err)
	}
	opened, err := s.Open(ctx, "u", target, core.OpenOptions{Range: &core.Range{Start: 1, End: 3}})
	if err != nil {
		t.Fatal(err)
	}
	bytes, err := io.ReadAll(opened.Body)
	if err := errors.Join(err, opened.Body.Close()); err != nil || string(bytes) != "bcd" || opened.Range == nil ||
		opened.Range.Total != 6 || bodies.Load() != 4 {
		t.Fatal(string(bytes), opened.Range, bodies.Load(), err)
	}
	state.Store("echo-redirect")
	before := bodies.Load()
	_, err = s.Open(ctx, "u", target, core.OpenOptions{})
	if core.CodeOf(err) != core.InvalidUpstreamResponse || strings.Contains(err.Error(), token) || bodies.Load() != before+2 {
		t.Fatal("redirect carrying the app token reached the delegated origin", err, bodies.Load()-before)
	}
	state.Store("")
	opened, err = s.Open(ctx, "u", target, core.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	bytes, err = io.ReadAll(opened.Body)
	if err = errors.Join(err, opened.Body.Close()); err != nil || string(bytes) != data || opened.Range != nil {
		t.Fatalf("full original: %q, range=%v, error=%v", bytes, opened.Range, err)
	}
	state.Store("wait")
	opened, err = s.Open(ctx, "u", target, core.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("Wedrive download did not start")
	}
	done := make(chan error, 1)
	go func() { _, err := io.ReadAll(opened.Body); done <- err }()
	if err := mount.Retire(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-done; !errors.Is(err, context.Canceled) || unexpected.Load() != 0 {
		t.Fatal("unexpected request or retirement failed to cancel the stream", unexpected.Load(), err)
	}
}
