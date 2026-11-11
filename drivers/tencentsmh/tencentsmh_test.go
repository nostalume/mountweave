package tencentsmh

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/local"
)

func TestMountedSpaceThroughCore(t *testing.T) {
	const (
		root   = "Project Hub"
		name   = "Report % 版"
		body   = "abcdef"
		cas    = "native-content-cas"
		secret = "sealed-token"
	)
	var state atomic.Value
	state.Store("")
	var controls, grants, pages, downloads, unexpected atomic.Int32
	started := make(chan struct{}, 1)
	content := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		downloads.Add(1)
		if r.Method != http.MethodGet || r.URL.Query().Get("grant") != "sealed" ||
			r.URL.Query().Has("access_token") || r.Header.Get("Authorization") != "" ||
			r.Header.Get("Cookie") != "" || r.Header.Get("Referer") != "" || r.Header.Get("Accept-Encoding") != "identity" {
			unexpected.Add(1)
			http.Error(w, "unexpected COS request", http.StatusBadRequest)
			return
		}
		if r.URL.Path == "/short" {
			w.Header().Set("Location", "/object?grant=sealed")
			w.WriteHeader(http.StatusFound)
			return
		}
		if r.URL.Path != "/object" {
			unexpected.Add(1)
			http.NotFound(w, r)
			return
		}
		switch {
		case state.Load() == "wait":
			w.Header().Set("Content-Length", "6")
			w.WriteHeader(http.StatusOK)
			w.(http.Flusher).Flush()
			started <- struct{}{}
			<-r.Context().Done()
		case r.Header.Get("Range") == "bytes=1-3":
			w.Header().Set("Content-Range", "bytes 1-3/6")
			w.Header().Set("Content-Length", "3")
			w.WriteHeader(http.StatusPartialContent)
			_, _ = io.WriteString(w, body[1:4])
		default:
			w.(http.Flusher).Flush() // The body itself does not declare a length.
			_, _ = io.WriteString(w, body)
		}
	}))
	defer content.Close()

	api := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		const dir = "/api/v1/directory/library/space/"
		const file = "/api/v1/file/library/space/"
		q := r.URL.Query()
		if r.Method != http.MethodGet || !strings.HasPrefix(q.Get("access_token"), secret+"-") ||
			r.Header.Get("Accept-Encoding") != "identity" {
			unexpected.Add(1)
			http.Error(w, "unexpected SMH request", http.StatusBadRequest)
			return
		}
		switch r.URL.Path {
		case "/api/v1/directory/library/-/":
			_ = json.NewEncoder(w).Encode(map[string]any{"name": "", "path": []string{}, "type": "dir"})
		case dir + root:
			if q.Get("info") == "1" {
				kind := "dir"
				if state.Load() == "root-link" {
					kind = "symlink"
				}
				_ = json.NewEncoder(w).Encode(map[string]any{"name": root, "path": []string{root}, "type": kind})
				return
			}
			pages.Add(1)
			if q.Get("by-marker") != "1" || q.Get("limit") != "1" {
				unexpected.Add(1)
			}
			path := []string{root}
			if state.Load() == "wrong-parent" {
				path = []string{"unrelated"}
			}
			files := []map[string]any{}
			var marker any
			switch q.Get("marker") {
			case "":
				files = append(files, map[string]any{"name": name, "type": "file", "size": "6"},
					map[string]any{"name": "Shortcut", "type": "symlink"})
				marker = 17
			case "17":
				marker = "opaque+17"
				if state.Load() == "cycle" {
					marker = "17"
				}
			case "opaque+17":
				files = append(files, map[string]any{"name": "Virtual", "type": "virtual", "size": "123"})
			default:
				unexpected.Add(1)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"path": path, "contents": files, "nextMarker": marker})
		case dir + root + "/" + name, dir + root + "/" + strings.ToLower(name), dir + root + "/Shortcut":
			if q.Get("info") != "1" || strings.HasSuffix(r.URL.Path, name) &&
				!strings.HasSuffix(r.URL.EscapedPath(), "/Report%20%25%20%E7%89%88") {
				unexpected.Add(1)
			}
			if strings.HasSuffix(r.URL.Path, "/Shortcut") {
				_ = json.NewEncoder(w).Encode(map[string]any{"name": "Shortcut", "path": []string{root, "Shortcut"},
					"type": "symlink", "contentCas": cas})
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"name": name, "path": []string{root, name},
				"type": "file", "size": "6", "contentCas": cas})
		case file + root + "/" + name:
			grants.Add(1)
			if q.Get("info") != "1" || q.Get("content_cas") != cas || q.Has("preview") {
				unexpected.Add(1)
			}
			if state.Load() == "changed" {
				w.WriteHeader(http.StatusConflict)
				_ = json.NewEncoder(w).Encode(map[string]string{"code": "ContentCasMismatch", "message": q.Get("access_token")})
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]string{
				"type": "file", "size": "6", "contentCas": cas, "cosUrl": content.URL + "/short?grant=sealed",
			})
		default:
			unexpected.Add(1)
			http.NotFound(w, r)
		}
	}))
	defer api.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	o := Options{Endpoint: api.URL, RootPath: "/" + root, LibraryID: "library", SpaceID: "space",
		Client: api.Client(), ContentClient: content.Client(), AccessToken: func(context.Context) (string, error) {
			return secret + "-" + strconv.Itoa(int(controls.Add(1))), nil
		}}
	defaultRoot := o
	defaultRoot.RootPath, defaultRoot.SpaceID = "", ""
	whole, err := Open(ctx, defaultRoot)
	if err != nil {
		t.Fatal("the empty native path did not select the single-tenant root", err)
	}
	if err := whole.Close(); err != nil {
		t.Fatal(err)
	}
	state.Store("root-link")
	rejected, err := Open(ctx, o)
	if rejected == nil || core.CodeOf(err) != core.NotDirectory {
		t.Fatal("a shortcut was accepted as the root", err)
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
	mount, err := s.Install("/smh", source)
	if err != nil {
		t.Fatal(err)
	}
	list := core.ListRequest{Query: core.Children(core.Path("/smh")), Limit: 1, CursorTTL: time.Minute}
	checks := [][]string{{name}, {"Shortcut"}, nil, {"Virtual"}}
	for index, expected := range checks {
		page, err := s.List(ctx, "u", list)
		if err != nil || len(page.Items) != len(expected) || (index < len(checks)-1) != (page.NextCursor != nil) ||
			len(expected) > 0 && page.Items[0].Name != expected[0] {
			t.Fatal(page, err)
		}
		if index == 1 {
			replayed, err := s.List(ctx, "u", list)
			if err != nil || len(replayed.Items) != 1 || replayed.Items[0].Name != "Shortcut" ||
				replayed.Items[0].Size != nil || pages.Load() != 1 {
				t.Fatal("Core replay did not reuse the native page", replayed, err)
			}
		}
		if page.NextCursor != nil {
			list.Cursor = *page.NextCursor
		}
	}
	if pages.Load() != 3 {
		t.Fatal("unexpected native marker progress", pages.Load())
	}
	state.Store("wrong-parent")
	list.Cursor = ""
	if _, err := s.List(ctx, "u", list); core.CodeOf(err) != core.InvalidUpstreamResponse {
		t.Fatal("unrelated page was accepted", err)
	}
	state.Store("cycle")
	for index := 0; index < 3; index++ {
		page, err := s.List(ctx, "u", list)
		if index == 2 {
			if core.CodeOf(err) != core.InvalidUpstreamResponse {
				t.Fatal("repeated marker was accepted", err)
			}
		} else if err != nil || page.NextCursor == nil {
			t.Fatal(page, err)
		} else {
			list.Cursor = *page.NextCursor
		}
	}
	target := core.Path("/smh/" + name)
	state.Store("")
	for _, rejected := range []struct {
		path string
		code core.Code
	}{{"Shortcut", core.UnsupportedOperation}, {strings.ToLower(name), core.InvalidUpstreamResponse}} {
		if _, err := s.Open(ctx, "u", core.Path("/smh/"+rejected.path), core.OpenOptions{}); core.CodeOf(err) != rejected.code || grants.Load() != 0 {
			t.Fatal(rejected, err)
		}
	}
	state.Store("changed")
	before := controls.Load()
	if _, err := s.Open(ctx, "u", target, core.OpenOptions{}); core.CodeOf(err) != core.PreconditionFailed ||
		strings.Contains(err.Error(), secret) || downloads.Load() != 0 || controls.Load() != before+1 {
		t.Fatal("a changed file or a private response escaped the control boundary", err)
	}
	state.Store("")
	if meta, err := s.Stat(ctx, "u", target); err != nil || meta.Name != name || meta.Size == nil || *meta.Size != 6 {
		t.Fatal(meta, err)
	}
	for _, read := range []struct {
		rangeRequest *core.Range
		expected     string
	}{{nil, body}, {&core.Range{Start: 1, End: 3}, "bcd"}} {
		opened, err := s.Open(ctx, "u", target, core.OpenOptions{Range: read.rangeRequest})
		if err != nil {
			t.Fatal(err)
		}
		bytes, err := io.ReadAll(opened.Body)
		if err := errors.Join(err, opened.Body.Close()); err != nil || string(bytes) != read.expected ||
			read.rangeRequest == nil && (opened.Length != nil || opened.Range != nil) ||
			read.rangeRequest != nil && (opened.Range == nil || opened.Range.Total != 6) {
			t.Fatal(string(bytes), opened.Length, opened.Range, err)
		}
	}
	state.Store("wait")
	opened, err := s.Open(ctx, "u", target, core.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("COS download did not start")
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

func TestMountedRecipientShareThroughCore(t *testing.T) {
	const secret, code, extraction, file = "share-token", "share-code", "p123", "测试 %.txt"
	var verifications, grants, bodies atomic.Int32
	var insecure, echoedToken, badExpiry atomic.Bool
	content := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		bodies.Add(1)
		if r.URL.RawQuery != "signature=a%2Bb" || r.Header.Get("Authorization") != "" ||
			r.Header.Get("Cookie") != "" || r.Header.Get("Referer") != "" {
			t.Error("download received control credentials or altered signature")
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		if r.Header.Get("Range") == "bytes=1-3" {
			w.Header().Set("Content-Range", "bytes 1-3/6")
			w.WriteHeader(http.StatusPartialContent)
			_, _ = io.WriteString(w, "bcd")
			return
		}
		_, _ = io.WriteString(w, "abcdef")
	}))
	defer content.Close()

	api := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		const route = "/api/v1/share/file/" + code + "/"
		q := r.URL.Query()
		if r.URL.Path == "/api/v1/share/verify/"+code {
			verifications.Add(1)
			var proof map[string]string
			if r.Method != http.MethodPost || q.Has("access_token") || r.Header.Get("Content-Type") != "application/json" ||
				json.NewDecoder(r.Body).Decode(&proof) != nil || len(proof) != 1 || proof["extractionCode"] != extraction {
				t.Error("invalid share token request")
				http.Error(w, "bad proof", http.StatusBadRequest)
				return
			}
			expiry := time.Now().Add(10 * time.Minute).Format(time.RFC3339Nano)
			if badExpiry.Load() {
				expiry = secret
			}
			_ = json.NewEncoder(w).Encode(map[string]string{"accessToken": secret, "expireTime": expiry})
			return
		}
		if r.Method != http.MethodGet || q.Get("access_token") != secret || r.Header.Get("Range") != "" {
			t.Error("unexpected share control request")
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		switch {
		case q.Get("list") == "1" && r.URL.Path == route+"~":
			if q.Get("marker") == "" {
				_, _ = io.WriteString(w, `{"contents":[{"inode":"folder","name":"Project","type":"dir"}],"hasMore":true,"marker":"opaque+"}`)
			} else if q.Get("marker") == "opaque+" {
				_, _ = io.WriteString(w, `{"contents":[{"inode":"unreadable","name":"Blocked","type":"file","canDownload":false}],"hasMore":false,"marker":"terminal"}`)
			} else {
				t.Error("unexpected share marker")
				http.Error(w, "bad marker", http.StatusBadRequest)
			}
		case q.Get("list") == "1" && r.URL.Path == route+"folder":
			_ = json.NewEncoder(w).Encode(map[string]any{"contents": []map[string]any{
				{"inode": "leaf", "name": file, "type": "file", "canDownload": true, "size": "999"},
			}, "hasMore": false})
		case q.Get("download") == "1" && q.Get("internal_domain") == "0" && r.URL.EscapedPath() == route+"folder%2Fleaf":
			grants.Add(1)
			target := content.URL + "/object?signature=a%2Bb"
			if insecure.Load() {
				target = strings.Replace(target, "https:", "http:", 1)
			}
			if echoedToken.Load() {
				target += "&access_token=body-capability&access%5Ftoken=" + url.QueryEscape(secret)
			}
			w.Header().Set("Location", target)
			w.WriteHeader(http.StatusFound)
		default:
			t.Errorf("unexpected request path %q", r.URL.EscapedPath())
			http.NotFound(w, r)
		}
	}))
	defer api.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	source, err := OpenShare(ctx, ShareOptions{Endpoint: api.URL, ShareCode: code, ExtractionCode: extraction,
		Client: api.Client(), ContentClient: content.Client()})
	if err != nil {
		t.Fatal(err)
	}
	c, err := core.New(core.Options{Authorize: func(context.Context, core.Principal, core.Action, core.Address) error { return nil }})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close(ctx) }()
	if _, err := c.Install("/share", source); err != nil {
		t.Fatal(err)
	}
	root := core.Path("/share")
	q := core.ListRequest{Query: core.Children(root), Limit: 1, CursorTTL: time.Minute}
	page, err := c.List(ctx, "u", q)
	if err != nil || len(page.Items) != 1 || page.Items[0].Name != "Project" || page.NextCursor == nil {
		t.Fatal(page, err)
	}
	q.Cursor = *page.NextCursor
	page, err = c.List(ctx, "u", q)
	if err != nil || len(page.Items) != 1 || page.Items[0].Name != "Blocked" || page.NextCursor != nil {
		t.Fatal(page, err)
	}
	if _, err := c.Open(ctx, "u", core.Path("/share/Blocked"), core.OpenOptions{}); core.CodeOf(err) != core.UpstreamAccessDenied ||
		grants.Load() != 0 {
		t.Fatal("download was attempted without permission", err)
	}
	address := core.Path("/share/Project/" + file)
	opened, err := c.Open(ctx, "u", address, core.OpenOptions{Range: &core.Range{Start: 1, End: 3}})
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(opened.Body)
	if err != nil || string(data) != "bcd" || opened.Range == nil || opened.Range.Total != 6 || verifications.Load() != 1 {
		t.Fatal("range and token reuse", string(data), opened.Range, err)
	}
	if err := opened.Body.Close(); err != nil {
		t.Fatal(err)
	}
	source.grant.ExpireTime = time.Time{}
	dir := t.TempDir()
	target, err := local.New(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Install("/target", target); err != nil {
		t.Fatal(err)
	}
	result, err := c.Copy(ctx, "u", address, core.Path("/target/copied"), core.CopyOptions{})
	if err != nil || result.Effect != core.Applied || verifications.Load() != 2 {
		t.Fatal("copy and token renewal", result, err)
	}
	got, err := os.ReadFile(filepath.Join(dir, "copied"))
	if err != nil || string(got) != "abcdef" {
		t.Fatal(string(got), err)
	}
	insecure.Store(true)
	if _, err := c.Open(ctx, "u", address, core.OpenOptions{}); core.CodeOf(err) != core.InvalidUpstreamResponse ||
		strings.Contains(err.Error(), secret) || grants.Load() != 3 || bodies.Load() != 2 {
		t.Fatal("unsafe grant was followed or disclosed", err)
	}
	insecure.Store(false)
	echoedToken.Store(true)
	if _, err := c.Open(ctx, "u", address, core.OpenOptions{}); core.CodeOf(err) != core.InvalidUpstreamResponse ||
		strings.Contains(err.Error(), secret) || grants.Load() != 4 || bodies.Load() != 2 {
		t.Fatal("redirect forwarded or disclosed the share token", err)
	}
	badExpiry.Store(true)
	source.grant.ExpireTime = time.Time{}
	if _, err := c.Open(ctx, "u", address, core.OpenOptions{}); core.CodeOf(err) != core.InvalidUpstreamResponse ||
		strings.Contains(err.Error(), secret) || grants.Load() != 4 {
		t.Fatal("malformed token receipt was accepted or disclosed", err)
	}
}
