package gokuai_test

import (
	"context"
	"crypto/hmac"
	"crypto/sha1"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/gokuai"
)

type transport func(*http.Request) (*http.Response, error)

func (f transport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type waiting struct{ context.Context }

func (w waiting) Read([]byte) (int, error) { <-w.Done(); return 0, w.Err() }
func (waiting) Close() error               { return nil }

func TestLibraryPaginationAndOriginal(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	const library, key, root = "fixture-library", "fixture-key", "Team docs"
	const file, version, grantQuery = "报告 +%.txt", "original-version", "sig=x%2By%3d&z=4"
	const nativePath = root + "/" + file
	var mode string
	var grants, bodies int
	entry := func(path, name, hash string, dir, size int) map[string]any {
		return map[string]any{"hash": hash, "dir": dir, "fullpath": path, "filename": name,
			"filehash": version, "filesize": size}
	}
	reply := func(status int, data any) *http.Response {
		wire, _ := json.Marshal(data)
		return &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader(string(wire)))}
	}
	api := &http.Client{Transport: transport(func(r *http.Request) (*http.Response, error) {
		defer r.Body.Close()
		if r.Method != "POST" || r.URL.Scheme != "https" || r.URL.Host != "yk3.gokuai.com" ||
			r.URL.RawQuery != "" || r.Header.Get("Accept-Encoding") != "identity" ||
			r.Header.Get("Content-Type") != "application/x-www-form-urlencoded" {
			return nil, errors.New("control request did not use the fixed signed API")
		}
		raw, err := io.ReadAll(r.Body)
		if err != nil {
			return nil, err
		}
		p, err := url.ParseQuery(string(raw))
		if err != nil || strings.ContainsAny(string(raw), "+ ") || strings.Contains(string(raw), key) ||
			p.Get("org_client_id") != library {
			return nil, errors.New("control fields were not RFC 3986 encoded")
		}
		stamp, err := strconv.ParseInt(p.Get("dateline"), 10, 64)
		if err != nil || time.Since(time.Unix(stamp, 0)).Abs() > 30*time.Second {
			return nil, errors.New("invalid signature timestamp")
		}
		path := p.Get("fullpath")
		var signature []string
		var response *http.Response
		switch r.URL.Path {
		case "/m-open/1/file/info":
			if p.Get("attribute") != "10" {
				return nil, errors.New("missing minimal metadata selection")
			}
			signature = []string{"10", p.Get("dateline"), path, library}
			switch path {
			case root:
				response = reply(200, entry(root, root, "root-id", 1, 0))
			case nativePath:
				f := entry(nativePath, file, "file-id", 0, 6)
				if mode == "outside" {
					f["fullpath"] = "Elsewhere/" + file
				}
				response = reply(200, f)
			default:
				response = reply(404, map[string]any{"error_code": 40402})
			}
		case "/m-open/1/file/ls":
			if path != root || p.Get("size") != "3" {
				return nil, errors.New("wrong directory or page size")
			}
			signature = []string{p.Get("dateline"), root, library, "3", p.Get("start")}
			switch p.Get("start") {
			case "0":
				response = reply(200, map[string]any{"count": 2, "list": []any{entry(root+"/Archive", "Archive", "folder-id", 1, 0)}})
			case "1":
				response = reply(200, map[string]any{"count": 2, "list": []any{entry(nativePath, file, "file-id", 0, 6)}})
			default:
				return nil, errors.New("listing skipped a short page")
			}
		case "/m-open/1/file/download_url":
			grants++
			if path != nativePath || p.Get("filehash") != version || p.Get("open") != "0" || p.Has("hash") {
				return nil, errors.New("wrong path or version in original grant")
			}
			signature = []string{p.Get("dateline"), version, nativePath, "0", library}
			links := []string{"http://grant.example.test/insecure"}
			switch mode {
			case "denied":
				response = reply(405, map[string]any{"error_code": 40501, "error_msg": key})
			case "insecure":
				response = reply(200, map[string]any{"urls": links})
			default:
				response = reply(200, map[string]any{"urls": append(links, "https://grant.example.test/original?"+grantQuery)})
			}
		default:
			return nil, errors.New("unexpected control route")
		}
		mac := hmac.New(sha1.New, []byte(key))
		_, _ = io.WriteString(mac, strings.Join(signature, "\n"))
		if p.Get("sign") != base64.StdEncoding.EncodeToString(mac.Sum(nil)) || len(p) != len(signature)+1 {
			response.Body.Close()
			return nil, errors.New("library signature did not cover the exact wire fields")
		}
		return response, nil
	})}
	content := &http.Client{Transport: transport(func(r *http.Request) (*http.Response, error) {
		bodies++
		if r.Method != "GET" || r.URL.Scheme != "https" || r.Header.Get("Accept-Encoding") != "identity" ||
			r.Header.Get("Cookie") != "" || r.Header.Get("Referer") != "" || r.Header.Get("Authorization") != "" || r.Body != nil {
			return nil, errors.New("download was not isolated from control requests")
		}
		switch r.URL.Host + r.URL.Path {
		case "grant.example.test/original":
			if r.URL.RawQuery != grantQuery {
				return nil, errors.New("signed grant query was changed")
			}
			return &http.Response{StatusCode: 302, Body: http.NoBody,
				Header: http.Header{"Location": {"https://yk3.gokuai.com/body"}}}, nil
		case "yk3.gokuai.com/body":
			if r.URL.RawQuery != "" {
				return nil, errors.New("grant query was forwarded to another receiver")
			}
			if mode == "wait" {
				return &http.Response{StatusCode: 200, ContentLength: 6, Body: waiting{r.Context()}}, nil
			}
			if r.Header.Get("Range") == "bytes=1-3" {
				return &http.Response{StatusCode: 206, ContentLength: 3, Body: io.NopCloser(strings.NewReader("bcd")),
					Header: http.Header{"Content-Range": {"bytes 1-3/6"}}}, nil
			}
			return &http.Response{StatusCode: 200, ContentLength: -1, Body: io.NopCloser(strings.NewReader("abcdef"))}, nil
		}
		return nil, errors.New("unexpected download receiver")
	})}
	d, err := gokuai.Open(ctx, gokuai.Options{LibraryID: library, LibrarySecret: key, RootPath: "/" + root,
		Client: api, ContentClient: content})
	if err != nil {
		t.Fatal(err)
	}
	c, err := core.New(core.Options{MaxCursors: 2, Authorize: func(context.Context, core.Principal, core.Action, core.Address) error { return nil }})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close(ctx)
	mount, err := c.Install("/library", d)
	if err != nil {
		t.Fatal(err)
	}
	list := core.ListRequest{Query: core.Children(core.Path("/library")), Limit: 3, CursorTTL: time.Minute}
	page, err := c.List(ctx, "u", list)
	if err != nil || len(page.Items) != 1 || page.Items[0].Name != "Archive" || page.NextCursor == nil {
		t.Fatal("short nonterminal listing", page, err)
	}
	list.Cursor = *page.NextCursor
	page, err = c.List(ctx, "u", list)
	if err != nil || len(page.Items) != 1 || page.Items[0].Name != file || page.NextCursor != nil {
		t.Fatal("terminal listing", page, err)
	}
	path := core.Path("/library/" + file)
	if f, err := c.Stat(ctx, "u", path); err != nil || f.Size == nil || *f.Size != 6 {
		t.Fatal("original metadata", f, err)
	}
	mode = "outside"
	if _, err := c.Open(ctx, "u", path, core.OpenOptions{}); core.CodeOf(err) != core.InvalidUpstreamResponse || grants != 0 {
		t.Fatal("out-of-scope path reached grant request", err)
	}
	for _, s := range []struct {
		mode string
		code core.Code
	}{{"denied", core.UpstreamAccessDenied}, {"insecure", core.UnsupportedOperation}} {
		mode = s.mode
		_, err := c.Open(ctx, "u", path, core.OpenOptions{})
		if core.CodeOf(err) != s.code || strings.Contains(err.Error(), key) || bodies != 0 {
			t.Fatal("invalid grant reached body or exposed credentials", mode, err)
		}
	}
	mode = ""
	opened, err := c.Open(ctx, "u", path, core.OpenOptions{Range: &core.Range{Start: 1, End: 3}})
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(opened.Body)
	if err = errors.Join(err, opened.Body.Close()); err != nil || string(data) != "bcd" ||
		opened.Range == nil || *opened.Range != (core.ContentRange{Start: 1, End: 3, Total: 6}) {
		t.Fatal("native original range", string(data), opened.Range, err)
	}
	opened, err = c.Open(ctx, "u", path, core.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	data, err = io.ReadAll(opened.Body)
	if err = errors.Join(err, opened.Body.Close()); err != nil || string(data) != "abcdef" || opened.Range != nil {
		t.Fatal("full original", string(data), opened.Range, err)
	}
	mode = "wait"
	opened, err = c.Open(ctx, "u", path, core.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { _, err := io.ReadAll(opened.Body); done <- err }()
	if err := mount.Retire(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal("retirement did not cancel the original body", err)
	}
}
