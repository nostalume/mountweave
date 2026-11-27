package baidupan

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	core "github.com/nostalume/mountweave"
)

func TestWebsiteDirectoryBindingAndPaging(t *testing.T) {
	ctx := t.Context()
	first := make([]json.RawMessage, websitePageSize)
	for n := range first {
		first[n] = json.RawMessage(fmt.Sprintf(`{"fs_id":%d,"path":"/Archive/%03d","server_filename":"%03d","isdir":0,"size":1}`, uint64(9007199254741000)+uint64(n), n, n))
	}
	tail := json.RawMessage(`{"fs_id":9007199254741999,"path":"/Archive/tail","server_filename":"tail","isdir":0,"size":4}`)
	root := json.RawMessage(`{"fs_id":9007199254740993,"path":"/Archive","server_filename":"Archive","isdir":1}`)
	receipt := func(nodes ...json.RawMessage) json.RawMessage {
		raw, err := json.Marshal(struct {
			Errno int               `json:"errno"`
			List  []json.RawMessage `json:"list"`
		}{List: nodes})
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
	pages := 0
	closes := 0
	var rewrite func(*WebsitePage)
	var reading chan struct{}
	read := func(ctx context.Context, parent string, number int) (WebsitePage, error) {
		if reading != nil {
			close(reading)
			<-ctx.Done()
			return WebsitePage{}, ctx.Err()
		}
		got := WebsitePage{Account: "fixture-account", Directory: parent, Number: number}
		switch {
		case parent == "/" && number == 1:
			got.Body = receipt(root)
		case parent == "/Archive" && number == 1:
			got.Body = receipt(first...)
		case parent == "/Archive" && number == 2:
			got.Body = receipt(tail)
		default:
			return WebsitePage{}, errors.New("unexpected directory or page")
		}
		pages++
		if rewrite != nil {
			rewrite(&got)
		}
		return got, nil
	}
	options := WebsiteOptions{Account: "fixture-account", RootPath: "/Archive", RootID: "9007199254740993",
		ReadPage: read, Release: func() error { closes++; return nil }}
	wrong := options
	wrong.RootID = "9007199254740992"
	if d, err := OpenWebsite(ctx, wrong); d != nil || core.CodeOf(err) != core.UpstreamAccessDenied || closes != 0 {
		t.Fatalf("wrong root open = %v, releases = %d", err, closes)
	}
	driver, err := OpenWebsite(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	defer driver.Close()
	c, err := core.New(core.Options{Authorize: func(context.Context, core.Principal, core.Action, core.Address) error { return nil }})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close(ctx)
	if _, err := c.Install("/site", driver); err != nil {
		t.Fatal(err)
	}
	item, err := c.Stat(ctx, "u", core.Path("/site/tail"))
	if err != nil || item.Size == nil || *item.Size != 4 {
		t.Fatalf("lookup on second page: %+v, %v", item, err)
	}
	query := core.Children(core.Path("/site"))
	firstPage, err := c.List(ctx, "u", core.ListRequest{Query: query, Limit: websitePageSize, CursorTTL: time.Minute})
	if err != nil || len(firstPage.Items) != websitePageSize || firstPage.NextCursor == nil {
		t.Fatalf("first Core page: len=%d cursor=%t err=%v", len(firstPage.Items), firstPage.NextCursor != nil, err)
	}
	last, err := c.List(ctx, "u", core.ListRequest{Query: query, Cursor: *firstPage.NextCursor})
	if err != nil || len(last.Items) != 1 || last.Items[0].Name != "tail" || last.NextCursor != nil {
		t.Fatalf("final Core page: %+v, %v", last, err)
	}
	rewrite = func(p *WebsitePage) { p.Account = "changed" }
	if _, err := driver.Stat(ctx, core.NativePath("/")); core.CodeOf(err) != core.UpstreamAccessDenied {
		t.Fatalf("root stat with a changed account: %v", err)
	}
	for _, tc := range []struct {
		change func(*WebsitePage)
		code   core.Code
	}{
		{func(p *WebsitePage) { p.Account = "changed" }, core.UpstreamAccessDenied},
		{func(p *WebsitePage) { p.Directory = "/Different" }, core.InvalidUpstreamResponse},
		{func(p *WebsitePage) { p.Number++ }, core.InvalidUpstreamResponse},
		{func(p *WebsitePage) { p.Body = json.RawMessage(`{"errno":0}`) }, core.InvalidUpstreamResponse},
		{func(p *WebsitePage) { p.Body = receipt(tail, tail) }, core.InvalidUpstreamResponse},
	} {
		rewrite = tc.change
		page, err := c.List(ctx, "u", core.ListRequest{Query: query, CursorTTL: time.Minute})
		if errors.Is(err, io.EOF) || core.CodeOf(err) != tc.code || len(page.Items) != 0 {
			t.Fatalf("invalid receipt escaped: %+v, %v", page, err)
		}
	}
	rewrite = func(p *WebsitePage) {
		if p.Number == 2 {
			p.Body = receipt(first...)
		}
	}
	if _, err := c.Stat(ctx, "u", core.Path("/site/missing")); core.CodeOf(err) != core.InvalidUpstreamResponse {
		t.Fatalf("repeated full page in path lookup: %v", err)
	}
	rewrite = nil
	before := pages
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := driver.Stat(cancelled, core.NativePath("/tail")); !errors.Is(err, context.Canceled) || pages != before {
		t.Fatalf("cancelled stat = %v, pages = %d", err, pages-before)
	}
	iter, err := driver.Enumerate(ctx, core.NativeChildren(core.NativePath("/")))
	if err != nil {
		t.Fatal(err)
	}
	reading = make(chan struct{})
	next := make(chan error, 1)
	go func() { _, err := iter.Next(ctx, 1); next <- err }()
	<-reading
	_ = iter.Close()
	select {
	case err := <-next:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("closed iterator returned %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("closing an iterator retained its page request")
	}
	reading = nil
	if _, err := c.Open(ctx, "u", core.Path("/site/tail"), core.OpenOptions{}); core.CodeOf(err) != core.UnsupportedOperation {
		t.Fatalf("directory-only source accepted a body request: %v", err)
	}
	if err := c.Close(ctx); err != nil || closes != 1 {
		t.Fatalf("release count=%d, error=%v", closes, err)
	}
	if err := driver.Close(); err != nil || closes != 1 {
		t.Fatalf("second close release count=%d, error=%v", closes, err)
	}
}

type websiteFixtureBody struct {
	io.ReadCloser
	closes *atomic.Int32
}

func (b websiteFixtureBody) Close() error {
	b.closes.Add(1)
	return b.ReadCloser.Close()
}

func TestWebsiteOriginalReceipt(t *testing.T) {
	const filename = "/file.bin"
	want := WebsiteFile{Account: "fixture-account", Path: filename, ID: 9007199254740993, Size: 4}
	list := func(_ context.Context, dir string, number int) (WebsitePage, error) {
		return WebsitePage{Account: want.Account, Directory: dir, Number: number, Body: json.RawMessage(`{"errno":0,"list":[{"fs_id":9007199254740993,"path":"/file.bin","server_filename":"file.bin","isdir":0,"size":4}]}`)}, nil
	}
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/grant" {
			http.Redirect(w, r, "/original", http.StatusFound)
			return
		}
		if r.URL.Path != "/unrelated" {
			w.Header().Set("Content-Disposition", `attachment; filename="file.bin"`)
		}
		w.(http.Flusher).Flush() // Keep length unknown until the native stream reaches EOF.
		if r.URL.Path == "/short" {
			_, _ = io.WriteString(w, "abc")
			return
		}
		_, _ = io.WriteString(w, "abcd")
	}))
	defer srv.Close()
	for _, tc := range []struct {
		name, route string
		alter       func(*WebsiteFile)
		openErr     error
		code        core.Code
		short       bool
	}{
		{name: "whole original", route: "/grant"},
		{name: "wrong selected ID", route: "/grant", alter: func(f *WebsiteFile) { f.ID-- }, code: core.InvalidUpstreamResponse},
		{name: "unrelated HTTP 200", route: "/unrelated", code: core.InvalidUpstreamResponse},
		{name: "wrong account", route: "/grant", alter: func(f *WebsiteFile) { f.Account = "another" }, code: core.UpstreamAccessDenied},
		{name: "wrong path", route: "/grant", alter: func(f *WebsiteFile) { f.Path = "/other.bin" }, code: core.InvalidUpstreamResponse},
		{name: "grant error carrying a body", route: "/grant", openErr: core.Fail(core.UpstreamAccessDenied, nil), code: core.UpstreamAccessDenied},
		{name: "truncated original", route: "/short", short: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			defer cancel()
			var bodyCloses, releases atomic.Int32
			read := func(call context.Context, file WebsiteFile) (WebsiteDownload, error) {
				if file != want {
					t.Fatalf("selected file = %+v, want %+v", file, want)
				}
				grant := []byte(fmt.Sprintf(`{"errno":0,"dlink":[{"dlink":%q,"fs_id":9007199254740993}]}`, srv.URL+tc.route+"?fixture=1"))
				target, err := WebsiteDownloadTarget(file.ID, []byte("[9007199254740993]"), grant)
				if err != nil {
					return WebsiteDownload{}, err
				}
				req, err := http.NewRequestWithContext(call, http.MethodGet, target, nil)
				if err != nil {
					return WebsiteDownload{}, err
				}
				resp, err := srv.Client().Do(req)
				if err != nil {
					return WebsiteDownload{}, err
				}
				resp.Body = websiteFixtureBody{resp.Body, &bodyCloses}
				if tc.alter != nil {
					tc.alter(&file)
				}
				return WebsiteDownload{File: file, Response: resp}, tc.openErr
			}
			d, err := OpenWebsite(ctx, WebsiteOptions{Account: want.Account, RootPath: "/", ReadPage: list, ReadFile: read, Release: func() error { releases.Add(1); return nil }})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = d.Close() })
			c, err := core.New(core.Options{Authorize: func(context.Context, core.Principal, core.Action, core.Address) error { return nil }})
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close(ctx)
			if _, err := c.Install("/site", d); err != nil {
				t.Fatal(err)
			}
			got, err := c.Open(ctx, "u", core.Path("/site"+filename), core.OpenOptions{})
			if tc.code != "" {
				if core.CodeOf(err) != tc.code || got.Body != nil {
					t.Fatalf("rejected receipt: body=%t, err=%v", got.Body != nil, err)
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				data, readErr := io.ReadAll(got.Body)
				if closeErr := got.Body.Close(); closeErr != nil {
					t.Fatal(closeErr)
				}
				if tc.short && !errors.Is(readErr, io.ErrUnexpectedEOF) || !tc.short && (readErr != nil || string(data) != "abcd") {
					t.Fatalf("original = %q, err=%v", data, readErr)
				}
			}
			if err := c.Close(ctx); err != nil || bodyCloses.Load() != 1 || releases.Load() != 1 {
				t.Fatalf("cleanup: body=%d session=%d error=%v", bodyCloses.Load(), releases.Load(), err)
			}
		})
	}
}
