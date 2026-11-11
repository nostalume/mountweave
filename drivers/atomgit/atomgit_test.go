package atomgit

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	core "github.com/nostalume/mountweave"
)

type fixture func(*http.Request) (*http.Response, error)

func (f fixture) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestFixedCommitThroughCore(t *testing.T) {
	const repo = "/api/v5/repos/org/project"
	const commit = "c7d168c6e412392a6e21f68ff48c625157d1ac19"
	const folder, name = "docs", "a #%.bin"
	const payload = "\x00\xffabc"
	treeEntry := func(path, name, kind, mode string) string {
		return fmt.Sprintf(`{"sha":%q,"path":%q,"name":%q,"type":%q,"mode":%q}`, commit, path, name, kind, mode)
	}
	treeMode, rawMode := "", ""
	bodyCalls, treeCalls := 0, 0
	bodyStarted := make(chan struct{}, 1)
	client := &http.Client{Transport: fixture(func(r *http.Request) (*http.Response, error) {
		if r.URL.Scheme != "https" || r.URL.Host != "api.atomgit.com" ||
			r.Method != http.MethodGet || r.Header.Get("Accept-Encoding") != "identity" {
			return nil, errors.New("unexpected AtomGit request")
		}
		h := make(http.Header)
		status, body := http.StatusOK, ""
		q := r.URL.Query()
		switch r.URL.EscapedPath() {
		case repo + "/commits/main":
			body = fmt.Sprintf(`{"sha":%q}`, commit)
		case repo + "/git/trees/" + commit:
			treeCalls++
			if q.Get("per_page") != "100" {
				return nil, errors.New("inconsistent native page size")
			}
			page := q.Get("page")
			row := ""
			switch q.Get("file_path") {
			case "":
				switch page {
				case "1":
					row = treeEntry(folder, folder, "tree", "040000")
				case "2":
					row = treeEntry("link", "link", "blob", "120000") + "," +
						treeEntry("other", "other", "blob", "100644")
				case "3":
				default:
					return nil, errors.New("wrong root page")
				}
			case folder:
				switch page {
				case "1":
					row = treeEntry(folder+"/"+name, name, "blob", "100755")
				case "2":
					if treeMode == "repeat" {
						row = treeEntry(folder+"/"+name, name, "blob", "100755")
					}
				default:
					return nil, errors.New("wrong subdirectory page")
				}
			default:
				return nil, errors.New("unexpected native parent")
			}
			switch treeMode {
			case "missing":
				body = "{}"
			case "other commit":
				body = `{"sha":"other","tree":[]}`
			default:
				body = `{"tree":[` + row + `]}`
			}
		case repo + "/raw/docs%2Fa%20%23%25.bin":
			bodyCalls++
			if q.Get("ref") != commit {
				return nil, errors.New("blob request did not use the fixed commit")
			}
			switch rawMode {
			case "redirect":
				status = http.StatusFound
				h.Set("Location", "https://outside.example/file")
			case "cancel":
				bodyStarted <- struct{}{}
				<-r.Context().Done()
				return nil, r.Context().Err()
			default:
				body = payload
				if r.Header.Get("Range") == "bytes=1-2" {
					status, body = http.StatusPartialContent, payload[1:3]
					h.Set("Content-Range", "bytes 1-2/5")
				}
			}
		default:
			return nil, fmt.Errorf("unexpected route %s", r.URL.EscapedPath())
		}
		return &http.Response{StatusCode: status, Header: h, Body: io.NopCloser(strings.NewReader(body)),
			ContentLength: int64(len(body)), Request: r}, nil
	})}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	d, err := Open(ctx, Options{Owner: "org", Repository: "project", Ref: "main", Client: client})
	if err != nil {
		t.Fatal(err)
	}
	c, err := core.New(core.Options{Authorize: func(context.Context, core.Principal, core.Action, core.Address) error { return nil }})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close(context.Background())
	mount, err := c.Install("/repo", d)
	if err != nil {
		t.Fatal(err)
	}
	q := core.ListRequest{Query: core.Children(core.Path("/repo")), Limit: 1, CursorTTL: time.Minute}
	for i, name := range []string{folder, "other", ""} {
		page, err := c.List(ctx, "reader", q)
		if err != nil || (page.NextCursor == nil) != (i == 2) {
			t.Fatalf("page %d: %v %v", i, page, err)
		}
		if name == "" && len(page.Items) != 0 || name != "" && (len(page.Items) != 1 || page.Items[0].Name != name) {
			t.Fatalf("wrong page items: %v", page.Items)
		}
		if page.NextCursor != nil {
			q.Cursor = *page.NextCursor
		}
	}
	if treeCalls != 3 {
		t.Fatalf("unexpected native page count: %d", treeCalls)
	}
	sub, err := Open(ctx, Options{Owner: "org", Repository: "project", Ref: "main", RootPath: "/docs", Client: client})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Install("/sub", sub); err != nil {
		t.Fatal(err)
	}
	source := core.Path("/sub/" + name)
	if _, err := c.Open(ctx, "reader", core.Path("/repo/link"), core.OpenOptions{}); core.CodeOf(err) != core.UnsupportedOperation {
		t.Fatalf("link lookup: %v", err)
	}
	for _, mode := range []string{"missing", "other commit", "repeat"} {
		treeMode = mode
		if _, err := c.Open(ctx, "reader", source, core.OpenOptions{}); core.CodeOf(err) != core.InvalidUpstreamResponse || bodyCalls != 0 {
			t.Fatalf("%s: %v, raw requests %d", mode, err, bodyCalls)
		}
	}
	treeMode = ""
	opened, err := c.Open(ctx, "reader", source, core.OpenOptions{Range: &core.Range{Start: 1, End: 2}})
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(opened.Body)
	if err = errors.Join(err, opened.Body.Close()); err != nil || string(got) != payload[1:3] || opened.Range == nil {
		t.Fatalf("range: %q %v %v", got, opened.Range, err)
	}
	opened, err = c.Open(ctx, "reader", source, core.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	got, err = io.ReadAll(opened.Body)
	if err = errors.Join(err, opened.Body.Close()); err != nil || string(got) != payload || opened.Range != nil {
		t.Fatalf("full body: %q %v %v", got, opened.Range, err)
	}
	rawMode = "redirect"
	if _, err := c.Open(ctx, "reader", source, core.OpenOptions{}); core.CodeOf(err) != core.UpstreamFailure {
		t.Fatalf("unexpected redirect result: %v", err)
	}
	rawMode = "cancel"
	readCtx, stop := context.WithCancel(ctx)
	defer stop()
	go func() {
		select {
		case <-bodyStarted:
			stop()
		case <-readCtx.Done():
		}
	}()
	if _, err := c.Open(readCtx, "reader", source, core.OpenOptions{}); !errors.Is(err, context.Canceled) || bodyCalls != 4 {
		t.Fatalf("body cancellation: %v, %d requests", err, bodyCalls)
	}
	if err := mount.Retire(ctx); err != nil {
		t.Fatal(err)
	}
}
