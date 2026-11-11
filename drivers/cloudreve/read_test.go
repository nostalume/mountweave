package cloudreve_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	core "github.com/nostalume/mountweave"
	driver "github.com/nostalume/mountweave/drivers/cloudreve"
)

type transport func(*http.Request) (*http.Response, error)

func (f transport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
func reply(r *http.Request, status, code int, data any) *http.Response {
	body, _ := json.Marshal(map[string]any{"code": code, "msg": "native response", "data": data})
	return &http.Response{Request: r, StatusCode: status, Header: make(http.Header), Body: io.NopCloser(bytes.NewReader(body)), ContentLength: int64(len(body))}
}
func file(id, name, uri string, kind int, size int64) map[string]any {
	return map[string]any{"id": id, "name": name, "path": uri, "type": kind, "size": size}
}

func TestReadWorkflow(t *testing.T) {
	ctx := context.Background()
	const root = "cloudreve://my/vault"
	numbered := false
	links := 0
	api := &http.Client{Transport: transport(func(r *http.Request) (*http.Response, error) {
		// Prepared account transport adds credentials only to the API recipient.
		r.Header.Set("Authorization", "Bearer account")
		if r.URL.Host != "cloudreve.test" || !strings.HasPrefix(r.URL.Path, "/api/v4/") {
			t.Errorf("account credential recipient: %s", r.URL)
			return nil, errors.New("wrong API recipient")
		}
		switch r.URL.Path {
		case "/api/v4/file/info":
			uri := r.URL.Query().Get("uri")
			switch uri {
			case "cloudreve://my":
				t.Error("file info does not admit the account root")
				return reply(r, 200, 40019, nil), nil
			case root:
				return reply(r, 200, 0, file("vault", "vault", uri, 1, 0)), nil
			case root + "/a", root + "/b":
				return reply(r, 200, 0, file(uri[len(uri)-1:], uri[len(uri)-1:], uri, 0, 999)), nil
			}
			return reply(r, 200, 40044, nil), nil
		case "/api/v4/file":
			if r.URL.Query().Get("uri") == "cloudreve://my" {
				return reply(r, 200, 0, map[string]any{"files": []any{}, "parent": file("home", "", "cloudreve://my", 1, 0), "pagination": map[string]any{"is_cursor": true}}), nil
			}
			if r.URL.Query().Get("uri") != root {
				t.Errorf("list root: %s", r.URL)
			}
			name, next := "a", "second"
			if r.URL.Query().Get("next_page_token") == next {
				name, next = "b", ""
			}
			if numbered {
				if r.URL.Query().Get("page_size") != "1" {
					t.Error("numbered pages changed their native size")
				}
				page := 0
				if r.URL.Query().Get("page") == "1" {
					name, page = "b", 1
				}
				return reply(r, 200, 0, map[string]any{"files": []any{file(name, name, root+"/"+name, 0, 999)}, "parent": file("vault", "vault", root, 1, 0), "pagination": map[string]any{"page": page, "page_size": 1, "total_items": 2}}), nil
			}
			return reply(r, 200, 0, map[string]any{"files": []any{file(name, name, root+"/"+name, 0, 999)}, "parent": file("vault", "vault", root, 1, 0), "pagination": map[string]any{"is_cursor": true, "next_token": next}}), nil
		case "/api/v4/file/url":
			links++
			var query struct {
				URIs []string `json:"uris"`
			}
			if err := json.NewDecoder(r.Body).Decode(&query); err != nil || len(query.URIs) != 1 || query.URIs[0] != root+"/a" {
				t.Errorf("content selection: %+v, %v", query, err)
			}
			return reply(r, 200, 0, map[string]any{"urls": []any{map[string]string{"url": "https://content.test/a?capability=read"}}}), nil
		}
		return reply(r, 404, 404, nil), nil
	})}
	content := &http.Client{Transport: transport(func(r *http.Request) (*http.Response, error) {
		if r.URL.Host != "content.test" || r.Header.Get("Authorization") != "" || r.Header.Get("Range") != "bytes=1-2" || r.Header.Get("Accept-Encoding") != "identity" {
			t.Errorf("content request: %s %+v", r.URL, r.Header)
		}
		return &http.Response{Request: r, StatusCode: 206, Header: http.Header{"Content-Range": {"bytes 1-2/4"}}, Body: io.NopCloser(strings.NewReader("bc")), ContentLength: 2}, nil
	})}
	opts := driver.Options{Endpoint: "https://cloudreve.test", RootURI: root, APIClient: api, ContentClient: content}
	home := opts
	home.RootURI = "cloudreve://my/"
	accountRoot, err := driver.Open(ctx, home)
	if err != nil {
		t.Fatal(err)
	}
	if err := accountRoot.Close(); err != nil {
		t.Fatal(err)
	}
	first, err := driver.Open(ctx, opts)
	if err != nil {
		t.Fatal(err)
	}
	second, err := driver.Open(ctx, opts)
	if err != nil || first == second {
		t.Fatalf("independent construction: %v", err)
	}
	defer second.Close()
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := first.Stat(ctx, core.NativePath("/")); core.CodeOf(err) != core.MountUnavailable {
		t.Fatalf("closed instance: %v", err)
	}
	iter, err := second.Enumerate(ctx, core.NativeChildren(core.NativePath("/")))
	if err != nil {
		t.Fatal(err)
	}
	for index, name := range []string{"a", "b"} {
		items, err := iter.Next(ctx, 1)
		if len(items) != 1 || items[0].Name != name || (index == 0 && err != nil) || (index == 1 && err != io.EOF) {
			t.Fatalf("native page: %+v %v", items, err)
		}
	}
	if err := iter.Close(); err != nil {
		t.Fatal(err)
	}
	numbered = true
	iter, err = second.Enumerate(ctx, core.NativeChildren(core.NativePath("/")))
	if err != nil {
		t.Fatal(err)
	}
	for index, hint := range []int{1, 5} {
		items, err := iter.Next(ctx, hint)
		if len(items) != 1 || items[0].Name != []string{"a", "b"}[index] || (index == 0 && err != nil) || (index == 1 && err != io.EOF) {
			t.Fatalf("numbered page: %+v %v", items, err)
		}
	}
	if err := iter.Close(); err != nil {
		t.Fatal(err)
	}
	absent := core.OpenOptions{Selection: core.ReadSelection{When: core.IfAbsent()}}
	if _, err := second.Open(ctx, core.NativePath("/missing"), absent); core.CodeOf(err) != core.NotFound {
		t.Fatalf("condition invented a file: %v", err)
	}
	if _, err := second.Open(ctx, core.NativePath("/a"), absent); core.CodeOf(err) != core.PreconditionFailed || links != 0 {
		t.Fatalf("failed condition reached download authorization: %v", err)
	}
	opened, err := second.Open(ctx, core.NativePath("/a"), core.OpenOptions{Range: &core.Range{Start: 1, End: 2}})
	if err != nil {
		t.Fatal(err)
	}
	body, readErr := io.ReadAll(opened.Body)
	if err := errors.Join(readErr, opened.Body.Close()); err != nil || string(body) != "bc" || opened.Metadata.Size == nil || *opened.Metadata.Size != 4 {
		t.Fatalf("actual content metadata/body: %+v %q %v", opened.Metadata, body, err)
	}
	started := make(chan struct{})
	content.Transport = transport(func(r *http.Request) (*http.Response, error) {
		close(started)
		<-r.Context().Done()
		return nil, r.Context().Err()
	})
	// A new instance borrows the changed transport; existing clients stay isolated.
	third, err := driver.Open(ctx, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer third.Close()
	cancelled, cancel := context.WithCancel(ctx)
	result := make(chan error, 1)
	go func() { _, err := third.Open(cancelled, core.NativePath("/a"), core.OpenOptions{}); result <- err }()
	<-started
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled content: %v", err)
	}
	for _, uri := range []string{"cloudreve://share/x", "cloudreve://my/vault?name=x", "cloudreve://user@my/vault"} {
		invalid := opts
		invalid.RootURI = uri
		if err := invalid.Validate(); err == nil {
			t.Fatalf("accepted nonordinary root %q", uri)
		}
	}
	for _, redirect := range []bool{false, true} {
		bad := opts
		bad.APIClient = &http.Client{Transport: transport(func(r *http.Request) (*http.Response, error) {
			uri := r.URL.Query().Get("uri")
			f := file("home", "", uri, 1, 0)
			if uri == root {
				f = file("vault", "vault", root+"-other", 1, 0)
				if redirect {
					f["path"], f["metadata"] = root, map[string]string{"sys:shared_redirect": "cloudreve://share/foreign"}
				}
			}
			return reply(r, 200, 0, f), nil
		})}
		failed, err := driver.Open(ctx, bad)
		if err == nil {
			t.Fatalf("accepted root substitution, redirect=%v", redirect)
		}
		if failed != nil {
			if err := failed.Close(); err != nil {
				t.Fatal(err)
			}
		}
	}
}
