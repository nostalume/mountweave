package githubreleases

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

type wire func(*http.Request) (*http.Response, error)

func (w wire) RoundTrip(req *http.Request) (*http.Response, error) { return w(req) }

type failedClose struct {
	io.ReadCloser
	err   error
	calls int
}

func (b *failedClose) Close() error {
	b.calls++
	if b.calls == 1 {
		return b.err
	}
	return b.ReadCloser.Close()
}

func TestPinnedReleaseAssets(t *testing.T) {
	const base = "/repos/acme/project/releases"
	const release = "/9007199254740993"
	const asset = "/assets/9007199254740995"
	const name = "archive%中.zip"
	const body = "abcdef"
	const file = `{"id":9007199254740995,"name":"archive%中.zip","state":"uploaded","size":6}`
	response := func(status int, data string) *http.Response {
		return &http.Response{StatusCode: status, Header: make(http.Header),
			Body: io.NopCloser(strings.NewReader(data)), ContentLength: int64(len(data))}
	}
	mode := ""
	tags, pages, assetCalls, contentCalls := 0, 0, 0, 0
	started := make(chan struct{})
	api := &http.Client{Transport: wire(func(r *http.Request) (*http.Response, error) {
		if r.Method != http.MethodGet || r.URL.Scheme != "https" || r.URL.Host != "api.github.com" ||
			r.Header.Get("X-GitHub-Api-Version") != "2022-11-28" || r.Header.Get("Cookie") != "" {
			return nil, errors.New("incorrect account API receiver")
		}
		switch r.URL.EscapedPath() {
		case base + "/tags/stable%2Fv1":
			tags++
			return response(200, `{"id":9007199254740993,"tag_name":"stable/v1","draft":false}`), nil
		case base + release:
			return response(200, `{"id":9007199254740993,"tag_name":"renamed-upstream"}`), nil
		case base + release + "/assets":
			pages++
		case base + asset:
			assetCalls++
			if r.Header.Get("Accept") != "application/octet-stream" {
				return nil, errors.New("binary asset was requested as metadata")
			}
			if mode == "" && r.Header.Get("Range") == "" {
				return response(200, body), nil
			}
			reply := response(302, "")
			target := "https://assets.example/file?sig=private%2fvalue"
			if mode == "unsafe" {
				target = "http://untrusted.example/file?sig=private%2fvalue"
			}
			reply.Header.Set("Location", target)
			return reply, nil
		default:
			return nil, fmt.Errorf("request escaped its repository and release: %s", r.URL.Path)
		}
		if r.Header.Get("Accept") != "application/vnd.github+json" || r.URL.Query().Get("per_page") != "100" {
			return nil, errors.New("incorrect asset page request")
		}
		if mode == "wait" {
			close(started)
			<-r.Context().Done()
			return nil, r.Context().Err()
		}
		switch r.URL.Query().Get("page") {
		case "1":
			reply := response(200, `[`+file+`,{"id":5,"name":"more","state":"uploaded","size":0}]`)
			continuation := `<https://api.github.com/repositories/77/releases/9007199254740993/assets?per_page=100&page=2>; rel="next"`
			if mode == "bad-link" {
				continuation = strings.Replace(continuation, "api.github.com", "untrusted.example", 1)
			}
			reply.Header.Set("Link", continuation)
			return reply, nil
		case "2":
			last := `{"id":7,"name":"last","state":"uploaded","size":0}`
			if mode == "ambiguous" {
				last = strings.Replace(file, "9007199254740995", "7", 1)
			}
			return response(200, `[`+last+`,{"id":8,"name":"pending","state":"starter","size":0}]`), nil
		default:
			return nil, errors.New("pagination passed the native end")
		}
	})}
	var broken *failedClose
	content := &http.Client{Transport: wire(func(r *http.Request) (*http.Response, error) {
		contentCalls++
		if r.URL.Scheme != "https" || r.URL.Host != "assets.example" || r.URL.RawQuery != "sig=private%2fvalue" ||
			r.Header.Get("Authorization") != "" || r.Header.Get("Cookie") != "" || r.Header.Get("Referer") != "" ||
			r.Header.Get("X-GitHub-Api-Version") != "" {
			return nil, errors.New("invalid or credentialed content request")
		}
		if mode == "close-failure" {
			reply := response(404, "")
			broken = &failedClose{ReadCloser: reply.Body, err: fmt.Errorf("closing %s: %w", r.URL, io.EOF)}
			reply.Body = broken
			return reply, nil
		}
		if r.Header.Get("Range") != "bytes=1-3" {
			return nil, errors.New("range was lost in the authorized redirect")
		}
		reply := response(206, "bcd")
		reply.Header.Set("Content-Range", "bytes 1-3/6")
		return reply, nil
	})}
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()
	driver, err := Open(ctx, Options{Owner: "acme", Repository: "project", Tag: "stable/v1", Client: api, ContentClient: content})
	if err != nil {
		t.Fatal(err)
	}
	storage, err := core.New(core.Options{Authorize: func(_ context.Context, _ core.Principal, _ core.Action, a core.Address) error {
		if strings.HasSuffix(a.Value(), "/denied") {
			return core.Fail(core.PermissionDenied, nil)
		}
		return nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = storage.Close(ctx) }()
	mount, err := storage.Install("/release", driver)
	if err != nil {
		t.Fatal(err)
	}
	if root, err := storage.Stat(ctx, "u", core.Path("/release")); err != nil || root.Kind != core.Directory || tags != 1 {
		t.Fatal("root followed the old tag again", root, err, tags)
	}
	req := core.ListRequest{Query: core.Children(core.Path("/release")), Limit: 1, CursorTTL: time.Minute}
	first, err := storage.List(ctx, "u", req)
	if err != nil || len(first.Items) != 1 || first.Items[0].Name != name || first.NextCursor == nil {
		t.Fatal(first, err)
	}
	req.Cursor = *first.NextCursor
	before := pages
	second, err := storage.List(ctx, "u", req)
	if err != nil || len(second.Items) != 1 || second.Items[0].Name != "more" || second.NextCursor == nil {
		t.Fatal(second, err)
	}
	if _, err := storage.List(ctx, "u", req); err != nil || pages != before {
		t.Fatal("Core replay fetched another asset page", err, pages)
	}
	req.Cursor = *second.NextCursor
	if last, err := storage.List(ctx, "u", req); err != nil || len(last.Items) != 1 || last.Items[0].Name != "last" || last.NextCursor != nil {
		t.Fatal(last, err)
	}
	target := core.Path("/release/" + name)
	before = pages
	if _, err := storage.Open(ctx, "u", core.Path("/release/denied"), core.OpenOptions{}); core.CodeOf(err) != core.PermissionDenied || pages != before {
		t.Fatal("authorization did not precede native I/O", err)
	}
	for _, tc := range []struct {
		mode string
		code core.Code
	}{{"ambiguous", core.PathAmbiguous}, {"bad-link", core.InvalidUpstreamResponse}, {"unsafe", core.InvalidUpstreamResponse}} {
		mode = tc.mode
		if _, err := storage.Open(ctx, "u", target, core.OpenOptions{}); core.CodeOf(err) != tc.code ||
			strings.Contains(err.Error(), "private") || contentCalls != 0 {
			t.Fatal(tc.mode, err, contentCalls)
		}
	}
	mode = ""
	opened, err := storage.Open(ctx, "u", target, core.OpenOptions{Range: &core.Range{Start: 1, End: 3}})
	if err != nil {
		t.Fatal(err)
	}
	bytes, err := io.ReadAll(opened.Body)
	if err := errors.Join(err, opened.Body.Close()); err != nil || string(bytes) != "bcd" || opened.Range == nil ||
		opened.Range.Total != 6 || assetCalls != 2 || contentCalls != 1 {
		t.Fatal(string(bytes), opened.Range, err, assetCalls, contentCalls)
	}
	opened, err = storage.Open(ctx, "u", target, core.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	bytes, err = io.ReadAll(opened.Body)
	if err = errors.Join(err, opened.Body.Close()); err != nil || string(bytes) != body || opened.Range != nil {
		t.Fatalf("full original: %q, range=%v, error=%v", bytes, opened.Range, err)
	}
	mode = "close-failure"
	if _, err := storage.Open(ctx, "u", target, core.OpenOptions{}); core.CodeOf(err) != core.NotFound ||
		!errors.Is(err, io.ErrUnexpectedEOF) || strings.Contains(err.Error(), "private") || broken == nil || broken.calls != 1 {
		t.Fatal("failed signed response lost its private cleanup", err)
	}
	mode = "wait"
	done := make(chan error, 1)
	go func() { _, err := storage.Stat(ctx, "u", target); done <- err }()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("asset page did not start")
	}
	if err := mount.Retire(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-done; !errors.Is(err, context.Canceled) || broken.calls != 2 {
		t.Fatal("retirement did not join and clean up", err, broken.calls)
	}
}
