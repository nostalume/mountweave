package cnbreleases

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/local"
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

func TestLivePublishedTagAssets(t *testing.T) {
	const base = "/org/team/project/-/releases"
	const tag = "series/v1"
	const name = "file%中#.json"
	const data = "abcdef"
	const asset = `{"id":"asset-one","name":"file%中#.json","size":6,"url":"https://ignored.example/not-the-selected-asset"}`
	response := func(status int, data string) *http.Response {
		return &http.Response{StatusCode: status, Header: make(http.Header),
			Body: io.NopCloser(strings.NewReader(data)), ContentLength: int64(len(data))}
	}
	mode := ""
	releases, downloads, bodies := 0, 0, 0
	started := make(chan struct{})
	api := &http.Client{Transport: wire(func(r *http.Request) (*http.Response, error) {
		if r.Method != http.MethodGet || r.URL.Scheme != "https" || r.URL.Host != "api.cnb.cool" ||
			r.URL.RawQuery != "" || r.Header.Get("Cookie") != "" || r.Header.Get("Range") != "" {
			return nil, errors.New("incorrect CNB control receiver")
		}
		switch r.URL.EscapedPath() {
		case base + "/tags/series%2Fv1":
			releases++
			if r.Header.Get("Accept") != "application/json" {
				return nil, errors.New("release details request did not select JSON")
			}
			id, actual, draft, selected := "release-one", tag, false, asset
			switch mode {
			case "wait":
				close(started)
				<-r.Context().Done()
				return nil, r.Context().Err()
			case "retagged":
				id = "release-two"
				selected = strings.Replace(asset, "asset-one", "asset-two", 1)
				selected = strings.Replace(selected, `"size":6`, `"size":4`, 1)
			case "wrong-tag":
				actual = "another-tag"
			case "draft":
				draft = true
			}
			other := `{"id":"extra","name":"more","size":0}`
			if mode == "ambiguous" {
				other = strings.Replace(asset, "asset-one", "asset-two", 1)
			}
			return response(200, fmt.Sprintf(`{"id":%q,"tag_name":%q,"draft":%t,"assets":[%s,%s]}`, id, actual, draft, selected, other)), nil
		case base + "/download/series%2Fv1/file%25%E4%B8%AD%23.json":
			downloads++
			if mode == "no-redirect" {
				return response(200, `{"url":"private"}`), nil
			}
			reply := response(302, "")
			target := "https://assets.example/file?sig=private%2fvalue"
			if mode == "unsafe" {
				target = "http://untrusted.example/file?sig=private%2fvalue"
			}
			reply.Header.Set("Location", target)
			return reply, nil
		default:
			return nil, fmt.Errorf("request escaped its configured repository or tag: %s", r.URL.Path)
		}
	})}
	var broken *failedClose
	content := &http.Client{Transport: wire(func(r *http.Request) (*http.Response, error) {
		bodies++
		if r.URL.Scheme != "https" || r.URL.Host != "assets.example" || r.URL.RawQuery != "sig=private%2fvalue" ||
			r.Header.Get("Authorization") != "" || r.Header.Get("Cookie") != "" || r.Header.Get("Referer") != "" {
			return nil, errors.New("incorrect credential-free content request")
		}
		if mode == "close-failure" {
			reply := response(404, "")
			broken = &failedClose{ReadCloser: reply.Body, err: fmt.Errorf("closing %s: %w", r.URL, io.EOF)}
			reply.Body = broken
			return reply, nil
		}
		if mode == "retagged" {
			return response(200, "next"), nil
		}
		if r.Header.Get("Range") != "bytes=1-3" {
			return nil, errors.New("delegated download lost its range")
		}
		reply := response(206, data[1:4])
		reply.Header.Set("Content-Range", "bytes 1-3/6")
		return reply, nil
	})}
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()
	driver, err := Open(ctx, Options{Repository: "org/team/project", Tag: tag, Client: api, ContentClient: content})
	if err != nil {
		t.Fatal(err)
	}
	storage, err := core.New(core.Options{Authorize: func(context.Context, core.Principal, core.Action, core.Address) error { return nil }})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = storage.Close(ctx) }()
	mount, err := storage.Install("/release", driver)
	if err != nil {
		t.Fatal(err)
	}
	req := core.ListRequest{Query: core.Children(core.Path("/release")), Limit: 1, CursorTTL: time.Minute}
	first, err := storage.List(ctx, "u", req)
	if err != nil || len(first.Items) != 1 || first.Items[0].Name != name || first.NextCursor == nil {
		t.Fatal(first, err)
	}
	req.Cursor = *first.NextCursor
	if more, err := storage.List(ctx, "u", req); err != nil || len(more.Items) != 1 ||
		more.Items[0].Name != "more" || more.NextCursor != nil || releases != 2 {
		t.Fatal("public page refetched the native release", more, err, releases)
	}
	target := core.Path("/release/" + name)
	for _, tc := range []struct {
		mode string
		code core.Code
	}{{"ambiguous", core.PathAmbiguous}, {"wrong-tag", core.InvalidUpstreamResponse}, {"draft", core.NotFound},
		{"unsafe", core.InvalidUpstreamResponse}, {"no-redirect", core.InvalidUpstreamResponse}} {
		mode = tc.mode
		if _, err := storage.Open(ctx, "u", target, core.OpenOptions{}); core.CodeOf(err) != tc.code ||
			strings.Contains(err.Error(), "private") || bodies != 0 {
			t.Fatal(tc.mode, err, bodies)
		}
	}
	if downloads != 2 {
		t.Fatal("a rejected release or ambiguous asset reached the download endpoint", downloads)
	}
	mode = ""
	opened, err := storage.Open(ctx, "u", target, core.OpenOptions{Range: &core.Range{Start: 1, End: 3}})
	if err != nil {
		t.Fatal(err)
	}
	raw, err := io.ReadAll(opened.Body)
	if err := errors.Join(err, opened.Body.Close()); err != nil || string(raw) != "bcd" ||
		opened.Range == nil || opened.Range.Total != 6 || bodies != 1 {
		t.Fatal(string(raw), opened.Range, err, bodies)
	}
	mode = "retagged"
	root := t.TempDir()
	destination, err := local.New(root)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := storage.Install("/local", destination); err != nil {
		t.Fatal(err)
	}
	if change, err := storage.Copy(ctx, "u", target, core.Path("/local/copied")); err != nil || change.Effect != core.Applied {
		t.Fatal(change, err)
	}
	if copied, err := os.ReadFile(filepath.Join(root, "copied")); err != nil || string(copied) != "next" {
		t.Fatal(string(copied), err)
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
		t.Fatal("release detail request did not start")
	}
	if err := mount.Retire(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-done; !errors.Is(err, context.Canceled) || broken.calls != 2 {
		t.Fatal("retirement did not join and retry cleanup", err, broken.calls)
	}
}
