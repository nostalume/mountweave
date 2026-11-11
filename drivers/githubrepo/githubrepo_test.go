package githubrepo_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/githubrepo"
)

type transport func(*http.Request) (*http.Response, error)

func (f transport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

var errClose = errors.New("tree response close failed")

type closeOnce struct {
	io.ReadCloser
	calls *atomic.Int32
}

func (b closeOnce) Close() error {
	err := b.ReadCloser.Close()
	if b.calls.Add(1) == 1 {
		return errors.Join(err, errClose)
	}
	return err
}

func TestRepositoryThroughCore(t *testing.T) {
	const prefix = "/repos/example/project"
	const data = "version https://git-lfs.github.com/spec/v1\noid sha256:fixture\nsize 40000\n"
	const object = "obj % 中.lfs"
	sha := func(n int) string { return fmt.Sprintf("%040x", n) }
	root, selected, incomplete := sha(1), sha(2), sha(3)
	blob, other := sha(4), sha(5)
	var refs, trees, blobs, closes, unframed atomic.Int32
	var moved, failClose, redirect atomic.Bool
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.Header.Get("Authorization") != "Bearer fixture" ||
			r.Header.Get("Accept-Encoding") != "identity" || r.URL.RawQuery != "" {
			t.Errorf("unexpected GitHub request: %s %s", r.Method, r.URL)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		tree := func(id string, truncated bool, files string) {
			trees.Add(1)
			_, _ = fmt.Fprintf(w, `{"sha":%q,"truncated":%t,"tree":[%s]}`, id, truncated, files)
		}
		switch r.URL.Path {
		case prefix + "/commits/heads/topic/feature":
			if refs.Add(1) != 1 || moved.Load() || !strings.HasSuffix(r.URL.EscapedPath(), "/heads%2Ftopic%2Ffeature") {
				t.Error("the mount did not resolve its explicit ref just once")
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"commit": map[string]any{"tree": map[string]string{"sha": root}},
			})
		case prefix + "/git/trees/" + root:
			tree(root, false, fmt.Sprintf(`{"path":"src","mode":"040000","type":"tree","sha":%q}`, selected))
		case prefix + "/git/trees/" + selected:
			tree(selected, false, fmt.Sprintf(`{"path":%q,"mode":"100644","type":"blob","sha":%q,"size":%d},
				{"path":"other","mode":"100755","type":"blob","sha":%q,"size":1},
				{"path":"bad","mode":"040000","type":"tree","sha":%q},
				{"path":"link","mode":"120000","type":"blob","sha":%q},
				{"path":"module","mode":"160000","type":"commit","sha":%q}`, object, blob, len(data), other, incomplete, sha(6), sha(7)))
		case prefix + "/git/trees/" + incomplete:
			tree(incomplete, true, "")
		case prefix + "/git/blobs/" + blob:
			blobs.Add(1)
			if r.Header.Get("Accept") != "application/vnd.github.raw+json" {
				t.Error("blob request did not ask for raw bytes")
			}
			if redirect.Load() {
				w.Header().Set("Location", "https://foreign.invalid/credential")
				w.WriteHeader(http.StatusFound)
				return
			}
			if mode := unframed.Load(); mode != 0 {
				w.(http.Flusher).Flush()
				content := data
				switch mode {
				case 2:
					content += "!"
				case 3:
					content = content[:len(content)-1]
				}
				_, _ = io.WriteString(w, content)
			} else if r.Header.Get("Range") == "bytes=1-3" {
				w.Header().Set("Content-Range", fmt.Sprintf("bytes 1-3/%d", len(data)))
				w.WriteHeader(http.StatusPartialContent)
				_, _ = io.WriteString(w, data[1:4])
			} else {
				_, _ = io.WriteString(w, data)
			}
		default:
			t.Errorf("unexpected repository request: %s", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(server.Close)

	client := server.Client()
	loopback, _ := url.Parse(server.URL)
	base := client.Transport
	client.Transport = transport(func(r *http.Request) (*http.Response, error) {
		if r.URL.Scheme != "https" || r.URL.Host != "api.github.com" {
			t.Error("repository request left the GitHub API origin")
			return nil, errors.New("unexpected origin")
		}
		req := r.Clone(r.Context())
		req.URL.Host = loopback.Host
		req.Header.Set("Authorization", "Bearer fixture")
		resp, err := base.RoundTrip(req)
		if err == nil && failClose.Swap(false) {
			resp.Body = closeOnce{resp.Body, &closes}
		}
		return resp, err
	})
	driver, err := githubrepo.Open(ctx, githubrepo.Options{
		Owner: "example", Repository: "project", Ref: "heads/topic/feature", RootPath: "/src", Client: client,
	})
	if err != nil {
		if driver != nil {
			_ = driver.Close()
		}
		t.Fatal(err)
	}
	moved.Store(true)
	if trees.Load() != 1 {
		t.Fatal("constructing a subroot did not stop at its native tree identity")
	}
	files, err := core.New(core.Options{
		MaxCursors: 3, Authorize: func(context.Context, core.Principal, core.Action, core.Address) error { return nil },
	})
	if err != nil {
		_ = driver.Close()
		t.Fatal(err)
	}
	defer func() {
		if err := files.Close(ctx); err != nil {
			t.Error(err)
		}
	}()
	mount, err := files.Install("/repo", driver)
	if err != nil {
		_ = driver.Close()
		t.Fatal(err)
	}

	request := core.ListRequest{Query: core.Children(core.Path("/repo")), Limit: 1, CursorTTL: time.Minute}
	var names []string
	for range 4 {
		page, err := files.List(ctx, "reader", request)
		if err != nil {
			t.Fatal(err)
		}
		for _, item := range page.Items {
			names = append(names, item.Name)
		}
		if page.NextCursor == nil {
			break
		}
		request.Cursor = *page.NextCursor
	}
	if got := strings.Join(names, "|"); got != object+"|other|bad" || trees.Load() != 2 {
		t.Fatalf("expected one nonrecursive directory read and Core pages, got %q across %d trees", got, trees.Load())
	}
	if _, err := files.List(ctx, "reader", core.ListRequest{Query: core.Children(core.Path("/repo/bad")), Limit: 1, CursorTTL: time.Minute}); core.CodeOf(err) != core.InvalidUpstreamResponse {
		t.Fatalf("truncated tree was accepted: %v", err)
	}
	for _, name := range []string{"link", "module/foreign"} {
		if _, err := files.Open(ctx, "reader", core.Path("/repo/"+name), core.OpenOptions{}); core.CodeOf(err) != core.UnsupportedOperation || blobs.Load() != 0 {
			t.Fatalf("special node was followed: %v", err)
		}
	}
	address := core.Path("/repo/" + object)
	stat, err := files.Stat(ctx, "reader", address)
	if err != nil || stat.Name != object || stat.Size == nil || *stat.Size != int64(len(data)) {
		t.Fatalf("blob metadata: %+v %v", stat, err)
	}
	condition := core.OpenOptions{Selection: core.ReadSelection{When: core.IfAbsent()}}
	if _, err := files.Open(ctx, "reader", core.Path("/repo/missing"), condition); core.CodeOf(err) != core.NotFound {
		t.Fatalf("condition invented an existing blob: %v", err)
	}
	if _, err := files.Open(ctx, "reader", address, condition); core.CodeOf(err) != core.PreconditionFailed || blobs.Load() != 0 {
		t.Fatalf("failed condition reached the blob body: %v", err)
	}
	opened, err := files.Open(ctx, "reader", address, core.OpenOptions{Range: &core.Range{Start: 1, End: 3}})
	if err != nil {
		t.Fatal(err)
	}
	bytes, err := io.ReadAll(opened.Body)
	if err := errors.Join(err, opened.Body.Close()); err != nil || string(bytes) != data[1:4] ||
		opened.Range == nil || *opened.Range != (core.ContentRange{Start: 1, End: 3, Total: int64(len(data))}) {
		t.Fatalf("raw range: %q %+v %v", bytes, opened.Range, err)
	}
	for _, mode := range []int32{2, 3, 1} {
		unframed.Store(mode)
		stream, err := files.Open(ctx, "reader", address, core.OpenOptions{})
		if err != nil {
			t.Fatal(err)
		}
		got, err := io.ReadAll(stream.Body)
		err = errors.Join(err, stream.Body.Close())
		if mode == 2 && (string(got) != data || core.CodeOf(err) != core.InvalidUpstreamResponse) ||
			mode == 3 && !errors.Is(err, io.ErrUnexpectedEOF) ||
			mode == 1 && (string(got) != data || err != nil) {
			t.Fatalf("unframed blob mode %d: %q %v", mode, got, err)
		}
		if stream.Length != nil || stream.Entry.Size == nil || *stream.Entry.Size != int64(len(data)) {
			t.Fatalf("unframed blob metadata: %+v", stream)
		}
	}
	unframed.Store(0)
	opened, err = files.Open(ctx, "reader", address, core.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	bytes, err = io.ReadAll(opened.Body)
	if err = errors.Join(err, opened.Body.Close()); err != nil || string(bytes) != data || opened.Range != nil || opened.Length == nil || *opened.Length != int64(len(data)) {
		t.Fatalf("full original: %q, range=%v, error=%v", bytes, opened.Range, err)
	}

	redirect.Store(true)
	if _, err := files.Open(ctx, "reader", address, core.OpenOptions{}); core.CodeOf(err) != core.UpstreamFailure {
		t.Fatalf("foreign blob redirect was accepted: %v", err)
	}
	failClose.Store(true)
	if _, err := files.Stat(ctx, "reader", address); !errors.Is(err, errClose) || closes.Load() != 1 {
		t.Fatalf("tree response close failure was lost: %v", err)
	}
	if err := mount.Retire(ctx); err != nil || closes.Load() != 2 || refs.Load() != 1 {
		t.Fatalf("retirement did not retry response cleanup or ref was re-resolved: %v", err)
	}
}
