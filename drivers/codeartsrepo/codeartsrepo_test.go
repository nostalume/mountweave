package codeartsrepo

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	core "github.com/nostalume/mountweave"
)

type fixtureTransport func(*http.Request) (*http.Response, error)

func (f fixtureTransport) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

func TestCommitThroughCore(t *testing.T) {
	const origin = "https://codehub-ext.cn-north-4.myhuaweicloud.com"
	const prefix = "/v4/repositories/42/repository"
	const commit = "f1edcba9876543210fedcba9876543210fedcba9"
	const folder = "Source +%"
	const name = "draft #.bin"
	const data = "\x00\xff{binary}"
	var mode atomic.Int32
	var reads atomic.Int32
	started := make(chan struct{}, 1)
	client := &http.Client{Transport: fixtureTransport(func(req *http.Request) (*http.Response, error) {
		if req.Method != http.MethodGet || req.URL.Scheme+"://"+req.URL.Host != origin ||
			req.Header.Get("Accept-Encoding") != "identity" {
			return nil, errors.New("unexpected CodeArts control request")
		}
		q := req.URL.Query()
		header := make(http.Header)
		status, payload := http.StatusOK, ""
		switch req.URL.Path {
		case prefix + "/commits":
			if q.Get("sha") != commit || q.Get("stats") != "false" {
				return nil, errors.New("unexpected commit lookup")
			}
			payload = fmt.Sprintf(`{"id":%q}`, commit)
		case prefix + "/trees":
			if q.Get("ref") != commit || q.Get("recursive") != "false" ||
				(q.Get("limit") != "1" && q.Get("limit") != "100") {
				return nil, errors.New("unexpected tree lookup")
			}
			switch q.Get("path") {
			case "":
				header.Set("X-Total", "3")
				switch q.Get("offset") {
				case "0":
					payload = fmt.Sprintf(`[{"id":"tree","name":%q,"path":%q,"type":"tree","mode":"040000"}]`, folder, folder)
				case "1":
					payload = `[{"id":"link","name":"link","path":"link","type":"blob","mode":"120000"}]`
				case "2":
					payload = `[{"id":"file","name":"plain","path":"plain","type":"blob","mode":"100755"}]`
				default:
					return nil, errors.New("unexpected native offset")
				}
			case folder:
				header.Set("X-Total", "1")
				path := folder + "/" + name
				if mode.Load() == 1 {
					path = "other/" + name
				}
				payload = fmt.Sprintf(`[{"id":"blob+%%","name":%q,"path":%q,"type":"blob","mode":"100644"}]`, name, path)
			default:
				return nil, errors.New("unexpected native parent")
			}
		case prefix + "/blobs/blob+%/raw":
			reads.Add(1)
			if q.Get("file_path") != folder+"/"+name || req.Header.Get("Range") != "" {
				return nil, errors.New("wrong native blob path or unexpected range")
			}
			switch mode.Load() {
			case 2:
				status = http.StatusFound
				header.Set("Location", "https://outside.example/body")
			case 3:
				started <- struct{}{}
				<-req.Context().Done()
				return nil, req.Context().Err()
			default:
				payload = data
			}
		default:
			return nil, errors.New("unexpected API route")
		}
		return &http.Response{StatusCode: status, Header: header, Body: io.NopCloser(strings.NewReader(payload)),
			ContentLength: int64(len(payload)), Request: req}, nil
	})}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	d, err := Open(ctx, Options{Endpoint: origin, RepositoryID: 42, CommitSHA: commit, Client: client})
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

	request := core.ListRequest{Query: core.Children(core.Path("/repo")), Limit: 1, CursorTTL: time.Minute}
	for i, want := range []string{folder, "", "plain"} {
		page, err := c.List(ctx, "reader", request)
		if err != nil || (page.NextCursor == nil) != (i == 2) {
			t.Fatalf("page %d: %v %v", i, page, err)
		}
		if want == "" && len(page.Items) != 0 || want != "" && (len(page.Items) != 1 || page.Items[0].Name != want) {
			t.Fatalf("unexpected page items: %v", page.Items)
		}
		if page.NextCursor != nil {
			request.Cursor = *page.NextCursor
		}
	}
	source := core.Path("/repo/" + folder + "/" + name)
	for _, check := range []struct {
		mode    int32
		address core.Address
		code    core.Code
	}{
		{0, core.Path("/repo/link"), core.UnsupportedOperation},
		{1, source, core.InvalidUpstreamResponse},
		{2, source, core.UpstreamFailure},
	} {
		mode.Store(check.mode)
		if _, err := c.Open(ctx, "reader", check.address, core.OpenOptions{}); core.CodeOf(err) != check.code {
			t.Fatalf("unexpected failure for mode %d: %v", check.mode, err)
		}
	}
	mode.Store(0)
	opened, err := c.Open(ctx, "reader", source, core.OpenOptions{Range: &core.Range{Start: 1, End: 2}})
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(opened.Body)
	if err = errors.Join(err, opened.Body.Close()); err != nil || string(body) != data || opened.Range != nil {
		t.Fatalf("raw full body: %q %v %v", body, opened.Range, err)
	}
	mode.Store(3)
	readCtx, stopRead := context.WithCancel(ctx)
	defer stopRead()
	go func() {
		select {
		case <-started:
			stopRead()
		case <-readCtx.Done():
		}
	}()
	if _, err := c.Open(readCtx, "reader", source, core.OpenOptions{}); !errors.Is(err, context.Canceled) || reads.Load() != 3 {
		t.Fatalf("cancellation or unexpected raw I/O: %v, %d", err, reads.Load())
	}
	subtree, err := Open(ctx, Options{Endpoint: origin, RepositoryID: 42, CommitSHA: commit, RootPath: "/" + folder, Client: client})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Install("/subtree", subtree); err != nil {
		t.Fatal(err)
	}
	page, err := c.List(ctx, "reader", core.ListRequest{Query: core.Children(core.Path("/subtree")), Limit: 1, CursorTTL: time.Minute})
	if err != nil || len(page.Items) != 1 || page.Items[0].Name != name || page.NextCursor != nil {
		t.Fatalf("selected root: %v %v", page, err)
	}
	if err := mount.Retire(ctx); err != nil {
		t.Fatal(err)
	}
}
