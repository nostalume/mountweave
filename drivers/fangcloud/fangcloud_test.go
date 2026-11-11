package fangcloud

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

type closeFailure struct {
	io.ReadCloser
	err   error
	calls int
}

func (b *closeFailure) Close() error {
	b.calls++
	if b.calls == 1 {
		return b.err
	}
	return b.ReadCloser.Close()
}

func TestPersonalSpaceSource(t *testing.T) {
	const exact = "file%中"
	const download = "/api/v2/file/9007199254740993/download_v2"
	const folder = `{"id":7,"name":"Work","type":"folder"}`
	const file = `{"id":9007199254740993,"name":"file%中","type":"file","size":9}`
	response := func(status int, text string) *http.Response {
		return &http.Response{StatusCode: status, Header: make(http.Header),
			Body: io.NopCloser(strings.NewReader(text)), ContentLength: int64(len(text))}
	}
	mode := ""
	nativePages, details, capabilities, downloads := 0, 0, 0, 0
	started := make(chan struct{})
	api := &http.Client{Transport: wire(func(r *http.Request) (*http.Response, error) {
		if r.Method != http.MethodGet || r.URL.Scheme != "https" || r.URL.Host != "open.fangcloud.com" ||
			r.Header.Get("Accept-Encoding") != "identity" || r.Header.Get("Cookie") != "" {
			return nil, errors.New("wrong Fangcloud control receiver")
		}
		switch r.URL.Path {
		case "/api/v2/folder/7/info":
			details++
			return response(200, `{"id":7,"name":"Work","type":"folder","space":{"type":"personal"}}`), nil
		case download:
			capabilities++
			if mode == "capability-error" {
				return response(403, `{"message":"https://content.example/download?sig=private"}`), nil
			}
			return response(200, `{"download_url":"https://content.example/download?sig=private%2Bkey"}`), nil
		case "/api/v2/folder/personal_items", "/api/v2/folder/7/children":
			nativePages++
		default:
			return nil, fmt.Errorf("unexpected control route %s", r.URL.Path)
		}
		if mode == "wait" {
			close(started)
			<-r.Context().Done()
			return nil, r.Context().Err()
		}
		query := r.URL.Query()
		if query.Get("page_capacity") != "20" || query.Get("type") != "all" || query.Get("sort_by") != "name" || query.Get("sort_direction") != "asc" {
			return nil, errors.New("wrong paging scope")
		}
		if r.URL.Path == "/api/v2/folder/7/children" {
			if query.Get("folder_id") != "7" || query.Get("page_id") != "0" {
				return nil, errors.New("missing folder ID in the native child query")
			}
			return response(200, `{"files":[],"page_id":0,"page_capacity":20,"page_count":0,"total_count":0}`), nil
		}
		switch query.Get("page_id") {
		case "0":
			selected := file
			if mode == "parent" {
				selected = strings.TrimSuffix(file, "}") + `,"parent":{"id":77}}`
			}
			return response(200, fmt.Sprintf(`{"folders":[%s],"files":[%s],"page_id":0,"page_capacity":20,"page_count":2,"total_count":3}`, folder, selected)), nil
		case "1":
			if mode == "type-collision" {
				return response(200, `{"folders":[{"id":9007199254740993,"name":"file%中","type":"folder"}],"page_id":1,"page_capacity":20,"page_count":2,"total_count":3}`), nil
			}
			last := `{"id":21,"name":"last","type":"file","size":0,"path":[{"id":0}]}`
			if mode == "duplicate" {
				last = strings.Replace(file, "9007199254740993", "21", 1)
			}
			return response(200, fmt.Sprintf(`{"files":[%s],"page_id":1,"page_capacity":20,"page_count":2,"total_count":3}`, last)), nil
		default:
			return nil, errors.New("paging did not stop at native terminal")
		}
	})}
	var broken, finalClose *closeFailure
	content := &http.Client{Transport: wire(func(r *http.Request) (*http.Response, error) {
		downloads++
		if r.URL.Scheme != "https" || r.URL.Host != "content.example" || r.Header.Get("Authorization") != "" ||
			r.Header.Get("Cookie") != "" || r.Header.Get("Referer") != "" || r.Header.Get("Accept-Encoding") != "identity" {
			return nil, errors.New("wrong anonymous receiver or forwarded credentials")
		}
		if r.URL.Path == "/download" {
			if r.URL.RawQuery != "sig=private%2Bkey" {
				return nil, errors.New("signed query changed")
			}
			redirect := response(302, "")
			redirect.Header.Set("Location", "/body")
			if mode == "close" {
				broken = &closeFailure{ReadCloser: redirect.Body, err: fmt.Errorf("closing %s: %w", r.URL, io.EOF)}
				redirect.Body = broken
			}
			return redirect, nil
		}
		if r.URL.Path != "/body" {
			return nil, errors.New("wrong content route")
		}
		if mode == "final-close" {
			reply := response(403, "")
			finalClose = &closeFailure{ReadCloser: reply.Body, err: fmt.Errorf("closing signed private URL: %w", io.EOF)}
			reply.Body = finalClose
			return reply, nil
		}
		if r.Header.Get("Range") == "bytes=1-3" {
			reply := response(206, "bcd")
			reply.Header.Set("Content-Range", "bytes 1-3/6")
			return reply, nil
		}
		return response(200, "abcdef"), nil
	})}
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()
	options := Options{Client: api, ContentClient: content}
	driver, err := Open(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	storage, err := core.New(core.Options{Authorize: func(context.Context, core.Principal, core.Action, core.Address) error { return nil }})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = storage.Close(ctx) }()
	mount, err := storage.Install("/cloud", driver)
	if err != nil {
		t.Fatal(err)
	}
	for _, choice := range []Options{{RootPath: "/Work", Client: api}, {RootID: 7, Client: api}} {
		sub, err := Open(ctx, choice)
		if err != nil || sub.root.ID != 7 {
			t.Fatal("native root did not bind to the selected folder", err)
		}
		if err := sub.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if details != 1 {
		t.Fatal("path root repeated the native details request", details)
	}
	req := core.ListRequest{Query: core.Children(core.Path("/cloud")), Limit: 1, CursorTTL: time.Minute}
	first, err := storage.List(ctx, "u", req)
	if err != nil || len(first.Items) != 1 || first.Items[0].Name != "Work" || first.NextCursor == nil {
		t.Fatal(first, err)
	}
	req.Cursor = *first.NextCursor
	before := nativePages
	second, err := storage.List(ctx, "u", req)
	if err != nil || len(second.Items) != 1 || second.Items[0].Name != exact || second.NextCursor == nil || nativePages != before {
		t.Fatal("native page remainder", second, err)
	}
	req.Cursor = *second.NextCursor
	if last, err := storage.List(ctx, "u", req); err != nil || len(last.Items) != 1 || last.Items[0].Name != "last" || last.NextCursor != nil {
		t.Fatal("native terminal page did not close the public cursor", last, err)
	}
	target := core.Path("/cloud/" + exact)
	for _, tc := range []struct {
		mode string
		code core.Code
	}{{"parent", core.InvalidUpstreamResponse}, {"duplicate", core.PathAmbiguous}, {"type-collision", core.PathAmbiguous}} {
		mode = tc.mode
		if _, err := storage.Open(ctx, "u", target, core.OpenOptions{}); core.CodeOf(err) != tc.code || capabilities != 0 {
			t.Fatal(mode, err, capabilities)
		}
	}
	mode = ""
	opened, err := storage.Open(ctx, "u", target, core.OpenOptions{Range: &core.Range{Start: 1, End: 3}})
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(opened.Body)
	if err := errors.Join(err, opened.Body.Close()); err != nil || string(data) != "bcd" ||
		opened.Range == nil || opened.Range.Total != 6 || capabilities != 1 || downloads != 2 {
		t.Fatal(string(data), opened.Range, err, capabilities, downloads)
	}
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
	if content, err := os.ReadFile(filepath.Join(root, "copied")); err != nil || string(content) != "abcdef" {
		t.Fatal(string(content), err)
	}
	mode, before = "capability-error", downloads
	if _, err := storage.Open(ctx, "u", target, core.OpenOptions{}); core.CodeOf(err) != core.UpstreamAccessDenied ||
		strings.Contains(err.Error(), "private") || downloads != before {
		t.Fatal("private authorization failed without a safe cause", err)
	}
	mode, before = "close", downloads
	if _, err := storage.Open(ctx, "u", target, core.OpenOptions{}); !errors.Is(err, io.ErrUnexpectedEOF) ||
		strings.Contains(err.Error(), "private") || downloads != before+1 || broken == nil || broken.calls != 1 {
		t.Fatal("private redirect cleanup did not stop the chain", err)
	}
	mode, before = "final-close", downloads
	if _, err := storage.Open(ctx, "u", target, core.OpenOptions{}); core.CodeOf(err) != core.UpstreamFailure ||
		!errors.Is(err, io.ErrUnexpectedEOF) || strings.Contains(err.Error(), "private") ||
		downloads != before+2 || finalClose == nil || finalClose.calls != 1 {
		t.Fatal("private final response cleanup lost its cause or leaked a signature", err)
	}
	mode = "wait"
	done := make(chan error, 1)
	go func() { _, err := storage.Stat(ctx, "u", target); done <- err }()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("native call did not start")
	}
	if err := mount.Retire(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-done; !errors.Is(err, context.Canceled) || broken.calls != 2 || finalClose.calls != 2 {
		t.Fatal("retirement did not join and clean up", err, broken.calls)
	}
}
