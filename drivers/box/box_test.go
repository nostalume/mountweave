package box

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

func (w wire) RoundTrip(r *http.Request) (*http.Response, error) { return w(r) }

type retryClose struct {
	io.ReadCloser
	calls int
}

func (b *retryClose) Close() error {
	b.calls++
	if b.calls == 1 {
		return fmt.Errorf("signed capability secret: %w", io.EOF)
	}
	return b.ReadCloser.Close()
}

func TestBoxSourceThroughCore(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	const folder = `{"type":"folder","id":"7","name":"Team","parent":{"type":"folder","id":"0"}}`
	const file = `{"type":"file","id":"16","name":"Report 版","size":6,"parent":{"type":"folder","id":"7"}}`
	const link = `{"type":"web_link","id":"18","name":"Link","url":"https://untrusted.invalid","parent":{"type":"folder","id":"7"}}`
	const capability = "https://api.box.com/delegated?sig=secret"
	const bodyHost = "cdn.fixture.invalid"
	const marker = "a+/ ="
	reply := func(status int, body string) *http.Response {
		return &http.Response{StatusCode: status, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body)), ContentLength: int64(len(body))}
	}
	redirect := func(location string) *http.Response {
		r := reply(http.StatusFound, "")
		r.Header.Set("Location", location)
		return r
	}
	var mode string
	var broken *retryClose
	var pages, apiDownloads, bodyRequests int
	started := make(chan struct{}, 1)
	api := &http.Client{Transport: wire(func(r *http.Request) (*http.Response, error) {
		if r.URL.Scheme != "https" || r.URL.Host != "api.box.com" || r.Method != http.MethodGet {
			t.Errorf("credentialed request escaped Box API: %s %s", r.Method, r.URL)
			return nil, errors.New("unexpected API request")
		}
		r.Header.Set("Authorization", "Bearer fixture")
		q := r.URL.Query()
		switch r.URL.Path {
		case "/2.0/folders/0", "/2.0/folders/7":
			return reply(200, fmt.Sprintf(`{"type":"folder","id":%q}`, r.URL.Path[len(r.URL.Path)-1:])), nil
		case "/2.0/folders/0/items", "/2.0/folders/7/items":
			pages++
			if q.Get("usemarker") != "true" || q.Has("offset") || q.Has("sort") || !strings.Contains(q.Get("fields"), "parent") {
				return nil, errors.New("invalid native marker request")
			}
			if strings.HasPrefix(r.URL.Path, "/2.0/folders/7/") {
				entry := file
				if mode == "wrong-parent" {
					entry = strings.Replace(file, `"id":"7"`, `"id":"6"`, 1)
				}
				return reply(200, `{"entries":[`+entry+`,`+link+`],"next_marker":null}`), nil
			}
			switch q.Get("marker") {
			case "":
				return reply(200, fmt.Sprintf(`{"entries":[%s],"next_marker":%q}`, folder, marker)), nil
			case marker:
				if mode == "marker-cycle" {
					return reply(200, fmt.Sprintf(`{"entries":[],"next_marker":%q}`, marker)), nil
				}
				return reply(200, `{"entries":[],"next_marker":"last"}`), nil
			case "last":
				return reply(200, `{"entries":[],"next_marker":null}`), nil
			}
		case "/2.0/files/16/content":
			apiDownloads++
			if mode == "wait" {
				response := reply(http.StatusAccepted, "")
				response.Header.Set("Retry-After", "3")
				return response, nil
			}
			return redirect(capability), nil
		}
		return nil, errors.New("unexpected Box API route")
	})}
	content := &http.Client{Transport: wire(func(r *http.Request) (*http.Response, error) {
		bodyRequests++
		if r.URL.Scheme != "https" || r.Header.Get("Authorization") != "" || r.Header.Get("Cookie") != "" ||
			r.Header.Get("Referer") != "" || r.Header.Get("Accept-Encoding") != "identity" {
			return nil, errors.New("unsafe content request")
		}
		if r.URL.String() == capability {
			response := redirect("https://" + bodyHost + "/report")
			switch mode {
			case "downgrade":
				response.Header.Set("Location", "http://"+bodyHost+"/report")
			case "close":
				broken = &retryClose{ReadCloser: response.Body}
				response.Body = broken
			}
			return response, nil
		}
		if r.URL.Host != bodyHost || r.URL.Path != "/report" {
			return nil, errors.New("unexpected content route")
		}
		if mode == "cancel" {
			started <- struct{}{}
			<-r.Context().Done()
			return nil, r.Context().Err()
		}
		if r.Header.Get("Range") == "bytes=1-3" {
			response := reply(206, "bcd")
			response.Header.Set("Content-Range", "bytes 1-3/6")
			return response, nil
		}
		return reply(200, "abcdef"), nil
	})}
	source, err := Open(ctx, Options{Client: api, ContentClient: content})
	if err != nil {
		t.Fatal(err)
	}
	sub, err := Open(ctx, Options{RootID: "7", Client: api})
	if err != nil || sub.root != "7" {
		t.Fatal("explicit root did not bind", err)
	}
	if err := sub.Close(); err != nil {
		t.Fatal(err)
	}
	storage, err := core.New(core.Options{Authorize: func(context.Context, core.Principal, core.Action, core.Address) error { return nil }})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = storage.Close(ctx) }()
	mount, err := storage.Install("/box", source)
	if err != nil {
		t.Fatal(err)
	}
	q := core.ListRequest{Query: core.Children(core.Path("/box")), Limit: 1, CursorTTL: time.Minute}
	first, err := storage.List(ctx, "u", q)
	if err != nil || len(first.Items) != 1 || first.Items[0].Name != "Team" || first.NextCursor == nil {
		t.Fatal(first, err)
	}
	q.Cursor = *first.NextCursor
	empty, err := storage.List(ctx, "u", q)
	if err != nil || len(empty.Items) != 0 || empty.NextCursor == nil {
		t.Fatal("nonterminal empty native page stopped listing", empty, err)
	}
	q.Cursor = *empty.NextCursor
	if last, err := storage.List(ctx, "u", q); err != nil || len(last.Items) != 0 || last.NextCursor != nil || pages != 3 {
		t.Fatal("native terminal marker did not end listing", last, err)
	}
	filePath := core.Path("/box/Team/Report 版")
	for _, failure := range []struct {
		mode, path string
		code       core.Code
	}{
		{"marker-cycle", filePath.Value(), core.InvalidUpstreamResponse},
		{"wrong-parent", filePath.Value(), core.InvalidUpstreamResponse},
		{"", "/box/Team/report 版", core.NotFound},
		{"", "/box/Team/Link", core.UnsupportedOperation},
	} {
		mode = failure.mode
		if _, err := storage.Open(ctx, "u", core.Path(failure.path), core.OpenOptions{}); core.CodeOf(err) != failure.code || apiDownloads != 0 {
			t.Fatal(failure, err, apiDownloads)
		}
	}
	mode = ""
	if link, err := storage.Stat(ctx, "u", core.Path("/box/Team/Link")); err != nil || link.Kind != core.File || link.Size != nil {
		t.Fatal("web link should be visible without a file size", link, err)
	}
	opened, err := storage.Open(ctx, "u", filePath, core.OpenOptions{Range: &core.Range{Start: 1, End: 3}})
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(opened.Body)
	if err := errors.Join(err, opened.Body.Close()); err != nil || string(data) != "bcd" || opened.Range == nil || opened.Range.Total != 6 {
		t.Fatal("range", string(data), opened.Range, err)
	}
	root := t.TempDir()
	destination, err := local.New(root)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := storage.Install("/local", destination); err != nil {
		t.Fatal(err)
	}
	if change, err := storage.Copy(ctx, "u", filePath, core.Path("/local/report")); err != nil || change.Effect != core.Applied {
		t.Fatal(change, err)
	}
	if data, err := os.ReadFile(filepath.Join(root, "report")); err != nil || string(data) != "abcdef" {
		t.Fatal(string(data), err)
	}
	mode = "wait"
	before := bodyRequests
	if _, err := storage.Open(ctx, "u", filePath, core.OpenOptions{}); err == nil || !strings.Contains(err.Error(), "retry after 3 seconds") || bodyRequests != before {
		t.Fatal("readiness response lost the retry boundary", err)
	}
	mode, before = "downgrade", bodyRequests
	if _, err := storage.Open(ctx, "u", filePath, core.OpenOptions{}); core.CodeOf(err) != core.InvalidUpstreamResponse || bodyRequests != before+1 {
		t.Fatal("unsafe redirect reached a second content host", err)
	}
	mode, before = "close", bodyRequests
	if _, err := storage.Open(ctx, "u", filePath, core.OpenOptions{}); !errors.Is(err, io.ErrUnexpectedEOF) ||
		strings.Contains(err.Error(), "secret") || bodyRequests != before+1 || broken == nil || broken.calls != 1 {
		t.Fatal("failed redirect close did not stop the chain", err)
	}
	mode = "cancel"
	done := make(chan error, 1)
	go func() { _, err := storage.Open(ctx, "u", filePath, core.OpenOptions{}); done <- err }()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("content request did not start")
	}
	if err := mount.Retire(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-done; !errors.Is(err, context.Canceled) || broken.calls != 2 {
		t.Fatal("retirement did not cancel and retry failed cleanup", err, broken.calls)
	}
}
