package pikpak

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

func (w wire) RoundTrip(r *http.Request) (*http.Response, error) { return w(r) }

func TestPikPakThroughCore(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	const (
		device   = "0123456789abcdef0123456789abcdef"
		folderID = "folder:版+1"
		fileID   = "file?version%2F1"
		folder   = `{"id":"` + folderID + `","kind":"drive#folder","name":"Reports"}`
		file     = `{"id":"` + fileID + `","kind":"drive#file","parent_id":"` + folderID + `","user_id":"owner","name":"Test.zip","size":"6"}`
		token    = "page +/&"
		grant    = "http://cdn.fixture.invalid:8080/a%2Fb?signed=%2B"
	)
	reply := func(status int, body string) *http.Response {
		return &http.Response{StatusCode: status, Header: make(http.Header),
			Body: io.NopCloser(strings.NewReader(body)), ContentLength: int64(len(body))}
	}
	var mode string
	var issued, reads int
	started := make(chan struct{}, 1)
	api := &http.Client{Transport: wire(func(r *http.Request) (*http.Response, error) {
		if r.Method != http.MethodGet || r.URL.Scheme != "https" || r.URL.Host != ControlHost || r.Header.Get("X-Device-Id") != device {
			return nil, errors.New("unexpected control route or client identifier")
		}
		q := r.URL.Query()
		switch r.URL.Path {
		case "/drive/v1/files/" + folderID:
			if q.Get("usage") == "ALL" {
				return reply(200, folder), nil
			}
		case "/drive/v1/files/" + fileID:
			issued++
			if q.Get("usage") == "FETCH" {
				id := fileID
				if mode == "wrong-grant" {
					id = "other-id"
				}
				return reply(200, fmt.Sprintf(`{"id":%q,"user_id":"owner","web_content_link":%q}`, id, grant)), nil
			}
		case "/drive/v1/files":
			if !q.Has("parent_id") || q.Get("filters") == "" {
				return nil, errors.New("listing omitted its native filters")
			}
			switch q.Get("parent_id") {
			case "":
				return reply(200, `{"files":[`+folder+`]}`), nil
			case folderID:
				switch q.Get("page_token") {
				case "":
					return reply(200, fmt.Sprintf(`{"files":[],"next_page_token":%q}`, token)), nil
				case token:
					return reply(200, `{"files":[`+file+`]}`), nil
				}
			}
		}
		return nil, errors.New("unexpected PikPak API request")
	})}
	content := &http.Client{Transport: wire(func(r *http.Request) (*http.Response, error) {
		reads++
		if r.Method != http.MethodGet || r.URL.String() != grant || r.Header.Get("Accept-Encoding") != "identity" ||
			r.Header.Get("Authorization") != "" || r.Header.Get("Cookie") != "" || r.Header.Get("Referer") != "" || r.Header.Get("X-Device-Id") != "" {
			return nil, errors.New("original request changed the signed address or received account material")
		}
		switch mode {
		case "redirect-http":
			res := reply(http.StatusFound, "")
			res.Header.Set("Location", "http://other.fixture.invalid/file")
			return res, nil
		case "retire":
			started <- struct{}{}
			<-r.Context().Done()
			return nil, r.Context().Err()
		}
		if r.Header.Get("Range") == "bytes=1-3" {
			res := reply(http.StatusPartialContent, "bcd")
			res.Header.Set("Content-Range", "bytes 1-3/6")
			return res, nil
		}
		return reply(http.StatusOK, "abcdef"), nil
	})}
	defaultRoot, err := Open(ctx, Options{Client: api, DeviceID: device})
	if err != nil {
		t.Fatal(err)
	}
	if err := defaultRoot.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(ctx, Options{RootID: "*", DeviceID: device, Client: api}); core.CodeOf(err) != core.InvalidRequest {
		t.Fatal("all-files selector accepted as folder root", err)
	}
	source, err := Open(ctx, Options{RootID: folderID, DeviceID: device, Client: api, ContentClient: content})
	if err != nil {
		t.Fatal(err)
	}
	storage, err := core.New(core.Options{Authorize: func(context.Context, core.Principal, core.Action, core.Address) error { return nil }})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = storage.Close(ctx) }()
	mount, err := storage.Install("/pikpak", source)
	if err != nil {
		t.Fatal(err)
	}
	q := core.ListRequest{Query: core.Children(core.Path("/pikpak")), Limit: 1, CursorTTL: time.Minute}
	first, err := storage.List(ctx, "user", q)
	if err != nil || len(first.Items) != 0 || first.NextCursor == nil {
		t.Fatal("nonterminal native page", first, err)
	}
	q.Cursor = *first.NextCursor
	second, err := storage.List(ctx, "user", q)
	if err != nil || len(second.Items) != 1 || second.Items[0].Name != "Test.zip" || second.NextCursor != nil {
		t.Fatal("terminal native page", second, err)
	}
	name := core.Path("/pikpak/Test.zip")
	for _, tc := range []struct {
		rangeOf *core.Range
		want    string
	}{{nil, "abcdef"}, {&core.Range{Start: 1, End: 3}, "bcd"}} {
		opened, err := storage.Open(ctx, "user", name, core.OpenOptions{Range: tc.rangeOf})
		if err != nil {
			t.Fatal(err)
		}
		data, err := io.ReadAll(opened.Body)
		if err := errors.Join(err, opened.Body.Close()); err != nil || string(data) != tc.want ||
			tc.rangeOf != nil && (opened.Range == nil || opened.Range.Total != 6) {
			t.Fatal("read result", opened.Range, string(data), err)
		}
	}
	if issued != 2 || reads != 2 {
		t.Fatal("unexpected transfer requests", issued, reads)
	}
	mode = "wrong-grant"
	if _, err := storage.Open(ctx, "user", name, core.OpenOptions{}); core.CodeOf(err) != core.InvalidUpstreamResponse || reads != 2 {
		t.Fatal("mismatched grant reached content", err)
	}
	mode = "redirect-http"
	if _, err := storage.Open(ctx, "user", name, core.OpenOptions{}); core.CodeOf(err) != core.InvalidUpstreamResponse || reads != 3 {
		t.Fatal("unsafe HTTP redirect reached content", err)
	}
	mode = "retire"
	done := make(chan error, 1)
	go func() { _, err := storage.Open(ctx, "user", name, core.OpenOptions{}); done <- err }()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("content request did not start")
	}
	if err := mount.Retire(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal("retirement did not cancel the request", err)
	}
}
