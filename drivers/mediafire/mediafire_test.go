package mediafire

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

type wire func(*http.Request) (*http.Response, error)

func (w wire) RoundTrip(r *http.Request) (*http.Response, error) { return w(r) }

type failedClose struct {
	io.ReadCloser
	private string
	calls   int
}

func (b *failedClose) Close() error {
	b.calls++
	if b.calls == 1 {
		return fmt.Errorf("closing %s: %w", b.private, io.EOF)
	}
	return b.ReadCloser.Close()
}

func TestScopedFolderAndDirectDownloadThroughCore(t *testing.T) {
	const name, original = "Report 版.txt", "abcdef"
	const capability = "https://download.fixture/file?sig=private"
	const delegated = "https://www.mediafire.com/delegated?sig=next"
	var mode, currentToken atomic.Value
	mode.Store("")
	currentToken.Store("token+/first=")
	var pages, links, bodies, controlCalls, tokenCalls atomic.Int32
	var broken *failedClose
	started := make(chan struct{}, 1)
	reply := func(status int, data string) *http.Response {
		return &http.Response{StatusCode: status, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(data)), ContentLength: int64(len(data))}
	}
	native := func(action, fields string) *http.Response {
		return reply(200, fmt.Sprintf(`{"response":{"action":%q,"result":"Success",%s}}`, action, fields))
	}
	control := &http.Client{Transport: wire(func(r *http.Request) (*http.Response, error) {
		controlCalls.Add(1)
		q := r.URL.Query()
		if r.URL.Scheme != "https" || r.URL.Host != "www.mediafire.com" || r.Method != http.MethodGet ||
			q.Get("session_token") != currentToken.Load().(string) || len(q["session_token"]) != 1 || q.Get("response_format") != "json" ||
			r.Header.Get("Accept-Encoding") != "identity" || r.Header.Get("Range") != "" {
			return nil, errors.New("wrong credentialed MediaFire request")
		}
		state := mode.Load().(string)
		switch r.URL.Path {
		case "/api/1.5/folder/get_info.php":
			if q.Get("folder_key") != "root" {
				return nil, errors.New("wrong root key")
			}
			key := "root"
			if state == "wrong-root" {
				key = "other"
			}
			return native("folder/get_info", fmt.Sprintf(`"folder_info":{"folderkey":%q}`, key)), nil
		case "/api/1.5/folder/get_content.php":
			pages.Add(1)
			if q.Get("chunk_size") != "100" {
				return nil, errors.New("wrong native chunk size")
			}
			parent, kind, chunk := q.Get("folder_key"), q.Get("content_type"), q.Get("chunk")
			var rows string
			more := "no"
			switch parent + ":" + kind + ":" + chunk {
			case "root:folders:1":
				rows, more = `{"folderkey":"team","name":"Team","parent_folderkey":"root"}`, "yes"
			case "root:folders:2":
				rows = `{"folderkey":"other","name":"Other"}`
			case "root:files:1":
				filename := "root.bin"
				if state == "ambiguous" {
					filename = "Team"
				}
				rows = fmt.Sprintf(`{"quickkey":"root-file","filename":%q,"size":"0"}`, filename)
			case "team:folders:1":
			case "team:files:1":
				rows = fmt.Sprintf(`{"quickkey":"report","filename":%q,"size":6}`, name)
			default:
				return nil, errors.New("wrong native parent or chunk progression")
			}
			if state == "bad-chunk" {
				chunk = "9"
			}
			if state == "empty-more" {
				rows, more = "", "yes"
			}
			return native("folder/get_content", fmt.Sprintf(`"folder_content":{"content_type":%q,"chunk_number":%q,"more_chunks":%q,%q:[%s]}`, kind, chunk, more, kind, rows)), nil
		case "/api/1.5/file/get_links.php":
			links.Add(1)
			if q.Get("quick_key") != "report" || q.Get("link_type") != "direct_download" {
				return nil, errors.New("wrong direct-download lookup")
			}
			row := fmt.Sprintf(`"quickkey":"report","direct_download":%q`, capability)
			switch state {
			case "normal-only":
				row = `"quickkey":"report","normal_download":"https://www.mediafire.com/file/landing"`
			case "http-link":
				row = fmt.Sprintf(`"quickkey":"report","direct_download":%q`, strings.Replace(capability, "https:", "http:", 1))
			case "wrong-key":
				row = fmt.Sprintf(`"quickkey":"unrelated","direct_download":%q`, capability)
			case "bandwidth":
				row = `"quickkey":"report","direct_download_error":"44"`
			}
			return native("file/get_links", `"links":[{`+row+`}]`), nil
		}
		return nil, errors.New("unexpected MediaFire control route")
	})}
	content := &http.Client{Transport: wire(func(r *http.Request) (*http.Response, error) {
		bodies.Add(1)
		if r.URL.Scheme != "https" || r.Method != http.MethodGet || r.URL.Query().Has("session_token") ||
			r.Header.Get("Authorization") != "" || r.Header.Get("Cookie") != "" || r.Header.Get("Referer") != "" {
			return nil, errors.New("private account context reached download transport")
		}
		switch r.URL.String() {
		case capability:
			response := reply(http.StatusFound, "")
			response.Header.Set("Location", delegated)
			if mode.Load() == "close" {
				broken = &failedClose{ReadCloser: response.Body, private: r.URL.String()}
				response.Body = broken
			}
			return response, nil
		case delegated:
			switch mode.Load() {
			case "wait":
				started <- struct{}{}
				<-r.Context().Done()
				return nil, r.Context().Err()
			case "wrong-size":
				return reply(200, "different data"), nil
			}
			if r.Header.Get("Range") == "bytes=1-3" {
				response := reply(206, original[1:4])
				response.Header.Set("Content-Range", "bytes 1-3/6")
				return response, nil
			}
			return reply(200, original), nil
		}
		return nil, errors.New("unexpected MediaFire content route")
	})}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	o := Options{RootKey: "root", Client: control, ContentClient: content, SessionToken: func(context.Context) (string, error) {
		tokenCalls.Add(1)
		return currentToken.Load().(string), nil
	}}
	source, err := Open(ctx, o)
	if err != nil {
		t.Fatal(err)
	}
	mode.Store("wrong-root")
	wrong, err := Open(ctx, o)
	if core.CodeOf(err) != core.InvalidUpstreamResponse || wrong == nil {
		t.Fatal("wrong MediaFire folder was admitted", err)
	}
	if err := wrong.Close(); err != nil {
		t.Fatal(err)
	}
	mode.Store("")
	currentToken.Store("token+/rotated=")
	s, err := core.New(core.Options{Authorize: func(context.Context, core.Principal, core.Action, core.Address) error { return nil }})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close(ctx) }()
	mount, err := s.Install("/remote", source)
	if err != nil {
		t.Fatal(err)
	}
	q := core.ListRequest{Query: core.Children(core.Path("/remote")), Limit: 1, CursorTTL: time.Minute}
	for k, expected := range []string{"Team", "Other", "root.bin"} {
		page, err := s.List(ctx, "u", q)
		if err != nil || len(page.Items) != 1 || page.Items[0].Name != expected || (k < 2) != (page.NextCursor != nil) {
			t.Fatal(page, err)
		}
		if k == 1 {
			if again, err := s.List(ctx, "u", q); err != nil || len(again.Items) != 1 || pages.Load() != 2 {
				t.Fatal("public replay repeated the native chunk", again, err)
			}
		}
		if page.NextCursor != nil {
			q.Cursor = *page.NextCursor
		}
	}
	target := core.Path("/remote/Team/" + name)
	for _, check := range []struct {
		state string
		code  core.Code
	}{{"ambiguous", core.PathAmbiguous}, {"bad-chunk", core.InvalidUpstreamResponse}, {"empty-more", core.InvalidUpstreamResponse}, {"normal-only", core.UnsupportedOperation},
		{"http-link", core.UnsupportedOperation}, {"wrong-key", core.InvalidUpstreamResponse}, {"bandwidth", core.UpstreamThrottled}} {
		mode.Store(check.state)
		if _, err := s.Open(ctx, "u", target, core.OpenOptions{}); core.CodeOf(err) != check.code || bodies.Load() != 0 {
			t.Fatal(check, err)
		}
	}
	mode.Store("")
	if meta, err := s.Stat(ctx, "u", target); err != nil || meta.Size == nil || *meta.Size != 6 {
		t.Fatal(meta, err)
	}
	opened, err := s.Open(ctx, "u", target, core.OpenOptions{Range: &core.Range{Start: 1, End: 3}})
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(opened.Body)
	if err := errors.Join(err, opened.Body.Close()); err != nil || string(data) != "bcd" || opened.Range == nil || opened.Range.Total != 6 {
		t.Fatal(string(data), opened.Range, err)
	}
	opened, err = s.Open(ctx, "u", target, core.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	data, err = io.ReadAll(opened.Body)
	if err = errors.Join(err, opened.Body.Close()); err != nil || string(data) != original || opened.Range != nil {
		t.Fatal("complete original", string(data), err)
	}
	mode.Store("wrong-size")
	if _, err := s.Open(ctx, "u", target, core.OpenOptions{}); core.CodeOf(err) != core.InvalidUpstreamResponse {
		t.Fatal("mismatched direct body was admitted", err)
	}
	mode.Store("close")
	before := bodies.Load()
	if _, err := s.Open(ctx, "u", target, core.OpenOptions{}); !errors.Is(err, io.ErrUnexpectedEOF) || strings.Contains(err.Error(), "sig=") || bodies.Load() != before+1 {
		t.Fatal("failed private close advanced the chain or leaked its URL", err)
	}
	mode.Store("wait")
	done := make(chan error, 1)
	go func() { _, err := s.Open(ctx, "u", target, core.OpenOptions{}); done <- err }()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("native download did not start")
	}
	if err := mount.Retire(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-done; !errors.Is(err, context.Canceled) || broken == nil || broken.calls != 2 || tokenCalls.Load() != controlCalls.Load() || links.Load() == 0 {
		t.Fatal("retirement or session ownership failed", err)
	}
}
