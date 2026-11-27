package mediafire

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"os"
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

type websiteFixture struct {
	list     func(WebsiteListRequest) (WebsitePage, error)
	download func(WebsiteFile) (WebsiteDownload, error)
}

func (s websiteFixture) List(_ context.Context, q WebsiteListRequest) (WebsitePage, error) {
	return s.list(q)
}

func (s websiteFixture) Download(_ context.Context, f WebsiteFile) (WebsiteDownload, error) {
	return s.download(f)
}

func TestScopedOrdinaryWebsiteDownload(t *testing.T) {
	const account, name, key, body = "bound", "sample.zip", "sample", "original"
	const landing = "https://www.mediafire.com/file/sample/sample.zip/file"
	var mode string
	var opened, released int
	session := websiteFixture{
		list: func(q WebsiteListRequest) (WebsitePage, error) {
			if q.FolderKey != "myfiles" || q.Chunk != 1 || q.ChunkSize != 100 {
				t.Fatalf("unexpected directory request: %+v", q)
			}
			subject, folder, rows := account, q.FolderKey, ""
			if mode == "replaced" {
				subject = "another"
			}
			if mode == "wrong-folder" {
				folder = "another"
			}
			if mode == "expired" {
				return WebsitePage{}, ErrWebsiteSignIn
			}
			if q.ContentType == "files" {
				link := landing
				if mode == "wrong-link" {
					link = "https://www.mediafire.com/file/other/sample.zip/file"
				}
				rows = fmt.Sprintf(`{"quickkey":%q,"filename":%q,"size":"%d","links":{"normal_download":%q}}`, key, name, len(body), link)
			}
			data := fmt.Sprintf(`{"response":{"action":"folder/get_content","result":"Success",
				"folder_content":{"folderkey":%q,"content_type":%q,"chunk_number":"1","more_chunks":"no",%q:[%s]}}}`,
				folder, q.ContentType, q.ContentType, rows)
			return WebsitePage{Account: subject, Status: 200, Body: []byte(data)}, nil
		},
		download: func(f WebsiteFile) (WebsiteDownload, error) {
			if f != (WebsiteFile{Account: account, ID: key, Name: name, URL: landing, Size: int64(len(body))}) {
				t.Fatalf("unexpected file receipt: %+v", f)
			}
			opened++
			data := body
			if mode == "truncated" {
				data = data[:1]
			}
			return WebsiteDownload{Account: account, FileID: key, Response: &http.Response{
				StatusCode: 200, Body: io.NopCloser(strings.NewReader(data)), ContentLength: -1,
			}}, nil
		},
	}
	ctx := t.Context()
	options := WebsiteOptions{Session: session, Account: account, RootPath: "/", RootID: "myfiles",
		Release: func() error { released++; return nil }}
	d, err := OpenWebsite(ctx, options)
	if err != nil || d.RootID() != options.RootID {
		t.Fatal("website root did not open", err)
	}
	for _, fault := range []struct {
		mode string
		code core.Code
	}{
		{"replaced", core.UpstreamAccessDenied},
		{"wrong-folder", core.InvalidUpstreamResponse},
		{"wrong-link", core.InvalidUpstreamResponse},
		{"expired", core.UpstreamAccessDenied},
	} {
		mode = fault.mode
		if _, err := d.Open(ctx, core.NativePath("/"+name), core.OpenOptions{}); core.CodeOf(err) != fault.code || opened != 0 {
			t.Fatal(fault, err)
		}
	}
	for _, fault := range []struct {
		mode string
		err  error
	}{{"", nil}, {"truncated", io.ErrUnexpectedEOF}} {
		mode = fault.mode
		result, err := d.Open(ctx, core.NativePath("/"+name), core.OpenOptions{})
		if err != nil {
			t.Fatal(err)
		}
		got, err := io.ReadAll(result.Body)
		if err = errors.Join(err, result.Body.Close()); !errors.Is(err, fault.err) || fault.err == nil && string(got) != body {
			t.Fatal(fault, err)
		}
	}
	if err := errors.Join(d.Close(), d.Close()); err != nil || released != 1 {
		t.Fatal("website session was not released once", released, err)
	}
}

func TestBrowserWebsiteSeparatesPublicLandingAndAttachment(t *testing.T) {
	const account, key, name, original = "bound", "sample", "sample.txt", "original"
	const landing = "https://www.mediafire.com/file/sample/sample.txt/file"
	const body = "https://download7.mediafire.com/file/sample/sample.txt"
	jar, _ := cookiejar.New(nil)
	page, _ := url.Parse(landing)
	jar.SetCookies(page, []*http.Cookie{{Name: "fixture", Value: "private", Domain: ".mediafire.com", Path: "/"}})
	var responseLocation string
	var calls int
	client := &http.Client{Jar: jar, Transport: wire(func(req *http.Request) (*http.Response, error) {
		if req.Method != http.MethodGet || req.Header.Get("Cookie") != "" ||
			req.Header.Get("Authorization") != "" || req.Header.Get("Accept-Encoding") != "identity" {
			t.Fatalf("unexpected headers on public %s request", req.Method)
		}
		calls++
		if req.URL.String() != body || req.Header.Get("Referer") != landing {
			t.Fatal("attachment request used an unexpected public address or referer")
		}
		if responseLocation != "" {
			return &http.Response{StatusCode: 302, Header: http.Header{"Location": {responseLocation}},
				Body: io.NopCloser(strings.NewReader(""))}, nil
		}
		return &http.Response{StatusCode: 200, Header: http.Header{"Content-Disposition": {`attachment; filename="sample.txt"`}},
			Body: io.NopCloser(strings.NewReader(original))}, nil
	})}
	landings := 0
	session := NewBrowserSession(nil, client, func(_ context.Context, entry, prefix, expression string) (string, error) {
		landings++
		if entry != landing || prefix != "/file/sample/" || !strings.Contains(expression, "a#downloadButton[href]") {
			t.Fatal("browser did not receive the selected public page and ordinary selector")
		}
		return body, nil
	})
	file := WebsiteFile{URL: landing, Account: account, ID: key, Name: name, Size: int64(len(original))}
	result, err := session.Download(t.Context(), file)
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(result.Response.Body)
	if err = errors.Join(err, result.Response.Body.Close()); err != nil || string(data) != original || calls != 1 || landings != 1 {
		t.Fatalf("public download did not complete: requests=%d %v", calls, err)
	}
	file.ID = "another"
	if _, err := session.Download(t.Context(), file); core.CodeOf(err) != core.InvalidRequest || landings != 1 {
		t.Fatal("unbound landing reached browser", err)
	}
	file.ID = key
	responseLocation = "https://elsewhere.invalid/redirect"
	result, err = session.Download(t.Context(), file)
	if result.Response != nil {
		_ = result.Response.Body.Close()
	}
	if err == nil || calls != 2 {
		t.Fatal("public redirect was followed", err)
	}
}

func TestBrowserWebsiteFolderMutations(t *testing.T) {
	const account, key, name = "bound", "mweavefile12345", "sample.zip"
	const folderKey, folder = "mweavefolder1", "/folder"
	var calls int
	var fault string
	var mkdirCalls, moveCalls int
	newFolder := ""
	session := NewBrowserSession(func(_ context.Context, _ string, raw json.RawMessage) (json.RawMessage, error) {
		var q browserRequest
		if err := json.Unmarshal(raw, &q); err != nil {
			t.Fatal(err)
		}
		switch q.Operation {
		case "mkdir":
			if q.Account != account || q.ObjectID != "myfiles" {
				t.Fatalf("wrong mkdir binding: %+v", q)
			}
			mkdirCalls++
			r := browserReply{Status: 200, Account: account}
			switch q.Name {
			case "new":
				newFolder, r.ObjectID = q.Name, "mweavefolder2"
			case "wrong":
				r.ObjectID = "mweavefolder3"
			case "folder", name:
			default:
				t.Fatalf("unexpected mkdir name: %q", q.Name)
			}
			return json.Marshal(r)
		case "move":
			moveCalls++
			if q.Account != account || q.ObjectID != key || q.ParentID != folderKey {
				t.Fatalf("wrong file and directory binding: %+v", q)
			}
			r := browserReply{Status: 200, Account: account, ObjectID: key, Name: "sample (1).zip"}
			if fault == "wrong move ID" {
				r.ObjectID = "unrelatedid1234"
			}
			return json.Marshal(r)
		case "recycle", "recycle-folder":
			calls++
			want := key
			if q.Operation == "recycle-folder" {
				want = folderKey
			}
			if q.Account != account || q.ObjectID != want {
				t.Fatalf("wrong recycle request: %+v", q)
			}
			if fault == "wrong ID" {
				return json.Marshal(browserReply{Status: 200, Account: account, ObjectID: "unrelatedid1234"})
			}
			if fault == "wrong account" {
				return json.Marshal(browserReply{Mismatch: true})
			}
			if fault == "pending" {
				return json.Marshal(browserReply{Error: "pending"})
			}
			return json.Marshal(browserReply{Status: 200, Account: account, ObjectID: want})
		}
		if q.Operation != "list" || q.List.FolderKey != "myfiles" && q.List.FolderKey != folderKey {
			t.Fatalf("wrong directory request: %+v", q)
		}
		var rows string
		if q.List.FolderKey == "myfiles" {
			rows = fmt.Sprintf(`{"folderkey":%q,"name":"folder"}`, folderKey)
			if newFolder != "" {
				rows += fmt.Sprintf(`,{"folderkey":"mweavefolder2","name":%q}`, newFolder)
			}
			if q.List.ContentType == "files" {
				rows = fmt.Sprintf(`{"quickkey":%q,"filename":%q,"size":"2"}`, key, name)
			}
		} else if q.List.ContentType == "files" && moveCalls > 0 {
			rows = fmt.Sprintf(`{"quickkey":%q,"filename":"sample (1).zip","size":"2"}`, key)
			if fault == "duplicate move name" {
				rows += `,{"quickkey":"existingfile123","filename":"sample (1).zip","size":"1"}`
			}
		}
		data := fmt.Sprintf(`{"response":{"action":"folder/get_content","result":"Success","folder_content":{
			"folderkey":%q,"content_type":%q,"chunk_number":"1","more_chunks":"no",%q:[%s]}}}`,
			q.List.FolderKey, q.List.ContentType, q.List.ContentType, rows)
		return json.Marshal(browserReply{Account: account, Status: 200, Body: json.RawMessage(data)})
	}, nil, nil)
	ctx := t.Context()
	d, err := OpenWebsite(ctx, WebsiteOptions{Session: session, Account: account, Release: func() error { return nil }})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = d.Close() })
	for _, scenario := range []struct {
		name   string
		effect core.Effect
		code   core.Code
	}{
		{"new", core.Applied, ""},
		{"folder", core.Unchanged, core.AlreadyExists},
		{name, core.Unchanged, core.AlreadyExists},
		{"wrong", core.Unknown, core.InvalidUpstreamResponse},
	} {
		got, err := d.Mkdir(ctx, core.NativePath("/"+scenario.name))
		if got.Effect != scenario.effect || scenario.code == "" && err != nil ||
			scenario.code != "" && core.CodeOf(err) != scenario.code {
			t.Fatalf("Mkdir %q = %+v, %v", scenario.name, got, err)
		}
	}
	if mkdirCalls != 4 {
		t.Fatalf("native mkdir requests = %d", mkdirCalls)
	}
	typed := core.RemoveOptions{Target: core.RemoveNonDirectory}
	for _, test := range []struct {
		fault, path string
		subtree     bool
		effect      core.Effect
		code        core.Code
		calls       int
	}{
		{"", folder, false, core.Unchanged, core.UnsupportedOperation, 0},
		{"wrong account", "/" + name, false, core.Unknown, core.UpstreamAccessDenied, 1},
		{"wrong ID", "/" + name, false, core.Unknown, core.InvalidUpstreamResponse, 2},
		{"", "/" + name, false, core.Applied, core.CodeOf(nil), 3},
		{"", "/", true, core.Unchanged, core.UnsupportedOperation, 3},
		{"", "/" + name, true, core.Unchanged, core.NotDirectory, 3},
		{"pending", folder, true, core.Unknown, core.UpstreamFailure, 4},
		{"wrong ID", folder, true, core.Unknown, core.InvalidUpstreamResponse, 5},
		{"", folder, true, core.Applied, core.CodeOf(nil), 6},
	} {
		fault = test.fault
		var got core.Change
		if test.subtree {
			got, err = d.RemoveSubtree(ctx, core.NativePath(test.path), core.TreeOptions{})
		} else {
			got, err = d.Remove(ctx, core.NativePath(test.path), typed)
		}
		if got.Effect != test.effect || core.CodeOf(err) != test.code || calls != test.calls {
			t.Fatalf("%+v: %+v, %v, recycle calls=%d", test, got, err, calls)
		}
	}
	target := core.NativePath("/" + name)
	typed.Disposition = core.RemovePermanent
	if got, err := d.RemoveWithDisposition(ctx, target, typed); got.Effect != core.Unchanged ||
		core.CodeOf(err) != core.UnsupportedOperation || calls != 6 {
		t.Fatalf("permanent removal reached Trash: %+v, %v, calls=%d", got, err, calls)
	}
	typed.Disposition = core.RemoveTrash
	if got, err := d.RemoveWithDisposition(ctx, target, typed); got.Effect != core.Applied || err != nil || calls != 7 {
		t.Fatalf("explicit Trash: %+v, %v, calls=%d", got, err, calls)
	}

	if _, got, err := d.RelocateInto(ctx, core.NativePath(folder), core.NativePath("/")); got.Effect != core.Unchanged ||
		core.CodeOf(err) != core.UnsupportedOperation || moveCalls != 0 {
		t.Fatalf("directory move = %+v, %v", got, err)
	}
	if leaf, got, err := d.RelocateInto(ctx, target, core.NativePath(folder)); leaf != "sample (1).zip" ||
		got.Effect != core.Applied || err != nil || moveCalls != 1 {
		t.Fatalf("renamed move = %q, %+v, %v", leaf, got, err)
	}
	for _, fault = range []string{"wrong move ID", "duplicate move name"} {
		if leaf, got, err := d.RelocateInto(ctx, target, core.NativePath(folder)); leaf != "" ||
			got.Effect != core.Unknown || core.CodeOf(err) != core.InvalidUpstreamResponse {
			t.Fatalf("move %q = %q, %+v, %v", fault, leaf, got, err)
		}
	}
	if moveCalls != 3 {
		t.Fatalf("native move requests = %d", moveCalls)
	}
}

func TestBrowserWebsiteCreate(t *testing.T) {
	const account, name, key, uploadKey = "bound", "report.zip", "nativefile1", "upload-attempt"
	const digest = "bef57ec7f53a6d40beb640a780a639c83bc29ac8a9816f1fc6c5c6dcd93c4721"
	for _, test := range []struct {
		name   string
		effect core.Effect
		code   core.Code
	}{{"confirmed", core.Applied, core.CodeOf(nil)}, {"duplicate", core.Unchanged, core.AlreadyExists},
		{"missing proof", core.Unknown, core.InvalidUpstreamResponse}, {"early duplicate", core.Unchanged, core.AlreadyExists}, {"directory duplicate", core.Unchanged, core.AlreadyExists}, {"short input", core.Unchanged, core.CodeOf(io.ErrUnexpectedEOF)}} {
		t.Run(test.name, func(t *testing.T) {
			var checks, units, polls int
			staging := t.TempDir()
			call := func(_ context.Context, _ string, raw json.RawMessage) (json.RawMessage, error) {
				var q browserRequest
				if err := json.Unmarshal(raw, &q); err != nil {
					t.Fatal(err)
				}
				r := browserReply{Account: account, Status: 200}
				if q.Operation == "list" {
					rows := "[]"
					if q.List.ContentType == "folders" && test.name == "directory duplicate" {
						rows = fmt.Sprintf(`[{"folderkey":"collisiondir1","name":%q}]`, name)
					}
					if q.List.ContentType == "files" && polls > 0 && test.name == "confirmed" {
						rows = fmt.Sprintf(`[{"quickkey":%q,"filename":%q,"size":"6"}]`, key, name)
					}
					r.Body = json.RawMessage(fmt.Sprintf(`{"response":{"action":"folder/get_content","result":"Success","folder_content":{"folderkey":"myfiles","content_type":%q,"chunk_number":"1","more_chunks":"no",%q:%s}}}`, q.List.ContentType, q.List.ContentType, rows))
					return json.Marshal(r)
				}
				u := q.Upload
				if q.Account != account || u == nil || u.Parent != "myfiles" || u.Name != name || u.Hash != digest || u.Length != 6 {
					t.Fatal("unbound upload")
				}
				receipt := uploadReceipt{Units: 2, UnitBytes: 3, Bitmap: []uint16{0}}
				switch q.Operation {
				case "upload-check":
					checks++
					if test.name == "early duplicate" {
						receipt = uploadReceipt{Exists: true}
					}
					if test.name == "directory duplicate" {
						return json.Marshal(browserReply{Error: "receipt"})
					}
					if units == 1 {
						receipt.Key, receipt.Bitmap[0] = uploadKey, 1
					}
				case "upload-unit":
					units++
					if units == 1 && u.Unit == 0 && string(u.Data) == "abc" ||
						units == 2 && u.Unit == 1 && string(u.Data) == "def" && u.Key == uploadKey {
						return nil, io.ErrUnexpectedEOF
					}
					t.Fatal("unexpected part or blind retry")
				case "upload-poll":
					polls++
					if u.Key != uploadKey || len(u.Data) != 0 {
						t.Fatal("poll lost the bound upload key")
					}
					receipt = uploadReceipt{Terminal: 99, FileKey: key, Name: name, Hash: digest, Size: 6}
					if test.name == "duplicate" {
						receipt.FileKey, receipt.Failure = "", 13
					}
				default:
					t.Fatal("unexpected website operation")
				}
				r.Body, _ = json.Marshal(receipt)
				return json.Marshal(r)
			}
			w, err := OpenWebsite(t.Context(), WebsiteOptions{Session: NewBrowserSession(call, nil, nil), Account: account,
				Release: func() error { return nil }, Upload: &WebsiteUploadOptions{StagingDirectory: staging, MaxStagingBytes: 10}})
			if err != nil {
				t.Fatal(err)
			}
			defer w.Close()
			length := int64(6)
			wantChecks, wantUnits, wantPolls := 2, 2, 1
			switch test.name {
			case "short input":
				length, wantChecks, wantUnits, wantPolls = 7, 0, 0, 0
			case "early duplicate", "directory duplicate":
				wantChecks, wantUnits, wantPolls = 1, 0, 0
			}
			change, err := w.Create(t.Context(), core.NativePath("/"+name), strings.NewReader("abcdef"), &length)
			if change.Effect != test.effect || core.CodeOf(err) != test.code || checks != wantChecks || units != wantUnits || polls != wantPolls {
				t.Fatalf("change=%+v err=%v checks=%d units=%d polls=%d", change, err, checks, units, polls)
			}
			if entries, err := os.ReadDir(staging); err != nil || len(entries) != 0 {
				t.Fatalf("staging cleanup: entries=%d err=%v", len(entries), err)
			}
		})
	}
}
