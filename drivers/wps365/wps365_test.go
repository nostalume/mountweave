package wps365

import (
	"context"
	"encoding/json"
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

func (f wire) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func reply(r *http.Request, status int, body string) *http.Response {
	return &http.Response{
		StatusCode: status, Request: r, Header: make(http.Header),
		Body: io.NopCloser(strings.NewReader(body)), ContentLength: int64(len(body)),
	}
}

type failedResponseClose struct {
	io.ReadCloser
	calls int
	cause error
}

func (b *failedResponseClose) Close() error {
	b.calls++
	if b.calls == 1 {
		return b.cause
	}
	return b.ReadCloser.Close()
}

func TestV7Mount(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	const base = "/v7/drives/drive-a/files/"
	const capability = "https://files.example/report?sig=private"
	const folder = `{"id":"folder","drive_id":"drive-a","parent_id":"0","name":"team","type":"folder"}`
	const file = `{"id":"file","drive_id":"drive-a","parent_id":"folder","name":"Report 版","type":"file","size":6}`
	const shortcut = `{"id":"shortcut","drive_id":"drive-a","parent_id":"0","name":"other","type":"shortcut"}`
	const token = "token+ /="
	var mode string
	bodyCalls, creates, deletes, cyclePages := 0, 0, 0, 0
	var deleted bool
	var deletionBody, rejectedBody *failedResponseClose
	started := make(chan struct{}, 1)
	control := &http.Client{Transport: wire(func(r *http.Request) (*http.Response, error) {
		if r.URL.Scheme != "https" || r.URL.Host != "openapi.wps.cn" ||
			r.Header.Get("Content-Type") != "application/json" {
			t.Errorf("incorrect control request: %s %s", r.Method, r.URL)
			return nil, errors.New("unexpected control request")
		}
		r.Header.Set("Authorization", "Bearer synthetic")
		r.Header.Set("X-Kso-Authorization", "KSO-1 synthetic")
		if r.Method == http.MethodPost {
			if r.URL.Path == base+"file/delete" {
				deletes++
				if r.Body != nil && r.Body != http.NoBody || r.URL.RawQuery != "" {
					t.Error("file deletion did not use the selected native ID alone")
				}
				if mode == "lost-delete" {
					return nil, io.ErrUnexpectedEOF
				}
				deleted = true
				response := reply(r, http.StatusOK, `{"code":0}`)
				deletionBody = &failedResponseClose{ReadCloser: response.Body, cause: io.ErrUnexpectedEOF}
				response.Body = deletionBody
				return response, nil
			}
			creates++
			if r.Body == nil {
				return nil, errors.New("unexpected control POST")
			}
			defer r.Body.Close()
			var fields map[string]string
			if err := json.NewDecoder(r.Body).Decode(&fields); err != nil || r.URL.Path != base+"0/create" ||
				r.GetBody != nil || len(fields) != 3 || fields["name"] != "new folder" ||
				fields["file_type"] != "folder" || fields["on_name_conflict"] != "fail" {
				return nil, errors.New("unexpected directory creation request")
			}
			switch mode {
			case "conflict":
				return reply(r, http.StatusBadRequest, `{"code":400008007}`), nil
			case "bad-receipt":
				return reply(r, 200, `{"code":0,"data":`+folder+`}`), nil
			}
			created := `{"id":"new-folder","drive_id":"drive-a","parent_id":"0","name":"new folder","type":"folder"}`
			return reply(r, 200, `{"code":0,"data":`+created+`}`), nil
		}
		if r.Method != http.MethodGet {
			return nil, errors.New("unexpected WPS control method")
		}
		var data string
		switch r.URL.Path {
		case base + "0/children":
			if r.URL.Query().Get("page_size") == "" {
				t.Error("missing WPS page size")
			}
			switch r.URL.Query().Get("page_token") {
			case "":
				data = `{"items":[` + folder + `],"next_page_token":"` + token + `"}`
			case token:
				data = `{"items":[` + shortcut + `]}`
			default:
				t.Errorf("incorrect native cursor: %s", r.URL.RawQuery)
			}
		case base + "folder/meta":
			data = folder
			if mode == "foreign-drive" {
				data = strings.Replace(folder, "drive-a", "drive-b", 1)
			}
		case base + "folder/children":
			if mode == "cursor-cycle" {
				cyclePages++
				if cyclePages > 3 {
					return nil, errors.New("listing fetched a repeated cursor")
				}
				next := "cycle-a"
				if r.URL.Query().Get("page_token") == next {
					next = "cycle-b"
				}
				data = fmt.Sprintf(`{"items":[],"next_page_token":%q}`, next)
				break
			}
			if mode == "cancel" {
				started <- struct{}{}
				<-r.Context().Done()
				return nil, r.Context().Err()
			}
			if deleted {
				data = `{"items":[]}`
				break
			}
			if mode == "duplicate" && r.URL.Query().Get("page_token") == "" {
				data = `{"items":[` + file + `],"next_page_token":"two"}`
			} else {
				item := file
				if mode == "duplicate" {
					item = strings.Replace(file, `"id":"file"`, `"id":"second"`, 1)
				} else if mode == "foreign-parent" {
					item = strings.Replace(file, `"parent_id":"folder"`, `"parent_id":"other"`, 1)
				}
				data = `{"items":[` + item + `]}`
			}
		case base + "file/download":
			address := capability
			if mode == "insecure" {
				address = "http://files.example/report"
			}
			data = fmt.Sprintf(`{"url":%q}`, address)
		default:
			t.Errorf("unexpected endpoint: %s", r.URL)
			return nil, errors.New("unexpected endpoint")
		}
		return reply(r, 200, `{"code":0,"data":`+data+`}`), nil
	})}
	content := &http.Client{Transport: wire(func(r *http.Request) (*http.Response, error) {
		bodyCalls++
		if r.URL.String() != capability || r.Header.Get("Authorization") != "" || r.Header.Get("X-Kso-Authorization") != "" ||
			r.Header.Get("Cookie") != "" || r.Header.Get("Range") != "bytes=2-4" {
			t.Errorf("incorrect body request: %s %v", r.URL, r.Header)
		}
		if mode == "redirect" {
			response := reply(r, http.StatusFound, "")
			response.Header.Set("Location", "https://other.example/")
			rejectedBody = &failedResponseClose{ReadCloser: response.Body, cause: fmt.Errorf("%s: %w", capability, io.EOF)}
			response.Body = rejectedBody
			return response, nil
		}
		response := reply(r, http.StatusPartialContent, "cde")
		response.Header.Set("Content-Range", "bytes 2-4/6")
		return response, nil
	})}

	mode = "foreign-drive"
	wrongRoot, err := Open(ctx, Options{DriveID: "drive-a", RootID: "folder", Client: control})
	if wrongRoot != nil {
		t.Cleanup(func() { _ = wrongRoot.Close() })
	}
	if core.CodeOf(err) != core.InvalidUpstreamResponse {
		t.Fatalf("accepted a folder from another drive: %v", err)
	}
	mode = ""
	driver, err := Open(ctx, Options{DriveID: "drive-a", Client: control, ContentClient: content})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = driver.Close() })
	storage, err := core.New(core.Options{Authorize: func(context.Context, core.Principal, core.Action, core.Address) error { return nil }})
	if err != nil {
		t.Fatal(err)
	}
	mount, err := storage.Install("/wps", driver)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = mount.Retire(context.Background()) })
	query := core.Children(core.Path("/wps"))
	page, err := storage.List(ctx, "reader", core.ListRequest{Query: query, CursorTTL: time.Minute})
	if err != nil || len(page.Items) != 1 || page.Items[0].Name != "team" || page.NextCursor == nil {
		t.Fatalf("paged root: %+v, %v", page, err)
	}
	page, err = storage.List(ctx, "reader", core.ListRequest{Query: query, Cursor: *page.NextCursor})
	if err != nil || len(page.Items) != 0 || page.NextCursor != nil {
		t.Fatalf("root continuation: %+v, %v", page, err)
	}
	if _, err := storage.Stat(ctx, "reader", core.Path("/wps/other")); core.CodeOf(err) != core.UnsupportedOperation {
		t.Fatalf("shortcut: %v", err)
	}
	path := core.Path("/wps/team/Report 版")
	mode = "cursor-cycle"
	if _, err := storage.Stat(ctx, "reader", path); core.CodeOf(err) != core.InvalidUpstreamResponse || cyclePages != 3 {
		t.Fatalf("cursor cycle was not stopped: %v, %d native requests", err, cyclePages)
	}
	mode = ""
	meta, err := storage.Stat(ctx, "reader", path)
	if err != nil || meta.Size == nil || *meta.Size != 6 {
		t.Fatalf("metadata: %+v, %v", meta, err)
	}
	absent := core.OpenOptions{Selection: core.ReadSelection{When: core.IfAbsent()}}
	if _, err := storage.Open(ctx, "reader", core.Path("/wps/team/missing"), absent); core.CodeOf(err) != core.NotFound {
		t.Fatalf("condition invented a file: %v", err)
	}
	if _, err := storage.Open(ctx, "reader", path, absent); core.CodeOf(err) != core.PreconditionFailed {
		t.Fatalf("existing file did not fail the absence condition: %v", err)
	}
	options := core.OpenOptions{Range: &core.Range{Start: 2, End: 4}}
	opened, err := storage.Open(ctx, "reader", path, options)
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(opened.Body)
	if err = errors.Join(err, opened.Body.Close()); err != nil || string(body) != "cde" || opened.Range == nil {
		t.Fatalf("range read: %q, %+v, %v", body, opened.Range, err)
	}
	for _, tc := range []struct {
		mode  string
		code  core.Code
		calls int
	}{
		{"duplicate", core.PathAmbiguous, 1},
		{"foreign-parent", core.InvalidUpstreamResponse, 1},
		{"insecure", core.InvalidUpstreamResponse, 1},
		{"redirect", core.UpstreamFailure, 2},
	} {
		mode = tc.mode
		opened, err := storage.Open(ctx, "reader", path, options)
		if core.CodeOf(err) != tc.code || bodyCalls != tc.calls || opened.Body != nil {
			t.Fatalf("%s: %v, %d body requests", mode, err, bodyCalls)
		}
		if mode == "redirect" && (!errors.Is(err, io.ErrUnexpectedEOF) || strings.Contains(err.Error(), capability) || rejectedBody.calls != 1) {
			t.Fatalf("rejected body cleanup: %v, close calls=%d", err, rejectedBody.calls)
		}
	}
	directory := core.Path("/wps/new folder")
	if change, err := storage.Mkdir(ctx, "reader", directory); change.Effect != core.Unchanged ||
		core.CodeOf(err) != core.UnsupportedOperation || creates != 0 {
		t.Fatalf("default mkdir: %+v %v, requests=%d", change, err, creates)
	}
	if change, err := storage.Remove(ctx, "reader", path); change.Effect != core.Unchanged ||
		core.CodeOf(err) != core.UnsupportedOperation || deletes != 0 {
		t.Fatalf("default remove: %+v %v, requests=%d", change, err, deletes)
	}
	writer, err := Open(ctx, Options{DriveID: "drive-a", Client: control, AllowMkdir: true, AllowRemove: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = writer.Close() })
	writeMount, err := storage.Install("/write", writer)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = writeMount.Retire(context.Background()) })
	directory = core.Path("/write/new folder")
	for _, tc := range []struct {
		mode   string
		effect core.Effect
		code   core.Code
	}{
		{"success", core.Applied, ""},
		{"conflict", core.Unchanged, core.AlreadyExists},
		{"bad-receipt", core.Unknown, core.InvalidUpstreamResponse},
	} {
		mode = tc.mode
		before := creates
		change, err := storage.Mkdir(ctx, "reader", directory)
		var code core.Code
		if err != nil {
			code = core.CodeOf(err)
		}
		if change.Effect != tc.effect || code != tc.code || creates != before+1 {
			t.Fatalf("%s: %+v %v, requests=%d", tc.mode, change, err, creates-before)
		}
	}
	filePath := core.Path("/write/team/Report 版")
	for _, refused := range []struct {
		address core.Address
		policy  core.RemoveOptions
	}{
		{core.Path("/write/team"), core.RemoveOptions{}},
		{core.Path("/write/other"), core.RemoveOptions{}},
		{filePath, core.RemoveOptions{Target: core.RemoveNonDirectory}},
		{filePath, core.RemoveOptions{When: core.IfPresent()}},
	} {
		if change, err := storage.Remove(ctx, "reader", refused.address, refused.policy); change.Effect != core.Unchanged ||
			core.CodeOf(err) != core.UnsupportedOperation || deletes != 0 {
			t.Fatalf("remove accepted an unsupported target or policy: %+v %v, requests=%d", change, err, deletes)
		}
	}
	mode = "lost-delete"
	if change, err := storage.Remove(ctx, "reader", filePath); change.Effect != core.Unknown ||
		!errors.Is(err, io.ErrUnexpectedEOF) || deletes != 1 {
		t.Fatalf("lost deletion: %+v %v, requests=%d", change, err, deletes)
	}
	mode = ""
	if change, err := storage.Remove(ctx, "reader", filePath); change.Effect != core.Applied ||
		!errors.Is(err, io.ErrUnexpectedEOF) || deletes != 2 || deletionBody == nil || deletionBody.calls != 1 {
		t.Fatalf("confirmed deletion: %+v %v, requests=%d", change, err, deletes)
	}
	if _, err := storage.Stat(ctx, "reader", filePath); core.CodeOf(err) != core.NotFound {
		t.Fatalf("deleted file remains visible: %v", err)
	}
	if err := writeMount.Retire(ctx); err != nil || deletionBody.calls != 2 {
		t.Fatalf("deletion response cleanup: %v, close calls=%d", err, deletionBody.calls)
	}
	mode = "cancel"
	done := make(chan error, 1)
	go func() {
		_, err := storage.List(ctx, "reader", core.ListRequest{Query: core.Children(core.Path("/wps/team")), CursorTTL: time.Minute})
		done <- err
	}()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("listing did not start")
	}
	if err := mount.Retire(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("retired listing: %v", err)
	}
	if rejectedBody.calls != 2 {
		t.Fatalf("retirement did not retry failed download cleanup: %d calls", rejectedBody.calls)
	}
}
