package guangyapan_test

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
	"github.com/nostalume/mountweave/drivers/guangyapan"
)

type transport func(*http.Request) (*http.Response, error)

func (f transport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestWebsiteMountBindingPaginationAndOrdinaryBody(t *testing.T) {
	ctx := t.Context()
	account := strings.Repeat("a", 64)
	const fileID = "9007199254740993"
	const folderID = "folder-id"
	var bodyCalls, releases int
	content := &http.Client{Transport: transport(func(r *http.Request) (*http.Response, error) {
		bodyCalls++
		if r.URL.String() != "https://body.example/file?sig=a%2B" || r.Header.Get("Authorization") != "" || r.Header.Get("Cookie") != "" {
			t.Fatal("website body request changed the signed address or sent account credentials")
		}
		return &http.Response{StatusCode: 200, ContentLength: 3, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("zip"))}, nil
	})}
	wrongParent := false
	denied := false
	wrongAccount := false
	var mkdirCode int
	var mkdirParent string
	fileName := "sample.zip"
	var renameCalls int
	confirmRename := false
	var moveCalls int
	wrongTask, moved := true, false
	session := guangyapan.NewBrowserSession(func(_ context.Context, script string, args json.RawMessage) (json.RawMessage, error) {
		var q struct {
			Operation, Account, ParentID, FileID, Name, TaskID string
			Index, PageSize, Size                              int64
		}
		if err := json.Unmarshal(args, &q); err != nil {
			t.Fatal(err)
		}
		if script == "" || (q.Operation != "account" && q.Account != account) {
			t.Fatal("website request lost its account binding")
		}
		if wrongAccount {
			return json.RawMessage(`{"Mismatch":true}`), nil
		}
		switch q.Operation {
		case "account":
			return json.Marshal(map[string]any{"Status": 200, "Account": account})
		case "list":
			if q.ParentID == folderID && q.Index == 0 {
				list := []map[string]any{{"fileId": "occupant", "fileName": "renamed.zip", "resType": 2}}
				if moved {
					list = append(list, map[string]any{"fileId": fileID, "fileName": "renamed (1).zip",
						"resType": 1, "fileSize": 3, "parentId": folderID})
				}
				return json.Marshal(map[string]any{"Status": 200, "Account": account, "Total": len(list), "Page": 0, "List": list})
			}
			if q.ParentID != "" || q.PageSize != 50 || q.Index > 1 {
				t.Fatal("website directory pagination changed")
			}
			var list []map[string]any
			if q.Index == 0 {
				list = append(list, map[string]any{"fileId": folderID, "fileName": "folder", "resType": 2})
				for j := range 49 {
					list = append(list, map[string]any{"fileId": fmt.Sprint(j), "fileName": fmt.Sprintf("f-%d", j), "resType": 1})
				}
			} else {
				entry := map[string]any{"fileId": fileID, "fileName": fileName, "resType": 1, "fileSize": 3}
				if wrongParent {
					entry["parentId"] = "other"
				}
				list = append(list, entry)
			}
			total := 51
			if moved {
				if q.Index != 0 {
					t.Fatal("moved item was still paged from the source")
				}
				total = 50
			}
			return json.Marshal(map[string]any{"Status": 200, "Account": account, "Total": total, "Page": q.Index, "List": list})
		case "mkdir":
			if q.ParentID != folderID || q.Name != "new" || !strings.Contains(script, "failIfNameExist:true") {
				t.Fatal("website mkdir lost its native parent, name, or exclusive policy")
			}
			reply := map[string]any{"Status": 200, "Account": account, "Code": mkdirCode}
			if mkdirCode == 0 {
				reply["Folder"] = map[string]any{"fileId": "new-id", "fileName": "new", "parentId": mkdirParent, "resType": 2}
			}
			return json.Marshal(reply)
		case "rename":
			if q.FileID != fileID || q.Name != "occupied.zip" && q.Name != "renamed.zip" ||
				!strings.Contains(script, "/userres/v1/file/rename") {
				t.Fatal("website rename lost its native identity or exact target")
			}
			renameCalls++
			reply := map[string]any{"Status": 200, "Account": account}
			if q.Name == "occupied.zip" {
				reply["Code"] = 160
			} else if confirmRename {
				fileName = q.Name
			}
			return json.Marshal(reply)
		case "move":
			if q.FileID != fileID || q.ParentID != folderID || !strings.Contains(script, "/userres/v1/file/move_file") {
				t.Fatal("move lost its source identity or destination parent")
			}
			moveCalls++
			return json.Marshal(map[string]any{"Status": 200, "Account": account, "TaskID": "bound-task"})
		case "move-status":
			if q.TaskID != "bound-task" {
				t.Fatal("poll lost the issued task")
			}
			task := q.TaskID
			if wrongTask {
				task = "other"
			} else {
				moved = true
			}
			return json.Marshal(map[string]any{"Status": 200, "Account": account, "TaskID": task, "State": 2})
		case "download":
			if q.FileID != fileID || q.Size != 3 {
				t.Fatal("download broke the selected file binding")
			}
			if denied {
				return json.RawMessage(`{"Denied":true}`), nil
			}
			return json.Marshal(map[string]any{"Status": 200, "Account": account, "FileID": fileID, "SignedURL": "https://body.example/file?sig=a%2B"})
		}
		t.Fatal("unexpected browser operation")
		return nil, nil
	})
	if got, err := session.Account(ctx); err != nil || got != account {
		t.Fatalf("verify account: %v", err)
	}
	options := guangyapan.WebsiteOptions{Session: session, Account: account, RootPath: "/", RootID: "/", ContentClient: content, Release: func() error { releases++; return nil }}
	d, err := guangyapan.OpenWebsite(ctx, options)
	if err != nil || d.RootID() != "/" {
		t.Fatalf("mount website account root: %v", err)
	}
	file := core.NativePath("/sample.zip")
	opened, err := d.Open(ctx, file, core.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if body, readErr := io.ReadAll(opened.Body); string(body) != "zip" || errors.Join(readErr, opened.Body.Close()) != nil {
		t.Fatalf("ordinary body did not complete: %v", readErr)
	}
	target := core.NativePath("/folder/new")
	for _, test := range []struct {
		code   int
		parent string
		effect core.Effect
		before core.TargetPresence
		err    core.Code
	}{
		{0, folderID, core.Applied, core.TargetMissing, ""},
		{159, "", core.Unchanged, core.TargetPresent, core.AlreadyExists},
		{160, "", core.Unchanged, core.TargetPresent, core.AlreadyExists},
		{0, "wrong-parent", core.Unknown, 0, core.InvalidUpstreamResponse},
	} {
		mkdirCode, mkdirParent = test.code, test.parent
		result, err := d.Mkdir(ctx, target)
		if result.Effect != test.effect || result.Before != test.before ||
			(test.err == "" && err != nil) || (test.err != "" && core.CodeOf(err) != test.err) {
			t.Fatalf("mkdir native code %d: result=%+v error=%v", test.code, result, err)
		}
	}
	denied = true
	if _, err = d.Open(ctx, file, core.OpenOptions{}); core.CodeOf(err) != core.UnsupportedOperation || bodyCalls != 1 {
		t.Fatalf("website ordinary download restriction was lost: %v", err)
	}
	wrongParent = true
	if _, err = d.Open(ctx, file, core.OpenOptions{}); core.CodeOf(err) != core.InvalidUpstreamResponse || bodyCalls != 1 {
		t.Fatalf("wrong native parent was accepted: %v", err)
	}
	wrongParent = false
	renamed := core.NativePath("/renamed.zip")
	if result, err := d.Relocate(ctx, file, core.NativePath("/folder/sample.zip")); result.Effect != core.Unchanged ||
		core.CodeOf(err) != core.UnsupportedOperation || renameCalls != 0 {
		t.Fatalf("cross-directory rename was not refused before dispatch: %+v %v", result, err)
	}
	if result, err := d.Relocate(ctx, core.NativePath("/folder"), renamed); result.Effect != core.Unchanged ||
		core.CodeOf(err) != core.UnsupportedOperation || renameCalls != 0 {
		t.Fatalf("directory rename was not refused before dispatch: %+v %v", result, err)
	}
	if result, err := d.Relocate(ctx, file, core.NativePath("/occupied.zip")); result.Effect != core.Unchanged ||
		result.Before != core.TargetPresent || core.CodeOf(err) != core.AlreadyExists || renameCalls != 1 {
		t.Fatalf("native name conflict did not preserve the target: %+v %v", result, err)
	}
	if result, err := d.Relocate(ctx, file, renamed); result.Effect != core.Unknown ||
		core.CodeOf(err) != core.InvalidUpstreamResponse || renameCalls != 2 {
		t.Fatalf("success without destination readback was accepted: %+v %v", result, err)
	}
	confirmRename = true
	if result, err := d.Relocate(ctx, file, renamed); result.Effect != core.Applied ||
		result.Before != core.TargetMissing || err != nil || renameCalls != 3 {
		t.Fatalf("bound rename did not complete: %+v %v", result, err)
	}
	directory := core.NativePath("/folder")
	if name, result, err := d.RelocateInto(ctx, renamed, directory); name != "" || result.Effect != core.Unknown ||
		core.CodeOf(err) != core.InvalidUpstreamResponse || moveCalls != 1 {
		t.Fatalf("a different task confirmed the move: %q %+v %v", name, result, err)
	}
	wrongTask = false
	if name, result, err := d.RelocateInto(ctx, renamed, directory); name != "renamed (1).zip" ||
		result.Effect != core.Applied || err != nil || moveCalls != 2 {
		t.Fatalf("directory move did not return the allocated name: %q %+v %v", name, result, err)
	}
	if err := errors.Join(d.Close(), d.Close()); err != nil || releases != 1 {
		t.Fatalf("website ownership was released %d times: %v", releases, err)
	}
	wrongAccount = true
	if _, err := guangyapan.OpenWebsite(ctx, options); !errors.Is(err, guangyapan.ErrWebsiteBindingMismatch) || releases != 1 {
		t.Fatalf("wrong account or failed constructor changed ownership: %v", err)
	}
}

func TestOwnDriveAndSignedBody(t *testing.T) {
	const rootID, fileID, name = "9007199254740993", "9007199254741001", "space % &"
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var mode atomic.Value
	mode.Store("")
	var secondPages, grants, bodies atomic.Int32
	content := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		bodies.Add(1)
		if r.Method != http.MethodGet || r.Header.Get("Authorization") != "" || r.Header.Get("X-Client-ID") != "" ||
			r.Header.Get("Cookie") != "" || r.Header.Get("Referer") != "" || r.Header.Get("Accept-Encoding") != "identity" {
			t.Error("signed request used API credentials or the wrong method")
		}
		if r.URL.Path == "/redirect" {
			w.Header().Set("Location", "/file?token=cap%2b")
			w.WriteHeader(http.StatusFound)
			return
		}
		if r.URL.Path != "/file" || r.URL.RawQuery != "token=cap%2b" {
			t.Error("signed body target was changed")
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		switch mode.Load() {
		case "wait":
			w.Header().Set("Content-Length", "6")
			w.WriteHeader(http.StatusOK)
			w.(http.Flusher).Flush()
			<-r.Context().Done()
		case "wrong-size":
			_, _ = io.WriteString(w, "abcd")
		case "unframed-short", "unframed-long":
			w.(http.Flusher).Flush()
			if mode.Load() == "unframed-short" {
				_, _ = io.WriteString(w, "abcd")
			} else {
				_, _ = io.WriteString(w, "abcdefg")
			}
		default:
			if r.Header.Get("Range") == "bytes=1-3" {
				w.Header().Set("Content-Range", "bytes 1-3/6")
				w.Header().Set("Content-Length", "3")
				w.WriteHeader(http.StatusPartialContent)
				_, _ = io.WriteString(w, "bcd")
			} else {
				_, _ = io.WriteString(w, "abcdef")
			}
		}
	}))
	defer content.Close()
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.Header.Get("Authorization") != "Bearer fixture" || r.Header.Get("X-Client-ID") != "fixture-app" {
			t.Error("native request did not carry the approved application and user credentials")
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		reply := func(code int, data any) {
			_ = json.NewEncoder(w).Encode(map[string]any{"code": code, "msg": "fixture-secret", "data": data})
		}
		q := r.URL.Query()
		switch r.URL.Path {
		case "/openapi/v1/file/get_file_detail":
			if q.Get("fileId") != rootID {
				t.Error("selected root ID changed")
			}
			id := rootID
			if mode.Load() == "wrong-root" {
				id = "other"
			}
			reply(0, map[string]any{"fileInfo": map[string]any{"fileId": id, "resType": 2}})
		case "/openapi/v1/file/get_file_list":
			if q.Get("parentId") != rootID || q.Get("pageSize") != "20" || q.Get("orderBy") != "0" || q.Get("sortType") != "0" {
				t.Error("native listing left its root or changed paging parameters")
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			item := func(id, name, parent string) map[string]any {
				return map[string]any{"fileId": id, "fileName": name, "parentId": parent, "resType": 1, "fileSize": 6}
			}
			switch q.Get("page") {
			case "0":
				entries := make([]map[string]any, 20)
				for j := range entries {
					entries[j] = item(fmt.Sprintf("f%02d", j), fmt.Sprintf("entry%02d", j), rootID)
				}
				if mode.Load() == "duplicate" {
					entries[0]["fileName"] = name
				}
				reply(0, map[string]any{"total": 21, "list": entries})
			case "1":
				secondPages.Add(1)
				if mode.Load() == "shrink" {
					reply(0, map[string]any{"total": 19, "list": []any{}})
					return
				}
				parent := rootID
				if mode.Load() == "wrong-parent" {
					parent = "other"
				}
				reply(0, map[string]any{"total": 21, "list": []any{item(fileID, name, parent)}})
			default:
				t.Error("unexpected native continuation")
				w.WriteHeader(http.StatusBadRequest)
			}
		case "/openapi/v1/file/get_res_download_url":
			grants.Add(1)
			if q.Get("fileId") != fileID {
				t.Error("resolved file ID changed")
			}
			switch mode.Load() {
			case "token":
				reply(117, "fixture-secret")
			case "wrong-grant":
				reply(0, map[string]string{"url": content.URL + "/file"})
			default:
				reply(0, map[string]any{"signedURL": content.URL + "/redirect", "urlDuration": 21600})
			}
		default:
			t.Error("unexpected native operation")
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer api.Close()
	base, _ := url.Parse(api.URL)
	control := &http.Client{Transport: transport(func(r *http.Request) (*http.Response, error) {
		if r.URL.Scheme != "https" || r.URL.Host != "openapi.guangyapan.com" {
			return nil, errors.New("authenticated request left the vendor origin")
		}
		r = r.Clone(r.Context())
		r.URL.Scheme, r.URL.Host = base.Scheme, base.Host
		r.Header.Set("Authorization", "Bearer fixture")
		return http.DefaultTransport.RoundTrip(r)
	})}
	opts := guangyapan.Options{ClientID: "fixture-app", RootID: rootID, Client: control, ContentClient: content.Client()}
	mode.Store("wrong-root")
	bad, err := guangyapan.Open(ctx, opts)
	if core.CodeOf(err) != core.InvalidUpstreamResponse {
		t.Fatal("wrong native root accepted", err)
	}
	if bad != nil {
		_ = bad.Close()
	}
	mode.Store("")
	driver, err := guangyapan.Open(ctx, opts)
	if err != nil {
		t.Fatal(err)
	}
	c, err := core.New(core.Options{MaxCursors: 2, Authorize: func(context.Context, core.Principal, core.Action, core.Address) error { return nil }})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close(ctx)
	mount, err := c.Install("/remote", driver)
	if err != nil {
		t.Fatal(err)
	}
	list := core.ListRequest{Query: core.Children(core.Path("/remote")), Limit: 20, CursorTTL: time.Minute}
	first, err := c.List(ctx, "u", list)
	if err != nil || len(first.Items) != 20 || first.NextCursor == nil {
		t.Fatal("native first page", first, err)
	}
	list.Cursor = *first.NextCursor
	last, err := c.List(ctx, "u", list)
	if err != nil || len(last.Items) != 1 || last.Items[0].Name != name || last.NextCursor != nil || secondPages.Load() != 1 {
		t.Fatal("native final page", last, err)
	}
	mode.Store("shrink")
	list.Cursor = ""
	first, err = c.List(ctx, "u", list)
	if err != nil || first.NextCursor == nil {
		t.Fatal("live listing first page", err)
	}
	list.Cursor = *first.NextCursor
	last, err = c.List(ctx, "u", list)
	if err != nil || len(last.Items) != 0 || last.NextCursor != nil {
		t.Fatal("live total shrink failed to end enumeration", last, err)
	}
	target := core.Path("/remote/" + name)
	for state, code := range map[string]core.Code{"wrong-parent": core.InvalidUpstreamResponse, "duplicate": core.PathAmbiguous} {
		mode.Store(state)
		if _, err := c.Open(ctx, "u", target, core.OpenOptions{}); core.CodeOf(err) != code || grants.Load() != 0 {
			t.Fatal("invalid native selection reached a grant", state, err)
		}
	}
	mode.Store("token")
	if _, err := c.Open(ctx, "u", target, core.OpenOptions{}); core.CodeOf(err) != core.UpstreamAccessDenied ||
		strings.Contains(err.Error(), "fixture-secret") {
		t.Fatal("native error lost classification or leaked data", err)
	} else if native := (*guangyapan.APIError)(nil); !errors.As(err, &native) || native.Code != 117 {
		t.Fatal("native error lost its code", err)
	}
	mode.Store("wrong-grant")
	if _, err := c.Open(ctx, "u", target, core.OpenOptions{}); core.CodeOf(err) != core.InvalidUpstreamResponse || bodies.Load() != 0 {
		t.Fatal("invalid grant was used", err)
	}
	mode.Store("wrong-size")
	if _, err := c.Open(ctx, "u", target, core.OpenOptions{}); core.CodeOf(err) != core.InvalidUpstreamResponse {
		t.Fatal("wrong body size accepted", err)
	}
	for _, state := range []string{"unframed-short", "unframed-long"} {
		mode.Store(state)
		opened, err := c.Open(ctx, "u", target, core.OpenOptions{})
		if err != nil {
			t.Fatal(state, err)
		}
		data, err := io.ReadAll(opened.Body)
		err = errors.Join(err, opened.Body.Close())
		if opened.Length != nil || state == "unframed-short" && (string(data) != "abcd" || !errors.Is(err, io.ErrUnexpectedEOF)) ||
			state == "unframed-long" && (string(data) != "abcdef" || core.CodeOf(err) != core.InvalidUpstreamResponse) {
			t.Fatal(state, string(data), err)
		}
	}
	mode.Store("")
	opened, err := c.Open(ctx, "u", target, core.OpenOptions{Range: &core.Range{Start: 1, End: 3}})
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(opened.Body)
	if err = errors.Join(err, opened.Body.Close()); err != nil || string(data) != "bcd" ||
		opened.Range == nil || *opened.Range != (core.ContentRange{Start: 1, End: 3, Total: 6}) {
		t.Fatal("delegated bytes or actual range lost", string(data), opened.Range, err)
	}
	opened, err = c.Open(ctx, "u", target, core.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	data, err = io.ReadAll(opened.Body)
	if err = errors.Join(err, opened.Body.Close()); err != nil || string(data) != "abcdef" || opened.Range != nil {
		t.Fatalf("full original: %q, range=%v, error=%v", data, opened.Range, err)
	}
	mode.Store("wait")
	opened, err = c.Open(ctx, "u", target, core.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { _, err := io.ReadAll(opened.Body); done <- err }()
	if err := mount.Retire(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal("retirement did not cancel body", err)
	}
}
