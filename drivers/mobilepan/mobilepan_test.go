package mobilepan

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	core "github.com/nostalume/mountweave"
)

type observedBody struct {
	io.Reader
	closed bool
}

func (b *observedBody) Close() error { b.closed = true; return nil }

type roundTrip func(*http.Request) (*http.Response, error)

func (f roundTrip) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestPersonalDriveRead(t *testing.T) {
	t.Run("application", func(t *testing.T) { testPersonalDriveRead(t, false) })
	t.Run("website", func(t *testing.T) { testPersonalDriveRead(t, true) })
}

func TestWebsiteRootBinding(t *testing.T) {
	ctx := t.Context()
	account := strings.Repeat("a", 64)
	var released bool
	website := Website{
		ExpectedRootID: "saved-folder",
		Release:        func() error { released = true; return nil },
		Call: func(_ context.Context, _ string, raw json.RawMessage) (json.RawMessage, error) {
			var req struct {
				Route, Account string
				Data           struct{ ParentFileID string }
			}
			if err := json.Unmarshal(raw, &req); err != nil {
				t.Fatal(err)
			}
			if req.Account != account || req.Route != "list" || req.Data.ParentFileID != "/" {
				t.Fatalf("unexpected request: %+v", req)
			}
			return json.RawMessage(`{"success":true,"data":{"items":[{"fileId":"replacement","parentFileId":"/","name":"Folder","type":"folder"}]}}`), nil
		},
	}
	if d, err := website.Open(ctx, account, "/Folder", nil); d != nil || !errors.Is(err, ErrWebsiteBindingMismatch) || released {
		t.Fatalf("replacement folder was accepted or released the caller's session: %v", err)
	}
}

func TestWebsiteRename(t *testing.T) {
	account := strings.Repeat("a", 64)
	for _, test := range []struct {
		name, reply string
		after       string
		effect      core.Effect
		code        core.Code
	}{
		{"applied", `{"fileId":"source","name":"new"}`, `[{"fileId":"source","name":"new","type":"file","size":1}]`, core.Applied, ""},
		{"occupied file", `{"occupied":true}`, `[{"fileId":"other","name":"new","type":"file","size":1}]`, core.Unchanged, core.AlreadyExists},
		{"occupied directory", `{"occupied":true}`, `[{"fileId":"other","name":"new","type":"folder"}]`, core.Unchanged, core.AlreadyExists},
		{"automatic suffix", `{"fileId":"source","name":"other"}`, "", core.Unknown, core.InvalidUpstreamResponse},
		{"different file", `{"fileId":"source","name":"new"}`, `[{"fileId":"other","name":"new","type":"file","size":1}]`, core.Unknown, core.InvalidUpstreamResponse},
	} {
		t.Run(test.name, func(t *testing.T) {
			updates := 0
			driver := &Driver{accountID: account, resolvePages: 2, limit: 4096,
				root: entry{id: rootID, metadata: core.Metadata{Kind: core.Directory}}}
			driver.website.Call = func(_ context.Context, _ string, raw json.RawMessage) (json.RawMessage, error) {
				var req struct {
					Account, Route string
					Data           struct{ FileID, Name, FileRenameMode string }
				}
				if json.Unmarshal(raw, &req) != nil || req.Account != account {
					t.Fatal("invalid native account")
				}
				switch req.Route {
				case "list":
					items := `[{"fileId":"source","name":"old","type":"file","size":1}]`
					if updates > 0 {
						items = test.after
					}
					return json.RawMessage(`{"success":true,"data":{"items":` + items + `}}`), nil
				case "update":
					updates++
					if req.Data.FileID != "source" || req.Data.Name != "new" || req.Data.FileRenameMode != "refuse" {
						t.Fatal("rename did not bind selected file and exact target name")
					}
					return json.RawMessage(`{"success":true,"data":` + test.reply + `}`), nil
				default:
					t.Fatalf("unexpected route %q", req.Route)
					return nil, nil
				}
			}
			if got, err := driver.Relocate(t.Context(), core.NativePath("/old"), core.NativePath("/other/new")); got.Effect != core.Unchanged || core.CodeOf(err) != core.UnsupportedOperation || updates != 0 {
				t.Fatalf("cross-directory move = %+v, %v", got, err)
			}
			got, err := driver.Relocate(t.Context(), core.NativePath("/old"), core.NativePath("/new"))
			if got.Effect != test.effect || updates != 1 || test.code != "" && core.CodeOf(err) != test.code || test.code == "" && err != nil {
				t.Fatalf("rename = %+v, %v", got, err)
			}
		})
	}
}

func TestWebsiteMoveInto(t *testing.T) {
	account := strings.Repeat("a", 64)
	for _, test := range []struct {
		name                                          string
		unboundTask, reboundParent, duplicate, cancel bool
	}{
		{name: "renamed on a later page"},
		{name: "unrelated task", unboundTask: true},
		{name: "replaced destination", reboundParent: true},
		{name: "ambiguous name", duplicate: true},
		{name: "cancelled after dispatch", cancel: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			d := &Driver{accountID: account, resolvePages: 3, limit: 4096,
				root: entry{id: rootID, metadata: core.Metadata{Kind: core.Directory}}}
			var submits, polls, destinationPages int
			d.website.Call = func(_ context.Context, _ string, raw json.RawMessage) (json.RawMessage, error) {
				var request struct {
					Account, Route string
					Data           struct {
						ParentFileID, ToParentFileID, TaskID string
						FileIDs                              []string
						PageInfo                             struct{ PageCursor string }
					}
				}
				if json.Unmarshal(raw, &request) != nil || request.Account != account {
					t.Fatal("invalid native account")
				}
				switch request.Route {
				case "list":
					switch request.Data.ParentFileID {
					case "/":
						targetID := "destination"
						if test.reboundParent && destinationPages > 0 {
							targetID = "replacement"
						}
						return json.RawMessage(`{"success":true,"data":{"items":[{"fileId":"selected","name":"report.txt","type":"file"},{"fileId":"` + targetID + `","name":"target","type":"folder"}]}}`), nil
					case "destination":
						destinationPages++
						if request.Data.PageInfo.PageCursor == "" {
							name := "report.txt"
							if test.duplicate {
								name = "report (1).txt"
							}
							return json.RawMessage(`{"success":true,"data":{"items":[{"fileId":"prior","name":"` + name + `","type":"folder"}],"nextPageCursor":"next"}}`), nil
						}
						if request.Data.PageInfo.PageCursor == "next" {
							return json.RawMessage(`{"success":true,"data":{"items":[{"fileId":"selected","parentFileId":"destination","name":"report (1).txt","type":"file"}]}}`), nil
						}
					}
				case "batchMove":
					submits++
					if len(request.Data.FileIDs) != 1 || request.Data.FileIDs[0] != "selected" || request.Data.ToParentFileID != "destination" {
						t.Fatal("move did not bind the selected file and folder")
					}
					if test.cancel {
						cancel()
					}
					return json.RawMessage(`{"success":true,"data":{"taskId":"move-task"}}`), nil
				case "task/get":
					polls++
					if request.Data.TaskID != "move-task" {
						t.Fatal("wrong native task")
					}
					id := "move-task"
					if test.unboundTask {
						id = "other-task"
					}
					return json.RawMessage(`{"success":true,"data":{"taskInfo":{"taskId":"` + id + `","status":"Succeed"},"batchFileResults":[{"srcFile":{"fileId":"selected"},"errCode":"0000"}]}}`), nil
				}
				t.Fatalf("unexpected request: %+v", request)
				return nil, nil
			}
			name, result, err := d.RelocateInto(ctx, core.NativePath("/report.txt"), core.NativePath("/target"))
			switch {
			case test.cancel:
				if polls != 0 || !errors.Is(err, context.Canceled) {
					t.Fatalf("cancelled result = %v, task polls = %d", err, polls)
				}
			case test.unboundTask, test.reboundParent:
				if core.CodeOf(err) != core.InvalidUpstreamResponse {
					t.Fatalf("invalid receipt = %v", err)
				}
			case test.duplicate:
				if core.CodeOf(err) != core.PathAmbiguous {
					t.Fatalf("ambiguous receipt = %v", err)
				}
			default:
				if name != "report (1).txt" || result.Effect != core.Applied || err != nil || destinationPages != 2 {
					t.Fatalf("move = %q, %+v, %v, pages = %d", name, result, err, destinationPages)
				}
			}
			if submits != 1 || err != nil && (name != "" || result.Effect != core.Unknown) {
				t.Fatalf("move receipt = %q, %+v, %v, submissions = %d", name, result, err, submits)
			}
		})
	}
}

func TestWebsiteMkdir(t *testing.T) {
	account := strings.Repeat("a", 64)
	for _, tt := range []struct {
		name, receipt, children string
		effect                  core.Effect
		code                    core.Code
	}{
		{"new folder", `{"fileId":"new","parentFileId":"/","name":"sample","type":"folder"}`, "", core.Applied, ""},
		{"occupied directory", `{"fileId":"old","parentFileId":"/","name":"sample","exist":true}`, "", core.Unchanged, core.AlreadyExists},
		{"occupied file with sparse receipt", `{"fileId":"old","exist":true}`, `[{"fileId":"old","name":"sample","type":"file","size":1}]`, core.Unchanged, core.AlreadyExists},
		{"mismatched conflict", `{"fileId":"wrong","exist":true}`, `[{"fileId":"old","name":"sample","type":"file","size":1}]`, core.Unknown, core.InvalidUpstreamResponse},
		{"unbound creation", `{"fileId":"new","parentFileId":"wrong","name":"sample"}`, "", core.Unknown, core.InvalidUpstreamResponse},
	} {
		t.Run(tt.name, func(t *testing.T) {
			created := false
			d := &Driver{accountID: account, root: entry{id: rootID, metadata: core.Metadata{Kind: core.Directory}},
				limit: 4096, resolvePages: 3}
			d.website.Call = func(_ context.Context, _ string, raw json.RawMessage) (json.RawMessage, error) {
				var request struct {
					Route, Account string
					Data           struct{ ParentFileID, Name, Type, FileRenameMode string }
				}
				if err := json.Unmarshal(raw, &request); err != nil || request.Account != account || request.Data.ParentFileID != "/" {
					t.Fatal("invalid native scope")
				}
				switch request.Route {
				case "create":
					if created || request.Data.Name != "sample" || request.Data.Type != "folder" || request.Data.FileRenameMode != "refuse" {
						t.Fatal("unexpected native create")
					}
					created = true
					return json.RawMessage(`{"success":true,"data":` + tt.receipt + `}`), nil
				case "list":
					if !created || tt.children == "" {
						t.Fatal("unexpected native post-read")
					}
					return json.RawMessage(`{"success":true,"data":{"items":` + tt.children + `}}`), nil
				default:
					t.Fatalf("unexpected operation %s", request.Route)
					return nil, nil
				}
			}
			got, err := d.Mkdir(t.Context(), core.NativePath("/sample"))
			if !created || got.Effect != tt.effect || (tt.code == "" && err != nil) ||
				(tt.code != "" && core.CodeOf(err) != tt.code) {
				t.Fatalf("mkdir = %+v, %v", got, err)
			}
		})
	}
	if got, err := (&Driver{}).Mkdir(t.Context(), core.NativePath("/sample")); got.Effect != core.Unchanged || core.CodeOf(err) != core.UnsupportedOperation {
		t.Fatalf("application mkdir = %+v, %v", got, err)
	}
}

func testPersonalDriveRead(t *testing.T, website bool) {
	ctx := t.Context()
	const principal core.Principal = "fixture"
	var wrongFile, cyclic, denied bool
	var recycleCalls, taskCalls int
	var cancelRecycle context.CancelFunc
	taskStatus := "Succeed"
	taskReceiptID := "task-1"
	contentRequests := 0
	var cancelAtHeaders context.CancelFunc
	var canceledBody *observedBody
	api := &http.Client{Transport: roundTrip(func(r *http.Request) (*http.Response, error) {
		if r.Method != http.MethodPost || r.URL.Scheme != "https" || r.URL.Host != "openapi.yun.139.com" {
			t.Fatalf("unexpected control target %s", r.URL.Host)
		}
		var req struct {
			ID     string   `json:"fileId"`
			Parent string   `json:"parentFileId"`
			IDs    []string `json:"fileIds"`
			Task   string   `json:"taskId"`
			Page   struct {
				Size   int    `json:"pageSize"`
				Cursor string `json:"pageCursor"`
			} `json:"pageInfo"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatal(err)
		}
		r.Header.Set("Authorization", "fixture-control-credential")
		w := httptest.NewRecorder()
		switch r.URL.String() {
		case fileAPI + "list":
			if req.Parent != "/" || req.Page.Size < 10 || req.Page.Size > 100 {
				t.Fatalf("bad list request %+v", req)
			}
			if req.Page.Cursor == "" {
				io.WriteString(w, `{"success":true,"code":"0000","data":{"items":[],"nextPageCursor":"page-2"}}`)
			} else if req.Page.Cursor == "page-2" && cyclic {
				io.WriteString(w, `{"success":true,"data":{"items":[],"nextPageCursor":"page-2"}}`)
			} else if req.Page.Cursor == "page-2" {
				io.WriteString(w, `{"code":0,"data":{"items":[{"fileId":"id-1","parentFileId":"/","name":"sample","type":"file","size":5,"updatedAt":"2025-01-01T08:00:00+08:00"}]}}`)
			} else {
				t.Fatalf("wrong native cursor %q", req.Page.Cursor)
			}
		case fileAPI + "getDownloadUrl":
			if req.ID != "id-1" {
				t.Fatalf("grant requested for wrong file %q", req.ID)
			}
			id := req.ID
			if wrongFile {
				id = "unrelated"
			}
			data := `{"code":"0000","data":{"fileId":"` + id + `","size":"5","url":"https://body.fixture/object?signature=fixture"}}`
			if website && !wrongFile {
				data = `{"success":true,"data":{"url":"https://body.fixture/object?signature=fixture"}}`
			}
			io.WriteString(w, data)
		case fileAPI + "batchDelete":
			if !website || len(req.IDs) != 1 || req.IDs[0] != "id-1" {
				t.Fatalf("wrong recycle target %+v", req)
			}
			recycleCalls++
			if cancelRecycle != nil {
				cancelRecycle()
			}
			if wrongFile {
				io.WriteString(w, `{"success":true,"data":{"batchFileResults":[{"srcFile":{"fileId":"other"},"errCode":"0000"}]}}`)
			} else {
				io.WriteString(w, `{"success":true,"data":{"taskId":"task-1"}}`)
			}
		case fileAPI + "task/get":
			if !website || req.Task != "task-1" {
				t.Fatalf("wrong recycle task %+v", req)
			}
			taskCalls++
			io.WriteString(w, `{"success":true,"data":{"taskInfo":{"taskId":"`+taskReceiptID+`","status":"`+taskStatus+`"},"batchFileResults":[{"srcFile":{"fileId":"id-1"},"errCode":"0000"}]}}`)
		default:
			t.Fatalf("bad control operation: %s", r.URL.Path)
		}
		return w.Result(), nil
	})}
	content := &http.Client{Transport: roundTrip(func(r *http.Request) (*http.Response, error) {
		contentRequests++
		if r.Method != http.MethodGet || r.URL.Host != "body.fixture" ||
			r.Header.Get("Authorization") != "" || r.Header.Get("Cookie") != "" {
			t.Fatalf("bad download request: host=%s headers=%v", r.URL.Host, r.Header)
		}
		if r.Header.Get("Range") == "" {
			return &http.Response{StatusCode: http.StatusOK, ContentLength: 5,
				Body: io.NopCloser(strings.NewReader("hello"))}, nil
		}
		if r.Header.Get("Range") != "bytes=1-3" {
			t.Fatalf("bad download range %q", r.Header.Get("Range"))
		}
		body := &observedBody{Reader: strings.NewReader("ell")}
		if cancelAtHeaders != nil {
			canceledBody = body
			cancelAtHeaders()
		}
		return &http.Response{StatusCode: http.StatusPartialContent, ContentLength: 3,
			Header: http.Header{"Content-Range": {"bytes 1-3/5"}}, Body: body}, nil
	})}
	var d *Driver
	var err error
	var releases int
	if website {
		id := strings.Repeat("a", 64)
		w := Website{
			Release: func() error { releases++; return nil },
			Call: func(ctx context.Context, script string, raw json.RawMessage) (json.RawMessage, error) {
				var args struct {
					Route, Account string
					Data           json.RawMessage
				}
				if script != websiteRequest || json.Unmarshal(raw, &args) != nil {
					t.Fatal("invalid website call")
				}
				if args.Route == "account" {
					return json.RawMessage(`{"success":true,"data":{"account":"` + id + `"}}`), nil
				}
				if args.Account != id {
					return json.RawMessage(`{"account_mismatch":true}`), nil
				}
				if denied && args.Route == "getDownloadUrl" {
					return json.RawMessage(`{"status":403}`), nil
				}
				req := httptest.NewRequestWithContext(ctx, http.MethodPost, fileAPI+args.Route, strings.NewReader(string(args.Data)))
				res, err := api.Transport.RoundTrip(req)
				if err != nil {
					return nil, err
				}
				defer res.Body.Close()
				return io.ReadAll(res.Body)
			},
		}
		actualID, err := w.AccountID(ctx)
		if err != nil || actualID != id {
			t.Fatalf("account binding failed: %v", err)
		}
		if failed, err := w.Open(ctx, strings.Repeat("b", 64), "/", content); failed != nil || releases != 0 || !errors.Is(err, ErrWebsiteBindingMismatch) {
			t.Fatalf("foreign account received a driver or lost its session: %v", err)
		}
		d, err = w.Open(ctx, actualID, "/", content)
		if err != nil {
			t.Fatal(err)
		}
	} else {
		d, err = Open(ctx, Options{Client: api, ContentClient: content})
		if err != nil {
			t.Fatal(err)
		}
	}
	c, err := core.New(core.Options{Authorize: func(context.Context, core.Principal, core.Action, core.Address) error { return nil }})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := c.Close(ctx); err != nil {
			t.Error(err)
		}
		if website {
			if err := d.Close(); err != nil || releases != 1 {
				t.Errorf("session release count %d, error %v", releases, err)
			}
		}
	}()
	if _, err := c.Install("/phone", d); err != nil {
		t.Fatal(err)
	}
	folder := core.Path("/phone")
	query := core.Children(folder)
	page, err := c.List(ctx, principal, core.ListRequest{Query: query, Limit: 5, CursorTTL: time.Minute})
	if err != nil || len(page.Items) != 0 || page.NextCursor == nil {
		t.Fatalf("missing continuation for the empty native page: %+v %v", page, err)
	}
	page, err = c.List(ctx, principal, core.ListRequest{Query: query, Limit: 5, Cursor: *page.NextCursor})
	if err != nil || len(page.Items) != 1 || page.Items[0].Name != "sample" || page.Items[0].Size == nil || *page.Items[0].Size != 5 {
		t.Fatalf("list did not carry an empty native page: %+v %v", page, err)
	}
	if website {
		denied = true
		if _, err := c.Open(ctx, principal, page.Items[0].Address, core.OpenOptions{}); core.CodeOf(err) != core.UpstreamAccessDenied || errors.Is(err, ErrWebsiteSignIn) || contentRequests != 0 {
			t.Fatalf("file permission denial prompted reconnect or opened content: %v", err)
		}
		denied = false
		full, err := c.Open(ctx, principal, page.Items[0].Address, core.OpenOptions{})
		if err != nil {
			t.Fatal(err)
		}
		data, err := io.ReadAll(full.Body)
		if err := errors.Join(err, full.Body.Close()); err != nil || string(data) != "hello" || full.Range != nil || contentRequests != 1 {
			t.Fatalf("full read after denial: %q, range=%v, error=%v", data, full.Range, err)
		}
	}
	verifiedRequests := contentRequests + 1
	selection := core.OpenOptions{Range: &core.Range{Start: 1, End: 3}}
	open, err := c.Open(ctx, principal, page.Items[0].Address, selection)
	if err != nil {
		t.Fatal(err)
	}
	got, re := io.ReadAll(open.Body)
	if ce := open.Body.Close(); errors.Join(re, ce) != nil || string(got) != "ell" || open.Range == nil || open.Range.Total != 5 || contentRequests != verifiedRequests {
		t.Fatalf("ranged body %q range=%+v errors=%v %v", got, open.Range, re, ce)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := c.Open(canceled, principal, page.Items[0].Address, selection); !errors.Is(err, context.Canceled) || contentRequests != verifiedRequests {
		t.Fatalf("canceled download started body acquisition: %v", err)
	}
	wrongFile = true
	if _, err := c.Open(ctx, principal, page.Items[0].Address, selection); core.CodeOf(err) != core.InvalidUpstreamResponse || contentRequests != verifiedRequests {
		t.Fatalf("foreign grant was not refused before asking for its body: %v", err)
	}
	wrongFile = false
	receiving, cancelResponse := context.WithCancel(ctx)
	defer cancelResponse()
	cancelAtHeaders = cancelResponse
	if opened, err := d.Open(receiving, core.NativePath("/sample"), selection); !errors.Is(err, context.Canceled) ||
		opened.Body != nil || canceledBody == nil || !canceledBody.closed {
		t.Fatalf("canceled response body was not reclaimed: %v", err)
	}
	cyclic = true
	if _, err := c.Stat(ctx, principal, page.Items[0].Address); core.CodeOf(err) != core.InvalidUpstreamResponse {
		t.Fatalf("persistent cursor was not rejected: %v", err)
	}
	cyclic = false
	address := page.Items[0].Address
	if change, err := c.Remove(ctx, principal, address, core.RemoveOptions{Target: core.RemoveEmptyDirectory}); change.Effect != core.Unchanged || core.CodeOf(err) != core.UnsupportedOperation || recycleCalls != 0 {
		t.Fatalf("typed directory removal reached recycle: %+v %v", change, err)
	}
	if !website {
		return
	}
	wrongFile = true
	fileOnly := core.RemoveOptions{Target: core.RemoveNonDirectory}
	if change, err := c.Remove(ctx, principal, address, fileOnly); change.Effect != core.Unknown || core.CodeOf(err) != core.InvalidUpstreamResponse {
		t.Fatalf("recycle receipt claimed a different file: %+v %v", change, err)
	}
	wrongFile = false
	recycling, stopRecycle := context.WithCancel(ctx)
	cancelRecycle = stopRecycle
	if change, err := c.Remove(recycling, principal, address, fileOnly); change.Effect != core.Unknown || !errors.Is(err, context.Canceled) || taskCalls != 0 {
		t.Fatalf("canceled recycle did not preserve its uncertain effect: %+v %v", change, err)
	}
	cancelRecycle = nil
	taskStatus = "unexpected"
	if change, err := c.Remove(ctx, principal, address, fileOnly); change.Effect != core.Unknown || core.CodeOf(err) != core.InvalidUpstreamResponse {
		t.Fatalf("unrecognized recycle status was accepted: %+v %v", change, err)
	}
	taskStatus = "Succeed"
	taskReceiptID = "unrelated"
	if change, err := c.Remove(ctx, principal, address, fileOnly); change.Effect != core.Unknown || core.CodeOf(err) != core.InvalidUpstreamResponse {
		t.Fatalf("unbound recycle task was accepted: %+v %v", change, err)
	}
	taskReceiptID = "task-1"
	if change, err := c.Remove(ctx, principal, address, fileOnly); change.Effect != core.Applied || err != nil || recycleCalls != 5 || taskCalls != 3 {
		t.Fatalf("bound recycle did not complete: %+v %v", change, err)
	}
}
