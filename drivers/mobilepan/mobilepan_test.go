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

func testPersonalDriveRead(t *testing.T, website bool) {
	ctx := t.Context()
	const principal core.Principal = "fixture"
	var wrongFile, cyclic bool
	contentRequests := 0
	var cancelAtHeaders context.CancelFunc
	var canceledBody *observedBody
	api := &http.Client{Transport: roundTrip(func(r *http.Request) (*http.Response, error) {
		if r.Method != http.MethodPost || r.URL.Scheme != "https" || r.URL.Host != "openapi.yun.139.com" {
			t.Fatalf("unexpected control target %s", r.URL.Host)
		}
		var req struct {
			ID     string `json:"fileId"`
			Parent string `json:"parentFileId"`
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
		default:
			t.Fatalf("bad control operation: %s", r.URL.Path)
		}
		return w.Result(), nil
	})}
	content := &http.Client{Transport: roundTrip(func(r *http.Request) (*http.Response, error) {
		contentRequests++
		if r.Method != http.MethodGet || r.URL.Host != "body.fixture" || r.Header.Get("Range") != "bytes=1-3" ||
			r.Header.Get("Authorization") != "" || r.Header.Get("Cookie") != "" {
			t.Fatalf("bad download request: host=%s headers=%v", r.URL.Host, r.Header)
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
	selection := core.OpenOptions{Range: &core.Range{Start: 1, End: 3}}
	open, err := c.Open(ctx, principal, page.Items[0].Address, selection)
	if err != nil {
		t.Fatal(err)
	}
	got, re := io.ReadAll(open.Body)
	if ce := open.Body.Close(); errors.Join(re, ce) != nil || string(got) != "ell" || open.Range == nil || open.Range.Total != 5 {
		t.Fatalf("ranged body %q range=%+v errors=%v %v", got, open.Range, re, ce)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := c.Open(canceled, principal, page.Items[0].Address, selection); !errors.Is(err, context.Canceled) || contentRequests != 1 {
		t.Fatalf("canceled download started body acquisition: %v", err)
	}
	wrongFile = true
	if _, err := c.Open(ctx, principal, page.Items[0].Address, selection); core.CodeOf(err) != core.InvalidUpstreamResponse || contentRequests != 1 {
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
}
