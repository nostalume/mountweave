package febbox

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	core "github.com/nostalume/mountweave"
)

type fixtureTransport func(*http.Request) (*http.Response, error)

func (f fixtureTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestPrivateRootAndOriginal(t *testing.T) {
	const original = "abcdef"
	var bodyCalls atomic.Int32
	bodyStarted, bodyCanceled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	content := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		bodyCalls.Add(1)
		if r.Method != http.MethodGet || r.URL.Query().Get("ticket") != "signed-fixture" ||
			r.Header.Get("Authorization") != "" || r.Header.Get("Cookie") != "" {
			t.Error("invalid or credential-bearing content request")
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if r.URL.Path == "/stalled" {
			w.Header().Set("Content-Length", "6")
			w.WriteHeader(http.StatusOK)
			if err := http.NewResponseController(w).Flush(); err != nil {
				t.Error(err)
				return
			}
			close(bodyStarted)
			select {
			case <-r.Context().Done():
				close(bodyCanceled)
			case <-release:
			}
			return
		}
		if r.URL.Path == "/unavailable" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		switch r.Header.Get("Range") {
		case "":
			_, _ = io.WriteString(w, original)
		case "bytes=2-4":
			w.Header().Set("Content-Range", "bytes 2-4/6")
			w.Header().Set("Content-Length", "3")
			w.WriteHeader(http.StatusPartialContent)
			_, _ = io.WriteString(w, original[2:5])
		default:
			t.Error("unexpected content range request")
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	defer content.Close()
	defer close(release)

	var receiptMode atomic.Int32
	var mounted atomic.Bool
	control := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/oauth" || r.Header.Get("Authorization") != "Bearer fixture-account" ||
			r.ParseMultipartForm(1<<16) != nil {
			t.Error("invalid authenticated multipart request")
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		switch r.FormValue("module") {
		case "file_info":
			switch r.FormValue("fid") {
			case "10":
				_, _ = io.WriteString(w, `{"code":1,"data":{"file":{"fid":10,"parent_id":0,"file_name":"archive","is_dir":1,"file_size":0,"is_share":0}}}`)
			case "90":
				_, _ = io.WriteString(w, `{"code":1,"data":{"file":{"fid":90,"parent_id":0,"file_name":"Subscribed","is_dir":1,"from_uid":41}}}`)
			default:
				t.Error("unexpected file detail request")
			}
		case "file_list":
			size, page := r.FormValue("pagelimit"), r.FormValue("page")
			if r.FormValue("parent_id") != "0" || r.FormValue("order") == "" ||
				size != "100" && (mounted.Load() || size != "1" || page != "1") {
				t.Errorf("wrong root, order or native page size on page %s", page)
			}
			switch page {
			case "1":
				_, _ = fmt.Fprintf(w, `{"code":1,"data":{"page":1,"pagelimit":%s,"has_more":1,"file_list":[{"fid":90,"parent_id":0,"file_name":"Subscribed","is_dir":1,"file_size":0,"is_share":1,"from_uid":41,"share_fid":42}]}}`, size)
			case "2":
				_, _ = fmt.Fprintf(w, `{"code":1,"data":{"page":2,"pagelimit":%s,"has_more":0,"file_list":[{"fid":21,"parent_id":0,"file_name":"data.bin","is_dir":0,"file_size":6,"is_share":0},{"fid":22,"parent_id":0,"file_name":"notes.txt","is_dir":0,"file_size":1,"is_share":0}]}}`, size)
			default:
				t.Error("unexpected native page")
				w.WriteHeader(http.StatusBadRequest)
			}
		case "file_get_download_url":
			if ids := r.MultipartForm.Value["fids[]"]; len(ids) != 1 || ids[0] != "21" {
				t.Error("signed the wrong file")
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			fid, parent, route := 21, 0, "/original"
			switch receiptMode.Load() {
			case 1:
				fid = 22
			case 2:
				parent = 10
			case 3:
				route = "/stalled"
			case 4:
				route = "/unavailable"
			}
			_, _ = fmt.Fprintf(w, `{"code":1,"data":[{"fid":%d,"parent_id":%d,"file_name":"data.bin","file_size":6,"download_url":%q,"accept_ranges":1,"expires_in":14400}]}`, fid, parent, content.URL+route+"?ticket=signed-fixture")
		default:
			t.Error("unexpected native operation")
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	defer control.Close()

	target, err := url.Parse(control.URL)
	if err != nil {
		t.Fatal(err)
	}
	client := &http.Client{Transport: fixtureTransport(func(r *http.Request) (*http.Response, error) {
		if r.URL.Scheme != "https" || r.URL.Host != "api.febbox.com" {
			return nil, errors.New("request left the control origin")
		}
		req := r.Clone(r.Context())
		req.URL.Scheme, req.URL.Host, req.Host = target.Scheme, target.Host, target.Host
		req.Header.Set("Authorization", "Bearer fixture-account")
		return control.Client().Transport.RoundTrip(req)
	})}
	ctx, stop := context.WithTimeout(t.Context(), 10*time.Second)
	defer stop()
	opts := Options{RootID: "10", Client: client, ContentClient: content.Client()}
	root, err := Open(ctx, opts)
	if err != nil {
		t.Fatal(err)
	}
	if err := root.Close(); err != nil {
		t.Fatal(err)
	}
	opts.RootID = "90"
	if candidate, err := Open(ctx, opts); candidate != nil || core.CodeOf(err) != core.UnsupportedOperation {
		t.Fatal("opened a subscribed root", err)
	}
	opts.RootID = "0"
	driver, err := Open(ctx, opts)
	if err != nil {
		t.Fatal(err)
	}
	mounted.Store(true)
	runtime, err := core.New(core.Options{Authorize: func(context.Context, core.Principal, core.Action, core.Address) error { return nil }})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.Install("/feb", driver); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := runtime.Close(ctx); err != nil {
			t.Error(err)
		}
	}()

	query := core.ListRequest{Query: core.Children(core.Path("/feb")), CursorTTL: time.Minute, Limit: 1}
	var names []string
	for range 4 {
		page, err := runtime.List(ctx, "u", query)
		if err != nil {
			t.Fatal(err)
		}
		for _, item := range page.Items {
			names = append(names, item.Name)
		}
		if page.NextCursor == nil {
			break
		}
		query.Cursor = *page.NextCursor
	}
	if len(names) != 2 || names[0] != "data.bin" || names[1] != "notes.txt" {
		t.Fatalf("listing lost continuation or leaked subscribed files: %v", names)
	}
	path := core.Path("/feb/data.bin")
	entry, err := runtime.Stat(ctx, "u", path)
	if err != nil || entry.Size == nil || *entry.Size != 6 {
		t.Fatal(entry, err)
	}
	for _, want := range []*core.Range{nil, {Start: 2, End: 4}} {
		opened, err := runtime.Open(ctx, "u", path, core.OpenOptions{Range: want})
		if err != nil {
			t.Fatal(err)
		}
		data, readErr := io.ReadAll(opened.Body)
		if err := errors.Join(readErr, opened.Body.Close()); err != nil {
			t.Fatal(err)
		}
		if want == nil && (string(data) != original || opened.Range != nil) ||
			want != nil && (string(data) != original[2:5] || opened.Range == nil ||
				*opened.Range != (core.ContentRange{Start: 2, End: 4, Total: 6})) {
			t.Fatal(string(data), opened.Range)
		}
	}
	for _, wrong := range []int32{1, 2} {
		receiptMode.Store(wrong)
		if _, err := runtime.Open(ctx, "u", path, core.OpenOptions{}); core.CodeOf(err) != core.InvalidUpstreamResponse || bodyCalls.Load() != 2 {
			t.Fatalf("wrong receipt reached the content server: mode %d, err %v", wrong, err)
		}
	}

	receiptMode.Store(4)
	if _, err := runtime.Open(ctx, "u", path, core.OpenOptions{}); core.CodeOf(err) != core.UpstreamFailure {
		t.Fatal("unavailable download grant did not preserve upstream failure", err)
	}
	receiptMode.Store(3)
	call, cancel := context.WithCancel(ctx)
	defer cancel()
	opened, err := runtime.Open(call, "u", path, core.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := io.Copy(io.Discard, opened.Body)
		done <- errors.Join(err, opened.Body.Close())
	}()
	select {
	case <-bodyStarted:
	case <-ctx.Done():
		t.Fatal("content request did not reach the server")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("the active body did not close after cancellation")
	}
	select {
	case <-bodyCanceled:
	case <-ctx.Done():
		t.Fatal("content request did not cancel")
	}
}
