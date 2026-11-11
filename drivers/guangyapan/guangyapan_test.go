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
