package weiyun_test

import (
	"context"
	"encoding/json"
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
	"github.com/nostalume/mountweave/drivers/weiyun"
)

type transport func(*http.Request) (*http.Response, error)

func (f transport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type body struct {
	read  func([]byte) (int, error)
	close func() error
}

func (b body) Read(p []byte) (int, error) { return b.read(p) }
func (b body) Close() error               { return b.close() }

func TestMountTokenBoundDirectory(t *testing.T) {
	const rootKey, docsKey, fileID, name = "abab", "CDCD", "file-01", "report %.txt"
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var mode string
	var grants, bodyRequests, failedCloses, rootPages int
	readStarted := make(chan struct{})
	api := &http.Client{Transport: transport(func(r *http.Request) (*http.Response, error) {
		if r.Method != http.MethodPost || r.URL.String() != "https://www.weiyun.com/api/v3/mcpserver" ||
			r.Header.Get("WyHeader") != "mcp_token=fixture-private" || r.Header.Get("Cookie") != "" {
			t.Error("control operation used the wrong endpoint or credentials")
		}
		defer r.Body.Close()
		var req struct {
			JSONRPC, Method string
			ID              uint64
			Params          struct {
				Name      string
				Arguments struct {
					Dir           string `json:"dir_key"`
					Parent        string `json:"pdir_key"`
					Limit, Offset int
					GetType       int                           `json:"get_type"`
					Header        struct{ Qua, Version string } `json:"req_header"`
					Items         []struct {
						ID     string `json:"file_id"`
						Parent string `json:"pdir_key"`
					}
				}
			}
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			return nil, err
		}
		a := req.Params.Arguments
		if req.JSONRPC != "2.0" || req.Method != "tools/call" || req.ID == 0 || a.Header.Qua == "" || a.Header.Version != "1.0.6" {
			t.Error("MCP request omitted its protocol or caller identity")
		}
		file := func(id, name string) map[string]any {
			return map[string]any{"file_id": id, "filename": name, "file_size": 6, "pdir_key": ""}
		}
		var data any
		switch req.Params.Name {
		case "weiyun.list":
			if a.Limit != 50 || a.GetType != 0 {
				t.Error("wrong native page limit or kind")
			}
			switch {
			case a.Dir == "" && a.Parent == "" && a.Offset == 0:
				rootPages++
				rows := make([]map[string]any, 0, 49)
				for i := 0; i < 49 && mode != "short"; i++ {
					rows = append(rows, file(fmt.Sprint(i), fmt.Sprintf("sibling-%02d", i)))
				}
				page := map[string]any{"pdir_key": rootKey, "finish_flag": false, "file_list": rows,
					"dir_list": []any{map[string]any{"dir_key": docsKey, "dir_name": "Docs"}}}
				if mode == "missing-finish" {
					delete(page, "finish_flag")
				}
				data = page
			case a.Dir == "" && a.Parent == "" && a.Offset == 50:
				last := file("last", "last.txt")
				if mode == "ambiguous" {
					last["file_id"], last["filename"] = docsKey, "Docs"
				}
				data = map[string]any{"pdir_key": rootKey, "finish_flag": true, "file_list": []any{last}}
			case a.Dir == docsKey && a.Parent == rootKey && a.Offset == 0:
				key := strings.ToLower(docsKey)
				if mode == "wrong-parent" {
					key = "dddd"
				}
				data = map[string]any{"pdir_key": key, "finish_flag": true, "file_list": []any{file(fileID, name)}}
			default:
				return nil, fmt.Errorf("unexpected fixture list coordinates")
			}
		case "weiyun.download":
			grants++
			if len(a.Items) != 1 || a.Items[0].ID != fileID || a.Items[0].Parent != strings.ToLower(docsKey) {
				t.Error("download did not bind the selected file to the returned directory key")
			}
			id := fileID
			if mode == "wrong-id" {
				id = "wrong-file"
			}
			data = map[string]any{"items": []any{map[string]any{"file_id": id, "https_download_url": "https://download.weiyun.com/start?sig=cap%2b",
				"file_size": 6, "cookie": "FTN5K=fixture-cookie"}}}
			if mode == "denied" {
				data = map[string]any{"retcode": 117405, "error": "fixture-private"}
			}
		default:
			return nil, fmt.Errorf("unexpected fixture tool")
		}
		inner, _ := json.Marshal(data)
		outer, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": req.ID, "result": map[string]any{
			"content": []any{map[string]any{"type": "text", "text": string(inner)}}}})
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(string(outer)))}, nil
	})}
	content := &http.Client{Transport: transport(func(r *http.Request) (*http.Response, error) {
		bodyRequests++
		if r.Method != http.MethodGet || r.URL.Scheme != "https" || r.Header.Get("WyHeader") != "" ||
			r.Header.Get("Authorization") != "" || r.Header.Get("Referer") != "" || r.Header.Get("Accept-Encoding") != "identity" {
			t.Error("a download used control authority or an invalid request")
		}
		wantCookie, next := "", ""
		switch r.URL.Host + r.URL.Path {
		case "download.weiyun.com/start":
			wantCookie, next = "FTN5K=fixture-cookie", "https://edge.weiyun.com/edge"
			if r.URL.RawQuery != "sig=cap%2b" {
				t.Error("the grant URL was rewritten")
			}
		case "edge.weiyun.com/edge":
			wantCookie, next = "FTN5K=fixture-cookie", "https://cdn.example.test/cdn"
		case "cdn.example.test/cdn":
			next = "https://edge.weiyun.com/end"
		case "edge.weiyun.com/end":
		default:
			return nil, fmt.Errorf("unexpected fixture download URL")
		}
		if r.Header.Get("Cookie") != wantCookie {
			t.Error("wrong temporary cookie handoff")
		}
		if next != "" {
			return &http.Response{StatusCode: 302, Header: http.Header{"Location": {next}}, Body: http.NoBody}, nil
		}
		switch mode {
		case "wait":
			waiting := body{read: func([]byte) (int, error) {
				close(readStarted)
				<-r.Context().Done()
				return 0, r.Context().Err()
			}, close: func() error { return nil }}
			return &http.Response{StatusCode: 200, ContentLength: -1, Body: waiting}, nil
		case "bad-body":
			bad := body{read: strings.NewReader("wrong").Read, close: func() error {
				failedCloses++
				if failedCloses == 1 {
					return errors.New("signed URL and fixture-cookie")
				}
				return nil
			}}
			return &http.Response{StatusCode: 200, ContentLength: 5, Body: bad}, nil
		}
		if r.Header.Get("Range") == "bytes=1-3" {
			return &http.Response{StatusCode: 206, ContentLength: 3, Header: http.Header{"Content-Range": {"bytes 1-3/6"}},
				Body: io.NopCloser(strings.NewReader("bcd"))}, nil
		}
		data := "abcdef"
		switch mode {
		case "short-body":
			data = "abc"
		case "long-body":
			data = "abcdef!"
		}
		return &http.Response{StatusCode: 200, ContentLength: -1, Body: io.NopCloser(strings.NewReader(data))}, nil
	})}
	d, err := weiyun.Open(ctx, weiyun.Options{Token: "fixture-private", Client: api, ContentClient: content})
	if err != nil {
		t.Fatal(err)
	}
	c, err := core.New(core.Options{MaxCursors: 2, Authorize: func(context.Context, core.Principal, core.Action, core.Address) error { return nil }})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close(ctx)
	mount, err := c.Install("/remote", d)
	if err != nil {
		t.Fatal(err)
	}
	list := core.ListRequest{Query: core.Children(core.Path("/remote")), Limit: 50, CursorTTL: time.Minute}
	first, err := c.List(ctx, "u", list)
	if err != nil || len(first.Items) != 50 || first.NextCursor == nil || first.Items[0].Name != "Docs" {
		t.Fatal("first mixed page", err)
	}
	list.Cursor = *first.NextCursor
	last, err := c.List(ctx, "u", list)
	if err != nil || len(last.Items) != 1 || last.Items[0].Name != "last.txt" || last.NextCursor != nil || rootPages != 2 {
		t.Fatal("final page", last, err)
	}
	target := core.Path("/remote/Docs/" + name)
	for state, code := range map[string]core.Code{
		"short": core.UpstreamFailure, "ambiguous": core.PathAmbiguous, "wrong-parent": core.InvalidUpstreamResponse,
		"missing-finish": core.InvalidUpstreamResponse,
	} {
		mode = state
		if _, err := c.Open(ctx, "u", target, core.OpenOptions{}); core.CodeOf(err) != code || grants != 0 {
			t.Fatal("unconfirmed selection reached a grant", state, err)
		}
	}
	mode = "wrong-id"
	if _, err := c.Open(ctx, "u", target, core.OpenOptions{}); core.CodeOf(err) != core.InvalidUpstreamResponse || bodyRequests != 0 {
		t.Fatal("unconfirmed grant reached the body", err)
	}
	mode = "denied"
	if _, err := c.Open(ctx, "u", target, core.OpenOptions{}); core.CodeOf(err) != core.UpstreamAccessDenied || strings.Contains(err.Error(), "fixture-private") {
		t.Fatal("native error classification or privacy", err)
	}
	mode = "bad-body"
	if _, err := c.Open(ctx, "u", target, core.OpenOptions{}); core.CodeOf(err) != core.InvalidUpstreamResponse || strings.Contains(err.Error(), "fixture-cookie") {
		t.Fatal("body-size contradiction or close error privacy", err)
	}
	mode = ""
	opened, err := c.Open(ctx, "u", target, core.OpenOptions{Range: &core.Range{Start: 1, End: 3}})
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(opened.Body)
	if err = errors.Join(err, opened.Body.Close()); err != nil || string(data) != "bcd" ||
		opened.Range == nil || *opened.Range != (core.ContentRange{Start: 1, End: 3, Total: 6}) {
		t.Fatal("actual content range", string(data), opened.Range, err)
	}
	opened, err = c.Open(ctx, "u", target, core.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	data, err = io.ReadAll(opened.Body)
	if err = errors.Join(err, opened.Body.Close()); err != nil || string(data) != "abcdef" || opened.Length != nil || opened.Entry.Size != nil {
		t.Fatal("unframed body or size changed", string(data), opened.Length, opened.Entry.Size, err)
	}
	root := t.TempDir()
	localDriver, err := local.New(root)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Install("/local", localDriver); err != nil {
		t.Fatal(err)
	}
	for _, broken := range []struct{ mode, written string }{{"short-body", "abc"}, {"long-body", "abcdef"}} {
		mode = broken.mode
		result, err := c.Copy(ctx, "u", target, core.Path("/local/"+mode))
		if result.Effect != core.Incomplete ||
			(mode == "short-body" && !errors.Is(err, io.ErrUnexpectedEOF)) ||
			(mode == "long-body" && core.CodeOf(err) != core.InvalidUpstreamResponse) {
			t.Fatal("Copy accepted a body contradicting its receipt", mode, result, err)
		}
		if copied, err := os.ReadFile(filepath.Join(root, mode)); err != nil || string(copied) != broken.written {
			t.Fatal("Copy wrote bytes beyond the receipt", mode, string(copied), err)
		}
	}
	mode = "wait"
	opened, err = c.Open(ctx, "u", target, core.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := io.ReadAll(opened.Body)
		done <- err
	}()
	select {
	case <-readStarted:
	case <-ctx.Done():
		t.Fatal("unframed download did not start")
	}
	if err := mount.Retire(ctx); err != nil {
		t.Fatal(err)
	}
	if !errors.Is(<-done, context.Canceled) || failedCloses != 2 {
		t.Fatal("retirement did not cancel the body and retry the failed close")
	}
}
