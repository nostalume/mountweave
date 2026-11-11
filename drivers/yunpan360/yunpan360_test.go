package yunpan360_test

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
	"github.com/nostalume/mountweave/drivers/yunpan360"
)

type transport func(*http.Request) (*http.Response, error)

func (t transport) RoundTrip(r *http.Request) (*http.Response, error) { return t(r) }

type waitingBody struct{ context.Context }

func (b waitingBody) Read([]byte) (int, error) { <-b.Done(); return 0, b.Err() }
func (waitingBody) Close() error               { return nil }

func TestReadPersonalCloudDisk(t *testing.T) {
	const nid, name = "9223372036854775809", "report %.txt"
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()
	var mode string
	var grants, bodies int
	api := &http.Client{Transport: transport(func(r *http.Request) (*http.Response, error) {
		if r.Method != http.MethodPost || r.URL.String() != "https://mcp.yunpan.com/mcp" ||
			r.Header.Get("X-API-Key") != "fixture-private-key" || r.Header.Get("X-ECS-Env") != "prod" ||
			r.Header.Get("X-Sub-Channel") != "open" || r.Header.Get("Cookie") != "" ||
			r.Header.Get("Accept") != "application/json, text/event-stream" {
			return nil, errors.New("control authority used the wrong receiver or wire")
		}
		defer r.Body.Close()
		var req struct {
			JSONRPC, Method string
			ID              uint64
			Params          struct {
				Name      string
				Arguments struct {
					Path string
					Page int
					Size int `json:"page_size"`
					NID  string
				}
			}
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			return nil, err
		}
		if req.ID == 0 || req.JSONRPC != "2.0" || req.Method != "tools/call" {
			t.Error("wrong MCP call")
		}
		a := req.Params.Arguments
		var data any
		errno := 0
		switch req.Params.Name {
		case "file-list":
			if a.Size != 50 {
				t.Error("native list lost its page size")
			}
			switch {
			case a.Path == "/" && a.Page == 0:
				rows := make([]map[string]any, 0, 50)
				rows = append(rows, map[string]any{"nid": "folder-id", "name": "Docs", "is_dir": "1"})
				for i := 1; i < 50 && mode != "short"; i++ {
					rows = append(rows, map[string]any{"nid": fmt.Sprint(i), "name": fmt.Sprintf("file-%d", i), "is_dir": 0, "count_size": 1})
				}
				page := map[string]any{"list": rows, "total_count": "51"}
				if mode == "missing-total" {
					delete(page, "total_count")
				}
				if mode == "wrong-total" {
					page["total"] = 50
				}
				data = page
			case a.Path == "/" && a.Page == 1:
				last := map[string]any{"nid": "last-id", "fname": "last.txt", "type": "0", "size": 1}
				if mode == "ambiguous" {
					last["fname"] = "Docs"
				}
				data = map[string]any{"node_list": []any{last}, "total": 51}
			case a.Path == "/Docs/" && a.Page == 0:
				data = map[string]any{"data": []any{map[string]any{"nid": json.Number(nid), "fname": name, "type": "file",
					"size": "6", "mtime": 1723450000000}}, "total": 1}
			default:
				return nil, errors.New("unexpected directory or page")
			}
		case "get-download-url":
			grants++
			if a.NID != nid {
				t.Error("selected NID was changed")
			}
			grant := map[string]any{"downloadUrl": "https://body.example.test/start?cap=token%2b", "fname": "/Docs/" + name, "size": 6}
			switch mode {
			case "wrong-name":
				grant["fname"] = "elsewhere.txt"
			case "http":
				grant["downloadUrl"] = "http://body.example.test/secret"
			case "denied":
				errno = 3006
			}
			data = grant
		default:
			return nil, errors.New("unexpected MCP tool")
		}
		native := map[string]any{"data": data}
		if req.Params.Name == "get-download-url" && mode != "no-grant-status" {
			native["errno"], native["errmsg"] = errno, "fixture-private-key"
		}
		inner, _ := json.Marshal(native)
		outer, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": req.ID, "result": map[string]any{"content": []any{
			map[string]string{"type": "text", "text": string(inner)},
		}}})
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(string(outer)))}, nil
	})}
	content := &http.Client{Transport: transport(func(r *http.Request) (*http.Response, error) {
		bodies++
		if r.Method != http.MethodGet || r.URL.Scheme != "https" || r.Header.Get("X-API-Key") != "" ||
			r.Header.Get("Authorization") != "" || r.Header.Get("Cookie") != "" || r.Header.Get("Referer") != "" ||
			r.Header.Get("User-Agent") != "yunpan_mcp_server" || r.Header.Get("Accept-Encoding") != "identity" {
			t.Error("download mixed in control authority or lost native transport fields")
		}
		switch r.URL.Host + r.URL.Path {
		case "body.example.test/start":
			if r.URL.RawQuery != "cap=token%2b" {
				t.Error("signed URL was rewritten")
			}
			return &http.Response{StatusCode: http.StatusFound, Body: http.NoBody, Header: http.Header{"Location": {"https://mcp.yunpan.com/body"}}}, nil
		case "mcp.yunpan.com/body":
			if mode == "wait" {
				return &http.Response{StatusCode: http.StatusOK, ContentLength: 6, Body: waitingBody{r.Context()}}, nil
			}
			if r.Header.Get("Range") == "bytes=9-" {
				total := "6"
				if mode == "wrong-range-total" {
					total = "9"
				}
				return &http.Response{StatusCode: http.StatusRequestedRangeNotSatisfiable, Body: http.NoBody,
					Header: http.Header{"Content-Range": {"bytes */" + total}}}, nil
			}
			if r.Header.Get("Range") == "bytes=1-3" {
				return &http.Response{StatusCode: http.StatusPartialContent, ContentLength: 3, Body: io.NopCloser(strings.NewReader("bcd")),
					Header: http.Header{"Content-Range": {"bytes 1-3/6"}}}, nil
			}
			text := "abcdef"
			switch mode {
			case "short-body":
				text = "abcde"
			case "long-body":
				text = "abcdef!"
			}
			return &http.Response{StatusCode: http.StatusOK, ContentLength: -1, Body: io.NopCloser(strings.NewReader(text))}, nil
		}
		return nil, errors.New("unexpected download recipient")
	})}
	driver, err := yunpan360.Open(ctx, yunpan360.Options{APIKey: "fixture-private-key", Client: api, ContentClient: content})
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
	list := core.ListRequest{Query: core.Children(core.Path("/remote")), Limit: 50, CursorTTL: time.Minute}
	first, err := c.List(ctx, "u", list)
	if err != nil || len(first.Items) != 50 || first.NextCursor == nil || first.Items[0].Name != "Docs" {
		t.Fatal("initial positional page", err)
	}
	list.Cursor = *first.NextCursor
	last, err := c.List(ctx, "u", list)
	if err != nil || len(last.Items) != 1 || last.Items[0].Name != "last.txt" || last.NextCursor != nil {
		t.Fatal("last positional page", last, err)
	}
	target := core.Path("/remote/Docs/" + name)
	if f, err := c.Stat(ctx, "u", target); err != nil || f.Size == nil || *f.Size != 6 || f.ModifiedAt == nil {
		t.Fatal("metadata projection", f, err)
	}
	for scenario, code := range map[string]core.Code{"short": core.InvalidUpstreamResponse, "missing-total": core.InvalidUpstreamResponse,
		"wrong-total": core.InvalidUpstreamResponse, "ambiguous": core.PathAmbiguous} {
		mode = scenario
		if _, err := c.Open(ctx, "u", target, core.OpenOptions{}); core.CodeOf(err) != code || grants != 0 {
			t.Fatal("unconfirmed directory reached the body grant", scenario, err)
		}
	}
	for scenario, code := range map[string]core.Code{"wrong-name": core.InvalidUpstreamResponse, "http": core.InvalidUpstreamResponse,
		"denied": core.UpstreamAccessDenied, "no-grant-status": core.InvalidUpstreamResponse} {
		mode = scenario
		if _, err := c.Open(ctx, "u", target, core.OpenOptions{}); core.CodeOf(err) != code || bodies != 0 || strings.Contains(err.Error(), "fixture-private-key") {
			t.Fatal("bad grant reached the body or lost its private error", scenario, err)
		}
	}
	mode = ""
	opened, err := c.Open(ctx, "u", target, core.OpenOptions{Range: &core.Range{Start: 1, End: 3}})
	if err != nil {
		t.Fatal(err)
	}
	bytes, err := io.ReadAll(opened.Body)
	if err = errors.Join(err, opened.Body.Close()); err != nil || string(bytes) != "bcd" ||
		opened.Range == nil || *opened.Range != (core.ContentRange{Start: 1, End: 3, Total: 6}) {
		t.Fatal("actual range or byte stream", string(bytes), opened.Range, err)
	}
	unsatisfied := core.OpenOptions{Range: &core.Range{Start: 9, End: -1}}
	_, err = c.Open(ctx, "u", target, unsatisfied)
	var rejected *core.Error
	if !errors.As(err, &rejected) || rejected.Code != core.RangeNotSatisfiable || rejected.Total == nil || *rejected.Total != 6 {
		t.Fatal("consistent range rejection lost the file size", err)
	}
	mode = "wrong-range-total"
	if _, err := c.Open(ctx, "u", target, unsatisfied); core.CodeOf(err) != core.InvalidUpstreamResponse {
		t.Fatal("range rejection exposed a size that contradicted the grant", err)
	}
	for _, scenario := range []string{"short-body", "long-body"} {
		mode = scenario
		opened, err := c.Open(ctx, "u", target, core.OpenOptions{})
		if err != nil || opened.Length != nil {
			t.Fatal("unframed response lost streaming length semantics", err)
		}
		data, err := io.ReadAll(opened.Body)
		_ = opened.Body.Close()
		if scenario == "short-body" && (!errors.Is(err, io.ErrUnexpectedEOF) || string(data) != "abcde") ||
			scenario == "long-body" && (core.CodeOf(err) != core.InvalidUpstreamResponse || string(data) != "abcdef") {
			t.Fatal("unframed body contradicted its grant", scenario, string(data), err)
		}
	}
	mode = ""
	opened, err = c.Open(ctx, "u", target, core.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	bytes, err = io.ReadAll(opened.Body)
	if err = errors.Join(err, opened.Body.Close()); err != nil || string(bytes) != "abcdef" || opened.Range != nil || opened.Length != nil {
		t.Fatalf("full original: %q, range=%v, error=%v", bytes, opened.Range, err)
	}
	mode = "wait"
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
		t.Fatal("retirement did not cancel the body", err)
	}
}
