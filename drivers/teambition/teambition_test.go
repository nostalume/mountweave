package teambition_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/teambition"
)

type transport func(*http.Request) (*http.Response, error)

func (f transport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type waiting struct{ context.Context }

func (w waiting) Read([]byte) (int, error) { <-w.Done(); return 0, w.Err() }
func (waiting) Close() error               { return nil }

func TestProjectFolderAndSignedOriginal(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	const rootID, directoryID, fileID, name = "collection", "reports", "work", "Report %+.pdf"
	const secret = "grant%2b"
	var mode string
	var grants, downloads int
	reply := func(code int, result any) *http.Response {
		data, _ := json.Marshal(map[string]any{"code": code, "result": result, "errorMessage": secret})
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(string(data)))}
	}
	api := &http.Client{Transport: transport(func(r *http.Request) (*http.Response, error) {
		if r.Method != "GET" || r.URL.Scheme != "https" || r.URL.Host != "open.teambition.com" ||
			r.Header.Get("X-Tenant-Id") != "organization-id" || r.Header.Get("X-Tenant-Type") != "organization" ||
			r.Header.Get("X-Operator-Id") != "operator-id" || r.Header.Get("Accept-Encoding") != "identity" {
			return nil, errors.New("invalid Teambition control request")
		}
		q := r.URL.Query()
		switch r.URL.Path {
		case "/api/v3/collection/query":
			if q.Get("collectionIds") != rootID {
				return nil, errors.New("wrong root collection")
			}
			return reply(200, []any{map[string]any{"id": rootID}}), nil
		case "/api/v3/work/list":
			if q.Get("projectId") != "project-id" || q.Get("includeArchived") != "false" || q.Get("pageSize") != "50" {
				return nil, errors.New("wrong Teambition project or list bounds")
			}
			switch q.Get("parentId") {
			case rootID:
				switch q.Get("pageToken") {
				case "":
					return reply(200, map[string]any{"nextPageToken": "opaque/+?"}), nil
				case "opaque/+?":
					folder := map[string]any{"id": directoryID, "parentId": rootID, "title": "Reports"}
					return reply(200, map[string]any{"collections": []any{folder}}), nil
				}
			case directoryID:
				file := map[string]any{"id": fileID, "parentId": directoryID, "fileName": name, "fileSize": 6}
				switch mode {
				case "wrong-parent":
					file["parentId"] = "unrelated"
				case "ambiguous":
					dir := map[string]any{"id": fileID, "parentId": directoryID, "title": name}
					return reply(200, map[string]any{"collections": []any{dir}, "works": []any{file}}), nil
				}
				return reply(200, map[string]any{"works": []any{file}}), nil
			}
		case "/api/v3/work/query":
			grants++
			if q.Get("workIds") != fileID || q.Get("needSign") != "true" {
				return nil, errors.New("incorrect signed grant request")
			}
			if mode == "denied" {
				return reply(403, struct{}{}), nil
			}
			grant := map[string]any{"id": fileID, "parentId": directoryID, "fileName": name, "fileSize": 6,
				"downloadUrl": "https://grant.example.test/original?sig=" + secret}
			switch mode {
			case "moved":
				grant["parentId"] = "outside"
			case "insecure":
				grant["downloadUrl"] = "http://grant.example.test/original?sig=" + secret
			}
			return reply(200, []any{grant}), nil
		}
		return nil, errors.New("unexpected Teambition control route")
	})}
	content := &http.Client{Transport: transport(func(r *http.Request) (*http.Response, error) {
		downloads++
		if r.Method != "GET" || r.URL.Scheme != "https" || r.Header.Get("Accept-Encoding") != "identity" ||
			r.Header.Get("Authorization") != "" || r.Header.Get("X-Tenant-Id") != "" || r.Header.Get("X-Operator-Id") != "" ||
			r.Header.Get("Cookie") != "" || r.Header.Get("Referer") != "" {
			t.Error("download carried control credentials")
		}
		switch r.URL.Host + r.URL.Path {
		case "grant.example.test/original":
			if r.URL.RawQuery != "sig="+secret {
				t.Error("issued grant query was changed")
			}
			return &http.Response{StatusCode: http.StatusFound, Body: http.NoBody,
				Header: http.Header{"Location": {"https://open.teambition.com/body"}}}, nil
		case "open.teambition.com/body":
			if mode == "wait" {
				return &http.Response{StatusCode: http.StatusOK, ContentLength: 6, Body: waiting{r.Context()}}, nil
			}
			if r.Header.Get("Range") == "bytes=1-3" {
				return &http.Response{StatusCode: http.StatusPartialContent, ContentLength: 3, Body: io.NopCloser(strings.NewReader("bcd")),
					Header: http.Header{"Content-Range": {"bytes 1-3/6"}}}, nil
			}
			return &http.Response{StatusCode: http.StatusOK, ContentLength: -1, Body: io.NopCloser(strings.NewReader("abcdef"))}, nil
		}
		return nil, errors.New("unexpected download receiver")
	})}
	d, err := teambition.Open(ctx, teambition.Options{OrganizationID: "organization-id", OperatorID: "operator-id",
		ProjectID: "project-id", RootID: rootID, Client: api, ContentClient: content})
	if err != nil {
		t.Fatal(err)
	}
	c, err := core.New(core.Options{MaxCursors: 2, Authorize: func(context.Context, core.Principal, core.Action, core.Address) error { return nil }})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close(ctx)
	mount, err := c.Install("/project", d)
	if err != nil {
		t.Fatal(err)
	}
	list := core.ListRequest{Query: core.Children(core.Path("/project")), Limit: 10, CursorTTL: time.Minute}
	page, err := c.List(ctx, "u", list)
	if err != nil || len(page.Items) != 0 || page.NextCursor == nil {
		t.Fatal("empty native page lost its continuation", page, err)
	}
	list.Cursor = *page.NextCursor
	page, err = c.List(ctx, "u", list)
	if err != nil || len(page.Items) != 1 || page.Items[0].Name != "Reports" || page.NextCursor != nil {
		t.Fatal("project listing did not reach its terminal page", page, err)
	}
	path := core.Path("/project/Reports/" + name)
	if f, err := c.Stat(ctx, "u", path); err != nil || f.Size == nil || *f.Size != 6 {
		t.Fatal("file metadata", f, err)
	}
	mode = "wrong-parent"
	if _, err := c.Open(ctx, "u", path, core.OpenOptions{}); core.CodeOf(err) != core.InvalidUpstreamResponse || grants != 0 {
		t.Fatal("out-of-scope entry reached grant request", err)
	}
	mode = "ambiguous"
	if _, err := c.Open(ctx, "u", path, core.OpenOptions{}); core.CodeOf(err) != core.PathAmbiguous || grants != 0 {
		t.Fatal("same-named folder and work were conflated", err)
	}
	for _, scenario := range []struct {
		mode string
		code core.Code
	}{{"moved", core.InvalidUpstreamResponse}, {"insecure", core.InvalidUpstreamResponse}, {"denied", core.UpstreamAccessDenied}} {
		mode = scenario.mode
		_, err := c.Open(ctx, "u", path, core.OpenOptions{})
		if core.CodeOf(err) != scenario.code || strings.Contains(err.Error(), secret) || downloads != 0 {
			t.Fatal("unconfirmed or unsafe grant reached body or escaped into diagnostics", scenario.mode, err)
		}
	}
	mode = ""
	opened, err := c.Open(ctx, "u", path, core.OpenOptions{Range: &core.Range{Start: 1, End: 3}})
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(opened.Body)
	if err = errors.Join(err, opened.Body.Close()); err != nil || string(data) != "bcd" ||
		opened.Range == nil || *opened.Range != (core.ContentRange{Start: 1, End: 3, Total: 6}) {
		t.Fatal("native body range", string(data), opened.Range, err)
	}
	opened, err = c.Open(ctx, "u", path, core.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	data, err = io.ReadAll(opened.Body)
	if err = errors.Join(err, opened.Body.Close()); err != nil || string(data) != "abcdef" || opened.Range != nil || opened.Length != nil {
		t.Fatal("complete original", string(data), err)
	}
	mode = "wait"
	opened, err = c.Open(ctx, "u", path, core.OpenOptions{})
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
