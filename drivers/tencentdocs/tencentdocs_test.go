package tencentdocs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"testing/iotest"
	"time"

	core "github.com/nostalume/mountweave"
)

type fixtureTransport func(*http.Request) (*http.Response, error)

func (f fixtureTransport) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

func fixtureResponse(req *http.Request, status int, body string) *http.Response {
	return &http.Response{StatusCode: status, Header: make(http.Header), ContentLength: int64(len(body)),
		Body: io.NopCloser(strings.NewReader(body)), Request: req}
}

func TestFolderExportThroughCore(t *testing.T) {
	const prefix = "/openapi/drive/v2"
	const document = "300000000$DOC"
	const title = "Budget +% 中"
	const payload = "export bytes"
	pending := make(chan struct{}, 1)
	var pause, truncate atomic.Bool
	var submissions, downloads atomic.Int32
	control := &http.Client{Transport: fixtureTransport(func(req *http.Request) (*http.Response, error) {
		if req.URL.Scheme != "https" || req.URL.Host != "docs.qq.com" || req.Header.Get("Accept-Encoding") != "identity" {
			return nil, errors.New("unexpected control request")
		}
		var reply string
		switch req.Method + " " + req.URL.Path {
		case "GET " + prefix + "/folders/root/metadata":
			reply = `{"folderInfo":{"ID":"root","status":"normal"}}`
		case "GET " + prefix + "/folders/root":
			if req.URL.Query().Get("limit") != "20" {
				return nil, errors.New("unexpected page limit")
			}
			switch req.URL.Query().Get("start") {
			case "0":
				reply = `{"next":20,"list":[{"ID":"sub","title":"Reports","type":"folder","status":"NORMAL"}]}`
			case "20":
				reply = `{"next":0,"list":[{"ID":"root","title":"Link","type":"folder","shortcutID":"shortcut","status":"normal"}]}`
			default:
				return nil, errors.New("invalid native offset")
			}
		case "GET " + prefix + "/folders/sub":
			if truncate.Load() {
				resp := fixtureResponse(req, http.StatusOK, "")
				resp.Body = io.NopCloser(iotest.ErrReader(io.ErrUnexpectedEOF))
				return resp, nil
			}
			switch req.URL.Query().Get("start") {
			case "0":
				reply = `{"next":20,"list":[{"ID":"` + document + `","title":"` + title + `","type":"doc","status":"normal"}]}`
			case "20":
				reply = `{"next":0}`
			default:
				return nil, errors.New("invalid native offset")
			}
		case "POST " + prefix + "/files/" + document + "/async-export":
			if req.GetBody != nil || req.Header.Get("Content-Type") != "application/x-www-form-urlencoded" {
				return nil, errors.New("unexpected export request")
			}
			submissions.Add(1)
			reply = `{"operationID":"op+/1"}`
		case "GET " + prefix + "/files/" + document + "/export-progress":
			if req.URL.Query().Get("operationID") != "op+/1" {
				return nil, errors.New("unexpected export operation")
			}
			if pause.Load() {
				select {
				case pending <- struct{}{}:
				default:
				}
				reply = `{"progress":42}`
			} else {
				reply = `{"progress":100,"url":"https://docs.qq.com/signed?proof=only"}`
			}
		default:
			return nil, errors.New("unexpected control route")
		}
		return fixtureResponse(req, http.StatusOK, fmt.Sprintf(`{"ret":0,"data":%s}`, reply)), nil
	})}
	content := &http.Client{Transport: fixtureTransport(func(req *http.Request) (*http.Response, error) {
		for _, key := range []string{"Access-Token", "Client-Id", "Open-Id", "Authorization", "Cookie", "Range"} {
			if req.Header.Get(key) != "" {
				t.Errorf("unexpected export download header %s", key)
			}
		}
		switch req.URL.String() {
		case "https://docs.qq.com/signed?proof=only":
			resp := fixtureResponse(req, http.StatusFound, "")
			resp.Header.Set("Location", "https://body.example.test/file?auth=private")
			return resp, nil
		case "https://body.example.test/file?auth=private":
			downloads.Add(1)
			return fixtureResponse(req, http.StatusOK, payload), nil
		default:
			return nil, errors.New("unexpected export address")
		}
	})}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := core.New(core.Options{Authorize: func(context.Context, core.Principal, core.Action, core.Address) error {
		return nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := c.Close(context.Background()); err != nil {
			t.Error(err)
		}
	}()
	options := Options{FolderID: "root", Client: control, ContentClient: content}
	for _, enabled := range []bool{false, true} {
		options.AllowExport = enabled
		d, err := Open(ctx, options)
		if err != nil {
			t.Fatal(err)
		}
		path := "/metadata"
		if enabled {
			path = "/docs"
		}
		if _, err := c.Install(path, d); err != nil {
			t.Fatal(err)
		}
	}
	req := core.ListRequest{Query: core.Children(core.Path("/docs")), Limit: 1, CursorTTL: time.Minute}
	first, err := c.List(ctx, "reader", req)
	if err != nil || len(first.Items) != 1 || first.Items[0].Kind != core.Directory || first.NextCursor == nil {
		t.Fatalf("first page: %v %v", first, err)
	}
	req.Cursor = *first.NextCursor
	last, err := c.List(ctx, "reader", req)
	if err != nil || len(last.Items) != 1 || last.Items[0].Name != "Link" || last.NextCursor != nil {
		t.Fatalf("last page: %v %v", last, err)
	}
	for _, name := range []string{"/docs/Link", "/metadata/Reports/" + title} {
		if _, err := c.Open(ctx, "reader", core.Path(name), core.OpenOptions{}); core.CodeOf(err) != core.UnsupportedOperation {
			t.Fatalf("export should be refused for %q: %v", name, err)
		}
	}
	source := core.Path("/docs/Reports/" + title)
	truncate.Store(true)
	if _, err := c.Stat(ctx, "reader", source); core.CodeOf(err) != core.UpstreamFailure || !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("truncated control response lost its read failure: %v", err)
	}
	truncate.Store(false)
	entry, err := c.Stat(ctx, "reader", source)
	if err != nil || entry.Size != nil || submissions.Load() != 0 {
		t.Fatalf("metadata dispatched an export or invented size: %v %v", entry, err)
	}
	opened, err := c.Open(ctx, "reader", source, core.OpenOptions{Range: &core.Range{Start: 1, End: 2}})
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(opened.Body)
	if err = errors.Join(err, opened.Body.Close()); err != nil || string(body) != payload || opened.Range != nil {
		t.Fatalf("full export: %q %v %v", body, opened.Range, err)
	}
	pause.Store(true)
	poll, stopPoll := context.WithCancel(ctx)
	defer stopPoll()
	go func() {
		select {
		case <-pending:
			stopPoll()
		case <-poll.Done():
		}
	}()
	if _, err := c.Open(poll, "reader", source, core.OpenOptions{}); !errors.Is(err, context.Canceled) ||
		submissions.Load() != 2 || downloads.Load() != 1 {
		t.Fatalf("canceled poll or unexpected extra I/O: %v %d %d", err, submissions.Load(), downloads.Load())
	}
}
