package feishudrive_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/feishudrive"
)

type transport func(*http.Request) (*http.Response, error)

func (f transport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

var errDeleteClose = errors.New("delete response close failed")

type closeOnce struct {
	io.ReadCloser
	calls *atomic.Int64
}

func (b closeOnce) Close() error {
	err := b.ReadCloser.Close()
	if b.calls.Add(1) == 1 {
		err = errors.Join(err, errDeleteClose)
	}
	return err
}

func TestFolderThroughCore(t *testing.T) {
	const (
		authority  = "open.feishu.cn"
		basePath   = "/open-apis/drive/v1/files"
		exportPath = "/open-apis/drive/v1/export_tasks"
		exportBody = "%PDF-fixture"
		badFormat  = 1
		failedJob  = 2
		cancelJob  = 3
		extraBody  = 4
		shortBody  = 5
		uploaded   = `{"token":"uploaded","parent_token":"root","name":"space % 中.json","type":"file"},`
		firstPage  = `{"code":0,"data":{"has_more":true,"next_page_token":"a+/=?","files":[
			` + uploaded + `
			{"token":"online","parent_token":"root","name":"Online","type":"docx"},
			{"token":"dupe-file","parent_token":"root","name":"same","type":"file"}
		]}}`
		lastPage = `{"code":0,"data":{"has_more":false,"files":[
			{"token":"shortcut","parent_token":"root","name":"same","type":"shortcut","shortcut_info":{"target_token":"foreign"}},
			{"token":"broken","parent_token":"root","name":"bad","type":"folder"},
			{"token":"redir","parent_token":"root","name":"redirect","type":"file"},
			{"token":"held","parent_token":"root","name":"hold","type":"file"}
		]}}`
		document = `{"v":7}`
	)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	var lists, downloads, starts, polls, results atomic.Int64
	var deletes, deleteCloses atomic.Int64
	var exportMode atomic.Int32
	var deleted atomic.Bool
	waiting := make(chan struct{}, 1)
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		method := http.MethodGet
		switch r.URL.Path {
		case exportPath:
			method = http.MethodPost
		case basePath + "/uploaded", basePath + "/redir":
			method = http.MethodDelete
		}
		if r.Method != method || r.Header.Get("Authorization") != "Bearer fixture" ||
			r.Header.Get("Cookie") != "" || r.Header.Get("Accept-Encoding") != "identity" {
			t.Errorf("invalid API request for %s", r.URL.Path)
			w.WriteHeader(http.StatusForbidden)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case basePath:
			lists.Add(1)
			q := r.URL.Query()
			switch q.Get("folder_token") {
			case "root":
				switch q.Get("page_token") {
				case "":
					page := firstPage
					if deleted.Load() {
						page = strings.Replace(page, uploaded, "", 1)
					}
					_, _ = io.WriteString(w, page)
				case "a+/=?":
					_, _ = io.WriteString(w, lastPage)
				default:
					t.Errorf("changed native page token: %q", q.Get("page_token"))
					w.WriteHeader(http.StatusBadRequest)
				}
			case "broken":
				_, _ = io.WriteString(w, `{"code":0,"data":{"has_more":false,"files":[
					{"token":"foreign","parent_token":"other","name":"secret","type":"file"}]}}`)
			default:
				w.WriteHeader(http.StatusForbidden)
				_, _ = io.WriteString(w, `{"code":1061004,"msg":"folder access denied"}`)
			}
		case basePath + "/redir":
			_, _ = io.WriteString(w, `{"code":1061007,"msg":"file has been delete"}`)
		case basePath + "/uploaded":
			if q := r.URL.Query(); q.Get("type") != "file" || q.Get("async") != "false" || r.ContentLength != 0 {
				t.Error("wrong native file deletion request")
			}
			switch deletes.Add(1) {
			case 1:
				_, _ = io.WriteString(w, `{"code":0,"data":{"task_id":"unexpected"}}`)
			case 2:
				deleted.Store(true)
				_, _ = io.WriteString(w, `{"code":0,"data":{}}`)
			default:
				t.Error("native deletion was repeated")
			}
		case basePath + "/uploaded/download":
			downloads.Add(1)
			if r.Header.Get("Range") == "bytes=1-3" {
				w.Header().Set("Content-Range", "bytes 1-3/7")
				w.WriteHeader(http.StatusPartialContent)
				_, _ = io.WriteString(w, document[1:4])
			} else {
				_, _ = io.WriteString(w, document)
			}
		case exportPath:
			starts.Add(1)
			var request map[string]string
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil || len(request) != 3 ||
				request["token"] != "online" || request["type"] != "docx" || request["file_extension"] != "pdf" {
				t.Errorf("wrong export request: %v, %v", request, err)
			}
			_, _ = io.WriteString(w, `{"code":0,"data":{"ticket":"ticket"}}`)
		case exportPath + "/ticket":
			attempt := polls.Add(1)
			if r.URL.Query().Get("token") != "online" {
				t.Error("export query lost the selected source token")
			}
			if mode := exportMode.Load(); attempt == 1 || mode == cancelJob {
				_, _ = io.WriteString(w, `{"code":0,"data":{"result":{"job_status":0}}}`)
				if mode == cancelJob {
					select {
					case waiting <- struct{}{}:
					default:
					}
				}
			} else if mode == failedJob {
				_, _ = io.WriteString(w, `{"code":0,"data":{"result":{"job_status":3,"job_error_msg":"native conversion failed"}}}`)
			} else {
				format := "pdf"
				if mode == badFormat {
					format = "docx"
				}
				_, _ = fmt.Fprintf(w, `{"code":0,"data":{"result":{"job_status":0,"token":"online","type":"docx","file_extension":%q,"file_token":"converted","file_size":%d}}}`,
					format, len(exportBody))
			}
		case exportPath + "/file/converted/download":
			results.Add(1)
			if r.Header.Get("Range") != "" {
				t.Error("export download sent a range")
			}
			w.Header().Set("Content-Type", "application/pdf")
			w.Header().Set("Content-Disposition", `attachment; filename="changed.pdf"`)
			w.(http.Flusher).Flush()
			output := exportBody
			switch exportMode.Load() {
			case extraBody:
				output += "x"
			case shortBody:
				output = output[:len(output)-1]
			}
			_, _ = io.WriteString(w, output)
		case basePath + "/redir/download":
			w.Header().Set("Location", "https://foreign.invalid/private")
			w.WriteHeader(http.StatusFound)
		case basePath + "/held/download":
			w.Header().Set("Content-Length", "1")
			w.WriteHeader(http.StatusOK)
			w.(http.Flusher).Flush()
			<-r.Context().Done()
		default:
			t.Errorf("unexpected Feishu request: %s", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(server.Close)

	client := server.Client()
	loopback, _ := url.Parse(server.URL)
	upstream, _ := url.Parse("https://" + authority)
	base := client.Transport
	client.Transport = transport(func(r *http.Request) (*http.Response, error) {
		if r.URL.Scheme != "https" || r.URL.Host != authority {
			t.Error("driver tried to leave the Feishu API origin")
			return nil, errors.New("unexpected origin")
		}
		req := r.Clone(r.Context())
		req.URL.Host = loopback.Host
		req.Header.Set("Authorization", "Bearer fixture")
		resp, err := base.RoundTrip(req)
		if err == nil && r.Method == http.MethodDelete && deleted.Load() {
			resp.Body = closeOnce{resp.Body, &deleteCloses}
		}
		return resp, err
	})
	client.Jar, _ = cookiejar.New(nil)
	client.Jar.SetCookies(upstream, []*http.Cookie{{Name: "account", Value: "fixture"}})
	d, err := feishudrive.Open(ctx, feishudrive.Options{RootToken: "root", Client: client})
	if err != nil {
		if d != nil {
			_ = d.Close()
		}
		t.Fatal(err)
	}
	c, err := core.New(core.Options{MaxCursors: 4, Authorize: func(context.Context, core.Principal, core.Action, core.Address) error { return nil }})
	if err != nil {
		_ = d.Close()
		t.Fatal(err)
	}
	defer func() {
		if err := c.Close(ctx); err != nil {
			t.Error(err)
		}
	}()
	mount, err := c.Install("/drive", d)
	if err != nil {
		_ = d.Close()
		t.Fatal(err)
	}
	list := core.ListRequest{Query: core.Children(core.Path("/drive")), Limit: 1, CursorTTL: time.Minute}
	var names []string
	for range 6 {
		page, err := c.List(ctx, "reader", list)
		if err != nil {
			t.Fatal(err)
		}
		for _, f := range page.Items {
			names = append(names, f.Name)
		}
		if page.NextCursor == nil {
			break
		}
		list.Cursor = *page.NextCursor
	}
	if got := strings.Join(names, "|"); got != "space % 中.json|same|bad|redirect|hold" {
		t.Fatalf("visible original files and folders: %q", got)
	}
	if _, err := c.Open(ctx, "reader", core.Path("/drive/same"), core.OpenOptions{}); core.CodeOf(err) != core.PathAmbiguous {
		t.Fatalf("shortcut collision: %v", err)
	}
	if _, err := c.Open(ctx, "reader", core.Path("/drive/Online"), core.OpenOptions{}); core.CodeOf(err) != core.UnsupportedOperation {
		t.Fatalf("online document opened as an original: %v", err)
	}
	if _, err := c.Stat(ctx, "reader", core.Path("/drive/bad/secret")); core.CodeOf(err) != core.InvalidUpstreamResponse || downloads.Load() != 0 {
		t.Fatalf("wrong parent reached download: %v", err)
	}
	file := core.Path("/drive/space % 中.json")
	beforeRemove := lists.Load()
	if changed, err := c.Remove(ctx, "reader", file); changed.Effect != core.Unchanged ||
		core.CodeOf(err) != core.UnsupportedOperation || lists.Load() != beforeRemove {
		t.Fatalf("disabled removal reached Feishu: %+v %v", changed, err)
	}
	stat, err := c.Stat(ctx, "reader", file)
	if err != nil || stat.Kind != core.File || stat.Size != nil {
		t.Fatalf("list metadata invented a body size: %+v %v", stat, err)
	}
	absent := core.OpenOptions{Selection: core.ReadSelection{When: core.IfAbsent()}}
	if _, err := c.Open(ctx, "reader", core.Path("/drive/missing"), absent); core.CodeOf(err) != core.NotFound {
		t.Fatalf("condition invented a file: %v", err)
	}
	if _, err := c.Open(ctx, "reader", file, absent); core.CodeOf(err) != core.PreconditionFailed {
		t.Fatalf("existing file did not fail the absence condition: %v", err)
	}
	body, err := c.Open(ctx, "reader", file, core.OpenOptions{Range: &core.Range{Start: 1, End: 3}})
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(body.Body)
	if err := errors.Join(err, body.Body.Close()); err != nil || string(data) != document[1:4] ||
		body.Range == nil || *body.Range != (core.ContentRange{Start: 1, End: 3, Total: 7}) {
		t.Fatalf("native body range: %q %+v %v", data, body.Range, err)
	}
	body, err = c.Open(ctx, "reader", file, core.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	data, err = io.ReadAll(body.Body)
	if err = errors.Join(err, body.Body.Close()); err != nil || string(data) != document || body.Range != nil {
		t.Fatalf("JSON file body: %q %v %v", data, body.Range, err)
	}
	if _, err := c.Open(ctx, "reader", core.Path("/drive/redirect"), core.OpenOptions{}); core.CodeOf(err) != core.UpstreamFailure {
		t.Fatalf("foreign body redirect accepted: %v", err)
	}
	openCtx, stop := context.WithCancel(ctx)
	defer stop()
	hold, err := c.Open(openCtx, "reader", core.Path("/drive/hold"), core.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	stop()
	_, err = io.ReadAll(hold.Body)
	_ = hold.Body.Close()
	if err == nil {
		t.Fatal("canceled read completed normally")
	}
	if err := mount.Retire(ctx); err != nil {
		t.Fatal(err)
	}
	before := lists.Load()
	if _, err := c.Stat(ctx, "reader", file); err == nil || lists.Load() != before {
		t.Fatalf("retired mount admitted native I/O: %v", err)
	}

	options := feishudrive.Options{RootToken: "root", Client: client, AllowRemove: true, ExportFormats: map[string]string{"docx": "csv"}}
	if _, err := feishudrive.Open(ctx, options); core.CodeOf(err) != core.InvalidRequest || lists.Load() != before {
		t.Fatalf("unsupported export configuration reached native I/O: %v", err)
	}
	options.ExportFormats["docx"] = "pdf"
	exporting, err := feishudrive.Open(ctx, options)
	if err != nil {
		if exporting != nil {
			_ = exporting.Close()
		}
		t.Fatal(err)
	}
	exportMount, err := c.Install("/export", exporting)
	if err != nil {
		_ = exporting.Close()
		t.Fatal(err)
	}
	options.ExportFormats["docx"] = "docx"
	online := core.Path("/export/Online")
	page, err := c.List(ctx, "reader", core.ListRequest{Query: core.Children(core.Path("/export")), Limit: 10, CursorTTL: time.Minute})
	if err != nil || len(page.Items) != 3 || page.NextCursor == nil || page.Items[1].Name != "Online" || page.Items[1].Kind != core.File || page.Items[1].Size != nil || starts.Load() != 0 {
		t.Fatalf("exportable document projection: %+v %v", page, err)
	}
	body, err = c.Open(ctx, "reader", online, core.OpenOptions{Range: &core.Range{Start: 1, End: 3}})
	if err != nil {
		t.Fatal(err)
	}
	data, err = io.ReadAll(body.Body)
	if err := errors.Join(err, body.Body.Close()); err != nil || string(data) != exportBody ||
		body.Range != nil || body.Length != nil || body.Entry.Size == nil || *body.Entry.Size != int64(len(exportBody)) ||
		body.Entry.Name != "Online" || starts.Load() != 1 || polls.Load() != 2 {
		t.Fatalf("export body, name, or task lifecycle: %q %+v %v", data, body, err)
	}
	for _, mode := range []int32{extraBody, shortBody} {
		exportMode.Store(mode)
		body, err = c.Open(ctx, "reader", online, core.OpenOptions{})
		if err != nil {
			t.Fatal(err)
		}
		data, err = io.ReadAll(body.Body)
		err = errors.Join(err, body.Body.Close())
		if mode == extraBody && (string(data) != exportBody || core.CodeOf(err) != core.InvalidUpstreamResponse) ||
			mode == shortBody && (string(data) != exportBody[:len(exportBody)-1] || !errors.Is(err, io.ErrUnexpectedEOF)) {
			t.Fatalf("unframed export did not verify the received bytes: mode=%d bytes=%q err=%v", mode, data, err)
		}
	}
	exportMode.Store(badFormat)
	if _, err := c.Open(ctx, "reader", online, core.OpenOptions{}); core.CodeOf(err) != core.InvalidUpstreamResponse {
		t.Fatalf("mismatched export format accepted: %v", err)
	}
	exportMode.Store(failedJob)
	if _, err := c.Open(ctx, "reader", online, core.OpenOptions{}); core.CodeOf(err) != core.UpstreamFailure ||
		!strings.Contains(err.Error(), "native conversion failed") {
		t.Fatalf("native task failure was lost: %v", err)
	}
	exportMode.Store(cancelJob)
	downloaded := results.Load()
	pollCtx, stopPoll := context.WithCancel(ctx)
	defer stopPoll()
	go func() {
		select {
		case <-waiting:
			stopPoll()
		case <-pollCtx.Done():
		}
	}()
	if _, err := c.Open(pollCtx, "reader", online, core.OpenOptions{}); !errors.Is(err, context.Canceled) || results.Load() != downloaded {
		t.Fatalf("pending export cancellation reached the result body: %v", err)
	}

	original := core.Path("/export/space % 中.json")
	for _, test := range []struct {
		address core.Address
		options core.RemoveOptions
		code    core.Code
	}{
		{online, core.RemoveOptions{}, core.UnsupportedOperation},
		{core.Path("/export/bad"), core.RemoveOptions{}, core.UnsupportedOperation},
		{core.Path("/export/same"), core.RemoveOptions{}, core.PathAmbiguous},
		{original, core.RemoveOptions{When: core.IfPresent()}, core.UnsupportedOperation},
		{original, core.RemoveOptions{Target: core.RemoveNonDirectory}, core.UnsupportedOperation},
	} {
		if changed, err := c.Remove(ctx, "reader", test.address, test.options); changed.Effect != core.Unchanged ||
			core.CodeOf(err) != test.code || deletes.Load() != 0 {
			t.Fatalf("unsupported removal reached Feishu: %+v %v", changed, err)
		}
	}
	if changed, err := c.Remove(ctx, "reader", core.Path("/export/redirect")); changed.Effect != core.Unchanged ||
		core.CodeOf(err) != core.NotFound || !strings.Contains(err.Error(), "1061007") {
		t.Fatalf("already-deleted token lost its native outcome: %+v %v", changed, err)
	}
	if changed, err := c.Remove(ctx, "reader", original); changed.Effect != core.Unknown ||
		core.CodeOf(err) != core.UpstreamFailure || deletes.Load() != 1 {
		t.Fatalf("asynchronous task was treated as completion: %+v %v", changed, err)
	}
	if changed, err := c.Remove(ctx, "reader", original); changed.Effect != core.Applied ||
		!errors.Is(err, errDeleteClose) || deletes.Load() != 2 || deleteCloses.Load() != 1 {
		t.Fatalf("response close failure erased confirmed removal: %+v %v", changed, err)
	}
	if _, err := c.Stat(ctx, "reader", original); core.CodeOf(err) != core.NotFound {
		t.Fatalf("removed original remained visible: %v", err)
	}
	if err := exportMount.Retire(ctx); err != nil || deleteCloses.Load() != 2 {
		t.Fatalf("retirement did not retry the failed close: %v", err)
	}
}
