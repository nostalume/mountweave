package wuyingdrive_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/wuyingdrive"
)

type transport func(*http.Request) (*http.Response, error)

func (f transport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type waiting struct{ context.Context }

func (w waiting) Read([]byte) (int, error) { <-w.Done(); return 0, w.Err() }
func (waiting) Close() error               { return nil }

func TestSpaceAndOriginal(t *testing.T) {
	const user, group = "user@company", "cg-fixture"
	t.Run("personal", func(t *testing.T) { testSpaceAndOriginal(t, user, "") })
	t.Run("team", func(t *testing.T) { testSpaceAndOriginal(t, "", group) })
}

func testSpaceAndOriginal(t *testing.T, user, group string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	const region, drive = "cn-hangzhou", "cn-hangzhou+cds-fixture"
	const apiHost = "ecd." + region + ".aliyuncs.com"
	const name, token, sig = "报告.txt", "opaque+/=\n", "Signature=x%2By%3d"
	var mode string
	var grants, downloads int
	jsonResponse := func(status int, data any) *http.Response {
		wire, _ := json.Marshal(data)
		return &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader(string(wire)))}
	}
	item := func(id, parent, name, kind string, size int) map[string]any {
		return map[string]any{"FileId": id, "ParentId": parent, "Name": name, "FileType": kind, "Size": size,
			"ModifiedTime": "2026-03-01T09:10:11Z"}
	}
	page := func(next string, entries ...any) *http.Response {
		return jsonResponse(200, map[string]any{"Success": true, "Code": "success", "NextToken": next, "FileModels": entries})
	}
	api := &http.Client{Transport: transport(func(r *http.Request) (*http.Response, error) {
		q := r.URL.Query()
		if r.Method != "POST" || r.Body != nil || r.URL.Scheme != "https" || r.URL.Host != apiHost || r.URL.Path != "/" ||
			q.Get("RegionId") != region || q.Get("CdsId") != drive ||
			q.Get("EndUserId") != user || q.Has("EndUserId") != (user != "") ||
			q.Get("GroupId") != group || q.Has("GroupId") != (group != "") ||
			r.Header.Get("x-acs-version") != "2020-09-30" || r.Header.Get("Accept-Encoding") != "identity" ||
			!strings.Contains(r.URL.RawQuery, "CdsId=cn-hangzhou%2Bcds-fixture") {
			return nil, errors.New("invalid WUYING RPC wire or scope")
		}
		r = r.Clone(r.Context())
		r.Header.Set("Authorization", "fixture-RAM-signature")
		r.Header.Set("x-acs-security-token", "fixture-RAM-token")
		switch r.Header.Get("x-acs-action") {
		case "ListCdsFiles":
			size, err := strconv.Atoi(q.Get("MaxResults"))
			if err != nil || size < 1 || size > 100 || q.Get("Status") != "available" {
				return nil, errors.New("invalid WUYING listing parameters")
			}
			switch q.Get("ParentFileId") {
			case "root":
				switch q.Get("NextToken") {
				case "":
					return page(token, item("folder-id", "root", "Reports", "folder", 0)), nil
				case token:
					return page("", item("other-id", "root", "Other", "folder", 0)), nil
				}
			case "folder-id":
				parent := "folder-id"
				if mode == "wrong-parent" {
					parent = "unrelated"
				}
				if mode == "ambiguous" {
					return page("", item("file-id", parent, name, "file", 9), item("duplicate-id", parent, name, "file", 9)), nil
				}
				return page("", item("file-id", parent, name, "file", 9)), nil
			}
		case "DownloadCdsFile":
			grants++
			if q.Get("FileId") != "file-id" {
				return nil, errors.New("grant used a different file identity")
			}
			grant := map[string]any{"FileId": "file-id", "Size": 6, "DownloadUrl": "https://oss.example.test/object%2Fkey?" + sig}
			switch mode {
			case "wrong-id":
				grant["FileId"] = "unrelated"
			case "denied":
				return jsonResponse(403, map[string]any{"Code": "Forbidden", "Message": sig}), nil
			}
			return jsonResponse(200, map[string]any{"Success": true, "DownloadFileModel": grant}), nil
		}
		return nil, errors.New("unexpected WUYING control request")
	})}
	content := &http.Client{Transport: transport(func(r *http.Request) (*http.Response, error) {
		downloads++
		if r.Method != "GET" || r.URL.Scheme != "https" || r.Header.Get("Accept-Encoding") != "identity" ||
			r.Header.Get("Authorization") != "" || r.Header.Get("x-acs-security-token") != "" ||
			r.Header.Get("x-acs-action") != "" || r.Header.Get("Cookie") != "" || r.Header.Get("Referer") != "" {
			return nil, errors.New("WUYING body request carried control credentials")
		}
		switch r.URL.Host + r.URL.EscapedPath() {
		case "oss.example.test/object%2Fkey":
			if r.URL.RawQuery != sig {
				return nil, errors.New("signed URL was changed")
			}
			location := "https://" + apiHost + "/body"
			if mode == "empty-fragment" {
				location = "#"
			}
			return &http.Response{StatusCode: 302, Body: http.NoBody,
				Header: http.Header{"Location": {location}}}, nil
		case apiHost + "/body":
			if r.URL.RawQuery != "" {
				return nil, errors.New("redirect forwarded the signed query")
			}
			if mode == "wait" {
				return &http.Response{StatusCode: 200, ContentLength: 6, Body: waiting{r.Context()}}, nil
			}
			if r.Header.Get("Range") == "bytes=1-3" {
				return &http.Response{StatusCode: 206, ContentLength: 3, Body: io.NopCloser(strings.NewReader("bcd")),
					Header: http.Header{"Content-Range": {"bytes 1-3/6"}}}, nil
			}
			text := "abcdef"
			if mode == "overlong" {
				text += "g"
			}
			return &http.Response{StatusCode: 200, ContentLength: -1, Body: io.NopCloser(strings.NewReader(text))}, nil
		}
		return nil, errors.New("unexpected WUYING content receiver")
	})}
	opts := wuyingdrive.Options{Region: region, DriveID: drive, Client: api, ContentClient: content}
	if user != "" {
		for _, scope := range [][2]string{{"", ""}, {user, "cg-fixture"}} {
			opts.EndUserID, opts.GroupID = scope[0], scope[1]
			if d, err := wuyingdrive.Open(ctx, opts); d != nil || core.CodeOf(err) != core.InvalidRequest {
				t.Fatal("invalid scope accepted", scope, err)
			}
		}
	}
	opts.EndUserID, opts.GroupID = user, group
	d, err := wuyingdrive.Open(ctx, opts)
	if err != nil {
		t.Fatal(err)
	}
	c, err := core.New(core.Options{MaxCursors: 2, Authorize: func(context.Context, core.Principal, core.Action, core.Address) error { return nil }})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close(ctx)
	mount, err := c.Install("/wuying", d)
	if err != nil {
		t.Fatal(err)
	}
	list := core.ListRequest{Query: core.Children(core.Path("/wuying")), Limit: 2, CursorTTL: time.Minute}
	first, err := c.List(ctx, "u", list)
	if err != nil || len(first.Items) != 1 || first.Items[0].Name != "Reports" || first.NextCursor == nil {
		t.Fatal("short native page", first, err)
	}
	list.Cursor = *first.NextCursor
	last, err := c.List(ctx, "u", list)
	if err != nil || len(last.Items) != 1 || last.Items[0].Name != "Other" || last.NextCursor != nil {
		t.Fatal("terminal native page", last, err)
	}
	path := core.Path("/wuying/Reports/" + name)
	if f, err := c.Stat(ctx, "u", path); err != nil || f.Size == nil || *f.Size != 9 {
		t.Fatal("ordinary metadata", f, err)
	}
	readOriginal := func() {
		t.Helper()
		opened, err := c.Open(ctx, "u", path, core.OpenOptions{})
		if err != nil {
			t.Fatal(err)
		}
		data, err := io.ReadAll(opened.Body)
		if err = errors.Join(err, opened.Body.Close()); err != nil || string(data) != "abcdef" || opened.Range != nil || opened.Length != nil || opened.Entry.Size == nil || *opened.Entry.Size != 6 {
			t.Fatal("complete original", string(data), err)
		}
	}
	if group != "" {
		readOriginal()
		return
	}
	mode = "wrong-parent"
	if _, err := c.Open(ctx, "u", path, core.OpenOptions{}); core.CodeOf(err) != core.InvalidUpstreamResponse || grants != 0 {
		t.Fatal("unrelated file reached the grant request", err)
	}
	mode = "ambiguous"
	if _, err := c.Open(ctx, "u", path, core.OpenOptions{}); core.CodeOf(err) != core.PathAmbiguous || grants != 0 {
		t.Fatal("same-name native files were conflated", err)
	}
	for _, s := range []struct {
		mode string
		code core.Code
	}{{"wrong-id", core.InvalidUpstreamResponse}, {"denied", core.UpstreamAccessDenied}} {
		mode = s.mode
		_, err := c.Open(ctx, "u", path, core.OpenOptions{})
		if core.CodeOf(err) != s.code || strings.Contains(err.Error(), sig) || downloads != 0 {
			t.Fatal("rejected grant reached the body or disclosed credentials", mode, err)
		}
	}
	mode = "empty-fragment"
	if _, err := c.Open(ctx, "u", path, core.OpenOptions{}); core.CodeOf(err) != core.InvalidUpstreamResponse || downloads != 1 {
		t.Fatal("fragment-only redirect replayed the signed request", downloads, err)
	}
	mode = ""
	opened, err := c.Open(ctx, "u", path, core.OpenOptions{Range: &core.Range{Start: 1, End: 3}})
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(opened.Body)
	if err = errors.Join(err, opened.Body.Close()); err != nil || string(data) != "bcd" ||
		opened.Range == nil || *opened.Range != (core.ContentRange{Start: 1, End: 3, Total: 6}) {
		t.Fatal("original range and same-grant size", string(data), opened.Range, err)
	}
	mode = "overlong"
	opened, err = c.Open(ctx, "u", path, core.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	data, err = io.ReadAll(opened.Body)
	if closeErr := opened.Body.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}
	if opened.Entry.Size == nil || *opened.Entry.Size != 6 || opened.Length != nil ||
		string(data) != "abcdef" || core.CodeOf(err) != core.InvalidUpstreamResponse {
		t.Fatal("unframed download did not preserve and verify the receipt size", opened.Entry.Size, opened.Length, string(data), err)
	}
	mode = ""
	readOriginal()
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
		t.Fatal("retirement did not cancel the original body", err)
	}
}
