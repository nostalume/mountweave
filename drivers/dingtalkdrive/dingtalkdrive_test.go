package dingtalkdrive_test

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
	"github.com/nostalume/mountweave/drivers/dingtalkdrive"
	"github.com/nostalume/mountweave/drivers/local"
)

type transport func(*http.Request) (*http.Response, error)

func (f transport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type pending struct{ context.Context }

func (p pending) Read([]byte) (int, error) { <-p.Done(); return 0, p.Err() }
func (pending) Close() error               { return nil }

func reply(status int, data any) *http.Response {
	body, _ := json.Marshal(data)
	return &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader(string(body)))}
}

func TestFolderAndPrivateDownloadGrant(t *testing.T) {
	const route = "/v1.0/storage/spaces/24680/dentries"
	const rootID, dirID, fileID, filename = "root-folder", "reports", "file-id", "Report %.pdf"
	const accountHeader = "X-Acs-Dingtalk-Access-Token"
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()
	var mode string
	var grants, bodies int
	dentry := func(id, parent, name, kind string) map[string]any {
		return map[string]any{"spaceId": "24680", "id": id, "parentId": parent, "name": name, "type": kind,
			"status": "NORMAL", "size": 6, "modifiedTime": "2022-01-01T10:00:00Z"}
	}
	page := func(next string, files ...map[string]any) *http.Response {
		if files == nil {
			files = []map[string]any{}
		}
		return reply(http.StatusOK, map[string]any{"dentries": files, "nextToken": next})
	}
	api := &http.Client{Transport: transport(func(r *http.Request) (*http.Response, error) {
		if r.URL.Scheme != "https" || r.URL.Host != "api.dingtalk.com" || r.URL.Query().Get("unionId") != "union-fixture" || r.Header.Get("Cookie") != "" {
			return nil, errors.New("wrong app token receiver, operator, or request state")
		}
		r = r.Clone(r.Context())
		r.Header.Set(accountHeader, "app-secret-fixture")
		defer r.Body.Close()
		switch r.Method + " " + r.URL.Path {
		case "POST " + route + "/" + rootID + "/query":
			root := dentry(rootID, "external-parent", "Root", "FOLDER")
			if mode == "wrong-root" {
				root["spaceId"] = "other-space"
			}
			return reply(http.StatusOK, map[string]any{"dentry": root}), nil
		case "GET " + route:
			q := r.URL.Query()
			if q.Get("maxResults") != "30" {
				t.Error("native list omitted its page bound")
			}
			switch q.Get("parentId") {
			case rootID:
				if mode == "missing-dentries" {
					return reply(http.StatusOK, map[string]any{}), nil
				}
				if q.Get("nextToken") == "" {
					return page("token+/?"), nil
				}
				if q.Get("nextToken") == "token+/?" {
					return page("", dentry(dirID, rootID, "Reports", "FOLDER")), nil
				}
			case dirID:
				file := dentry(fileID, dirID, filename, "FILE")
				link := dentry("shortcut", dirID, "Shortcut.dlink", "FILE")
				link["extension"] = "dlink"
				if mode == "wrong-parent" {
					file["parentId"] = "unrelated"
				}
				if mode == "duplicate" {
					if q.Get("nextToken") == "" {
						return page("more", file), nil
					}
					return page("", dentry("different-id", dirID, filename, "FILE")), nil
				}
				return page("", file, link), nil
			}
		case "POST " + route + "/" + fileID + "/downloadInfos/query":
			grants++
			if mode == "denied" {
				return reply(http.StatusForbidden, map[string]any{"code": "permissionDenied", "message": "app-secret-fixture"}), nil
			}
			headers := map[string]string{"authorization": "OSS temporary-signature", "x-oSS-security-token": "scoped-fixture", "host": "body.example.test"}
			switch mode {
			case "unsafe-header":
				headers[accountHeader] = "app-secret-fixture"
			case "wrong-range":
				headers["range"] = "bytes=0-3"
			case "signed-range":
				headers["range"] = "bytes=1-3"
			}
			return reply(http.StatusOK, map[string]any{"protocol": "HEADER_SIGNATURE", "headerSignatureInfo": map[string]any{
				"resourceUrls": []string{"http://body.example.test/obsolete", "https://body.example.test/start?sig=cap%2b"}, "headers": headers,
				"internalResourceUrls": []string{"https://internal.example.test/file"},
			}}), nil
		}
		return nil, fmt.Errorf("unexpected DingTalk fixture operation")
	})}
	content := &http.Client{Transport: transport(func(r *http.Request) (*http.Response, error) {
		bodies++
		if r.Method != http.MethodGet || r.URL.Scheme != "https" || r.Header.Get(accountHeader) != "" ||
			r.Header.Get("Cookie") != "" || r.Header.Get("Referer") != "" || r.Header.Get("Accept-Encoding") != "identity" {
			t.Error("signed body request mixed in control authority")
		}
		switch r.URL.Host + r.URL.Path {
		case "body.example.test/start":
			if r.URL.RawQuery != "sig=cap%2b" || r.Header.Get("Authorization") != "OSS temporary-signature" ||
				r.Header.Get("X-Oss-Security-Token") != "scoped-fixture" || r.Host != "body.example.test" ||
				mode == "signed-range" && r.Header.Get("Range") != "bytes=1-3" {
				t.Error("signed grant was changed")
			}
			return &http.Response{StatusCode: http.StatusFound, Header: http.Header{"Location": {"https://cdn.example.test/body"}}, Body: http.NoBody}, nil
		case "cdn.example.test/body":
			if r.Header.Get("Authorization") != "" || r.Header.Get("X-Oss-Security-Token") != "" {
				t.Error("signed headers followed a redirect")
			}
			if mode == "wait" {
				return &http.Response{StatusCode: http.StatusOK, ContentLength: 6, Body: pending{r.Context()}}, nil
			}
			if r.Header.Get("Range") == "bytes=1-3" {
				return &http.Response{StatusCode: http.StatusPartialContent, ContentLength: 3, Body: io.NopCloser(strings.NewReader("bcd")),
					Header: http.Header{"Content-Range": {"bytes 1-3/6"}}}, nil
			}
			return &http.Response{StatusCode: http.StatusOK, ContentLength: -1, Body: io.NopCloser(strings.NewReader("abcdef!"))}, nil
		}
		return nil, errors.New("unexpected body receiver")
	})}
	opts := dingtalkdrive.Options{SpaceID: "24680", RootID: rootID, OperatorUnionID: "union-fixture", Client: api, ContentClient: content}
	mode = "wrong-root"
	if failed, err := dingtalkdrive.Open(ctx, opts); core.CodeOf(err) != core.InvalidUpstreamResponse {
		t.Fatal("mismatched root accepted", err)
	} else if failed != nil {
		_ = failed.Close()
	}
	mode = ""
	d, err := dingtalkdrive.Open(ctx, opts)
	if err != nil {
		t.Fatal(err)
	}
	c, err := core.New(core.Options{MaxCursors: 2, Authorize: func(context.Context, core.Principal, core.Action, core.Address) error { return nil }})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close(ctx)
	mount, err := c.Install("/ding", d)
	if err != nil {
		t.Fatal(err)
	}
	list := core.ListRequest{Query: core.Children(core.Path("/ding")), Limit: 10, CursorTTL: time.Minute}
	page1, err := c.List(ctx, "u", list)
	if err != nil || len(page1.Items) != 0 || page1.NextCursor == nil {
		t.Fatal("empty native page lost its cursor", page1, err)
	}
	list.Cursor = *page1.NextCursor
	page2, err := c.List(ctx, "u", list)
	if err != nil || len(page2.Items) != 1 || page2.Items[0].Name != "Reports" || page2.NextCursor != nil {
		t.Fatal("native cursor continuation", page2, err)
	}
	mode, list.Cursor = "missing-dentries", ""
	if _, err := c.List(ctx, "u", list); core.CodeOf(err) != core.InvalidUpstreamResponse {
		t.Fatal("missing directory entries were treated as EOF", err)
	}
	mode = ""
	target := core.Path("/ding/Reports/" + filename)
	if f, err := c.Stat(ctx, "u", target); err != nil || f.Size == nil || *f.Size != 6 || f.ModifiedAt == nil {
		t.Fatal("projected native file metadata", f, err)
	}
	if _, err := c.Open(ctx, "u", core.Path("/ding/Reports/Shortcut.dlink"), core.OpenOptions{}); core.CodeOf(err) != core.UnsupportedOperation || grants != 0 {
		t.Fatal("shortcut reached the download grant", err)
	}
	for scenario, code := range map[string]core.Code{"wrong-parent": core.InvalidUpstreamResponse, "duplicate": core.PathAmbiguous} {
		mode = scenario
		if _, err := c.Open(ctx, "u", target, core.OpenOptions{}); core.CodeOf(err) != code || grants != 0 {
			t.Fatal("unconfirmed path reached the download grant", scenario, err)
		}
	}
	mode = "denied"
	if _, err := c.Open(ctx, "u", target, core.OpenOptions{}); core.CodeOf(err) != core.UpstreamAccessDenied ||
		strings.Contains(err.Error(), "app-secret-fixture") || !strings.Contains(err.Error(), "permissionDenied") {
		t.Fatal("grant error lost its cause or revealed a secret", err)
	}
	mode = "unsafe-header"
	if _, err := c.Open(ctx, "u", target, core.OpenOptions{}); core.CodeOf(err) != core.InvalidUpstreamResponse || bodies != 0 {
		t.Fatal("control credential reached the body transport", err)
	}
	mode = "wrong-range"
	if _, err := c.Open(ctx, "u", target, core.OpenOptions{}); core.CodeOf(err) != core.InvalidUpstreamResponse || bodies != 0 {
		t.Fatal("body range conflicting with the caller was used", err)
	}
	mode = "signed-range"
	opened, err := c.Open(ctx, "u", target, core.OpenOptions{Range: &core.Range{Start: 1, End: 3}})
	if err != nil {
		t.Fatal(err)
	}
	bytes, err := io.ReadAll(opened.Body)
	if err = errors.Join(err, opened.Body.Close()); err != nil || string(bytes) != "bcd" ||
		opened.Range == nil || *opened.Range != (core.ContentRange{Start: 1, End: 3, Total: 6}) {
		t.Fatal("actual range and body", string(bytes), opened.Range, err)
	}
	mode = ""
	root := t.TempDir()
	localStore, err := local.New(root)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Install("/local", localStore); err != nil {
		t.Fatal(err)
	}
	if result, err := c.Copy(ctx, "u", target, core.Path("/local/copied")); err != nil || result.Effect != core.Applied {
		t.Fatal("Copy from DingTalk", result, err)
	}
	if data, err := os.ReadFile(filepath.Join(root, "copied")); err != nil || string(data) != "abcdef!" {
		t.Fatal("Copy did not read to the actual body EOF", string(data), err)
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
