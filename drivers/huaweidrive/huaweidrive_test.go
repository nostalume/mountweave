package huaweidrive_test

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
	"github.com/nostalume/mountweave/drivers/huaweidrive"
)

type transport func(*http.Request) (*http.Response, error)

func (t transport) RoundTrip(r *http.Request) (*http.Response, error) { return t(r) }

type waitingBody struct{ ctx context.Context }

func (b waitingBody) Read([]byte) (int, error) { <-b.ctx.Done(); return 0, b.ctx.Err() }
func (waitingBody) Close() error               { return nil }

func response(status int, body string) *http.Response {
	return &http.Response{StatusCode: status, ContentLength: int64(len(body)), Body: io.NopCloser(strings.NewReader(body)), Header: make(http.Header)}
}

func TestReadPersonalDrive(t *testing.T) {
	const (
		apiHost     = "driveapis.cloud.huawei.com.cn"
		docsID      = "folder-a"
		rootID      = "opaque-account-root"
		fileID      = "file-a"
		versionID   = "362238192574807808.362238192574807809"
		fileName    = "report's %.txt"
		rootQuery   = "'root' in parentFolder and recycled = false"
		folderQuery = "'folder-a' in parentFolder and recycled = false"
	)
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()
	var mode string
	var grants, delegated int
	file := func(id, name, parent, mime string) map[string]any {
		return map[string]any{"id": id, "fileName": name, "mimeType": mime, "parentFolder": []string{parent}, "containers": []string{"drive"},
			"ownedByMe": true, "size": 6, "editedTime": "2020-06-13T03:17:55.000Z"}
	}
	folder := func() map[string]any { return file(docsID, "Docs", rootID, "application/vnd.huawei-apps.folder") }
	page := func(next string, entries ...map[string]any) *http.Response {
		data, _ := json.Marshal(map[string]any{"category": "drive#fileList", "nextCursor": next, "files": entries})
		return response(http.StatusOK, string(data))
	}
	redirect := func(target string) *http.Response {
		resp := response(http.StatusFound, "")
		resp.Header.Set("Location", target)
		return resp
	}
	api := &http.Client{Transport: transport(func(r *http.Request) (*http.Response, error) {
		if r.URL.Scheme != "https" || r.URL.Host != apiHost || r.Method != http.MethodGet || r.Header.Get("Cookie") != "" {
			return nil, errors.New("authorized transport received an unexpected origin, method or cookie")
		}
		r = r.Clone(r.Context())
		r.Header.Set("Authorization", "Bearer host-fixture")
		q := r.URL.Query()
		switch r.URL.Path {
		case "/drive/v1/files/" + fileID:
			grants++
			if q.Get("form") != "content" || len(q) != 1 {
				t.Error("content request did not use Files:get.content")
			}
			if mode == "direct" {
				return response(http.StatusOK, "abcdef"), nil
			}
			return redirect("https://content.example.test/grant?sig=private%2b"), nil
		case "/drive/v1/files/" + fileID + "/historyVersions/" + versionID:
			if q.Get("form") == "content" && len(q) == 1 {
				grants++
				return redirect("https://content.example.test/grant?sig=private%2b"), nil
			}
			if q.Get("fields") != "category,id,size,editedTime" || len(q) != 1 {
				return nil, errors.New("invalid historical metadata request")
			}
			id := versionID
			switch mode {
			case "wrong-version":
				id = "another-version"
			case "missing-version":
				return response(http.StatusNotFound, "{}"), nil
			}
			return response(http.StatusOK, fmt.Sprintf(`{"category":"drive#historyVersion","id":%q,"size":4,"editedTime":"2020-05-14T11:02:38Z"}`, id)), nil
		case "/drive/v1/files":
			if q.Get("containers") != "drive" || !strings.Contains(q.Get("fields"), "parentFolder") ||
				q.Get("pageSize") != "1" && q.Get("pageSize") != "100" {
				t.Error("list lost its native scope or field selection")
			}
			cursor := q.Get("cursor")
			switch q.Get("queryParam") {
			case rootQuery:
				if q.Get("pageSize") == "1" {
					if mode == "unconfirmed-root" {
						return page(""), nil
					}
					return page("unused", folder()), nil
				}
				if cursor == "" {
					return page("opaque/+?"), nil
				}
				if cursor == "opaque/+?" {
					if mode == "expired" {
						return response(http.StatusGone, `{"error":{"errorDetail":[{"errorCode":"21004100","description":"host-fixture"}]}}`), nil
					}
					return page("", folder()), nil
				}
			case rootQuery + " and fileName = 'Docs'", rootQuery + " and fileName = 'docs'":
				if cursor == "" && mode == "ambiguous" {
					return page("duplicate", folder()), nil
				}
				if cursor == "duplicate" {
					return page("", file("other-folder", "Docs", rootID, "application/vnd.huawei-apps.folder")), nil
				}
				if mode == "wrong-root-parent" || mode == "unconfirmed-root" {
					parent := "elsewhere"
					if mode == "unconfirmed-root" {
						parent = "applicationData"
					}
					return page("", file(docsID, "Docs", parent, "application/vnd.huawei-apps.folder")), nil
				}
				return page("", folder()), nil
			case folderQuery:
				entry := file(fileID, fileName, docsID, "text/plain")
				if mode == "wrong-parent" {
					entry["parentFolder"] = []string{"elsewhere"}
				}
				if mode == "shared" || mode == "history" {
					entry["ownedByMe"] = false
				}
				return page("", entry), nil
			}
		}
		return nil, fmt.Errorf("unexpected native request")
	})}
	content := &http.Client{Transport: transport(func(r *http.Request) (*http.Response, error) {
		delegated++
		if r.URL.Scheme != "https" || r.Header.Get("Authorization") != "" || r.Header.Get("Cookie") != "" ||
			r.Header.Get("Referer") != "" || r.Header.Get("Accept-Encoding") != "identity" {
			t.Error("body transport received account credentials or unsafe request state")
		}
		switch r.URL.Host + r.URL.Path {
		case "content.example.test/grant":
			if r.URL.RawQuery != "sig=private%2b" {
				t.Error("signed URL was rewritten")
			}
			return redirect("https://" + apiHost + "/delegated-body"), nil
		case apiHost + "/delegated-body":
			if mode == "history" || mode == "history-overlong" {
				body := "past"
				if mode == "history-overlong" {
					body += "!"
				}
				return &http.Response{StatusCode: http.StatusOK, ContentLength: -1, Body: io.NopCloser(strings.NewReader(body))}, nil
			}
			if mode == "wait" {
				return &http.Response{StatusCode: http.StatusOK, ContentLength: 6, Body: waitingBody{r.Context()}}, nil
			}
			if r.Header.Get("Range") == "bytes=1-3" {
				resp := response(http.StatusPartialContent, "bcd")
				resp.Header.Set("Content-Range", "bytes 1-3/6")
				return resp, nil
			}
			return &http.Response{StatusCode: http.StatusOK, ContentLength: -1, Body: io.NopCloser(strings.NewReader("abcdef!"))}, nil
		}
		return nil, errors.New("unexpected delegated receiver")
	})}
	mode = "unconfirmed-root"
	options := huaweidrive.Options{Client: api, ContentClient: content}
	unconfirmed, err := huaweidrive.Open(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	openedOutside, err := unconfirmed.Open(ctx, core.NativePath("/Docs/"+fileName), core.OpenOptions{
		Selection: core.ReadSelection{Version: versionID},
	})
	if openedOutside.Body != nil {
		_ = openedOutside.Body.Close()
	}
	if core.CodeOf(err) != core.InvalidUpstreamResponse || grants != 0 {
		t.Fatal("application data escaped the ordinary root", err, grants)
	}
	mode = ""
	if file, err := unconfirmed.Stat(ctx, core.NativePath("/Docs/"+fileName)); err != nil || file.Kind != core.File {
		t.Fatal("valid entries were refused after an empty root probe", file, err)
	}
	if err := unconfirmed.Close(); err != nil {
		t.Fatal(err)
	}

	driver, err := huaweidrive.Open(ctx, options)
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
	listed, err := c.List(ctx, "u", list)
	if err != nil || len(listed.Items) != 0 || listed.NextCursor == nil {
		t.Fatal("empty native page lost its continuation", listed, err)
	}
	list.Cursor = *listed.NextCursor
	listed, err = c.List(ctx, "u", list)
	if err != nil || len(listed.Items) != 1 || listed.Items[0].Name != "Docs" || listed.NextCursor != nil {
		t.Fatal("native continuation did not reach its last page", listed, err)
	}
	mode = "expired"
	list.Cursor = ""
	listed, err = c.List(ctx, "u", list)
	if err != nil || listed.NextCursor == nil {
		t.Fatal("first page for expired cursor", listed, err)
	}
	list.Cursor = *listed.NextCursor
	if _, err := c.List(ctx, "u", list); core.CodeOf(err) != core.UpstreamFailure || strings.Contains(err.Error(), "host-fixture") {
		t.Fatal("cursor failure escaped or was treated as EOF", err)
	} else if detail := (*huaweidrive.APIError)(nil); !errors.As(err, &detail) || detail.Status != http.StatusGone || detail.Code != 21004100 {
		t.Fatal("lost native cursor error", err)
	}
	mode = ""
	target := core.Path("/remote/Docs/" + fileName)
	if m, err := c.Stat(ctx, "u", target); err != nil || m.Size == nil || *m.Size != 6 || m.ModifiedAt == nil {
		t.Fatal("exact Stat for a name containing a quote", m, err)
	}
	if _, err := c.Open(ctx, "u", core.Path("/remote/docs/"+fileName), core.OpenOptions{}); core.CodeOf(err) != core.NotFound {
		t.Fatal("case-folded result passed exact path selection", err)
	}
	for state, code := range map[string]core.Code{"wrong-root-parent": core.InvalidUpstreamResponse, "wrong-parent": core.InvalidUpstreamResponse,
		"ambiguous": core.PathAmbiguous, "shared": core.UpstreamAccessDenied} {
		mode = state
		if _, err := c.Open(ctx, "u", target, core.OpenOptions{}); core.CodeOf(err) != code || grants != 0 {
			t.Fatal("unconfirmed or unowned selection reached the body route", state, err)
		}
	}
	read := func(options core.OpenOptions, wantBody string) core.Content {
		t.Helper()
		got, err := c.Open(ctx, "u", target, options)
		if err != nil {
			t.Fatal(err)
		}
		data, err := io.ReadAll(got.Body)
		if err = errors.Join(err, got.Body.Close()); err != nil || string(data) != wantBody {
			t.Fatal("wrong body", string(data), err)
		}
		return got
	}
	mode = "direct"
	if got := read(core.OpenOptions{}, "abcdef"); got.Length == nil || *got.Length != 6 || delegated != 0 {
		t.Fatal("direct content lost its single native request")
	}
	mode = ""
	if got := read(core.OpenOptions{Range: &core.Range{Start: 1, End: 3}}, "bcd"); got.Range == nil || *got.Range != (core.ContentRange{Start: 1, End: 3, Total: 6}) {
		t.Fatal("lost actual delegated range")
	}
	version := core.ReadSelection{Version: versionID}
	versionOpen := core.OpenOptions{Selection: version}
	before := grants
	for state, code := range map[string]core.Code{"wrong-version": core.InvalidUpstreamResponse, "missing-version": core.NotFound} {
		mode = state
		if _, err := c.Open(ctx, "u", target, versionOpen); core.CodeOf(err) != code || grants != before {
			t.Fatal("unconfirmed version reached its body route", state, err)
		}
	}
	mode = "history"
	historical, err := c.Stat(ctx, "u", target, version)
	if err != nil || historical.Size == nil || *historical.Size != 4 || historical.Evidence.Version != versionID ||
		historical.ModifiedAt == nil || historical.ModifiedAt.Format(time.RFC3339) != "2020-05-14T11:02:38Z" {
		t.Fatal("historical Stat used current file metadata", historical, err)
	}
	if got := read(versionOpen, "past"); got.Length != nil || got.Entry.Evidence.Version != historical.Evidence.Version ||
		got.Entry.Size == nil || *got.Entry.Size != *historical.Size || grants != before+1 {
		t.Fatal("historical Open lost its selected version, size or wire framing", got, grants)
	}
	mode = "history-overlong"
	tooLong, err := c.Open(ctx, "u", target, versionOpen)
	if err != nil {
		t.Fatal(err)
	}
	data, readErr := io.ReadAll(tooLong.Body)
	if err := errors.Join(readErr, tooLong.Body.Close()); string(data) != "past" || core.CodeOf(err) != core.InvalidUpstreamResponse {
		t.Fatal("historical stream exceeded its selected size", string(data), err)
	}
	mode = ""
	if got := read(core.OpenOptions{}, "abcdef!"); got.Length != nil || got.Range != nil {
		t.Fatal("ordinary delegated read invented wire framing", got.Length, got.Range)
	}
	mode = "wait"
	opened, err := c.Open(ctx, "u", target, core.OpenOptions{})
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
