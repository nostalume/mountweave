package tianyipan

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"strings"
	"testing"

	core "github.com/nostalume/mountweave"
)

type transportFunc func(*http.Request) (*http.Response, error)

func (f transportFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestWebsiteReopensBoundFolderAndReadsPageTwoOriginal(t *testing.T) {
	const fileID = "9007199254740993"
	account := strings.Repeat("a", 64)
	size, count := int64(8), 61
	items := make([]item, 60)
	for i := range items {
		items[i] = item{ID: fmt.Sprint(i + 100), Name: fmt.Sprintf("folder-%d", i), Folder: true}
	}
	var pages []int
	var releases, hops int
	var renames, recycles int
	fileName := "sample.zip"
	receiptID := "9007199254740992"
	session := NewBrowserSession(func(ctx context.Context, _ string, args json.RawMessage) (json.RawMessage, error) {
		var request websiteRequest
		if err := json.Unmarshal(args, &request); err != nil {
			t.Fatal(err)
		}
		switch request.Operation {
		case "account":
			return json.Marshal(websiteResponse{Account: account})
		case "list":
			if request.Account != account || request.ID != rootID {
				t.Fatalf("unbound list request: %+v", request)
			}
			pages = append(pages, request.Page)
			found := items
			if request.Page == 2 {
				found = []item{{ID: fileID, Name: fileName, Size: &size}}
			}
			return json.Marshal(websiteResponse{Account: account, Folder: rootID, Page: request.Page, Count: &count, Items: found})
		case "grant":
			if request.ID != fileID || request.Account != account {
				t.Fatalf("unbound grant request: %+v", request)
			}
			return json.Marshal(websiteResponse{Account: account, FileID: request.ID, URL: "https://edge.example/first?sig=fake"})
		case "rename":
			if request.Account != account || request.ID != fileID || request.UntilMS <= 0 {
				t.Fatalf("unbound rename request: %+v", request)
			}
			renames++
			if request.Name == "occupied.zip" {
				return json.Marshal(websiteResponse{Error: "FileAlreadyExists"})
			}
			if request.Name == "renamed.zip" {
				fileName = request.Name
			}
			return json.Marshal(websiteResponse{Account: account, FileID: request.ID, Name: request.Name})
		case "recycle":
			if request.Account != account || request.ID != fileID || request.Name != fileName || request.UntilMS <= 0 {
				t.Fatalf("unbound recycle request: %+v", request)
			}
			recycles++
			return json.Marshal(websiteResponse{Account: account, TaskID: "task-1"})
		case "recycle-status":
			if request.Account != account || request.ID != "task-1" {
				t.Fatalf("unbound task query: %+v", request)
			}
			return json.Marshal(websiteResponse{Account: account, TaskJSON: `{"taskId":"task-1","taskStatus":4,"subTaskCount":1,"successedCount":1,"failedCount":0,"skipCount":0,"successedFileIdList":[` + receiptID + `]}`})
		default:
			t.Fatalf("unexpected operation %q", request.Operation)
			return nil, nil
		}
	})
	selected, err := session.Account(t.Context())
	if err != nil || selected != account {
		t.Fatal("account check failed", err)
	}
	jar, _ := cookiejar.New(nil)
	for _, host := range []string{"edge.example", "objects.example"} {
		target, _ := url.Parse("https://" + host)
		jar.SetCookies(target, []*http.Cookie{{Name: "private", Value: "account", Path: "/"}})
	}
	client := &http.Client{Jar: jar, Transport: transportFunc(func(req *http.Request) (*http.Response, error) {
		hops++
		if req.Method != "GET" || req.Header.Get("Cookie") != "" || req.Header.Get("Authorization") != "" ||
			req.Header.Get("Referer") != "" || req.Header.Get("Accept-Encoding") != "identity" {
			t.Fatalf("invalid signed transfer headers")
		}
		if hops == 1 && req.URL.Host == "edge.example" {
			return &http.Response{StatusCode: 302, Body: http.NoBody,
				Header: http.Header{"Location": {"https://objects.example/original?sig=redirect"}}}, nil
		}
		if hops != 2 || req.URL.Host != "objects.example" {
			t.Fatal("unexpected transfer destination")
		}
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader("test ZIP")),
			ContentLength: -1, Header: http.Header{"Content-Disposition": {`attachment; filename="sample.zip"`}}}, nil
	})}
	d, err := OpenWebsite(t.Context(), WebsiteOptions{Session: session, Account: selected, RootID: rootID,
		ContentClient: client, Release: func() error { releases++; return nil }})
	if err != nil || d.RootID() != rootID {
		t.Fatal("reopen failed", err)
	}
	read, err := d.Open(t.Context(), core.NativePath("/sample.zip"), core.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(read.Body)
	if err != nil || string(body) != "test ZIP" {
		t.Fatalf("original received %q, %v", body, err)
	}
	if !strings.Contains(fmt.Sprint(pages), "1 2") || hops != 2 {
		t.Fatalf("expected page two and the native redirect, got pages %v and %d hops", pages, hops)
	}
	file := core.NativePath("/sample.zip")
	if change, err := d.Relocate(t.Context(), file, core.NativePath("/other/sample.zip")); change.Effect != core.Unchanged || core.CodeOf(err) != core.UnsupportedOperation || renames != 0 {
		t.Fatalf("a cross-directory move reached rename: %+v %v", change, err)
	}
	if change, err := d.Relocate(t.Context(), file, core.NativePath("/occupied.zip")); change.Effect != core.Unchanged || core.CodeOf(err) != core.AlreadyExists {
		t.Fatalf("native conflict did not preserve both entries: %+v %v", change, err)
	}
	if change, err := d.Relocate(t.Context(), file, core.NativePath("/missing.zip")); change.Effect != core.Unknown || core.CodeOf(err) != core.InvalidUpstreamResponse {
		t.Fatalf("rename without path confirmation was accepted: %+v %v", change, err)
	}
	renamed := core.NativePath("/renamed.zip")
	if change, err := d.Relocate(t.Context(), file, renamed); change.Effect != core.Applied || err != nil || renames != 3 {
		t.Fatalf("exclusive rename was not confirmed: %+v %v", change, err)
	}
	file = renamed
	if change, err := d.Remove(t.Context(), file, core.RemoveOptions{Target: core.RemoveEmptyDirectory}); change.Effect != core.Unchanged || core.CodeOf(err) != core.UnsupportedOperation || recycles != 0 {
		t.Fatalf("directory policy reached recycle: %+v %v", change, err)
	}
	options := core.RemoveOptions{Target: core.RemoveNonDirectory}
	if change, err := d.Remove(t.Context(), file, options); change.Effect != core.Unknown || core.CodeOf(err) != core.InvalidUpstreamResponse {
		t.Fatalf("adjacent 64-bit file ID was accepted: %+v %v", change, err)
	}
	receiptID = fileID
	options.Disposition = core.RemoveTrash
	if change, err := d.RemoveWithDisposition(t.Context(), file, options); change.Effect != core.Applied || err != nil || recycles != 2 {
		t.Fatalf("single-file recycle did not confirm the task: %+v %v", change, err)
	}
	if err := errors.Join(read.Body.Close(), d.Close(), d.Close()); err != nil || releases != 1 {
		t.Fatalf("unexpected release %d: %v", releases, err)
	}
}

func TestWebsiteRejectsAccountChangeAndLoginExpiry(t *testing.T) {
	account := strings.Repeat("a", 64)
	options := WebsiteOptions{Account: account, Release: func() error {
		t.Fatal("failed open attempted to release the caller's browser")
		return nil
	}}
	for _, tc := range []struct {
		reply websiteResponse
		cause error
	}{
		{websiteResponse{Account: strings.Repeat("b", 64)}, ErrWebsiteBindingMismatch},
		{websiteResponse{Error: "InvalidSessionKey"}, ErrWebsiteSignIn},
	} {
		options.Session = NewBrowserSession(func(context.Context, string, json.RawMessage) (json.RawMessage, error) {
			return json.Marshal(tc.reply)
		})
		if _, err := OpenWebsite(t.Context(), options); !errors.Is(err, tc.cause) {
			t.Fatalf("open failure %v, want %v", err, tc.cause)
		}
	}
}
