package quark

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	core "github.com/nostalume/mountweave"
)

func TestWebsiteRead(t *testing.T) {
	const signed = "https://download.quark.cn/content?signature=one%3Ftwo"
	account := strings.Repeat("a", 64)
	rows := make([]websiteItem, websitePageSize)
	dir, file, size := true, true, json.Number("6")
	for i := range rows {
		id := fmt.Sprintf("dir-%d", i)
		rows[i] = websiteItem{FID: id, Parent: "0", Name: id, Dir: &dir}
	}
	var accountReply, grantReply, startReply string
	pageOverrides := make(map[int]string)
	call := func(_ context.Context, script string, args json.RawMessage) (json.RawMessage, error) {
		switch script {
		case websiteReadScript:
			var request websiteRequest
			if err := json.Unmarshal(args, &request); err != nil {
				return nil, err
			}
			switch {
			case request.FID != "":
				if request != (websiteRequest{Account: account, FID: "selected"}) {
					t.Fatalf("unexpected grant request: %+v", request)
				}
				if grantReply != "" {
					return json.RawMessage(grantReply), nil
				}
				return json.RawMessage(`{"http":200,"account":"` + account + `","code":0,"grants":[{"fid":"selected","size":"6","download_url":"` + signed + `"}]}`), nil
			case request.Size == 1:
				if request.Parent != "0" || request.Page != 1 || request.Account != "" && request.Account != account {
					t.Fatalf("unexpected account request: %+v", request)
				}
				if accountReply != "" {
					return json.RawMessage(accountReply), nil
				}
				return json.RawMessage(`{"http":200,"account":"` + account + `","code":0,"page":1,"total":51,"items":[{}]}`), nil
			}
			if request.Parent != "0" || request.Size != websitePageSize || request.Account != account {
				t.Fatalf("unexpected list request: %+v", request)
			}
			if override := pageOverrides[request.Page]; override != "" {
				return json.RawMessage(override), nil
			}
			code, total := 0, websitePageSize+1
			reply := websiteReply{Account: account, HTTP: 200, Code: &code, Page: request.Page, Total: &total, Items: rows}
			if request.Page == 2 {
				reply.Items = []websiteItem{{FID: "selected", Parent: "0", Name: "original.txt", File: &file, Size: &size}}
			}
			return json.Marshal(reply)
		case websiteStartDownload:
			var request map[string]string
			if err := json.Unmarshal(args, &request); err != nil {
				return nil, err
			}
			if request["url"] != signed || request["account"] != account || request["range"] != "bytes=1-4" {
				t.Fatalf("unexpected browser start parameters: %s", args)
			}
			if startReply != "" {
				return json.RawMessage(startReply), nil
			}
			return json.RawMessage(`{"origin":"` + WebsiteOrigin + `","cross":true}`), nil
		case websiteStopDownload:
			return json.RawMessage(`{}`), nil
		default:
			t.Fatalf("unexpected browser script")
			return nil, nil
		}
	}
	var invalidRange bool
	transfer := func(ctx context.Context, prepare func(context.Context) (WebsiteDownload, error)) (io.ReadCloser, error) {
		download, err := prepare(ctx)
		if err != nil {
			return nil, err
		}
		if download.URL != signed {
			t.Fatalf("download URL was not preserved: %q", download.URL)
		}
		defer func() {
			if err := download.Stop(ctx); err != nil {
				t.Error(err)
			}
		}()
		if err := download.Start(ctx); err != nil {
			return nil, err
		}
		headers := http.Header{}
		headers.Set("Access-Control-Allow-Origin", WebsiteOrigin)
		headers.Set("Access-Control-Allow-Credentials", "true")
		headers.Set("Content-Range", "bytes 1-4/6")
		if invalidRange {
			headers.Set("Content-Range", "bytes 0-3/6")
		}
		length, checkEOF, err := download.Check(http.StatusPartialContent, headers)
		if err != nil {
			return nil, err
		}
		if length != 4 || !checkEOF {
			t.Fatalf("unexpected body contract: length %d, check EOF %t", length, checkEOF)
		}
		return io.NopCloser(strings.NewReader("ampl")), nil
	}
	ctx := t.Context()
	if observed, err := WebsiteAccount(ctx, call); err != nil || observed != account {
		t.Fatalf("unexpected verified account: %q, %v", observed, err)
	}
	releases := 0
	session := WebsiteOptions{Call: call, Transfer: transfer, Account: account, RootPath: "/", Release: func() error {
		releases++
		return nil
	}}
	bound := session
	bound.RootPath, bound.RootID = "/dir-0", "old-folder"
	if failed, err := OpenWebsite(ctx, bound); failed != nil || !errors.Is(err, ErrWebsiteBindingMismatch) || releases != 0 {
		t.Fatalf("selected path no longer refers to the saved folder: %v, %v", failed, err)
	}
	pageOverrides[1] = `{"http":403}`
	if failed, err := OpenWebsite(ctx, session); failed != nil || core.CodeOf(err) != core.UpstreamAccessDenied || releases != 0 {
		t.Fatalf("root access was not verified before the session changed owners: %v, %v", failed, err)
	}
	delete(pageOverrides, 1)
	d, err := OpenWebsite(ctx, session)
	if err != nil || d.RootID() != "0" {
		t.Fatalf("unexpected website mount: %v, %v", d, err)
	}
	t.Cleanup(func() { _ = d.Close() })
	address := core.NativePath("/original.txt")
	opts := core.OpenOptions{Range: &core.Range{Start: 1, End: 4}}
	opened, err := d.Open(ctx, address, opts)
	if err != nil || opened.Body == nil || opened.Length == nil || *opened.Length != 4 ||
		opened.Range == nil || *opened.Range != (core.ContentRange{Start: 1, End: 4, Total: 6}) ||
		opened.Metadata.Size == nil || *opened.Metadata.Size != 6 || opened.Metadata.Name != "original.txt" {
		t.Fatalf("unexpected download: %+v, %v", opened, err)
	}
	data, err := io.ReadAll(opened.Body)
	if err := errors.Join(err, opened.Body.Close()); err != nil || string(data) != "ampl" {
		t.Fatalf("unexpected file body: %q, %v", data, err)
	}
	for _, row := range []string{
		`{"fid":"selected","dir":true,"file":true}`,
		`{"fid":"selected","file":true,"size":"9223372036854775808"}`,
	} {
		pageOverrides[1] = `{"http":200,"account":"` + account + `","code":0,"page":1,"total":1,"items":[` + row + `]}`
		if _, err := d.Stat(ctx, address); core.CodeOf(err) != core.InvalidUpstreamResponse {
			t.Fatalf("accepted a malformed directory entry: %v", err)
		}
	}
	delete(pageOverrides, 1)
	for _, page := range []string{
		`"page":2,"total":50,"items":[]`,
		`"page":2,"total":51,"items":[]`,
		`"page":1,"total":51,"items":[{"fid":"selected","file":true,"size":6,"file_name":"original.txt"}]`,
		`"page":2,"total":51,"items":[{"fid":"dir-0","dir":true,"file_name":"dir-0"}]`,
	} {
		pageOverrides[2] = `{"http":200,"account":"` + account + `","code":0,` + page + `}`
		if _, err := d.Stat(ctx, address); core.CodeOf(err) != core.InvalidUpstreamResponse {
			t.Fatalf("accepted invalid pagination: %v", err)
		}
	}
	delete(pageOverrides, 2)
	for _, receipt := range []string{
		`{"fid":"different","size":6,"download_url":"` + signed + `"}`,
		`{"fid":"selected","size":5,"download_url":"` + signed + `"}`,
		`{"fid":"selected","size":6,"download_url":"https://127.0.0.1/content"}`,
		`{"fid":"selected","size":6,"download_url":"https://quark.cn.example/content"}`,
	} {
		grantReply = `{"http":200,"code":0,"account":"` + account + `","grants":[` + receipt + `]}`
		if opened, err := d.Open(ctx, address, opts); opened.Body != nil || core.CodeOf(err) != core.InvalidUpstreamResponse {
			t.Fatalf("accepted a malformed grant: %v", err)
		}
	}
	grantReply = ""
	startReply = `{"conflict":true}`
	if opened, err := d.Open(ctx, address, opts); opened.Body != nil || !errors.Is(err, ErrWebsiteBindingMismatch) {
		t.Fatalf("did not reject an account change at download start: %v", err)
	}
	startReply = ""
	invalidRange = true
	if opened, err := d.Open(ctx, address, opts); opened.Body != nil || core.CodeOf(err) != core.InvalidUpstreamResponse {
		t.Fatalf("accepted the wrong range receipt: %v", err)
	}
	accountReply = `{"http":401,"code":31001}`
	if _, err := d.Stat(ctx, core.NativePath("/")); !errors.Is(err, ErrWebsiteSignIn) {
		t.Fatalf("did not distinguish expired sign-in: %v", err)
	}
	if err := errors.Join(d.Close(), d.Close()); err != nil || releases != 1 {
		t.Fatalf("session releases: %d, %v", releases, err)
	}
}
