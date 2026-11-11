package pdsshare

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
)

type transport func(*http.Request) (*http.Response, error)

func (f transport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type failedClose struct {
	io.ReadCloser
	calls int
	cause error
	stop  context.CancelFunc
}

func (b *failedClose) Close() error {
	b.calls++
	if b.calls == 1 {
		if b.stop != nil {
			b.stop()
		}
		return b.cause
	}
	return b.ReadCloser.Close()
}

func TestShareAndDriveThroughCore(t *testing.T) {
	const (
		shareID    = "one-share"
		driveID    = "one-drive"
		password   = "share-code"
		account    = "Bearer sealed-drive-token"
		name       = "File %+ 中.txt"
		contents   = "abcdef"
		capability = "https://body.example/%2Ffile?cap=one%2b"
	)
	var tokens, lists, downloads, bodies int
	var foreign, redirect, failClose, wrongParent, cycle, unframed bool
	var broken, failedDownload *failedClose
	var stopListing context.CancelFunc
	reply := func(r *http.Request, status int, body string) *http.Response {
		return &http.Response{Request: r, StatusCode: status, Header: make(http.Header),
			Body: io.NopCloser(strings.NewReader(body)), ContentLength: int64(len(body))}
	}
	control := &http.Client{Transport: transport(func(r *http.Request) (*http.Response, error) {
		if r.Method != http.MethodPost || r.URL.Scheme != "https" || r.URL.Host != "tenant.api.aliyunpds.com" ||
			r.Header.Get("Cookie") != "" || r.GetBody != nil {
			return nil, errors.New("wrong PDS control recipient or credentials")
		}
		defer r.Body.Close()
		var fields map[string]json.RawMessage
		if err := json.NewDecoder(r.Body).Decode(&fields); err != nil {
			return nil, err
		}
		drive := r.Header.Get("Authorization") == account
		scopeKey, scopeID := "share_id", shareID
		if drive {
			scopeKey, scopeID = "drive_id", driveID
			if fields["share_id"] != nil || r.Header.Get("X-Share-Token") != "" {
				return nil, errors.New("drive request used share authentication")
			}
		} else if fields["drive_id"] != nil || r.Header.Get("Authorization") != "" {
			return nil, errors.New("share request used drive authentication")
		}
		if string(fields[scopeKey]) != fmt.Sprintf("%q", scopeID) {
			return nil, errors.New("request left its selected PDS scope")
		}
		if r.URL.Path == "/v2/share_link/get_share_token" {
			if drive || r.Header.Get("X-Share-Token") != "" || string(fields["share_pwd"]) != `"share-code"` || len(fields) != 2 {
				return nil, errors.New("unexpected credentials for share token exchange")
			}
			tokens++
			return reply(r, 200, fmt.Sprintf(`{"share_token":"token-%d","expire_time":%q}`, tokens,
				time.Now().Add(time.Hour).UTC().Format(time.RFC3339))), nil
		}
		if !drive && r.Header.Get("X-Share-Token") != fmt.Sprintf("token-%d", tokens) || fields["share_pwd"] != nil {
			return nil, errors.New("share token was missing, stale or sent with its password")
		}
		switch r.URL.Path {
		case "/v2/file/list":
			lists++
			var result string
			switch parent, marker := string(fields["parent_file_id"]), string(fields["marker"]); {
			case parent == `"root"` && marker == "":
				nativeParent := "native-owner-parent"
				if drive && !wrongParent {
					nativeParent = "root"
				}
				result = fmt.Sprintf(`{"items":[{%q:%q,"file_id":"folder","parent_file_id":%q,"type":"folder","name":"Archive"}]}`,
					scopeKey, scopeID, nativeParent)
			case parent == `"folder"` && marker == "":
				result = `{"items":[],"next_marker":"page+/%"}`
			case parent == `"folder"` && marker == `"page+/%"` && cycle:
				result = `{"items":[],"next_marker":"another"}`
			case parent == `"folder"` && marker == `"another"` && cycle:
				result = `{"items":[],"next_marker":"page+/%"}`
			case parent == `"folder"` && marker == `"page+/%"`:
				observed := scopeID
				if foreign {
					observed = "another-scope"
				}
				result = fmt.Sprintf(`{"items":[{%q:%q,"file_id":"body","parent_file_id":"folder","type":"file","name":%q,"size":999}]}`,
					scopeKey, observed, name)
			default:
				return nil, errors.New("wrong parent or marker")
			}
			resp := reply(r, 200, result)
			if failClose {
				broken = &failedClose{ReadCloser: resp.Body, cause: io.EOF, stop: stopListing}
				resp.Body = broken
			}
			return resp, nil
		case "/v2/file/get":
			if !drive || string(fields["file_id"]) != `"folder"` || len(fields) != 2 {
				return nil, errors.New("folder verification used a different scope")
			}
			observed := scopeID
			if foreign {
				observed = "another-drive"
			}
			return reply(r, 200, fmt.Sprintf(`{"domain_id":"tenant","drive_id":%q,"file_id":"folder","type":"folder","status":"available"}`, observed)), nil
		case "/v2/file/get_download_url":
			if string(fields["file_id"]) != `"body"` || len(fields) != 2 {
				return nil, errors.New("download was not scoped to the selected file")
			}
			downloads++
			if drive {
				return reply(r, 200, fmt.Sprintf(`{"url":%q,"size":%d}`, capability, len(contents))), nil
			}
			return reply(r, 200, fmt.Sprintf(`{"url":%q}`, capability)), nil
		default:
			return nil, errors.New("unexpected PDS request")
		}
	})}
	content := &http.Client{Transport: transport(func(r *http.Request) (*http.Response, error) {
		if r.Method != http.MethodGet || r.URL.String() != capability || r.Header.Get("X-Share-Token") != "" ||
			r.Header.Get("Authorization") != "" || r.Header.Get("Cookie") != "" || r.Header.Get("Referer") != "" {
			return nil, errors.New("wrong content recipient, signed URL or credentials")
		}
		bodies++
		if redirect {
			resp := reply(r, 302, "")
			resp.Header.Set("Location", "https://foreign.example/")
			failedDownload = &failedClose{ReadCloser: resp.Body, cause: errors.New(capability)}
			resp.Body = failedDownload
			return resp, nil
		}
		if r.Header.Get("Range") == "bytes=1-3" {
			resp := reply(r, 206, contents[1:4])
			resp.Header.Set("Content-Range", "bytes 1-3/6")
			return resp, nil
		}
		resp := reply(r, 200, contents)
		if unframed {
			resp.ContentLength = -1
		}
		return resp, nil
	})}
	ctx := context.Background()
	options := Options{DomainID: "tenant", ShareID: shareID, Password: password, Client: control, ContentClient: content}
	d, err := Open(ctx, options)
	if err != nil {
		if d != nil {
			err = errors.Join(err, d.Close())
		}
		t.Fatal(err)
	}
	if tokens != 1 || lists != 1 {
		t.Fatalf("constructor did not verify the authorized share: token=%d list=%d", tokens, lists)
	}
	c, err := core.New(core.Options{MaxCursors: 4, Authorize: func(context.Context, core.Principal, core.Action, core.Address) error { return nil }})
	if err != nil {
		t.Fatal(errors.Join(err, d.Close()))
	}
	defer func() {
		if err := c.Close(ctx); err != nil {
			t.Error(err)
		}
	}()
	mount, err := c.Install("/share", d)
	if err != nil {
		t.Fatal(errors.Join(err, d.Close()))
	}
	list := core.ListRequest{Query: core.Children(core.Path("/share/Archive")), Limit: 1, CursorTTL: time.Minute}
	page, err := c.List(ctx, "reader", list)
	if err != nil || len(page.Items) != 0 || page.NextCursor == nil {
		t.Fatalf("empty native page lost continuation: %+v %v", page, err)
	}
	list.Cursor = *page.NextCursor
	// The operation holds no active request here. Advance the cached expiry
	// without waiting or making the clock configurable in production.
	d.expiry = time.Now().Add(-time.Minute)
	page, err = c.List(ctx, "reader", list)
	if err != nil || len(page.Items) != 1 || page.Items[0].Name != name || tokens != 2 {
		t.Fatalf("continuation did not preserve its parent across token expiry: %+v %v", page, err)
	}
	before := lists
	if _, err := c.List(ctx, "reader", list); err != nil || lists != before {
		t.Fatalf("public replay reached the native share: %v", err)
	}

	address := core.Path("/share/Archive/" + name)
	foreign = true
	if _, err := c.Open(ctx, "reader", address, core.OpenOptions{}); core.CodeOf(err) != core.InvalidUpstreamResponse || downloads != 0 {
		t.Fatalf("foreign share entry reached download authorization: %v", err)
	}
	foreign = false
	opened, err := c.Open(ctx, "reader", address, core.OpenOptions{Range: &core.Range{Start: 1, End: 3}})
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(opened.Body)
	if err := errors.Join(err, opened.Body.Close()); err != nil || string(data) != contents[1:4] ||
		opened.Range == nil || *opened.Range != (core.ContentRange{Start: 1, End: 3, Total: 6}) {
		t.Fatalf("native range: %q %+v %v", data, opened.Range, err)
	}
	redirect = true
	if _, err := c.Open(ctx, "reader", address, core.OpenOptions{}); core.CodeOf(err) != core.UpstreamFailure ||
		strings.Contains(err.Error(), capability) || bodies != 2 || failedDownload.calls != 1 {
		t.Fatalf("failed download escaped its recipient or close boundary: %v", err)
	}
	listingCtx, stop := context.WithCancel(ctx)
	defer stop()
	failClose, stopListing = true, stop
	if _, err := c.List(listingCtx, "reader", core.ListRequest{Query: core.Children(core.Path("/share")), Limit: 1, CursorTTL: time.Minute}); !errors.Is(err, io.ErrUnexpectedEOF) ||
		!errors.Is(err, context.Canceled) || errors.Is(err, io.EOF) || broken == nil || broken.calls != 1 {
		t.Fatalf("cancellation or the native close failure was lost: %v", err)
	}
	if err := mount.Retire(ctx); err != nil || broken.calls != 2 || failedDownload.calls != 2 {
		t.Fatalf("retirement did not retry response cleanup: %v", err)
	}

	redirect, failClose = false, false
	signed := &http.Client{Transport: transport(func(r *http.Request) (*http.Response, error) {
		req := r.Clone(r.Context())
		req.Header.Set("Authorization", account)
		return control.Transport.RoundTrip(req)
	})}
	driveOptions := DriveOptions{DomainID: "tenant", DriveID: driveID, Client: signed, ContentClient: content}
	wrongParent = true
	rejected, err := OpenDrive(ctx, driveOptions)
	if rejected == nil || core.CodeOf(err) != core.InvalidUpstreamResponse {
		t.Fatal("drive root accepted a row from a different parent", err)
	}
	if err := rejected.Close(); err != nil {
		t.Fatal(err)
	}
	wrongParent = false
	drive, err := OpenDrive(ctx, driveOptions)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Install("/drive", drive); err != nil {
		t.Fatal(errors.Join(err, drive.Close()))
	}
	top, err := c.List(ctx, "reader", core.ListRequest{Query: core.Children(core.Path("/drive")), Limit: 1, CursorTTL: time.Minute})
	if err != nil || len(top.Items) != 1 || top.Items[0].Name != "Archive" {
		t.Fatal(top, err)
	}
	address = core.Path("/drive/Archive/" + name)
	foreign, cycle = true, false
	before = downloads
	if _, err := c.Open(ctx, "reader", address, core.OpenOptions{}); core.CodeOf(err) != core.InvalidUpstreamResponse || downloads != before {
		t.Fatal("a different drive reached download authorization", err)
	}
	foreign, cycle = false, true
	if _, err := c.Open(ctx, "reader", address, core.OpenOptions{}); core.CodeOf(err) != core.InvalidUpstreamResponse || downloads != before {
		t.Fatal("a nonadjacent repeated marker reached download authorization", err)
	}
	cycle = false
	if meta, err := c.Stat(ctx, "reader", address); err != nil || meta.Name != name {
		t.Fatal(meta, err)
	}
	driveOptions.RootID = "folder"
	foreign = true
	rejected, err = OpenDrive(ctx, driveOptions)
	if rejected == nil || core.CodeOf(err) != core.InvalidUpstreamResponse {
		t.Fatal("explicit root was admitted from a different drive", err)
	}
	if err := rejected.Close(); err != nil {
		t.Fatal(err)
	}
	foreign = false
	folder, err := OpenDrive(ctx, driveOptions)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Install("/folder", folder); err != nil {
		t.Fatal(errors.Join(err, folder.Close()))
	}
	unframed = true
	opened, err = c.Open(ctx, "reader", core.Path("/folder/"+name), core.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	data, err = io.ReadAll(opened.Body)
	if err = errors.Join(err, opened.Body.Close()); err != nil || string(data) != contents || opened.Range != nil || opened.Length != nil || tokens != 2 {
		t.Fatal("complete original", string(data), err)
	}

}
