package quark

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/internal/httpcontent"
)

type roundTrip func(*http.Request) (*http.Response, error)

func (f roundTrip) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type trackedBody struct {
	io.ReadCloser
	closed bool
}

func (b *trackedBody) Close() error {
	b.closed = true
	return b.ReadCloser.Close()
}

func TestOpenDrive(t *testing.T) {
	ctx := t.Context()
	const contentOrigin = "https://content.fixture:8443"
	const principal = core.Principal("fixture")
	const clientID, signKey, applicationToken = "fixture-app", "fixture-signing-key", "fixture-app-token"
	grantURL, grantFID := contentOrigin+"/blob?ticket=data", "file-id"
	token, grantToken := "fixture-account-1", ""
	requestIDs := make(map[string]bool)
	var redirect, cyclic bool
	var cancelDownload context.CancelFunc
	var canceledBody trackedBody
	apiCalls, contentCalls := 0, 0
	api := &http.Client{Transport: roundTrip(func(r *http.Request) (*http.Response, error) {
		apiCalls++
		defer r.Body.Close()
		if r.Method != http.MethodPost || r.URL.Scheme != "https" || r.URL.Host != "open-api-drive.quark.cn" ||
			r.Header.Get("Cookie") != "" || r.Header.Get("Content-Type") != "application/json" {
			t.Fatalf("unexpected control request %s %s", r.Method, r.URL.Path)
		}
		query := r.URL.Query()
		requestID := query.Get("req_id")
		timestamp := r.Header.Get("X-Pan-Tm")
		_, stampErr := strconv.ParseInt(timestamp, 10, 64)
		signature := fmt.Sprintf("%x", sha256.Sum256([]byte("POST&"+r.URL.Path+"&"+timestamp+"&"+signKey)))
		if len(requestID) != 36 || requestIDs[requestID] || query.Get("access_token") != token ||
			stampErr != nil || r.Header.Get("X-Pan-Client-Id") != clientID ||
			r.Header.Get("X-Pan-Token") != signature || r.Header.Get("Authorization") != "Bearer "+token ||
			r.Header.Get("X-Pan-Client-Token") != applicationToken || r.Header.Get("X-Client-Token") != applicationToken {
			t.Fatal("control request did not match the Quark authorization contract")
		}
		requestIDs[requestID] = true
		var req struct {
			FID    string  `json:"fid"`
			Parent string  `json:"parent_fid"`
			Size   int     `json:"size"`
			Sort   string  `json:"sort"`
			Cursor *cursor `json:"query_cursor"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatal(err)
		}
		rec := httptest.NewRecorder()
		var data string
		switch APIOrigin + r.URL.Path {
		case fileAPI + "list":
			if req.Sort != "updated_at:desc" || req.Size < 1 || req.Size > 100 {
				t.Fatalf("bad page request %+v", req)
			}
			switch {
			case req.Parent == "0" && req.Cursor == nil:
				data = `{"last_page":true,"file_list":[{"fid":"folder-id","parent_fid":"0","filename":"docs","file_type":"0"}]}`
			case req.Parent == "folder-id" && (req.Cursor == nil || cyclic):
				data = `{"last_page":false,"next_query_cursor":{"version":"native","token":"page-2"},"file_list":null}`
			case req.Parent == "folder-id" && *req.Cursor == (cursor{Version: "native", Token: "page-2"}):
				data = `{"last_page":true,"file_list":[{"fid":"file-id","parent_fid":"folder-id","filename":"a.txt","file_type":"1","size":5}]}`
			default:
				t.Fatalf("unexpected native page %+v", req)
			}
		case fileAPI + "get_download_url":
			if req.FID != "file-id" {
				t.Fatalf("grant requested for %q", req.FID)
			}
			grantToken, token = token, "fixture-account-2"
			data = fmt.Sprintf(`{"fid":%q,"file_name":"a.txt","download_url":%q,"size":"5"}`, grantFID, grantURL)
		default:
			t.Fatalf("unexpected control path %s", r.URL.Path)
		}
		fmt.Fprintf(rec, `{"status":0,"errno":0,"data":%s}`, data)
		return rec.Result(), nil
	})}
	content := &http.Client{Transport: roundTrip(func(r *http.Request) (*http.Response, error) {
		contentCalls++
		if r.Method != http.MethodGet || r.URL.Host != "content.fixture:8443" ||
			r.URL.Path != "/blob" || r.Header.Get("Authorization") != "" ||
			r.Header.Get("Accept-Encoding") != "identity" || r.Header.Get("Range") != "bytes=1-3" {
			t.Fatalf("invalid body request %s %s", r.Method, r.URL.Path)
		}
		for name, wanted := range map[string]string{
			"x_pan_client_id": clientID, "x_pan_access_token": grantToken, "x_pan_client_token": applicationToken,
		} {
			cookie, err := r.Cookie(name)
			if err != nil || cookie.Value != wanted {
				t.Fatalf("invalid download credential for %s", name)
			}
		}
		if r.Header.Get("X-Pan-Token") != "" || r.URL.Query().Get("access_token") != "" {
			t.Fatal("control credentials reached the content request")
		}
		rec := httptest.NewRecorder()
		if redirect {
			rec.Header().Set("Location", "https://foreign.fixture/object")
			rec.WriteHeader(http.StatusFound)
		} else {
			rec.Header().Set("Content-Range", "bytes 1-3/5")
			rec.Header().Set("Content-Length", "3")
			rec.WriteHeader(http.StatusPartialContent)
			io.WriteString(rec, "ell")
		}
		res := rec.Result()
		if cancelDownload != nil {
			cancelDownload()
			canceledBody.ReadCloser = res.Body
			res.Body = &canceledBody
		}
		return res, nil
	})}
	d, err := Open(ctx, Options{
		Application: &Application{
			ClientID: clientID, SignKey: signKey, ClientToken: applicationToken,
			AccessToken: func(context.Context) (string, error) { return token, nil },
		},
		Client: api, ContentClient: content, ContentOrigin: contentOrigin, RootPath: "/docs",
	})
	if d != nil {
		defer d.Close()
	}
	if err != nil {
		t.Fatal(err)
	}
	host, err := core.New(core.Options{Authorize: func(context.Context, core.Principal, core.Action, core.Address) error { return nil }})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := host.Close(ctx); err != nil {
			t.Error(err)
		}
	}()
	if _, err := host.Install("/drive", d); err != nil {
		t.Fatal(err)
	}
	query := core.Children(core.Path("/drive"))
	first, err := host.List(ctx, principal, core.ListRequest{Query: query, Limit: 5, CursorTTL: time.Minute})
	if err != nil || len(first.Items) != 0 || first.NextCursor == nil {
		t.Fatalf("empty first page: %+v, %v", first, err)
	}
	second, err := host.List(ctx, principal, core.ListRequest{Query: query, Limit: 5, Cursor: *first.NextCursor})
	if err != nil || len(second.Items) != 1 || second.Items[0].Name != "a.txt" || second.NextCursor != nil {
		t.Fatalf("second page: %+v, %v", second, err)
	}
	sample := second.Items[0].Address
	options := core.OpenOptions{Range: &core.Range{Start: 1, End: 3}}
	opened, err := host.Open(ctx, principal, sample, options)
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(opened.Body)
	if err := errors.Join(err, opened.Body.Close()); err != nil || string(body) != "ell" || opened.Range == nil || opened.Range.Total != 5 {
		t.Fatalf("ranged open: %q, %+v, %v", body, opened.Range, err)
	}
	for _, badURL := range []string{"https://foreign.fixture/blob", "https://content.fixture/blob", "https://user@content.fixture:8443/blob"} {
		grantURL = badURL
		if _, err := host.Open(ctx, principal, sample, options); core.CodeOf(err) != core.InvalidUpstreamResponse || contentCalls != 1 {
			t.Fatalf("untrusted grant crossed the credential boundary: %v", err)
		}
	}
	grantURL, grantFID = contentOrigin+"/blob", "different-file"
	if _, err := host.Open(ctx, principal, sample, options); core.CodeOf(err) != core.InvalidUpstreamResponse || contentCalls != 1 {
		t.Fatalf("unrelated file grant reached body transport: %v", err)
	}
	grantFID, redirect = "file-id", true
	if _, err := host.Open(ctx, principal, sample, options); !errors.Is(err, httpcontent.ErrContentRedirect) || contentCalls != 2 {
		t.Fatalf("body redirect was followed or misreported: %v", err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	before := apiCalls
	if _, err := host.Open(canceled, principal, sample, options); !errors.Is(err, context.Canceled) || apiCalls != before {
		t.Fatalf("canceled read contacted the control API: %v", err)
	}
	redirect = false
	downloadCtx, stop := context.WithCancel(ctx)
	defer stop()
	cancelDownload = stop
	interrupted, err := d.Open(downloadCtx, core.NativePath("/a.txt"), options)
	if !errors.Is(err, context.Canceled) || interrupted.Body != nil || !canceledBody.closed {
		t.Fatalf("body returned at cancellation was not closed: %v", err)
	}
	cyclic = true
	if _, err := host.Stat(ctx, principal, sample); core.CodeOf(err) != core.InvalidUpstreamResponse {
		t.Fatalf("cyclic cursor not rejected: %v", err)
	}
}
