package alipan_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	core "github.com/nostalume/mountweave"
	driver "github.com/nostalume/mountweave/drivers/alipan"
	"github.com/nostalume/mountweave/drivers/alipan/credential"
	"github.com/nostalume/mountweave/drivers/local"
)

type transport func(*http.Request) (*http.Response, error)

func (f transport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
func response(r *http.Request, status int, body string) *http.Response {
	return &http.Response{StatusCode: status, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body)), ContentLength: int64(len(body)), Request: r}
}
func file(id, name string) map[string]any {
	return map[string]any{"drive_id": "drive", "file_id": id, "parent_file_id": "root", "name": name, "type": "file", "size": 99}
}
func jsonResponse(r *http.Request, value any) *http.Response {
	data, _ := json.Marshal(value)
	return response(r, 200, string(data))
}
func prepared(t *testing.T, rt transport, configure func(*driver.Options)) (*driver.Driver, *core.Core) {
	t.Helper()
	tokens, err := credential.New(credential.Record{Token: credential.Token{Access: "provider-secret", Refresh: "refresh", ExpiresAt: time.Now().Add(time.Hour)}}, func(context.Context, string) (credential.Token, error) {
		t.Error("unexpected refresh")
		return credential.Token{}, errors.New("unexpected refresh")
	}, func(_ context.Context, record credential.Record) error {
		if record.Pending {
			return nil
		}
		t.Error("unexpected persistence")
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	options := driver.Options{Client: &http.Client{Transport: rt}, DriveID: "drive", RootID: "root", Credentials: tokens}
	if configure != nil {
		configure(&options)
	}
	d, err := driver.New(options)
	if err != nil {
		_ = tokens.Close()
		t.Fatal(err)
	}
	c, err := core.New(core.Options{Authorize: func(context.Context, core.Principal, core.Action, core.Address) error {
		return nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = c.Install("/cloud", d); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := c.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	return d, c
}
func requestBody(t *testing.T, r *http.Request) map[string]any {
	t.Helper()
	var body map[string]any
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	return body
}
func requireCode(t *testing.T, err error, want core.Code) {
	t.Helper()
	if err == nil || core.CodeOf(err) != want {
		t.Fatalf("error %v, want %s", err, want)
	}
}
func TestIDReadWorkflowThroughCore(t *testing.T) {
	directory := t.TempDir()
	var pages, links, downloads atomic.Int32
	rt := transport(func(r *http.Request) (*http.Response, error) {
		if r.URL.Host == "download.example.test" {
			downloads.Add(1)
			if r.Header.Get("Authorization") != "" {
				t.Error("provider bearer leaked to download origin")
			}
			if r.Header.Get("Range") == "" {
				return response(r, 200, "abcdefghij"), nil
			}
			if r.Header.Get("Range") != "bytes=2-5" {
				t.Error("range not forwarded")
			}
			result := response(r, 206, "cdef")
			result.Header.Set("Content-Range", "bytes 2-5/10")
			return result, nil
		}
		if r.URL.Host != "openapi.alipan.com" || r.Header.Get("Authorization") != "Bearer provider-secret" {
			t.Error("wrong API authority")
		}
		body := requestBody(t, r)
		if body["drive_id"] != "drive" {
			t.Error("wrong prepared drive")
		}
		switch r.URL.Path {
		case "/adrive/v1.0/openFile/list":
			pages.Add(1)
			if body["parent_file_id"] == "folder-id" {
				return jsonResponse(r, map[string]any{"items": []any{}}), nil
			}
			if body["parent_file_id"] != "root" {
				t.Error("wrong parent")
			}
			if body["marker"] == "" {
				return jsonResponse(r, map[string]any{"items": []any{file("file-id", "song")}, "next_marker": "page2"}), nil
			}
			folder := file("folder-id", "folder")
			folder["type"] = "folder"
			return jsonResponse(r, map[string]any{"items": []any{folder}}), nil
		case "/adrive/v1.0/openFile/getDownloadUrl":
			links.Add(1)
			if body["file_id"] != "file-id" {
				t.Error("native ID not retained privately")
			}
			return jsonResponse(r, map[string]any{"url": "https://download.example.test/file?private=signature", "method": "GET"}), nil
		}
		t.Errorf("unexpected operation %s", r.URL.Path)
		return response(r, 500, ""), nil
	})
	d, c := prepared(t, rt, nil)
	ctx := context.Background()
	entry, err := c.Stat(ctx, "reader", core.Path("/cloud/song"))
	if err != nil || entry.Size == nil || *entry.Size != 99 || pages.Load() != 2 {
		t.Fatalf("stat: %#v %v", entry, err)
	}
	first, err := c.List(ctx, "reader", core.ListRequest{Query: core.Children(core.Path("/cloud")), Limit: 1, CursorTTL: time.Minute})
	if err != nil || len(first.Items) != 1 || first.NextCursor == nil {
		t.Fatal(first, err)
	}
	req := core.ListRequest{Query: core.Children(core.Path("/cloud")), Cursor: *first.NextCursor}
	second, err := c.List(ctx, "reader", req)
	if err != nil || len(second.Items) != 1 || second.Items[0].Kind != core.Directory || second.Items[0].Size != nil {
		t.Fatal(second, err)
	}
	before := pages.Load()
	if _, err = c.List(ctx, "reader", req); err != nil || pages.Load() != before {
		t.Fatal("replay advanced native state", err)
	}
	opened, err := c.Open(ctx, "reader", core.Path("/cloud/song"), core.OpenOptions{Range: &core.Range{Start: 2, End: 5}})
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(opened.Body)
	if err != nil || string(data) != "cdef" || opened.Range == nil || *opened.Entry.Size != 10 {
		t.Fatalf("opened facts: %q %#v %v", data, opened, err)
	}
	if pages.Load()-before != 2 || links.Load() != 1 || downloads.Load() != 1 {
		t.Fatal("redundant path resolution or download")
	}
	scope, err := d.OpenDirectory(ctx, core.NativePath("/"))
	if err != nil {
		t.Fatal(err)
	}
	defer scope.Close()
	for {
		_, err = scope.Next(ctx, 1)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	before = pages.Load()
	for i := 0; i < 2; i++ {
		opened, err := scope.OpenFile(ctx, "song")
		if err != nil {
			t.Fatal(err)
		}
		data, readErr := io.ReadAll(opened.Body)
		closeErr := opened.Body.Close()
		if readErr != nil || closeErr != nil || string(data) != "abcdefghij" {
			t.Fatal(string(data), readErr, closeErr)
		}
	}
	if pages.Load() != before {
		t.Fatal("scoped file reads repeated path resolution")
	}
	output, err := local.New(directory)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Install("/local", output); err != nil {
		t.Fatal(err)
	}
	rows, err := c.CopyTree(ctx, "reader", core.Path("/cloud"), core.Path("/local/copied"), core.TreeCopyOptions{MaxEntries: 20, MaxPlanBytes: 1024})
	if err != nil || len(rows.Items) != 3 {
		t.Fatal(rows, err)
	}
	data, err = os.ReadFile(filepath.Join(directory, "copied", "song"))
	if err != nil || string(data) != "abcdefghij" {
		t.Fatal(string(data), err)
	}

}

func TestResolutionNeverInventsUniquenessOrAbsence(t *testing.T) {
	for _, scenario := range []string{"duplicate", "incomplete", "budget", "missing", "repeated-marker", "wrong-drive", "invalid-name", "missing-items"} {
		t.Run(scenario, func(t *testing.T) {
			_, c := prepared(t, func(r *http.Request) (*http.Response, error) {
				body := requestBody(t, r)
				marker := body["marker"]
				candidate := file("one", "song")
				switch scenario {
				case "wrong-drive":
					candidate["drive_id"] = "foreign"
				case "invalid-name":
					candidate["name"] = "bad/name"
				case "missing":
					candidate["name"] = "other"
				case "missing-items":
					return jsonResponse(r, map[string]any{"next_marker": ""}), nil
				}
				if marker == "" {
					return jsonResponse(r, map[string]any{"items": []any{candidate}, "next_marker": "next"}), nil
				}
				switch scenario {
				case "duplicate":
					return jsonResponse(r, map[string]any{"items": []any{file("two", "song")}}), nil
				case "incomplete":
					return response(r, 503, "{}"), nil
				case "repeated-marker":
					return jsonResponse(r, map[string]any{"items": []any{}, "next_marker": "next"}), nil
				}
				return jsonResponse(r, map[string]any{"items": []any{}}), nil
			}, func(o *driver.Options) {
				if scenario == "budget" {
					o.MaxResolvePages = 1
				}
			})
			_, err := c.Stat(context.Background(), "reader", core.Path("/cloud/song"))
			want := map[string]core.Code{"duplicate": core.PathAmbiguous, "incomplete": core.UpstreamFailure, "budget": core.ResolutionBudgetExceeded, "missing": core.NotFound, "repeated-marker": core.InvalidUpstreamResponse, "wrong-drive": core.InvalidUpstreamResponse, "invalid-name": core.UnrepresentableName, "missing-items": core.InvalidUpstreamResponse}[scenario]
			requireCode(t, err, want)
		})
	}
}

type trackedBody struct {
	io.Reader
	closed atomic.Bool
}

func (b *trackedBody) Close() error { b.closed.Store(true); return nil }
func TestDownloadRangeFallbackAndFailureOwnership(t *testing.T) {
	for _, scenario := range []string{"fallback", "unknown", "bad-range", "missing-range", "redirect", "expired", "short", "cancel", "unsafe-url"} {
		t.Run(scenario, func(t *testing.T) {
			body := &trackedBody{Reader: strings.NewReader("abc")}
			_, c := prepared(t, func(r *http.Request) (*http.Response, error) {
				if strings.HasSuffix(r.URL.Path, "/list") {
					return jsonResponse(r, map[string]any{"items": []any{file("id", "song")}}), nil
				}
				if strings.HasSuffix(r.URL.Path, "/getDownloadUrl") {
					target := "https://download.example.test/file"
					if scenario == "unsafe-url" {
						target = "http://download.example.test/file"
					}
					return jsonResponse(r, map[string]any{"url": target, "method": "GET"}), nil
				}
				if r.Header.Get("Authorization") != "" {
					t.Error("bearer leaked")
				}
				result := response(r, 200, "abc")
				result.Body = body
				switch scenario {
				case "unknown":
					result.ContentLength = -1
				case "short":
					result.ContentLength = 8
				case "bad-range":
					result.StatusCode = 206
					result.Header.Set("Content-Range", "bytes 0-2/10")
				case "missing-range":
					result.StatusCode = 206
				case "expired":
					result.StatusCode = 403
				case "redirect":
					result.StatusCode = 302
					result.Header.Set("Location", "https://other.example.test/file")
				}
				return result, nil
			}, nil)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			opened, err := c.Open(ctx, "reader", core.Path("/cloud/song"), core.OpenOptions{Range: &core.Range{Start: 1, End: 2}})
			switch scenario {
			case "bad-range", "missing-range", "unsafe-url":
				requireCode(t, err, core.InvalidUpstreamResponse)
				if scenario != "unsafe-url" && !body.closed.Load() {
					t.Fatal("rejected body leaked")
				}
				return
			case "expired":
				requireCode(t, err, core.UpstreamAccessDenied)
				if !body.closed.Load() {
					t.Fatal("error body leaked")
				}
				return
			case "redirect":
				requireCode(t, err, core.UpstreamFailure)
				if !body.closed.Load() {
					t.Fatal("redirect body leaked")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if opened.Range != nil {
				t.Fatal("invented partial response")
			}
			if scenario == "unknown" && (opened.Length != nil || opened.Entry.Size != nil) {
				t.Fatal("invented length")
			}
			if scenario == "cancel" {
				cancel()
			}
			_, err = io.ReadAll(opened.Body)
			if scenario == "short" && !errors.Is(err, io.ErrUnexpectedEOF) {
				t.Fatal(err)
			}
			if scenario == "cancel" && !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
			if scenario != "short" && scenario != "cancel" && err != nil {
				t.Fatal(err)
			}
			if !body.closed.Load() {
				t.Fatal("body not released")
			}
		})
	}
}
func TestCancellationReachesNativeRequest(t *testing.T) {
	started := make(chan struct{})
	_, c := prepared(t, func(r *http.Request) (*http.Response, error) {
		close(started)
		<-r.Context().Done()
		return nil, r.Context().Err()
	}, nil)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { _, err := c.Stat(ctx, "reader", core.Path("/cloud/song")); done <- err }()
	<-started
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("native request ignored cancellation")
	}
}
func TestOAuthRefreshAndOneReplay(t *testing.T) {
	var exchanges, saves, requests atomic.Int32
	var client *http.Client
	client = &http.Client{Transport: transport(func(r *http.Request) (*http.Response, error) {
		if r.URL.Path == "/oauth/access_token" {
			exchanges.Add(1)
			body := requestBody(t, r)
			if body["grant_type"] != "refresh_token" || body["refresh_token"] != "old-refresh" || r.Header.Get("Authorization") != "" {
				t.Error("invalid OAuth request")
			}
			return jsonResponse(r, map[string]any{"token_type": "Bearer", "access_token": "new-access", "refresh_token": "new-refresh", "expires_in": 3600}), nil
		}
		requests.Add(1)
		if r.Header.Get("Authorization") == "Bearer old-access" {
			return response(r, 401, `{"code":"AccessTokenExpired"}`), nil
		}
		if r.Header.Get("Authorization") != "Bearer new-access" || saves.Load() != 1 {
			t.Error("token used before durable save")
		}
		return jsonResponse(r, map[string]any{"items": []any{file("id", "song")}}), nil
	})}
	oauth := driver.OAuth{Client: client, ClientID: "test-app", ClientSecret: "test-secret"}
	source, err := credential.New(credential.Record{Token: credential.Token{Access: "old-access", Refresh: "old-refresh", ExpiresAt: time.Now().Add(time.Hour)}}, oauth.Refresh, func(_ context.Context, record credential.Record) error {
		if record.Pending {
			return nil
		}
		token := record.Token

		if token.Refresh != "new-refresh" {
			t.Error("wrong saved token")
		}
		saves.Add(1)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	d, err := driver.New(driver.Options{Client: client, Credentials: source, DriveID: "drive", RootID: "root"})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := d.Close(); err != nil {
			t.Error(err)
		}
	}()
	if _, err = d.Stat(context.Background(), core.NativePath("/song")); err != nil {
		t.Fatal(err)
	}
	if exchanges.Load() != 1 || saves.Load() != 1 || requests.Load() != 2 {
		t.Fatal("unexpected refresh or read retry")
	}
}
func TestResponseBudgetAndNativeThrottle(t *testing.T) {
	for _, scenario := range []string{"exact-budget", "over-budget", "throttled"} {
		t.Run(scenario, func(t *testing.T) {
			payload := `{"items":[]}`
			_, c := prepared(t, func(r *http.Request) (*http.Response, error) {
				if scenario == "throttled" {
					return response(r, 429, `{"code":"TooManyRequests"}`), nil
				}
				return response(r, 200, payload), nil
			}, func(o *driver.Options) {
				if scenario == "exact-budget" {
					o.MaxResponseBytes = int64(len(payload))
				}
				if scenario == "over-budget" {
					o.MaxResponseBytes = int64(len(payload) - 1)
				}
			})
			_, err := c.List(context.Background(), "reader", core.ListRequest{Query: core.Children(core.Path("/cloud")), CursorTTL: time.Minute})
			switch scenario {
			case "exact-budget":
				if err != nil {
					t.Fatal(err)
				}
			case "over-budget":
				requireCode(t, err, core.ResponseBudgetExceeded)
			case "throttled":
				requireCode(t, err, core.UpstreamThrottled)
			}
		})
	}
}
