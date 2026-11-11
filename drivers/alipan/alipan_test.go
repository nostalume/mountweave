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
func nativeOptions(t *testing.T, rt transport) driver.Options {
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
	return driver.Options{Client: &http.Client{Transport: rt}, DriveID: "drive", RootID: "root", Credentials: tokens}
}
func prepared(t *testing.T, rt transport, configure func(*driver.Options)) (*driver.Driver, *core.Core) {
	t.Helper()
	options := nativeOptions(t, rt)
	if configure != nil {
		configure(&options)
	}
	d, err := driver.Open(context.Background(), options)
	if err != nil {
		if d != nil {
			err = errors.Join(err, d.Close())
		} else {
			err = errors.Join(err, options.Credentials.Close())
		}
		t.Fatal(err)
	}
	c := testCore(t)
	if _, err := c.Install("/cloud", d); err != nil {
		t.Fatal(errors.Join(err, d.Close()))
	}
	return d, c
}

func testCore(t *testing.T) *core.Core {
	t.Helper()
	c, err := core.New(core.Options{Authorize: func(context.Context, core.Principal, core.Action, core.Address) error { return nil }})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := c.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	return c
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
				return jsonResponse(r, map[string]any{"items": []any{file("file-id", "song")}, "next_marker": "empty"}), nil
			}
			if body["marker"] == "empty" {
				return jsonResponse(r, map[string]any{"items": []any{}, "next_marker": "page2"}), nil
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
	if err != nil || entry.Size == nil || *entry.Size != 99 || pages.Load() != 3 {
		t.Fatalf("stat: %#v %v", entry, err)
	}
	first, err := c.List(ctx, "reader", core.ListRequest{Query: core.Children(core.Path("/cloud")), Limit: 1, CursorTTL: time.Minute})
	if err != nil || len(first.Items) != 1 || first.NextCursor == nil {
		t.Fatal(first, err)
	}
	req := core.ListRequest{Query: core.Children(core.Path("/cloud")), Cursor: *first.NextCursor}
	empty, err := c.List(ctx, "reader", req)
	if err != nil || len(empty.Items) != 0 || empty.NextCursor == nil {
		t.Fatal("empty continuing page", empty, err)
	}
	req.Cursor = *empty.NextCursor
	second, err := c.List(ctx, "reader", req)
	if err != nil || len(second.Items) != 1 || second.Items[0].Kind != core.Directory || second.Items[0].Size != nil {
		t.Fatal(second, err)
	}
	before := pages.Load()
	opened, err := c.Open(ctx, "reader", core.Path("/cloud/song"), core.OpenOptions{Range: &core.Range{Start: 2, End: 5}})
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(opened.Body)
	if err != nil || string(data) != "cdef" || opened.Range == nil || *opened.Entry.Size != 10 {
		t.Fatalf("opened facts: %q %#v %v", data, opened, err)
	}
	if pages.Load()-before != 3 || links.Load() != 1 || downloads.Load() != 1 {
		t.Fatal("redundant path resolution or download")
	}
	scope, err := d.OpenDirectory(ctx, core.NativePath("/"))
	if err != nil {
		t.Fatal(err)
	}
	defer scope.Close()
	for index, count := range []int{1, 0, 1} {
		batch, nextErr := scope.Next(ctx, 1)
		if len(batch) != count || (index < 2 && nextErr != nil) || (index == 2 && !errors.Is(nextErr, io.EOF)) {
			t.Fatal("native page facts", index, batch, nextErr)
		}
	}
	before = pages.Load()
	if batch, err := scope.Next(ctx, 1); len(batch) != 0 || !errors.Is(err, io.EOF) || pages.Load() != before {
		t.Fatal("native iterator advanced after EOF", batch, err)
	}
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
	ctx := context.Background()
	for _, tc := range []struct {
		name string
		code core.Code
	}{
		{"duplicate", core.PathAmbiguous},
		{"repeated-id", core.InvalidUpstreamResponse},
		{"wrapped-eof", core.UpstreamFailure},
		{"incomplete", core.UpstreamFailure},
		{"budget", core.ResolutionBudgetExceeded},
		{"missing", core.NotFound},
		{"repeated-marker", core.InvalidUpstreamResponse},
		{"cursor-cycle", core.InvalidUpstreamResponse},
		{"wrong-drive", core.InvalidUpstreamResponse},
		{"invalid-name", core.UnrepresentableName},
		{"invalid-type", core.InvalidUpstreamResponse},
		{"negative-size", core.InvalidUpstreamResponse},
		{"missing-items", core.InvalidUpstreamResponse},
		{"close-failure", core.InternalError},
	} {
		t.Run(tc.name, func(t *testing.T) {
			closeFailure := errors.New("native response close failed")
			cycleCalls := 0
			rt := transport(func(r *http.Request) (*http.Response, error) {
				marker := requestBody(t, r)["marker"]
				candidate := file("one", "song")
				switch tc.name {
				case "cursor-cycle":
					if marker == "" {
						cycleCalls = 0
					}
					cycleCalls++
					if cycleCalls > 4 {
						return response(r, 503, "{}"), nil
					}
					next := "first"
					if marker == "first" {
						next = "second"
					}
					return jsonResponse(r, map[string]any{"items": []any{}, "next_marker": next}), nil
				case "wrong-drive":
					candidate["drive_id"] = "foreign"
				case "invalid-name":
					candidate["name"] = "bad/name"
				case "invalid-type":
					candidate["type"] = "redirect"
				case "negative-size":
					candidate["size"] = -1
				case "missing":
					candidate["name"] = "other"
				case "missing-items":
					return jsonResponse(r, map[string]any{"next_marker": ""}), nil
				case "close-failure":
					result := jsonResponse(r, map[string]any{"items": []any{candidate}})
					result.Body = &trackedBody{Reader: result.Body, closeErr: closeFailure}
					return result, nil
				}
				if marker == "" {
					return jsonResponse(r, map[string]any{"items": []any{candidate}, "next_marker": "next"}), nil
				}
				switch tc.name {
				case "duplicate":
					return jsonResponse(r, map[string]any{"items": []any{file("two", "song")}}), nil
				case "repeated-id":
					return jsonResponse(r, map[string]any{"items": []any{candidate}}), nil
				case "wrapped-eof":
					return nil, errors.Join(closeFailure, io.EOF)
				case "incomplete":
					return response(r, 503, "{}"), nil
				case "repeated-marker":
					return jsonResponse(r, map[string]any{"items": []any{}, "next_marker": "next"}), nil
				}
				return jsonResponse(r, map[string]any{"items": []any{}}), nil
			})
			configure := func(o *driver.Options) {
				if tc.name == "budget" {
					o.MaxResolvePages = 1
				}
			}
			switch tc.name {
			case "duplicate", "budget", "cursor-cycle", "close-failure":
				opts := nativeOptions(t, rt)
				opts.RootID, opts.RootPath = "", "/song"
				configure(&opts)
				d, err := driver.Open(ctx, opts)
				if d == nil {
					_ = opts.Credentials.Close()
					t.Fatal("root failure lost its cleanup owner", err)
				}
				_, statErr := d.Stat(ctx, core.NativePath("/"))
				closeErr := d.Close()
				requireCode(t, err, tc.code)
				if core.CodeOf(statErr) != core.MountUnavailable {
					t.Fatal("failed construction admitted an operation", statErr)
				}
				if _, err := opts.Credentials.Access(ctx); !errors.Is(err, credential.ErrClosed) {
					t.Fatal("failed construction left credentials active", err)
				}
				if tc.name == "close-failure" {
					if !errors.Is(closeErr, closeFailure) {
						t.Fatal("failed response cleanup was lost", closeErr)
					}
					return
				}
				if closeErr != nil {
					t.Fatal(closeErr)
				}
			}
			d, c := prepared(t, rt, configure)
			_, err := c.Stat(ctx, "reader", core.Path("/cloud/song"))
			requireCode(t, err, tc.code)
			switch tc.name {
			case "wrapped-eof":
				checkCause := func(err error) {
					t.Helper()
					if errors.Is(err, io.EOF) || !errors.Is(err, closeFailure) || !errors.Is(err, io.ErrUnexpectedEOF) {
						t.Fatal("failed page became normal EOF or lost cause", err)
					}
				}
				checkCause(err)
				req := core.ListRequest{Query: core.Children(core.Path("/cloud")), Limit: 1, CursorTTL: time.Minute}
				page, err := c.List(ctx, "reader", req)
				if err != nil || page.NextCursor == nil {
					t.Fatal("first native page", page, err)
				}
				req.Cursor = *page.NextCursor
				_, err = c.List(ctx, "reader", req)
				requireCode(t, err, tc.code)
				checkCause(err)
			case "duplicate", "repeated-id", "missing", "cursor-cycle":
				scope, err := d.OpenDirectory(ctx, core.NativePath("/"))
				if err != nil {
					t.Fatal(err)
				}
				defer scope.Close()
				for err == nil {
					_, err = scope.Next(ctx, 100)
				}
				if errors.Is(err, io.EOF) {
					_, err = scope.OpenFile(ctx, "song")
				}
				requireCode(t, err, tc.code)
			}
		})
	}
}

type trackedBody struct {
	io.Reader
	closed   atomic.Bool
	closeErr error
	failOnce bool
}

func (b *trackedBody) Close() error {
	if b.closed.Swap(true) && b.failOnce {
		return nil
	}
	return b.closeErr
}
func TestDownloadRangeFallbackAndFailureOwnership(t *testing.T) {
	failures := map[string]core.Code{
		"bad-range": core.InvalidUpstreamResponse, "missing-range": core.InvalidUpstreamResponse,
		"unsafe-url": core.InvalidUpstreamResponse, "encoded": core.InvalidUpstreamResponse,
		"expired": core.UpstreamAccessDenied, "redirect": core.UpstreamFailure,
	}
	for _, scenario := range []string{"fallback", "unknown", "bad-range", "missing-range", "redirect", "expired", "frozen", "short", "cancel", "unsafe-url", "encoded", "unsatisfied", "invalid-total-close", "private-transport"} {
		t.Run(scenario, func(t *testing.T) {
			body := &trackedBody{Reader: strings.NewReader("abc")}
			var cleanup error
			if scenario == "invalid-total-close" {
				cleanup = errors.New("download close failed")
				body.closeErr = cleanup
			}
			rt := transport(func(r *http.Request) (*http.Response, error) {
				if strings.HasSuffix(r.URL.Path, "/list") {
					entry := file("id", "song")
					switch scenario {
					case "frozen":
						entry["punish_flag"] = 2
					case "fallback":
						entry["punish_flag"] = 3
					}
					return jsonResponse(r, map[string]any{"items": []any{entry}}), nil
				}
				if scenario == "frozen" {
					t.Errorf("frozen file requested %s", r.URL.Path)
					return response(r, 500, ""), nil
				}
				if strings.HasSuffix(r.URL.Path, "/getDownloadUrl") {
					target := "https://download.example.test/file?private=signature"
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
				case "private-transport":
					return nil, context.DeadlineExceeded
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
				case "encoded":
					result.Header.Set("Content-Encoding", "gzip")
				case "unsatisfied":
					result.StatusCode = 416
					result.Header.Set("Content-Range", "bytes */1")
				case "invalid-total-close":
					result.StatusCode = 416
					result.Header.Set("Content-Range", "bytes */1x")
				}
				return result, nil
			})
			d, c := prepared(t, rt, nil)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if scenario == "invalid-total-close" {
				rejected, err := d.Open(ctx, core.NativePath("/song"), core.OpenOptions{Range: &core.Range{Start: 1, End: 2}})
				requireCode(t, err, core.RangeNotSatisfiable)
				var failure *core.Error
				if !errors.As(err, &failure) || failure.Total != nil || rejected.Body != body || body.closed.Load() {
					t.Fatal("refused body ownership or invalid total", rejected, err)
				}
				if err := rejected.Body.Close(); !errors.Is(err, cleanup) || !body.closed.Load() {
					t.Fatal("returned body lost close failure", err)
				}
				return
			}
			opened, err := c.Open(ctx, "reader", core.Path("/cloud/song"), core.OpenOptions{Range: &core.Range{Start: 1, End: 2}})
			if code := failures[scenario]; code != "" {
				requireCode(t, err, code)
				if body.closed.Load() != (scenario != "unsafe-url") {
					t.Fatal("unexpected ownership of rejected download response")
				}
				return
			}
			switch scenario {
			case "frozen":
				requireCode(t, err, core.UpstreamAccessDenied)
				scope, err := d.OpenDirectory(ctx, core.NativePath("/"))
				if err != nil {
					t.Fatal(err)
				}
				defer scope.Close()
				if batch, err := scope.Next(ctx, 1); len(batch) != 1 || !errors.Is(err, io.EOF) {
					t.Fatal(batch, err)
				}
				_, err = scope.OpenFile(ctx, "song")
				requireCode(t, err, core.UpstreamAccessDenied)
				return
			case "private-transport":
				requireCode(t, err, core.UpstreamTimeout)
				if !errors.Is(err, context.DeadlineExceeded) || strings.Contains(err.Error(), "private=signature") {
					t.Fatal("private download transport failure was not isolated", err)
				}
				return
			case "unsatisfied":
				requireCode(t, err, core.RangeNotSatisfiable)
				var failure *core.Error
				if !errors.As(err, &failure) || !body.closed.Load() {
					t.Fatal("range refusal lost response ownership", err)
				}
				if failure.Total == nil || *failure.Total != 1 {
					t.Fatal("range refusal lost total", failure.Total)
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
			var readErr error
			switch scenario {
			case "short":
				readErr = io.ErrUnexpectedEOF
			case "cancel":
				cancel()
				readErr = context.Canceled
			}
			if _, err := io.ReadAll(opened.Body); !errors.Is(err, readErr) {
				t.Fatalf("read returned %v, want %v", err, readErr)
			}
			if !body.closed.Load() {
				t.Fatal("body not released")
			}
		})
	}
}
func TestCancellationReachesNativeRequest(t *testing.T) {
	for _, construction := range []bool{false, true} {
		name := "read"
		if construction {
			name = "construct"
		}
		t.Run(name, func(t *testing.T) {
			started := make(chan struct{})
			rt := transport(func(r *http.Request) (*http.Response, error) {
				close(started)
				<-r.Context().Done()
				return nil, r.Context().Err()
			})
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			if construction {
				opts := nativeOptions(t, rt)
				opts.RootID, opts.RootPath = "", "/docs"
				canceled, stop := context.WithCancel(ctx)
				stop()
				if d, err := driver.Open(canceled, opts); d != nil || !errors.Is(err, context.Canceled) {
					t.Fatal("canceled construction acquired resources", d, err)
				}
				if _, err := opts.Credentials.Access(ctx); err != nil {
					t.Fatal("unacquired credentials were closed", err)
				}
				go func() {
					d, err := driver.Open(ctx, opts)
					if d != nil {
						err = errors.Join(err, d.Close())
					} else {
						err = errors.Join(err, opts.Credentials.Close())
					}
					done <- err
				}()
			} else {
				_, c := prepared(t, rt, nil)
				go func() { _, err := c.Stat(ctx, "reader", core.Path("/cloud/song")); done <- err }()
			}
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
		})
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
