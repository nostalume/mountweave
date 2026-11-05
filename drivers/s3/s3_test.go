package s3_test

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
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/s3"
	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/local"
	driver "github.com/nostalume/mountweave/drivers/s3"
)

type transport func(*http.Request) (*http.Response, error)

func (f transport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
func response(r *http.Request, status int, body string) *http.Response {
	return &http.Response{StatusCode: status, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body)), ContentLength: int64(len(body)), Request: r}
}
func listing(r *http.Request, items, marker string, truncated bool) *http.Response {
	return response(r, 200, fmt.Sprintf("<ListBucketResult><IsTruncated>%t</IsTruncated><EncodingType>url</EncodingType>%s<NextContinuationToken>%s</NextContinuationToken></ListBucketResult>", truncated, items, marker))
}
func prepared(t *testing.T, rt transport, options ...func(*sdk.Options)) (*driver.Driver, *core.Core) {
	return preparedDriver(t, rt, nil, nil, options...)
}
func preparedDriver(t *testing.T, rt transport, storageOptions []driver.Options, authorize core.Authorizer, options ...func(*sdk.Options)) (*driver.Driver, *core.Core) {
	t.Helper()
	client := sdk.New(sdk.Options{Region: "us-east-1", BaseEndpoint: aws.String("https://s3.example.test"), UsePathStyle: true, RetryMaxAttempts: 1, Credentials: aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
		return aws.Credentials{AccessKeyID: "test-key", SecretAccessKey: "test-secret"}, nil
	}), HTTPClient: &http.Client{Transport: rt}}, options...)
	d, err := driver.New(client, "bucket", "vault/", storageOptions...)
	if err != nil {
		t.Fatal(err)
	}
	if authorize == nil {
		authorize = func(context.Context, core.Principal, core.Action, core.Address) error { return nil }
	}
	c, err := core.New(core.Options{Authorize: authorize})
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
func requireCode(t *testing.T, err error, want core.Code) {
	t.Helper()
	if err == nil || core.CodeOf(err) != want {
		t.Fatalf("error %v, want %s", err, want)
	}
}

func TestSDKProtocolThroughCore(t *testing.T) {
	directory := t.TempDir()
	var mu sync.Mutex
	calls := map[string]int{}
	rootPages := 0
	rt := transport(func(r *http.Request) (*http.Response, error) {
		if !strings.HasPrefix(r.Header.Get("Authorization"), "AWS4-HMAC-SHA256 ") {
			t.Error("missing SDK signature")
		}
		if r.URL.Host != "s3.example.test" {
			t.Errorf("host %q", r.URL.Host)
		}
		key := r.Method + " " + r.URL.EscapedPath() + "?" + r.URL.Query().Get("prefix")
		mu.Lock()
		calls[key]++
		mu.Unlock()
		if r.URL.Query().Get("list-type") == "2" {
			if r.URL.Query().Get("prefix") != "vault/" {
				if r.URL.Query().Get("prefix") == "vault/folder/" {
					return listing(r, "<Contents><Key>vault/folder/</Key><Size>0</Size></Contents>", "", false), nil
				}
				return listing(r, "", "", false), nil
			}
			rootPages++
			if r.URL.Query().Get("continuation-token") == "" {
				return listing(r, "<Contents><Key>vault/a%20%2B%25</Key><Size>10</Size></Contents>", "opaque", true), nil
			}
			return listing(r, "<CommonPrefixes><Prefix>vault/folder/</Prefix></CommonPrefixes>", "", false), nil
		}
		if r.URL.Path != "/bucket/vault/a +%" {
			return response(r, 404, ""), nil
		}
		switch r.Method {
		case "HEAD":
			result := response(r, 200, "")
			result.ContentLength = 10
			result.Header.Set("Content-Length", "10")
			return result, nil
		case "GET":
			if r.Header.Get("Range") == "" {
				result := response(r, 200, "abcdefghij")
				result.Header.Set("Content-Length", "10")
				return result, nil
			}
			if r.Header.Get("Range") != "bytes=2-5" {
				t.Errorf("range %q", r.Header.Get("Range"))
			}
			result := response(r, 206, "cdef")
			result.Header.Set("Content-Length", "4")
			result.Header.Set("Content-Range", "bytes 2-5/10")
			return result, nil
		}
		return nil, errors.New("unexpected operation")
	})
	d, c := prepared(t, rt)
	ctx := context.Background()
	entry, err := c.Stat(ctx, "reader", core.Path("/cloud/a +%"))
	if err != nil || entry.Size == nil || *entry.Size != 10 {
		t.Fatalf("stat: %v %v", entry, err)
	}
	first, err := c.List(ctx, "reader", core.ListRequest{Query: core.Children(core.Path("/cloud")), Limit: 1, CursorTTL: time.Minute})
	if err != nil || len(first.Items) != 1 || first.Items[0].Name != "a +%" || first.NextCursor == nil {
		t.Fatalf("first: %v %v", first, err)
	}
	req := core.ListRequest{Query: core.Children(core.Path("/cloud")), Cursor: *first.NextCursor}
	second, err := c.List(ctx, "reader", req)
	if err != nil || len(second.Items) != 1 || second.Items[0].Kind != core.Directory || second.NextCursor != nil {
		t.Fatalf("second: %v %v", second, err)
	}
	before := rootPages
	replay, err := c.List(ctx, "reader", req)
	if err != nil || len(replay.Items) != 1 {
		t.Fatal(replay, err)
	}
	if rootPages != before {
		t.Fatal("replay advanced the native page")
	}
	opened, err := c.Open(ctx, "reader", core.Path("/cloud/a +%"), core.OpenOptions{Range: &core.Range{Start: 2, End: 5}})
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(opened.Body)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "cdef" || opened.Range == nil || *opened.Entry.Size != 10 {
		t.Fatalf("content: %q %#v", data, opened)
	}
	// Stat used HEAD; Open goes directly to GET after its directory probe.
	if calls["HEAD /bucket/vault/a%20+%25?"]+calls["HEAD /bucket/vault/a%20%2B%25?"] != 1 {
		t.Fatalf("unexpected repeated HEAD: %v", calls)
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
	before = rootPages
	for i := 0; i < 2; i++ {
		opened, err := scope.OpenFile(ctx, "a +%")
		if err != nil {
			t.Fatal(err)
		}
		data, readErr := io.ReadAll(opened.Body)
		closeErr := opened.Body.Close()
		if readErr != nil || closeErr != nil || string(data) != "abcdefghij" {
			t.Fatal(string(data), readErr, closeErr)
		}
	}
	if rootPages != before {
		t.Fatal("scoped file reads repeated directory resolution")
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
	data, err = os.ReadFile(filepath.Join(directory, "copied", "a +%"))
	if err != nil || string(data) != "abcdefghij" {
		t.Fatal(string(data), err)
	}

}

func TestAmbiguousNamesAndIncompleteProbes(t *testing.T) {
	for _, scenario := range []string{"ambiguous", "incomplete", "missing", "bad-marker", "denied"} {
		t.Run(scenario, func(t *testing.T) {
			_, c := prepared(t, func(r *http.Request) (*http.Response, error) {
				if r.URL.Query().Get("list-type") == "2" {
					switch scenario {
					case "incomplete":
						return response(r, 503, "<Error><Code>ServiceUnavailable</Code></Error>"), nil
					case "ambiguous":
						return listing(r, "<CommonPrefixes><Prefix>vault/a/</Prefix></CommonPrefixes>", "", false), nil
					case "bad-marker":
						return listing(r, "", "", true), nil
					}
					return listing(r, "", "", false), nil
				}
				if scenario == "ambiguous" {
					out := response(r, 200, "")
					out.ContentLength = 1
					return out, nil
				}
				if scenario == "denied" {
					return response(r, 403, ""), nil
				}
				return response(r, 404, ""), nil
			})
			_, err := c.Stat(context.Background(), "reader", core.Path("/cloud/a"))
			want := map[string]core.Code{"ambiguous": core.PathAmbiguous, "incomplete": core.UpstreamFailure, "missing": core.NotFound, "bad-marker": core.InvalidUpstreamResponse, "denied": core.UpstreamAccessDenied}[scenario]
			requireCode(t, err, want)
			if scenario == "ambiguous" {
				_, err = c.Open(context.Background(), "reader", core.Path("/cloud/a/child"), core.OpenOptions{})
				requireCode(t, err, core.PathAmbiguous)
			}
		})
	}
}

func TestNativeKeyFlow(t *testing.T) {
	ctx := context.Background()
	server := newProtocol()
	keys := []string{"a", "a/b", "a/", "a//b", "/", "/a", "a +%", "a/../b", `a\b`, "a\x00b", "folder/x"}
	for _, key := range keys {
		server.objects["vault/"+key] = []byte(key)
	}
	lists, pages := 0, 0
	denied := core.Address{}
	currentVersion, historyAvailable := "old", true
	_, c := preparedDriver(t, func(r *http.Request) (*http.Response, error) {
		if version := r.URL.Query().Get("versionId"); version != "" {
			if r.URL.Path != "/bucket/vault/history" || version != "old" || !historyAvailable {
				return response(r, 404, "<Error><Code>NoSuchVersion</Code></Error>"), nil
			}
		}
		if r.URL.Path == "/bucket/vault/history" {
			version := r.URL.Query().Get("versionId")
			if version == "" {
				version = currentVersion
			}
			data := "past"
			if version == "new" {
				data = "fresh"
			}
			out := response(r, 200, data)
			total := len(data)
			if r.Method == "HEAD" {
				out = response(r, 200, "")
			} else if r.Header.Get("Range") != "" {
				var start, end int
				if _, err := fmt.Sscanf(r.Header.Get("Range"), "bytes=%d-%d", &start, &end); err != nil {
					t.Fatal(err)
				}
				out = response(r, 206, data[start:end+1])
				out.Header.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, total))
				data = data[start : end+1]
			}
			out.Header.Set("Content-Length", fmt.Sprint(len(data)))
			out.ContentLength = int64(len(data))
			out.Header.Set("ETag", `"past"`)
			out.Header.Set("X-Amz-Version-Id", version)
			return out, nil
		}
		if r.URL.Query().Get("list-type") == "2" {
			lists++
			q := r.URL.Query()
			if q.Get("prefix") == "vault/pages" {
				pages++
				if q.Get("delimiter") != ":" {
					t.Fatal("delimiter lost", q)
				}
				if q.Get("continuation-token") == "" {
					if q.Get("start-after") != "vault/pages:0" {
						t.Fatal("after lost", q)
					}
					return listing(r, "<Contents><Key>vault/pages:first</Key><Size>1</Size></Contents>", "next", true), nil
				}
				if q.Get("continuation-token") != "next" || q.Get("start-after") != "" {
					t.Fatal("cursor options changed", q)
				}
				return listing(r, "<CommonPrefixes><Prefix>vault/pages:folder:</Prefix></CommonPrefixes>", "", false), nil
			}
		}
		return server.roundTrip(r)
	}, nil, func(_ context.Context, _ core.Principal, _ core.Action, a core.Address) error {
		if a == denied {
			return errors.New("revoked")
		}
		return nil
	})
	defer func() {
		if err := c.Close(ctx); err != nil {
			t.Error(err)
		}
	}()
	for _, key := range keys {
		address := core.Key("/cloud", key)
		e, err := c.Stat(ctx, "reader", address)
		if err != nil || e.Address != address || e.Name != key || e.Kind != core.File {
			t.Fatal(e, err)
		}
		o, err := c.Open(ctx, "reader", address, core.OpenOptions{})
		if err != nil {
			t.Fatal(key, err)
		}
		data, err := io.ReadAll(o.Body)
		closeErr := o.Body.Close()
		if err != nil || closeErr != nil || string(data) != key {
			t.Fatal(key, string(data), err, closeErr)
		}
	}
	if lists != 0 || server.calls["HEAD  "] != len(keys) || server.calls["GET  "] != len(keys) {
		t.Fatal("raw read acquired tree probes", lists, server.calls)
	}
	selection := core.ReadSelection{Version: "old", When: core.IfMatch(core.Validator{Domain: "s3-etag", Tag: `"past"`})}
	history := core.Key("/cloud", "history")
	if opened, err := c.Open(ctx, "reader", history, core.OpenOptions{Mode: core.LiveEdit, Truncate: true}); core.CodeOf(err) != core.UnsupportedOperation || opened.Change.Effect != core.Unchanged {
		t.Fatal(opened, err)
	}
	if e, err := c.Stat(ctx, "reader", history, selection); err != nil || e.Evidence.Version != "old" {
		t.Fatal(e, err)
	}
	o, err := c.Open(ctx, "reader", history, core.OpenOptions{Selection: selection})
	if err != nil {
		t.Fatal(err)
	}
	data, readErr := io.ReadAll(o.Body)
	if err = errors.Join(readErr, o.Body.Close()); err != nil || string(data) != "past" || o.Entry.Evidence.Version != "old" {
		t.Fatal(string(data), err)
	}
	opening, cancelOpening := context.WithCancel(ctx)
	fixed, err := c.Open(opening, "reader", history, core.OpenOptions{Mode: core.BoundRead, Binding: core.PinnedVersion})
	if err != nil || fixed.Binding != core.PinnedVersion {
		t.Fatal(fixed, err)
	}
	defer fixed.File.Close()
	cancelOpening()
	currentVersion = "new"
	buf := make([]byte, 5)
	if n, err := fixed.File.ReadAt(ctx, buf, 0); n != 4 || err != io.EOF || string(buf[:n]) != "past" {
		t.Fatal(n, err, string(buf))
	}
	if n, err := fixed.File.ReadAt(ctx, buf[:2], 1); n != 2 || err != nil || string(buf[:2]) != "as" {
		t.Fatal(n, err, string(buf))
	}
	state, err := fixed.File.Stat(ctx)
	if err != nil || state.Evidence.Version != "old" {
		t.Fatal(state, err)
	}
	historyAvailable = false
	if _, err := fixed.File.ReadAt(ctx, buf[:2], 0); core.CodeOf(err) != core.NotFound {
		t.Fatal("deleted version rebound", err)
	}
	if err := fixed.File.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Open(ctx, "reader", core.Key("/cloud", "a"), core.OpenOptions{Mode: core.BoundRead}); core.CodeOf(err) != core.UnsupportedOperation {
		t.Fatal("unversioned key bound by ETag", err)
	}
	_, err = c.Stat(ctx, "reader", core.Path("/cloud/a"))
	requireCode(t, err, core.PathAmbiguous)
	page, err := c.List(ctx, "reader", core.ListRequest{Query: core.Keys("/cloud", "", ""), CursorTTL: time.Minute})
	if err != nil || len(page.Items) != len(keys) {
		t.Fatal(page, err)
	}
	seen := map[string]bool{}
	for _, e := range page.Items {
		if e.Address.IsPrefix() || e.Kind != core.File {
			t.Fatal("marker became directory", e)
		}
		seen[e.Address.Value()] = true
		data, err := json.Marshal(e.Address)
		var restored core.Address
		if err != nil || json.Unmarshal(data, &restored) != nil || restored != e.Address {
			t.Fatal(string(data), err)
		}
	}
	for _, key := range keys {
		if !seen[key] {
			t.Fatal("key changed", key)
		}
	}
	for _, raw := range []string{`{"namespace":"keys","space":"/cloud","kind":"key","key":"a","path":""}`, `{"namespace":"tree","path":"/cloud","key":""}`, `{"namespace":"tree","path":"/cloud","extra":1}`} {
		var a core.Address
		if json.Unmarshal([]byte(raw), &a) == nil {
			t.Fatal("mixed address admitted", raw)
		}
	}
	page, err = c.List(ctx, "reader", core.ListRequest{Query: core.Keys("/cloud", "a", "/"), CursorTTL: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	group := false
	for _, e := range page.Items {
		if e.Address == core.Prefix("/cloud", "a/") {
			group = e.Kind == core.PrefixGroup
		}
	}
	if !group {
		t.Fatal("missing explicit prefix group", page)
	}
	if _, err = c.Remove(ctx, "writer", core.Prefix("/cloud", "a/")); core.CodeOf(err) != core.UnsupportedOperation {
		t.Fatal("prefix is not a file", err)
	}
	req := core.ListRequest{Query: core.Keys("/cloud", "pages", ":").After("pages:0"), Limit: 1, CursorTTL: time.Minute}
	first, err := c.List(ctx, "reader", req)
	if err != nil || first.NextCursor == nil {
		t.Fatal(first, err)
	}
	req.Cursor = *first.NextCursor
	changed := req
	changed.Query = core.Keys("/cloud", "pages", ":").After("other")
	_, err = c.List(ctx, "reader", changed)
	requireCode(t, err, core.CursorUnavailable)
	for range 2 {
		last, err := c.List(ctx, "reader", req)
		if err != nil || len(last.Items) != 1 || last.Items[0].Address != core.Prefix("/cloud", "pages:folder:") {
			t.Fatal(last, err)
		}
	}
	if pages != 2 {
		t.Fatal("replay repeated native listing", pages)
	}
	capture := func(a core.Address, write bool) *core.Selection {
		t.Helper()
		s, err := c.Capture(ctx, core.AddressAccess{Address: a, Write: write})
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	a := capture(core.Key("/cloud", "a"), true)
	child := capture(core.Key("/cloud", "a/b"), false)
	tree := capture(core.Path("/cloud/a"), true)
	if a.Conflicts(child) || !tree.Conflicts(child) || tree.Conflicts(capture(core.Key("/cloud", "ab"), false)) {
		t.Fatal("exact keys and tree extents conflated")
	}
	binding, _ := c.Binding("/cloud")
	if err = c.BindAlias(ctx, "/alias", binding.Mount, "/folder"); err != nil {
		t.Fatal(err)
	}
	alias := capture(core.Key("/alias", "x"), true)
	for _, a := range []core.Address{core.Key("/cloud", "folder/x"), core.Key("/alias", "x")} {
		denied = a
		_, err = c.Stat(ctx, "reader", core.Key("/alias", "x"))
		requireCode(t, err, core.PermissionDenied)
	}
	denied = core.Address{}
	if e, err := c.Stat(ctx, "reader", core.Key("/alias", "x")); err != nil || e.Name != "x" || e.Address != core.Key("/alias", "x") {
		t.Fatal(e, err)
	}
	if !alias.Conflicts(capture(core.Key("/cloud", "folder/x"), false)) {
		t.Fatal("alias physical identity lost")
	}
	if err = c.UnbindAlias(ctx, "/alias"); err != nil {
		t.Fatal(err)
	}
	if err = c.BindAlias(ctx, "/alias", binding.Mount, "/folder"); err != nil {
		t.Fatal(err)
	}
	_, err = c.Stat(alias.Context(ctx), "reader", core.Key("/alias", "x"))
	requireCode(t, err, core.MountUnavailable)
	root := t.TempDir()
	localDriver, err := local.New(root)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = c.Install("/local", localDriver); err != nil {
		t.Fatal(err)
	}
	if change, err := c.Copy(ctx, "writer", core.Key("/cloud", "a//b"), core.Path("/local/copied")); err != nil || change.Effect != core.Applied {
		t.Fatal(change, err)
	}
	if change, err := c.Copy(ctx, "writer", core.Path("/local/copied"), core.Key("/cloud", "a/new")); err != nil || change.Effect != core.Applied {
		t.Fatal(change, err)
	}
	if string(server.objects["vault/a/new"]) != "a//b" {
		t.Fatal("copy changed native key")
	}
	input := strings.NewReader("untouched")
	length := int64(input.Len())
	if change, err := c.Create(ctx, "writer", core.Key("/local", "key"), input, &length); core.CodeOf(err) != core.UnsupportedOperation || change.Effect != core.Unchanged || input.Len() != int(length) {
		t.Fatal(change, err)
	}
	if change, err := c.Copy(ctx, "writer", core.Path("/cloud/folder/x"), core.Key("/cloud", "folder/x")); core.CodeOf(err) != core.InvalidRequest || change.Effect != core.Unchanged {
		t.Fatal("cross-view self copy", change, err)
	}
	opened, err := c.Open(ctx, "reader", core.Key("/cloud", "a"), core.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	stop, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	if err = binding.Mount.Retire(stop); err != nil {
		t.Fatal(err)
	}
	if _, err = opened.Body.Read(make([]byte, 1)); err == nil {
		t.Fatal("retired raw body still readable")
	}
	if err = opened.Body.Close(); err != nil {
		t.Fatal(err)
	}
	_, err = c.Stat(a.Context(ctx), "reader", core.Key("/cloud", "a"))
	requireCode(t, err, core.MountUnavailable)
}

type trackedBody struct {
	io.Reader
	closed bool
}

func (b *trackedBody) Close() error { b.closed = true; return nil }
func TestContentFactsFailuresAndCleanup(t *testing.T) {
	for _, scenario := range []string{"fallback", "unknown-length", "short", "bad-range", "missing-range", "unsatisfied", "cancel"} {
		t.Run(scenario, func(t *testing.T) {
			body := &trackedBody{Reader: strings.NewReader("abc")}
			_, c := prepared(t, func(r *http.Request) (*http.Response, error) {
				if r.URL.Query().Get("list-type") == "2" {
					return listing(r, "", "", false), nil
				}
				result := response(r, 200, "abc")
				result.Body = body
				result.Header.Set("Content-Length", "3")
				switch scenario {
				case "unknown-length":
					result.ContentLength = -1
					result.Header.Del("Content-Length")
				case "short":
					result.ContentLength = 8
					result.Header.Set("Content-Length", "8")
				case "bad-range":
					result.StatusCode = 206
					result.Header.Set("Content-Range", "bytes 0-2/10")
				case "missing-range":
					result.StatusCode = 206
				case "unsatisfied":
					result.StatusCode = 416
					result.Header.Set("Content-Range", "bytes */3")
					result.Body = io.NopCloser(strings.NewReader("<Error><Code>InvalidRange</Code></Error>"))
				}
				return result, nil
			})
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			address := core.Path("/cloud/a")
			if scenario == "cancel" {
				address = core.Key("/cloud", "a")
			}
			opened, err := c.Open(ctx, "reader", address, core.OpenOptions{Range: &core.Range{Start: 1, End: 2}})
			switch scenario {
			case "bad-range", "missing-range":
				requireCode(t, err, core.InvalidUpstreamResponse)
				if !body.closed {
					t.Fatal("rejected body leaked")
				}
				return
			case "unsatisfied":
				requireCode(t, err, core.RangeNotSatisfiable)
				var failure *core.Error
				if !errors.As(err, &failure) || failure.Total == nil || *failure.Total != 3 {
					t.Fatalf("range total %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if opened.Range != nil {
				t.Fatal("fabricated partial response")
			}
			if scenario == "unknown-length" && (opened.Length != nil || opened.Entry.Size != nil) {
				t.Fatal("invented length")
			}
			if scenario == "cancel" {
				cancel()
			}
			_, err = io.ReadAll(opened.Body)
			if scenario == "short" && !errors.Is(err, io.ErrUnexpectedEOF) {
				t.Fatalf("short read: %v", err)
			}
			if scenario == "cancel" && !errors.Is(err, context.Canceled) {
				t.Fatalf("cancellation: %v", err)
			}
			if scenario != "short" && scenario != "cancel" && err != nil {
				t.Fatal(err)
			}
			if !body.closed {
				t.Fatal("completed body leaked")
			}
		})
	}
}

// TestConditionalCreateThroughCore reuses the protocol fixture and exercises
// only the new write boundary, without a provider service or account.
func TestConditionalCreateThroughCore(t *testing.T) {
	for _, mode := range []string{"normal", "empty", "ack-cancel", "conflict", "lost", "short", "long", "long-ack", "unknown", "directory", "missing-parent"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			sourceDir := t.TempDir()
			data, length := "abc", int64(3)
			var n *int64 = &length
			if mode == "empty" {
				data, length = "", 0
			}
			if mode == "short" {
				data = "ab"
			}
			if mode == "long" || mode == "long-ack" {
				data = "abcd"
			}
			if mode == "unknown" {
				n = nil
			}
			input := strings.NewReader(data)
			puts := 0
			fault := errors.New("response lost")
			_, c := prepared(t, func(r *http.Request) (*http.Response, error) {
				switch r.Method {
				case "HEAD":
					return response(r, 404, ""), nil
				case "GET":
					if mode == "directory" {
						return listing(r, "<CommonPrefixes><Prefix>vault/new/child/</Prefix></CommonPrefixes>", "", false), nil
					}
					return listing(r, "", "", false), nil
				case "PUT":
					puts++
					if r.Header.Get("If-None-Match") != "*" || r.ContentLength != length {
						t.Fatal("conditional length contract was lost", r.Header, r.ContentLength)
					}
					if mode == "conflict" {
						return response(r, 412, "<Error><Code>PreconditionFailed</Code></Error>"), nil
					}
					var got []byte
					var err error
					if r.Body != nil {
						got, err = io.ReadAll(r.Body)
					}
					if err != nil && mode != "long-ack" {
						return nil, err
					}
					if mode == "normal" && string(got) != data {
						t.Fatal(string(got))
					}
					if mode == "lost" {
						return nil, fault
					}
					if mode == "ack-cancel" {
						cancel()
						if _, end := r.Body.Read(make([]byte, 1)); end != io.EOF {
							t.Fatal("completed input changed after cancellation", end)
						}
					}
					return response(r, 200, ""), nil
				}
				return nil, errors.New("unexpected protocol call")
			}, func(o *sdk.Options) { o.RetryMaxAttempts = 3 })
			target := "/cloud/new"
			if mode == "missing-parent" {
				target = "/cloud/missing/new"
			}
			var change core.Change
			var err error
			if mode == "normal" {
				if err := os.WriteFile(filepath.Join(sourceDir, "a"), []byte(data), 0600); err != nil {
					t.Fatal(err)
				}
				source, openErr := local.New(sourceDir)
				if openErr != nil {
					t.Fatal(openErr)
				}
				if _, err := c.Install("/local", source); err != nil {
					_ = source.Close()
					t.Fatal(err)
				}
				change, err = c.Copy(ctx, "writer", core.Path("/local/a"), core.Path(target))
			} else {
				change, err = c.Create(ctx, "writer", core.Path(target), input, n)
			}
			switch mode {
			case "normal", "empty":
				if change.Effect != core.Applied || err != nil {
					t.Fatal(change, err)
				}
			case "ack-cancel":
				if change.Effect != core.Applied || !errors.Is(err, context.Canceled) {
					t.Fatal(change, err)
				}
			case "conflict", "directory":
				if change.Effect != core.Unchanged || core.CodeOf(err) != core.AlreadyExists {
					t.Fatal(change, err)
				}
			case "unknown":
				if change.Effect != core.Unchanged || core.CodeOf(err) != core.UnsupportedOperation || input.Len() != len(data) {
					t.Fatal(change, err)
				}
			case "missing-parent":
				if change.Effect != core.Unchanged || core.CodeOf(err) != core.NotFound {
					t.Fatal(change, err)
				}
			case "long-ack":
				if change.Effect != core.Incomplete || core.CodeOf(err) != core.InvalidRequest {
					t.Fatal(change, err)
				}
			default:
				if change.Effect != core.Unknown || err == nil {
					t.Fatal(change, err)
				}
				if mode == "lost" && !errors.Is(err, fault) {
					t.Fatal(err)
				}
			}
			wantPuts := 1
			if mode == "unknown" || mode == "directory" || mode == "missing-parent" {
				wantPuts = 0
			}
			if puts != wantPuts {
				t.Fatal("write was retried or issued before admission", puts)
			}
		})
	}
}

type heldUpload struct {
	started, release chan struct{}
	reads            int
}

func (r *heldUpload) Read(p []byte) (int, error) {
	r.reads++
	if r.reads == 1 {
		close(r.started)
		<-r.release
		return copy(p, "abc"), nil
	}
	return 0, io.EOF
}

func TestCreateWaitsForBorrowedInput(t *testing.T) {
	input := &heldUpload{started: make(chan struct{}), release: make(chan struct{})}
	release := sync.OnceFunc(func() { close(input.release) })
	t.Cleanup(release)
	var requestBody io.Reader
	readDone := make(chan struct{})
	_, c := prepared(t, func(r *http.Request) (*http.Response, error) {
		if r.Method != "PUT" {
			return listing(r, "", "", false), nil
		}
		requestBody = r.Body
		go func() { _, _ = io.Copy(io.Discard, r.Body); close(readDone) }()
		<-input.started
		return response(r, 412, "<Error><Code>PreconditionFailed</Code></Error>"), nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	length := int64(3)
	go func() { _, _ = c.Create(ctx, "writer", core.Key("/cloud", "new"), input, &length); close(done) }()
	select {
	case <-input.started:
	case <-time.After(3 * time.Second):
		t.Fatal("borrowed input was not read")
	}
	cancel()
	select {
	case <-done:
		t.Fatal("write returned while the borrowed input was active")
	default:
	}
	release()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("write did not acknowledge input completion")
	}
	<-readDone
	before := input.reads
	_, _ = requestBody.Read(make([]byte, 1))
	if input.reads != before {
		t.Fatal("late transport read reached returned input")
	}
}
