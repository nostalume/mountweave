package s3_test

import (
	"context"
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
	driver "github.com/nostalume/mountweave/drivers/s3"
	"github.com/nostalume/mountweave/local"
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
	return preparedDriver(t, rt, nil, options...)
}
func preparedDriver(t *testing.T, rt transport, storageOptions []driver.Options, options ...func(*sdk.Options)) (*driver.Driver, *core.Core) {
	t.Helper()
	client := sdk.New(sdk.Options{Region: "us-east-1", BaseEndpoint: aws.String("https://s3.example.test"), UsePathStyle: true, RetryMaxAttempts: 1, Credentials: aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
		return aws.Credentials{AccessKeyID: "test-key", SecretAccessKey: "test-secret"}, nil
	}), HTTPClient: &http.Client{Transport: rt}}, options...)
	d, err := driver.New(client, "bucket", "vault/", storageOptions...)
	if err != nil {
		t.Fatal(err)
	}
	c, err := core.New(core.Options{Authorize: func(context.Context, core.Principal, core.Action, string) error { return nil }})
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
	entry, err := c.Stat(ctx, "reader", "/cloud/a +%")
	if err != nil || entry.Size == nil || *entry.Size != 10 {
		t.Fatalf("stat: %v %v", entry, err)
	}
	first, err := c.List(ctx, "reader", core.ListRequest{Path: "/cloud", Limit: 1, CursorTTL: time.Minute})
	if err != nil || len(first.Items) != 1 || first.Items[0].Name != "a +%" || first.NextCursor == nil {
		t.Fatalf("first: %v %v", first, err)
	}
	req := core.ListRequest{Path: "/cloud", Cursor: *first.NextCursor}
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
	opened, err := c.Open(ctx, "reader", "/cloud/a +%", &core.Range{Start: 2, End: 5})
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
	scope, err := d.OpenDirectory(ctx, "/")
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
	rows, err := c.CopyTree(ctx, "reader", "/cloud", "/local/copied", core.TreeCopyOptions{MaxEntries: 20, MaxPlanBytes: 1024})
	if err != nil || len(rows) != 3 {
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
			_, err := c.Stat(context.Background(), "reader", "/cloud/a")
			want := map[string]core.Code{"ambiguous": core.PathAmbiguous, "incomplete": core.UpstreamFailure, "missing": core.NotFound, "bad-marker": core.InvalidUpstreamResponse, "denied": core.UpstreamAccessDenied}[scenario]
			requireCode(t, err, want)
			if scenario == "ambiguous" {
				_, err = c.Open(context.Background(), "reader", "/cloud/a/child", nil)
				requireCode(t, err, core.PathAmbiguous)
			}
		})
	}
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
			opened, err := c.Open(ctx, "reader", "/cloud/a", &core.Range{Start: 1, End: 2})
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
				change, err = c.Copy(ctx, "writer", "/local/a", target)
			} else {
				change, err = c.Create(ctx, "writer", target, input, n)
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
	go func() { _, _ = c.Create(ctx, "writer", "/cloud/new", input, &length); close(done) }()
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
