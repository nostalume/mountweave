package azureblob_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"testing/iotest"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	core "github.com/nostalume/mountweave"
	driver "github.com/nostalume/mountweave/drivers/azureblob"
	"github.com/nostalume/mountweave/drivers/local"
)

type request struct{ method, key, operation, block, absent, match string }

type service struct {
	mu       sync.Mutex
	files    map[string][]byte
	versions map[string]int
	blocks   map[string]map[string][]byte
	calls    []request
	markers  []string
	entered  chan struct{}
	release  chan struct{}
	evict    string
}

func (s *service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !strings.HasPrefix(r.Header.Get("Authorization"), "SharedKey account:") {
		http.Error(w, "synthetic account signature required", 403)
		return
	}
	key := strings.TrimPrefix(r.URL.Path, "/account/container/")
	q := r.URL.Query()
	call := request{r.Method, key, q.Get("comp"), q.Get("blockid"), r.Header.Get("If-None-Match"), r.Header.Get("If-Match")}
	s.mu.Lock()
	s.calls = append(s.calls, call)
	s.mu.Unlock()
	input, err := io.ReadAll(r.Body)
	if err != nil {
		return
	}
	if key == "vault/cancel" && call.operation == "block" {
		select {
		case s.entered <- struct{}{}:
		default:
		}
		select {
		case <-r.Context().Done():
		case <-s.release:
		}
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	w.Header().Set("x-ms-request-id", "fixture")
	w.Header().Set("x-ms-version", "2023-11-03")
	if call.operation == "list" {
		s.markers = append(s.markers, q.Get("marker"))
		if q.Get("prefix") != "vault/" || q.Get("delimiter") != "/" {
			nativeError(w, 400, "InvalidQueryParameterValue")
			return
		}
		items, next := `<Blob><Name>vault/a</Name><Properties><Last-Modified>Wed, 21 Oct 2015 07:28:00 GMT</Last-Modified><Etag>"v1"</Etag><Content-Length>1</Content-Length><BlobType>BlockBlob</BlobType></Properties></Blob>`, "opaque&amp;marker"
		if q.Get("marker") != "" {
			items, next = `<BlobPrefix><Name>vault/a/</Name></BlobPrefix>`, ""
		}
		w.Header().Set("Content-Type", "application/xml")
		fmt.Fprintf(w, `<EnumerationResults><Prefix>vault/</Prefix><Delimiter>/</Delimiter><Blobs>%s</Blobs><NextMarker>%s</NextMarker></EnumerationResults>`, items, next)
		return
	}
	if call.operation == "block" {
		id, err := base64.StdEncoding.DecodeString(call.block)
		if err != nil || len(id) == 0 {
			nativeError(w, 400, "InvalidBlockId")
			return
		}
		if s.blocks[key] == nil || key == s.evict {
			s.blocks[key] = make(map[string][]byte)
		}
		if old, ok := s.blocks[key][call.block]; ok && !bytes.Equal(old, input) {
			nativeError(w, 400, "BlockIdReusedForDifferentBytes")
			return
		}
		s.blocks[key][call.block] = input
		w.WriteHeader(201)
		return
	}
	old, exists := s.files[key]
	etag := fmt.Sprintf(`"v%d"`, s.versions[key])
	if (r.Method == "GET" || r.Method == "HEAD") && exists && (call.absent == "*" || call.absent == etag) {
		w.Header().Set("ETag", etag)
		w.WriteHeader(304)
		return
	}
	if call.absent == "*" && exists || call.match != "" && (!exists || call.match != "*" && call.match != etag) {
		nativeError(w, 412, "ConditionNotMet")
		return
	}
	if call.operation == "blocklist" {
		var list struct {
			IDs []string `xml:"Latest"`
		}
		if xml.Unmarshal(input, &list) != nil {
			nativeError(w, 400, "InvalidBlockList")
			return
		}
		input = nil
		for _, id := range list.IDs {
			data, ok := s.blocks[key][id]
			if !ok {
				nativeError(w, 400, "InvalidBlockList")
				return
			}
			input = append(input, data...)
		}
	}
	switch r.Method {
	case "PUT":
		s.files[key] = append([]byte(nil), input...)
		s.versions[key]++
		delete(s.blocks, key)
		w.Header().Set("ETag", fmt.Sprintf(`"v%d"`, s.versions[key]))
		w.Header().Set("Last-Modified", "Wed, 21 Oct 2015 07:28:00 GMT")
		if key == "vault/bad-receipt" {
			w.Header().Set("Last-Modified", "invalid timestamp")
		}
		w.WriteHeader(201)
	case "DELETE":
		if !exists {
			nativeError(w, 404, "BlobNotFound")
			return
		}
		delete(s.files, key)
		w.WriteHeader(202)
	case "HEAD", "GET":
		if !exists {
			nativeError(w, 404, "BlobNotFound")
			return
		}
		w.Header().Set("ETag", etag)
		w.Header().Set("Last-Modified", "Wed, 21 Oct 2015 07:28:00 GMT")
		w.Header().Set("x-ms-blob-type", "BlockBlob")
		data, status := old, 200
		raw := r.Header.Get("x-ms-range")
		if raw == "" {
			raw = r.Header.Get("Range")
		}
		if raw != "" {
			a, b, _ := strings.Cut(strings.TrimPrefix(raw, "bytes="), "-")
			start, _ := strconv.Atoi(a)
			end := len(old) - 1
			if a == "" {
				n, _ := strconv.Atoi(b)
				start = max(0, len(old)-n)
			} else if b != "" {
				end, _ = strconv.Atoi(b)
				end = min(end, len(old)-1)
			}
			if start > end {
				nativeError(w, 416, "InvalidRange")
				return
			}
			data, status = old[start:end+1], 206
			w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(old)))
		}
		w.Header().Set("Content-Length", strconv.Itoa(len(data)))
		w.WriteHeader(status)
		if r.Method == "GET" {
			_, _ = w.Write(data)
		}
	default:
		nativeError(w, 400, "UnsupportedFixtureRequest")
	}
}

func nativeError(w http.ResponseWriter, status int, code string) {
	w.Header().Set("x-ms-error-code", code)
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	fmt.Fprintf(w, `<Error><Code>%s</Code><Message>closed fixture</Message></Error>`, code)
}

type transport func(*http.Request) (*http.Response, error)

func (f transport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func setup(t *testing.T) (*service, *driver.Driver, *core.Core) {
	t.Helper()
	s := &service{files: make(map[string][]byte), versions: make(map[string]int), blocks: make(map[string]map[string][]byte), entered: make(chan struct{}, 2), release: make(chan struct{})}
	server := httptest.NewServer(s)
	t.Cleanup(server.Close)
	t.Cleanup(func() { close(s.release) })
	origin, _ := url.Parse(server.URL)
	wire := server.Client().Transport.(*http.Transport).Clone()
	wire.Proxy = nil
	wire.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		if address != origin.Host {
			return nil, errors.New("destination outside closed fixture")
		}
		return (&net.Dialer{}).DialContext(ctx, network, address)
	}
	t.Cleanup(wire.CloseIdleConnections)
	client := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }, Transport: transport(func(r *http.Request) (*http.Response, error) {
		out, err := wire.RoundTrip(r)
		if err == nil && r.URL.Query().Get("comp") == "blocklist" && strings.HasSuffix(r.URL.Path, "/lost") && out.StatusCode == 201 {
			_ = out.Body.Close()
			return nil, errors.New("commit acknowledgement lost")
		}
		return out, err
	})}
	credential, err := azblob.NewSharedKeyCredential("account", base64.StdEncoding.EncodeToString(make([]byte, 32)))
	if err != nil {
		t.Fatal(err)
	}
	native, err := container.NewClientWithSharedKeyCredential(server.URL+"/account/container", credential, &container.ClientOptions{ClientOptions: azcore.ClientOptions{
		Transport: client, InsecureAllowCredentialWithHTTP: true,
		Retry: policy.RetryOptions{MaxRetries: 3, RetryDelay: time.Millisecond, MaxRetryDelay: time.Millisecond},
	}})
	if err != nil {
		t.Fatal(err)
	}
	d, err := driver.Open(context.Background(), driver.Options{Client: native, Prefix: "vault/", PartSize: 4, MaxBufferedBytes: 16, PartConcurrency: 2})
	if err != nil {
		t.Fatal(err)
	}
	c, err := core.New(core.Options{Authorize: func(context.Context, core.Principal, core.Action, core.Address) error { return nil }})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = c.Install("/objects", d); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := c.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	return s, d, c
}

func (s *service) count(method, key, operation string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, c := range s.calls {
		if (method == "" || c.method == method) && (key == "" || c.key == key) && (operation == "" || c.operation == operation) {
			n++
		}
	}
	return n
}

func TestSDKExactKeysAndNativePages(t *testing.T) {
	s, _, c := setup(t)
	ctx := context.Background()
	for _, key := range []string{"a", "a/b", "a//b%+中"} {
		s.files["vault/"+key], s.versions["vault/"+key] = []byte("abcdef"), 1
		entry, err := c.Stat(ctx, "alice", core.Key("/objects", key))
		if err != nil || entry.Name != key || entry.Size == nil || *entry.Size != 6 {
			t.Fatalf("exact key %q: %+v %v", key, entry, err)
		}
	}
	for _, r := range []core.Range{{Start: 1, End: 3}, {Start: 2, End: -1, Suffix: true}} {
		before := s.count("HEAD", "", "")
		opened, err := c.Open(ctx, "alice", core.Key("/objects", "a//b%+中"), core.OpenOptions{Range: &r})
		if err != nil {
			t.Fatal(err)
		}
		data, err := io.ReadAll(opened.Body)
		want, _ := r.Resolve(6)
		valid := opened.Range != nil && *opened.Range == want && string(data) == "abcdef"[want.Start:want.End+1]
		if r.Suffix && opened.Range == nil {
			valid = string(data) == "abcdef"
		}
		if err = errors.Join(err, opened.Body.Close()); err != nil || !valid || s.count("HEAD", "", "") != before {
			t.Fatalf("actual range: %q %+v %v", data, opened.Range, err)
		}
	}
	if opened, err := c.Open(ctx, "alice", core.Key("/objects", "a"), core.OpenOptions{Selection: core.ReadSelection{When: core.IfNotMatch(core.Validator{Tag: `"v1"`})}}); core.CodeOf(err) != core.PreconditionFailed || opened.Body != nil {
		t.Fatalf("SDK 304 was mistaken for a body: %+v %v", opened, err)
	}
	_, err := c.Open(ctx, "alice", core.Key("/objects", "a"), core.OpenOptions{Selection: core.ReadSelection{When: core.IfMatch(core.Validator{Tag: `"different"`})}})
	var failed *core.ConditionFailure
	if core.CodeOf(err) != core.PreconditionFailed || !errors.As(err, &failed) || failed.Negative {
		t.Fatalf("positive condition failure became negative: %v", err)
	}
	q := core.Keys("/objects", "", "/")
	page, err := c.List(ctx, "alice", core.ListRequest{Query: q, Limit: 1, CursorTTL: time.Minute})
	if err != nil || len(page.Items) != 1 || page.Items[0].Name != "a" || page.NextCursor == nil {
		t.Fatalf("first native page: %+v %v", page, err)
	}
	r := core.ListRequest{Query: q, Limit: 1, Cursor: *page.NextCursor}
	page, err = c.List(ctx, "alice", r)
	if err != nil || len(page.Items) != 1 || page.Items[0].Name != "a/" || page.Items[0].Kind != core.PrefixGroup {
		t.Fatalf("prefix group: %+v %v", page, err)
	}
	if len(s.markers) != 2 || s.markers[0] != "" || s.markers[1] != "opaque&marker" {
		t.Fatalf("opaque marker or short page lost: %q", s.markers)
	}
}

func TestSDKPublicationAndCopy(t *testing.T) {
	root := t.TempDir()
	s, _, c := setup(t)
	ctx := context.Background()
	key := core.Key("/objects", "file")
	length := int64(3)
	if changed, err := c.Write(ctx, "alice", key, strings.NewReader("one"), &length); err != nil || changed.Effect != core.Applied {
		t.Fatalf("single PUT: %+v %v", changed, err)
	}
	if changed, err := c.Write(ctx, "alice", key, strings.NewReader("two"), &length); core.CodeOf(err) != core.AlreadyExists || changed.Effect != core.Unchanged {
		t.Fatalf("exclusive PUT: %+v %v", changed, err)
	}
	if changed, err := c.Write(ctx, "alice", core.Key("/objects", "bad-receipt"), strings.NewReader("one"), &length); err == nil || changed.Effect != core.Applied || string(s.files["vault/bad-receipt"]) != "one" {
		t.Fatalf("response parsing lost acknowledged publication: %+v %v", changed, err)
	}
	zero := int64(0)
	for name, size := range map[string]*int64{"empty": &zero, "empty-stream": nil} {
		if changed, err := c.Write(ctx, "alice", core.Key("/objects", name), strings.NewReader(""), size); err != nil || changed.Effect != core.Applied || len(s.files["vault/"+name]) != 0 {
			t.Fatalf("empty object %s: %+v %v", name, changed, err)
		}
	}
	tail := errors.New("input failed at EOF")
	for _, prefix := range []string{"bad", "long invalid stream"} {
		input := io.MultiReader(strings.NewReader(prefix), iotest.ErrReader(tail))
		if changed, err := c.Write(ctx, "alice", key, input, nil, core.WriteOptions{Policy: core.Publish()}); !errors.Is(err, tail) || changed.Effect != core.Unchanged || string(s.files["vault/file"]) != "one" {
			t.Fatalf("failed input published: %+v %v", changed, err)
		}
	}
	entry, err := c.Stat(ctx, "alice", key)
	if err != nil {
		t.Fatal(err)
	}
	payload := "a longer body"
	length = int64(len(payload))
	if changed, err := c.Write(ctx, "alice", key, strings.NewReader(payload), &length, core.WriteOptions{Policy: core.Publish(core.IfMatch(entry.Evidence.Validator))}); err != nil || changed.Effect != core.Applied || string(s.files["vault/file"]) != payload {
		t.Fatalf("conditional block list: %+v %v", changed, err)
	}
	before := s.count("", "", "")
	if changed, err := c.Write(ctx, "alice", key, iotest.ErrReader(tail), nil, core.WriteOptions{Policy: core.Publish(core.All(core.IfMatch(entry.Evidence.Validator), core.IfAbsent()))}); core.CodeOf(err) != core.UnsupportedOperation || changed.Effect != core.Unchanged || s.count("", "", "") != before {
		t.Fatalf("unsupported condition consumed input or sent I/O: %+v %v", changed, err)
	}
	output, err := local.New(root)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = c.Install("/local", output); err != nil {
		t.Fatal(err)
	}
	if changed, err := c.Copy(ctx, "alice", key, core.Path("/local/file")); err != nil || changed.Effect != core.Applied {
		t.Fatalf("Azure to Local: %+v %v", changed, err)
	}
	if changed, err := c.Copy(ctx, "alice", core.Path("/local/file"), core.Key("/objects", "copied")); err != nil || changed.Effect != core.Applied || string(s.files["vault/copied"]) != payload {
		t.Fatalf("Local to Azure: %+v %v", changed, err)
	}
	if changed, err := c.Remove(ctx, "alice", key, core.RemoveOptions{When: core.IfAbsent()}); core.CodeOf(err) != core.PreconditionFailed || changed.Effect != core.Unchanged || string(s.files["vault/file"]) != payload {
		t.Fatalf("delete condition became a creation failure: %+v %v", changed, err)
	}
	if changed, err := c.Remove(ctx, "alice", key); err != nil || changed.Effect != core.Applied {
		t.Fatalf("remove current exact object: %+v %v", changed, err)
	}
}

func TestSDKCancellationAndUnknownPublication(t *testing.T) {
	s, _, c := setup(t)
	// Watchdogs bound a broken test, not the Driver's timing contract.
	watchdog, stop := context.WithTimeout(context.Background(), 5*time.Second)
	defer stop()
	ctx, cancel := context.WithCancel(watchdog)
	type result struct {
		change core.Change
		err    error
	}
	done := make(chan result, 1)
	go func() {
		change, err := c.Write(ctx, "alice", core.Key("/objects", "cancel"), strings.NewReader("a longer body"), nil)
		done <- result{change, err}
	}()
	select {
	case <-s.entered:
	case <-watchdog.Done():
		t.Fatal("stage did not start")
	}
	cancel()
	select {
	case got := <-done:
		if !errors.Is(got.err, context.Canceled) || got.change.Effect != core.Unchanged {
			t.Fatalf("canceled staging: %+v %v", got.change, got.err)
		}
	case <-watchdog.Done():
		t.Fatal("canceled upload did not join")
	}
	if s.count("DELETE", "", "") != 0 || s.count("PUT", "vault/cancel", "blocklist") != 0 {
		t.Fatal("canceled staging deleted by name or committed")
	}
	if changed, err := c.Write(watchdog, "alice", core.Key("/objects", "lost"), strings.NewReader("a longer body"), nil); err == nil || changed.Effect != core.Unknown || s.count("PUT", "vault/lost", "blocklist") != 1 || string(s.files["vault/lost"]) != "a longer body" {
		t.Fatalf("lost commit confirmation was replayed or misclassified: %+v %v", changed, err)
	}
	s.evict = "vault/interference"
	s.files[s.evict] = []byte("old")
	if changed, err := c.Write(watchdog, "alice", core.Key("/objects", "interference"), strings.NewReader("a longer body"), nil, core.WriteOptions{Policy: core.Publish()}); err == nil || changed.Effect == core.Applied || string(s.files[s.evict]) != "old" {
		t.Fatalf("discarded staged blocks became mixed success: %+v %v", changed, err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	seen := make(map[string]string)
	width := -1
	for _, call := range s.calls {
		if call.operation != "block" {
			continue
		}
		decoded, err := base64.StdEncoding.DecodeString(call.block)
		if err != nil || width >= 0 && len(decoded) != width {
			t.Fatalf("invalid or unequal block ID widths: %q", call.block)
		}
		width = len(decoded)
		if old, ok := seen[call.block]; ok && old != call.key {
			t.Fatal("different attempts reused a block ID")
		}
		seen[call.block] = call.key
	}
}
