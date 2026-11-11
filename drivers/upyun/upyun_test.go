package upyun

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/md5"
	"crypto/sha1"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	core "github.com/nostalume/mountweave"
)

type nativeService struct {
	mu               sync.Mutex
	calls, markers   []string
	files            map[string][]byte
	folders          map[string]bool
	entered, release chan struct{}
}

func (s *nativeService) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	key := md5.Sum([]byte("password123"))
	mac := hmac.New(sha1.New, []byte(hex.EncodeToString(key[:])))
	_, _ = io.WriteString(mac, r.Method+"&"+r.URL.RequestURI()+"&"+r.Header.Get("Date"))
	if r.Header.Get("Authorization") != "UPYUN operator123:"+base64.StdEncoding.EncodeToString(mac.Sum(nil)) {
		http.Error(w, "signature mismatch", 401)
		return
	}
	s.mu.Lock()
	s.calls = append(s.calls, r.Method+" "+r.URL.EscapedPath())
	s.mu.Unlock()
	if strings.HasSuffix(r.URL.Path, "/waiting") {
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
	p := r.URL.Path
	if strings.HasSuffix(p, "/redirect") {
		w.Header().Set("Location", "http://outside.invalid/secret")
		w.WriteHeader(307)
		return
	}
	if r.Method == "GET" && r.Header.Get("X-Upyun-Folder") == "true" {
		marker := r.Header.Get("X-List-Iter")
		s.markers = append(s.markers, marker)
		w.Header().Set("Content-Type", "application/json")
		if strings.HasSuffix(p, "/malformed") {
			_, _ = io.WriteString(w, `{"files":[],"iter":"x"} trailing`)
			return
		}
		if strings.HasSuffix(p, "/huge") {
			_, _ = io.WriteString(w, strings.Repeat("x", 257))
			return
		}
		if strings.HasSuffix(p, "/cycle") {
			_, _ = io.WriteString(w, `{"files":[],"iter":"same"}`)
			return
		}
		if marker == "" {
			_, _ = io.WriteString(w, `{"files":[],"iter":"opaque&marker"}`)
		} else {
			w.Header().Set("X-Upyun-List-Iter", endMarker)
			_, _ = io.WriteString(w, `{"files":[{"name":"p%+中.txt","type":"text/plain","length":6,"last_modified":1445412480}],"iter":"`+endMarker+`"}`)
		}
		return
	}
	if r.Method == "POST" {
		if r.Header.Get("folder") != "true" || r.Header.Get("X-Upyun-Folder") != "true" {
			http.Error(w, "not mkdir", 400)
			return
		}
		s.folders[p] = true
		w.WriteHeader(200)
		return
	}
	if r.Method == "DELETE" {
		if r.Header.Get("X-Upyun-Async") != "" {
			http.Error(w, "not synchronous", 400)
			return
		}
		if strings.HasSuffix(p, "/full") {
			w.WriteHeader(403)
			_, _ = io.WriteString(w, `{"code":40300005,"msg":"directory is not empty"}`)
			return
		}
		delete(s.files, p)
		delete(s.folders, p)
		w.WriteHeader(200)
		return
	}
	data, found := s.files[p]
	if !found && !s.folders[p] {
		http.Error(w, "native missing", 404)
		return
	}
	if r.Method == "HEAD" {
		kind := "file"
		if s.folders[p] {
			kind = "folder"
		}
		w.Header().Set("X-Upyun-File-Type", kind)
		w.Header().Set("X-Upyun-File-Size", strconv.Itoa(len(data)))
		w.Header().Set("X-Upyun-File-Date", "1445412480")
		w.WriteHeader(200)
		return
	}
	if r.Method != "GET" || r.Header.Get("X-Upyun-Folder") != "false" {
		http.Error(w, "unexpected request", 400)
		return
	}
	w.Header().Set("X-Upyun-File-Type", "file")
	status := 200
	if raw := r.Header.Get("Range"); raw != "" {
		var start, end int
		if _, err := fmt.Sscanf(raw, "bytes=%d-%d", &start, &end); err == nil {
			end = min(end, len(data)-1)
			w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(data)))
			data, status = data[start:end+1], 206
		}
	}
	w.Header().Set("Content-Length", strconv.Itoa(len(data)))
	w.WriteHeader(status)
	_, _ = w.Write(data)
}

func setup(t *testing.T, limit int64) (*nativeService, *Driver, *core.Core) {
	t.Helper()
	s := &nativeService{files: map[string][]byte{"/bucket/root/p%+中.txt": []byte("abcdef")}, folders: map[string]bool{"/bucket/root/": true}, entered: make(chan struct{}, 1), release: make(chan struct{})}
	server := httptest.NewServer(s)
	t.Cleanup(server.Close)
	t.Cleanup(func() { close(s.release) })
	d, err := Open(context.Background(), Options{RootURL: server.URL + "/bucket/root/", Operator: "operator123", Password: "password123", Client: server.Client(), MaxResponseBytes: limit})
	if err != nil {
		t.Fatal(err)
	}
	c, err := core.New(core.Options{Authorize: func(_ context.Context, _ core.Principal, _ core.Action, p core.Address) error {
		if p.Value() == "/remote/denied" {
			return core.Fail(core.PermissionDenied, nil)
		}
		return nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = c.Install("/remote", d); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := c.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	return s, d, c
}

func TestNativeWorkflowAndMutations(t *testing.T) {
	if got := signature([]byte("482c811da5d5b4bc6d497ffa98491e38"), "PUT", "/upyun-temp/demo.jpg", "Wed, 09 Nov 2016 14:26:58 GMT", "7ac66c0f148de9519b8bd264312c4d64"); got != "YUaAZX+WNAcJdNGHS5SBlITME5A=" {
		t.Fatal(got)
	}
	s, _, c := setup(t, 512)
	ctx := context.Background()
	entry, err := c.Stat(ctx, "user", core.Path("/remote/p%+中.txt"))
	if err != nil || entry.Name != "p%+中.txt" || entry.Size == nil || *entry.Size != 6 || entry.ModifiedAt != nil {
		t.Fatalf("HEAD facts: %+v %v", entry, err)
	}
	query := core.Children(core.Path("/remote"))
	page, err := c.List(ctx, "user", core.ListRequest{Query: query, Limit: 1, CursorTTL: time.Minute})
	if err != nil || len(page.Items) != 0 || page.NextCursor == nil {
		t.Fatalf("native empty page: %+v %v", page, err)
	}
	page, err = c.List(ctx, "user", core.ListRequest{Query: query, Limit: 1, Cursor: *page.NextCursor})
	if err != nil || len(page.Items) != 1 || page.Items[0].Name != "p%+中.txt" {
		t.Fatalf("continued page: %+v %v", page, err)
	}
	if len(s.markers) != 2 || s.markers[1] != "opaque&marker" {
		t.Fatal(s.markers)
	}
	for _, r := range []*core.Range{nil, {Start: 1, End: 3}, {Start: 2, End: -1, Suffix: true}} {
		opened, err := c.Open(ctx, "user", core.Path("/remote/p%+中.txt"), core.OpenOptions{Range: r})
		if err != nil {
			t.Fatal(err)
		}
		data, err := io.ReadAll(opened.Body)
		if err = errors.Join(err, opened.Body.Close()); err != nil {
			t.Fatal(err)
		}
		want := "abcdef"
		if opened.Range != nil {
			want = want[opened.Range.Start : opened.Range.End+1]
		}
		if string(data) != want {
			t.Fatalf("actual body %q %+v", data, opened.Range)
		}
	}
	if change, err := c.Mkdir(ctx, "user", core.Path("/remote/new")); err != nil || change.Effect != core.Applied {
		t.Fatal(change, err)
	}
	if change, err := c.Remove(ctx, "user", core.Path("/remote/new")); err != nil || change.Effect != core.Applied {
		t.Fatal(change, err)
	}
	before := len(s.calls)
	for _, o := range []core.RemoveOptions{{Target: core.RemoveNonDirectory}, {When: core.IfPresent()}} {
		if change, err := c.Remove(ctx, "user", core.Path("/remote/p%+中.txt"), o); core.CodeOf(err) != core.UnsupportedOperation || change.Effect != core.Unchanged {
			t.Fatal(change, err)
		}
	}
	if change, err := c.Write(ctx, "user", core.Path("/remote/file"), strings.NewReader("new"), nil); core.CodeOf(err) != core.UnsupportedOperation || change.Effect != core.Unchanged {
		t.Fatal(change, err)
	}
	if _, err := c.Open(ctx, "user", core.Path("/remote/denied"), core.OpenOptions{}); core.CodeOf(err) != core.PermissionDenied {
		t.Fatal(err)
	}
	if len(s.calls) != before {
		t.Fatal("refused operation reached native endpoint")
	}
	if change, err := c.Remove(ctx, "user", core.Path("/remote/full")); change.Effect != core.Unchanged || core.CodeOf(err) != core.DirectoryNotEmpty {
		t.Fatal(change, err)
	}
}

type transportFunc func(*http.Request) (*http.Response, error)

func (f transportFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type failedClose struct {
	io.ReadCloser
	fail bool
}

func (b *failedClose) Close() error {
	if b.fail {
		return errors.New("metadata close failed")
	}
	return b.ReadCloser.Close()
}

func TestCancellationBudgetsAndCleanup(t *testing.T) {
	s, d, c := setup(t, 128)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { _, err := c.Open(ctx, "user", core.Path("/remote/waiting"), core.OpenOptions{}); done <- err }()
	select {
	case <-s.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("request not started")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("request not joined")
	}
	for _, name := range []string{"malformed", "huge", "cycle"} {
		iter, err := d.Enumerate(context.Background(), core.NativeChildren(core.NativePath("/"+name)))
		if err != nil {
			t.Fatal(err)
		}
		_, err = iter.Next(context.Background(), 1)
		if name == "cycle" && err == nil {
			_, err = iter.Next(context.Background(), 1)
		}
		if err == nil || name == "huge" && core.CodeOf(err) != core.ResponseBudgetExceeded {
			t.Fatalf("%s: %v", name, err)
		}
		if e := iter.Close(); e != nil {
			t.Fatal(e)
		}
	}
	if _, err := c.Open(context.Background(), "user", core.Path("/remote/redirect"), core.OpenOptions{}); err == nil {
		t.Fatal("redirect followed")
	}
	response := &failedClose{ReadCloser: io.NopCloser(bytes.NewReader(nil)), fail: true}
	client := &http.Client{Transport: transportFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Header: http.Header{"X-Upyun-File-Type": {"folder"}}, Body: response}, nil
	})}
	bad, err := Open(context.Background(), Options{RootURL: "https://fixture.invalid/bucket/", Client: client})
	if err == nil || bad == nil {
		t.Fatal("failed cleanup forgotten", err)
	}
	response.fail = false
	if err = bad.Close(); err != nil {
		t.Fatal(err)
	}
	terminal := &failedClose{ReadCloser: io.NopCloser(strings.NewReader(`{"files":[],"iter":"` + endMarker + `"}`)), fail: true}
	client.Transport = transportFunc(func(req *http.Request) (*http.Response, error) {
		body := io.ReadCloser(io.NopCloser(strings.NewReader("")))
		if req.Method == "GET" {
			body = terminal
		}
		return &http.Response{StatusCode: 200, Header: http.Header{"X-Upyun-File-Type": {"folder"}}, Body: body}, nil
	})
	owned, err := Open(context.Background(), Options{RootURL: "https://fixture.invalid/bucket/", Client: client})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = c.Install("/cleanup", owned); err != nil {
		t.Fatal(err)
	}
	_, err = c.List(context.Background(), "user", core.ListRequest{Query: core.Children(core.Path("/cleanup")), Limit: 1, CursorTTL: time.Minute})
	if err == nil || !strings.Contains(err.Error(), "metadata close failed") {
		t.Fatalf("terminal cleanup disguised as EOF: %v", err)
	}
	terminal.fail = false
	client.Transport = transportFunc(func(req *http.Request) (*http.Response, error) {
		if req.Method == "GET" {
			return nil, io.EOF
		}
		return &http.Response{StatusCode: 200, Header: http.Header{"X-Upyun-File-Type": {"folder"}}, Body: io.NopCloser(strings.NewReader(""))}, nil
	})
	disconnected, err := Open(context.Background(), Options{RootURL: "https://fixture.invalid/bucket/", Client: client})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = c.Install("/disconnected", disconnected); err != nil {
		t.Fatal(err)
	}
	_, err = c.List(context.Background(), "user", core.ListRequest{Query: core.Children(core.Path("/disconnected")), Limit: 1, CursorTTL: time.Minute})
	if err == nil || !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("transport EOF disguised as listing end: %v", err)
	}
}

func TestMutationReceiptAndRetirement(t *testing.T) {
	s, d, c := setup(t, 512)
	original := d.client.Transport
	d.client.Transport = transportFunc(func(req *http.Request) (*http.Response, error) {
		out, err := original.RoundTrip(req)
		if err == nil && req.Method == "POST" {
			_ = out.Body.Close()
			return nil, errors.New("native acknowledgement lost")
		}
		return out, err
	})
	change, err := c.Mkdir(context.Background(), "user", core.Path("/remote/lost"))
	if err == nil || change.Effect != core.Unknown || !s.folders["/bucket/root/lost"] {
		t.Fatal(change, err)
	}
	done := make(chan error, 1)
	go func() {
		_, e := c.Open(context.Background(), "user", core.Path("/remote/waiting"), core.OpenOptions{})
		done <- e
	}()
	select {
	case <-s.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("retirement request did not start")
	}
	wait, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err = c.Close(wait); err != nil {
		t.Fatal(err)
	}
	select {
	case e := <-done:
		if !errors.Is(e, context.Canceled) {
			t.Fatal(e)
		}
	case <-wait.Done():
		t.Fatal("retirement did not join native request")
	}
	if _, err = d.Stat(context.Background(), core.NativePath("/")); core.CodeOf(err) != core.MountUnavailable {
		t.Fatal(err)
	}
}
