package httpapi_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"testing/iotest"
	"time"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/local"
	"github.com/nostalume/mountweave/protocols/httpapi"
)

func setup(t *testing.T) (*core.Core, http.Handler, string) {
	t.Helper()
	root := t.TempDir()
	for name, data := range map[string][]byte{"data.bin": []byte("0123456789"), "empty": {}, "space % + &": []byte("encoded"), "literal%2e%2e": []byte("once")} {
		if err := os.WriteFile(filepath.Join(root, name), data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	c, h, err := assemble(root)
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
	return c, h, root
}
func assemble(root string) (*core.Core, http.Handler, error) {
	c, err := core.New(core.Options{Authorize: func(ctx context.Context, p core.Principal, a core.Action, targetAddress core.Address) error {
		if p == "writer" || p == "reader" && (a == core.StatAction || a == core.ListAction || a == core.ReadAction) {
			return nil
		}
		return errors.New("denied")
	}})
	if err != nil {
		return nil, nil, err
	}
	d, err := local.New(root)
	if err != nil {
		return nil, nil, err
	}
	if _, err = c.Install("/files", d); err != nil {
		_ = d.Close()
		return nil, nil, err
	}
	h, err := httpapi.New(c, httpapi.Options{Authenticate: func(ctx context.Context, token string) (core.Principal, error) {
		switch token {
		case "read-token":
			return "reader", nil
		case "write-token":
			return "writer", nil
		case "denied-token":
			return "denied", nil
		}
		return "", errors.New("invalid token")
	}, CursorTTL: time.Minute})
	return c, h, err
}
func request(t *testing.T, h http.Handler, method, target, token string, headers map[string]string, bodies ...io.Reader) *httptest.ResponseRecorder {
	t.Helper()
	var body io.Reader
	if len(bodies) != 0 {
		body = bodies[0]
	}
	r := httptest.NewRequest(method, target, body)
	if token != "" {
		r.Header.Set("Authorization", "Bearer "+token)
	}
	for k, v := range headers {
		r.Header.Set(k, v)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}
func fsURL(path string) string { return "/api/v1/fs/content?path=" + url.QueryEscape(path) }
func TestAuthenticationAndWireContract(t *testing.T) {
	_, h, _ := setup(t)
	tests := []struct {
		method, target, token string
		status                int
		code                  string
	}{{"GET", "/api/v1/info", "", 401, "unauthenticated"}, {"GET", fsURL("/files/data.bin"), "invalid", 401, "unauthenticated"}, {"GET", fsURL("/files/data.bin"), "denied-token", 403, "permission_denied"}, {"POST", fsURL("/files/data.bin"), "read-token", 405, "invalid_request"}, {"GET", fsURL("/files/missing"), "read-token", 404, "not_found"}, {"GET", fsURL("/files"), "read-token", 409, "not_file"}, {"GET", "/api/v1/info?unknown=1", "read-token", 400, "invalid_request"}, {"GET", "/api/v1/fs/entry?path=%2Ffiles&path=%2F", "read-token", 400, "invalid_request"}, {"GET", fsURL("/files/../data.bin"), "read-token", 400, "invalid_path"}}
	for _, tt := range tests {
		w := request(t, h, tt.method, tt.target, tt.token, nil)
		if w.Code != tt.status {
			t.Fatalf("%s: status %d body %s", tt.target, w.Code, w.Body.String())
		}
		var p map[string]any
		if err := json.Unmarshal(w.Body.Bytes(), &p); err != nil {
			t.Fatal(err)
		}
		if p["code"] != tt.code || p["status"] != float64(tt.status) || p["request_id"] == "" {
			t.Fatal(p)
		}
		if strings.Contains(w.Body.String(), "data.bin") || strings.Contains(w.Body.String(), tt.token) && tt.token != "" {
			t.Fatal("diagnostic leak")
		}
		if w.Header().Get("Content-Type") != "application/problem+json" {
			t.Fatal(w.Header())
		}
	}
	w := request(t, h, "GET", "/api/v1/info", "read-token", nil)
	if w.Code != 200 {
		t.Fatal(w.Code)
	}
	var info struct {
		APIMajor int            `json:"api_major"`
		Limits   map[string]int `json:"limits"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &info); err != nil || info.APIMajor != 1 || info.Limits["page_max"] != 200 {
		t.Fatal(info, err)
	}
	w = request(t, h, "GET", "/api/v1/fs/entry?path=%2Ffiles%2Fdata.bin", "read-token", nil)
	var entry core.Entry
	if err := json.Unmarshal(w.Body.Bytes(), &entry); err != nil || entry.Size == nil || *entry.Size != 10 {
		t.Fatal(entry, err)
	}
}
func TestContentRangesConditionsAndHead(t *testing.T) {
	_, h, _ := setup(t)
	tests := []struct {
		method, path      string
		headers           map[string]string
		status            int
		body, rangeHeader string
	}{{"GET", "/files/data.bin", nil, 200, "0123456789", ""}, {"GET", "/files/data.bin", map[string]string{"Range": "bytes=2-4"}, 206, "234", "bytes 2-4/10"}, {"GET", "/files/data.bin", map[string]string{"Range": "bytes=8-"}, 206, "89", "bytes 8-9/10"}, {"GET", "/files/data.bin", map[string]string{"Range": "bytes=-3"}, 206, "789", "bytes 7-9/10"}, {"GET", "/files/data.bin", map[string]string{"Range": "bytes=0-1,4-5"}, 200, "0123456789", ""}, {"GET", "/files/data.bin", map[string]string{"Range": "items=0-1"}, 200, "0123456789", ""}, {"GET", "/files/data.bin", map[string]string{"Range": "bytes=0-1", "If-Range": "\"tag\""}, 200, "0123456789", ""}, {"GET", "/files/data.bin", map[string]string{"If-Match": "*"}, 200, "0123456789", ""}, {"GET", "/files/data.bin", map[string]string{"If-None-Match": "\"other\""}, 501, "", ""}, {"GET", "/files/data.bin", map[string]string{"If-None-Match": "*"}, 304, "", ""}, {"HEAD", "/files/data.bin", map[string]string{"Range": "bytes=2-4"}, 200, "", ""}, {"HEAD", "/files/data.bin", map[string]string{"If-Match": "\"other\""}, 501, "", ""}, {"HEAD", "/files/missing", nil, 404, "", ""}, {"GET", "/files/empty", nil, 200, "", ""}, {"GET", "/files/space % + &", nil, 200, "encoded", ""}, {"GET", "/files/literal%2e%2e", nil, 200, "once", ""}}
	for _, tt := range tests {
		w := request(t, h, tt.method, fsURL(tt.path), "read-token", tt.headers)
		if w.Code != tt.status || tt.status < 400 && w.Body.String() != tt.body || tt.method == "HEAD" && w.Body.Len() != 0 || w.Header().Get("Content-Range") != tt.rangeHeader {
			t.Fatalf("%s %s %+v: %d %q %v", tt.method, tt.path, tt.headers, w.Code, w.Body.String(), w.Header())
		}
		if w.Header().Get("ETag") != "" || w.Header().Get("Last-Modified") != "" {
			t.Fatal("fabricated validator")
		}
		if tt.method == "HEAD" && tt.status == 200 && w.Header().Get("Content-Length") != "10" {
			t.Fatal(w.Header())
		}
	}
	for _, test := range []struct {
		path, value string
		status      int
		rangeHeader string
	}{{"/files/data.bin", "bytes=99-", 416, "bytes */10"}, {"/files/empty", "bytes=0-0", 416, "bytes */0"}, {"/files/data.bin", "bytes=5-2", 400, ""}, {"/files/data.bin", "bytes=-0", 416, "bytes */10"}, {"/files/data.bin", "bytes=+1-3", 400, ""}} {
		w := request(t, h, "GET", fsURL(test.path), "read-token", map[string]string{"Range": test.value})
		if w.Code != test.status || w.Header().Get("Content-Range") != test.rangeHeader {
			t.Fatal(test, w.Code, w.Header())
		}
	}
	w := request(t, h, "GET", fsURL("/files/missing"), "read-token", map[string]string{"If-Match": "\"x\""})
	if w.Code != 404 {
		t.Fatal("precondition hid resource failure", w.Code)
	}
	w = request(t, h, "GET", fsURL("/files/data.bin"), "read-token", map[string]string{"If-Match": "\"x\""})
	if w.Code != 501 {
		t.Fatal(w.Code)
	}
	w = request(t, h, "HEAD", fsURL("/files"), "read-token", map[string]string{"If-None-Match": "*"})
	if w.Code != 409 || w.Body.Len() != 0 {
		t.Fatal("condition hid the file endpoint kind error", w.Code)
	}
}
func TestConditionalListDoesNotAdvance(t *testing.T) {
	_, h, _ := setup(t)
	base := "/api/v1/fs/entries?path=%2Ffiles&limit=1"
	first := request(t, h, "GET", base, "read-token", nil)
	var page core.Page
	if err := json.Unmarshal(first.Body.Bytes(), &page); err != nil || page.NextCursor == nil {
		t.Fatal(page, err)
	}
	continuation := "/api/v1/fs/entries?path=%2Ffiles&cursor=" + url.QueryEscape(*page.NextCursor)
	conditional := request(t, h, "GET", continuation, "read-token", map[string]string{"If-None-Match": "*"})
	if conditional.Code != 304 {
		t.Fatal(conditional.Code, conditional.Body.String())
	}
	next := request(t, h, "GET", continuation, "read-token", nil)
	replay := request(t, h, "GET", continuation, "read-token", nil)
	if next.Code != 200 || !bytes.Equal(next.Body.Bytes(), replay.Body.Bytes()) {
		t.Fatal("continuation was not replayable")
	}
	invalid := request(t, h, "GET", continuation+"&limit=2", "read-token", map[string]string{"If-None-Match": "*"})
	if invalid.Code != 410 {
		t.Fatal(invalid.Code)
	}
}

// The subprocess host is explicit test assembly, not a configuration loader.
func TestReadHostProcessHelper(t *testing.T) {
	if os.Getenv("CORE_READ_HELPER") != "1" {
		return
	}
	c, h, err := assemble(os.Getenv("CORE_READ_ROOT"))
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{Handler: h}
	served := make(chan error, 1)
	go func() { served <- server.Serve(listener) }()
	fmt.Fprintln(os.Stdout, listener.Addr().String())
	_, _ = io.Copy(io.Discard, os.Stdin)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err = c.Close(ctx); err != nil {
		t.Error(err)
	}
	if err = server.Shutdown(ctx); err != nil {
		t.Error(err)
	}
	if err = <-served; !errors.Is(err, http.ErrServerClosed) {
		t.Error(err)
	}
}
func TestHeadlessOperationsInSeparateProcess(t *testing.T) {
	root := t.TempDir()
	data := bytes.Repeat([]byte("0123456789abcdef"), 4097)
	if err := os.WriteFile(filepath.Join(root, "large.bin"), data, 0600); err != nil {
		t.Fatal(err)
	}
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, binary, "-test.run=^TestReadHostProcessHelper$")
	cmd.Env = append(os.Environ(), "CORE_READ_HELPER=1", "CORE_READ_ROOT="+root)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = stdin.Close()
		if cmd.ProcessState == nil {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	})
	line, err := bufio.NewReader(stdout).ReadString('\n')
	if err != nil {
		t.Fatal(err, stderr.String())
	}
	address := "http://" + strings.TrimSpace(line)
	client := &http.Client{Timeout: 3 * time.Second}
	get := func(method, rangeHeader string) ([]byte, *http.Response) {
		t.Helper()
		r, err := http.NewRequest(method, address+fsURL("/files/large.bin"), nil)
		if err != nil {
			t.Fatal(err)
		}
		r.Header.Set("Authorization", "Bearer read-token")
		if rangeHeader != "" {
			r.Header.Set("Range", rangeHeader)
		}
		resp, err := client.Do(r)
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if err != nil {
			t.Fatal(err)
		}
		return body, resp
	}
	body, resp := get("GET", "bytes=65535-65537")
	if resp.StatusCode != 206 || !bytes.Equal(body, data[65535:65538]) {
		t.Fatal(resp.StatusCode, string(body))
	}
	body, resp = get("HEAD", "")
	if len(body) != 0 || resp.Header.Get("Content-Length") != strconv.Itoa(len(data)) {
		t.Fatal(resp.Header)
	}
	body, resp = get("GET", "")
	if resp.StatusCode != 200 || !bytes.Equal(body, data) {
		t.Fatal("process content mismatch")
	}

	for _, step := range []struct {
		method, target, body string
		status               int
	}{{"PUT", fsURL("/files/uploaded"), "transport bytes", 201}, {"DELETE", "/api/v1/fs/entry?path=%2Ffiles%2Fuploaded", "", 200}} {
		r, e := http.NewRequest(step.method, address+step.target, strings.NewReader(step.body))
		if e != nil {
			t.Fatal(e)
		}
		r.Header.Set("Authorization", "Bearer write-token")
		response, e := client.Do(r)
		if e != nil {
			t.Fatal(e)
		}
		payload, e := io.ReadAll(response.Body)
		_ = response.Body.Close()
		if e != nil || response.StatusCode != step.status || !bytes.Contains(payload, []byte("applied")) {
			t.Fatal(response.StatusCode, string(payload), e)
		}
		if step.method == "PUT" {
			saved, e := os.ReadFile(filepath.Join(root, "uploaded"))
			if e != nil || string(saved) != step.body {
				t.Fatal(string(saved), e)
			}
		}
	}
	_ = stdin.Close()
	if err = cmd.Wait(); err != nil {
		t.Fatal(err, stderr.String())
	}
}

type shortDriver struct{}

func (d *shortDriver) Stat(context.Context, core.NativeAddress, ...core.ReadSelection) (core.Metadata, error) {
	n := int64(9)
	return core.Metadata{Name: "a", Kind: core.File, Size: &n}, nil
}
func (d *shortDriver) Enumerate(context.Context, core.NativeQuery) (core.Iterator, error) {
	return nil, errors.New("not a directory")
}
func (d *shortDriver) Open(context.Context, core.NativeAddress, core.OpenOptions) (core.Opened, error) {
	n := int64(9)
	return core.Opened{Metadata: core.Metadata{Name: "a", Kind: core.File, Size: &n}, Length: &n, Body: io.NopCloser(strings.NewReader("abc"))}, nil
}
func (d *shortDriver) Close() error { return nil }
func TestCommittedShortReadAbortsTransfer(t *testing.T) {
	c, err := core.New(core.Options{Authorize: func(context.Context, core.Principal, core.Action, core.Address) error {
		return nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = c.Install("/broken", &shortDriver{}); err != nil {
		t.Fatal(err)
	}
	observed := make(chan error, 1)
	h, err := httpapi.New(c, httpapi.Options{Authenticate: func(context.Context, string) (core.Principal, error) { return "reader", nil }, CursorTTL: time.Minute, OnStreamError: func(_ string, err error) { observed <- err }})
	if err != nil {
		t.Fatal(err)
	}

	server := httptest.NewServer(h)
	defer server.Close()
	defer c.Close(context.Background())
	request, err := http.NewRequest("GET", server.URL+fsURL("/broken/a"), nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer token")
	response, err := server.Client().Do(request)
	if err == nil {
		body, readErr := io.ReadAll(response.Body)
		_ = response.Body.Close()
		if readErr == nil {
			t.Fatalf("short response reported success: %q", body)
		}
		if bytes.Contains(body, []byte("request_id")) {
			t.Fatal("Problem JSON appended to content")
		}
	}
	select {
	case err := <-observed:
		if !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("missing stream-failure observation")
	}
}
func TestMissingVerifierFailsClosed(t *testing.T) {
	c, _, _ := setup(t)
	if _, err := httpapi.New(c, httpapi.Options{Authenticate: nil, CursorTTL: time.Minute}); err == nil {
		t.Fatal("accepted a missing verifier")
	}
}

func TestMutationWireAndExistingReaderCompatibility(t *testing.T) {
	c, h, root := setup(t)
	if _, err := c.Install("/readonly", &shortDriver{}); err != nil {
		t.Fatal(err)
	}
	check := func(w *httptest.ResponseRecorder, status int, effect, code string) {
		t.Helper()
		var value map[string]any
		if err := json.Unmarshal(w.Body.Bytes(), &value); err != nil {
			t.Fatal(err, w.Body.String())
		}
		if w.Code != status || value["effect"] != effect {
			t.Fatal(w.Code, value)
		}
		if code != "" && value["code"] != code {
			t.Fatal(value)
		}
	}
	input := strings.NewReader("unread")
	check(request(t, h, "PUT", fsURL("/files/new"), "read-token", nil, input), 403, "unchanged", "permission_denied")
	if input.Len() != 6 {
		t.Fatal("rejected request consumed input")
	}
	check(request(t, h, "PUT", fsURL("/files/new"), "", nil, input), 401, "unchanged", "unauthenticated")
	if input.Len() != 6 {
		t.Fatal("authentication failure consumed input")
	}
	dirs := "/api/v1/fs/directories?path=" + url.QueryEscape("/files/folder")
	check(request(t, h, "POST", dirs, "write-token", nil), 201, "applied", "")
	check(request(t, h, "POST", dirs, "write-token", nil), 409, "unchanged", "already_exists")
	target := fsURL("/files/folder/new")
	check(request(t, h, "PUT", target, "write-token", nil, strings.NewReader("abc")), 201, "applied", "")
	got := request(t, h, "GET", target, "write-token", nil)
	if got.Code != 200 || got.Body.String() != "abc" {
		t.Fatal(got.Code, got.Body.String())
	}
	check(request(t, h, "PUT", target, "write-token", nil, strings.NewReader("overwrite")), 409, "unchanged", "already_exists")
	check(request(t, h, "PUT", target, "write-token", map[string]string{"If-None-Match": "*"}, strings.NewReader("overwrite")), 412, "unchanged", "precondition_failed")
	check(request(t, h, "PUT", fsURL("/files/refused"), "write-token", map[string]string{"If-Match": "*"}, strings.NewReader("unread")), 501, "unchanged", "unsupported_operation")
	if _, err := os.Stat(filepath.Join(root, "refused")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}
	check(request(t, h, "PUT", fsURL("/readonly/new"), "write-token", nil, input), 501, "unchanged", "unsupported_operation")
	if input.Len() != 6 {
		t.Fatal("unsupported mount consumed input")
	}
	failed := io.MultiReader(strings.NewReader("prefix"), iotest.ErrReader(io.ErrNoProgress))
	check(request(t, h, "PUT", fsURL("/files/partial"), "write-token", nil, failed), 502, "incomplete", "upstream_failure")
	data, err := os.ReadFile(filepath.Join(root, "partial"))
	if err != nil || string(data) != "prefix" {
		t.Fatal(string(data), err)
	}
	r := httptest.NewRequest("PUT", fsURL("/files/short"), strings.NewReader("ab"))
	r.Header.Set("Authorization", "Bearer write-token")
	r.ContentLength = 3
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	check(w, 400, "incomplete", "invalid_request")
	r = httptest.NewRequest("PUT", fsURL("/files/encoded"), strings.NewReader("unread"))
	r.Header.Set("Authorization", "Bearer write-token")
	r.Header.Add("Content-Encoding", "identity")
	r.Header.Add("Content-Encoding", "gzip")
	w = httptest.NewRecorder()
	h.ServeHTTP(w, r)
	check(w, 400, "unchanged", "invalid_request")
	removeDir := "/api/v1/fs/entry?path=" + url.QueryEscape("/files/folder")
	check(request(t, h, "DELETE", removeDir, "write-token", nil), 409, "unchanged", "directory_not_empty")
	removeFile := "/api/v1/fs/entry?path=" + url.QueryEscape("/files/folder/new")
	check(request(t, h, "DELETE", removeFile, "write-token", nil), 200, "applied", "")
	check(request(t, h, "DELETE", removeDir, "write-token", nil), 200, "applied", "")
	check(request(t, h, "DELETE", removeFile, "write-token", nil), 404, "unchanged", "not_found")
	info := request(t, h, "GET", "/api/v1/info", "write-token", nil)
	var capabilities struct {
		ReadOnly   bool     `json:"file_operations_read_only"`
		Operations []string `json:"operations"`
	}
	if err = json.Unmarshal(info.Body.Bytes(), &capabilities); err != nil || capabilities.ReadOnly || !slices.Contains(capabilities.Operations, "write") || !slices.Contains(capabilities.Operations, "copy_tree") {
		t.Fatal(capabilities, err)
	}
}

type receiptDriver struct{ shortDriver }

func (d *receiptDriver) Mkdir(context.Context, core.NativeAddress) (core.Change, error) {
	return core.Change{Effect: core.Applied}, core.Fail(core.UpstreamFailure, errors.New("private-provider-diagnostic"))
}
func (d *receiptDriver) Create(_ context.Context, _ core.NativeAddress, input io.Reader, _ *int64) (core.Change, error) {
	if _, err := io.Copy(io.Discard, input); err != nil {
		return core.Change{Effect: core.Unknown}, err
	}
	return core.Change{Effect: core.Applied}, core.Fail(core.AlreadyExists, errors.New("cleanup conflict after commit"))
}

func TestMutationProblemPreservesAppliedEffect(t *testing.T) {
	c, h, _ := setup(t)
	if _, err := c.Install("/receipt", &receiptDriver{}); err != nil {
		t.Fatal(err)
	}
	w := request(t, h, "POST", "/api/v1/fs/directories?path=%2Freceipt%2Fnew", "write-token", nil)
	var problem map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &problem); err != nil || w.Code != 502 || problem["effect"] != "applied" {
		t.Fatal(w.Code, problem, err)
	}
	if strings.Contains(w.Body.String(), "private-provider") {
		t.Fatal("provider cause leaked")
	}
	w = request(t, h, "PUT", fsURL("/receipt/new"), "write-token", map[string]string{"If-None-Match": "*"}, strings.NewReader(""))
	if err := json.Unmarshal(w.Body.Bytes(), &problem); err != nil || w.Code != 409 || problem["effect"] != "applied" {
		t.Fatal("confirmed effect misreported as a failed precondition", w.Code, problem, err)
	}
}
