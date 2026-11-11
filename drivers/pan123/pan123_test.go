package pan123

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	core "github.com/nostalume/mountweave"
)

type wire func(*http.Request) (*http.Response, error)

func (f wire) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type fixture struct {
	mu                sync.Mutex
	mode              string
	calls             []string
	content, redirect string
	entered           chan struct{}
	bodyCalls         atomic.Int32
	broken            *badClose
}

func (s *fixture) set(mode string) { s.mu.Lock(); s.mode = mode; s.mu.Unlock() }
func (s *fixture) state() string   { s.mu.Lock(); defer s.mu.Unlock(); return s.mode }
func (s *fixture) count(part string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, call := range s.calls {
		if strings.Contains(call, part) {
			n++
		}
	}
	return n
}
func (s *fixture) control(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("Authorization") != "Bearer synthetic" || r.Header.Get("Platform") != "open_platform" {
		http.Error(w, "missing control authentication", 401)
		return
	}
	s.mu.Lock()
	s.calls = append(s.calls, r.URL.RequestURI())
	mode := s.mode
	s.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	if mode == "native-error" {
		fmt.Fprint(w, `{"code":5113,"message":"download entitlement exhausted","x-traceID":"trace-native","data":null}`)
		return
	}
	q := r.URL.Query()
	if q.Has("searchData") {
		http.Error(w, "global search escaped root", 500)
		return
	}
	reply := func(data any) {
		json.NewEncoder(w).Encode(map[string]any{"code": 0, "message": "ok", "data": data, "x-traceID": "trace-ok"})
	}
	switch r.URL.Path {
	case "/api/v1/file/trash":
		var fields map[string]json.RawMessage
		if r.Method != http.MethodPost || r.Header.Get("Content-Type") != "application/json" || json.NewDecoder(r.Body).Decode(&fields) != nil ||
			len(fields) != 1 || string(fields["fileIDs"]) != `[9007199254740993]` {
			http.Error(w, "unexpected trash request", 500)
			return
		}
		switch mode {
		case "trash-refused":
			w.WriteHeader(http.StatusConflict)
			fmt.Fprint(w, `{"code":5999,"data":null}`)
		case "trash-truncated":
			fmt.Fprint(w, `{"code":0,"data":`)
		case "trash-wait":
			s.entered <- struct{}{}
			<-r.Context().Done()
		default:
			reply(nil)
		}
		return
	case "/upload/v1/file/mkdir":
		var fields map[string]json.RawMessage
		if r.Method != http.MethodPost || r.Header.Get("Content-Type") != "application/json" || json.NewDecoder(r.Body).Decode(&fields) != nil ||
			len(fields) != 2 || string(fields["name"]) != `"folder%中"` || string(fields["parentID"]) != "1" {
			http.Error(w, "unexpected mkdir request", 500)
			return
		}
		switch mode {
		case "mkdir-refused":
			w.WriteHeader(http.StatusConflict)
			fmt.Fprint(w, `{"code":5999,"data":null}`)
		case "mkdir-parent-id":
			reply(map[string]any{"dirID": 1})
		default:
			reply(map[string]any{"dirID": json.Number("9007199254740997")})
		}
		return
	case "/upload/v2/file/domain":
		if mode == "unsafe-upload" {
			reply([]string{"http://openapi-upload.123pan.com", "https://user:secret@upload.invalid", "https://upload.invalid/?cap=secret"})
			return
		}
		reply([]string{"https://openapi-upload.123pan.com"})
		return
	case "/api/v1/file/detail":
		id := q.Get("fileID")
		typ := 1
		trashed := 0
		if id == "0" {
			http.Error(w, "root details are not promised", 500)
			return
		}
		if id == "10" {
			typ = 0
		}
		if mode == "trashed-root" {
			trashed = 1
		}
		reply(map[string]any{"fileID": json.Number(id), "parentFileID": 0, "filename": "archive", "type": typ, "size": 6, "trashed": trashed})
		return
	case directLinkRoute:
		if r.Method != http.MethodGet || len(q) != 1 || q.Get("fileID") != "9007199254740993" {
			http.Error(w, "wrong direct-link request", http.StatusBadRequest)
			return
		}
		if mode == "direct-denied" {
			fmt.Fprint(w, `{"code":5113,"message":"direct-secret","x-traceID":"direct-secret","data":null}`)
			return
		}
		suffix := "/blob"
		if mode == "expired" {
			suffix = "/expired"
		}
		reply(map[string]any{"url": s.content + suffix + "?cap=direct-secret%2B", "downloadUrl": "http://unexpected.invalid/"})
		return
	case "/api/v1/file/download_info":
		if mode == "grant-quota" {
			fmt.Fprint(w, `{"code":5113,"message":"https://body.invalid/file?cap=private-capability","x-traceID":"private-capability","data":null}`)
			return
		}
		if q.Get("fileId") != "9007199254740993" {
			http.Error(w, "wrong selected ID", 500)
			return
		}
		suffix := "/blob"
		for _, candidate := range []string{"fallback", "redirect", "body-wait", "expired"} {
			if mode == candidate {
				suffix = "/" + candidate
			}
		}
		target := s.content + suffix + "?cap=private-capability"
		if mode == "unsafe" {
			target = "http://example.invalid/body"
		}
		reply(map[string]any{"downloadUrl": target})
		return
	case "/api/v2/file/list":
	default:
		http.Error(w, "unexpected control route", 500)
		return
	}
	if mode == "wait" {
		select {
		case s.entered <- struct{}{}:
		default:
		}
		<-r.Context().Done()
		return
	}
	if mode == "truncated" {
		fmt.Fprint(w, `{"code":0,"data":`)
		return
	}
	parent := q.Get("parentFileId")
	after := q.Get("lastFileId")
	id := func(v string) json.Number { return json.Number(v) }
	f := map[string]any{"fileId": id("9007199254740993"), "filename": "p%+中", "parentFileId": id(parent), "type": 0, "size": 6, "trashed": 0, "etag": "not-a-version"}
	next := int64(-1)
	items := []any{}
	if parent == "0" {
		items = append(items, map[string]any{"fileId": 1, "filename": "archive", "parentFileId": 0, "type": 1, "size": 0, "trashed": 0})
	} else if parent == "1" {
		switch after {
		case "":
			next = 0
			items = append(items, map[string]any{"fileId": 8, "filename": "deleted", "parentFileId": 1, "type": 0, "size": 6, "trashed": 1})
		case "0":
			next = 20
			items = append(items, f)
		case "20":
			items = append(items, map[string]any{"fileId": 2, "filename": "sub", "parentFileId": 1, "type": 1, "size": 0, "trashed": 0})
		default:
			http.Error(w, "invented native marker", 500)
			return
		}
	}
	if mode == "duplicate" && after == "20" {
		copy := map[string]any{"fileId": 42, "filename": "p%+中", "parentFileId": 1, "type": 0, "size": 6, "trashed": 0}
		items = []any{copy}
	}
	if mode == "repeated" && after == "20" {
		items = []any{f}
	}
	if mode == "wrong-parent" {
		f["parentFileId"] = 99
	}
	if mode == "wrong-type" {
		f["type"] = 7
	}
	if mode == "negative" {
		f["size"] = -1
	}
	if mode == "bad-name" {
		f["filename"] = "a/b"
	}
	if mode == "stalled" && after == "0" {
		next = 0
	}
	if mode == "cycle" && parent == "1" {
		items = items[:0]
		if after == "20" {
			next = 0
		}
	}
	data := map[string]any{"lastFileId": next, "fileList": items}
	if mode == "missing-cursor" {
		delete(data, "lastFileId")
	}
	reply(data)
}
func (s *fixture) body(w http.ResponseWriter, r *http.Request) {
	s.bodyCalls.Add(1)
	if r.Header.Get("Authorization") != "" || r.Header.Get("Cookie") != "" || r.Header.Get("Referer") != "" || r.Header.Get("Platform") != "" {
		http.Error(w, "control information leaked", 500)
		return
	}
	if r.URL.Path == "/redirect" {
		w.Header().Set("Location", s.redirect+"/blob")
		w.WriteHeader(302)
		return
	}
	if r.URL.Path == "/body-wait" {
		w.Header().Set("Content-Length", "6")
		w.WriteHeader(200)
		w.(http.Flusher).Flush()
		select {
		case s.entered <- struct{}{}:
		default:
		}
		<-r.Context().Done()
		return
	}
	if r.URL.Path == "/expired" {
		http.Error(w, "capability expired", 403)
		return
	}
	if s.state() == "unsolicited-range-rejection" {
		w.Header().Set("Content-Range", "bytes */6")
		w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
		return
	}
	data := "abcdef"
	status := 200
	if raw := r.Header.Get("Range"); raw != "" && r.URL.Path != "/fallback" {
		var start, end int
		if _, err := fmt.Sscanf(raw, "bytes=%d-%d", &start, &end); err != nil {
			http.Error(w, "invalid fixture range", 500)
			return
		}
		if start >= len(data) {
			w.Header().Set("Content-Range", "bytes */6")
			w.WriteHeader(416)
			return
		}
		end = min(end, len(data)-1)
		data = data[start : end+1]
		status = 206
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/6", start, end))
	}
	w.Header().Set("Content-Length", strconv.Itoa(len(data)))
	w.WriteHeader(status)
	io.WriteString(w, data)
}

type badClose struct {
	io.ReadCloser
	remaining, calls int
	cause            error
}

func (b *badClose) Close() error {
	b.calls++
	if b.remaining > 0 {
		b.remaining--
		if b.cause != nil {
			return b.cause
		}
		return io.EOF
	}
	return b.ReadCloser.Close()
}
func setup(t *testing.T, configure ...func(*Options)) (*fixture, *Driver, *core.Core, *core.Mount, Options) {
	t.Helper()
	s := &fixture{entered: make(chan struct{}, 2)}
	body := httptest.NewTLSServer(http.HandlerFunc(s.body))
	receiver := httptest.NewTLSServer(http.HandlerFunc(s.body))
	s.content, s.redirect = body.URL, receiver.URL
	t.Cleanup(body.Close)
	t.Cleanup(receiver.Close)
	api := httptest.NewServer(http.HandlerFunc(s.control))
	t.Cleanup(api.Close)
	base, _ := url.Parse(api.URL)
	client := &http.Client{Transport: wire(func(r *http.Request) (*http.Response, error) {
		if r.URL.Scheme != "https" || r.URL.Host != "open-api.123pan.com" {
			return nil, errors.New("control recipient changed")
		}
		call := r.Clone(r.Context())
		u := *r.URL
		u.Scheme, u.Host = base.Scheme, base.Host
		call.URL = &u
		call.Host = ""
		call.Header.Set("Authorization", "Bearer synthetic")
		if s.state() == "connection-eof" {
			return nil, io.EOF
		}
		response, err := http.DefaultTransport.RoundTrip(call)
		if err == nil && s.state() == "close" {
			s.broken = &badClose{ReadCloser: response.Body, remaining: 1}
			response.Body = s.broken
		}
		return response, err
	})}
	opts := Options{RootID: "1", Client: client, ContentClient: body.Client(), MaxResponseBytes: 4096}
	for _, apply := range configure {
		apply(&opts)
	}
	d, err := Open(context.Background(), opts)
	if err != nil {
		t.Fatal(err)
	}
	c, err := core.New(core.Options{Authorize: func(_ context.Context, _ core.Principal, _ core.Action, a core.Address) error {
		if strings.HasSuffix(a.Value(), "/denied") {
			return core.Fail(core.PermissionDenied, nil)
		}
		return nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	m, err := c.Install("/remote", d)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := c.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	return s, d, c, m, opts
}

func TestRootsPagesAndAuthorizedBody(t *testing.T) {
	s, d, c, m, opts := setup(t)
	ctx := context.Background()
	defer func() {
		if err := c.Close(ctx); err != nil {
			t.Error(err)
		}
	}()
	if err := c.BindAlias(ctx, "/alias", m, "/"); err != nil {
		t.Fatal(err)
	}
	details := s.count("/detail")
	uploadClient := &http.Client{Transport: opts.Client.Transport}
	for _, shared := range []*http.Client{opts.Client, uploadClient} {
		invalid := opts
		invalid.ContentClient, invalid.UploadClient, invalid.MaxUploadBytes = shared, uploadClient, 1
		driver, err := Open(ctx, invalid)
		if driver != nil {
			_ = driver.Close()
		}
		if driver != nil || core.CodeOf(err) != core.InvalidRequest || s.count("/detail") != details {
			t.Fatal("shared authenticated client passed constructor admission", driver != nil, err)
		}
	}
	opts.RootID, opts.RootPath = "", "/archive"
	second, err := Open(ctx, opts)
	if err != nil {
		t.Fatal(err)
	}
	if second == d || second.root.ID != d.root.ID || s.count("/detail") != details {
		t.Fatal("root path repeated details or shared instance")
	}
	if err := second.Close(); err != nil {
		t.Fatal(err)
	}
	opts.RootPath, opts.ContentClient = "", nil
	account, err := Open(ctx, opts)
	if err != nil {
		t.Fatal(err)
	}
	if account.content.Transport != nil {
		t.Fatal("default content client inherited the authenticated transport")
	}
	if meta, err := account.Stat(ctx, core.NativePath("/")); err != nil || meta.Kind != core.Directory {
		t.Fatal(meta, err)
	}
	if err := account.Close(); err != nil {
		t.Fatal(err)
	}
	req := core.ListRequest{Query: core.Children(core.Path("/alias")), Limit: 1, CursorTTL: time.Minute}
	empty, err := c.List(ctx, "u", req)
	if err != nil || len(empty.Items) != 0 || empty.NextCursor == nil {
		t.Fatal("recycle-only page lost continuation", empty, err)
	}
	req.Cursor = *empty.NextCursor
	first, err := c.List(ctx, "u", req)
	if err != nil || len(first.Items) != 1 || first.Items[0].Name != "p%+中" || first.NextCursor == nil {
		t.Fatal(first, err)
	}
	req.Cursor = *first.NextCursor
	secondPage, err := c.List(ctx, "u", req)
	if err != nil || len(secondPage.Items) != 1 || secondPage.Items[0].Name != "sub" {
		t.Fatal(secondPage, err)
	}
	calls := s.count("/list")
	replay, err := c.List(ctx, "u", req)
	if err != nil || len(replay.Items) != 1 || s.count("/list") != calls {
		t.Fatal("replay fetched native page", replay, err)
	}
	meta, err := c.Stat(ctx, "u", core.Path("/remote/p%+中"))
	if err != nil || meta.Kind != core.File || meta.Size == nil || *meta.Size != 6 || meta.Evidence.Validator.Tag != "" {
		t.Fatal(meta, err)
	}
	absent := core.OpenOptions{Selection: core.ReadSelection{When: core.IfAbsent()}}
	if _, err := c.Open(ctx, "u", core.Path("/remote/missing"), absent); core.CodeOf(err) != core.NotFound {
		t.Fatalf("condition invented a file: %v", err)
	}
	if _, err := c.Open(ctx, "u", core.Path("/remote/p%+中"), absent); core.CodeOf(err) != core.PreconditionFailed {
		t.Fatalf("existing file did not fail the absence condition: %v", err)
	}
	before := s.count("/detail")
	opened, err := c.Open(ctx, "u", core.Path("/alias/p%+中"), core.OpenOptions{Range: &core.Range{Start: 1, End: 3}})
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(opened.Body)
	err = errors.Join(err, opened.Body.Close())
	if err != nil || string(data) != "bcd" || opened.Range == nil || s.count("/detail") != before {
		t.Fatal("range or repeated details", string(data), opened.Range, err)
	}
	s.set("redirect")
	opened, err = c.Open(ctx, "u", core.Path("/remote/p%+中"), core.OpenOptions{Range: &core.Range{Start: 1, End: 3}})
	if err != nil {
		t.Fatal(err)
	}
	data, err = io.ReadAll(opened.Body)
	err = errors.Join(err, opened.Body.Close())
	if err != nil || string(data) != "bcd" || opened.Range == nil {
		t.Fatal(string(data), err)
	}
	s.set("fallback")
	opened, err = c.Open(ctx, "u", core.Path("/remote/p%+中"), core.OpenOptions{Range: &core.Range{Start: 1, End: 3}})
	if err != nil {
		t.Fatal(err)
	}
	data, err = io.ReadAll(opened.Body)
	err = errors.Join(err, opened.Body.Close())
	if err != nil || string(data) != "abcdef" || opened.Range != nil {
		t.Fatal(string(data), err)
	}
	s.set("")
	if _, err = c.Open(ctx, "u", core.Path("/remote/p%+中"), core.OpenOptions{Range: &core.Range{Start: 9, End: 11}}); core.CodeOf(err) != core.RangeNotSatisfiable {
		t.Fatal(err)
	}
	opened, err = c.Open(ctx, "u", core.Path("/remote/p%+中"), core.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	data, err = io.ReadAll(opened.Body)
	if err = errors.Join(err, opened.Body.Close()); err != nil || string(data) != "abcdef" || opened.Range != nil {
		t.Fatal("complete original", string(data), err)
	}
	calls = s.count("/list")
	if _, err = c.Stat(ctx, "u", core.Path("/remote/denied")); core.CodeOf(err) != core.PermissionDenied || s.count("/list") != calls {
		t.Fatal("unauthorized native request", err)
	}
	if change, err := c.Mkdir(ctx, "u", core.Path("/remote/folder%中")); change.Effect != core.Unchanged || core.CodeOf(err) != core.UnsupportedOperation || s.count("/mkdir") != 0 {
		t.Fatal("default directory creation was not disabled", change, err)
	}
	if change, err := c.Remove(ctx, "u", core.Path("/remote/p%+中")); change.Effect != core.Unchanged || core.CodeOf(err) != core.UnsupportedOperation || s.count("/trash") != 0 || s.count("/list") != calls {
		t.Fatal("default file removal was not disabled", change, err)
	}
}

func TestNativeDifferencesAndFailureBoundaries(t *testing.T) {
	s, d, c, _, opts := setup(t, func(o *Options) { o.AllowMkdir, o.AllowRemove = true, true })
	ctx := context.Background()
	if change, err := c.Mkdir(ctx, "u", core.Path("/remote/denied")); change.Effect != core.Unchanged || core.CodeOf(err) != core.PermissionDenied || s.count("/mkdir") != 0 {
		t.Fatal("unauthorized mkdir request", change, err)
	}
	lists := s.count("/list")
	for index, tc := range []struct {
		mode   string
		effect core.Effect
		code   core.Code
	}{{"", core.Applied, ""}, {"mkdir-refused", core.Unknown, core.UpstreamFailure}, {"mkdir-parent-id", core.Unknown, core.InvalidUpstreamResponse}} {
		s.set(tc.mode)
		change, err := c.Mkdir(ctx, "u", core.Path("/remote/folder%中"))
		if change.Effect != tc.effect || (tc.code == "") != (err == nil) || err != nil && core.CodeOf(err) != tc.code ||
			change.Effect == core.Applied && change.Before != core.TargetMissing || s.count("/mkdir") != index+1 || s.count("/list") != lists {
			t.Fatal(tc.mode, change, err)
		}
	}
	file := core.Path("/remote/p%+中")
	if change, err := c.Remove(ctx, "u", core.Path("/remote/denied")); change.Effect != core.Unchanged || core.CodeOf(err) != core.PermissionDenied || s.count("/list") != lists {
		t.Fatal("unauthorized trash request", change, err)
	}
	for _, policy := range []core.RemoveOptions{{When: core.IfPresent()}, {Target: core.RemoveNonDirectory}} {
		if change, err := c.Remove(ctx, "u", file, policy); change.Effect != core.Unchanged || core.CodeOf(err) != core.UnsupportedOperation || s.count("/list") != lists {
			t.Fatal(policy, change, err)
		}
	}
	if change, err := c.Remove(ctx, "u", core.Path("/remote/sub")); change.Effect != core.Unchanged || core.CodeOf(err) != core.UnsupportedOperation || s.count("/trash") != 0 {
		t.Fatal("directory was not refused", change, err)
	}
	for index, tc := range []struct {
		mode   string
		effect core.Effect
		code   core.Code
	}{{"", core.Applied, ""}, {"trash-refused", core.Unknown, core.UpstreamFailure}, {"trash-truncated", core.Unknown, core.InvalidUpstreamResponse}} {
		s.set(tc.mode)
		change, err := c.Remove(ctx, "u", file)
		if change.Effect != tc.effect || change.Before != core.TargetUnknown || (tc.code == "") != (err == nil) ||
			err != nil && core.CodeOf(err) != tc.code || s.count("/trash") != index+1 {
			t.Fatal(tc.mode, change, err)
		}
	}
	s.set("trash-wait")
	call, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		change, err := c.Remove(call, "u", file)
		if change.Effect != core.Unknown {
			t.Errorf("canceled removal effect: %v", change)
		}
		done <- err
	}()
	select {
	case <-s.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("trash did not start")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) || s.count("/trash") != 4 {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("trash did not cancel")
	}
	for _, test := range []struct {
		mode string
		code core.Code
	}{{"duplicate", core.PathAmbiguous}, {"repeated", core.InvalidUpstreamResponse}, {"wrong-parent", core.InvalidUpstreamResponse}, {"wrong-type", core.InvalidUpstreamResponse}, {"negative", core.InvalidUpstreamResponse}, {"bad-name", core.UnrepresentableName}, {"stalled", core.InvalidUpstreamResponse}, {"missing-cursor", core.InvalidUpstreamResponse}, {"truncated", core.InvalidUpstreamResponse}, {"connection-eof", core.UpstreamFailure}} {
		s.set(test.mode)
		_, err := c.Stat(ctx, "u", core.Path("/remote/p%+中"))
		if core.CodeOf(err) != test.code || errors.Is(err, io.EOF) {
			t.Fatal(test.mode, err)
		}
	}
	s.set("cycle")
	d.resolvePages = 4
	beforeCycle := s.count("/list")
	if _, err := c.Stat(ctx, "u", file); core.CodeOf(err) != core.InvalidUpstreamResponse || s.count("/list") != beforeCycle+4 {
		t.Fatal("native cursor cycle was not rejected", err)
	}
	d.resolvePages = 0
	s.set("native-error")
	_, err := c.Stat(ctx, "u", core.Path("/remote/p%+中"))
	if core.CodeOf(err) != core.UpstreamAccessDenied || !strings.Contains(err.Error(), "5113") || !strings.Contains(err.Error(), "trace-native") || !strings.Contains(err.Error(), "entitlement") {
		t.Fatal(err)
	}
	s.set("trashed-root")
	failed, err := Open(ctx, opts)
	if core.CodeOf(err) != core.NotFound || failed == nil {
		t.Fatal(failed, err)
	}
	if err := failed.Close(); err != nil {
		t.Fatal(err)
	}
	s.set("")
	limited := opts
	limited.MaxResolvePages = 1
	limited.RootID, limited.RootPath = "", "/archive"
	source, err := Open(ctx, limited)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := source.Stat(ctx, core.NativePath("/p%+中")); core.CodeOf(err) != core.ResolutionBudgetExceeded {
		t.Fatal(err)
	}
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}
	oldLimit := d.limit
	d.limit = 32
	if _, err := c.Stat(ctx, "u", core.Path("/remote/p%+中")); core.CodeOf(err) != core.ResponseBudgetExceeded {
		t.Fatal(err)
	}
	d.limit = oldLimit
	s.set("unsafe")
	before := s.bodyCalls.Load()
	if _, err := c.Open(ctx, "u", core.Path("/remote/p%+中"), core.OpenOptions{}); core.CodeOf(err) != core.InvalidUpstreamResponse || s.bodyCalls.Load() != before {
		t.Fatal(err)
	}
	s.set("grant-quota")
	if _, err := c.Open(ctx, "u", file, core.OpenOptions{}); core.CodeOf(err) != core.UpstreamThrottled ||
		!strings.Contains(err.Error(), "5113") || strings.Contains(err.Error(), "private-capability") || s.bodyCalls.Load() != before {
		t.Fatal("ordinary download quota lost its code or disclosed the URL", err)
	}
	s.set("unsolicited-range-rejection")
	if _, err := c.Open(ctx, "u", file, core.OpenOptions{}); core.CodeOf(err) != core.InvalidUpstreamResponse {
		t.Fatal("unrequested range rejection", err)
	}
	s.set("")
	calls := s.count("/list")
	if _, err := c.Open(ctx, "u", core.Path("/remote/p%+中"), core.OpenOptions{NoFollow: true}); core.CodeOf(err) != core.UnsupportedOperation || calls != s.count("/list") {
		t.Fatal(err)
	}
	s.set("close")
	if _, err := c.Stat(ctx, "u", core.Path("/remote/p%+中")); err == nil || errors.Is(err, io.EOF) {
		t.Fatal("cleanup was false EOF", err)
	}
	broken := s.broken
	s.set("")
	if err := d.Close(); err != nil || broken.calls != 2 {
		t.Fatal("cleanup ownership", broken.calls, err)
	}
}

func TestCancellationRetirementAndRedirectCleanup(t *testing.T) {
	s, d, c, m, opts := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	stream, err := d.Enumerate(ctx, core.NativeChildren(core.NativePath("/")))
	if err != nil {
		t.Fatal(err)
	}
	s.set("wait")
	call, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { _, err := stream.Next(call, 1); done <- err }()
	select {
	case <-s.entered:
	case <-ctx.Done():
		t.Fatal("list did not start")
	}
	stop()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("list cancellation did not join")
	}
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
	base := d.content.Transport
	d.content.Transport = wire(func(r *http.Request) (*http.Response, error) {
		if s.state() == "signed-failure" {
			return nil, fmt.Errorf("could not open %s: %w", r.URL, io.EOF)
		}
		response, err := base.RoundTrip(r)
		if err == nil && r.URL.Path == "/redirect" {
			s.broken = &badClose{ReadCloser: response.Body, remaining: 1, cause: fmt.Errorf("closing %s: %w", r.URL, io.EOF)}
			response.Body = s.broken
		}
		return response, err
	})
	s.set("signed-failure")
	path := core.Path("/remote/p%+中")
	if _, err := c.Open(ctx, "u", path, core.OpenOptions{}); !errors.Is(err, io.ErrUnexpectedEOF) || strings.Contains(err.Error(), "private-capability") {
		t.Fatal("download failure lost its cause or disclosed its signed URL", err)
	}
	s.set("redirect")
	before := s.bodyCalls.Load()
	if _, err := c.Open(ctx, "u", path, core.OpenOptions{}); !errors.Is(err, io.ErrUnexpectedEOF) || strings.Contains(err.Error(), "private-capability") ||
		s.bodyCalls.Load() != before+1 || s.broken.calls != 1 {
		t.Fatal("redirect cleanup or subsequent request", err)
	}
	d.content.Transport = base

	s.set("")
	opts.UseDirectLinks = true
	directContent := *opts.ContentClient
	var terminal *badClose
	directContent.Transport = wire(func(r *http.Request) (*http.Response, error) {
		if r.URL.RawQuery != "cap=direct-secret%2B" {
			t.Error("signed direct-link URL changed")
		}
		response, err := base.RoundTrip(r)
		if err == nil && r.URL.Path == "/expired" {
			terminal = &badClose{ReadCloser: response.Body, remaining: 1, cause: fmt.Errorf("closing %s: %w", r.URL, io.EOF)}
			response.Body = terminal
		}
		return response, err
	})
	opts.ContentClient = &directContent
	direct, err := Open(ctx, opts)
	if err != nil {
		t.Fatal(err)
	}
	directMount, err := c.Install("/direct", direct)
	if err != nil {
		t.Fatal(errors.Join(err, direct.Close()))
	}
	ordinary := s.count("/download_info")
	address := core.Path("/direct/p%+中")
	got, err := c.Open(ctx, "u", address, core.OpenOptions{Range: &core.Range{Start: 1, End: 3}})
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(got.Body)
	if err := errors.Join(err, got.Body.Close()); err != nil || string(data) != "bcd" || got.Range == nil {
		t.Fatalf("direct-link body or range changed: %q %v", data, err)
	}
	s.set("direct-denied")
	before = s.bodyCalls.Load()
	if _, err := c.Open(ctx, "u", address, core.OpenOptions{}); core.CodeOf(err) != core.UpstreamAccessDenied ||
		strings.Contains(err.Error(), "direct-secret") || s.bodyCalls.Load() != before {
		t.Fatalf("direct-link refusal was ignored or disclosed credentials: %v", err)
	}
	s.set("expired")
	if _, err := c.Open(ctx, "u", address, core.OpenOptions{}); !errors.Is(err, io.ErrUnexpectedEOF) ||
		strings.Contains(err.Error(), "direct-secret") || terminal == nil || terminal.calls != 1 {
		t.Fatalf("failed direct-link body escaped its private cleanup: %v", err)
	}
	if err := directMount.Retire(ctx); err != nil || terminal.calls != 2 || s.count(directLinkRoute) != 3 || s.count("/download_info") != ordinary ||
		s.count("/direct-link/enable") != 0 {
		t.Fatalf("direct-link retirement, mutation or fallback: %v", err)
	}

	s.set("body-wait")
	opened, err := c.Open(ctx, "u", core.Path("/remote/p%+中"), core.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-s.entered:
	case <-ctx.Done():
		t.Fatal("body did not start")
	}
	go func() { _, err := io.ReadAll(opened.Body); done <- err }()
	if err := m.Retire(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("retirement did not join body")
	}
	if err := opened.Body.Close(); err != nil {
		t.Fatal(err)
	}
	if s.broken.calls != 2 {
		t.Fatal("intermediate body cleanup did not retry once", s.broken.calls)
	}
}
