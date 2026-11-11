package graphdrive_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	core "github.com/nostalume/mountweave"
	driver "github.com/nostalume/mountweave/drivers/graphdrive"
	"github.com/nostalume/mountweave/input"
)

const chunk = 320 << 10

type transport func(*http.Request) (*http.Response, error)

type retryClose struct {
	io.ReadCloser
	fault error
	calls int
}

func (b *retryClose) Close() error {
	b.calls++
	if b.calls == 1 {
		return b.fault
	}
	return b.ReadCloser.Close()
}

func (f transport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
func reply(r *http.Request, code int, body any) *http.Response {
	data, _ := json.Marshal(body)
	return &http.Response{Request: r, StatusCode: code, Header: make(http.Header), Body: io.NopCloser(bytes.NewReader(data)), ContentLength: int64(len(data))}
}

type item struct {
	id, parent, name string
	folder           bool
	data             []byte
	url              string
}

func (e *item) metadata() map[string]any {
	out := map[string]any{"id": e.id, "name": e.name, "size": len(e.data), "parentReference": map[string]string{"id": e.parent, "driveId": "d"}}
	if e.folder {
		out["folder"] = map[string]any{}
	} else {
		out["file"] = map[string]any{}
	}
	if e.url != "" {
		out["@microsoft.graph.downloadUrl"] = e.url
	}
	return out
}

type session struct {
	parent, name string
	data         []byte
	total        int64
	lost         bool
	holes        []core.Range
}

func (u *session) progress() []string {
	if len(u.holes) != 0 {
		ranges := make([]string, len(u.holes))
		for i, hole := range u.holes {
			ranges[i] = fmt.Sprintf("%d-%d", hole.Start, hole.End)
		}
		return ranges
	}
	if int64(len(u.data)) == u.total {
		return []string{}
	}
	return []string{fmt.Sprintf("%d-", len(u.data))}
}

type graphProtocol struct {
	mu                                                              sync.Mutex
	items                                                           map[string]*item
	sessions                                                        map[string]*session
	next, apiCalls, bodyCalls, badAPI, queries, finals, pathDeletes int
	poisonLink, failAbort                                           bool
	started, exited                                                 chan struct{}
	late                                                            io.ReadCloser
	api                                                             *http.Client
	puts, deletes, uploadedBytes, initiations                       int
}

func newProtocol() *graphProtocol {
	s := &graphProtocol{items: map[string]*item{}, sessions: map[string]*session{}}
	s.items["root"] = &item{id: "root", name: "drive", folder: true}
	s.items["vault"] = &item{id: "vault", parent: "root", name: "vault", folder: true}
	s.items["a"] = &item{id: "a", parent: "vault", name: "a.txt", data: []byte("alpha")}
	s.items["b"] = &item{id: "b", parent: "vault", name: "b.txt", data: []byte("beta"), url: "https://content.test/file/b"}
	return s
}
func (s *graphProtocol) child(parent, name string) *item {
	for _, e := range s.items {
		if e.parent == parent && strings.EqualFold(e.name, name) {
			return e
		}
	}
	return nil
}
func (s *graphProtocol) publish(parent, name string, data []byte) *item {
	s.next++
	e := &item{id: fmt.Sprintf("new%d", s.next), parent: parent, name: name, data: bytes.Clone(data)}
	s.items[e.id] = e
	return e
}
func (s *graphProtocol) roundTrip(r *http.Request, api bool) (*http.Response, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if api {
		s.apiCalls++
		if r.Method == http.MethodDelete {
			s.pathDeletes++
			return nil, errors.New("unexpected path compensation")
		}
		if r.Method == http.MethodPut {
			s.puts++
		}
		if r.URL.Host != "graph.test" {
			s.badAPI++
			return nil, errors.New("authenticated request escaped Graph")
		}
		if r.Header.Get("Authorization") != "Bearer account-secret" {
			return nil, errors.New("missing API authentication")
		}
	} else {
		s.bodyCalls++
		if r.Header.Get("Authorization") != "" || r.Header.Get("Cookie") != "" {
			return nil, errors.New("account authentication reached content")
		}
		if r.URL.Host != "content.test" {
			return nil, errors.New("outside closed content authority")
		}
	}
	missing := func() (*http.Response, error) {
		return reply(r, 404, map[string]any{"error": map[string]string{"code": "itemNotFound"}}), nil
	}
	conflict := func() (*http.Response, error) {
		return reply(r, 409, map[string]any{"error": map[string]string{"code": "nameAlreadyExists"}}), nil
	}
	if !api {
		if strings.HasPrefix(r.URL.Path, "/file/") {
			e := s.items[strings.TrimPrefix(r.URL.Path, "/file/")]
			if e == nil {
				return missing()
			}
			out := reply(r, 200, nil)
			out.Body = io.NopCloser(bytes.NewReader(e.data))
			out.ContentLength = int64(len(e.data))
			return out, nil
		}
		id := strings.TrimPrefix(r.URL.Path, "/session/")
		if r.Method == http.MethodGet {
			s.queries++
		} else if r.Method == http.MethodDelete {
			s.deletes++
		}
		u := s.sessions[id]
		if u == nil {
			return missing()
		}
		switch r.Method {
		case "GET":
			return reply(r, 200, map[string]any{"nextExpectedRanges": u.progress()}), nil
		case "DELETE":
			if s.failAbort {
				return reply(r, 503, map[string]any{}), nil
			}
			delete(s.sessions, id)
			return reply(r, 204, nil), nil
		case "PUT":
			s.puts++
			if u.name == "cancel" {
				s.late = r.Body
				close(s.started)
				s.mu.Unlock()
				<-r.Context().Done()
				s.mu.Lock()
				close(s.exited)
				return nil, r.Context().Err()
			}
			var start, end, total int64
			if _, err := fmt.Sscanf(r.Header.Get("Content-Range"), "bytes %d-%d/%d", &start, &end, &total); err != nil || total != u.total {
				return nil, errors.New("incorrect range authority")
			}
			if len(u.holes) != 0 {
				if start != u.holes[0].Start || end > u.holes[0].End {
					return nil, errors.New("upload left its requested hole")
				}
			} else if start != int64(len(u.data)) {
				return nil, errors.New("upload left its confirmed prefix")
			}
			data, err := io.ReadAll(r.Body)
			if err != nil {
				return nil, err
			}
			if int64(len(data)) != end-start+1 {
				return nil, errors.New("incorrect range length")
			}
			s.uploadedBytes += len(data)
			complete := end+1 == total
			if len(u.holes) != 0 {
				copy(u.data[start:end+1], data)
				u.holes[0].Start = end + 1
				if u.holes[0].Start > u.holes[0].End {
					u.holes = u.holes[1:]
				}
				complete = len(u.holes) == 0
			} else {
				u.data = append(u.data, data...)
			}
			if complete && u.name == "final-hole" && !u.lost {
				u.lost = true
				u.holes = []core.Range{{Start: 5 * chunk / 4, End: 7*chunk/4 - 1}}
				clear(u.data[5*chunk/4 : 7*chunk/4])
				complete = false
			}
			if complete {
				s.finals++
				if u.name == "race" {
					s.publish(u.parent, u.name, []byte("peer"))
					return conflict()
				}
				e := s.publish(u.parent, u.name, u.data)
				delete(s.sessions, id)
				if u.name == "terminal" {
					return nil, errors.New("final acknowledgement lost")
				}
				return reply(r, 201, e.metadata()), nil
			}
			if u.name == "lost" && !u.lost {
				u.lost = true
				return nil, errors.New("fragment acknowledgement lost")
			}
			return reply(r, 202, map[string]any{"nextExpectedRanges": u.progress()}), nil
		}
		return nil, errors.New("unexpected session method")
	}
	route := strings.TrimPrefix(r.URL.Path, "/v1.0/drives/d/")
	if route == "root" {
		return reply(r, 200, s.items["root"].metadata()), nil
	}
	route = strings.TrimPrefix(route, "items/")
	if at := strings.Index(route, ":/"); at >= 0 {
		parent, name := route[:at], route[at+2:]
		if strings.HasSuffix(name, ":/createUploadSession") {
			s.initiations++
			name = strings.TrimSuffix(name, ":/createUploadSession")
			var input struct {
				Item map[string]any `json:"item"`
			}
			if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
				return nil, err
			}
			if input.Item["@microsoft.graph.conflictBehavior"] != "fail" {
				return nil, errors.New("session exclusion lost")
			}
			if s.child(parent, name) != nil {
				return conflict()
			}
			s.next++
			id := strconv.Itoa(s.next)
			s.sessions[id] = &session{parent: parent, name: name, total: 2 * chunk}
			return reply(r, 200, map[string]any{"uploadUrl": "https://content.test/session/" + id, "expirationDateTime": time.Now().Add(time.Hour).Format(time.RFC3339), "nextExpectedRanges": []string{"0-"}}), nil
		}
		if strings.HasSuffix(name, ":/content") {
			name = strings.TrimSuffix(name, ":/content")
			if r.URL.Query().Get("@microsoft.graph.conflictBehavior") != "fail" {
				return nil, errors.New("small-file exclusion lost")
			}
			if s.child(parent, name) != nil {
				return conflict()
			}
			data, err := io.ReadAll(r.Body)
			if err != nil {
				return nil, err
			}
			if r.ContentLength != int64(len(data)) {
				return nil, errors.New("declared content length changed")
			}
			s.uploadedBytes += len(data)
			return reply(r, 201, s.publish(parent, name, data).metadata()), nil
		}
		if e := s.child(parent, name); e != nil {
			return reply(r, 200, e.metadata()), nil
		}
		return missing()
	}
	if strings.HasSuffix(route, "/children") {
		parent := strings.TrimSuffix(route, "/children")
		var items []*item
		for _, e := range s.items {
			if e.parent == parent {
				items = append(items, e)
			}
		}
		sort.Slice(items, func(i, j int) bool { return items[i].name < items[j].name })
		out := map[string]any{"value": []any{}}
		switch r.URL.Query().Get("page") {
		case "":
			if len(items) > 0 {
				out["value"] = []any{items[0].metadata()}
			}
			next := "https://graph.test" + r.URL.Path + "?page=2"
			if s.poisonLink {
				next = "https://escape.test" + r.URL.Path + "?page=2"
			}
			out["@odata.nextLink"] = next
		case "2":
			out["@odata.nextLink"] = "https://graph.test" + r.URL.Path + "?page=3"
		case "3":
			var tail []any
			for _, e := range items[1:] {
				tail = append(tail, e.metadata())
			}
			if tail != nil {
				out["value"] = tail
			}
		}
		return reply(r, 200, out), nil
	}
	if strings.HasSuffix(route, "/content") {
		id := strings.TrimSuffix(route, "/content")
		out := reply(r, 302, nil)
		out.Header.Set("Location", "https://content.test/file/"+id)
		return out, nil
	}
	if e := s.items[route]; e != nil {
		return reply(r, 200, e.metadata()), nil
	}
	return missing()
}
func options(s *graphProtocol) driver.Options {
	s.api = &http.Client{Transport: transport(func(r *http.Request) (*http.Response, error) {
		// Unscoped authentication catches accidental use for content.
		r.Header.Set("Authorization", "Bearer account-secret")
		return s.roundTrip(r, true)
	})}
	content := &http.Client{Transport: transport(func(r *http.Request) (*http.Response, error) { return s.roundTrip(r, false) })}
	return driver.Options{APIClient: s.api, ContentClient: content, Endpoint: "https://graph.test", DriveID: "d", RootID: "vault", ChunkSize: chunk, MaxBufferedBytes: chunk, RangeAttempts: 2, CleanupTimeout: time.Second}
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

func mount(t *testing.T, s *graphProtocol, configure ...func(*driver.Options)) (*driver.Driver, *core.Core, *core.Mount) {
	t.Helper()
	opts := options(s)
	for _, change := range configure {
		change(&opts)
	}
	d, err := driver.Open(context.Background(), opts)
	if err != nil {
		if d != nil {
			_ = d.Close()
		}
		t.Fatal(err)
	}
	c := testCore(t)
	m, err := c.Install("/cloud", d)
	if err != nil {
		t.Fatal(errors.Join(err, d.Close()))
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := d.Cleanup(ctx); err != nil {
			t.Error(err)
		}
	})
	return d, c, m
}
func read(t *testing.T, c *core.Core, p string) []byte {
	t.Helper()
	opened, err := c.Open(context.Background(), "user", core.Path(p), core.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(opened.Body)
	if err = errors.Join(err, opened.Body.Close()); err != nil {
		t.Fatal(err)
	}
	return data
}

func TestTypedConstructionAndReadScope(t *testing.T) {
	ctx := context.Background()
	s := newProtocol()
	cfg := options(s)
	cfg.RootID = ""
	cfg.RootPath = "/vault"
	var settings driver.Options
	if err := json.Unmarshal([]byte(`{"drive_id":"d","root_path":"/vault","chunk_size":327680,"max_buffered_bytes":327680}`), &settings); err != nil {
		t.Fatal(err)
	}
	if err := settings.Validate(); err != nil || s.apiCalls != 0 {
		t.Fatal(err, s.apiCalls)
	}
	first, err := driver.Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	app := cfg
	app.RootID, app.RootPath = "vault", ""
	app.ChunkSize, app.MaxBufferedBytes = 0, 0
	downloadMode := ""
	app.APIClient = &http.Client{Transport: transport(func(r *http.Request) (*http.Response, error) {
		if r.Method != http.MethodGet || !strings.HasPrefix(r.URL.Path, "/v1.0/drives/d/items/") {
			return nil, errors.New("app-only read attempted a mutation or drive discovery")
		}
		response, err := cfg.APIClient.Transport.RoundTrip(r)
		if err == nil && response.StatusCode == http.StatusFound && downloadMode == "malformed-location" {
			response.Header.Set("Location", "https://content.test/file/%zz?sig=private-capability")
		}
		return response, err
	})}
	var rejected *retryClose
	app.ContentClient = &http.Client{Transport: transport(func(r *http.Request) (*http.Response, error) {
		switch downloadMode {
		case "network":
			return nil, fmt.Errorf("fetching %s: %w", r.URL, context.DeadlineExceeded)
		case "response-close":
			response := reply(r, http.StatusForbidden, nil)
			rejected = &retryClose{ReadCloser: response.Body, fault: fmt.Errorf("closing %s: %w", r.URL, io.EOF)}
			response.Body = rejected
			return response, nil
		}
		return cfg.ContentClient.Transport.RoundTrip(r)
	})}
	beforeApp := s.apiCalls
	second, err := driver.Open(ctx, app)
	if err != nil || s.apiCalls != beforeApp+1 {
		t.Fatal("app-only mount did not bind its configured folder directly", err, s.apiCalls-beforeApp)
	}
	if first == second {
		t.Fatal("constructor reused a mutable instance")
	}
	c := testCore(t)
	if _, err = c.Install("/one", first); err != nil {
		t.Fatal(err)
	}
	appMount, err := c.Install("/two", second)
	if err != nil {
		t.Fatal(err)
	}
	if listed, err := c.List(ctx, "user", core.ListRequest{Query: core.Children(core.Path("/two")), Limit: 1, CursorTTL: time.Minute}); err != nil ||
		len(listed.Items) != 1 {
		t.Fatal("app-only mount did not list its folder", listed, err)
	}
	beforeApp = s.apiCalls
	appInput, appSize := strings.NewReader("x"), int64(1)
	if change, err := c.Write(ctx, "user", core.Path("/two/new"), appInput, &appSize); change.Effect != core.Unchanged ||
		core.CodeOf(err) != core.UnsupportedOperation || appInput.Len() != 1 || s.apiCalls != beforeApp {
		t.Fatal("app-only read configuration admitted a write", change, err)
	}
	const shareURL = "https://tenant.sharepoint.test/folder?token=private-link"
	encoded := "u!" + base64.RawURLEncoding.EncodeToString([]byte(shareURL))
	sharing := cfg
	sharing.DriveID, sharing.RootPath, sharing.ShareURL = "", "", shareURL
	sharing.ChunkSize, sharing.MaxBufferedBytes = 0, 0
	settingsJSON, err := json.Marshal(sharing)
	if err != nil || bytes.Contains(settingsJSON, []byte("private-link")) || bytes.Contains(settingsJSON, []byte("share_url")) {
		t.Fatal("ordinary Graph configuration serialized the share capability")
	}
	api := cfg.APIClient.Transport
	mode, shareCalls, directCalls := "", 0, 0
	sharing.APIClient = &http.Client{Transport: transport(func(r *http.Request) (*http.Response, error) {
		if r.URL.Host != "graph.test" || r.Header.Get("Prefer") != "" || r.Method != http.MethodGet {
			t.Errorf("sharing mode escaped read-only Graph access: %s %s", r.Method, r.URL.Host)
			return nil, errors.New("invalid share request")
		}
		switch r.URL.Path {
		case "/v1.0/shares/" + encoded + "/driveItem":
			if !strings.Contains(","+r.URL.Query().Get("$select")+",", ",parentReference,") {
				t.Error("sharing mode did not request the ordinary drive identity")
			}
			shareCalls++
			root := s.items["vault"].metadata()
			switch mode {
			case "unsafe":
				root["id"] = ".."
			case "malformed":
				root["lastModifiedDateTime"] = shareURL + encoded
			}
			return reply(r, 200, root), nil
		case "/v1.0/drives/d/items/vault":
			directCalls++
			if mode == "denied" {
				return reply(r, 403, map[string]any{"error": map[string]string{"message": shareURL + encoded}}), nil
			}
			if mode == "mismatched" || mode == "wrong-drive" {
				wrong := s.items["vault"].metadata()
				if mode == "mismatched" {
					wrong["id"] = "other"
				} else {
					wrong["parentReference"] = map[string]string{"id": "root", "driveId": "other"}
				}
				return reply(r, 200, wrong), nil
			}
		}
		return api.RoundTrip(r)
	})}
	for _, outcome := range []struct {
		mode string
		code core.Code
	}{
		{"denied", core.UpstreamAccessDenied},
		{"mismatched", core.InvalidUpstreamResponse},
		{"wrong-drive", core.InvalidUpstreamResponse},
		{"unsafe", core.InvalidUpstreamResponse},
		{"malformed", core.InvalidUpstreamResponse},
	} {
		mode = outcome.mode
		failed, err := driver.Open(ctx, sharing)
		if failed != nil {
			err = errors.Join(err, failed.Close())
		}
		if core.CodeOf(err) != outcome.code || strings.Contains(err.Error(), shareURL) || strings.Contains(err.Error(), encoded) {
			t.Fatalf("shared root accepted an invalid grant or exposed the link: %v", err)
		}
	}
	mode = ""
	shared, err := driver.Open(ctx, sharing)
	if err != nil {
		if shared != nil {
			err = errors.Join(err, shared.Close())
		}
		t.Fatal(err)
	}
	if _, err := c.Install("/shared", shared); err != nil {
		t.Fatal(errors.Join(err, shared.Close()))
	}
	if string(read(t, c, "/shared/a.txt")) != "alpha" || shareCalls != 6 || directCalls != 4 {
		t.Fatal("shared folder did not switch to its ordinary root identity")
	}
	if listed, err := c.List(ctx, "user", core.ListRequest{Query: core.Children(core.Path("/shared")), Limit: 1, CursorTTL: time.Minute}); err != nil ||
		len(listed.Items) != 1 || shareCalls != 6 {
		t.Fatalf("shared listing did not stay in its ordinary directory: %+v %v", listed, err)
	}
	apiCalls := s.apiCalls
	body, size := strings.NewReader("x"), int64(1)
	address := core.Path("/shared/new")
	for _, policy := range []core.WritePolicy{{}, core.Publish(core.IfAbsent())} {
		if changed, err := c.Write(ctx, "user", address, body, &size, core.WriteOptions{Policy: policy}); changed.Effect != core.Unchanged ||
			core.CodeOf(err) != core.UnsupportedOperation || body.Len() != 1 {
			t.Fatalf("shared root admitted a mutation: %+v %v", changed, err)
		}
	}
	if changed, err := shared.ContinueWrite(ctx, core.NativePath("/new"), nil, 0, core.WriteOptions{Continuation: &core.WriteContinuation{Abort: true}}); changed.Effect != core.Unchanged || core.CodeOf(err) != core.UnsupportedOperation || s.apiCalls != apiCalls || shareCalls != 6 || directCalls != 4 {
		t.Fatalf("shared root admitted recovery or made additional requests: %+v %v", changed, err)
	}

	s.items["vault"].name = "moved"
	s.items["replacement"] = &item{id: "replacement", parent: "root", name: "vault", folder: true}
	if string(read(t, c, "/one/b.txt")) != "beta" || string(read(t, c, "/two/a.txt")) != "alpha" {
		t.Fatal("root path rebound or content authentication leaked")
	}
	request := core.ListRequest{Query: core.Children(core.Path("/one")), Limit: 1, CursorTTL: time.Minute}
	page, err := c.List(ctx, "user", request)
	if err != nil || len(page.Items) != 1 || page.NextCursor == nil {
		t.Fatal(page, err)
	}
	request.Cursor = *page.NextCursor
	empty, err := c.List(ctx, "user", request)
	if err != nil || len(empty.Items) != 0 || empty.NextCursor == nil {
		t.Fatal(empty, err)
	}
	calls := s.apiCalls
	again, err := c.List(ctx, "user", request)
	if err != nil || again.NextCursor == nil || *again.NextCursor != *empty.NextCursor || s.apiCalls != calls {
		t.Fatal("cursor replay repeated native query", again, err)
	}
	request.Cursor = *empty.NextCursor
	page, err = c.List(ctx, "user", request)
	if err != nil || len(page.Items) != 1 || page.NextCursor != nil {
		t.Fatal(page, err)
	}
	absent := core.OpenOptions{Selection: core.ReadSelection{When: core.IfAbsent()}}
	if _, err := c.Open(ctx, "user", core.Path("/one/missing"), absent); core.CodeOf(err) != core.NotFound {
		t.Fatalf("condition invented a file: %v", err)
	}
	if _, err := c.Open(ctx, "user", core.Path("/one/a.txt"), absent); core.CodeOf(err) != core.PreconditionFailed {
		t.Fatalf("existing file did not fail the absence condition: %v", err)
	}
	s.items["inner"] = &item{id: "inner", parent: "vault", name: "Exact", folder: true}
	if _, err = c.Stat(ctx, "user", core.Path("/one/exact/child")); core.CodeOf(err) != core.NotFound {
		t.Fatal("case-variant intermediate was accepted", err)
	}
	s.poisonLink = true
	request = core.ListRequest{Query: core.Children(core.Path("/two")), Limit: 1, CursorTTL: time.Minute}
	if page, err := c.List(ctx, "user", request); core.CodeOf(err) != core.InvalidUpstreamResponse || page.NextCursor != nil || s.badAPI != 0 {
		t.Fatal("foreign nextLink escaped page validation", page, err, s.badAPI)
	}
	s.items["a"].url = "https://user:secret@content.test/file/a"
	before := s.bodyCalls
	if _, err = c.Open(ctx, "user", core.Path("/one/a.txt"), core.OpenOptions{}); core.CodeOf(err) != core.InvalidUpstreamResponse || s.bodyCalls != before {
		t.Fatal("invalid download authority accepted", err)
	}
	s.items["a"].url = ""
	s.items["b"].url += "?sig=private-capability"
	for _, failure := range []struct {
		mode, path string
		code       core.Code
		cause      error
	}{
		{"malformed-location", "/two/a.txt", core.UpstreamFailure, nil},
		{"network", "/two/b.txt", core.UpstreamTimeout, context.DeadlineExceeded},
		{"response-close", "/two/b.txt", core.UpstreamAccessDenied, io.ErrUnexpectedEOF},
	} {
		downloadMode = failure.mode
		_, err := c.Open(ctx, "user", core.Path(failure.path), core.OpenOptions{})
		if core.CodeOf(err) != failure.code || failure.cause != nil && !errors.Is(err, failure.cause) ||
			strings.Contains(err.Error(), "private-capability") || s.bodyCalls != before {
			t.Fatal("download failure exposed a capability or lost its cause", failure.mode, err)
		}
	}
	if rejected == nil || rejected.calls != 1 {
		t.Fatal("rejected response did not retain one cleanup owner")
	}
	if err := appMount.Retire(ctx); err != nil || rejected.calls != 2 {
		t.Fatal("retirement did not retry rejected response cleanup", err, rejected.calls)
	}
}

type failedTail struct {
	io.Reader
	cause error
}

func (r failedTail) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	if err == io.EOF {
		return n, r.cause
	}
	return n, err
}
func TestExclusiveWriteAndNativeConfirmation(t *testing.T) {
	ctx := context.Background()
	s := newProtocol()
	_, c, _ := mount(t, s)
	large := bytes.Repeat([]byte("x"), 2*chunk)
	for _, name := range []string{"empty", "small", "lost", "final-hole"} {
		data := large
		if name == "empty" {
			data = nil
		}
		if name == "small" {
			data = []byte("small")
		}
		n := int64(len(data))
		change, err := c.Write(ctx, "user", core.Path("/cloud/"+name), bytes.NewReader(data), &n, core.WriteOptions{Policy: core.Publish(core.IfAbsent())})
		if err != nil || change.Effect != core.Applied || !bytes.Equal(read(t, c, "/cloud/"+name), data) {
			t.Fatal(name, change, err)
		}
	}
	if s.queries != 1 {
		t.Fatal("lost acknowledgement was not reconciled", s.queries)
	}
	untouched := strings.NewReader("keep")
	n := int64(4)
	if change, err := c.Write(ctx, "user", core.Path("/cloud/small"), untouched, &n, core.WriteOptions{Policy: core.Publish()}); core.CodeOf(err) != core.UnsupportedOperation || change.Effect != core.Unchanged || untouched.Len() != 4 {
		t.Fatal(change, err, untouched.Len())
	}
	n = int64(len(large))
	change, err := c.Write(ctx, "user", core.Path("/cloud/race"), bytes.NewReader(large), &n, core.WriteOptions{Policy: core.Publish(core.IfAbsent())})
	if core.CodeOf(err) != core.PreconditionFailed || change.Effect != core.Unchanged || string(read(t, c, "/cloud/race")) != "peer" {
		t.Fatal("commit race replaced peer", change, err)
	}
	tail := errors.New("input tail failed")
	before := s.finals
	for _, input := range []io.Reader{failedTail{bytes.NewReader(large), tail}, io.MultiReader(bytes.NewReader(large), strings.NewReader("extra"))} {
		change, err = c.Write(ctx, "user", core.Path("/cloud/tail"), input, &n)
		if err == nil || change.Effect != core.Unchanged || s.finals != before {
			t.Fatal("final block preceded input validation", change, err)
		}
		if _, ok := input.(failedTail); ok && !errors.Is(err, tail) {
			t.Fatal("tail cause lost", err)
		}
	}
	change, err = c.Write(ctx, "user", core.Path("/cloud/terminal"), bytes.NewReader(large), &n)
	if err == nil || change.Effect != core.Unknown || !bytes.Equal(read(t, c, "/cloud/terminal"), large) || s.pathDeletes != 0 {
		t.Fatal("404 inferred nonpublication or triggered compensation", change, err)
	}
	for _, mode := range []string{"query-failure", "no-progress"} {
		t.Run(mode, func(t *testing.T) {
			sendCause, queryCause := errors.New("PUT transport failed"), errors.New("GET progress tail failed")
			_, c, _ := mount(t, newProtocol(), func(o *driver.Options) {
				next := o.ContentClient.Transport
				o.RangeAttempts = 1
				o.ContentClient.Transport = transport(func(r *http.Request) (*http.Response, error) {
					if r.Method == http.MethodPut {
						if mode == "query-failure" {
							return nil, sendCause
						}
						return reply(r, 503, map[string]any{"error": map[string]string{"code": "native-blocked", "message": "native-retry-reason"}}), nil
					}
					response, err := next.RoundTrip(r)
					if err == nil && r.Method == http.MethodGet && mode == "query-failure" {
						response.Body = struct {
							io.Reader
							io.Closer
						}{failedTail{response.Body, queryCause}, response.Body}
					}
					return response, err
				})
			})
			change, err := c.Write(ctx, "user", core.Path("/cloud/retry"), bytes.NewReader(large), &n)
			if err == nil || change.Effect != core.Unchanged {
				t.Fatal(change, err)
			}
			if mode == "query-failure" && (!errors.Is(err, sendCause) || !errors.Is(err, queryCause)) {
				t.Fatal("query failure hid original transport cause", err)
			}
			if mode == "no-progress" && (!strings.Contains(err.Error(), "native-blocked") || !strings.Contains(err.Error(), "native-retry-reason")) {
				t.Fatal("zero progress hid original native reason", err)
			}
		})
	}
}

func TestCancellationCleanupAndBorrowedClients(t *testing.T) {
	data := bytes.Repeat([]byte("x"), 2*chunk)
	size := int64(len(data))
	t.Run("confirmed-file-and-closure", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		fault := errors.New("file receipt close failed")
		var body *retryClose
		_, c, _ := mount(t, newProtocol(), func(o *driver.Options) {
			next := o.ContentClient.Transport
			o.ContentClient.Transport = transport(func(r *http.Request) (*http.Response, error) {
				response, err := next.RoundTrip(r)
				if err == nil && response.StatusCode == 201 {
					body = &retryClose{ReadCloser: response.Body, fault: fault}
					response.Body = body
					cancel()
				}
				return response, err
			})
		})
		change, err := c.Write(ctx, "user", core.Path("/cloud/confirmed"), bytes.NewReader(data), &size)
		if change.Effect != core.Applied || !errors.Is(err, fault) || !errors.Is(err, context.Canceled) || body == nil || body.calls != 1 {
			t.Fatal("file result or single response owner lost", change, err)
		}
	})
	t.Run("independent-unknown-initializations", func(t *testing.T) {
		opts := options(newProtocol())
		opts.MaxBufferedBytes = 2 * chunk
		next := opts.APIClient.Transport
		entered, release := make(chan struct{}, 2), make(chan struct{})
		finish := sync.OnceFunc(func() { close(release) })
		defer finish()
		opts.APIClient.Transport = transport(func(r *http.Request) (*http.Response, error) {
			if strings.HasSuffix(r.URL.Path, "/createUploadSession") {
				entered <- struct{}{}
				<-release
				return nil, errors.New("upload initialization reply lost")
			}
			return next.RoundTrip(r)
		})
		d, err := driver.Open(context.Background(), opts)
		if err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 2)
		for _, target := range []string{"/init-a", "/init-b"} {
			go func() {
				_, err := d.Create(context.Background(), core.NativePath(target), bytes.NewReader(data), &size)
				done <- err
			}()
		}
		<-entered
		<-entered
		finish()
		if (<-done) == nil || (<-done) == nil {
			t.Fatal("unknown initialization was accepted")
		}
		if err := errors.Join(d.Cleanup(context.Background()), d.Close()); err == nil || !strings.Contains(err.Error(), "/init-a") || !strings.Contains(err.Error(), "/init-b") || !strings.Contains(err.Error(), "upload 1 for ") || !strings.Contains(err.Error(), "upload 2 for ") {
			t.Fatal("unknown resources were coalesced or lost their targets", err)
		}
	})
	ctx := context.Background()
	s := newProtocol()
	d, c, m := mount(t, s)
	s.started = make(chan struct{})
	s.exited = make(chan struct{})
	s.failAbort = true
	callCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		change, err := c.Write(callCtx, "user", core.Path("/cloud/cancel"), bytes.NewReader(data), &size)
		if change.Effect != core.Unchanged {
			err = errors.Join(err, errors.New("private session reported visible effect"))
		}
		done <- err
	}()
	select {
	case <-s.started:
	case <-time.After(time.Second):
		t.Fatal("request did not start")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("cancelled request did not exit")
	}
	select {
	case <-s.exited:
	default:
		t.Fatal("return preceded request exit")
	}
	if count, _ := s.late.Read(make([]byte, 1)); count != 0 {
		t.Fatal("late transport read retained a released payload")
	}
	if err := d.Cleanup(ctx); err == nil {
		t.Fatal("failed cleanup lost session ownership")
	}
	if err := d.Close(); err == nil {
		t.Fatal("driver close hid failed private cleanup")
	}
	s.failAbort = false
	if err := d.Cleanup(ctx); err != nil || len(s.sessions) != 0 {
		t.Fatal("cleanup did not recover", err)
	}
	if err := m.Retire(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Stat(ctx, "user", core.Path("/cloud/a.txt")); core.CodeOf(err) != core.MountUnavailable && core.CodeOf(err) != core.NotFound {
		t.Fatal("retired generation admitted work", err)
	}
	r, err := s.api.Get("https://graph.test/v1.0/drives/d/root")
	if err != nil {
		t.Fatal("driver closed borrowed client", err)
	}
	_ = r.Body.Close()
}

// The ordinary Reader is deliberately unusable. Even a new durable transfer
// must verify and close each independent range of its sealed source.
type verifiedSource struct {
	ref                                *input.Reference
	version                            string
	fault, closeFault                  error
	opens, closes, bytesRead, rawReads int
}

func (s *verifiedSource) Read([]byte) (int, error) {
	s.rawReads++
	return 0, errors.New("durable transfer bypassed verified ranges")
}
func (s *verifiedSource) SourceVersion() string { return s.version }
func (s *verifiedSource) OpenRange(ctx context.Context, start, length int64) (io.ReadCloser, error) {
	body, err := s.ref.OpenRange(ctx, start, length)
	if err != nil {
		return nil, err
	}
	s.opens++
	return &verifiedRange{ReadCloser: body, source: s}, nil
}

type verifiedRange struct {
	io.ReadCloser
	source *verifiedSource
}

func (b *verifiedRange) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	b.source.bytesRead += n
	if err == io.EOF && b.source.fault != nil {
		err = b.source.fault
	}
	return n, err
}
func (b *verifiedRange) Close() error {
	b.source.closes++
	return errors.Join(b.ReadCloser.Close(), b.source.closeFault)
}

func TestDurableWriteRecovery(t *testing.T) {
	ctx := context.Background()
	data := make([]byte, 2*chunk)
	for i := range data {
		data[i] = byte(i % 251)
	}
	for _, mode := range []string{"pause-prefix", "resume-holes", "publish-barrier", "uncertain-barrier", "lost-done", "lost-init", "refused-final", "final-hole", "empty", "small", "close-range"} {
		t.Run(mode, func(t *testing.T) {
			payload := data
			if mode == "empty" {
				payload = nil
			} else if mode == "small" || mode == "close-range" {
				payload = data[:19]
			}
			store, err := input.Open(ctx, t.TempDir(), input.Options{MaxBytes: 8*chunk + 8192, MaxInputs: 1, MaxBlocks: 8, MaxAppendBytes: 2 * chunk})
			if err != nil {
				t.Fatal(err)
			}
			record, err := store.Create(ctx, "user")
			if err != nil {
				t.Fatal(err)
			}
			// The checksum block of a large input spans both native payloads.
			if len(payload) > 0 {
				if _, err = store.Append(ctx, record.ID, 0, bytes.NewReader(payload), int64(len(payload))); err != nil {
					t.Fatal(err)
				}
			}
			if _, err = store.Seal(ctx, record.ID, int64(len(payload))); err != nil {
				t.Fatal(err)
			}
			ref, err := store.Acquire(ctx, record.ID)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				wait, cancel := context.WithTimeout(ctx, time.Second)
				defer cancel()
				if err := errors.Join(ref.Close(), store.Close(wait)); err != nil {
					t.Error(err)
				}
			})
			source := &verifiedSource{ref: ref, version: ref.Record().Version}
			s := newProtocol()
			s.items["inner"] = &item{id: "inner", parent: "vault", name: "inner", folder: true}
			runContext := ctx
			var pause context.CancelFunc
			if mode == "pause-prefix" {
				runContext, pause = context.WithCancel(ctx)
				defer pause()
			}
			d, c, mounted := mount(t, s, func(o *driver.Options) {
				if mode == "lost-init" {
					next := o.APIClient.Transport
					o.APIClient.Transport = transport(func(r *http.Request) (*http.Response, error) {
						response, err := next.RoundTrip(r)
						if err == nil && strings.HasSuffix(r.URL.Path, "/createUploadSession") {
							_ = response.Body.Close()
							return nil, errors.New("upload initiation reply lost")
						}
						return response, err
					})
				} else if mode == "pause-prefix" {
					next := o.ContentClient.Transport
					o.ContentClient.Transport = transport(func(r *http.Request) (*http.Response, error) {
						response, err := next.RoundTrip(r)
						if err == nil && r.Method == http.MethodPut && response.StatusCode == http.StatusAccepted {
							pause()
						}
						return response, err
					})
				}
			})
			path := core.Path("/cloud/inner/durable")
			if mode == "refused-final" {
				path = core.Path("/cloud/inner/race")
			} else if mode == "final-hole" {
				path = core.Path("/cloud/inner/final-hole")
			}
			n := int64(len(payload))
			journal := filepath.Join(t.TempDir(), "native.log")
			storageFault := errors.New("checkpoint persistence failed")
			barrierFault := mode == "publish-barrier" || mode == "uncertain-barrier"
			appends := 0
			appendRecord := func(_ context.Context, delta []byte) error {
				appends++
				atBarrier := false
				for _, session := range s.sessions {
					atBarrier = atBarrier || len(session.data) == chunk
				}
				fail := barrierFault && atBarrier || mode == "lost-done" && s.child("inner", "durable") != nil
				if fail && mode != "uncertain-barrier" {
					return storageFault
				}
				file, err := os.OpenFile(journal, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
				if err != nil {
					return err
				}
				_, err = file.Write(delta)
				err = errors.Join(err, file.Sync(), file.Close())
				if fail {
					err = errors.Join(err, storageFault)
				}
				return err
			}
			write := func(client *core.Core, policy core.WritePolicy, abort bool) (core.Change, error) {
				checkpoint, err := os.ReadFile(journal)
				if err != nil && !errors.Is(err, os.ErrNotExist) {
					t.Fatal(err)
				}
				return client.Write(runContext, "user", path, source, &n, core.WriteOptions{Policy: policy, Continuation: &core.WriteContinuation{Checkpoint: checkpoint, Append: appendRecord, Abort: abort}})
			}
			if mode == "resume-holes" {
				source.fault = errors.New("sealed range verification failed")
			} else if mode == "close-range" {
				source.closeFault = errors.New("sealed range close failed")
			}
			change, err := write(c, core.WritePolicy{}, false)
			switch mode {
			case "close-range":
				if !errors.Is(err, source.closeFault) || change.Effect != core.Unchanged || s.puts != 0 {
					t.Fatal("close failure lost its cause or allowed publication", change, err, s.puts)
				}
				if err := mounted.Retire(ctx); err != nil || source.closes != 1 {
					t.Fatal("retirement retained an input range body", err, source.closes)
				}
			case "empty", "small":
				if err != nil || change.Effect != core.Applied || s.puts != 1 || s.initiations != 0 || s.uploadedBytes != len(payload) {
					t.Fatal("direct durable write acquired a session or lost its receipt", change, err, s.puts, s.initiations)
				}
			case "final-hole":
				if err != nil || change.Effect != core.Applied || s.puts != 3 || s.uploadedBytes != len(payload)+chunk/2 || source.opens != 2 || s.queries != 0 || !bytes.Equal(read(t, c, path.Value()), payload) {
					t.Fatal("fresh upload rejected completion of its retained final hole", change, err, s.puts, s.uploadedBytes)
				}
			case "pause-prefix", "resume-holes":
				initialPuts, initialFault := 0, source.fault
				if mode == "pause-prefix" {
					initialPuts, initialFault = 1, context.Canceled
				}
				if !errors.Is(err, initialFault) || change.Effect != core.Unchanged || s.puts != initialPuts || source.opens != 1 || source.closes != 1 {
					t.Fatal("fresh durable upload bypassed source verification", change, err, s.puts, source.opens, source.closes)
				}
				if err := errors.Join(d.Cleanup(ctx), mounted.Retire(ctx)); err != nil || s.deletes != 0 || len(s.sessions) != 1 {
					t.Fatal("retirement aborted a durable session", err, s.deletes)
				}
				failObservation := mode == "resume-holes"
				_, c, _ = mount(t, s, func(o *driver.Options) {
					next := o.ContentClient.Transport
					o.ContentClient.Transport = transport(func(r *http.Request) (*http.Response, error) {
						if failObservation && r.Method == http.MethodGet {
							s.mu.Lock()
							s.queries++
							s.mu.Unlock()
							return reply(r, 500, map[string]any{"error": map[string]string{"code": "sessionLookupFailed", "message": "native observation unavailable"}}), nil
						}
						return next.RoundTrip(r)
					})
				})
				source.fault = nil
				runContext = ctx
				if failObservation {
					if result, err := write(c, core.WritePolicy{}, false); result.Effect != core.Unchanged || core.CodeOf(err) != core.UpstreamFailure || !strings.Contains(err.Error(), "native observation unavailable") || s.puts != initialPuts || source.opens != 1 {
						t.Fatal("failed progress observation became a file effect or lost its cause", result, err)
					}
					failObservation = false
				}
				if mode == "pause-prefix" {
					// The saved binding is checked before consulting native progress.
					checkBinding := func(label string, client *core.Core, policy core.WritePolicy) {
						t.Helper()
						apiCalls, bodyCalls := s.apiCalls, s.bodyCalls
						if result, err := write(client, policy, false); core.CodeOf(err) != core.InvalidRequest || result.Effect != core.Unchanged ||
							s.apiCalls != apiCalls || s.bodyCalls != bodyCalls || source.opens != 1 {
							t.Fatal(label, "mismatch resumed work", result, err)
						}
					}
					version := source.version
					source.version = "different-source-version"
					checkBinding("source version", c, core.WritePolicy{})
					source.version = version
					checkBinding("policy", c, core.Publish(core.IfAbsent()))
					s.items["foreign"] = &item{id: "foreign", name: "foreign", folder: true}
					_, wrong, _ := mount(t, s, func(o *driver.Options) { o.RootID = "foreign" })
					checkBinding("native root", wrong, core.WritePolicy{})
				}
				expectedBytes := len(payload)
				if mode == "resume-holes" {
					expectedBytes = chunk
					for _, session := range s.sessions {
						session.data = bytes.Clone(payload)
						session.holes = []core.Range{{Start: 0, End: chunk/2 - 1}, {Start: chunk, End: 3*chunk/2 - 1}}
						clear(session.data[:chunk/2])
						clear(session.data[chunk : 3*chunk/2])
					}
				}
				beforeReads := source.bytesRead
				change, err = write(c, core.WritePolicy{}, false)
				if err != nil || change.Effect != core.Applied || s.puts != 2 || s.uploadedBytes != expectedBytes || source.bytesRead-beforeReads != chunk || !bytes.Equal(read(t, c, path.Value()), payload) {
					t.Fatal("range holes were flattened or middle completion was lost", change, err, s.puts, s.uploadedBytes, source.bytesRead-beforeReads)
				}
				if mode == "pause-prefix" {
					s.items["inner"].name = "renamed"
					apiCalls, bodyCalls := s.apiCalls, s.bodyCalls
					if change, err = write(c, core.WritePolicy{}, false); err != nil || change.Effect != core.Applied || s.apiCalls != apiCalls || s.bodyCalls != bodyCalls {
						t.Fatal("terminal receipt was rebound to the current path", change, err)
					}
				}
			case "publish-barrier", "uncertain-barrier":
				if !errors.Is(err, storageFault) || change.Effect != core.Unchanged || s.puts != 1 || s.child("inner", "durable") != nil {
					t.Fatal("publication crossed a failed checkpoint barrier", change, err, s.puts)
				}
				barrierFault = false
				before := appends
				// Deletion acknowledges only private cleanup, never file rollback.
				_, aborted, _ := mount(t, s, func(o *driver.Options) {
					next := o.ContentClient.Transport
					o.ContentClient.Transport = transport(func(r *http.Request) (*http.Response, error) {
						if r.Method == http.MethodDelete && appends <= before {
							return nil, errors.New("native cancellation preceded its journal")
						}
						return next.RoundTrip(r)
					})
				})
				change, err = write(aborted, core.WritePolicy{}, true)
				uncertain := mode == "uncertain-barrier"
				want := core.Unchanged
				if uncertain {
					want = core.Unknown
				}
				if change.Effect != want || (err != nil) != uncertain || s.deletes != 1 || len(s.sessions) != 0 {
					t.Fatal("private cleanup did not preserve publication effect", change, err, s.deletes)
				}
				if uncertain {
					if change, err = write(aborted, core.WritePolicy{}, true); err == nil || change.Effect != core.Unknown || s.puts != 1 {
						t.Fatal("missing cleaned session erased possible publication", change, err)
					}
				}
			case "lost-done":
				if !errors.Is(err, storageFault) || change.Effect != core.Applied || s.puts != 2 {
					t.Fatal("lost receipt persistence erased confirmed application", change, err)
				}
				_, c, _ = mount(t, s)
				puts, opened := s.puts, source.opens
				change, err = write(c, core.WritePolicy{}, false)
				if err == nil || change.Effect != core.Unknown || s.puts != puts || source.opens != opened || s.pathDeletes != 0 {
					t.Fatal("lost final receipt plus 404 was replayed", change, err, s.puts)
				}
				if change, err = write(c, core.WritePolicy{}, true); err == nil || change.Effect != core.Unknown || s.puts != puts || !bytes.Equal(read(t, c, path.Value()), payload) {
					t.Fatal("abort removed or re-created an unconfirmed publication", change, err)
				}
			case "lost-init":
				if err == nil || change.Effect != core.Unchanged || s.puts != 0 || s.initiations != 1 || source.opens != 0 {
					t.Fatal("initialization without body was treated as file publication", change, err)
				}
				if err := errors.Join(d.Cleanup(ctx), mounted.Retire(ctx)); err != nil || s.deletes != 0 {
					t.Fatal("ordinary cleanup adopted an unknown durable initialization", err)
				}
				_, c, _ = mount(t, s)
				change, err = write(c, core.WritePolicy{}, false)
				if change.Effect != core.Applied || s.initiations != 2 || s.puts != 2 || len(s.sessions) != 1 || !bytes.Equal(read(t, c, path.Value()), payload) {
					t.Fatal("finite restart lost the old orphan or failed to publish", change, err, s.initiations, s.puts)
				}
			case "refused-final":
				if err == nil || change.Effect != core.Unchanged || !strings.Contains(err.Error(), "nameAlreadyExists") || s.puts != 2 {
					t.Fatal("definite final refusal lost its cause or effect", change, err)
				}
				code, opened, queried := core.CodeOf(err), source.opens, s.queries
				_, c, _ = mount(t, s)
				change, err = write(c, core.WritePolicy{}, false)
				if core.CodeOf(err) != code || change.Effect != core.Unchanged || !strings.Contains(err.Error(), "nameAlreadyExists") || s.puts != 2 || source.opens != opened || s.queries != queried {
					t.Fatal("restored refusal was retried or lost its native reason", change, err)
				}
				if change, err = write(c, core.WritePolicy{}, true); err != nil || change.Effect != core.Unchanged || s.deletes != 1 || len(s.sessions) != 0 || s.pathDeletes != 0 || string(read(t, c, path.Value())) != "peer" {
					t.Fatal("refused session cleanup changed the peer file", change, err)
				}
			}
			if source.rawReads != 0 || source.opens != source.closes || appends > 12 {
				t.Fatal("source ownership or incremental journal cost regressed", source.rawReads, source.opens, source.closes, appends)
			}
			t.Logf("PUT=%d uploaded=%d range-opens=%d logical-source-read=%d query=%d append=%d cancel=%d", s.puts, s.uploadedBytes, source.opens, source.bytesRead, s.queries, appends, s.deletes)
		})
	}
}
