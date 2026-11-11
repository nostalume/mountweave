package yandex

import (
	"bytes"
	"context"
	"encoding/json"
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

type fixture struct {
	mu                           sync.Mutex
	calls                        []string
	bodyURL, apiURL, redirectURL string
	bodyCalls                    int
	removed                      bool
	entered, release             chan struct{}
}

func (s *fixture) control(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("Authorization") != "OAuth synthetic" {
		http.Error(w, "wrong account authorization", 401)
		return
	}
	q := r.URL.Query()
	p := q.Get("path")
	s.mu.Lock()
	s.calls = append(s.calls, r.Method+" "+r.URL.RequestURI())
	s.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	if strings.Contains(r.URL.Path, "/operations/") {
		if strings.HasSuffix(r.URL.Path, "/wait") {
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
		status := "success"
		if strings.HasSuffix(r.URL.Path, "/failed") {
			status = "failed"
		}
		if strings.HasSuffix(r.URL.Path, "/odd") {
			status = "unrecognized"
		}
		if status == "success" {
			s.mu.Lock()
			s.removed = true
			s.mu.Unlock()
		}
		json.NewEncoder(w).Encode(map[string]any{"status": status})
		return
	}
	if strings.HasSuffix(r.URL.Path, "/download") {
		method, href := "GET", s.bodyURL+"/blob"
		if strings.HasSuffix(p, "/bad") {
			method = "PUT"
		}
		if strings.HasSuffix(p, "/waiting") {
			href = s.bodyURL + "/waiting"
		}
		if strings.HasSuffix(p, "/control-redirect") {
			w.Header().Set("Location", s.redirectURL)
			w.WriteHeader(http.StatusFound)
			return
		}
		for _, name := range []string{"redirect", "unsafe-redirect", "redirect-loop"} {
			if strings.HasSuffix(p, "/"+name) {
				href = s.bodyURL + "/" + name
			}
		}
		json.NewEncoder(w).Encode(map[string]any{"href": href, "method": method, "templated": false})
		return
	}
	if r.Method == "PUT" {
		w.WriteHeader(201)
		return
	}
	if r.Method == "DELETE" {
		if strings.HasSuffix(p, "/immediate") {
			s.mu.Lock()
			s.removed = true
			s.mu.Unlock()
			w.WriteHeader(204)
			return
		}
		suffix := "done"
		for _, state := range []string{"wait", "failed", "odd"} {
			if strings.HasSuffix(p, "/"+state) {
				suffix = state
			}
		}
		href := s.apiURL + "/v1/disk/operations/" + suffix
		if strings.HasSuffix(p, "/foreign") {
			href = s.bodyURL + "/v1/disk/operations/done"
		}
		w.WriteHeader(202)
		json.NewEncoder(w).Encode(map[string]any{"href": href, "method": "GET", "templated": false})
		return
	}
	p = strings.Replace(p, "app:/Vault", "disk:/Apps/Native/Vault", 1)
	name := p[strings.LastIndex(p, "/")+1:]
	typ := "dir"
	result := map[string]any{"path": p, "name": name, "type": typ}
	if strings.HasSuffix(p, "/p%+中") {
		result["type"], result["size"] = "file", 6
	}
	if strings.HasSuffix(p, "/mismatch") {
		result["path"] = "disk:/outside"
	}
	if q.Has("offset") {
		offset, _ := strconv.Atoi(q.Get("offset"))
		items := []any{}
		if offset == 0 {
			items = append(items, map[string]any{"path": p + "/p%+中", "name": "p%+中", "type": "file", "size": 6})
		}
		if offset == 1 {
			items = append(items, map[string]any{"path": p + "/tree", "name": "tree", "type": "dir"})
		}
		total := 2
		if strings.HasSuffix(p, "/empty") {
			total = 0
			items = []any{}
		}
		if strings.HasSuffix(p, "/stalled") {
			items = []any{}
		}
		if strings.HasSuffix(p, "/junk") {
			io.WriteString(w, `{"path":"disk:/Vault/junk","name":"junk","type":"dir","_embedded":`)
			return
		}
		result["_embedded"] = map[string]any{"path": p, "offset": offset, "limit": 2, "total": total, "items": items}
	}
	json.NewEncoder(w).Encode(result)
}

func (s *fixture) content(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	s.bodyCalls++
	s.mu.Unlock()
	if r.Header.Get("Authorization") != "" || r.Header.Get("Cookie") != "" || r.Header.Get("Referer") != "" ||
		r.Header.Get("X-Control-Only") != "" || r.Header.Get("Accept-Encoding") != "identity" {
		http.Error(w, "account credentials leaked", 403)
		return
	}
	if r.URL.Path == "/waiting" {
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
	if strings.HasPrefix(r.URL.Path, "/redirect") || r.URL.Path == "/unsafe-redirect" {
		href := s.redirectURL
		if r.URL.Path == "/unsafe-redirect" {
			href = "http://user:pass@127.0.0.1:1/secret"
		}
		if r.URL.Path == "/redirect-loop" {
			href = s.bodyURL + "/redirect-loop"
		}
		w.Header().Set("Location", href)
		w.WriteHeader(http.StatusFound)
		return
	}
	data, status := "abcdef", 200
	if raw := r.Header.Get("Range"); raw != "" {
		var start, end int
		if _, err := fmt.Sscanf(raw, "bytes=%d-%d", &start, &end); err == nil {
			data, status = data[start:end+1], 206
			w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/6", start, end))
		}
	}
	w.Header().Set("ETag", `"body-v1"`)
	w.Header().Set("Content-Length", strconv.Itoa(len(data)))
	w.WriteHeader(status)
	io.WriteString(w, data)
}

type transport func(*http.Request) (*http.Response, error)

func (f transport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func setup(t *testing.T) (*fixture, *Driver, *core.Core, *core.Mount) {
	t.Helper()
	s := &fixture{entered: make(chan struct{}, 2), release: make(chan struct{})}
	body := httptest.NewServer(http.HandlerFunc(s.content))
	receiver := httptest.NewServer(http.HandlerFunc(s.content))
	api := httptest.NewServer(http.HandlerFunc(s.control))
	s.bodyURL, s.apiURL, s.redirectURL = body.URL, api.URL, receiver.URL+"/blob"
	t.Cleanup(body.Close)
	t.Cleanup(receiver.Close)
	t.Cleanup(api.Close)
	t.Cleanup(func() { close(s.release) })
	apiClient := *api.Client()
	base := apiClient.Transport
	if base == nil {
		base = http.DefaultTransport
	}
	apiClient.Transport = transport(func(r *http.Request) (*http.Response, error) {
		r.Header.Set("X-Control-Only", "private")
		return base.RoundTrip(r)
	})
	d, err := Open(context.Background(), Options{RootPath: "disk:/Vault", Endpoint: api.URL + "/v1/disk", Token: "synthetic", Client: &apiClient, ContentClient: body.Client(), MaxResponseBytes: 2048})
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
	return s, d, c, m
}

func TestPathPagesAndAuthorizedBody(t *testing.T) {
	s, d, c, m := setup(t)
	ctx := context.Background()
	if err := c.BindAlias(ctx, "/alias", m, "/"); err != nil {
		t.Fatal(err)
	}
	q := core.Children(core.Path("/remote"))
	p, err := c.List(ctx, "u", core.ListRequest{Query: q, Limit: 2, CursorTTL: time.Minute})
	if err != nil || len(p.Items) != 1 || p.NextCursor == nil {
		t.Fatal(p, err)
	}
	request := core.ListRequest{Query: q, Limit: 2, Cursor: *p.NextCursor}
	p, err = c.List(ctx, "u", request)
	if err != nil || len(p.Items) != 1 || p.Items[0].Name != "tree" {
		t.Fatal(p, err)
	}
	s.mu.Lock()
	before := len(s.calls)
	s.mu.Unlock()
	if _, err = c.List(ctx, "u", request); err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	after := len(s.calls)
	s.mu.Unlock()
	if before != after {
		t.Fatal("replay performed native request")
	}
	opened, err := c.Open(ctx, "u", core.Path("/alias/p%+中"), core.OpenOptions{Range: &core.Range{Start: 1, End: 3}})
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(opened.Body)
	err = errors.Join(err, opened.Body.Close())
	if err != nil || string(data) != "bcd" || opened.Range == nil {
		t.Fatal(string(data), opened.Range, err)
	}
	s.mu.Lock()
	before = len(s.calls)
	s.mu.Unlock()
	if _, err = c.Open(ctx, "u", core.Path("/remote/denied"), core.OpenOptions{}); core.CodeOf(err) != core.PermissionDenied {
		t.Fatal(err)
	}
	s.mu.Lock()
	after = len(s.calls)
	s.mu.Unlock()
	if before != after {
		t.Fatal("unauthorized native request")
	}
	opened, err = c.Open(ctx, "u", core.Path("/remote/p%+中"), core.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	data, err = io.ReadAll(opened.Body)
	if err = errors.Join(err, opened.Body.Close()); err != nil || string(data) != "abcdef" || opened.Range != nil {
		t.Fatalf("full original: %q, range=%v, error=%v", data, opened.Range, err)
	}
	redirected, err := c.Open(ctx, "u", core.Path("/remote/redirect"), core.OpenOptions{Range: &core.Range{Start: 1, End: 3}})
	if err != nil {
		t.Fatal("bounded content redirect", err)
	}
	data, err = io.ReadAll(redirected.Body)
	err = errors.Join(err, redirected.Body.Close())
	if err != nil || string(data) != "bcd" || redirected.Range == nil {
		t.Fatal("redirect changed range or headers", string(data), redirected.Range, err)
	}
	s.mu.Lock()
	before = s.bodyCalls
	s.mu.Unlock()
	if _, err := c.Open(ctx, "u", core.Path("/remote/control-redirect"), core.OpenOptions{}); err == nil {
		t.Fatal("followed control redirect")
	}
	s.mu.Lock()
	after = s.bodyCalls
	s.mu.Unlock()
	if before != after {
		t.Fatal("control redirect reached content receiver")
	}
	for _, name := range []string{"bad", "unsafe-redirect", "redirect-loop"} {
		if _, err := c.Open(ctx, "u", core.Path("/remote/"+name), core.OpenOptions{}); err == nil {
			t.Fatal("accepted unsafe capability", name)
		}
	}
	secure := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Location", s.redirectURL)
		w.WriteHeader(http.StatusFound)
	}))
	defer secure.Close()
	client := d.content
	client.Transport = secure.Client().Transport
	if response, err := client.Get(secure.URL); err == nil {
		response.Body.Close()
		t.Fatal("content redirect downgraded HTTPS")
	}
	plain, err := Open(ctx, Options{Endpoint: s.apiURL + "/v1/disk", RootPath: "disk:/Vault", Token: "synthetic", Client: &d.api})
	if err != nil {
		t.Fatal(err)
	}
	body, err := plain.Open(ctx, core.NativePath("/p%+中"), core.OpenOptions{})
	if err != nil {
		t.Fatal("default content client reused account transport", err)
	}
	if err = body.Body.Close(); err != nil {
		t.Fatal(err)
	}
	if err = plain.Close(); err != nil {
		t.Fatal(err)
	}
	app, err := Open(ctx, Options{Endpoint: s.apiURL + "/v1/disk", RootPath: "app:/Vault", Token: "synthetic", Client: &d.api, ContentClient: &d.content})
	if err != nil {
		t.Fatal("canonical app root", err)
	}
	defer app.Close()
	iter, err := app.Enumerate(ctx, core.NativeChildren(core.NativePath("/")))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = iter.Next(ctx, 2); err != nil {
		t.Fatal("canonical app page", err)
	}
	if err = iter.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestChangesCancellationAndRetirement(t *testing.T) {
	s, d, c, m := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	if change, err := c.Mkdir(ctx, "u", core.Path("/remote/new")); err != nil || change.Effect != core.Applied {
		t.Fatal(change, err)
	}
	result, err := c.RemoveTree(ctx, "u", core.Path("/remote/tree"), core.TreeOptions{MaxEntries: 10, MaxPlanBytes: 512}, core.DirectoryRemoveOptions{Scope: core.Subtree})
	if err != nil || result.Change.Effect != core.Applied {
		t.Fatal(result, err)
	}
	s.mu.Lock()
	removed := s.removed
	s.mu.Unlock()
	if !removed {
		t.Fatal("operation was not confirmed")
	}
	for _, name := range []string{"failed", "odd", "foreign"} {
		if change, err := d.RemoveSubtree(ctx, core.NativePath("/"+name), core.TreeOptions{}); err == nil || change.Effect != core.Unknown {
			t.Fatal(name, change, err)
		}
	}
	call, stop := context.WithCancel(ctx)
	done := make(chan core.Change, 1)
	go func() {
		change, _ := d.RemoveSubtree(call, core.NativePath("/wait"), core.TreeOptions{})
		done <- change
	}()
	select {
	case <-s.entered:
	case <-ctx.Done():
		t.Fatal("operation poll never started")
	}
	stop()
	select {
	case change := <-done:
		if change.Effect != core.Unknown {
			t.Fatal(change)
		}
	case <-ctx.Done():
		t.Fatal("poll did not join")
	}
	active := make(chan error, 1)
	go func() { _, err := c.Open(ctx, "u", core.Path("/remote/waiting"), core.OpenOptions{}); active <- err }()
	select {
	case <-s.entered:
	case <-ctx.Done():
		t.Fatal("body request never started")
	}
	if err := m.Retire(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-active:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("retirement did not join")
	}
}

type brokenClose struct {
	io.ReadCloser
	fail   bool
	cause  error
	closes int
}

func (b *brokenClose) Close() error {
	b.closes++
	if b.fail {
		if b.cause != nil {
			return b.cause
		}
		return errors.New("close failed")
	}
	return b.ReadCloser.Close()
}

func TestResponseFailuresAndUnknownReceipts(t *testing.T) {
	s, d, c, _ := setup(t)
	ctx := context.Background()
	contentTransport := d.content.Transport
	if contentTransport == nil {
		contentTransport = http.DefaultTransport
	}
	var redirectBody, finalBody *brokenClose
	const secret = "private-signed-query"
	d.content.Transport = transport(func(r *http.Request) (*http.Response, error) {
		if r.URL.Path == "/blob" {
			finalBody = &brokenClose{ReadCloser: http.NoBody, fail: true, cause: fmt.Errorf("%s: %w", secret, io.EOF)}
			return &http.Response{StatusCode: http.StatusForbidden, Body: finalBody}, nil
		}
		response, err := contentTransport.RoundTrip(r)
		if err == nil && r.URL.Path == "/redirect" {
			redirectBody = &brokenClose{ReadCloser: response.Body, fail: true, cause: fmt.Errorf("%s: %w", secret, io.EOF)}
			response.Body = redirectBody
		}
		return response, err
	})
	if _, err := c.Open(ctx, "u", core.Path("/remote/redirect"), core.OpenOptions{}); !errors.Is(err, io.ErrUnexpectedEOF) ||
		strings.Contains(err.Error(), secret) {
		t.Fatal("redirect ignored or exposed failed intermediate close", err)
	}
	s.mu.Lock()
	calls := s.bodyCalls
	s.mu.Unlock()
	if calls != 1 || redirectBody.closes != 1 {
		t.Fatal("redirect cleanup ownership or subsequent I/O", calls, redirectBody)
	}
	if _, err := c.Open(ctx, "u", core.Path("/remote/p%+中"), core.OpenOptions{}); core.CodeOf(err) != core.UpstreamAccessDenied ||
		!errors.Is(err, io.ErrUnexpectedEOF) || strings.Contains(err.Error(), secret) || finalBody == nil || finalBody.closes != 1 {
		t.Fatal("failed final download was not closed privately", err)
	}
	redirectBody.fail, finalBody.fail = false, false
	for _, name := range []string{"stalled", "junk"} {
		_, err := c.List(ctx, "u", core.ListRequest{Query: core.Children(core.Path("/remote/" + name)), Limit: 2, CursorTTL: time.Minute})
		if core.CodeOf(err) != core.InvalidUpstreamResponse {
			t.Fatal(name, err)
		}
	}
	base := d.api.Transport
	d.api.Transport = transport(func(r *http.Request) (*http.Response, error) {
		if r.URL.Query().Get("offset") != "" {
			return nil, io.EOF
		}
		return base.RoundTrip(r)
	})
	if _, err := c.List(ctx, "u", core.ListRequest{Query: core.Children(core.Path("/remote")), Limit: 2, CursorTTL: time.Minute}); err == nil || !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatal("transport EOF became directory end", err)
	}
	d.api.Transport = transport(func(r *http.Request) (*http.Response, error) {
		response, err := base.RoundTrip(r)
		if err == nil && r.Method == "PUT" {
			_ = response.Body.Close()
			return nil, errors.New("acknowledgement lost")
		}
		return response, err
	})
	if change, err := d.Mkdir(ctx, core.NativePath("/lost")); err == nil || change.Effect != core.Unknown {
		t.Fatal(change, err)
	}
	d.api.Transport = transport(func(r *http.Request) (*http.Response, error) {
		response, err := base.RoundTrip(r)
		if err == nil && r.Method == "PUT" {
			response.Body = &brokenClose{ReadCloser: response.Body, fail: true}
		}
		return response, err
	})
	change, err := d.Mkdir(ctx, core.NativePath("/known"))
	if err == nil || change.Effect != core.Applied {
		t.Fatal("known receipt lost", change, err)
	}
	for _, malformed := range []bool{false, true} {
		d.api.Transport = transport(func(r *http.Request) (*http.Response, error) {
			if strings.Contains(r.URL.Path, "/operations/") {
				payload := `{"status":"success"}`
				if malformed {
					payload = `{"status":"success","status":123}`
				}
				body := &brokenClose{ReadCloser: io.NopCloser(strings.NewReader(payload)), fail: !malformed}
				return &http.Response{StatusCode: 200, Header: make(http.Header), Body: body}, nil
			}
			return base.RoundTrip(r)
		})
		change, err := d.RemoveSubtree(ctx, core.NativePath("/tree"), core.TreeOptions{})
		expected := core.Applied
		if malformed {
			expected = core.Unknown
		}
		if err == nil || change.Effect != expected {
			t.Fatal("partial JSON and cleanup confirmation confused", malformed, change, err)
		}
	}
	d.mu.Lock()
	for _, owned := range d.failed {
		if b, ok := owned.(*brokenClose); ok {
			b.fail = false
		}
	}
	d.mu.Unlock()
	if err := c.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if redirectBody.closes != 2 || finalBody.closes != 2 {
		t.Fatal("download response cleanup was not retried exactly once", redirectBody.closes, finalBody.closes)
	}
	body := &brokenClose{ReadCloser: io.NopCloser(bytes.NewReader([]byte(`{"path":"disk:/","name":"disk","type":"dir"}`))), fail: true}
	bad, err := Open(ctx, Options{Client: &http.Client{Transport: transport(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: body}, nil
	})}})
	if err == nil || bad == nil {
		t.Fatal("failed root cleanup forgotten")
	}
	body.fail = false
	if err := bad.Close(); err != nil {
		t.Fatal(err)
	}
}
