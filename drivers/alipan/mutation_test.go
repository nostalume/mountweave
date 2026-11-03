package alipan_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	core "github.com/nostalume/mountweave"
	driver "github.com/nostalume/mountweave/drivers/alipan"
	"github.com/nostalume/mountweave/drivers/alipan/credential"
	"github.com/nostalume/mountweave/local"
)

type idEntry struct {
	id, parent, name, kind string
	size                   int64
	parts                  map[int][]byte
	data                   []byte
}

func (e *idEntry) metadata() map[string]any {
	return map[string]any{"drive_id": "drive", "file_id": e.id, "parent_file_id": e.parent, "name": e.name, "type": e.kind, "size": e.size}
}

type idProtocol struct {
	mu           sync.Mutex
	entries      map[string]*idEntry
	next         int
	calls        map[string]int
	lostComplete bool
	rejectPart   bool
	rejectedBody io.Reader
	afterCreate  func()
	asyncDelete  bool
	deleteID     string
	deletePolls  int
	cancelQuery  func()
}

func newIDProtocol() *idProtocol {
	return &idProtocol{entries: make(map[string]*idEntry), calls: make(map[string]int)}
}
func (s *idProtocol) roundTrip(r *http.Request) (*http.Response, error) {
	if r.URL.Host == "upload.example.test" {
		if r.Header.Get("Authorization") != "" || r.Header.Get("Cookie") != "" {
			return nil, errors.New("bearer/cookie leaked to native upload")
		}
		parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/"), "/")
		if len(parts) != 2 {
			return nil, errors.New("invalid upload identity")
		}
		s.mu.Lock()
		if s.rejectPart {
			s.rejectPart, s.rejectedBody = false, r.Body
			s.mu.Unlock()
			return response(r, 403, ""), nil
		}
		s.mu.Unlock()
		data, err := io.ReadAll(r.Body)
		if err != nil {
			return nil, err
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		s.calls["part"]++
		e := s.entries[parts[0]]
		n, _ := strconv.Atoi(parts[1])
		e.parts[n] = data
		return response(r, 200, ""), nil
	}
	if r.URL.Host == "download.example.test" {
		s.mu.Lock()
		data := bytes.Clone(s.entries[strings.TrimPrefix(r.URL.Path, "/")].data)
		s.mu.Unlock()
		if r.Header.Get("Authorization") != "" {
			return nil, errors.New("bearer leaked to download")
		}
		if r.Header.Get("Range") != "" {
			var start, end int
			fmt.Sscanf(r.Header.Get("Range"), "bytes=%d-%d", &start, &end)
			out := response(r, 206, string(data[start:end+1]))
			out.Header.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(data)))
			return out, nil
		}
		return response(r, 200, string(data)), nil
	}
	if r.URL.Host != "openapi.alipan.com" || r.Header.Get("Authorization") != "Bearer provider-secret" {
		return nil, errors.New("wrong API authority")
	}
	var body map[string]any
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls[r.URL.Path]++
	text := func(key string) string { v, _ := body[key].(string); return v }
	var drop func(string)
	drop = func(id string) {
		for child, entry := range s.entries {
			if entry.parent == id {
				drop(child)
			}
		}
		delete(s.entries, id)
	}
	switch r.URL.Path {
	case "/adrive/v1.0/openFile/list":
		items := []any{}
		for _, e := range s.entries {
			if e.parent == text("parent_file_id") {
				items = append(items, e.metadata())
			}
		}
		return jsonResponse(r, map[string]any{"items": items}), nil
	case "/adrive/v1.0/openFile/create":
		if text("check_name_mode") != "refuse" {
			return nil, errors.New("new-file exclusion lost")
		}
		for _, e := range s.entries {
			if e.parent == text("parent_file_id") && e.name == text("name") {
				return jsonResponse(r, map[string]any{"exist": true, "drive_id": "drive", "file_id": e.id, "parent_file_id": e.parent, "file_name": e.name}), nil
			}
		}
		s.next++
		id := fmt.Sprint("id", s.next)
		n, _ := body["size"].(float64)
		e := &idEntry{id: id, parent: text("parent_file_id"), name: text("name"), kind: text("type"), size: int64(n), parts: make(map[int][]byte)}
		s.entries[id] = e
		if s.afterCreate != nil {
			s.afterCreate()
			s.afterCreate = nil
		}
		var urls []map[string]any
		if raw, ok := body["part_info_list"].([]any); ok {
			for _, p := range raw {
				number := int(p.(map[string]any)["part_number"].(float64))
				urls = append(urls, map[string]any{"part_number": number, "upload_url": fmt.Sprintf("https://upload.example.test/%s/%d?private=signature", id, number)})
			}
		}
		return jsonResponse(r, map[string]any{"drive_id": "drive", "file_id": id, "parent_file_id": e.parent, "file_name": e.name, "upload_id": "u-" + id, "part_info_list": urls}), nil
	case "/adrive/v1.0/openFile/complete":
		e := s.entries[text("file_id")]
		if text("upload_id") != "u-"+e.id {
			return nil, errors.New("wrong upload")
		}
		for i := 1; i <= len(e.parts); i++ {
			e.data = append(e.data, e.parts[i]...)
		}
		if int64(len(e.data)) != e.size {
			return nil, errors.New("native size mismatch")
		}
		if s.lostComplete {
			return nil, errors.New("completion acknowledgement lost")
		}
		return jsonResponse(r, e.metadata()), nil
	case "/adrive/v1.0/openFile/getDownloadUrl":
		return jsonResponse(r, map[string]any{"url": "https://download.example.test/" + text("file_id"), "method": "GET"}), nil
	case "/adrive/v1.0/openFile/update":
		e := s.entries[text("file_id")]
		if text("check_name_mode") != "refuse" {
			return nil, errors.New("rename lost exclusion")
		}
		e.name = text("name")
		return jsonResponse(r, e.metadata()), nil
	case "/adrive/v1.0/openFile/move":
		e := s.entries[text("file_id")]
		if text("check_name_mode") != "refuse" {
			return nil, errors.New("move lost exclusion")
		}
		e.parent = text("to_parent_file_id")
		return jsonResponse(r, map[string]any{"drive_id": "drive", "file_id": e.id, "exist": false}), nil
	case "/adrive/v1.0/openFile/delete":
		e := s.entries[text("file_id")]
		if s.asyncDelete {
			s.deleteID, s.deletePolls = e.id, 0
			return jsonResponse(r, map[string]any{"drive_id": "drive", "file_id": e.id, "async_task_id": "delete-task"}), nil
		}
		drop(e.id)
		return jsonResponse(r, map[string]any{"drive_id": "drive", "file_id": e.id}), nil
	case "/adrive/v1.0/openFile/async_task/get":
		if text("async_task_id") != "delete-task" {
			return nil, errors.New("wrong deletion task")
		}
		s.deletePolls++
		state := "Running"
		if s.deletePolls > 1 {
			drop(s.deleteID)
			state = "Succeed"
		}
		if s.cancelQuery != nil {
			s.cancelQuery()
			s.cancelQuery = nil
		}
		return jsonResponse(r, map[string]any{"async_task_id": "delete-task", "state": state}), nil
	}
	return nil, fmt.Errorf("unexpected API %s", r.URL.Path)
}

type moveOnRead struct {
	io.Reader
	once   sync.Once
	move   func()
	closed bool
}

func (r *moveOnRead) Read(p []byte) (int, error) { r.once.Do(r.move); return r.Reader.Read(p) }
func (r *moveOnRead) Close() error               { r.closed = true; return nil }

type brokenEOF struct {
	io.Reader
	fault error
}

func (r brokenEOF) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	if err == io.EOF {
		return n, r.fault
	}
	return n, err
}
func readIDFile(t *testing.T, c *core.Core, p string, wanted *core.Range) []byte {
	t.Helper()
	o, err := c.Open(context.Background(), "writer", p, wanted)
	if err != nil {
		t.Fatal(err)
	}
	b, err := io.ReadAll(o.Body)
	err = errors.Join(err, o.Body.Close())
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestIDMutationAndBoundedUploadWorkflow(t *testing.T) {
	ctx := context.Background()
	localRoot := t.TempDir()
	server := newIDProtocol()
	_, c := prepared(t, server.roundTrip, func(o *driver.Options) {
		o.PartSize = 3
		o.MaxBufferedBytes = 6
		o.PartConcurrency = 2
		o.DeletionPollInterval = time.Microsecond
	})
	if _, err := c.Mkdir(ctx, "writer", "/cloud/dir"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Mkdir(ctx, "writer", "/cloud/dir"); core.CodeOf(err) != core.AlreadyExists {
		t.Fatal(err)
	}
	input := &moveOnRead{Reader: strings.NewReader("abcdefg"), move: func() {
		if _, err := c.Move(ctx, "writer", "/cloud/dir", "/cloud/renamed"); err != nil {
			t.Error(err)
		}
	}}
	n := int64(7)
	if change, err := c.Write(ctx, "writer", "/cloud/dir/file", input, &n); err != nil || change.Effect != core.Applied || input.closed {
		t.Fatal(change, err)
	}
	if got := readIDFile(t, c, "/cloud/renamed/file", &core.Range{Start: 2, End: 5}); string(got) != "cdef" {
		t.Fatal(string(got))
	}
	untouched := strings.NewReader("abcdefg")
	if change, err := c.Write(ctx, "writer", "/cloud/renamed/file", untouched, &n, core.WriteOptions{Replace: true}); core.CodeOf(err) != core.UnsupportedOperation || change.Effect != core.Unchanged || untouched.Len() != 7 {
		t.Fatal(change, err)
	}
	if change, err := c.Write(ctx, "writer", "/cloud/renamed/unknown", untouched, nil); core.CodeOf(err) != core.UnsupportedOperation || change.Effect != core.Unchanged || untouched.Len() != 7 {
		t.Fatal(change, err)
	}
	native, err := local.New(localRoot)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Install("/local", native); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Copy(ctx, "writer", "/cloud/renamed/file", "/local/copied"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Copy(ctx, "writer", "/local/copied", "/cloud/renamed/copied"); err != nil {
		t.Fatal(err)
	}
	rows, err := c.CopyTree(ctx, "writer", "/cloud/renamed", "/local/tree", core.TreeCopyOptions{MaxEntries: 10, MaxPlanBytes: 1024})
	if err != nil || len(rows) != 3 {
		t.Fatal(rows, err)
	}
	if _, err := c.Move(ctx, "writer", "/cloud/renamed/file", "/cloud/renamed/new-name"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Mkdir(ctx, "writer", "/cloud/other"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Move(ctx, "writer", "/cloud/renamed/new-name", "/cloud/other/new-name"); err != nil {
		t.Fatal(err)
	}
	if result, err := c.Move(ctx, "writer", "/cloud/other/new-name", "/cloud/renamed/another-name"); core.CodeOf(err) != core.UnsupportedOperation || result.Source.Effect != core.Unchanged {
		t.Fatal(result, err)
	}
	if change, err := c.Remove(ctx, "writer", "/cloud/other"); core.CodeOf(err) != core.UnsupportedOperation || change.Effect != core.Unchanged {
		t.Fatal(change, err)
	}
	if _, err := c.Remove(ctx, "writer", "/cloud/other/new-name"); err != nil {
		t.Fatal(err)
	}
	fault := errors.New("failed input EOF")
	if change, err := c.Write(ctx, "writer", "/cloud/renamed/partial", brokenEOF{strings.NewReader("abcdefg"), fault}, &n); !errors.Is(err, fault) || change.Effect != core.Incomplete {
		t.Fatal(change, err)
	}
	if _, err := c.Stat(ctx, "writer", "/cloud/renamed/partial"); err != nil {
		t.Fatal("incomplete native object was hidden", err)
	}
	if _, err := c.Remove(ctx, "writer", "/cloud/renamed/partial"); err != nil {
		t.Fatal(err)
	}
	server.rejectPart = true
	if change, err := c.Write(ctx, "writer", "/cloud/renamed/rejected", strings.NewReader("abcdefg"), &n); err == nil || change.Effect != core.Incomplete {
		t.Fatal(change, err)
	}
	if n, _ := server.rejectedBody.Read(make([]byte, 1)); n != 0 {
		t.Fatal("late upload read retained a released payload buffer")
	}
	if _, err := c.Remove(ctx, "writer", "/cloud/renamed/rejected"); err != nil {
		t.Fatal(err)
	}
	cancelCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	server.afterCreate = cancel
	if change, err := c.Write(cancelCtx, "writer", "/cloud/renamed/cancelled", strings.NewReader("abcdefg"), &n); !errors.Is(err, context.Canceled) || change.Effect != core.Incomplete {
		t.Fatal("creation acknowledgement is not upload completion", change, err)
	}
	if _, err := c.Remove(ctx, "writer", "/cloud/renamed/cancelled"); err != nil {
		t.Fatal(err)
	}
	server.mu.Lock()
	server.lostComplete = true
	before := server.calls["/adrive/v1.0/openFile/complete"]
	deletes := server.calls["/adrive/v1.0/openFile/delete"]
	server.mu.Unlock()
	if change, err := c.Write(ctx, "writer", "/cloud/renamed/lost", strings.NewReader("abcdefg"), &n); err == nil || change.Effect != core.Unknown {
		t.Fatal(change, err)
	}
	server.mu.Lock()
	if server.calls["/adrive/v1.0/openFile/complete"] != before+1 || server.calls["/adrive/v1.0/openFile/delete"] != deletes {
		t.Error("uncertain completion replayed or path-compensated")
	}
	server.mu.Unlock()
	if got := readIDFile(t, c, "/cloud/renamed/lost", nil); string(got) != "abcdefg" {
		t.Fatal(string(got))
	}
	policy := core.DirectoryRemoveOptions{Scope: core.Subtree}
	limits := core.TreeOptions{MaxEntries: 10, MaxPlanBytes: 1024}
	if _, err := c.RemoveTree(ctx, "writer", "/cloud/renamed", limits); core.CodeOf(err) != core.UnsupportedOperation {
		t.Fatal(err)
	}
	server.asyncDelete = true
	beforeDelete := server.calls["/adrive/v1.0/openFile/delete"]
	if rows, err := c.RemoveTree(ctx, "writer", "/cloud/renamed", limits, policy); err != nil || len(rows) != 1 || rows[0].Change.Effect != core.Applied || server.deletePolls != 2 || server.calls["/adrive/v1.0/openFile/delete"] != beforeDelete+1 {
		t.Fatal(rows, err, server.deletePolls)
	}
	if _, err := c.Stat(ctx, "writer", "/cloud/renamed"); core.CodeOf(err) != core.NotFound {
		t.Fatal(err)
	}
	if _, err := c.Mkdir(ctx, "writer", "/cloud/cancel-tree"); err != nil {
		t.Fatal(err)
	}
	pollCtx, stop := context.WithCancel(ctx)
	defer stop()
	server.cancelQuery = stop
	if rows, err := c.RemoveTree(pollCtx, "writer", "/cloud/cancel-tree", limits, policy); !errors.Is(err, context.Canceled) || len(rows) != 1 || rows[0].Change.Effect != core.Unknown {
		t.Fatal(rows, err)
	}
	if _, err := c.Stat(ctx, "writer", "/cloud/cancel-tree"); err != nil {
		t.Fatal("acceptance was mistaken for completion", err)
	}
}

func TestMutationAuthRejectionRefreshesWithoutReplay(t *testing.T) {
	ctx := context.Background()
	refreshes, creates := 0, 0
	source, err := credential.New(credential.Record{Token: credential.Token{Access: "old", Refresh: "refresh", ExpiresAt: time.Now().Add(time.Hour)}}, func(context.Context, string) (credential.Token, error) {
		refreshes++
		return credential.Token{Access: "new", Refresh: "next", ExpiresAt: time.Now().Add(time.Hour)}, nil
	}, func(context.Context, credential.Record) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	d, err := driver.New(driver.Options{DriveID: "drive", RootID: "root", Credentials: source, Client: &http.Client{Transport: transport(func(r *http.Request) (*http.Response, error) {
		if strings.HasSuffix(r.URL.Path, "/list") {
			return jsonResponse(r, map[string]any{"items": []any{}}), nil
		}
		creates++
		if r.Header.Get("Authorization") == "Bearer old" {
			return response(r, 401, `{"code":"AccessTokenExpired"}`), nil
		}
		return jsonResponse(r, map[string]any{"drive_id": "drive", "parent_file_id": "root", "file_id": "new-folder", "file_name": "dir"}), nil
	})}})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	if change, err := d.Mkdir(ctx, "/dir"); core.CodeOf(err) != core.UpstreamAccessDenied || change.Effect != core.Unchanged || creates != 1 || refreshes != 1 {
		t.Fatal(change, err, creates, refreshes)
	}
	if change, err := d.Mkdir(ctx, "/dir"); err != nil || change.Effect != core.Applied || creates != 2 || refreshes != 1 {
		t.Fatal(change, err, creates, refreshes)
	}
}
