package cloudreve_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path"
	"strings"
	"sync"
	"testing"
	"time"

	core "github.com/nostalume/mountweave"
	driver "github.com/nostalume/mountweave/drivers/cloudreve"
)

const uploadChunk = 320 << 10

type nativeWrite struct {
	uri                 string
	size, offset        int64
	id, entity, primary string
	complete, retired   bool
	data                []byte
}

type writeFixture struct {
	mode                                                                  string
	mu                                                                    sync.Mutex
	sessions                                                              map[string]*nativeWrite
	files                                                                 map[string]*nativeWrite
	rootID                                                                string
	finals, callbacks                                                     int
	aborts                                                                int
	bodyAborts, puts, queries, identities, initiations, creates, uploaded int
	failAbort, held                                                       bool
	started, release                                                      chan struct{}
	late                                                                  io.Reader
	closeFault                                                            error
	abortBody                                                             *retryCloseBody
	responseBody                                                          *retryCloseBody
	cancel                                                                context.CancelFunc
}

type retryCloseBody struct {
	io.ReadCloser
	fault error
	calls int
}

func (b *retryCloseBody) Close() error {
	b.calls++
	if b.calls == 1 {
		return b.fault
	}
	return b.ReadCloser.Close()
}

func (s *writeFixture) roundTrip(r *http.Request) (*http.Response, error) {
	if r.URL.Host == "upload.test" {
		if r.Header.Get("Authorization") != "" || r.Header.Get("Cookie") != "" {
			return nil, errors.New("body recipient or credentials changed")
		}
		if r.Method == http.MethodGet || r.Method == http.MethodDelete {
			s.mu.Lock()
			defer s.mu.Unlock()
			u := s.sessions[strings.TrimPrefix(r.URL.Path, "/")]
			if r.Method == http.MethodDelete {
				s.bodyAborts++
				if s.failAbort {
					return reply(r, 503, 500, nil), nil
				}
				if u != nil {
					u.retired = true
				}
				response := reply(r, 204, 0, nil)
				if s.mode == "tail" {
					s.abortBody = &retryCloseBody{ReadCloser: response.Body, fault: s.closeFault}
					response.Body = s.abortBody
				}
				return response, nil
			}
			s.queries++
			if u == nil || u.retired || u.offset == u.size {
				return reply(r, 404, 404, nil), nil
			}
			payload, _ := json.Marshal(map[string]any{"nextExpectedRanges": []string{fmt.Sprint(u.offset) + "-"}})
			return &http.Response{Request: r, StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(bytes.NewReader(payload)), ContentLength: int64(len(payload))}, nil
		}
		if r.Method != http.MethodPut {
			return nil, errors.New("unexpected body operation")
		}
		data, err := io.ReadAll(r.Body)
		if err != nil {
			return nil, err
		}
		var start, end, total int64
		if _, err := fmt.Sscanf(r.Header.Get("Content-Range"), "bytes %d-%d/%d", &start, &end, &total); err != nil {
			return nil, err
		}
		s.mu.Lock()
		s.puts++
		u := s.sessions[strings.TrimPrefix(r.URL.Path, "/")]
		if u == nil || total != u.size || start != u.offset || end-start+1 != int64(len(data)) || r.ContentLength != int64(len(data)) {
			s.mu.Unlock()
			return nil, errors.New("wrong native range or session")
		}
		if s.mode == "body-reject" {
			s.mu.Unlock()
			payload := `{"error":{"code":"accessDenied","message":"body permission denied"}}`
			return &http.Response{Request: r, StatusCode: 403, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(payload)), ContentLength: int64(len(payload))}, nil
		}
		u.offset = end + 1
		u.data = append(u.data, data...)
		s.uploaded += len(data)
		hold := s.mode == "hold" && !s.held
		if hold {
			s.held, s.late = true, r.Body
		}
		final := u.offset == total
		if final {
			s.finals++
		}
		next := u.offset
		s.mu.Unlock()
		if hold {
			close(s.started)
			<-s.release // Native I/O must actually exit before its buffer is returned.
			return nil, r.Context().Err()
		}
		status, result := 202, any(map[string]any{"nextExpectedRanges": []string{fmt.Sprint(next) + "-"}})
		if final {
			// Body storage acknowledgement is deliberately not a Graph file receipt.
			status, result = 201, map[string]any{"native_body_complete": true}
		}
		payload, _ := json.Marshal(result)
		response := &http.Response{Request: r, StatusCode: status, Header: make(http.Header), Body: io.NopCloser(bytes.NewReader(payload)), ContentLength: int64(len(payload))}
		if final && s.mode == "body-close" {
			s.responseBody = &retryCloseBody{ReadCloser: response.Body, fault: s.closeFault}
			response.Body = s.responseBody
		}
		if final && s.mode == "body-cancel" {
			s.cancel()
		}
		if !final && s.mode == "durable-pause" {
			s.cancel()
		}
		return response, nil
	}
	if r.URL.Host != "cloud.test" {
		return nil, errors.New("request left the closed service")
	}
	if strings.HasPrefix(r.URL.Path, "/api/v4/callback/onedrive/") {
		if r.Method != http.MethodPost || r.Header.Get("Authorization") != "" || r.Header.Get("Cookie") != "" {
			return nil, errors.New("account credentials reached callback capability")
		}
		parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/api/v4/callback/onedrive/"), "/")
		s.mu.Lock()
		defer s.mu.Unlock()
		if len(parts) != 2 || parts[1] != "cap" || s.sessions[parts[0]] == nil {
			return nil, errors.New("callback session changed")
		}
		s.callbacks++
		if s.mode == "callback-reject" {
			return reply(r, 200, 500, nil), nil
		}
		u := s.sessions[parts[0]]
		if u.offset != u.size {
			return nil, errors.New("callback preceded complete body")
		}
		u.complete, u.primary = true, u.entity
		if s.mode == "callback-lost" || strings.HasPrefix(s.mode, "durable-lost-") {
			return nil, errors.New("logical publication acknowledgement lost")
		}
		response := reply(r, 200, 0, nil)
		if s.mode == "callback-confirmed" {
			s.responseBody = &retryCloseBody{ReadCloser: response.Body, fault: s.closeFault}
			response.Body = s.responseBody
			s.cancel()
		}
		return response, nil
	}
	if r.Header.Get("Authorization") != "Bearer account" {
		return nil, errors.New("missing prepared account identity")
	}
	if r.URL.Path == "/api/v4/file/info" {
		uri := r.URL.Query().Get("uri")
		if uri == "cloudreve://my/vault" || uri == "cloudreve://my/other" {
			return reply(r, 200, 0, file(s.rootID, path.Base(uri), uri, 1, 0)), nil
		}
		if strings.HasPrefix(s.mode, "durable") && (r.URL.Query().Get("extended") == "true" || r.URL.Query().Get("id") != "") {
			s.identities++
			id := r.URL.Query().Get("id")
			for sid, u := range s.sessions {
				if id != "" && u.id != id || id == "" && u.uri != uri {
					continue
				}
				if s.files[u.id] == nil {
					break
				}
				out := file(u.id, path.Base(u.uri), u.uri, 0, 0)
				out["primary_entity"] = u.primary
				if !u.complete {
					out["metadata"] = map[string]string{"sys:upload_session_id": sid}
					if s.mode == "durable-foreign-session" {
						out["metadata"] = map[string]string{"sys:upload_session_id": "another-session"}
					}
				}
				entities := []any{}
				if u.entity != "" {
					entities = append(entities, map[string]any{"id": u.entity, "type": 0, "size": u.size})
				}
				if s.mode == "durable-ambiguous" {
					entities = append(entities, map[string]any{"id": "another-entity", "type": 0, "size": u.size})
				}
				if r.URL.Query().Get("extended") == "true" {
					out["extended_info"] = map[string]any{"entities": entities}
				}
				if u.complete {
					out["size"] = u.size
				}
				if id != "" && s.mode == "durable-lost-wrong-id" {
					out["id"] = "replacement"
				}
				return reply(r, 200, 0, out), nil
			}
			return reply(r, 200, 404, nil), nil
		}
		return nil, errors.New("publication acquired a target precheck or post-check")
	}
	if r.URL.Path == "/api/v4/file" && r.Method == http.MethodGet {
		s.mu.Lock()
		defer s.mu.Unlock()
		var files []any
		for id, u := range s.sessions {
			if s.mode == "complete" && s.callbacks != 0 {
				files = append(files, file(id, path.Base(u.uri), u.uri, 0, u.size))
			}
		}
		return reply(r, 200, 0, map[string]any{"files": files, "parent": file("vault", "vault", "cloudreve://my/vault", 1, 0), "pagination": map[string]any{"is_cursor": true}}), nil
	}
	var input map[string]any
	if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
		return nil, err
	}
	if r.URL.Path == "/api/v4/file/create" && r.Method == http.MethodPost {
		s.creates++
		if input["type"] != "file" || input["err_on_conflict"] != true || input["error_on_conflict"] != nil || input["entity_type"] != nil {
			return nil, errors.New("empty file lost exclusive creation")
		}
		uri, _ := input["uri"].(string)
		return reply(r, 200, 0, file("empty", path.Base(uri), uri, 0, 0)), nil
	}
	if r.URL.Path != "/api/v4/file/upload" {
		return nil, fmt.Errorf("unexpected native operation %s %s", r.Method, r.URL)
	}
	if s.mode == "init-lost" && r.Method == http.MethodPut {
		s.started <- struct{}{}
		<-s.release
		return nil, errors.New("upload initialization reply lost")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if r.Method == http.MethodDelete {
		s.aborts++
		id, _ := input["id"].(string)
		u := s.sessions[id]
		if u == nil || input["uri"] != u.uri {
			return nil, errors.New("abort selected a different native resource")
		}
		if s.failAbort {
			return reply(r, 500, 500, nil), nil
		}
		delete(s.sessions, id)
		response := reply(r, 200, 0, nil)
		if s.mode == "tail" {
			s.abortBody = &retryCloseBody{ReadCloser: response.Body, fault: s.closeFault}
			response.Body = s.abortBody
		}
		return response, nil
	}
	if r.Method != http.MethodPut || input["entity_type"] != nil {
		return nil, errors.New("upload selected a caller-owned entity type")
	}
	if s.mode == "exists" {
		return reply(r, 200, 40004, nil), nil
	}
	uri, _ := input["uri"].(string)
	size, _ := input["size"].(float64)
	s.initiations++
	id := fmt.Sprint("session-", len(s.sessions)+s.aborts+1)
	u := &nativeWrite{uri: uri, size: int64(size), id: "file-" + id, entity: "entity-" + id, primary: "encoded-zero"}
	s.sessions[id], s.files[u.id] = u, u
	if s.mode == "durable-init-lost" {
		return nil, errors.New("upload initialization reply lost")
	}
	chunk, policy := uploadChunk, "onedrive"
	if strings.HasSuffix(uri, "/wide") {
		chunk *= 2
	}
	if s.mode == "unsupported" {
		policy = "s3"
	}
	return reply(r, 200, 0, map[string]any{
		"session_id": id, "chunk_size": chunk, "uri": uri, "callback_secret": "cap",
		"expires":        time.Now().Add(time.Hour).Unix(),
		"upload_urls":    []string{"https://upload.test/" + id},
		"storage_policy": map[string]any{"type": policy, "relay": false},
	}), nil
}

func writing(t *testing.T, mode string) (*writeFixture, *driver.Driver, *core.Core) {
	t.Helper()
	s := &writeFixture{mode: mode, rootID: "vault", sessions: make(map[string]*nativeWrite), files: make(map[string]*nativeWrite), started: make(chan struct{}, 2), release: make(chan struct{})}
	if mode == "tail" || mode == "body-close" || mode == "callback-confirmed" {
		s.closeFault = errors.New("native response close failed")
	}
	d, c := mountWriting(t, s)
	return s, d, c
}

func mountWriting(t *testing.T, s *writeFixture, configure ...func(*driver.Options)) (*driver.Driver, *core.Core) {
	t.Helper()
	api := &http.Client{Transport: transport(func(r *http.Request) (*http.Response, error) {
		r.Header.Set("Authorization", "Bearer account")
		return s.roundTrip(r)
	})}
	opts := driver.Options{APIClient: api, ContentClient: &http.Client{Transport: transport(s.roundTrip)}, Endpoint: "https://cloud.test", RootURI: "cloudreve://my/vault", MaxBufferedBytes: 2 * uploadChunk, RangeAttempts: 2, CleanupTimeout: time.Second}
	for _, option := range configure {
		option(&opts)
	}
	d, err := driver.Open(context.Background(), opts)
	if err != nil {
		t.Fatal(err)
	}
	c, err := core.New(core.Options{Authorize: func(context.Context, core.Principal, core.Action, core.Address) error { return nil }})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Install("/cloud", d); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		err := c.Close(ctx)
		pending := s.mode == "callback-reject" || s.mode == "callback-lost" || s.mode == "init-lost" || s.mode == "unsupported"
		if (err != nil) != pending {
			t.Errorf("retirement with pending=%t: %v", pending, err)
		}
	})
	return d, c
}

type uploadTail struct {
	*bytes.Reader
	err error
}

func (r uploadTail) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	if err == io.EOF {
		err = r.err
	}
	return n, err
}

type uploadSource struct {
	data          []byte
	version       string
	opens, closes int
}

func (s *uploadSource) Read([]byte) (int, error) {
	return 0, errors.New("durable input bypassed verified ranges")
}
func (s *uploadSource) SourceVersion() string { return s.version }
func (s *uploadSource) OpenRange(ctx context.Context, start, length int64) (io.ReadCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if start < 0 || length < 0 || start+length > int64(len(s.data)) {
		return nil, io.ErrUnexpectedEOF
	}
	s.opens++
	return &uploadRange{Reader: bytes.NewReader(s.data[start : start+length]), source: s}, nil
}

type uploadRange struct {
	*bytes.Reader
	source *uploadSource
}

func (r *uploadRange) Close() error { r.source.closes++; return nil }

func TestDurableUploadRecovery(t *testing.T) {
	ctx := context.Background()
	data := make([]byte, 2*uploadChunk)
	for i := range data {
		data[i] = byte(i % 251)
	}
	for _, mode := range []string{"pause", "lost-rename", "lost-nonprimary", "lost-pruned", "lost-missing", "lost-wrong-id", "body_ready", "confirming", "done", "init-lost", "ambiguous", "foreign-session", "empty"} {
		t.Run(mode, func(t *testing.T) {
			s, d, c := writing(t, "durable-"+mode)
			source := &uploadSource{data: data, version: "sealed-source-1"}
			if mode == "empty" {
				source.data = nil
			}
			n := int64(len(source.data))
			runCtx, cancel := context.WithCancel(ctx)
			defer cancel()
			s.cancel = cancel
			var checkpoint []byte
			appends := 0
			fault := errors.New("durable append failed")
			failKind := ""
			if mode == "body_ready" || mode == "confirming" || mode == "done" {
				failKind = mode
			}
			appendRecord := func(_ context.Context, delta []byte) error {
				var record struct {
					Kind string `json:"kind"`
				}
				if err := json.Unmarshal(delta, &record); err != nil {
					return err
				}
				appends++
				if record.Kind == failKind && failKind != "aborting" {
					return fault
				}
				checkpoint = append(checkpoint, delta...)
				if record.Kind == failKind {
					return fault
				}
				return nil
			}
			write := func(client *core.Core, policy core.WritePolicy, abort bool) (core.Change, error) {
				return client.Write(runCtx, "user", core.Path("/cloud/output"), source, &n, core.WriteOptions{Policy: policy, Continuation: &core.WriteContinuation{Checkpoint: bytes.Clone(checkpoint), Append: appendRecord, Abort: abort}})
			}
			change, err := write(c, core.WritePolicy{}, false)
			switch {
			case mode == "empty":
				if err != nil || change.Effect != core.Applied || s.creates != 1 || s.initiations != 0 || s.identities != 0 {
					t.Fatal("empty creation acquired recovery resources", change, err)
				}
			case mode == "pause":
				if !errors.Is(err, context.Canceled) || change.Effect != core.Incomplete || s.puts != 1 || s.callbacks != 0 {
					t.Fatal("pause lost the visible placeholder", change, err)
				}
				if err := errors.Join(d.Cleanup(ctx), c.Close(ctx)); err != nil || s.bodyAborts != 0 {
					t.Fatal("ordinary retirement adopted a durable session", err)
				}
				runCtx, s.mode = ctx, "durable-complete"
				_, c = mountWriting(t, s)
				source.version = "other-source"
				if _, err := write(c, core.WritePolicy{}, false); err == nil || s.puts != 1 || source.opens != 1 {
					t.Fatal("different source resumed", err)
				}
				source.version = "sealed-source-1"
				if _, err := write(c, core.Publish(core.IfAbsent()), false); core.CodeOf(err) != core.UnsupportedOperation || s.puts != 1 || source.opens != 1 {
					t.Fatal("different policy resumed", err)
				}
				s.rootID = "other-account-root"
				_, wrong := mountWriting(t, s)
				if _, err := write(wrong, core.WritePolicy{}, false); err == nil || s.puts != 1 || source.opens != 1 {
					t.Fatal("different native root resumed", err)
				}
				s.rootID = "vault"
				_, wrong = mountWriting(t, s, func(o *driver.Options) { o.RootURI = "cloudreve://my/other" })
				if _, err := write(wrong, core.WritePolicy{}, false); err == nil || s.puts != 1 || source.opens != 1 {
					t.Fatal("different root URI resumed", err)
				}
				if change, err = write(c, core.WritePolicy{}, false); err != nil || change.Effect != core.Applied || s.puts != 2 || s.queries != 1 || s.uploaded != len(data) || s.callbacks != 1 {
					t.Fatal("native progress did not resume the remaining range", change, err, s.puts, s.queries)
				}
				for _, u := range s.files {
					if !bytes.Equal(u.data, data) {
						t.Fatal("resumed body changed")
					}
				}
			case strings.HasPrefix(mode, "lost-"):
				if err == nil || change.Effect != core.Unknown || s.callbacks != 1 {
					t.Fatal("body receipt became a logical receipt", change, err)
				}
				for _, u := range s.files {
					switch mode {
					case "lost-rename":
						u.uri = "cloudreve://my/vault/renamed"
					case "lost-nonprimary":
						u.primary = "later-version"
					case "lost-pruned":
						u.primary, u.entity = "later-version", ""
					case "lost-missing":
						delete(s.files, u.id)
					}
				}
				_, c = mountWriting(t, s)
				opened, puts := source.opens, s.puts
				change, err = write(c, core.WritePolicy{}, false)
				if mode == "lost-rename" {
					if err != nil || change.Effect != core.Applied {
						t.Fatal("same entity was lost across rename", change, err)
					}
				} else if err == nil || change.Effect != core.Unknown {
					t.Fatal("missing entity proof was invented", change, err)
				}
				if s.callbacks != 1 || s.puts != puts || source.opens != opened {
					t.Fatal("uncertain callback was blindly replayed")
				}
				if mode != "lost-rename" {
					if change, err = write(c, core.WritePolicy{}, true); err == nil || change.Effect != core.Unknown || s.bodyAborts != 1 {
						t.Fatal("private cleanup erased logical uncertainty", change, err)
					}
				}
			case mode == "init-lost":
				if err == nil || change.Effect != core.Unknown || s.initiations != 1 || source.opens != 0 {
					t.Fatal("unknown preparation lost its visible effect", change, err)
				}
				_, c = mountWriting(t, s)
				if change, err = write(c, core.WritePolicy{}, false); err == nil || change.Effect != core.Unknown || s.initiations != 1 {
					t.Fatal("unknown placeholder was recreated or adopted", change, err)
				}
			case mode == "ambiguous" || mode == "foreign-session":
				if err == nil || change.Effect != core.Incomplete || source.opens != 0 || s.puts != 0 || s.callbacks != 0 {
					t.Fatal("unproven file/entity identity consumed source", change, err)
				}
			default:
				want := core.Incomplete
				callbacks := 0
				if mode == "done" {
					want = core.Applied
					callbacks = 1
				}
				if !errors.Is(err, fault) || change.Effect != want || s.callbacks != callbacks {
					t.Fatal("journal failure crossed its effect boundary", change, err, s.callbacks)
				}
				failKind = ""
				_, c = mountWriting(t, s)
				puts := s.puts
				change, err = write(c, core.WritePolicy{}, false)
				if mode == "body_ready" {
					if err == nil || change.Effect != core.Incomplete || s.callbacks != 0 {
						t.Fatal("404 session substituted for a body receipt", change, err)
					}
					failKind = "aborting"
					if change, err = write(c, core.WritePolicy{}, true); !errors.Is(err, fault) || change.Effect != core.Incomplete || s.bodyAborts != 0 {
						t.Fatal("native cancellation crossed an unconfirmed journal barrier", change, err)
					}
					failKind = ""
					_, c = mountWriting(t, s)
					opened := source.opens
					if change, err = write(c, core.WritePolicy{}, false); err == nil || change.Effect != core.Incomplete || s.puts != puts || source.opens != opened || s.callbacks != 0 {
						t.Fatal("uncertain cancellation resumed content", change, err)
					}
					if change, err = write(c, core.WritePolicy{}, true); err == nil || change.Effect != core.Incomplete || s.bodyAborts != 1 {
						t.Fatal("body cleanup erased the placeholder", change, err)
					}
					if change, err = write(c, core.WritePolicy{}, false); err == nil || change.Effect != core.Incomplete || s.puts != puts || source.opens != opened || s.callbacks != 0 {
						t.Fatal("retired body resumed content", change, err)
					}
				} else if err != nil || change.Effect != core.Applied || s.callbacks != 1 {
					t.Fatal("saved body or file receipt was not recovered", change, err)
				}
				if s.puts != puts {
					t.Fatal("recovery resent confirmed content")
				}
			}
			if s.aborts != 0 || source.opens != source.closes || appends > 12 {
				t.Fatal("unsafe cancellation or resource/journal growth", s.aborts, source.opens, source.closes, appends)
			}
			t.Logf("PUT=%d bytes=%d ranges=%d GET=%d info=%d append=%d callback=%d body-delete=%d", s.puts, s.uploaded, source.opens, s.queries, s.identities, appends, s.callbacks, s.bodyAborts)
		})
	}
}

func TestUploadPublicationAndInput(t *testing.T) {
	for _, mode := range []string{"complete", "empty", "exists", "tail", "body-reject", "body-close", "body-cancel", "callback-confirmed", "callback-reject", "callback-lost", "unsupported"} {
		t.Run(mode, func(t *testing.T) {
			s, d, c := writing(t, mode)
			data := bytes.Repeat([]byte("x"), uploadChunk+17)
			if mode == "empty" {
				data = nil
			}
			reader := bytes.NewReader(data)
			var input io.Reader = reader
			fault := errors.New("source failed at EOF")
			if mode == "tail" {
				input = uploadTail{reader, fault}
			}
			n := int64(len(data))
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			s.cancel = cancel
			if mode == "complete" {
				unchanged := bytes.NewReader(data)
				if change, err := c.Write(context.Background(), "user", core.Path("/cloud/no-publish"), unchanged, &n, core.WriteOptions{Policy: core.Publish(core.IfAbsent())}); core.CodeOf(err) != core.UnsupportedOperation || change.Effect != core.Unchanged || unchanged.Len() != len(data) {
					t.Fatal("direct creation was advertised as complete publication", change, err)
				}
			}
			change, err := c.Write(ctx, "user", core.Path("/cloud/output"), input, &n)
			switch mode {
			case "complete", "empty":
				if err != nil || change.Effect != core.Applied {
					t.Fatal(change, err)
				}
			case "callback-confirmed":
				if change.Effect != core.Applied || !errors.Is(err, s.closeFault) || !errors.Is(err, context.Canceled) || s.responseBody.calls != 1 {
					t.Fatal("confirmed file or single response owner lost", change, err)
				}
			case "body-close", "body-cancel":
				if change.Effect != core.Incomplete || err == nil || mode == "body-close" && !errors.Is(err, s.closeFault) || mode == "body-cancel" && !errors.Is(err, context.Canceled) {
					t.Fatal("failed content escaped into file confirmation", change, err)
				}
			case "tail", "unsupported", "body-reject":
				if err == nil || change.Effect != core.Incomplete || mode == "tail" && !errors.Is(err, fault) || mode == "unsupported" && (reader.Len() != len(data) || core.CodeOf(err) != core.UnsupportedOperation) {
					t.Fatal(change, err)
				}
				if mode == "body-reject" && (core.CodeOf(err) != core.UpstreamAccessDenied || !strings.Contains(err.Error(), "accessDenied") || !strings.Contains(err.Error(), "body permission denied") || reader.Len() == 0 || reader.Len() == len(data)) {
					t.Fatal("body error lost its native cause or consumed later input", err, reader.Len())
				}
				if mode == "tail" {
					if !errors.Is(err, s.closeFault) || s.abortBody == nil || s.abortBody.calls != 1 {
						t.Fatal("confirmed cancellation lost its cleanup failure", err)
					}
					if err := d.Cleanup(context.Background()); err != nil || s.bodyAborts != 1 {
						t.Fatal("response close failure repeated native cancellation", err, s.aborts)
					}
					if err := c.Close(context.Background()); err != nil || s.abortBody.calls != 2 {
						t.Fatal("retirement did not retain response cleanup", err, s.abortBody.calls)
					}
				}
			case "exists":
				if core.CodeOf(err) != core.AlreadyExists || change.Effect != core.Unchanged || reader.Len() != len(data) {
					t.Fatal("native exclusion consumed input or changed the target", change, err)
				}
			default:
				if err == nil || change.Effect != core.Unknown {
					t.Fatal(change, err)
				}
				if cleanup := d.Cleanup(context.Background()); cleanup == nil {
					t.Fatal("uncertain logical publication was treated as safe to abort")
				}
			}
			if mode == "tail" || mode == "unsupported" || mode == "body-reject" {
				wantDeletes := 1
				if mode == "unsupported" {
					wantDeletes = 0
				}
				if s.finals != 0 || s.callbacks != 0 || s.aborts != 0 || s.bodyAborts != wantDeletes {
					t.Fatal("failed input/policy reached publication or unsafe cancellation", s.finals, s.callbacks, s.aborts, s.bodyAborts)
				}
			} else if mode == "body-close" || mode == "body-cancel" {
				if s.finals != 1 || s.callbacks != 0 || s.aborts != 0 || s.bodyAborts != 1 {
					t.Fatal("failed content triggered another effect", s.finals, s.callbacks, s.aborts)
				}
			} else if mode == "empty" || mode == "exists" {
				if s.finals != 0 || s.callbacks != 0 || len(s.sessions) != 0 {
					t.Fatal("empty file created an upload session")
				}
			} else if s.finals != 1 || s.callbacks != 1 || s.aborts != 0 {
				t.Fatal("body and logical publication were conflated", s.finals, s.callbacks, s.aborts)
			}
			if mode == "complete" {
				page, err := c.List(context.Background(), "user", core.ListRequest{Query: core.Children(core.Path("/cloud")), Limit: 10, CursorTTL: time.Minute})
				if err != nil || len(page.Items) != 1 || page.Items[0].Address != core.Path("/cloud/output") || page.Items[0].Size == nil || *page.Items[0].Size != n || page.NextCursor != nil {
					t.Fatal("published file did not enter the ordinary tree", page, err)
				}
			}
		})
	}
}

func TestUploadCancellationBudgetAndCleanup(t *testing.T) {
	t.Run("independent-unknown-initializations", func(t *testing.T) {
		s, d, c := writing(t, "init-lost")
		release := sync.OnceFunc(func() { close(s.release) })
		t.Cleanup(release)
		data := []byte("body")
		n := int64(len(data))
		done := make(chan error, 2)
		for _, target := range []string{"/cloud/init-a", "/cloud/init-b"} {
			go func() {
				_, err := c.Write(context.Background(), "user", core.Path(target), bytes.NewReader(data), &n)
				done <- err
			}()
		}
		<-s.started
		<-s.started
		release()
		if (<-done) == nil || (<-done) == nil {
			t.Fatal("unknown initialization was accepted")
		}
		if err := d.Cleanup(context.Background()); err == nil || !strings.Contains(err.Error(), "/init-a") || !strings.Contains(err.Error(), "/init-b") || !strings.Contains(err.Error(), "upload 1 for ") || !strings.Contains(err.Error(), "upload 2 for ") {
			t.Fatal("unknown resources were coalesced or lost their targets", err)
		}
	})
	s, d, c := writing(t, "hold")
	release := sync.OnceFunc(func() { close(s.release) })
	t.Cleanup(release)
	data := bytes.Repeat([]byte("x"), uploadChunk+17)
	n := int64(len(data))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	type outcome struct {
		change core.Change
		err    error
	}
	done := make(chan outcome, 1)
	go func() {
		change, err := c.Write(ctx, "user", core.Path("/cloud/held"), bytes.NewReader(data), &n)
		done <- outcome{change, err}
	}()
	select {
	case <-s.started:
	case <-time.After(3 * time.Second):
		t.Fatal("native fragment did not begin")
	}
	wide := bytes.NewReader(data)
	if change, err := c.Write(context.Background(), "user", core.Path("/cloud/wide"), wide, &n); core.CodeOf(err) != core.CapacityExceeded || change.Effect != core.Incomplete || wide.Len() != len(data) {
		t.Fatal("different chunk sizes escaped the shared budget", change, err)
	}
	s.mu.Lock()
	s.failAbort = true
	s.mu.Unlock()
	cancel()
	select {
	case result := <-done:
		t.Fatal("request retained a borrowed buffer after return", result)
	default:
	}
	release()
	select {
	case result := <-done:
		if !errors.Is(result.err, context.Canceled) || result.change.Effect != core.Incomplete {
			t.Fatal(result)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("canceled native request did not exit")
	}
	if got, err := s.late.Read(make([]byte, 1)); got != 0 || !errors.Is(err, io.ErrClosedPipe) {
		t.Fatal("late request read retained a returned buffer", got, err)
	}
	if err := d.Cleanup(context.Background()); err == nil {
		t.Fatal("failed abort lost ownership")
	}
	s.mu.Lock()
	s.failAbort = false
	s.mu.Unlock()
	before := s.bodyAborts
	if err := d.Cleanup(context.Background()); err == nil || s.aborts != 0 || s.bodyAborts != before+1 {
		t.Fatal("private cleanup erased the remaining logical placeholder", err, s.aborts, s.bodyAborts)
	}
	if change, err := c.Write(context.Background(), "user", core.Path("/cloud/reuse"), bytes.NewReader(data), &n); err != nil || change.Effect != core.Applied {
		t.Fatal("confirmed cleanup did not release admission", change, err)
	}
}
