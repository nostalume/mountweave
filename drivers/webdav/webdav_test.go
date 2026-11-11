package webdav_test

import (
	"context"
	"errors"
	"fmt"
	"html"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/local"
	driver "github.com/nostalume/mountweave/drivers/webdav"
	export "github.com/nostalume/mountweave/protocols/webdav"
)

func files(t *testing.T) *core.Core {
	t.Helper()
	c, err := core.New(core.Options{MaxCursors: 4, Authorize: func(context.Context, core.Principal, core.Action, core.Address) error { return nil }})
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

func TestSourceThroughExistingExport(t *testing.T) {
	ctx := context.Background()
	root, staging := t.TempDir(), t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "space % &"), []byte("abcdef"), 0600); err != nil {
		t.Fatal(err)
	}
	upstream := files(t)
	native, err := local.New(root, local.Options{StagingDirectory: staging, MaxStagingBytes: 64 << 10})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = upstream.Install("/files", native); err != nil {
		t.Fatal(err)
	}
	handler, err := export.New(upstream, export.Options{Prefix: "/webdav/", Root: core.Path("/files"), CursorTTL: time.Minute,
		Limits: export.Limits{XMLBytes: 4096, ListEntries: 16, ListNameBytes: 64 << 10},
		Tree:   core.TreeOptions{MaxEntries: 16, MaxPlanBytes: 64 << 10},
		Authenticate: func(w http.ResponseWriter, r *http.Request) (core.Principal, bool) {
			user, password, ok := r.BasicAuth()
			if ok && user == "alice" && password == "secret" {
				return "alice", true
			}
			w.Header().Set("WWW-Authenticate", "Basic realm=\"123pan\"")
			w.WriteHeader(http.StatusUnauthorized)
			return "", false
		}})
	if err != nil {
		t.Fatal(err)
	}
	foreign := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		t.Error("download contacted a foreign origin")
		w.WriteHeader(http.StatusForbidden)
	}))
	t.Cleanup(foreign.Close)
	var requests, canonicalizations atomic.Int64
	var redirectDownload atomic.Bool
	var deleteStatus atomic.Int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if r.Method == http.MethodDelete {
			if r.Header.Get("Depth") != "infinity" {
				t.Error("native recursive DELETE did not send Depth: infinity")
			}
			if status := int(deleteStatus.Swap(0)); status != 0 {
				if status == http.StatusTemporaryRedirect {
					w.Header().Set("Location", foreign.URL+"/folder")
				}
				w.WriteHeader(status)
				if status == http.StatusMultiStatus {
					_, _ = io.WriteString(w, `<d:multistatus xmlns:d="DAV:"><d:response><d:href>/webdav/folder/new</d:href><d:status>HTTP/1.1 423 Locked</d:status></d:response></d:multistatus>`)
				}
				return
			}
		}
		if r.Method == http.MethodGet && redirectDownload.Swap(false) {
			w.Header().Set("Location", foreign.URL+"/file")
			w.WriteHeader(http.StatusFound)
			return
		}
		if r.Method == "PROPFIND" && r.URL.Path == "/webdav/folder" {
			canonicalizations.Add(1)
			w.Header().Set("Location", "/webdav/folder/")
			w.WriteHeader(http.StatusMovedPermanently)
			return
		}
		if r.Method == http.MethodPut && r.URL.Path == "/webdav/folder/empty" && (r.ContentLength != 0 || len(r.TransferEncoding) != 0) {
			t.Errorf("zero-length PUT framing: length=%d transfer=%v", r.ContentLength, r.TransferEncoding)
		}
		handler.ServeHTTP(w, r)
	}))
	t.Cleanup(server.Close)
	d, err := driver.Open(ctx, driver.Options{RootURL: server.URL + "/webdav", Username: "alice", Password: "secret", Client: server.Client(), MaxResponseBytes: 64 << 10})
	if err != nil {
		t.Fatal(err)
	}
	c := files(t)
	mount, err := c.Install("/remote", d)
	if err != nil {
		t.Fatal(err)
	}
	if err = c.BindAlias(ctx, "/alias", mount, "/"); err != nil {
		t.Fatal(err)
	}
	entry, err := c.Stat(ctx, "alice", core.Path("/alias/space % &"))
	if err != nil || entry.Kind != core.File || entry.Size == nil || *entry.Size != 6 {
		t.Fatalf("stat: %+v %v", entry, err)
	}
	if _, err := c.Stat(ctx, "alice", core.Path("/remote/missing")); core.CodeOf(err) != core.NotFound {
		t.Errorf("missing stat lost native status: %v", err)
	}
	opened, err := c.Open(ctx, "alice", entry.Address, core.OpenOptions{Range: &core.Range{Start: 1, End: 3}})
	if err != nil {
		t.Fatal(err)
	}
	data, readErr := io.ReadAll(opened.Body)
	if err := errors.Join(readErr, opened.Body.Close()); err != nil || string(data) != "bcd" || opened.Range == nil || *opened.Range != (core.ContentRange{Start: 1, End: 3, Total: 6}) {
		t.Fatalf("range: %q %+v %v", data, opened.Range, err)
	}
	redirectDownload.Store(true)
	if _, err := c.Open(ctx, "alice", entry.Address, core.OpenOptions{}); core.CodeOf(err) != core.UpstreamFailure ||
		!strings.Contains(err.Error(), "HTTP 302") || redirectDownload.Load() {
		t.Fatalf("foreign download redirect: %v", err)
	}
	if changed, err := c.Mkdir(ctx, "alice", core.Path("/remote/folder")); err != nil || changed.Effect != core.Applied {
		t.Fatalf("mkdir: %+v %v", changed, err)
	}
	length := int64(6)
	if changed, err := c.Write(ctx, "alice", core.Path("/remote/folder/new"), strings.NewReader("stream"), &length); err != nil || changed.Effect != core.Applied {
		t.Fatalf("write: %+v %v", changed, err)
	}
	if changed, err := c.Write(ctx, "alice", core.Path("/remote/folder/new"), strings.NewReader("second"), &length); core.CodeOf(err) != core.AlreadyExists || changed.Effect != core.Unchanged {
		t.Fatalf("exclusive write: %+v %v", changed, err)
	}
	if got, err := os.ReadFile(filepath.Join(root, "folder", "new")); err != nil || string(got) != "stream" {
		t.Fatalf("old content: %q %v", got, err)
	}
	if changed, err := c.Write(ctx, "alice", core.Path("/remote/folder/unknown"), strings.NewReader("unknown"), nil); err != nil || changed.Effect != core.Applied {
		t.Fatalf("unknown length: %+v %v", changed, err)
	}
	empty := int64(0)
	if changed, err := c.Write(ctx, "alice", core.Path("/remote/folder/empty"), strings.NewReader(""), &empty); err != nil || changed.Effect != core.Applied {
		t.Fatalf("empty input: %+v %v", changed, err)
	}
	if stat, err := os.Stat(filepath.Join(root, "folder", "empty")); err != nil || stat.Size() != 0 {
		t.Fatalf("empty target: %v %v", stat, err)
	}
	if page, err := c.List(ctx, "alice", core.ListRequest{Query: core.Children(core.Path("/remote/folder")), CursorTTL: time.Minute}); err != nil || len(page.Items) != 3 || canonicalizations.Load() == 0 {
		t.Fatalf("canonical collection listing: %+v redirects=%d err=%v", page, canonicalizations.Load(), err)
	}
	page, err := c.List(ctx, "alice", core.ListRequest{Query: core.Children(core.Path("/remote")), Limit: 1, CursorTTL: time.Minute})
	if err != nil || len(page.Items) != 1 || page.NextCursor == nil {
		t.Fatalf("first page: %+v %v", page, err)
	}
	for page.NextCursor != nil {
		token := *page.NextCursor
		request := core.ListRequest{Query: core.Children(core.Path("/remote")), Limit: 1, Cursor: token}
		page, err = c.List(ctx, "alice", request)
		if err != nil {
			t.Fatal(err)
		}
		before := requests.Load()
		if _, err = c.List(ctx, "alice", request); err != nil || requests.Load() != before {
			t.Fatalf("cursor replay fetched again: %v", err)
		}
	}
	output, err := local.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = c.Install("/output", output); err != nil {
		t.Fatal(err)
	}
	if changed, err := c.Copy(ctx, "alice", entry.Address, core.Path("/output/copied")); err != nil || changed.Effect != core.Applied {
		t.Fatalf("copy: %+v %v", changed, err)
	}
	before := requests.Load()
	if changed, err := d.RemoveSubtree(ctx, core.NativePath("/"), core.TreeOptions{}); core.CodeOf(err) != core.UnsupportedOperation ||
		changed.Effect != core.Unchanged || requests.Load() != before {
		t.Fatalf("protected mount root: %+v %v", changed, err)
	}
	treeLimits := core.TreeOptions{MaxEntries: 16, MaxPlanBytes: 64 << 10}
	folder := core.Path("/remote/folder")
	recursive := core.DirectoryRemoveOptions{Scope: core.Subtree}
	for _, status := range []int{http.StatusMultiStatus, http.StatusAccepted, http.StatusTemporaryRedirect} {
		deleteStatus.Store(int32(status))
		changed, err := c.RemoveTree(ctx, "alice", folder, treeLimits, recursive)
		if changed.Change.Effect != core.Unknown || err == nil || !strings.Contains(err.Error(), fmt.Sprintf("HTTP %d", status)) ||
			deleteStatus.Load() != 0 {
			t.Fatalf("unconfirmed recursive DELETE: %+v %v", changed, err)
		}
	}
	if changed, err := c.RemoveTree(ctx, "alice", folder, treeLimits, recursive); err != nil || changed.Change.Effect != core.Applied {
		t.Fatalf("recursive DELETE: %+v %v", changed, err)
	}
	if _, err := os.Stat(filepath.Join(root, "folder")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("native subtree still exists: %v", err)
	}
	before = requests.Load()
	if changed, err := c.Write(ctx, "alice", core.Path("/remote/publish"), strings.NewReader("body"), nil, core.WriteOptions{Policy: core.Publish()}); core.CodeOf(err) != core.UnsupportedOperation || changed.Effect != core.Unchanged {
		t.Fatalf("publish: %+v %v", changed, err)
	}
	if _, err := c.Open(ctx, "alice", entry.Address, core.OpenOptions{NoFollow: true}); core.CodeOf(err) != core.UnsupportedOperation || requests.Load() != before {
		t.Fatalf("unsupported guarantees performed I/O: %v", err)
	}
	if err := mount.Retire(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Stat(ctx, "alice", entry.Address); err == nil || requests.Load() != before {
		t.Fatalf("retired mount admitted request: %v", err)
	}
}

type transport func(*http.Request) (*http.Response, error)

func (f transport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type contextBody struct {
	context.Context
	*strings.Reader
}

func (b contextBody) Read(p []byte) (int, error) {
	if err := b.Context.Err(); err != nil {
		return 0, err
	}
	return b.Reader.Read(p)
}
func (contextBody) Close() error { return nil }

func property(href string, directory bool) string {
	kind := ""
	if directory {
		kind = "<d:collection/>"
	}
	return fmt.Sprintf(`<d:response><d:href>%s</d:href><d:propstat><d:prop><d:resourcetype>%s</d:resourcetype></d:prop><d:status>HTTP/1.1 200 OK</d:status></d:propstat></d:response>`, html.EscapeString(href), kind)
}

func response(r *http.Request, body string) *http.Response {
	body = `<d:multistatus xmlns:d="DAV:">` + body + `</d:multistatus>`
	return &http.Response{Request: r, StatusCode: http.StatusMultiStatus, Header: make(http.Header), ContentLength: int64(len(body)), Body: contextBody{r.Context(), strings.NewReader(body)}}
}

func TestNativePagingAndTrustBoundaries(t *testing.T) {
	const objectID = "4da9b287af01802e15c3145ecb24056d85673bed"
	const description = `<d:responsedescription xml:lang="zh">Page &amp; message</d:responsedescription>`
	for _, fault := range []string{"", "outside-href", "outside-next", "empty-next", "ambiguous-next", "bad-property", "missing-type", "case-href", "encoded-separator",
		"bad-xml", "redirect-cross", "redirect-rename", "description-order", "description-budget", "trailing-budget"} {
		t.Run(fault, func(t *testing.T) {
			base, escaped := "/dav/", "/dav/"
			if fault == "" {
				base, escaped = "/dav:root/", "/dav%3Aroot/"
			}
			firstFile := strings.Replace(property(base+"one", false), "</d:prop>", "<d:getetag>"+objectID+"</d:getetag></d:prop>", 1)
			if fault == "" {
				firstFile = strings.Replace(firstFile, "</d:response>", `<d:propstat><d:prop><d:getlastmodified/></d:prop><d:status>HTTP/1.1 403 Forbidden</d:status></d:propstat></d:response>`, 1)
			}
			pages := 0
			client := &http.Client{Transport: transport(func(r *http.Request) (*http.Response, error) {
				if user, password, ok := r.BasicAuth(); !ok || user != "alice" || password != "secret" || r.URL.Host != "dav.test" {
					t.Errorf("credential target: %s", r.URL)
				}
				if !strings.HasPrefix(r.URL.EscapedPath(), escaped) {
					t.Errorf("collection URI changed spelling: %s", r.URL)
				}
				if r.Header.Get("Depth") == "0" {
					if r.URL.Path == base+"one" {
						return response(r, firstFile+description), nil
					}
					return response(r, property(base, true)), nil
				}
				pages++
				var out *http.Response
				switch r.URL.Query().Get("page") {
				case "":
					body := firstFile + strings.Repeat(" ", 8192) + property(base, true)
					switch fault {
					case "description-order":
						body = description + body
					case "description-budget":
						body += "<d:responsedescription>" + strings.Repeat("x", 64<<10) + "</d:responsedescription>"
					default:
						body += description
					}
					out = response(r, body)
					out.Header.Set("Link", `<?page=2>; rel="next"`)
					if fault == "" {
						out.Header.Set("Link", `<?page=2>; flag; REL="Next"; rel=prev; rel*=UTF-8''prev; anchor*=UTF-8''%2Fother`)
					} else if fault == "outside-href" {
						out = response(r, property("/private/one", false))
					} else if fault == "outside-next" {
						out.Header.Set("Link", `<https://foreign.test/dav/>; rel="next"`)
					} else if fault == "empty-next" {
						out.Header.Set("Link", `<>; rel=next`)
					} else if fault == "ambiguous-next" {
						out.Header.Add("Link", `<?page=3>; rel=next`)
					} else if fault == "bad-property" || fault == "missing-type" {
						status := "HTTP/1.1 403 Forbidden"
						if fault == "missing-type" {
							status = "HTTP/1.1 404 Not Found"
						}
						out = response(r, strings.Replace(property(base+"one", false), "HTTP/1.1 200 OK", status, 1))
					} else if fault == "case-href" {
						out = response(r, property("/DAV/one", false))
					} else if fault == "encoded-separator" {
						out = response(r, property("/dav/part%2Fchild", false))
					} else if fault == "bad-xml" {
						out = response(r, `<d:response>`)
					} else if fault == "redirect-cross" || fault == "redirect-rename" {
						out.StatusCode = http.StatusMovedPermanently
						target := "https://foreign.test/dav/"
						if fault == "redirect-rename" {
							target = "/dav/renamed/"
						}
						out.Header.Set("Location", target)
					} else if fault == "trailing-budget" {
						body := `<d:multistatus xmlns:d="DAV:">` + property(base+"one", false) + property(base, true) + `</d:multistatus>` + strings.Repeat(" ", 64<<10)
						out.Body = contextBody{r.Context(), strings.NewReader(body)}
						out.ContentLength = int64(len(body))
						out.Header.Del("Link")
					}
				case "2":
					out = response(r, description)
					out.Header.Set("Link", `<?page=1>; rel=prev; title="Open \"report, <continued", <https://dav.test`+base+`?page=3>; rel="next"`)
				case "3":
					out = response(r, property(base+"space%20%252F", false))
					out.Header.Set("Link", `<?page=4>; rel=next; anchor="/other"`)
					out.Header.Add("Link", `<https://foreign.test/dav/>; rel*=UTF-8''next`)
				default:
					t.Errorf("unexpected page: %s", r.URL)
					out = response(r, "")
				}
				return out, nil
			})}
			d, err := driver.Open(context.Background(), driver.Options{RootURL: "https://dav.test" + escaped, Username: "alice", Password: "secret", Client: client, MaxResponseBytes: 64 << 10})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := d.Close(); err != nil {
					t.Error(err)
				}
			})
			iter, err := d.Enumerate(context.Background(), core.NativeChildren(core.NativePath("/")))
			if err != nil {
				t.Fatal(err)
			}
			defer iter.Close()
			ctx, cancel := context.WithCancel(context.Background())
			items, err := iter.Next(ctx, 1)
			cancel()
			if fault == "" {
				if len(items) != 1 || items[0].ModifiedAt != nil || items[0].Evidence.Validator != (core.Validator{}) {
					t.Fatalf("unavailable optional properties changed listing: %+v %v", items, err)
				}
				tag := core.Validator{Tag: fmt.Sprintf("%q", objectID)}
				if _, err := d.Stat(context.Background(), core.NativePath("/one"), core.ReadSelection{When: core.IfMatch(tag)}); core.CodeOf(err) != core.UnsupportedOperation {
					t.Fatalf("conditional stat accepted a raw DAV:getetag: %v", err)
				}
			}
			var names []string
			for _, item := range items {
				names = append(names, item.Name)
			}
			for attempts := 0; err == nil && attempts < 8; attempts++ {
				items, err = iter.Next(context.Background(), 1)
				for _, item := range items {
					names = append(names, item.Name)
				}
			}
			if fault != "" {
				if err == nil || errors.Is(err, io.EOF) || pages != 1 {
					t.Fatalf("fault accepted: pages=%d names=%v err=%v", pages, names, err)
				}
				switch fault {
				case "bad-property":
					if core.CodeOf(err) != core.UpstreamAccessDenied {
						t.Fatalf("required property denial lost its classification: %v", err)
					}
				case "missing-type":
					if core.CodeOf(err) != core.UnsupportedOperation {
						t.Fatalf("missing type was mistaken for an absent resource: %v", err)
					}
				case "empty-next", "ambiguous-next":
					if core.CodeOf(err) != core.InvalidUpstreamResponse || len(names) != 0 {
						t.Fatalf("invalid next link did not stop continuation: %v %v", names, err)
					}
				case "description-order":
					if core.CodeOf(err) != core.InvalidUpstreamResponse || len(names) != 0 {
						t.Fatalf("response accepted after its trailing description: %v %v", names, err)
					}
				case "description-budget", "trailing-budget":
					if core.CodeOf(err) != core.ResponseBudgetExceeded {
						t.Fatalf("trailing metadata escaped budget: %v", err)
					}
				}
			} else if !errors.Is(err, io.EOF) || strings.Join(names, "|") != "one|space %2F" || pages != 3 {
				t.Fatalf("paging/lifetime: pages=%d names=%v err=%v", pages, names, err)
			}
		})
	}
}

type waitingBody struct {
	context.Context
	started, closed chan struct{}
}

func (b waitingBody) Read([]byte) (int, error) {
	close(b.started)
	<-b.Done()
	return 0, b.Err()
}
func (b waitingBody) Close() error { close(b.closed); return nil }

func TestEnumerationCancellationClosesResponse(t *testing.T) {
	started, closed := make(chan struct{}), make(chan struct{})
	client := &http.Client{Transport: transport(func(r *http.Request) (*http.Response, error) {
		if r.Header.Get("Depth") == "0" {
			return response(r, property("/dav/", true)), nil
		}
		out := response(r, "")
		out.ContentLength = -1
		out.Body = waitingBody{r.Context(), started, closed}
		return out, nil
	})}
	d, err := driver.Open(context.Background(), driver.Options{RootURL: "https://dav.test/dav/", Client: client})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	iter, err := d.Enumerate(context.Background(), core.NativeChildren(core.NativePath("/")))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() { _, err := iter.Next(ctx, 1); result <- err }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("enumeration did not start")
	}
	cancel()
	select {
	case err = <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancellation lost: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("enumeration did not stop")
	}
	if err := iter.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-closed:
	default:
		t.Fatal("canceled response retained")
	}
}

type closeFuncBody struct {
	io.Reader
	close func() error
}

func (b closeFuncBody) Close() error { return b.close() }

type readFunc func([]byte) (int, error)

func (f readFunc) Read(p []byte) (int, error) { return f(p) }

func TestFailedCleanupAndWriteAdmission(t *testing.T) {
	ctx := context.Background()
	fault := errors.New("injected persistent response close failure")
	failing, failOnce := true, false
	closes, requests := 0, 0
	client := &http.Client{Transport: transport(func(r *http.Request) (*http.Response, error) {
		requests++
		if r.Method == "PROPFIND" {
			out := response(r, property("/dav/", true))
			if failing {
				inner := out.Body
				persistent, failure := !failOnce, fault
				out.Body = closeFuncBody{inner, func() error {
					closes++
					if failure != nil && (!persistent || failing) {
						err := failure
						if !persistent {
							failure = nil
						}
						return err
					}
					return inner.Close()
				}}
			}
			return out, nil
		}
		if r.Method != http.MethodPut {
			t.Errorf("unexpected mutation: %s", r.Method)
		}
		if r.Body != nil {
			_, _ = io.Copy(io.Discard, r.Body)
			_ = r.Body.Close()
		}
		return &http.Response{Request: r, StatusCode: http.StatusRequestEntityTooLarge, Header: make(http.Header), Body: http.NoBody}, nil
	})}
	options := driver.Options{RootURL: "https://dav.test/dav/", Client: client}
	d, err := driver.Open(ctx, options)
	if d == nil || !errors.Is(err, fault) {
		t.Fatalf("failed constructor lost cleanup: %v %v", d, err)
	}
	before := closes
	if err := d.Close(); !errors.Is(err, fault) || closes <= before {
		t.Fatalf("failed close not retried: %d %v", closes, err)
	}
	failing, before = false, closes
	if err := d.Close(); err != nil || closes <= before || requests != 1 {
		t.Fatalf("same response not recovered: closes=%d requests=%d err=%v", closes, requests, err)
	}
	reads, zero := 0, int64(0)
	input := readFunc(func([]byte) (int, error) { reads++; return 0, io.EOF })
	if changed, err := d.Create(ctx, core.NativePath("/new"), input, &zero); err == nil || changed.Effect != core.Unchanged || reads != 0 || requests != 1 {
		t.Fatalf("closed write borrowed input: %+v reads=%d requests=%d err=%v", changed, reads, requests, err)
	}
	if changed, err := d.Mkdir(ctx, core.NativePath("/new")); err == nil || changed.Effect != core.Unchanged || requests != 1 {
		t.Fatalf("closed mkdir: %+v %v", changed, err)
	}
	d, err = driver.Open(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	c := files(t)
	if _, err := c.Install("/remote", d); err != nil {
		t.Fatal(err)
	}
	cause := fault
	fault = fmt.Errorf("wrapped response failure: %w", errors.Join(io.EOF, cause))
	failing, failOnce = true, true
	_, err = c.List(ctx, "alice", core.ListRequest{Query: core.Children(core.Path("/remote")), CursorTTL: time.Minute})
	failing = false
	if !errors.Is(err, io.ErrUnexpectedEOF) || !errors.Is(err, cause) || errors.Is(err, io.EOF) {
		t.Fatalf("failed close lost its cause or looked like normal directory EOF: %v", err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	before = requests
	if changed, err := d.Mkdir(canceled, core.NativePath("/new")); !errors.Is(err, context.Canceled) || changed.Effect != core.Unchanged || requests != before {
		t.Fatalf("canceled mkdir: %+v %v", changed, err)
	}
	length := int64(4)
	if changed, err := d.Create(ctx, core.NativePath("/new"), strings.NewReader("body"), &length); err == nil || changed.Effect != core.Unknown {
		t.Fatalf("413 invented unchanged effect: %+v %v", changed, err)
	}
}
