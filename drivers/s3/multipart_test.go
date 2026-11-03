package s3_test

import (
	"bytes"
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	core "github.com/nostalume/mountweave"
	driver "github.com/nostalume/mountweave/drivers/s3"
	"github.com/nostalume/mountweave/local"
)

type objectProtocol struct {
	mu                                sync.Mutex
	objects                           map[string][]byte
	uploads                           map[string]map[int][]byte
	calls                             map[string]int
	next                              int
	failPart, failAbort, lostComplete bool
	hold, started                     chan struct{}
	held                              sync.Once
	retryFirst                        bool
	retryData                         []byte
	second                            chan struct{}
	rejectNext                        bool
	rejectedBody                      io.Reader
	deleteErrorKey                    string
	addDuringDelete                   bool
	deleteBatches                     int
	metadata                          map[string]string
	uploadMetadata                    map[string]string
	failPartNumber                    int
	lostAbort                         bool
	lostInitiate                      bool
}

func newProtocol() *objectProtocol {
	return &objectProtocol{objects: make(map[string][]byte), uploads: make(map[string]map[int][]byte), calls: make(map[string]int), metadata: make(map[string]string), uploadMetadata: make(map[string]string)}
}
func (s *objectProtocol) roundTrip(r *http.Request) (*http.Response, error) {
	if r.URL.Host != "s3.example.test" {
		return nil, errors.New("outside closed protocol")
	}
	input := r.Body
	if input == nil {
		input = http.NoBody
	}
	key := strings.TrimPrefix(r.URL.Path, "/bucket/")
	q := r.URL.Query()
	id := q.Get("uploadId")
	s.mu.Lock()
	s.calls[r.Method+" "+q.Get("partNumber")+" "+q.Get("uploadId")]++
	hold, started := s.hold, s.started
	s.mu.Unlock()
	if r.Method == "PUT" && id != "" && q.Get("partNumber") == "1" && hold != nil {
		s.held.Do(func() { close(started) })
		<-hold
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, present := q["delete"]; present && r.Method == "POST" {
		var request struct {
			Objects []struct {
				Key string `xml:"Key"`
			} `xml:"Object"`
		}
		if err := xml.NewDecoder(input).Decode(&request); err != nil {
			return nil, err
		}
		if len(request.Objects) == 0 || len(request.Objects) > 1000 {
			return nil, errors.New("invalid native delete batch")
		}
		s.deleteBatches++
		var receipts strings.Builder
		for _, item := range request.Objects {
			if item.Key == s.deleteErrorKey {
				fmt.Fprintf(&receipts, "<Error><Key>%s</Key><Code>AccessDenied</Code></Error>", item.Key)
			} else {
				delete(s.objects, item.Key)
				fmt.Fprintf(&receipts, "<Deleted><Key>%s</Key></Deleted>", item.Key)
			}
		}
		if s.addDuringDelete {
			s.objects["vault/folder/concurrent"] = []byte("new")
			s.addDuringDelete = false
		}
		return response(r, 200, "<DeleteResult>"+receipts.String()+"</DeleteResult>"), nil
	}
	missing := func() (*http.Response, error) {
		return response(r, 404, "<Error><Code>NoSuchUpload</Code></Error>"), nil
	}
	if q.Get("list-type") == "2" {
		prefix, delimiter := q.Get("prefix"), q.Get("delimiter")
		encode := func(key string) string {
			if q.Get("encoding-type") == "url" {
				return url.PathEscape(key)
			}
			return key
		}
		var items strings.Builder
		seen := make(map[string]bool)
		for k, b := range s.objects {
			if !strings.HasPrefix(k, prefix) {
				continue
			}
			rest := strings.TrimPrefix(k, prefix)
			if i := strings.Index(rest, "/"); delimiter == "/" && i >= 0 {
				p := prefix + rest[:i+1]
				if seen[p] {
					continue
				}
				seen[p] = true
				fmt.Fprintf(&items, "<CommonPrefixes><Prefix>%s</Prefix></CommonPrefixes>", encode(p))
			} else {
				fmt.Fprintf(&items, "<Contents><Key>%s</Key><Size>%d</Size></Contents>", encode(k), len(b))
			}
		}
		encoding := ""
		if q.Get("encoding-type") == "url" {
			encoding = "<EncodingType>url</EncodingType>"
		}
		return response(r, 200, "<ListBucketResult><IsTruncated>false</IsTruncated>"+encoding+items.String()+"</ListBucketResult>"), nil
	}
	if _, present := q["uploads"]; present && r.Method == "POST" {
		s.next++
		id = fmt.Sprint("u", s.next)
		s.uploads[id] = make(map[int][]byte)
		s.uploadMetadata[id] = r.Header.Get("X-Amz-Meta-Mountweave-Upload")
		if s.lostInitiate {
			s.lostInitiate = false
			return nil, errors.New("initiation response lost")
		}
		return response(r, 200, fmt.Sprintf("<InitiateMultipartUploadResult><Bucket>bucket</Bucket><Key>%s</Key><UploadId>%s</UploadId></InitiateMultipartUploadResult>", key, id)), nil
	}
	if id != "" {
		parts, exists := s.uploads[id]
		if !exists {
			return missing()
		}
		switch r.Method {
		case "PUT":
			if s.failPart || q.Get("partNumber") == strconv.Itoa(s.failPartNumber) {
				return response(r, 503, "<Error><Code>SlowDown</Code></Error>"), nil
			}
			data, err := io.ReadAll(input)
			if err != nil {
				return nil, err
			}
			n, _ := strconv.Atoi(q.Get("partNumber"))
			if n == 1 && s.retryFirst {
				s.retryFirst = false
				s.retryData = bytes.Clone(data)
				return response(r, 503, "<Error><Code>SlowDown</Code></Error>"), nil
			}
			if n == 1 && s.retryData != nil {
				if !bytes.Equal(data, s.retryData) {
					return nil, errors.New("part retry changed retained bytes")
				}
				s.retryData = nil
			}
			parts[n] = data
			if n == 2 && s.second != nil {
				close(s.second)
				s.second = nil
			}
			out := response(r, 200, "")
			out.Header.Set("ETag", fmt.Sprintf("\"p%d\"", n))
			return out, nil
		case "POST":
			if r.Header.Get("If-None-Match") == "*" {
				if _, exists := s.objects[key]; exists {
					return response(r, 412, "<Error><Code>PreconditionFailed</Code></Error>"), nil
				}
			}
			var request struct {
				Parts []struct {
					Number int    `xml:"PartNumber"`
					ETag   string `xml:"ETag"`
				} `xml:"Part"`
			}
			if err := xml.NewDecoder(r.Body).Decode(&request); err != nil {
				return nil, err
			}
			var data []byte
			for i, p := range request.Parts {
				if p.Number != i+1 || p.ETag != fmt.Sprintf("\"p%d\"", p.Number) {
					return nil, errors.New("unordered or invalid native receipts")
				}
				data = append(data, parts[p.Number]...)
			}
			s.objects[key] = data
			s.metadata[key] = s.uploadMetadata[id]
			delete(s.uploads, id)
			if s.lostComplete {
				return nil, errors.New("completion response lost")
			}
			return response(r, 200, fmt.Sprintf("<CompleteMultipartUploadResult><Bucket>bucket</Bucket><Key>%s</Key><ETag>\"complete\"</ETag></CompleteMultipartUploadResult>", key)), nil
		case "DELETE":
			if s.failAbort {
				return response(r, 403, "<Error><Code>AccessDenied</Code></Error>"), nil
			}
			delete(s.uploads, id)
			if s.lostAbort {
				s.lostAbort = false
				return nil, errors.New("abort response lost")
			}
			return response(r, 204, ""), nil
		case "GET":
			return response(r, 200, "<ListPartsResult><IsTruncated>false</IsTruncated></ListPartsResult>"), nil
		}
	}
	switch r.Method {
	case "PUT":
		if s.rejectNext {
			s.rejectNext, s.rejectedBody = false, r.Body
			return response(r, 403, "<Error><Code>AccessDenied</Code></Error>"), nil
		}
		if _, exists := s.objects[key]; exists && r.Header.Get("If-None-Match") == "*" {
			return response(r, 412, "<Error><Code>PreconditionFailed</Code></Error>"), nil
		}
		data, err := io.ReadAll(input)
		if err != nil {
			return nil, err
		}
		s.objects[key] = data
		return response(r, 200, ""), nil
	case "HEAD", "GET":
		data, exists := s.objects[key]
		if !exists {
			return response(r, 404, "<Error><Code>NoSuchKey</Code></Error>"), nil
		}
		if r.Method == "HEAD" {
			out := response(r, 200, "")
			out.ContentLength = int64(len(data))
			out.Header.Set("Content-Length", strconv.Itoa(len(data)))
			out.Header.Set("ETag", "\"complete\"")
			if s.metadata[key] != "" {
				out.Header.Set("X-Amz-Meta-Mountweave-Upload", s.metadata[key])
			}
			return out, nil
		}
		if r.Header.Get("Range") != "" {
			var start, end int
			fmt.Sscanf(r.Header.Get("Range"), "bytes=%d-%d", &start, &end)
			out := response(r, 206, string(data[start:end+1]))
			out.Header.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(data)))
			out.Header.Set("Content-Length", strconv.Itoa(end-start+1))
			return out, nil
		}
		out := response(r, 200, string(data))
		out.Header.Set("Content-Length", strconv.Itoa(len(data)))
		return out, nil
	case "DELETE":
		delete(s.objects, key)
		return response(r, 204, ""), nil
	}
	return nil, fmt.Errorf("unexpected native operation %s %s", r.Method, r.URL)
}
func readFile(t *testing.T, c *core.Core, p string, wanted *core.Range) []byte {
	t.Helper()
	opened, err := c.Open(context.Background(), "writer", p, wanted)
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(opened.Body)
	err = errors.Join(err, opened.Body.Close())
	if err != nil {
		t.Fatal(err)
	}
	return data
}

type failedEOF struct {
	io.Reader
	err error
}

func (r failedEOF) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	if err == io.EOF {
		return n, r.err
	}
	return n, err
}

func TestMultipartFileWorkflowAndRecoveryOwnership(t *testing.T) {
	ctx := context.Background()
	server := newProtocol()
	server.second = make(chan struct{})
	server.hold, server.started, server.retryFirst = server.second, make(chan struct{}), true
	localRoot := t.TempDir()
	opts := driver.Options{PartSize: 5 << 20, MaxBufferedBytes: 10 << 20, PartConcurrency: 2, PartAttempts: 2, CleanupTimeout: time.Second}
	d, c := preparedDriver(t, server.roundTrip, []driver.Options{opts}, func(o *s3.Options) { o.RetryMaxAttempts = 3 })
	if _, err := c.Mkdir(ctx, "writer", "/cloud/folder"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Mkdir(ctx, "writer", "/cloud/folder"); core.CodeOf(err) != core.AlreadyExists {
		t.Fatal(err)
	}
	payload := bytes.Repeat([]byte("x"), (5<<20)+13)
	if change, err := c.Write(ctx, "writer", "/cloud/folder/file", bytes.NewReader(payload), nil); err != nil || change.Effect != core.Applied {
		t.Fatal(change, err)
	}
	if server.calls["PUT 1 u1"] != 2 || server.calls["PUT 2 u1"] != 1 {
		t.Fatal("retained part retry or out-of-order completion missing", server.calls)
	}
	if got := readFile(t, c, "/cloud/folder/file", &core.Range{Start: (5 << 20) - 3, End: (5 << 20) + 4}); !bytes.Equal(got, payload[(5<<20)-3:(5<<20)+5]) {
		t.Fatal("cross-part range")
	}
	if change, err := c.Write(ctx, "writer", "/cloud/folder/file", bytes.NewReader(payload), nil); core.CodeOf(err) != core.AlreadyExists || change.Effect != core.Unchanged {
		t.Fatal(change, err)
	}
	fault := errors.New("input failed after a complete part")
	if change, err := c.Write(ctx, "writer", "/cloud/folder/file", failedEOF{bytes.NewReader(payload), fault}, nil, core.WriteOptions{Replace: true}); !errors.Is(err, fault) || change.Effect != core.Unchanged {
		t.Fatal(change, err)
	}
	if got := readFile(t, c, "/cloud/folder/file", nil); !bytes.Equal(got, payload) {
		t.Fatal("failed replacement changed old content")
	}
	n := int64(3)
	if _, err := c.Write(ctx, "writer", "/cloud/folder/file", strings.NewReader("new"), &n, core.WriteOptions{Replace: true}); err != nil {
		t.Fatal(err)
	}
	if got := readFile(t, c, "/cloud/folder/file", nil); string(got) != "new" {
		t.Fatal(string(got))
	}
	localDriver, err := local.New(localRoot)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Install("/local", localDriver); err != nil {
		t.Fatal(err)
	}
	rows, err := c.CopyTree(ctx, "writer", "/cloud/folder", "/local/copied", core.TreeCopyOptions{MaxEntries: 10, MaxPlanBytes: 1024})
	if err != nil || len(rows) != 2 {
		t.Fatal(rows, err)
	}
	if _, err := c.Copy(ctx, "writer", "/local/copied/file", "/cloud/folder/copied"); err != nil {
		t.Fatal(err)
	}
	server.mu.Lock()
	server.lostComplete = true
	before := server.next
	server.mu.Unlock()
	if change, err := c.Write(ctx, "writer", "/cloud/folder/lost", bytes.NewReader(payload), nil); err == nil || change.Effect != core.Unknown {
		t.Fatal(change, err)
	}
	server.mu.Lock()
	server.lostComplete = false
	if server.next != before+1 || server.calls["POST  "+fmt.Sprint("u", server.next)] != 1 {
		t.Fatal("completion replayed")
	}
	server.failPart, server.failAbort = true, true
	server.mu.Unlock()
	if change, err := c.Write(ctx, "writer", "/cloud/folder/failed", bytes.NewReader(payload), nil); err == nil || change.Effect != core.Unchanged {
		t.Fatal(change, err)
	}
	if len(d.PendingUploads()) != 1 {
		t.Fatal("unconfirmed cleanup lost its owner")
	}
	untouched := strings.NewReader("z")
	one := int64(1)
	if change, err := c.Write(ctx, "writer", "/cloud/folder/blocked", untouched, &one); err == nil || change.Effect != core.Unchanged || untouched.Len() != 1 {
		t.Fatal(change, err)
	}
	server.mu.Lock()
	server.failAbort, server.failPart = false, false
	server.mu.Unlock()
	if err := d.Cleanup(ctx); err != nil || len(d.PendingUploads()) != 0 {
		t.Fatal(err)
	}
	server.rejectNext = true
	if _, err := c.Write(ctx, "writer", "/cloud/folder/rejected", strings.NewReader("x"), &one); err == nil {
		t.Fatal("native early rejection ignored")
	}
	if n, _ := server.rejectedBody.Read(make([]byte, 1)); n != 0 {
		t.Fatal("late transport read retained a released payload buffer")
	}
	if _, err := c.Remove(ctx, "writer", "/cloud/folder/file"); err != nil {
		t.Fatal(err)
	}
	if change, err := c.Remove(ctx, "writer", "/cloud/folder"); core.CodeOf(err) != core.UnsupportedOperation || change.Effect != core.Unchanged {
		t.Fatal(change, err)
	}
	if _, err := c.RemoveTree(ctx, "writer", "/cloud/folder", core.TreeOptions{MaxEntries: 10, MaxPlanBytes: 1024}); core.CodeOf(err) != core.UnsupportedOperation {
		t.Fatal(err)
	}
	if got := readFile(t, c, "/cloud/folder/copied", nil); string(got) != "new" {
		t.Fatal("unsupported tree changed children")
	}
	policy := core.DirectoryRemoveOptions{Scope: core.Subtree}
	limits := core.TreeOptions{MaxEntries: 10, MaxPlanBytes: 1024}
	if rows, err := c.RemoveTree(ctx, "writer", "/cloud/folder", core.TreeOptions{MaxEntries: 1, MaxPlanBytes: 1024}, policy); core.CodeOf(err) != core.CapacityExceeded || len(rows) != 1 || rows[0].Change.Effect != core.Unchanged || server.deleteBatches != 0 {
		t.Fatal(rows, err)
	}
	server.deleteErrorKey = "vault/folder/copied"
	if rows, err := c.RemoveTree(ctx, "writer", "/cloud/folder", limits, policy); core.CodeOf(err) != core.UpstreamAccessDenied || len(rows) != 1 || rows[0].Change.Effect != core.Incomplete {
		t.Fatal(rows, err)
	}
	if got := readFile(t, c, "/cloud/folder/copied", nil); string(got) != "new" {
		t.Fatal(string(got))
	}
	server.deleteErrorKey, server.addDuringDelete = "", true
	if rows, err := c.RemoveTree(ctx, "writer", "/cloud/folder", limits, policy); core.CodeOf(err) != core.DirectoryNotEmpty || len(rows) != 1 || rows[0].Change.Effect != core.Incomplete {
		t.Fatal(rows, err)
	}
	if got := readFile(t, c, "/cloud/folder/concurrent", nil); string(got) != "new" {
		t.Fatal(string(got))
	}
	if rows, err := c.RemoveTree(ctx, "writer", "/cloud/folder", limits, policy); err != nil || len(rows) != 1 || rows[0].Change.Effect != core.Applied {
		t.Fatal(rows, err)
	}
	for i := 0; i < 1001; i++ {
		server.objects[fmt.Sprintf("vault/bulk/%04d", i)] = []byte("x")
	}
	beforeBatches := server.deleteBatches
	if _, err := c.RemoveTree(ctx, "writer", "/cloud/bulk", core.TreeOptions{MaxEntries: 1010, MaxPlanBytes: 10000}, policy); err != nil || server.deleteBatches != beforeBatches+2 {
		t.Fatal(server.deleteBatches, err)
	}
}

func TestMultipartCancellationAndSharedBufferAdmission(t *testing.T) {
	ctx := context.Background()
	server := newProtocol()
	server.hold, server.started = make(chan struct{}), make(chan struct{})
	release := sync.OnceFunc(func() { close(server.hold) })
	t.Cleanup(release)
	d, c := preparedDriver(t, server.roundTrip, []driver.Options{{PartSize: 5 << 20, MaxBufferedBytes: 10 << 20, PartConcurrency: 2, CleanupTimeout: time.Second}})
	callCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		change, err := c.Write(callCtx, "writer", "/cloud/held", bytes.NewReader(bytes.Repeat([]byte("a"), (5<<20)+1)), nil)
		if change.Effect != core.Unchanged {
			err = errors.Join(err, errors.New("cancelled multipart published"))
		}
		done <- err
	}()
	select {
	case <-server.started:
	case <-time.After(5 * time.Second):
		t.Fatal("native part did not start")
	}
	cancel()
	select {
	case err := <-done:
		t.Fatal("native request still owns buffer", err)
	default:
	}
	untouched := strings.NewReader("x")
	n := int64(1)
	if change, err := c.Write(ctx, "writer", "/cloud/other", untouched, &n); core.CodeOf(err) != core.CapacityExceeded || change.Effect != core.Unchanged || untouched.Len() != 1 {
		t.Fatal(change, err)
	}
	release()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if len(d.PendingUploads()) != 0 {
		t.Fatal("acknowledged cleanup retained a private upload")
	}
	if _, err := c.Write(ctx, "writer", "/cloud/other", strings.NewReader("x"), &n); err != nil {
		t.Fatal(err)
	}
}
