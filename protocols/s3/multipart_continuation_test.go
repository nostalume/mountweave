package s3

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	bolt "go.etcd.io/bbolt"
)

func TestSDKMultipartContinuation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	const version = "native-proof"
	objects := &objectFixture{objects: map[string]string{}}
	type session struct {
		token string
		parts map[int]string
	}
	sessions := map[string]*session{}
	proofs := map[string]string{}
	calls := map[string]int{}
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := strings.TrimPrefix(r.URL.Path, "/upstream/vault/")
		q := r.URL.Query()
		if r.Method == http.MethodGet && !q.Has("uploadId") {
			objects.ServeHTTP(w, r)
			return
		}
		objects.mu.Lock()
		defer objects.mu.Unlock()
		fail := func(status int, code string) {
			w.WriteHeader(status)
			fmt.Fprintf(w, "<Error><Code>%s</Code></Error>", code)
		}
		switch {
		case r.Method == http.MethodPost && q.Has("uploads"):
			calls[key+" create"]++
			id := "native-" + key
			sessions[id] = &session{token: r.Header.Get("X-Amz-Meta-Mountweave-Upload"), parts: map[int]string{}}
			fmt.Fprintf(w, "<InitiateMultipartUploadResult><Bucket>upstream</Bucket><Key>%s</Key><UploadId>%s</UploadId></InitiateMultipartUploadResult>", key, id)
		case r.Method == http.MethodHead:
			calls[key+" head"]++
			data, ok := objects.objects[key]
			if !ok {
				fail(404, "NoSuchKey")
				return
			}
			w.Header().Set("Content-Length", strconv.Itoa(len(data)))
			w.Header().Set("ETag", tag(data))
			w.Header().Set("X-Amz-Version-Id", version)
			if token := proofs[key]; token != "" {
				w.Header().Set("X-Amz-Meta-Mountweave-Upload", token)
			}
			w.WriteHeader(200)
		default:
			id := q.Get("uploadId")
			s := sessions[id]
			if s == nil {
				fail(404, "NoSuchUpload")
				return
			}
			switch r.Method {
			case http.MethodPut:
				number, _ := strconv.Atoi(q.Get("partNumber"))
				event := key + " part " + strconv.Itoa(number)
				calls[event]++
				if (key == "resume" && number == 2 && calls[event] == 1) || key == "abort" {
					fail(503, "SlowDown")
					return
				}
				data, err := io.ReadAll(r.Body)
				if err != nil {
					fail(400, "InvalidRequest")
					return
				}
				s.parts[number] = string(data)
				w.Header().Set("ETag", tag(string(data)))
			case http.MethodPost:
				calls[key+" complete"]++
				var assembled strings.Builder
				for i := 1; i <= len(s.parts); i++ {
					data, ok := s.parts[i]
					if !ok {
						fail(400, "InvalidPart")
						return
					}
					assembled.WriteString(data)
				}
				objects.objects[key] = assembled.String()
				proofs[key] = s.token
				delete(sessions, id)
				// The native completion took effect but its receipt was lost.
				fail(500, "InternalError")
			case http.MethodDelete:
				calls[key+" abort"]++
				if calls[key+" abort"] > 1 {
					delete(sessions, id)
				}
				fail(500, "InternalError")
			case http.MethodGet:
				calls[key+" list-parts"]++
				fmt.Fprint(w, "<ListPartsResult><IsTruncated>false</IsTruncated></ListPartsResult>")
			}
		}
	}))
	defer upstream.Close()
	dir := t.TempDir()
	open := func() *multipartFixture { return openMultipartFixture(t, dir, upstream.URL, 64<<10) }
	f := open()
	stage := func(key string, parts ...string) multipartTestUpload {
		t.Helper()
		u := f.create(t, ctx, key)
		for i, data := range parts {
			p, err := u.upload(ctx, f.client, int32(i+1), data)
			if err != nil {
				t.Fatal(err)
			}
			u.Parts = append(u.Parts, p)
		}
		return u
	}
	retained := func(n int) {
		t.Helper()
		parts, err := f.inputs.List(ctx)
		if err != nil || len(parts) != n {
			t.Fatalf("retained inputs %d, want %d: %v", len(parts), n, err)
		}
	}
	first, second := strings.Repeat("a", 5<<20), strings.Repeat("b", 5<<20)
	resume := stage("resume", first, second)
	_, err := resume.complete(ctx, f.client, "")
	statusIs(t, err, 503)
	retained(2)
	// Simulate a paused upload saved by the previous manifest layout.
	if err := f.db.Update(func(tx *bolt.Tx) error {
		b, rec, err := readUpload(tx, resume.ID)
		if err != nil {
			return err
		}
		rec.LegacyFrozen = new(completeRequest)
		if err := json.Unmarshal(b.Get(uploadFrozen), rec.LegacyFrozen); err != nil {
			return err
		}
		if err := b.Delete(uploadFrozen); err != nil {
			return err
		}
		return putRecord(b, uploadMeta, rec)
	}); err != nil {
		t.Fatal(err)
	}
	f.close(t)
	f = open()
	_, err = resume.complete(ctx, f.client, tag("changed predicate"))
	statusIs(t, err, 409)
	_, err = resume.complete(ctx, f.client, "")
	statusIs(t, err, 500)
	retained(2)
	f.close(t)
	f = open()
	receipt, err := resume.complete(ctx, f.client, "")
	if err != nil || aws.ToString(receipt.ETag) != tag(first+second) || aws.ToString(receipt.VersionId) != version {
		t.Fatal(receipt, err)
	}
	retained(0)
	objects.mu.Lock()
	published := objects.objects["resume"] == first+second
	resumed := calls["resume create"] == 1 && calls["resume part 1"] == 1 && calls["resume part 2"] == 2 && calls["resume complete"] == 1 && calls["resume head"] == 1
	objects.mu.Unlock()
	if !published || !resumed {
		t.Fatal("range assembly or native checkpoint replay differed")
	}

	abort := stage("abort", "private")
	_, err = abort.complete(ctx, f.client, "")
	statusIs(t, err, 503)
	statusIs(t, abort.abort(ctx, f.client), 500)
	retained(1)
	_, err = abort.complete(ctx, f.client, "")
	statusIs(t, err, 409)
	if err := abort.abort(ctx, f.client); err != nil {
		t.Fatal(err)
	}
	retained(0)

	unproved := stage("unproved", "original")
	_, err = unproved.complete(ctx, f.client, "")
	statusIs(t, err, 500)
	objects.mu.Lock()
	objects.objects["unproved"] = "external"
	delete(proofs, "unproved")
	objects.mu.Unlock()
	_, err = unproved.complete(ctx, f.client, "")
	statusIs(t, err, 500)
	statusIs(t, unproved.abort(ctx, f.client), 500)
	retained(1)
	objects.mu.Lock()
	safe := calls["unproved complete"] == 1 && calls["unproved abort"] == 0 && objects.objects["unproved"] == "external"
	objects.mu.Unlock()
	if !safe {
		t.Fatal("unknown publication was replayed or removed")
	}
}
