package s3

import (
	"bytes"
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	core "github.com/nostalume/mountweave"
	driver "github.com/nostalume/mountweave/drivers/s3"
	"github.com/nostalume/mountweave/protocols"
)

func TestSigV4Example(t *testing.T) {
	secret := "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
	h := &Handler{options: Options{
		Region: "us-east-1",
		LookupIdentity: func(_ context.Context, id string) (Identity, error) {
			if id != "AKIAIOSFODNN7EXAMPLE" {
				return Identity{}, errors.New("unknown key")
			}
			return Identity{Principal: "example", SecretAccessKey: secret}, nil
		},
	}}
	r := httptest.NewRequest("GET", "https://examplebucket.s3.amazonaws.com/test.txt", nil)
	r.Header.Set("Range", "bytes=0-9")
	r.Header.Set("X-Amz-Date", "20130524T000000Z")
	r.Header.Set("X-Amz-Content-Sha256", emptyPayload)
	// Published AWS S3 GET Object example.
	r.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request,SignedHeaders=host;range;x-amz-content-sha256;x-amz-date,Signature=f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41")
	now := time.Date(2013, 5, 24, 0, 0, 0, 0, time.UTC)
	if actor, _, err := h.authenticate(r, r.URL, now); err != nil || actor != "example" {
		t.Fatal(actor, err)
	}
	if _, _, err := h.authenticate(r, r.URL, now.Add(time.Hour)); err == nil {
		t.Fatal("stale signature accepted")
	}
	secret = "rotated-fixture-secret"
	if _, _, err := h.authenticate(r, r.URL, now); err == nil {
		t.Fatal("signature made with the retired secret was accepted")
	}
}

type transportFunc func(*http.Request) (*http.Response, error)

func (f transportFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type objectFixture struct {
	mu      sync.Mutex
	objects map[string]string
	trace   []string
}

func (f *objectFixture) record(event string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.trace = append(f.trace, event)
}
func (f *objectFixture) requireTrace(t *testing.T, want ...string) {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	if strings.Join(f.trace, "\n") != strings.Join(want, "\n") {
		t.Fatalf("trace = %q, want %q", f.trace, want)
	}
	f.trace = nil
}

func (f *objectFixture) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/upstream" && r.URL.Query().Get("list-type") == "2" {
		f.list(w, r)
		return
	}
	key, ok := strings.CutPrefix(r.URL.Path, "/upstream/vault/")
	if !ok {
		http.Error(w, "wrong native bucket or prefix", 400)
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.trace = append(f.trace, "native "+r.Method+" "+key)
	data, exists := f.objects[key]
	if r.Method == http.MethodDelete {
		delete(f.objects, key)
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if r.Method == http.MethodPut {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "read failed", 400)
			return
		}
		if (r.Header.Get("If-None-Match") == "*" && exists) ||
			(r.Header.Get("If-Match") != "" && (!exists || r.Header.Get("If-Match") != tag(data))) {
			w.WriteHeader(412)
			io.WriteString(w, "<Error><Code>PreconditionFailed</Code></Error>")
			return
		}
		f.objects[key] = string(body)
		if key == "uncertain" {
			w.WriteHeader(500)
			io.WriteString(w, "<Error><Code>InternalError</Code></Error>")
			return
		}
		w.Header().Set("ETag", tag(string(body)))
		return
	}
	if !exists {
		w.WriteHeader(404)
		if r.Method != "HEAD" {
			io.WriteString(w, "<Error><Code>NoSuchKey</Code></Error>")
		}
		return
	}
	w.Header().Set("ETag", tag(data))
	http.ServeContent(w, r, key, time.Time{}, bytes.NewReader([]byte(data)))
}

func sdkClient(server *httptest.Server, actor string, alter func(*http.Request)) *sdk.Client {
	return sdkAt(server.URL, server.Client(), actor, alter)
}

func sdkAt(endpoint string, client *http.Client, actor string, alter func(*http.Request)) *sdk.Client {
	copyClient := *client
	if alter != nil {
		copyClient.Transport = transportFunc(func(r *http.Request) (*http.Response, error) {
			r = r.Clone(r.Context())
			alter(r)
			return client.Transport.RoundTrip(r)
		})
	}
	return sdk.New(sdk.Options{
		BaseEndpoint: aws.String(endpoint), UsePathStyle: true, Region: "us-east-1",
		Credentials: aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
			return aws.Credentials{AccessKeyID: actor, SecretAccessKey: "fixture-only-" + actor}, nil
		}),
		HTTPClient: &copyClient, RetryMaxAttempts: 1,
		RequestChecksumCalculation: aws.RequestChecksumCalculationWhenSupported,
	})
}

func statusIs(t *testing.T, err error, status int) {
	t.Helper()
	var httpErr interface{ HTTPStatusCode() int }
	if !errors.As(err, &httpErr) || httpErr.HTTPStatusCode() != status {
		t.Fatalf("expected HTTP %d, got %v", status, err)
	}
}

func TestSDKObjectWorkflow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	keys := []string{"a", "a/b", "a//b", "a/../b", "/a", "a%2Fb +汉?"}
	f := &objectFixture{objects: map[string]string{}}
	for i, key := range keys {
		f.objects[key] = fmt.Sprintf("%d:%s", i, key)
	}
	upstream := httptest.NewServer(f)
	defer upstream.Close()
	d, err := driver.New(sdkClient(upstream, "upstream", nil), "upstream", "vault/", driver.Options{PartSize: 5 << 20, MaxBufferedBytes: 5 << 20})
	if err != nil {
		t.Fatal(err)
	}
	admitted := make(chan struct{}, 1)
	files, err := core.New(core.Options{MaxCursors: 1, Authorize: func(_ context.Context, actor core.Principal, action core.Action, a core.Address) error {
		f.record(fmt.Sprintf("auth %s %s %s %s", actor, action, a.Space(), a.Value()))
		if actor == "cancelled" && action == core.WriteAction {
			admitted <- struct{}{}
		}
		if actor == "denied" || (actor == "primary-blocked" && a.Space() == "/native") {
			return errors.New("private policy detail")
		}
		return nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	mount, err := files.Install("/native", d)
	if err != nil {
		t.Fatal(err)
	}
	if err := files.BindAlias(ctx, "/alias", mount, "/"); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := files.Close(context.Background()); err != nil {
			t.Error(err)
		}
	}()
	unknown := make(chan protocols.Failure, 1)
	h, err := New(files, Options{Region: "us-east-1", Buckets: map[string]Bucket{"objects": {Space: "/native", UnversionedDelete: true}, "other": {Space: "/native"}, "alias": {Space: "/alias"}},
		OnFailure: func(f protocols.Failure) {
			if f.HasChange && f.Change.Effect == core.Unknown {
				unknown <- f
			}
		},
		LookupIdentity: func(_ context.Context, id string) (Identity, error) {
			return Identity{Principal: core.Principal(id), SecretAccessKey: "fixture-only-" + id}, nil
		},
		CursorTTL: time.Minute, MaxPutBytes: 256 << 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	export := httptest.NewTLSServer(h)
	defer export.Close()
	svc := sdkClient(export, "writer", nil)
	bucket := aws.String("objects")
	t.Run("signed reads", func(t *testing.T) {
		for i, key := range keys {
			get, err := svc.GetObject(ctx, &sdk.GetObjectInput{Bucket: bucket, Key: aws.String(key)})
			if err != nil {
				t.Fatal(err)
			}
			data, err := io.ReadAll(get.Body)
			if closeErr := get.Body.Close(); err != nil || closeErr != nil || string(data) != fmt.Sprintf("%d:%s", i, key) {
				t.Fatal(string(data), err, closeErr)
			}
			f.requireTrace(t, "auth writer read /native "+key, "native GET "+key)
		}
		key := aws.String(keys[len(keys)-1])
		head, err := svc.HeadObject(ctx, &sdk.HeadObjectInput{Bucket: bucket, Key: key})
		if err != nil || aws.ToString(head.ETag) != tag(f.objects[*key]) {
			t.Fatal(head, err)
		}
		f.requireTrace(t, "auth writer stat /native "+*key, "native HEAD "+*key)
		for _, tamper := range []func(*http.Request){
			func(r *http.Request) { r.URL.Path = "/objects/a/b"; r.URL.RawPath = "" },
			func(r *http.Request) { r.Header.Set("Authorization", r.Header.Get("Authorization")+"0") },
			func(r *http.Request) { r.Header.Set("If-Match", `"mw.native"`) },
		} {
			_, err := sdkClient(export, "writer", tamper).GetObject(ctx, &sdk.GetObjectInput{Bucket: bucket, Key: key})
			statusIs(t, err, 403)
			f.requireTrace(t)
		}
		_, err = sdkClient(export, "denied", nil).GetObject(ctx, &sdk.GetObjectInput{Bucket: bucket, Key: key})
		statusIs(t, err, 403)
		f.requireTrace(t, "auth denied read /native "+*key)
		alias := aws.String("alias")
		if _, err := svc.HeadObject(ctx, &sdk.HeadObjectInput{Bucket: alias, Key: key}); err != nil {
			t.Fatal(err)
		}
		f.requireTrace(t, "auth writer stat /alias "+*key, "auth writer stat /native "+*key, "native HEAD "+*key)
		_, err = sdkClient(export, "primary-blocked", nil).GetObject(ctx, &sdk.GetObjectInput{Bucket: alias, Key: key})
		statusIs(t, err, 403)
		f.requireTrace(t, "auth primary-blocked read /alias "+*key, "auth primary-blocked read /native "+*key)
	})

	t.Run("complete publication", func(t *testing.T) {
		plain := httptest.NewServer(h)
		defer plain.Close()
		key := "put//%2F"
		put := func(client *sdk.Client, data, match, none string) (*sdk.PutObjectOutput, error) {
			input := &sdk.PutObjectInput{Bucket: bucket, Key: aws.String(key), Body: strings.NewReader(data), ContentLength: aws.Int64(int64(len(data)))}
			if match != "" {
				input.IfMatch = aws.String(match)
			}
			if none != "" {
				input.IfNoneMatch = aws.String(none)
			}
			return client.PutObject(ctx, input)
		}
		check := func(want string) {
			t.Helper()
			result, err := svc.GetObject(ctx, &sdk.GetObjectInput{Bucket: bucket, Key: aws.String(key)})
			if err != nil {
				t.Fatal(err)
			}
			data, err := io.ReadAll(result.Body)
			if closeErr := result.Body.Close(); err != nil || closeErr != nil || string(data) != want {
				t.Fatalf("body = %q, want %q (%v %v)", data, want, err, closeErr)
			}
		}
		created, err := put(sdkClient(plain, "writer", nil), "original", "", "*")
		if err != nil || aws.ToString(created.ETag) != tag("original") {
			t.Fatal(created, err)
		}
		f.requireTrace(t, "auth writer create /native "+key, "native PUT "+key)
		updated, err := put(svc, strings.Repeat("ok", 5000), aws.ToString(created.ETag), "")
		if err != nil || aws.ToString(updated.ETag) != tag(strings.Repeat("ok", 5000)) {
			t.Fatal(updated, err)
		}
		f.requireTrace(t, "auth writer write /native "+key, "native PUT "+key)
		_, err = put(svc, "stale", aws.ToString(created.ETag), "")
		statusIs(t, err, 412)
		f.requireTrace(t, "auth writer write /native "+key, "native PUT "+key)
		_, err = put(sdkClient(plain, "writer", func(r *http.Request) {
			r.Body = io.NopCloser(strings.NewReader("efgh"))
		}), "abcd", "", "")
		statusIs(t, err, 400)
		f.requireTrace(t, "auth writer write /native "+key)
		_, err = put(sdkClient(export, "writer", func(r *http.Request) {
			b, e := io.ReadAll(r.Body)
			_ = r.Body.Close()
			if e != nil || !bytes.Contains(b, []byte("abcd")) {
				t.Error("missing SDK frame", e)
			}
			r.Body = io.NopCloser(bytes.NewReader(bytes.Replace(b, []byte("abcd"), []byte("efgh"), 1)))
		}), "abcd", "", "")
		statusIs(t, err, 400)
		f.requireTrace(t, "auth writer write /native "+key)
		_, err = put(sdkClient(export, "denied", nil), "forbidden", "", "")
		statusIs(t, err, 403)
		f.requireTrace(t, "auth denied write /native "+key)

		putCtx, stop := context.WithCancel(ctx)
		defer stop()
		stream, writer := io.Pipe()
		defer writer.Close()
		done := make(chan error, 1)
		pending := strings.Repeat("x", 16<<10)
		go func() {
			blocked := sdkClient(plain, "cancelled", func(r *http.Request) {
				r.Body = io.NopCloser(io.MultiReader(strings.NewReader(pending[:len(pending)/2]), stream))
			})
			_, err := blocked.PutObject(putCtx, &sdk.PutObjectInput{Bucket: bucket, Key: aws.String(key), Body: strings.NewReader(pending), ContentLength: aws.Int64(int64(len(pending)))})
			done <- err
		}()
		select {
		case <-admitted:
		case <-ctx.Done():
			t.Fatal("cancel fixture did not reach Core")
		}
		stop()
		_ = writer.CloseWithError(context.Canceled)
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatal("upload cancellation:", err)
		}
		check(strings.Repeat("ok", 5000))
		f.requireTrace(t, "auth cancelled write /native "+key, "auth writer read /native "+key, "native GET "+key)

		_, err = svc.PutObject(ctx, &sdk.PutObjectInput{Bucket: bucket, Key: aws.String("uncertain"), Body: strings.NewReader("sent"), ContentLength: aws.Int64(4)})
		statusIs(t, err, 500)
		f.requireTrace(t, "auth writer write /native uncertain", "native PUT uncertain")
		select {
		case failure := <-unknown:
			if !failure.HasChange || failure.Change.Effect != core.Unknown {
				t.Fatal(failure)
			}
		default:
			t.Fatal("missing unknown native commit receipt")
		}
	})
	t.Run("list and remove", func(t *testing.T) {
		target := "put//%2F"
		rangeRead, err := svc.GetObject(ctx, &sdk.GetObjectInput{Bucket: bucket, Key: aws.String(target), Range: aws.String("bytes=1-2")})
		if err != nil {
			t.Fatal(err)
		}
		got, err := io.ReadAll(rangeRead.Body)
		if closeErr := rangeRead.Body.Close(); err != nil || closeErr != nil || string(got) != "ko" || aws.ToString(rangeRead.ContentRange) != "bytes 1-2/10000" {
			t.Fatal(string(got), rangeRead.ContentRange, err, closeErr)
		}
		f.requireTrace(t, "auth writer read /native "+target, "native GET "+target)
		input := &sdk.ListObjectsV2Input{Bucket: bucket, Prefix: aws.String("a"), Delimiter: aws.String("/"), StartAfter: aws.String("A"), MaxKeys: aws.Int32(1), EncodingType: types.EncodingTypeUrl}
		pager := sdk.NewListObjectsV2Paginator(svc, input)
		first, err := pager.NextPage(ctx)
		if err != nil || !aws.ToBool(first.IsTruncated) || len(first.Contents) != 1 || aws.ToString(first.Contents[0].Key) != "a" {
			t.Fatal(first, err)
		}
		f.requireTrace(t, "auth writer list /native a", "native LIST vault/a")
		reuse := *input
		reuse.ContinuationToken = first.NextContinuationToken
		_, err = sdkClient(export, "another", nil).ListObjectsV2(ctx, &reuse)
		statusIs(t, err, 400)
		reuse.Bucket = aws.String("other")
		_, err = svc.ListObjectsV2(ctx, &reuse)
		statusIs(t, err, 400)
		reuse.Bucket = bucket
		reuse.ContinuationToken = aws.String("x" + aws.ToString(first.NextContinuationToken))
		_, err = svc.ListObjectsV2(ctx, &reuse)
		statusIs(t, err, 400)
		f.requireTrace(t)
		second, err := pager.NextPage(ctx)
		if err != nil || len(second.Contents) != 1 || !aws.ToBool(second.IsTruncated) {
			t.Fatal(second, err)
		}
		escaped := aws.ToString(second.Contents[0].Key)
		if decoded, err := url.PathUnescape(escaped); err != nil || decoded != "a%2Fb +汉?" || !strings.Contains(escaped, "%252F") {
			t.Fatal(escaped, decoded, err)
		}
		f.requireTrace(t, "auth writer list /native a", "native LIST vault/a")
		reuse.ContinuationToken = second.NextContinuationToken
		reuse.StartAfter = nil
		last, err := svc.ListObjectsV2(ctx, &reuse)
		if err != nil || aws.ToBool(last.IsTruncated) || len(last.CommonPrefixes) != 1 || aws.ToString(last.CommonPrefixes[0].Prefix) != "a%2F" {
			t.Fatal(last, err)
		}
		f.requireTrace(t, "auth writer list /native a", "native LIST vault/a")
		input.Prefix = aws.String(target)
		reopened, err := svc.ListObjectsV2(ctx, input)
		if err != nil || aws.ToBool(reopened.IsTruncated) || len(reopened.CommonPrefixes) != 0 || len(reopened.Contents) != 1 {
			t.Fatal("one-cursor budget not released", reopened, err)
		}
		f.requireTrace(t, "auth writer list /native "+target, "native LIST vault/"+target)

		remove := &sdk.DeleteObjectInput{Bucket: bucket, Key: aws.String(target)}
		_, err = sdkClient(export, "denied", nil).DeleteObject(ctx, remove)
		statusIs(t, err, 403)
		f.requireTrace(t, "auth denied remove /native "+target)
		remove.Bucket = aws.String("other")
		_, err = svc.DeleteObject(ctx, remove)
		statusIs(t, err, 501)
		f.requireTrace(t)
		remove.Bucket = bucket
		if _, err = svc.DeleteObject(ctx, remove); err != nil {
			t.Fatal(err)
		}
		f.requireTrace(t, "auth writer remove /native "+target, "native HEAD "+target, "native DELETE "+target)
		if _, err = svc.DeleteObject(ctx, remove); err != nil {
			t.Fatal(err)
		}
		f.requireTrace(t, "auth writer remove /native "+target, "native HEAD "+target)
		_, err = sdkClient(export, "denied", nil).DeleteObject(ctx, remove)
		statusIs(t, err, 403)
		f.requireTrace(t, "auth denied remove /native "+target)
	})
}

func tag(data string) string { return fmt.Sprintf("\"mw.%08x\"", crc32.ChecksumIEEE([]byte(data))) }

func (f *objectFixture) list(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	q := r.URL.Query()
	prefix, delim := q.Get("prefix"), q.Get("delimiter")
	f.trace = append(f.trace, "native LIST "+prefix)
	entries := map[string]bool{}
	for key := range f.objects {
		full := "vault/" + key
		suffix, ok := strings.CutPrefix(full, prefix)
		if !ok {
			continue
		}
		if group, _, found := strings.Cut(suffix, delim); delim != "" && found {
			entries[prefix+group+delim] = true
		} else {
			entries[full] = false
		}
	}
	after := max(q.Get("start-after"), q.Get("continuation-token"))
	names := make([]string, 0, len(entries))
	for key := range entries {
		if key > after {
			names = append(names, key)
		}
	}
	sort.Strings(names)
	n, err := strconv.Atoi(q.Get("max-keys"))
	if err != nil || n < 1 {
		http.Error(w, "invalid fixture limit", 400)
		return
	}
	page := bucketPage{EncodingType: "url", IsTruncated: len(names) > n}
	if page.IsTruncated {
		page.NextContinuationToken = names[n-1]
	}
	for _, key := range names[:min(n, len(names))] {
		encoded := strings.ReplaceAll(url.QueryEscape(key), "+", "%20")
		if entries[key] {
			page.CommonPrefixes = append(page.CommonPrefixes, prefixEntry{Prefix: encoded})
		} else {
			data := f.objects[strings.TrimPrefix(key, "vault/")]
			size := int64(len(data))
			page.Contents = append(page.Contents, objectEntry{Key: encoded, Size: &size, ETag: tag(data)})
		}
	}
	w.Header().Set("Content-Type", "application/xml")
	_ = xml.NewEncoder(w).Encode(page)
}
