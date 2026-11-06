package s3

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	core "github.com/nostalume/mountweave"
	driver "github.com/nostalume/mountweave/drivers/s3"
	"github.com/nostalume/mountweave/input"
	"github.com/nostalume/mountweave/protocols"
	bolt "go.etcd.io/bbolt"
)

const multipartTestKey = "a//../%2F +汉?"

type multipartFixture struct {
	server    *httptest.Server
	client    *sdk.Client
	files     *core.Core
	db        *bolt.DB
	inputs    *input.Store
	failures  chan protocols.Failure
	closeOnce sync.Once
}

func openMultipartFixture(t *testing.T, dir, upstream string, checkpoints ...int) *multipartFixture {
	t.Helper()
	var checkpointBytes int
	if len(checkpoints) > 0 {
		checkpointBytes = checkpoints[0]
	}
	ctx := context.Background()
	f := &multipartFixture{failures: make(chan protocols.Failure, 16)}
	t.Cleanup(func() { f.close(t) })
	var err error
	f.db, err = bolt.Open(filepath.Join(dir, "manifest.db"), 0600, &bolt.Options{Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	private := filepath.Join(dir, "inputs")
	if err := os.MkdirAll(private, 0700); err != nil {
		t.Fatal(err)
	}
	f.inputs, err = input.Open(ctx, private, input.Options{MaxBytes: 32 << 20, MaxInputs: 16, MaxBlocks: 32, MaxAppendBytes: 2 << 20})
	if err != nil {
		t.Fatal(err)
	}
	f.files, err = core.New(core.Options{Authorize: func(context.Context, core.Principal, core.Action, core.Address) error { return nil }})
	if err != nil {
		t.Fatal(err)
	}
	d, err := driver.New(sdkAt(upstream, http.DefaultClient, "upstream", nil), "upstream", "vault/", driver.Options{PartSize: 8 << 20, MaxBufferedBytes: 8 << 20, ResumeScope: "fixture-resume"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.files.Install("/native", d); err != nil {
		t.Fatal(err)
	}
	h, err := New(f.files, Options{
		Region: "us-east-1", Buckets: map[string]Bucket{"objects": {Space: "/native"}}, CursorTTL: time.Minute, MaxPutBytes: 6 << 20,
		LookupIdentity: func(_ context.Context, id string) (Identity, error) {
			return Identity{Principal: core.Principal(id), SecretAccessKey: "fixture-only-" + id}, nil
		},
		Multipart: &MultipartOptions{
			DB: f.db, Inputs: f.inputs, MaxUploads: 8, MaxPendingParts: 4, MaxObjectBytes: 12 << 20, Retention: time.Hour, MaxCheckpointBytes: checkpointBytes,
			Bind: func(ctx context.Context, _ core.Principal, _ string, target core.Address, expected string) (string, *core.Selection, error) {
				const binding = "fixture-definition/1"
				if expected != "" && expected != binding {
					return "", nil, core.Fail(core.MountUnavailable, nil)
				}
				selected, err := f.files.Capture(ctx, core.AddressAccess{Address: target, Write: true})
				return binding, selected, err
			},
		},
		OnFailure: func(failure protocols.Failure) {
			select {
			case f.failures <- failure:
			default:
			}
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	f.server = httptest.NewTLSServer(h)
	f.client = sdkClient(f.server, "writer", nil)
	return f
}

func (f *multipartFixture) close(t *testing.T) {
	t.Helper()
	f.closeOnce.Do(func() {
		if f.server != nil {
			f.server.Close()
		}
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if f.files != nil {
			if err := f.files.Close(ctx); err != nil {
				t.Error(err)
			}
		}
		if f.inputs != nil {
			if err := f.inputs.Close(ctx); err != nil {
				t.Error(err)
			}
		}
		if f.db != nil {
			if err := f.db.Close(); err != nil {
				t.Error(err)
			}
		}
	})
}

type multipartTestUpload struct {
	ID, Key string
	Parts   []types.CompletedPart
}

func (f *multipartFixture) create(t *testing.T, ctx context.Context, key string) multipartTestUpload {
	t.Helper()
	out, err := f.client.CreateMultipartUpload(ctx, &sdk.CreateMultipartUploadInput{Bucket: aws.String("objects"), Key: aws.String(key)})
	if err != nil || aws.ToString(out.UploadId) == "" {
		t.Fatal(out, err)
	}
	return multipartTestUpload{ID: *out.UploadId, Key: key}
}

func (u multipartTestUpload) upload(ctx context.Context, c *sdk.Client, n int32, data string) (types.CompletedPart, error) {
	out, err := c.UploadPart(ctx, &sdk.UploadPartInput{
		Bucket: aws.String("objects"), Key: aws.String(u.Key), UploadId: aws.String(u.ID), PartNumber: aws.Int32(n),
		Body: strings.NewReader(data), ContentLength: aws.Int64(int64(len(data))),
	})
	if err != nil {
		return types.CompletedPart{}, err
	}
	return types.CompletedPart{PartNumber: aws.Int32(n), ETag: out.ETag, ChecksumCRC32: out.ChecksumCRC32}, nil
}

func (u multipartTestUpload) complete(ctx context.Context, c *sdk.Client, match string) (*sdk.CompleteMultipartUploadOutput, error) {
	in := &sdk.CompleteMultipartUploadInput{Bucket: aws.String("objects"), Key: aws.String(u.Key), UploadId: aws.String(u.ID),
		MultipartUpload: &types.CompletedMultipartUpload{Parts: u.Parts}}
	if match != "" {
		in.IfMatch = aws.String(match)
	}
	return c.CompleteMultipartUpload(ctx, in)
}

func (u multipartTestUpload) abort(ctx context.Context, c *sdk.Client) error {
	_, err := c.AbortMultipartUpload(ctx, &sdk.AbortMultipartUploadInput{Bucket: aws.String("objects"), Key: aws.String(u.Key), UploadId: aws.String(u.ID)})
	return err
}

func TestSDKMultipart(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	part := func(u multipartTestUpload, c *sdk.Client, n int32, data string) types.CompletedPart {
		t.Helper()
		out, err := u.upload(ctx, c, n, data)
		if err != nil || aws.ToString(out.ETag) == "" || aws.ToString(out.ChecksumCRC32) == "" {
			t.Fatal(out, err)
		}
		return out
	}
	if mode := os.Getenv("MOUNTWEAVE_MULTIPART_TEST_CHILD"); mode != "" {
		dir := os.Getenv("MOUNTWEAVE_MULTIPART_TEST_DIR")
		f := openMultipartFixture(t, dir, os.Getenv("MOUNTWEAVE_MULTIPART_TEST_UPSTREAM"))
		key := multipartTestKey
		if mode == "commit" {
			key = "crash"
		}
		u := f.create(t, ctx, key)
		if mode == "stage" {
			part(u, f.client, 3, "TAIL")
			original := strings.Repeat("a", minPartBytes)
			first := part(u, f.client, 1, original)
			again := part(u, f.client, 1, original)
			if aws.ToString(first.ETag) != aws.ToString(again.ETag) {
				t.Fatal("same-number retry changed ETag")
			}
			// Interrupt after Input.Create but before recording the input ID.
			owner := "s3-multipart/" + u.ID + "/interrupted"
			must(f.db.Update(func(tx *bolt.Tx) error {
				b, _, err := readUpload(tx, u.ID)
				if err != nil {
					return err
				}
				if err = b.Bucket(pendingTable).Put([]byte(owner), []byte(unlinkedInput)); err != nil {
					return err
				}
				return addCount(tx.Bucket(manifestRoot), pendingCount, 1)
			}))
			_, err := f.inputs.Create(ctx, owner)
			must(err)
		} else {
			u.Parts = []types.CompletedPart{part(u, f.client, 1, "may-have-been-published")}
		}
		resume, err := json.Marshal(u)
		must(err)
		must(os.WriteFile(filepath.Join(dir, mode+".json"), resume, 0600))
		if mode == "commit" {
			_, err = u.complete(ctx, f.client, "")
			t.Fatalf("process was supposed to be killed during commit: %v", err)
		}
		os.Exit(0) // Deliberately skip Handler, Input and database shutdown.
	}

	dir := t.TempDir()
	upstreamObjects := &objectFixture{objects: map[string]string{multipartTestKey: "old"}}
	entered, release, crashing := make(chan struct{}), make(chan struct{}), make(chan struct{})
	crashDone := make(chan struct{})
	var crashOnce sync.Once
	releaseCrash := func() { crashOnce.Do(func() { close(crashDone) }) }
	var releaseOnce sync.Once
	openGate := func() { releaseOnce.Do(func() { close(release) }) }
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut && r.URL.Path == "/upstream/vault/"+multipartTestKey && r.Header.Get("If-Match") == tag("old") {
			select {
			case <-entered:
			default:
				close(entered)
			}
			select {
			case <-release:
			case <-r.Context().Done():
				return
			}
		}
		if r.Method == http.MethodPut && r.URL.Path == "/upstream/vault/crash" {
			close(crashing)
			<-crashDone
			return
		}
		upstreamObjects.ServeHTTP(w, r)
	}))
	defer upstream.Close()
	defer openGate()
	defer releaseCrash()
	self, err := os.Executable()
	must(err)
	child := func(mode string) *exec.Cmd {
		cmd := exec.CommandContext(ctx, self, "-test.run=^TestSDKMultipart$", "-test.timeout=40s")
		cmd.Env = append(os.Environ(), "MOUNTWEAVE_MULTIPART_TEST_CHILD="+mode,
			"MOUNTWEAVE_MULTIPART_TEST_DIR="+dir, "MOUNTWEAVE_MULTIPART_TEST_UPSTREAM="+upstream.URL)
		return cmd
	}
	readUploadResult := func(mode string) multipartTestUpload {
		t.Helper()
		data, err := os.ReadFile(filepath.Join(dir, mode+".json"))
		must(err)
		var result multipartTestUpload
		must(json.Unmarshal(data, &result))
		return result
	}
	if out, err := child("stage").CombinedOutput(); err != nil {
		t.Fatalf("staging child: %v\n%s", err, out)
	}
	u := readUploadResult("stage")
	f := openMultipartFixture(t, dir, upstream.URL)
	inputCount := func(f *multipartFixture, want int) {
		t.Helper()
		items, err := f.inputs.List(ctx)
		if err != nil || len(items) != want {
			t.Fatalf("private input count %d, want %d: %v", len(items), want, err)
		}
	}
	readObject := func(f *multipartFixture, key string) string {
		t.Helper()
		out, err := f.client.GetObject(ctx, &sdk.GetObjectInput{Bucket: aws.String("objects"), Key: aws.String(key)})
		must(err)
		body, err := io.ReadAll(out.Body)
		must(errors.Join(err, out.Body.Close()))
		return string(body)
	}
	inputCount(f, 2)
	if readObject(f, u.Key) != "old" {
		t.Fatal("staged parts changed the public object")
	}
	listInput := func(upload multipartTestUpload) *sdk.ListPartsInput {
		return &sdk.ListPartsInput{Bucket: aws.String("objects"), Key: aws.String(upload.Key), UploadId: aws.String(upload.ID)}
	}
	list, err := f.client.ListParts(ctx, listInput(u))
	must(err)
	if len(list.Parts) != 2 || aws.ToInt32(list.Parts[0].PartNumber) != 1 || aws.ToInt32(list.Parts[1].PartNumber) != 3 {
		t.Fatalf("recover installed parts in order: %+v", list)
	}
	old := list.Parts[0].ETag
	replacement := strings.Repeat("b", minPartBytes)
	newPart := part(u, f.client, 1, replacement)
	if aws.ToString(old) == aws.ToString(newPart.ETag) {
		t.Fatal("replacement retained old ETag")
	}

	// Reconstruct Complete exclusively from the durable manifest, one page at a time.
	paged := listInput(u)
	paged.MaxParts = aws.Int32(1)
	pager := sdk.NewListPartsPaginator(f.client, paged)
	for pager.HasMorePages() {
		page, err := pager.NextPage(ctx)
		must(err)
		if len(page.Parts) != 1 {
			t.Fatalf("bounded page: %+v", page)
		}
		p := page.Parts[0]
		wantSize := int64(len("TAIL"))
		if len(u.Parts) == 0 {
			wantSize = int64(len(replacement))
		}
		if aws.ToInt64(p.Size) != wantSize || p.LastModified == nil {
			t.Fatalf("part metadata: %+v", p)
		}
		u.Parts = append(u.Parts, types.CompletedPart{PartNumber: p.PartNumber, ETag: p.ETag, ChecksumCRC32: p.ChecksumCRC32})
	}
	if len(u.Parts) != 2 || aws.ToInt32(u.Parts[0].PartNumber) != 1 || aws.ToInt32(u.Parts[1].PartNumber) != 3 ||
		aws.ToString(u.Parts[0].ETag) != aws.ToString(newPart.ETag) || aws.ToString(u.Parts[0].ChecksumCRC32) != aws.ToString(newPart.ChecksumCRC32) {
		t.Fatalf("latest part receipts: %+v", u.Parts)
	}
	_, err = sdkClient(f.server, "another", nil).ListParts(ctx, listInput(u))
	statusIs(t, err, 404)
	wrongKey := listInput(u)
	wrongKey.Key = aws.String(u.Key + "/other")
	_, err = f.client.ListParts(ctx, wrongKey)
	statusIs(t, err, 404)
	stale := u
	stale.Parts = []types.CompletedPart{{PartNumber: aws.Int32(1), ETag: old}, u.Parts[1]}
	_, err = stale.complete(ctx, f.client, tag("old"))
	statusIs(t, err, 400)
	unordered := u
	unordered.Parts = []types.CompletedPart{u.Parts[1], u.Parts[0]}
	_, err = unordered.complete(ctx, f.client, tag("old"))
	statusIs(t, err, 400)
	_, err = u.upload(ctx, sdkClient(f.server, "another", nil), 1, "wrong owner")
	statusIs(t, err, 404)
	_, err = u.complete(ctx, f.client, tag("obsolete"))
	statusIs(t, err, 412)
	inputCount(f, 2)
	if readObject(f, u.Key) != "old" {
		t.Fatal("failed Complete changed the public object")
	}

	// Drop a successfully written Complete response after the server persisted it.
	lost := *f.server.Client()
	lost.Transport = transportFunc(func(r *http.Request) (*http.Response, error) {
		out, err := f.server.Client().Transport.RoundTrip(r)
		if err != nil || r.Method != http.MethodPost || r.URL.Query().Get("uploadId") == "" || out.StatusCode != 200 {
			return out, err
		}
		_, err = io.Copy(io.Discard, out.Body)
		return nil, errors.Join(io.ErrUnexpectedEOF, err, out.Body.Close())
	})
	completedCall := make(chan error, 1)
	go func() {
		_, err := u.complete(ctx, sdkAt(f.server.URL, &lost, "writer", nil), tag("old"))
		completedCall <- err
	}()
	select {
	case <-entered:
	case err := <-completedCall:
		t.Fatal("Complete returned before native publication:", err)
	case <-ctx.Done():
		t.Fatal("Complete did not enter native publication")
	}
	statusIs(t, u.abort(ctx, f.client), 409)
	frozen, err := f.client.ListParts(ctx, listInput(u))
	must(err)
	if len(frozen.Parts) != 2 {
		t.Fatalf("frozen parts: %+v", frozen)
	}
	openGate()
	if err := <-completedCall; !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatal("lost Complete reply:", err)
	}
	f.close(t)
	f = openMultipartFixture(t, dir, upstream.URL)
	receipt, err := u.complete(ctx, f.client, tag("old"))
	must(err)
	if aws.ToString(receipt.ETag) != tag(replacement+"TAIL") || readObject(f, u.Key) != replacement+"TAIL" {
		t.Fatal("completed receipt or assembled content differs")
	}
	inputCount(f, 0)
	_, err = f.client.ListParts(ctx, listInput(u))
	statusIs(t, err, 404)
	upstreamObjects.mu.Lock()
	puts := 0
	for _, event := range upstreamObjects.trace {
		if event == "native PUT "+u.Key {
			puts++
		}
	}
	upstreamObjects.mu.Unlock()
	if puts != 2 { // One rejected condition and one actual publication.
		t.Fatalf("Complete replay changed native PUT attempts: %d", puts)
	}

	abort := f.create(t, ctx, u.Key)
	abort.Parts = []types.CompletedPart{part(abort, f.client, 2, "private")}
	// Block a second part mid-body, then abort after its input is allocated.
	reader, writer := io.Pipe()
	defer writer.Close()
	blocked := sdkClient(f.server, "writer", func(r *http.Request) {
		r.Body = io.NopCloser(io.MultiReader(io.LimitReader(r.Body, 16<<10), reader))
	})
	uploadDone := make(chan error, 1)
	go func() {
		_, err := abort.upload(ctx, blocked, 1, strings.Repeat("p", minPartBytes))
		uploadDone <- err
	}()
	poll := time.NewTicker(5 * time.Millisecond)
	defer poll.Stop()
waitForInput:
	for {
		records, err := f.inputs.List(ctx)
		must(err)
		if len(records) == 2 {
			break waitForInput
		}
		select {
		case err := <-uploadDone:
			t.Fatalf("part completed before Abort: %v", err)
		case <-ctx.Done():
			t.Fatal("part did not reach Input")
		case <-poll.C:
		}
	}
	list, err = f.client.ListParts(ctx, listInput(abort))
	must(err)
	if len(list.Parts) != 1 || aws.ToInt32(list.Parts[0].PartNumber) != 2 {
		t.Fatalf("in-flight part became visible: %+v", list)
	}
	must(abort.abort(ctx, f.client))
	_ = writer.Close()
	if err := <-uploadDone; err == nil {
		t.Fatal("aborted in-flight part was accepted")
	}
	must(abort.abort(ctx, f.client))
	_, err = abort.complete(ctx, f.client, "")
	statusIs(t, err, 404)
	_, err = f.client.ListParts(ctx, listInput(abort))
	statusIs(t, err, 404)
	_, err = abort.upload(ctx, f.client, 1, "late")
	statusIs(t, err, 404)
	inputCount(f, 0)
	if readObject(f, u.Key) != replacement+"TAIL" {
		t.Fatal("Abort changed published content")
	}

	uncertain := f.create(t, ctx, "uncertain")
	uncertain.Parts = []types.CompletedPart{part(uncertain, f.client, 1, "uncertain publication")}
	_, err = uncertain.complete(ctx, f.client, "")
	statusIs(t, err, 500)
	statusIs(t, uncertain.abort(ctx, f.client), 409)
	var observed bool
	for len(f.failures) > 0 {
		failure := <-f.failures
		observed = observed || (failure.HasChange && failure.Change.Effect == core.Unknown)
	}
	if !observed {
		t.Fatal("missing unknown publication receipt")
	}
	inputCount(f, 1)
	f.close(t)

	process := child("commit")
	must(process.Start())
	select {
	case <-crashing:
	case <-ctx.Done():
		_ = process.Process.Kill()
		_ = process.Wait()
		t.Fatal("child did not start native publication")
	}
	must(process.Process.Kill())
	if err := process.Wait(); err == nil {
		t.Fatal("child unexpectedly finished")
	}
	releaseCrash()
	crashed := readUploadResult("commit")
	f = openMultipartFixture(t, dir, upstream.URL)
	unknowns := 0
	for len(f.failures) > 0 {
		failure := <-f.failures
		if failure.Phase == "recovery" && failure.Change.Effect == core.Unknown {
			unknowns++
		}
	}
	if unknowns != 2 {
		t.Fatalf("recovery reported %d unresolved uploads", unknowns)
	}
	for _, upload := range []multipartTestUpload{uncertain, crashed} {
		_, err = upload.complete(ctx, f.client, "")
		statusIs(t, err, 409)
		statusIs(t, upload.abort(ctx, f.client), 409)
	}
	inputCount(f, 2)
}
