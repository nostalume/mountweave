package pool

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/local"
	bolt "go.etcd.io/bbolt"
)

type memberFixture struct {
	*local.Driver
	mu           sync.Mutex
	used, quota  int64
	offline      bool
	failStripe   string
	closeFailure error
	create       func(context.Context, string, io.Reader, *int64) (core.Change, error)
}

func (m *memberFixture) Create(ctx context.Context, pAddress core.NativeAddress, r io.Reader, n *int64) (core.Change, error) {
	if pAddress.Namespace() != core.TreeNamespace {
		return core.Change{Effect: core.Unchanged}, core.Fail(core.UnsupportedOperation, nil)
	}
	p := pAddress.Value()

	m.mu.Lock()
	hook, offline := m.create, m.offline
	if offline {
		m.mu.Unlock()
		return core.Change{Effect: core.Unchanged}, core.Fail(core.UpstreamTimeout, nil)
	}
	if n == nil || *n > m.quota-m.used {
		m.mu.Unlock()
		return core.Change{Effect: core.Unchanged}, core.Fail(core.CapacityExceeded, nil)
	}
	m.used += *n
	m.mu.Unlock()
	if hook != nil {
		return hook(ctx, p, r, n)
	}
	return m.Driver.Create(ctx, core.NativePath(p), r, n)
}
func (m *memberFixture) Open(ctx context.Context, pAddress core.NativeAddress, options core.OpenOptions) (core.Opened, error) {
	if pAddress.Namespace() != core.TreeNamespace {
		return core.Opened{}, core.Fail(core.UnsupportedOperation, nil)
	}
	p := pAddress.Value()

	m.mu.Lock()
	offline, fail := m.offline, m.failStripe
	m.mu.Unlock()
	parts := strings.Split(strings.TrimPrefix(p, "/"), "-")
	if offline || (fail != "" && len(parts) == 4 && parts[1] == fail) {
		return core.Opened{}, core.Fail(core.UpstreamTimeout, nil)
	}
	opened, err := m.Driver.Open(ctx, core.NativePath(p), options)
	if opened.Body != nil && m.closeFailure != nil {
		opened.Body = &failedClose{ReadCloser: opened.Body, err: m.closeFailure}
	}
	return opened, err
}

type failedClose struct {
	io.ReadCloser
	err error
}

func (b *failedClose) Close() error { return errors.Join(b.ReadCloser.Close(), b.err) }

type readOnly struct{ core.Driver }

func rig(t *testing.T, count int) (Options, []*memberFixture, []string) {
	t.Helper()
	opts := Options{MetadataPath: filepath.Join(t.TempDir(), "pool.db"), LockTimeout: time.Second,
		DataShards: 2, ParityShards: 1, ShardBytes: 8, MaxBufferedBytes: 48, Concurrency: 2}
	var fixtures []*memberFixture
	var roots []string
	for i := 0; i < count; i++ {
		root := t.TempDir()
		native, err := local.New(root)
		if err != nil {
			t.Fatal(err)
		}
		m := &memberFixture{Driver: native, quota: 128}
		id := string(rune('A' + i))
		opts.Members = append(opts.Members, Member{ID: id, Identity: "private-root-" + id, FailureDomain: id, Driver: m})
		fixtures = append(fixtures, m)
		roots = append(roots, root)
	}
	return opts, fixtures, roots
}
func attach(t *testing.T, opts Options) (*Driver, *core.Core) {
	t.Helper()
	d, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	c, err := core.New(core.Options{Authorize: func(context.Context, core.Principal, core.Action, core.Address) error {
		return nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = c.Install("/pool", d); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := c.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	return d, c
}
func read(t *testing.T, c *core.Core, p string, r *core.Range) ([]byte, error) {
	t.Helper()
	opened, err := c.Open(context.Background(), "user", core.Path(p), core.OpenOptions{Range: r})
	if err != nil {
		return nil, err
	}
	b, err := io.ReadAll(opened.Body)
	return b, errors.Join(err, opened.Body.Close())
}

func TestDistributedPublicationRangeAndDegradedRestore(t *testing.T) {
	opts, members, roots := rig(t, 3)
	for _, m := range members {
		m.quota = 32
	}
	d, c := attach(t, opts)
	payload := []byte("0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJK") // 47 > each member's 32-byte quota.
	if change, err := c.Write(context.Background(), "user", core.Path("/pool/movie"), bytes.NewReader(payload), nil); err != nil || change.Effect != core.Applied {
		t.Fatal(change, err)
	}
	for i, root := range roots {
		entries, err := os.ReadDir(root)
		if err != nil || len(entries) != 3 || members[i].used != 24 {
			t.Fatal("content not distributed", entries, err, members[i].used)
		}
	}
	if got, err := read(t, c, "/pool/movie", nil); err != nil || !bytes.Equal(got, payload) {
		t.Fatal(string(got), err)
	}
	r := &core.Range{Start: 7, End: 34}
	if got, err := read(t, c, "/pool/movie", r); err != nil || !bytes.Equal(got, payload[7:35]) {
		t.Fatal(string(got), err)
	}
	// Each stripe position (including the padded tail) loses or corrupts one
	// private shard. Recovery must verify output rather than accepting zeros.
	f, err := d.file("/movie")
	if err != nil {
		t.Fatal(err)
	}
	for index := int64(0); index < f.Stripes; index++ {
		s, err := d.stripe(f, index)
		if err != nil {
			t.Fatal(err)
		}
		shard := s.Shards[0]
		memberIndex := int(shard.Member[0] - 'A')
		file := filepath.Join(roots[memberIndex], shard.Path[1:])
		old, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		if index%2 == 0 {
			err = os.Remove(file)
		} else {
			damaged := bytes.Clone(old)
			damaged[0] ^= 0xff
			err = os.WriteFile(file, damaged, 0600)
		}
		if err != nil {
			t.Fatal(err)
		}
		if got, err := read(t, c, "/pool/movie", nil); err != nil || !bytes.Equal(got, payload) {
			t.Fatal("degraded read", index, got, err)
		}
		if err = os.WriteFile(file, old, 0600); err != nil {
			t.Fatal(err)
		}
	}
	// A is entirely offline; B/C still cover every stripe despite rotation.
	members[0].mu.Lock()
	members[0].offline = true
	members[0].mu.Unlock()
	if got, err := read(t, c, "/pool/movie", r); err != nil || !bytes.Equal(got, payload[7:35]) {
		t.Fatal("member outage", got, err)
	}
	members[1].mu.Lock()
	members[1].failStripe = "1"
	members[1].mu.Unlock()
	if got, err := read(t, c, "/pool/movie", nil); err == nil || !bytes.Equal(got, payload[:16]) {
		t.Fatal("partial body was hidden", got, err)
	}
	members[0].mu.Lock()
	members[0].offline = false
	members[0].mu.Unlock()
	members[1].mu.Lock()
	members[1].failStripe = ""
	members[1].mu.Unlock()
	// Copy uses the ordinary Core API, including one reader and one writer budget.
	for _, m := range members {
		m.quota = 64
	}
	if result, err := c.Copy(context.Background(), "user", core.Path("/pool/movie"), core.Path("/pool/copy")); err != nil || result.Effect != core.Applied {
		t.Fatal(result, err)
	}
	zero := int64(0)
	if change, err := c.Write(context.Background(), "user", core.Path("/pool/empty"), bytes.NewReader(nil), &zero); err != nil || change.Effect != core.Applied {
		t.Fatal(change, err)
	}
	if got, err := read(t, c, "/pool/empty", nil); err != nil || len(got) != 0 {
		t.Fatal(got, err)
	}
	if _, err := read(t, c, "/pool/empty", &core.Range{Start: 0, End: -1}); core.CodeOf(err) != core.RangeNotSatisfiable {
		t.Fatal(err)
	}
	var backup bytes.Buffer
	if err := d.Backup(context.Background(), &backup); err != nil {
		t.Fatal(err)
	}
	if err := c.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	opts.MetadataPath = filepath.Join(t.TempDir(), "restored.db")
	if err := os.WriteFile(opts.MetadataPath, backup.Bytes(), 0600); err != nil {
		t.Fatal(err)
	}
	// Rebuild only B/C: no stale runtime pointers or A credential are needed.
	opts.Members = opts.Members[1:]
	for i := range opts.Members {
		native, err := local.New(roots[i+1])
		if err != nil {
			t.Fatal(err)
		}
		opts.Members[i].Driver = readOnly{native}
	}
	_, restored := attach(t, opts)
	if got, err := read(t, restored, "/pool/copy", &core.Range{Start: 13, End: -1, Suffix: true}); err != nil || !bytes.Equal(got, payload[len(payload)-13:]) {
		t.Fatal("restored range", got, err)
	}
	untouched := &faultInput{}
	if change, err := restored.Write(context.Background(), "user", core.Path("/pool/read-only"), untouched, nil); core.CodeOf(err) != core.UnsupportedOperation || change.Effect != core.Unchanged || untouched.reads != 0 {
		t.Fatal(change, err, untouched.reads)
	}
}

type faultInput struct {
	reads   int
	closed  bool
	failure error
}

func (r *faultInput) Read(p []byte) (int, error) {
	r.reads++
	if r.reads <= 2 {
		return copy(p, []byte("12345678")), nil
	}
	if r.failure != nil {
		return 0, r.failure
	}
	return 0, errors.New("input fault")
}
func (r *faultInput) Close() error { r.closed = true; return nil }

func TestPrivateIntentsSparePlacementAndBufferAdmission(t *testing.T) {
	opts, members, _ := rig(t, 4)
	opts.MaxBufferedBytes = 24
	// A stored bytes but lost its response. D is a spare; A's original Unknown
	// stays recorded for recovery instead of triggering a blind delete/replay.
	members[0].create = func(ctx context.Context, p string, r io.Reader, n *int64) (core.Change, error) {
		if _, err := members[0].Driver.Create(ctx, core.NativePath(p), r, n); err != nil {
			return core.Change{}, err
		}
		return core.Change{Effect: core.Unknown}, core.Fail(core.UpstreamTimeout, nil)
	}
	d, c := attach(t, opts)
	payload := []byte("one complete stripe")
	if change, err := c.Write(context.Background(), "user", core.Path("/pool/file"), bytes.NewReader(payload), nil); err != nil || change.Effect != core.Applied {
		t.Fatal(change, err)
	}
	f, err := d.file("/file")
	if err != nil {
		t.Fatal(err)
	}
	s, err := d.stripe(f, 0)
	if err != nil || len(s.Abandoned) != 1 || s.Abandoned[0].Effect != core.Unknown || s.Shards[0].Member != "D" {
		t.Fatal(s, err)
	}
	input := &faultInput{}
	if change, err := c.Write(context.Background(), "user", core.Path("/pool/bad"), input, nil); err == nil || change.Effect != core.Unchanged || input.closed {
		t.Fatal(change, err, input.closed)
	}
	if _, err = c.Stat(context.Background(), "user", core.Path("/pool/bad")); core.CodeOf(err) != core.NotFound {
		t.Fatal(err)
	}
	broken := &faultInput{failure: io.ErrUnexpectedEOF}
	if change, err := c.Write(context.Background(), "user", core.Path("/pool/truncated"), broken, nil); !errors.Is(err, io.ErrUnexpectedEOF) || change.Effect != core.Unchanged {
		t.Fatal("truncation accepted as normal EOF", change, err)
	}
	declared := int64(16)
	if change, err := c.Write(context.Background(), "user", core.Path("/pool/short"), strings.NewReader("short"), &declared); !errors.Is(err, io.ErrUnexpectedEOF) || change.Effect != core.Unchanged {
		t.Fatal(change, err)
	}
	page, err := c.List(context.Background(), "user", core.ListRequest{Query: core.Children(core.Path("/pool")), Limit: 10, CursorTTL: time.Minute})
	if err != nil || len(page.Items) != 1 || page.Items[0].Name != "file" {
		t.Fatal(page, err)
	}
	if err = d.db.View(func(tx *bolt.Tx) error {
		pending := 0
		err := tx.Bucket(uploadsKey).ForEach(func(k, _ []byte) error {
			var u uploadRecord
			if err := decode(tx.Bucket(uploadsKey).Bucket(k).Get(headerKey), &u); err != nil {
				return err
			}
			if !u.Complete {
				pending++
			}
			return nil
		})
		if err == nil && pending != 3 {
			t.Fatalf("unfinished intent not retained: %d", pending)
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if result, err := c.Copy(context.Background(), "user", core.Path("/pool/file"), core.Path("/pool/copy-refused")); core.CodeOf(err) != core.CapacityExceeded || result.Effect != core.Unchanged {
		t.Fatal("same-pool copy waited on its own budget", result, err)
	}
	// A held read buffer fills the shared allowance. A further write refuses
	// before touching input rather than deadlocking on a potentially borrowed Body.
	var bodies []io.ReadCloser
	for i := 0; i < 1; i++ {
		content, err := c.Open(context.Background(), "user", core.Path("/pool/file"), core.OpenOptions{})
		if err != nil {
			t.Fatal(err)
		}
		bodies = append(bodies, content.Body)
		if _, err = io.ReadFull(content.Body, make([]byte, 1)); err != nil {
			t.Fatal(err)
		}
	}
	untouched := &faultInput{}
	if change, err := c.Write(context.Background(), "user", core.Path("/pool/full"), untouched, nil); core.CodeOf(err) != core.CapacityExceeded || change.Effect != core.Unchanged || untouched.reads != 0 {
		t.Fatal(change, err, untouched.reads)
	}
	for _, b := range bodies {
		if err := b.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if got, err := read(t, c, "/pool/file", nil); err != nil || !bytes.Equal(got, payload) {
		t.Fatal(got, err)
	}
	if result, err := d.Collect(context.Background(), 64); !errors.Is(err, ErrUncertain) || !result.More {
		t.Fatal("uncertain upload was reclaimed", result, err)
	}
	info, err := d.Members(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range info {
		if m.ID == "A" && m.UsedBytes == 0 {
			t.Fatal("uncertain reservation was released")
		}
	}
}

func TestCancellationAndRetirementWaitForNativeAcknowledgement(t *testing.T) {
	opts, members, _ := rig(t, 3)
	opts.Concurrency = 1
	started, canceled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	members[0].create = func(ctx context.Context, _ string, _ io.Reader, _ *int64) (core.Change, error) {
		close(started)
		<-ctx.Done()
		close(canceled)
		<-release
		return core.Change{Effect: core.Unchanged}, ctx.Err()
	}
	d, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		change, err := d.Create(ctx, core.NativePath("/cancel"), strings.NewReader("123456789"), nil)
		if change.Effect != core.Unchanged {
			err = errors.New("canceled input was published")
		}
		done <- err
	}()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("native write did not start")
	}
	cancel()
	<-canceled
	closed := make(chan error, 1)
	go func() { closed <- d.Close() }()
	select {
	case err := <-closed:
		t.Fatal("close preceded native acknowledgement", err)
	default:
	}
	close(release)
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := <-closed; err != nil {
		t.Fatal(err)
	}
	// Reopen retains the private intent, while the logical name remains absent.
	for i := range opts.Members {
		opts.Members[i].Driver = nil
	}
	d, err = New(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	if _, err = d.Stat(context.Background(), core.NativePath("/cancel")); core.CodeOf(err) != core.NotFound {
		t.Fatal(err)
	}
	if err = d.Close(); err != nil {
		t.Fatal(err)
	}
	// Recoverable data must not conceal a failed native Body.Close.
	opts, members, _ = rig(t, 3)
	cleanup := errors.New("native close did not acknowledge")
	members[0].closeFailure = cleanup
	d, err = New(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	if _, err = d.Create(context.Background(), core.NativePath("/file"), strings.NewReader("content"), nil); err != nil {
		t.Fatal(err)
	}
	opened, err := d.Open(context.Background(), core.NativePath("/file"), core.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = io.ReadAll(opened.Body); !errors.Is(err, cleanup) {
		t.Fatal("cleanup error was hidden by reconstruction", err)
	}
	if err = opened.Body.Close(); err != nil {
		t.Fatal(err)
	}
	untouched := &faultInput{}
	if change, err := d.Create(context.Background(), core.NativePath("/blocked"), untouched, nil); !errors.Is(err, cleanup) || change.Effect != core.Unchanged || untouched.reads != 0 {
		t.Fatal("failed cleanup admitted more I/O", change, err)
	}
	var snapshot bytes.Buffer
	if err = d.Backup(context.Background(), &snapshot); err != nil {
		t.Fatal("metadata recovery blocked by a member cleanup failure", err)
	}
	built := false
	if _, err = d.ReconnectMember(context.Background(), "A", "private-root-A", func(context.Context) (core.Driver, error) {
		built = true
		return nil, nil
	}); !errors.Is(err, cleanup) || built {
		t.Fatal("failed native body cleanup allowed a replacement owner", err)
	}
	if err = d.Close(); !errors.Is(err, cleanup) {
		t.Fatal("retirement lost cleanup ownership", err)
	}
}
