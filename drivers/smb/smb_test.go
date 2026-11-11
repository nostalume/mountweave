package smb_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"iter"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/iotest"
	"time"

	smb2 "github.com/hirochachacha/go-smb2/v2"
	"github.com/hirochachacha/go-smb2/v2/auth"
	"github.com/hirochachacha/go-smb2/v2/x/wire"
	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/local"
	driver "github.com/nostalume/mountweave/drivers/smb"
	serverauth "github.com/sonroyaalmerol/go-smb-server/smb/auth"
	"github.com/sonroyaalmerol/go-smb-server/smb/ntlmssp"
	"github.com/sonroyaalmerol/go-smb-server/smb/server"
	"github.com/sonroyaalmerol/go-smb-server/smb/signing"
	"github.com/sonroyaalmerol/go-smb-server/smb/vfs"
)

// This adapter supplies actual OS-exclusive creation to the existing server's
// LocalBackend. Its v0.2.0 create flags otherwise open new files read-only.
type disk struct {
	*vfs.LocalBackend
	pages, fileOpens, literalLookups atomic.Int64
}

func (b *disk) Open(ctx context.Context, o vfs.OpenOptions) (vfs.Handle, error) {
	o.Path = strings.ReplaceAll(o.Path, "\\", "/")
	if strings.EqualFold(o.Path, "Exact") {
		o.Path = "Exact" // Native case-insensitive alias counterexample.
		b.fileOpens.Add(1)
	}
	if o.Disposition == vfs.DispositionCreate {
		name := filepath.Join(b.Root, filepath.FromSlash(o.Path))
		if o.CreateDir {
			if err := os.Mkdir(name, 0700); err != nil {
				return nil, err
			}
		} else {
			f, err := os.OpenFile(name, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
			if err != nil {
				return nil, err
			}
			if err := f.Close(); err != nil {
				return nil, err
			}
		}
		o.Disposition = vfs.DispositionOpen
	}
	h, err := b.LocalBackend.Open(ctx, o)
	if err != nil {
		return nil, err
	}
	return &diskHandle{Handle: h, disk: b}, nil
}

type diskHandle struct {
	vfs.Handle
	disk *disk
}

func (h *diskHandle) Enumerate(ctx context.Context, pattern string) iter.Seq2[vfs.FileInfo, error] {
	h.disk.pages.Add(1)
	if strings.EqualFold(pattern, "Exact") {
		h.disk.literalLookups.Add(1)
		pattern = "Exact" // A native case-insensitive query still returns the stored name.
	}
	return h.Handle.Enumerate(ctx, pattern)
}

type readyLog chan string

func (w readyLog) Write(p []byte) (int, error) {
	if _, tail, ok := strings.Cut(string(p), "addr="); ok {
		select {
		case w <- strings.Fields(tail)[0]:
		default:
		}
	}
	return len(p), nil
}

type transport struct {
	address          string
	count            atomic.Int64
	arm              atomic.Uint32
	entered          chan struct{}
	split            bool
	pages, continued atomic.Int64
	keyMu            sync.Mutex
	key              []byte
}

type captureAuth struct {
	serverauth.Authenticator
	fixture *transport
}

func (a captureAuth) Accept(ctx context.Context, token []byte) (serverauth.AcceptResult, error) {
	r, err := a.Authenticator.Accept(ctx, token)
	if err == nil && r.Identity != nil {
		a.fixture.keyMu.Lock()
		a.fixture.key = signing.DeriveSigningKey(r.SessionKey)
		a.fixture.keyMu.Unlock()
	}
	return r, err
}

func (d *transport) Dial(ctx context.Context, _ string) (smb2.Transport, error) {
	c, err := (&net.Dialer{}).DialContext(ctx, "tcp", d.address)
	if err != nil {
		return nil, err
	}
	d.count.Add(1)
	return smb2.NewTransport(&faultConn{Conn: c, fixture: d, closed: make(chan struct{})}), nil
}

// The server still authenticates, signs and executes every request. This small
// transport fault only holds/drops an already received reply; it is not a server.
type faultConn struct {
	net.Conn
	fixture   *transport
	closed    chan struct{}
	once      sync.Once
	pending   []byte
	mu        sync.Mutex
	directory [16]byte
	tail      []byte
}

func (c *faultConn) Close() error {
	c.once.Do(func() { close(c.closed) })
	c.mu.Lock()
	c.tail = nil
	c.mu.Unlock()
	return c.Conn.Close()
}

func (c *faultConn) Write(p []byte) (int, error) {
	if c.fixture.split && len(p) >= 88 && bytes.Equal(p[:4], []byte("\xfeSMB")) {
		command := wire.PacketCodec(p).Command()
		var id [16]byte
		copy(id[:], p[72:88])
		c.mu.Lock()
		if command == wire.SMB2_QUERY_DIRECTORY {
			c.directory = id
		} else if command == wire.SMB2_CLOSE && id == c.directory {
			c.tail = nil
		}
		c.mu.Unlock()
	}
	return c.Conn.Write(p)
}

type rawOutput []byte

func (b rawOutput) Size() int       { return len(b) }
func (b rawOutput) Encode(p []byte) { copy(p, b) }

// The existing server returns a directory in one native page. This signed
// fixture projects that same valid output into two pages, preserving request
// IDs and credits. No authentication, filesystem operation or entry is invented.
func (c *faultConn) directoryPage(frame []byte) ([]byte, error) {
	p := wire.PacketCodec(frame[4:])
	if !c.fixture.split || p.Command() != wire.SMB2_QUERY_DIRECTORY {
		return frame, nil
	}
	c.fixture.pages.Add(1)
	c.mu.Lock()
	defer c.mu.Unlock()
	select {
	case <-c.closed:
		return nil, net.ErrClosed
	default:
	}
	var output []byte
	if p.Status() == 0 {
		q := wire.QueryDirectoryResponseDecoder(p[64:])
		if q.IsInvalid() || len(q.Output()) < 4 {
			return nil, errors.New("invalid fixture directory output")
		}
		all := q.Output()
		n := int(binary.LittleEndian.Uint32(all[:4]))
		if n == 0 {
			return frame, nil
		}
		if n < 4 || n >= len(all) {
			return nil, errors.New("invalid fixture entry boundary")
		}
		output = append([]byte(nil), all[:n]...)
		binary.LittleEndian.PutUint32(output[:4], 0)
		c.tail = append([]byte(nil), all[n:]...)
	} else if p.Status() == 0x80000006 && len(c.tail) != 0 {
		output, c.tail = c.tail, nil
		c.fixture.continued.Add(1)
	} else {
		return frame, nil
	}
	response := wire.QueryDirectoryResponse{PacketHeader: wire.PacketHeader{
		Flags: p.Flags(), MessageId: p.MessageId(), TreeId: p.TreeId(), SessionId: p.SessionId(), CreditRequestResponse: p.CreditResponse(),
	}, Output: rawOutput(output)}
	result := make([]byte, 4+response.Size())
	binary.BigEndian.PutUint32(result[:4], uint32(response.Size()))
	response.Encode(result[4:])
	c.fixture.keyMu.Lock()
	err := signing.Sign(result[4:], c.fixture.key, signing.AlgoAESCMAC)
	c.fixture.keyMu.Unlock()
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (c *faultConn) Read(p []byte) (int, error) {
	if len(c.pending) == 0 {
		var header [4]byte
		if _, err := io.ReadFull(c.Conn, header[:]); err != nil {
			return 0, err
		}
		n := binary.BigEndian.Uint32(header[:])
		if n > 2<<20 {
			return 0, errors.New("fixture frame exceeds bound")
		}
		frame := make([]byte, int(n)+4)
		copy(frame, header[:])
		if _, err := io.ReadFull(c.Conn, frame[4:]); err != nil {
			return 0, err
		}
		if len(frame) >= 68 && bytes.Equal(frame[4:8], []byte("\xfeSMB")) {
			command := uint32(binary.LittleEndian.Uint16(frame[16:18]))
			if command != 0 && c.fixture.arm.CompareAndSwap(command, 0) {
				select {
				case c.fixture.entered <- struct{}{}:
				default:
				}
				if command == 8 { // Hold a READ reply until this activity is canceled.
					<-c.closed
				} else {
					_ = c.Close() // WRITE committed, but its confirmation is lost.
				}
				return 0, net.ErrClosed
			}
			var err error
			frame, err = c.directoryPage(frame)
			if err != nil {
				return 0, err
			}
		}
		c.pending = frame
	}
	n := copy(p, c.pending)
	c.pending = c.pending[n:]
	return n, nil
}

type service struct {
	root    string
	disk    *disk
	wire    *transport
	options driver.Options
}

type armOnRead struct {
	io.Reader
	arm *atomic.Uint32
}

func (r armOnRead) Read(p []byte) (int, error) {
	r.arm.Store(6) // File creation is acknowledged before borrowing its input.
	return r.Reader.Read(p)
}

func upstream(t *testing.T, split ...bool) *service {
	t.Helper()
	root := t.TempDir()
	b, err := vfs.NewLocalBackend(root)
	if err != nil {
		t.Fatal(err)
	}
	disk := &disk{LocalBackend: b}
	creds := ntlmssp.NewMemoryCredentials()
	creds.Add("", "test", "synthetic")
	ready := make(readyLog, 1)
	faults := &transport{entered: make(chan struct{}, 1)}
	if len(split) != 0 {
		faults.split = split[0]
	}
	factory := ntlmssp.NewServer(creds, "fixture")
	if faults.split {
		original := factory
		factory = func() serverauth.Authenticator { return captureAuth{original(), faults} }
	}
	options := []server.Option{server.WithAddr("127.0.0.1:0"), server.WithShares(vfs.NewDiskShare("data", disk)),
		server.WithAuth(factory), server.WithLogger(slog.New(slog.NewTextHandler(ready, nil)))}
	srv, err := server.New(options...)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.ListenAndServe(ctx) }()
	t.Cleanup(func() {
		cancel()
		_ = srv.Shutdown()
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(3 * time.Second):
			t.Error("SMB fixture did not stop")
		}
	})
	var address string
	select {
	case address = <-ready:
	case <-time.After(3 * time.Second):
		t.Fatal("SMB fixture did not listen")
	}
	faults.address = address
	domain := ""
	fixture := &service{root: root, disk: disk, wire: faults, options: driver.Options{
		Server: "fixture", Share: "data", RootPath: "/", PageBytes: 4096, BlockBytes: 4096,
		CleanupTimeout: time.Second, Dialer: &smb2.Dialer{TransportDialer: faults, RequireMessageSigning: true,
			Credentials:       auth.NTLMCredential{User: "test", Password: "synthetic", Domain: &domain},
			SpecifiedDialects: []smb2.Dialect{smb2.SMB302}, DisableAAPLExtension: true},
	}}
	return fixture
}

func mount(t *testing.T, options driver.Options) (*core.Core, *core.Mount) {
	t.Helper()
	d, err := driver.Open(context.Background(), options)
	if err != nil {
		if d != nil {
			_ = d.Close()
		}
		t.Fatal(err)
	}
	c, err := core.New(core.Options{MaxCursors: 4, Authorize: func(context.Context, core.Principal, core.Action, core.Address) error { return nil }})
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
	return c, m
}

func TestNativeFilesAndCoreComposition(t *testing.T) {
	t.Run("native continuation", func(t *testing.T) {
		f := upstream(t, true)
		for _, name := range []string{"a", "b", "c"} {
			if err := os.WriteFile(filepath.Join(f.root, name), nil, 0600); err != nil {
				t.Fatal(err)
			}
		}
		c, _ := mount(t, f.options)
		r := core.ListRequest{Query: core.Children(core.Path("/remote")), CursorTTL: time.Minute, Limit: 1}
		count := 0
		for {
			p, err := c.List(context.Background(), "reader", r)
			if err != nil {
				t.Fatal(err)
			}
			count += len(p.Items)
			if p.NextCursor == nil {
				break
			}
			r.Cursor = *p.NextCursor
		}
		if count != 3 || f.wire.continued.Load() != 1 {
			t.Fatalf("count=%d continuation=%d nativepages=%d", count, f.wire.continued.Load(), f.wire.pages.Load())
		}
	})
	f := upstream(t)
	payload := bytes.Repeat([]byte("0123456789abcdef"), 8193)
	if err := os.WriteFile(filepath.Join(f.root, "Exact"), payload, 0600); err != nil {
		t.Fatal(err)
	}
	c, m := mount(t, f.options)
	ctx := context.Background()
	if err := c.BindAlias(ctx, "/alias", m, "/"); err != nil {
		t.Fatal(err)
	}
	before := f.disk.fileOpens.Load()
	if _, err := c.Stat(ctx, "reader", core.Path("/alias/exact")); core.CodeOf(err) != core.NotFound || f.disk.fileOpens.Load() != before {
		t.Fatalf("request spelling became stored name: %v", err)
	}
	opened, err := c.Open(ctx, "reader", core.Path("/alias/Exact"), core.OpenOptions{Range: &core.Range{Start: 3, End: 65541}})
	if err != nil {
		t.Fatal(err)
	}
	got, readErr := io.ReadAll(opened.Body)
	if err := errors.Join(readErr, opened.Body.Close()); err != nil || !bytes.Equal(got, payload[3:65542]) || opened.Range == nil || opened.Range.Total != int64(len(payload)) {
		t.Fatalf("actual range: bytes=%d %+v %v", len(got), opened.Range, err)
	}
	if got := f.disk.literalLookups.Load(); got != 2 {
		t.Fatalf("exact path selection used %d native name-filtered lookups, want 2", got)
	}
	if change, err := c.Mkdir(ctx, "writer", core.Path("/remote/folder")); err != nil || change.Effect != core.Applied {
		t.Fatalf("mkdir: %+v %v", change, err)
	}
	if change, err := c.Write(ctx, "writer", core.Path("/remote/unknown"), strings.NewReader("content"), nil); err != nil || change.Effect != core.Applied {
		t.Fatalf("unknown length: %+v %v", change, err)
	}
	request := core.ListRequest{Query: core.Children(core.Path("/alias")), CursorTTL: time.Minute, Limit: 1}
	count := 0
	for {
		page, err := c.List(ctx, "reader", request)
		if err != nil {
			t.Fatal(err)
		}
		count += len(page.Items)
		if page.NextCursor == nil {
			break
		}
		request.Cursor = *page.NextCursor
	}
	if count != 3 {
		t.Fatalf("listed %d entries", count)
	}
	destination := t.TempDir()
	ld, err := local.New(destination)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Install("/local", ld); err != nil {
		t.Fatal(err)
	}
	for _, pair := range [][2]string{{"/remote/Exact", "/local/copied"}, {"/local/copied", "/remote/copied"}, {"/remote/copied", "/remote/copied-again"}} {
		if change, err := c.Copy(ctx, "writer", core.Path(pair[0]), core.Path(pair[1])); err != nil || change.Effect != core.Applied {
			t.Fatalf("copy %v: %+v %v", pair, change, err)
		}
	}
	if got, err := os.ReadFile(filepath.Join(f.root, "copied-again")); err != nil || !bytes.Equal(got, payload) {
		t.Fatalf("copied content: %d %v", len(got), err)
	}
	before = f.disk.pages.Load()
	if _, err := c.Open(ctx, "reader", core.Path("/remote/Exact"), core.OpenOptions{NoFollow: true}); core.CodeOf(err) != core.UnsupportedOperation || f.disk.pages.Load() != before {
		t.Fatalf("NoFollow performed I/O: %v", err)
	}
	if change, err := c.Write(ctx, "writer", core.Path("/remote/bad:stream"), strings.NewReader("unread"), nil); core.CodeOf(err) != core.UnrepresentableName || change.Effect != core.Unchanged {
		t.Fatalf("ADS accepted as a filename: %+v %v", change, err)
	}
}

func TestExclusivePartialAndUnknownEffects(t *testing.T) {
	f := upstream(t)
	c, _ := mount(t, f.options)
	competitor, _ := mount(t, f.options)
	ctx := context.Background()
	start := make(chan struct{})
	type outcome struct {
		change core.Change
		err    error
	}
	results := make(chan outcome, 2)
	for _, files := range []*core.Core{c, competitor} {
		go func() {
			<-start
			change, err := files.Write(ctx, "writer", core.Path("/remote/race"), strings.NewReader("winner"), nil)
			results <- outcome{change, err}
		}()
	}
	close(start)
	a, b := <-results, <-results
	if a.err != nil {
		a, b = b, a
	}
	if a.err != nil || a.change.Effect != core.Applied || core.CodeOf(b.err) != core.AlreadyExists || b.change.Effect != core.Unchanged {
		t.Fatalf("native exclusion: %+v %+v", a, b)
	}
	cause := errors.New("synthetic input tail failure")
	change, err := c.Write(ctx, "writer", core.Path("/remote/partial"), io.MultiReader(strings.NewReader("unconfirmed"), iotest.ErrReader(cause)), nil)
	if !errors.Is(err, cause) || change.Effect != core.Incomplete {
		t.Fatalf("input effect: %+v %v", change, err)
	}
	if _, err := os.Stat(filepath.Join(f.root, "partial")); err != nil {
		t.Fatalf("partial target was removed: %v", err)
	}
	zero := int64(0)
	if change, err := c.Write(ctx, "writer", core.Path("/remote/empty"), strings.NewReader(""), &zero); err != nil || change.Effect != core.Applied {
		t.Fatalf("empty input: %+v %v", change, err)
	}
	f.wire.arm.Store(9)
	change, err = c.Write(ctx, "writer", core.Path("/remote/unknown"), strings.NewReader("committed"), nil)
	if err == nil || change.Effect != core.Unknown {
		t.Fatalf("lost WRITE receipt: %+v %v", change, err)
	}
	if got, err := os.ReadFile(filepath.Join(f.root, "unknown")); err != nil || string(got) != "committed" {
		t.Fatalf("unconfirmed native effect: %q %v", got, err)
	}
	change, err = c.Write(ctx, "writer", core.Path("/remote/close-unknown"), armOnRead{strings.NewReader("complete"), &f.wire.arm}, nil)
	if err == nil || change.Effect != core.Unknown {
		t.Fatalf("lost CLOSE receipt: %+v %v", change, err)
	}
	if got, err := os.ReadFile(filepath.Join(f.root, "close-unknown")); err != nil || string(got) != "complete" {
		t.Fatalf("closed native effect: %q %v", got, err)
	}
}

func TestCancellationCapacityAndReturnedOwners(t *testing.T) {
	f := upstream(t)
	if err := os.WriteFile(filepath.Join(f.root, "data"), []byte("healthy"), 0600); err != nil {
		t.Fatal(err)
	}
	f.options.MaxSessions = 2
	c, _ := mount(t, f.options)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	first, err := c.Open(ctx, "reader", core.Path("/remote/data"), core.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	other, err := c.Open(context.Background(), "reader", core.Path("/remote/data"), core.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Stat(context.Background(), "reader", core.Path("/remote/data")); core.CodeOf(err) != core.CapacityExceeded {
		t.Fatalf("occupied sessions waited or escaped capacity: %v", err)
	}
	f.wire.arm.Store(8)
	done := make(chan error, 1)
	go func() { _, err := io.ReadAll(first.Body); done <- err }()
	select {
	case <-f.wire.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("read did not reach its reply")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancellation cause lost: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("canceled read did not stop")
	}
	got, readErr := io.ReadAll(other.Body)
	if err := errors.Join(readErr, other.Body.Close()); err != nil || string(got) != "healthy" {
		t.Fatalf("cancellation affected another activity: %q %v", got, err)
	}
	next, err := c.Open(context.Background(), "reader", core.Path("/remote/data"), core.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := other.Body.Close(); err != nil {
		t.Fatal(err)
	}
	got, readErr = io.ReadAll(next.Body)
	if err := errors.Join(readErr, next.Body.Close()); err != nil || string(got) != "healthy" {
		t.Fatalf("old owner affected successor: %q %v", got, err)
	}
	_ = first.Body.Close()
}
