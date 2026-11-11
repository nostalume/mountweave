package sftp_test

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/iotest"
	"time"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/local"
	driver "github.com/nostalume/mountweave/drivers/sftp"
	export "github.com/nostalume/mountweave/protocols/sftp"
	sshfx "github.com/pkg/sftp/v2/encoding/ssh/filexfer"
	"golang.org/x/crypto/ssh"
)

func files(t *testing.T, authorize core.Authorizer) *core.Core {
	t.Helper()
	if authorize == nil {
		authorize = func(context.Context, core.Principal, core.Action, core.Address) error { return nil }
	}
	c, err := core.New(core.Options{MaxCursors: 4, Authorize: authorize})
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
	return c
}

// The SSH envelope is real; file behavior comes from the existing export and
// Local Driver. No second SFTP test server or remote account is involved.
type service struct {
	root        string
	options     driver.Options
	connections atomic.Int64
	requests    atomic.Int64
	reorder     atomic.Bool
	statusFault atomic.Int32
	closes      atomic.Int64
	alias       atomic.Bool
	opens       atomic.Int64
}

func upstream(t *testing.T, authorize core.Authorizer) *service {
	t.Helper()
	f := &service{root: t.TempDir()}
	c := files(t, func(ctx context.Context, p core.Principal, action core.Action, address core.Address) error {
		f.requests.Add(1)
		if authorize != nil {
			return authorize(ctx, p, action, address)
		}
		return nil
	})
	d, err := local.New(f.root)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = c.Install("/files", d); err != nil {
		t.Fatal(err)
	}
	s, err := export.New(c, export.Options{Root: core.Path("/files"), CursorTTL: time.Minute, ModeView: export.ReadWriteView,
		Limits: export.Limits{Handles: 4, PageEntries: 1, PageNameBytes: 4096}})
	if err != nil {
		t.Fatal(err)
	}
	key, err := ssh.NewSignerFromKey(ed25519.NewKeyFromSeed(make([]byte, ed25519.SeedSize)))
	if err != nil {
		t.Fatal(err)
	}
	config := &ssh.ServerConfig{PasswordCallback: func(conn ssh.ConnMetadata, password []byte) (*ssh.Permissions, error) {
		if conn.User() != "account" || string(password) != "secret" {
			return nil, errors.New("invalid synthetic credentials")
		}
		return nil, nil
	}}
	config.AddHostKey(key)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	f.options = driver.Options{Address: listener.Addr().String(), RootPath: "/", MaxConnections: 4, MaxRequests: 4, MaxBufferedBytes: 1 << 20,
		SSHConfig: &ssh.ClientConfig{User: "account", Auth: []ssh.AuthMethod{ssh.Password("secret")}, HostKeyCallback: ssh.FixedHostKey(key.PublicKey())}}
	var mu sync.Mutex
	connections := make(map[net.Conn]struct{})
	var workers sync.WaitGroup
	workers.Add(1)
	go func() {
		defer workers.Done()
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			f.connections.Add(1)
			mu.Lock()
			connections[conn] = struct{}{}
			mu.Unlock()
			workers.Add(1)
			go func() {
				defer workers.Done()
				defer conn.Close()
				defer func() { mu.Lock(); delete(connections, conn); mu.Unlock() }()
				server, channels, requests, err := ssh.NewServerConn(conn, config)
				if err != nil {
					return
				}
				defer server.Close()
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				go func() { _ = server.Wait(); cancel() }()
				go ssh.DiscardRequests(requests)
				for incoming := range channels {
					if incoming.ChannelType() != "session" {
						_ = incoming.Reject(ssh.UnknownChannelType, "session required")
						continue
					}
					channel, requests, err := incoming.Accept()
					if err != nil {
						return
					}
					workers.Add(1)
					go func() {
						defer workers.Done()
						defer channel.Close()
						for request := range requests {
							var subsystem struct{ Name string }
							ok := request.Type == "subsystem" && ssh.Unmarshal(request.Payload, &subsystem) == nil && subsystem.Name == "sftp"
							_ = request.Reply(ok, nil)
							if ok {
								_ = s.ServeConn(ctx, "writer", &replies{ReadWriteCloser: channel, reorder: &f.reorder, fault: &f.statusFault, closes: &f.closes, alias: &f.alias, opens: &f.opens})
								return
							}
						}
					}()
				}
			}()
		}
	}()
	t.Cleanup(func() {
		_ = listener.Close()
		mu.Lock()
		for conn := range connections {
			_ = conn.Close()
		}
		mu.Unlock()
		done := make(chan struct{})
		go func() { workers.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Error("SSH fixture retained an activity")
		}
	})
	return f
}

// Reorder the first two DATA replies and shorten one, without replacing the
// server. Reaching the second reply before the first also checks pipelining.
type replies struct {
	io.ReadWriteCloser
	reorder  *atomic.Bool
	fault    *atomic.Int32
	closes   *atomic.Int64
	alias    *atomic.Bool
	opens    *atomic.Int64
	requests []byte
	incoming []byte
	pending  []byte
	done     bool
}

func (r *replies) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if len(r.requests) == 0 {
		var header [4]byte
		if _, err := io.ReadFull(r.ReadWriteCloser, header[:]); err != nil {
			return 0, err
		}
		r.requests = make([]byte, 4+int(binary.BigEndian.Uint32(header[:])))
		copy(r.requests, header[:])
		if _, err := io.ReadFull(r.ReadWriteCloser, r.requests[4:]); err != nil {
			return 0, err
		}
		if r.requests[4] == 4 {
			r.closes.Add(1)
		}
		if r.requests[4] == 3 {
			r.opens.Add(1)
		}
		if r.alias.Load() && len(r.requests) >= 13 && (r.requests[4] == 3 || r.requests[4] == 17) {
			end := 13 + int(binary.BigEndian.Uint32(r.requests[9:13]))
			if end <= len(r.requests) && string(r.requests[13:end]) == "/root/EXACT" {
				copy(r.requests[13:end], "/root/Exact")
			}
		}
	}
	n := copy(p, r.requests)
	r.requests = r.requests[n:]
	return n, nil
}

func (r *replies) Write(p []byte) (int, error) {
	r.incoming = append(r.incoming, p...)
	for len(r.incoming) >= 4 {
		length := int(binary.BigEndian.Uint32(r.incoming)) + 4
		if len(r.incoming) < length {
			break
		}
		if _, err := r.packet(r.incoming[:length]); err != nil {
			return 0, err
		}
		r.incoming = r.incoming[length:]
	}
	return len(p), nil
}

func (r *replies) packet(p []byte) (int, error) {
	if len(p) >= 13 && p[4] == 101 && binary.BigEndian.Uint32(p[9:13]) == 0 {
		switch r.fault.Swap(0) {
		case 1:
			message := "synthetic CLOSE refusal"
			failure := make([]byte, 21+len(message))
			binary.BigEndian.PutUint32(failure, uint32(len(failure)-4))
			failure[4] = 101
			copy(failure[5:9], p[5:9])
			binary.BigEndian.PutUint32(failure[9:13], 4)
			binary.BigEndian.PutUint32(failure[13:17], uint32(len(message)))
			copy(failure[17:], message)
			if _, err := r.ReadWriteCloser.Write(failure); err != nil {
				return 0, err
			}
			return len(p), nil
		case 2:
			return len(p), nil
		}
	}
	if !r.done && r.reorder.Load() && len(p) >= 13 && p[4] == 103 {
		if r.pending == nil {
			r.pending = append([]byte(nil), p...)
			if size := binary.BigEndian.Uint32(p[9:13]); size > 1 {
				r.pending = r.pending[:13+size/2]
				binary.BigEndian.PutUint32(r.pending, uint32(len(r.pending)-4))
				binary.BigEndian.PutUint32(r.pending[9:13], size/2)
			}
			return len(p), nil
		}
		r.done = true
		for _, frame := range [][]byte{p, r.pending} {
			if n, err := r.ReadWriteCloser.Write(frame); err != nil || n != len(frame) {
				return 0, errors.Join(err, io.ErrShortWrite)
			}
		}
		r.pending = nil
		return len(p), nil
	}
	return r.ReadWriteCloser.Write(p)
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
	c := files(t, nil)
	m, err := c.Install("/remote", d)
	if err != nil {
		t.Fatal(err)
	}
	return c, m
}

func TestSourceThroughExistingExport(t *testing.T) {
	f := upstream(t, nil)
	payload := bytes.Repeat([]byte("0123456789abcdef"), 8193)
	if err := os.WriteFile(filepath.Join(f.root, "space % &"), payload, 0600); err != nil {
		t.Fatal(err)
	}
	c, m := mount(t, f.options)
	ctx := context.Background()
	if err := c.BindAlias(ctx, "/alias", m, "/"); err != nil {
		t.Fatal(err)
	}
	connections := f.connections.Load()
	for i := 0; i < 2; i++ {
		entry, err := c.Stat(ctx, "reader", core.Path("/alias/space % &"))
		if err != nil || entry.Kind != core.File || entry.Size == nil || *entry.Size != int64(len(payload)) {
			t.Fatalf("stat: %+v %v", entry, err)
		}
	}
	if f.connections.Load() != connections {
		t.Fatal("metadata did not reuse the idle connection")
	}
	if _, err := c.Stat(ctx, "reader", core.Path("/remote/missing")); core.CodeOf(err) != core.NotFound {
		t.Fatalf("native missing status: %v", err)
	}
	f.reorder.Store(true)
	opened, err := c.Open(ctx, "reader", core.Path("/alias/space % &"), core.OpenOptions{Range: &core.Range{Start: 32760, End: 98500}})
	if err != nil {
		t.Fatal(err)
	}
	data, readErr := io.ReadAll(opened.Body)
	if err := errors.Join(readErr, opened.Body.Close()); err != nil || !bytes.Equal(data, payload[32760:98501]) || opened.Range == nil {
		t.Fatalf("actual range: bytes=%d range=%+v error=%v", len(data), opened.Range, err)
	}
	if changed, err := c.Mkdir(ctx, "writer", core.Path("/remote/folder")); err != nil || changed.Effect != core.Applied {
		t.Fatalf("mkdir: %+v %v", changed, err)
	}
	beforeOpen := f.opens.Load()
	if _, err := c.Open(ctx, "reader", core.Path("/remote/folder"), core.OpenOptions{}); core.CodeOf(err) != core.NotFile || f.opens.Load() != beforeOpen {
		t.Fatalf("known directory reached native file open: %v", err)
	}
	if _, err := c.List(ctx, "reader", core.ListRequest{Query: core.Children(core.Path("/remote/space % &")), Limit: 1, CursorTTL: time.Minute}); core.CodeOf(err) != core.NotDirectory {
		t.Fatalf("listing a known file: %v", err)
	}
	for _, tc := range []struct {
		name  string
		data  []byte
		known bool
	}{{"known", payload, true}, {"unknown", payload, false}, {"empty", nil, true}} {
		length := int64(len(tc.data))
		var size *int64
		if tc.known {
			size = &length
		}
		address := core.Path("/remote/folder/" + tc.name)
		if changed, err := c.Write(ctx, "writer", address, bytes.NewReader(tc.data), size); err != nil || changed.Effect != core.Applied {
			t.Fatalf("%s write: %+v %v", tc.name, changed, err)
		}
		if got, err := os.ReadFile(filepath.Join(f.root, "folder", tc.name)); err != nil || !bytes.Equal(got, tc.data) {
			t.Fatalf("%s target: bytes=%d error=%v", tc.name, len(got), err)
		}
	}
	if changed, err := c.Write(ctx, "writer", core.Path("/remote/folder/known"), strings.NewReader("replace"), nil); err == nil || changed.Effect == core.Applied {
		t.Fatalf("exclusive open: %+v %v", changed, err)
	}
	if got, err := os.ReadFile(filepath.Join(f.root, "folder", "known")); err != nil || !bytes.Equal(got, payload) {
		t.Fatal("exclusive failure changed existing content", err)
	}
	tail := errors.New("synthetic source tail failure")
	input := io.MultiReader(strings.NewReader("partial"), iotest.ErrReader(tail))
	if changed, err := c.Write(ctx, "writer", core.Path("/remote/failed-input"), input, nil); changed.Effect != core.Incomplete || !errors.Is(err, tail) {
		t.Fatalf("input failure lost visible creation or cause: %+v %v", changed, err)
	}
	if info, err := os.Stat(filepath.Join(f.root, "failed-input")); err != nil || info.Size() != 0 {
		t.Fatalf("failed input was removed or its invalid window sent: %v %v", info, err)
	}
	page, err := c.List(ctx, "reader", core.ListRequest{Query: core.Children(core.Path("/remote/folder")), Limit: 1, CursorTTL: time.Minute})
	listed := len(page.Items)
	for err == nil && page.NextCursor != nil {
		request := core.ListRequest{Query: core.Children(core.Path("/remote/folder")), Limit: 1, Cursor: *page.NextCursor}
		page, err = c.List(ctx, "reader", request)
		listed += len(page.Items)
	}
	if err != nil || listed != 3 {
		t.Fatalf("paged listing: count=%d error=%v", listed, err)
	}
	if result, err := c.Copy(ctx, "writer", core.Path("/remote/space % &"), core.Path("/remote/copied")); err != nil || result.Effect != core.Applied {
		t.Fatalf("same-instance copy: %+v %v", result, err)
	}
	if got, err := os.ReadFile(filepath.Join(f.root, "copied")); err != nil || !bytes.Equal(got, payload) {
		t.Fatal("copy content", err)
	}
	before := f.requests.Load()
	if _, err := c.Open(ctx, "reader", core.Path("/remote/copied"), core.OpenOptions{NoFollow: true}); core.CodeOf(err) != core.UnsupportedOperation || f.requests.Load() != before {
		t.Fatalf("unproven mode performed I/O: %v", err)
	}
	if err := m.Retire(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Stat(ctx, "reader", core.Path("/remote/copied")); err == nil {
		t.Fatal("retired mount still admitted work")
	}
}

func TestCloseReceiptsAndReturnedOwners(t *testing.T) {
	f := upstream(t, nil)
	options := f.options
	options.MaxConnections, options.CleanupTimeout = 1, 50*time.Millisecond
	d, err := driver.Open(context.Background(), options)
	if err != nil {
		t.Fatal(err)
	}
	c := files(t, nil)
	if _, err := c.Install("/remote", d); err != nil {
		t.Fatal(err)
	}
	zero := int64(0)
	for mode, name := range []string{"refused", "lost"} {
		closes, connections := f.closes.Load(), f.connections.Load()
		f.statusFault.Store(int32(mode + 1))
		changed, err := c.Write(context.Background(), "writer", core.Path("/remote/"+name), strings.NewReader(""), &zero)
		if changed.Effect != core.Unknown || err == nil || f.closes.Load() != closes+1 {
			t.Fatalf("%s CLOSE: effect=%v calls=%d error=%v", name, changed.Effect, f.closes.Load()-closes, err)
		}
		if mode == 0 {
			var status *sshfx.StatusPacket
			if !errors.As(err, &status) || status.StatusCode != sshfx.StatusFailure || !strings.Contains(err.Error(), "synthetic CLOSE refusal") {
				t.Fatalf("native close refusal lost: %v", err)
			}
		} else if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("close timeout lost: %v", err)
		}
		if changed, err := c.Write(context.Background(), "writer", core.Path("/remote/after-"+name), strings.NewReader(""), &zero); err != nil || changed.Effect != core.Applied || f.connections.Load() <= connections {
			t.Fatalf("new operation did not replace failed connection: %+v %v", changed, err)
		}
	}
	checkReuse := func(old io.Closer) {
		t.Helper()
		if err := old.Close(); err != nil {
			t.Fatal(err)
		}
		active, err := d.Open(context.Background(), core.NativePath("/after-lost"), core.OpenOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if err := old.Close(); err != nil {
			t.Fatal("returned owner closed its successor", err)
		}
		_, readErr := io.ReadAll(active.Body)
		if err := errors.Join(readErr, active.Body.Close()); err != nil {
			t.Fatal("successor connection was invalidated", err)
		}
	}
	opened, err := d.Open(context.Background(), core.NativePath("/after-lost"), core.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	checkReuse(opened.Body)
	iterator, err := d.Enumerate(context.Background(), core.NativeChildren(core.NativePath("/")))
	if err != nil {
		t.Fatal(err)
	}
	checkReuse(iterator)
}

func TestExactNamesAgainstCaseInsensitiveRequests(t *testing.T) {
	f := upstream(t, nil)
	if err := os.Mkdir(filepath.Join(f.root, "root"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(f.root, "root", "Exact"), []byte("content"), 0600); err != nil {
		t.Fatal(err)
	}
	// Simulate a server accepting alternate case in STAT/OPEN, while READDIR
	// remains authoritative about the stored name. Request/response IDs stay real.
	f.alias.Store(true)
	options := f.options
	options.RootPath = "/root"
	c, _ := mount(t, options)
	if _, err := c.Stat(context.Background(), "reader", core.Path("/remote/Exact")); err != nil {
		t.Fatal(err)
	}
	before := f.opens.Load()
	if _, err := c.Stat(context.Background(), "reader", core.Path("/remote/EXACT")); core.CodeOf(err) != core.NotFound {
		t.Errorf("STAT accepted an alternate stored spelling: %v", err)
	}
	opened, err := c.Open(context.Background(), "reader", core.Path("/remote/EXACT"), core.OpenOptions{})
	if opened.Body != nil {
		_ = opened.Body.Close()
	}
	if core.CodeOf(err) != core.NotFound || f.opens.Load() != before {
		t.Errorf("alternate spelling reached native OPEN: calls=%d error=%v", f.opens.Load()-before, err)
	}
}

func TestCancellationAndCapacityAreIsolated(t *testing.T) {
	var blocked atomic.Bool
	started := make(chan struct{})
	var once sync.Once
	f := upstream(t, func(ctx context.Context, _ core.Principal, action core.Action, address core.Address) error {
		if blocked.Load() && action == core.ReadAction && address == core.Path("/files/block") {
			once.Do(func() { close(started) })
			<-ctx.Done()
			return ctx.Err()
		}
		return nil
	})
	for _, name := range []string{"block", "other"} {
		if err := os.WriteFile(filepath.Join(f.root, name), []byte("content"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	c, _ := mount(t, f.options)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	first, err := c.Open(ctx, "reader", core.Path("/remote/block"), core.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	other, err := c.Open(context.Background(), "reader", core.Path("/remote/other"), core.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	blocked.Store(true)
	done := make(chan error, 1)
	go func() { _, err := first.Body.Read(make([]byte, 4)); done <- err }()
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("read did not reach native request")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancellation lost cause: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("cancellation did not join the active read")
	}
	_ = first.Body.Close()
	data, readErr := io.ReadAll(other.Body)
	if err := errors.Join(readErr, other.Body.Close()); err != nil || string(data) != "content" {
		t.Fatalf("cancel affected independent activity: %q %v", data, err)
	}
	limited := f.options
	limited.MaxConnections = 1
	one, _ := mount(t, limited)
	call, end := context.WithTimeout(context.Background(), time.Second)
	defer end()
	changed, err := one.Copy(call, "writer", core.Path("/remote/other"), core.Path("/remote/never-created"))
	if core.CodeOf(err) != core.CapacityExceeded || changed.Effect != core.Unchanged {
		t.Fatalf("held source must not wait for target capacity: %+v %v", changed, err)
	}
	if _, err := os.Stat(filepath.Join(f.root, "never-created")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("capacity refusal changed the target", err)
	}
	invalid := f.options
	invalid.MaxConnections = -1
	before := f.connections.Load()
	if d, err := driver.Open(context.Background(), invalid); err == nil || d != nil || f.connections.Load() != before {
		t.Fatalf("invalid configuration used a connection: %v %v", d, err)
	}
}
