package sftp_test

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/local"
	"github.com/nostalume/mountweave/protocols"
	adapter "github.com/nostalume/mountweave/protocols/sftp"
	client "github.com/pkg/sftp"
	sshfx "github.com/pkg/sftp/v2/encoding/ssh/filexfer"
)

func TestWireFileAndDirectoryWorkflow(t *testing.T) {
	var writes atomic.Int32
	var revokeWrite atomic.Bool
	revoked := errors.New("write policy revoked")
	c, root := fixture(t, func(_ context.Context, p core.Principal, action core.Action, address core.Address) error {
		if action == core.WriteAction {
			writes.Add(1)
			if revokeWrite.Load() {
				return revoked
			}
		}
		if p == "reader" && action != core.ReadAction && action != core.StatAction && action != core.ListAction {
			return errors.New("read-only export")
		}
		if p == "reader" && action == core.StatAction && address == core.Path("/files/renamed") {
			return errors.New("private metadata")
		}
		return nil
	})
	opts := options()
	opts.ModeView = adapter.ReadWriteView
	opts.Limits.Handles = 1
	opts.Limits.ReadBytes = 16
	failures := make(chan protocols.Failure, 8)
	opts.OnFailure = func(f protocols.Failure) {
		if code := core.CodeOf(f.Err); code == protocols.CreateModeConflict || code == core.PermissionDenied {
			failures <- f
		}
	}
	s, err := adapter.New(c, opts)
	if err != nil {
		t.Fatal(err)
	}
	remote, end := connect(t, s, context.Background(), "writer")
	if version, ok := remote.HasExtension("fsync@openssh.com"); !ok || version != "1" {
		t.Fatalf("fsync extension: version %q advertised=%v", version, ok)
	}
	f, err := remote.OpenFile("/data", os.O_CREATE|os.O_EXCL|os.O_RDWR)
	if err != nil {
		t.Fatal(err)
	}
	if extra, err := remote.Open("/data"); err == nil {
		_ = extra.Close()
		t.Fatal("handle budget ignored")
	}
	if _, err = f.WriteAt([]byte("abcdef"), 0); err != nil {
		t.Fatal(err)
	}
	change, err := c.Write(context.Background(), "writer", core.Path("/files/data"), strings.NewReader("new"), nil, core.WriteOptions{Policy: core.Publish()})
	if err != nil || change.Effect != core.Applied {
		t.Fatal(change, err)
	}
	info, err := f.Stat()
	if err != nil || info.Size() != 6 || !info.Mode().IsRegular() || info.Mode().Perm() != 0600 {
		t.Fatalf("rebound FSTAT: %v %v", info, err)
	}
	buf := make([]byte, 8)
	if _, err := f.ReadAt(make([]byte, 32), 0); err == nil || errors.Is(err, io.EOF) {
		t.Fatal("oversized read became data/EOF", err)
	}
	n, err := f.ReadAt(buf, 0)
	if n != 6 || err != io.EOF || string(buf[:n]) != "abcdef" {
		t.Fatalf("old handle: %q %v", buf[:n], err)
	}
	if _, err = f.ReadAt(buf[:1], 0); err != nil {
		t.Fatal("EOF closed handle:", err)
	}
	if err = f.Truncate(2); err != nil {
		t.Fatal(err)
	}
	if info, err = f.Stat(); err != nil || info.Size() != 2 {
		t.Fatal(info, err)
	}
	if err = f.Sync(); err != nil {
		t.Fatal("sync after path replacement:", err)
	}
	revokeWrite.Store(true)
	if err = f.Sync(); !errors.Is(err, os.ErrPermission) {
		t.Fatal("sync bypassed current write authorization:", err)
	}
	failure := <-failures
	if failure.Phase != "sync" || failure.HasChange || !errors.Is(failure.Err, revoked) {
		t.Fatal("sync lost the original failure:", failure)
	}
	revokeWrite.Store(false)
	if err = f.Close(); err != nil {
		t.Fatal(err)
	}
	current, err := os.ReadFile(root + "/data")
	if err != nil || string(current) != "new" {
		t.Fatal(string(current), err)
	}
	request, finish := rawConnection(t, s)
	defer finish()
	attrs := sshfx.Attributes{}
	attrs.SetPermissions(0777)
	r := request(&sshfx.MkdirPacket{Path: "/folder", Attrs: attrs})
	var status sshfx.StatusPacket
	if r.Type() != sshfx.PacketTypeStatus || status.UnmarshalPacketBody(&r.Data) != nil || status.StatusCode != sshfx.StatusOK {
		t.Fatal("compatible mkdir:", r.Type(), status)
	}
	for _, tc := range []struct {
		packet sshfx.PacketMarshaller
		reason string
	}{
		{&sshfx.RemovePacket{Path: "/folder"}, "Target is not a file"},
		{&sshfx.RmdirPacket{Path: "/data"}, "Target is not a directory"},
	} {
		r := request(tc.packet)
		if r.Type() != sshfx.PacketTypeStatus || status.UnmarshalPacketBody(&r.Data) != nil || status.StatusCode != sshfx.StatusFailure ||
			!strings.Contains(status.ErrorMessage, tc.reason) || !strings.Contains(status.ErrorMessage, "No changes made") {
			t.Fatal("typed deletion:", r.Type(), status)
		}
	}
	attrs.SetPermissions(0)
	before := writes.Load()
	r = request(&sshfx.OpenPacket{Filename: "/data", PFlags: sshfx.FlagWrite | sshfx.FlagCreate | sshfx.FlagTruncate, Attrs: attrs})
	if r.Type() != sshfx.PacketTypeStatus || status.UnmarshalPacketBody(&r.Data) != nil || status.StatusCode != sshfx.StatusOpUnsupported ||
		!strings.Contains(status.ErrorMessage, "Create mode conflicts") || !strings.Contains(status.ErrorMessage, "No changes made") {
		t.Fatal("conflicting open:", r.Type(), status)
	}
	failure = <-failures
	if core.CodeOf(failure.Err) != protocols.CreateModeConflict || failure.Phase != "request" || !failure.HasChange ||
		failure.Change.Effect != core.Unchanged || writes.Load() != before {
		t.Fatal("conflict reached Core or lost its receipt:", failure)
	}
	if data, err := os.ReadFile(root + "/data"); err != nil || string(data) != "new" {
		t.Fatal("conflicting open truncated the old target:", string(data), err)
	}
	attrs.SetPermissions(0644)
	r = request(&sshfx.OpenPacket{Filename: "/created", PFlags: sshfx.FlagWrite | sshfx.FlagCreate | sshfx.FlagExclusive, Attrs: attrs})
	if r.Type() != sshfx.PacketTypeHandle {
		t.Fatal("compatible open:", r.Type())
	}
	finish()
	if info, err := remote.Stat("/folder"); err != nil || !info.IsDir() || info.Mode().Perm() != 0700 {
		t.Fatal("directory STAT:", info, err)
	}
	if err = remote.Rename("/data", "/renamed"); err != nil {
		t.Fatal(err)
	}
	if info, err := remote.Lstat("/folder"); err != nil || !info.IsDir() {
		t.Fatal("directory LSTAT:", info, err)
	}
	if info, err := remote.Lstat("/renamed"); err != nil || !info.Mode().IsRegular() || info.Size() != 3 {
		t.Fatal("regular LSTAT:", info, err)
	}
	entries, err := remote.ReadDir("/")
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, entry := range entries {
		if entry.Name() == "folder" && (!entry.IsDir() || entry.Mode().Perm() != 0700) {
			t.Fatal("directory READDIR:", entry)
		}
		names = append(names, entry.Name())
	}
	sort.Strings(names)
	if strings.Join(names, ",") != "created,folder,renamed" {
		t.Fatal(names)
	}
	for _, operation := range []func() error{
		func() error { return remote.Chmod("/renamed", 0600) },
		func() error { return remote.Remove("/RENAMED") },
		func() error { _, err := remote.Stat("../outside"); return err },
	} {
		if err := operation(); err == nil {
			t.Fatal("unsupported operation succeeded")
		}
	}
	// A failed request leaves the channel usable; a read-only handle survives EOF.
	f, err = remote.Open("/renamed")
	if err != nil {
		t.Fatal(err)
	}
	if n, err := f.ReadAt(buf[:3], 0); err != nil || n != 3 || string(buf[:n]) != "new" {
		t.Fatalf("download: %q %v", buf[:n], err)
	}
	if _, err = f.WriteAt([]byte("x"), 0); err == nil {
		t.Fatal("read-only write succeeded")
	}
	var syncStatus *client.StatusError
	if err := f.Sync(); !errors.As(err, &syncStatus) || syncStatus.FxCode() != client.ErrSSHFxOpUnsupported {
		t.Fatal("read-only sync:", err)
	}
	if err = f.Close(); err != nil {
		t.Fatal(err)
	}
	if err := remote.Remove("/created"); err != nil {
		t.Fatal("remove file:", err)
	}
	if err := remote.Mkdir("/empty"); err != nil {
		t.Fatal(err)
	}
	if err := remote.RemoveDirectory("/empty"); err != nil {
		t.Fatal("remove empty directory:", err)
	}
	end()
	t.Run("subtree links", func(t *testing.T) {
		if err := os.Symlink("..", root+"/folder/parent"); err != nil {
			if runtime.GOOS == "windows" && (errors.Is(err, os.ErrPermission) || errors.Is(err, syscall.Errno(1314))) {
				t.Skipf("symlink creation unavailable: %v", err)
			}
			t.Fatal(err)
		}
		if err := os.Symlink("missing", root+"/folder/dangling"); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(root+"/folder/item", []byte("inside"), 0600); err != nil {
			t.Fatal(err)
		}
		nested := opts
		nested.Root = core.Path("/files/folder")
		s, err := adapter.New(c, nested)
		if err != nil {
			t.Fatal(err)
		}
		r, _ := connect(t, s, context.Background(), "writer")
		if info, err := r.Lstat("/"); err != nil || !info.IsDir() {
			t.Fatal("export root:", info, err)
		}
		if err := r.RemoveDirectory("/"); !errors.Is(err, os.ErrPermission) {
			t.Fatal("export root removal:", err)
		}
		<-failures // Protected-root diagnostic, before the read-only checks below.
		if err := r.Remove("/parent/renamed"); err == nil {
			t.Fatal("remove crossed the link")
		}
		if info, err := r.Lstat("/dangling"); err != nil || info.Mode()&os.ModeSymlink == 0 {
			t.Fatal("dangling link leaf:", info, err)
		}
		for _, name := range []string{"../renamed", "/parent/renamed"} {
			if info, err := r.Lstat(name); err == nil {
				t.Fatal("observed outside export:", name, info)
			}
		}
		if info, err := r.Lstat("/item"); err != nil || !info.Mode().IsRegular() {
			t.Fatal("observe before replacement:", info, err)
		}
		if err := os.Remove(root + "/folder/item"); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink("../renamed", root+"/folder/item"); err != nil {
			t.Fatal(err)
		}
		if f, err := r.Open("/item"); err == nil {
			_ = f.Close()
			t.Fatal("open followed replacement link outside export")
		}
		if err := r.Remove("/item"); err != nil {
			t.Fatal("unlink final link:", err)
		}
		if data, err := os.ReadFile(root + "/renamed"); err != nil || string(data) != "new" {
			t.Fatal("unlink changed the target:", string(data), err)
		}
	})
	opts.ModeView = adapter.ReadOnlyView
	s, err = adapter.New(c, opts)
	if err != nil {
		t.Fatal(err)
	}
	reader, done := connect(t, s, context.Background(), "reader")
	defer done()
	if info, err := reader.Stat("/folder"); err != nil || !info.IsDir() || info.Mode().Perm() != 0500 {
		t.Fatal("read-only directory:", info, err)
	}
	if _, err := reader.Lstat("/renamed"); !errors.Is(err, os.ErrPermission) {
		t.Fatal("LSTAT bypassed StatAction:", err)
	}
	if f := <-failures; f.Phase != "lstat" || f.HasChange {
		t.Fatal("LSTAT authorization receipt:", f)
	}
	if err := reader.Mkdir("/denied"); !errors.Is(err, os.ErrPermission) {
		t.Fatal("read-only create:", err)
	}
	failure = <-failures
	if core.CodeOf(failure.Err) != core.PermissionDenied || !failure.HasChange || failure.Change.Effect != core.Unchanged {
		t.Fatal("read-only failure:", failure)
	}
	if _, err := os.Stat(root + "/denied"); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("read-only create reached Local:", err)
	}
}

// The independent client omits create attributes. Send these specific requests
// through a real channel to exercise OpenSSH-style attribute-bearing frames.
func rawConnection(t *testing.T, s *adapter.Server) (func(sshfx.PacketMarshaller) sshfx.RawPacket, func()) {
	t.Helper()
	server, peer := net.Pipe()
	_ = peer.SetDeadline(time.Now().Add(5 * time.Second))
	done := make(chan error, 1)
	go func() { done <- s.ServeConn(context.Background(), "writer", server) }()
	var once sync.Once
	finish := func() {
		once.Do(func() {
			_ = peer.Close()
			if err := <-done; err != nil {
				t.Error(err)
			}
		})
	}
	t.Cleanup(finish)
	init, _ := (&sshfx.InitPacket{Version: 3}).MarshalBinary()
	if _, err := peer.Write(init); err != nil {
		t.Fatal(err)
	}
	var version sshfx.VersionPacket
	if err := version.ReadFrom(peer, nil, 4096); err != nil {
		t.Fatal(err)
	}
	var id uint32
	return func(p sshfx.PacketMarshaller) sshfx.RawPacket {
		t.Helper()
		id++
		wire, err := sshfx.ComposePacket(p.MarshalPacket(id, nil))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := peer.Write(wire); err != nil {
			t.Fatal(err)
		}
		var reply sshfx.RawPacket
		if err := reply.ReadFrom(peer, nil, 4096); err != nil || reply.RequestID != id {
			t.Fatal(reply.RequestID, id, err)
		}
		return reply
	}, finish
}

func TestCancellationClosesAnInFlightBoundFile(t *testing.T) {
	for _, disconnect := range []bool{false, true} {
		t.Run(map[bool]string{false: "cancel", true: "disconnect"}[disconnect], func(t *testing.T) {
			c, _ := fixture(t, nil)
			d, err := local.New(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			wrapped := &blockingDriver{Driver: d, started: make(chan struct{}), closed: make(chan struct{})}
			if _, err = c.Install("/blocked", wrapped); err != nil {
				t.Fatal(err)
			}
			// Create before installing the synthetic read behavior.
			if _, err = c.Write(context.Background(), "writer", core.Path("/blocked/data"), strings.NewReader("x"), nil); err != nil {
				t.Fatal(err)
			}
			o := options()
			o.Root = core.Path("/blocked")
			failures := make(chan protocols.Failure, 1)
			o.OnFailure = func(f protocols.Failure) {
				if f.Phase == "lstat" {
					failures <- f
				}
			}
			s, err := adapter.New(c, o)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			remote, finish := connect(t, s, ctx, "writer")
			_, err = remote.Lstat("/data")
			var status *client.StatusError
			if !errors.As(err, &status) || status.FxCode() != client.ErrSSHFxOpUnsupported {
				t.Fatal("missing leaf capability:", err)
			}
			select {
			case f := <-failures:
				if core.CodeOf(f.Err) != core.UnsupportedOperation || f.HasChange {
					t.Fatal("leaf capability diagnostic:", f)
				}
			default:
				t.Fatal("missing leaf capability diagnostic")
			}
			f, err := remote.Open("/data")
			if err != nil {
				t.Fatal(err)
			}
			readDone := make(chan struct{})
			go func() { defer close(readDone); _, _ = f.ReadAt(make([]byte, 1), 0) }()
			select {
			case <-wrapped.started:
			case <-time.After(3 * time.Second):
				t.Fatal("read did not start")
			}
			if disconnect {
				finish()
			} else {
				cancel()
			}
			select {
			case <-readDone:
			case <-time.After(3 * time.Second):
				t.Fatal("cancellation did not close native read")
			}
			finish()
			cancel()
		})
	}
}

func options() adapter.Options {
	return adapter.Options{Root: core.Path("/files"), CursorTTL: time.Minute, Limits: adapter.Limits{Handles: 4, PageEntries: 1, PageNameBytes: 4096}}
}

func fixture(t testing.TB, authorize core.Authorizer) (*core.Core, string) {
	t.Helper()
	root := t.TempDir()
	d, err := local.New(root, local.Options{StagingDirectory: t.TempDir(), MaxStagingBytes: 4096})
	if err != nil {
		t.Fatal(err)
	}
	if authorize == nil {
		authorize = func(context.Context, core.Principal, core.Action, core.Address) error { return nil }
	}
	c, err := core.New(core.Options{MaxCursors: 1, Authorize: authorize})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = c.Install("/files", d); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := c.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	return c, root
}

func connect(t *testing.T, s *adapter.Server, ctx context.Context, actor core.Principal) (*client.Client, func()) {
	t.Helper()
	server, peer := net.Pipe()
	done := make(chan error, 1)
	go func() { done <- s.ServeConn(ctx, actor, server) }()
	remote, err := client.NewClientPipe(peer, peer)
	if err != nil {
		t.Fatal(err)
	}
	var once sync.Once
	finish := func() {
		once.Do(func() {
			_ = remote.Close()
			select {
			case err := <-done:
				if err != nil && !errors.Is(err, ctx.Err()) {
					t.Error(err)
				}
			case <-time.After(3 * time.Second):
				t.Error("session did not finish")
			}
		})
	}
	t.Cleanup(finish)
	return remote, finish
}

// Compare the existing bound-read cost with the wire adapter on the same local
// workload. Pipe/OS/cache results are observations, never transport defaults.
func BenchmarkBoundRead32KiB(b *testing.B) {
	c, root := fixture(b, nil)
	if err := os.WriteFile(root+"/data", []byte(strings.Repeat("x", 32768)), 0600); err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	b.Run("Core", func(b *testing.B) {
		content, err := c.Open(ctx, "writer", core.Path("/files/data"), core.OpenOptions{Mode: core.BoundRead})
		if err != nil {
			b.Fatal(err)
		}
		defer content.File.Close()
		buf := make([]byte, 32768)
		b.SetBytes(int64(len(buf)))
		b.ReportAllocs()
		for b.Loop() {
			if _, err := content.File.ReadAt(ctx, buf, 0); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("SFTP", func(b *testing.B) {
		s, err := adapter.New(c, options())
		if err != nil {
			b.Fatal(err)
		}
		server, peer := net.Pipe()
		done := make(chan error, 1)
		go func() { done <- s.ServeConn(ctx, "writer", server) }()
		remote, err := client.NewClientPipe(peer, peer)
		if err != nil {
			b.Fatal(err)
		}
		defer func() {
			_ = remote.Close()
			if err := <-done; err != nil {
				b.Error(err)
			}
		}()
		file, err := remote.Open("/data")
		if err != nil {
			b.Fatal(err)
		}
		buf := make([]byte, 32768)
		b.SetBytes(int64(len(buf)))
		b.ReportAllocs()
		for b.Loop() {
			if _, err := file.ReadAt(buf, 0); err != nil {
				b.Fatal(err)
			}
		}
	})
}

type blockingDriver struct {
	core.Driver
	started, closed chan struct{}
}

func (d *blockingDriver) Create(ctx context.Context, a core.NativeAddress, r io.Reader, n *int64) (core.Change, error) {
	return d.Driver.(core.FileCreator).Create(ctx, a, r, n)
}
func (d *blockingDriver) Open(ctx context.Context, a core.NativeAddress, o core.OpenOptions) (core.Opened, error) {
	opened, err := d.Driver.Open(ctx, a, o)
	if err == nil && o.Mode == core.BoundRead {
		opened.File = &blockingFile{ReadFile: opened.File, started: d.started, closed: d.closed}
	}
	return opened, err
}

type blockingFile struct {
	core.ReadFile
	started, closed chan struct{}
	start, close    sync.Once
	err             error
}

func (f *blockingFile) ReadAt(context.Context, []byte, int64) (int, error) {
	f.start.Do(func() { close(f.started) })
	<-f.closed
	return 0, io.ErrClosedPipe
}
func (f *blockingFile) Close() error {
	f.close.Do(func() { close(f.closed); f.err = f.ReadFile.Close() })
	return f.err
}
