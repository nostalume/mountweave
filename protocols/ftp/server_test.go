package ftp_test

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http/httptest"
	"net/netip"
	"net/textproto"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/local"
	"github.com/nostalume/mountweave/protocols"
	"github.com/nostalume/mountweave/protocols/ftp"
)

func TestTransferPoliciesAndAbort(t *testing.T) {
	for _, policy := range []ftp.StorePolicy{ftp.CompletePublish, ftp.LiveEdit} {
		t.Run(fmt.Sprint(policy), func(t *testing.T) {
			root := t.TempDir()
			localDriver, err := local.New(root, local.Options{StagingDirectory: t.TempDir(), MaxStagingBytes: 4096})
			if err != nil {
				t.Fatal(err)
			}
			d := &observedLocal{Driver: localDriver, consumed: make(chan struct{})}
			c, err := core.New(core.Options{MaxCursors: 1, Authorize: func(context.Context, core.Principal, core.Action, core.Address) error { return nil }})
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
			o := ftp.Options{Root: core.Path("/files"), CursorTTL: time.Minute, StorePolicy: policy, DataConnectTimeout: 3 * time.Second,
				Limits: ftp.Limits{CommandBytes: 512, TransferBufferBytes: 32, PageEntries: 1, PageNameBytes: 4096},
				Authenticate: func(_ context.Context, login ftp.Login) (core.Principal, error) {
					if login.Username != "test" || login.Password != "synthetic" || (login.TLS != nil) != (policy == ftp.LiveEdit) {
						return "", errors.New("invalid login")
					}
					return "writer", nil
				},
				ListenPassive: func(ctx context.Context, _, _ net.Addr) (net.Listener, netip.AddrPort, error) {
					l, err := (&net.ListenConfig{}).Listen(ctx, "tcp4", "127.0.0.1:0")
					if err != nil {
						return nil, netip.AddrPort{}, err
					}
					return l, l.Addr().(*net.TCPAddr).AddrPort(), nil
				},
				DialActive: func(ctx context.Context, _ net.Addr, target netip.AddrPort) (net.Conn, error) {
					if !target.Addr().IsLoopback() {
						return nil, errors.New("fixture rejects non-loopback")
					}
					return (&net.Dialer{}).DialContext(ctx, "tcp", target.String())
				},
			}
			failures := make(chan protocols.Failure, 8)
			o.OnFailure = func(f protocols.Failure) { failures <- f }
			// Reuse the standard loopback certificate fixture for explicit control
			// and protected data TLS. No external service or real credential is used.
			if policy == ftp.LiveEdit {
				certificate := httptest.NewTLSServer(nil)
				t.Cleanup(certificate.Close)
				o.TLSConfig = &tls.Config{Certificates: certificate.TLS.Certificates}
			}
			clientTLS := &tls.Config{InsecureSkipVerify: true} // Synthetic loopback certificate only.
			s, err := ftp.New(c, o)
			if err != nil {
				t.Fatal(err)
			}
			peer, server := net.Pipe()
			_ = peer.SetDeadline(time.Now().Add(15 * time.Second))
			done := make(chan error, 1)
			ctx, cancel := context.WithCancel(context.Background())
			go func() { done <- s.ServeConn(ctx, server) }()
			control := textproto.NewConn(peer)
			t.Cleanup(func() {
				cancel()
				_ = peer.Close()
				select {
				case err := <-done:
					if err != nil && !errors.Is(err, context.Canceled) {
						t.Error(err)
					}
				case <-time.After(3 * time.Second):
					t.Error("control did not close")
				}
			})
			response := func(want int) string {
				t.Helper()
				_, message, err := control.ReadResponse(want)
				if err != nil {
					t.Fatal(err)
				}
				return message
			}
			command := func(line string, want int) string {
				t.Helper()
				if err := control.PrintfLine("%s", line); err != nil {
					t.Fatal(err)
				}
				return response(want)
			}
			failed := func() protocols.Failure {
				t.Helper()
				select {
				case f := <-failures:
					return f
				case <-time.After(3 * time.Second):
					t.Fatal("missing failure observation")
					return protocols.Failure{}
				}
			}
			response(220)
			command("FEAT", 211)
			if policy == ftp.LiveEdit {
				command("AUTH TLS", 234)
				secured := tls.Client(peer, clientTLS)
				if err := secured.Handshake(); err != nil {
					t.Fatal(err)
				}
				control = textproto.NewConn(secured)
				command("PBSZ 0", 200)
				command("PROT P", 200)
			}
			command("USER test", 331)
			command("PASS wrong", 530)
			command("PASS synthetic", 230)
			command("TYPE I", 200)
			dataConn := func() net.Conn {
				t.Helper()
				message := command("EPSV", 229)
				parts := strings.Split(message, "|")
				if len(parts) != 5 {
					t.Fatal(message)
				}
				port, err := strconv.Atoi(parts[3])
				if err != nil {
					t.Fatal(err)
				}
				conn, err := net.DialTimeout("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(port)), time.Second)
				if err != nil {
					t.Fatal(err)
				}
				_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
				if policy == ftp.LiveEdit {
					return tls.Client(conn, clientTLS)
				}
				return conn
			}
			upload := func(cmd, data string) {
				t.Helper()
				conn := dataConn()
				command(cmd, 150)
				if _, err := io.WriteString(conn, data); err != nil {
					t.Fatal(err)
				}
				_ = conn.Close()
				response(226)
			}
			download := func(cmd string) string {
				t.Helper()
				conn := dataConn()
				command(cmd, 150)
				data, err := io.ReadAll(conn)
				_ = conn.Close()
				if err != nil {
					t.Fatal(err)
				}
				response(226)
				return string(data)
			}
			upload("STOR data", "01234")
			command("SIZE data", 213)
			command("REST 2", 350)
			if value := download("RETR data"); value != "234" {
				t.Fatal(value)
			}
			command("REST 5", 350)
			if value := download("RETR data"); value != "" {
				t.Fatal(value)
			}
			upload("APPE data", "56")
			command("REST 3", 350)
			upload("STOR data", "ZZ")
			if value := download("RETR data"); value != "012ZZ56" {
				t.Fatal(value)
			}
			command("TYPE A", 200)
			upload("STOR ascii", "line\r\ncr\r\x00")
			command("SIZE ascii", 550)
			command("REST 1", 501)
			if value := download("RETR ascii"); value != "line\r\ncr\r\x00" {
				t.Fatalf("ASCII %q", value)
			}
			command("MKD folder", 257)
			if value := download("MLSD"); !strings.Contains(value, "type=dir;") || !strings.Contains(value, " folder\r\n") {
				t.Fatal(value)
			}
			if value := download("LIST"); !strings.Contains(value, "+/,\tfolder\r\n") || !strings.Contains(value, "+r,s7,\tdata\r\n") {
				t.Fatal("LIST facts:", value)
			}
			command("RNFR data", 350)
			command("RNTO moved", 250)
			for _, tc := range []struct {
				line string
				code core.Code
			}{
				{"DELE folder", core.NotFile}, {"RMD moved", core.NotDirectory}, {"DELE MOVED", core.NotFound},
				{"RMD /", core.PermissionDenied},
			} {
				command(tc.line, 550)
				if f := failed(); core.CodeOf(f.Err) != tc.code || !f.HasChange || f.Change.Effect != core.Unchanged {
					t.Fatal("removal diagnostic:", tc.line, f)
				}
			}
			command("CWD ../outside", 550)
			if f := failed(); f.Operation != "CWD" || core.CodeOf(f.Err) != core.InvalidPath || f.HasChange {
				t.Fatal("invalid path failure:", f)
			}
			if text := command("CWD denied", 550); text != "Storage denied the operation" {
				t.Fatal("incorrect public cause:", text)
			}
			if f := failed(); f.Operation != "CWD" || !errors.Is(f.Err, storageDenial) || f.HasChange {
				t.Fatal("lost storage cause:", f)
			}
			// Exercise an active endpoint through the same transfer path.
			listener, err := net.Listen("tcp4", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = listener.Close() })
			command(fmt.Sprintf("EPRT |1|127.0.0.1|%d|", listener.Addr().(*net.TCPAddr).Port), 200)
			command("TYPE I", 200)
			command("RETR moved", 150)
			conn, err := listener.Accept()
			if err != nil {
				t.Fatal(err)
			}
			if policy == ftp.LiveEdit {
				conn = tls.Client(conn, clientTLS)
			}
			data, err := io.ReadAll(conn)
			_ = conn.Close()
			if err != nil || string(data) != "012ZZ56" {
				t.Fatal(string(data), err)
			}
			response(226)
			command("DELE moved", 250)
			command("RMD folder", 250)
			for _, name := range []string{"moved", "folder"} {
				if _, err := os.Lstat(filepath.Join(root, name)); !errors.Is(err, os.ErrNotExist) {
					t.Fatal("remove did not take effect:", name, err)
				}
			}
			if err = os.WriteFile(filepath.Join(root, "abort"), []byte("old"), 0600); err != nil {
				t.Fatal(err)
			}
			conn = dataConn()
			command("STOR abort", 150)
			if _, err = io.WriteString(conn, "prefix"); err != nil {
				t.Fatal(err)
			}
			select {
			case <-d.consumed:
			case <-time.After(3 * time.Second):
				t.Fatal("prefix was not consumed")
			}
			if err = control.PrintfLine("ABOR"); err != nil {
				t.Fatal(err)
			}
			message := response(426)
			response(226)
			_ = conn.Close()
			value, err := os.ReadFile(filepath.Join(root, "abort"))
			if err != nil {
				t.Fatal(err)
			}
			want := "old"
			wantEffect, wantMessage := core.Unchanged, "No changes made"
			if policy == ftp.LiveEdit {
				want = "prefix"
				wantEffect, wantMessage = core.Incomplete, "Partial changes remain"
			}
			failure := failed()
			if !strings.Contains(message, wantMessage) || failure.Operation != "STOR" || !failure.HasChange ||
				failure.Change.Effect != wantEffect || !errors.Is(failure.Err, context.Canceled) {
				t.Fatal("abort outcome:", message, failure)
			}
			if string(value) != want {
				t.Fatalf("abort: %q want %q", value, want)
			}
			command("NOOP", 200)
			// Abort while Accept is still pending, before any file is opened.
			command("EPSV", 229)
			command("STOR pending", 150)
			if err := control.PrintfLine("ABOR"); err != nil {
				t.Fatal(err)
			}
			response(426)
			response(226)
			if f := failed(); f.Operation != "STOR" || f.Phase != "connect" || !f.HasChange || f.Change.Effect != core.Unchanged {
				t.Fatal("pending abort outcome:", f)
			}
			if _, err := os.Stat(filepath.Join(root, "pending")); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("pending transfer changed a file", err)
			}
			command("QUIT", 221)
		})
	}
}

var storageDenial = errors.New("private storage path\r\n226 forged response")

func (d *observedLocal) Stat(ctx context.Context, a core.NativeAddress, selections ...core.ReadSelection) (core.Metadata, error) {
	if a.Value() == "/denied" {
		return core.Metadata{}, core.Fail(core.UpstreamAccessDenied, storageDenial)
	}
	return d.Driver.Stat(ctx, a, selections...)
}

type observedLocal struct {
	*local.Driver
	consumed chan struct{}
	once     sync.Once
}
type observedReader struct {
	io.Reader
	notify func()
}

func (r observedReader) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	if n > 0 {
		r.notify()
	}
	return n, err
}
func (d *observedLocal) Publish(ctx context.Context, a core.NativeAddress, r io.Reader, n *int64, o core.PublicationOptions) (core.Change, error) {
	if a.Value() == "/abort" {
		r = observedReader{r, func() { d.once.Do(func() { close(d.consumed) }) }}
	}
	return d.Driver.Publish(ctx, a, r, n, o)
}
func (d *observedLocal) Open(ctx context.Context, a core.NativeAddress, o core.OpenOptions) (core.Opened, error) {
	opened, err := d.Driver.Open(ctx, a, o)
	if err == nil && o.Mode == core.LiveEdit && a.Value() == "/abort" {
		opened.File = observedFile{opened.File.(core.EditFile), func() { d.once.Do(func() { close(d.consumed) }) }}
	}
	return opened, err
}

type observedFile struct {
	core.EditFile
	notify func()
}

func (f observedFile) WriteAt(ctx context.Context, p []byte, offset int64) (int, core.Change, error) {
	n, c, err := f.EditFile.WriteAt(ctx, p, offset)
	if n > 0 {
		f.notify()
	}
	return n, c, err
}
