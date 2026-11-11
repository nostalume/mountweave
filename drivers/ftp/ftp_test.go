package ftp_test

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http/httptest"
	"net/netip"
	"net/textproto"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	core "github.com/nostalume/mountweave"
	driver "github.com/nostalume/mountweave/drivers/ftp"
	"github.com/nostalume/mountweave/drivers/local"
	export "github.com/nostalume/mountweave/protocols/ftp"
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

type service struct {
	root        string
	options     driver.Options
	connections atomic.Int64
	requests    atomic.Int64
	finalFault  atomic.Int32
	legacy      bool
}

// Only control replies are altered. File behavior, data streams and TLS come
// from the existing export and Local, not a second FTP server implementation.
type control struct {
	net.Conn
	service *service
	reader  *bufio.Reader
	pending []byte
	mu      sync.Mutex
	command string
}

func (c *control) Read(p []byte) (int, error) {
	if len(c.pending) == 0 {
		line, err := c.reader.ReadBytes('\n')
		if err != nil {
			return 0, err
		}
		command, _, _ := strings.Cut(string(line), " ")
		c.mu.Lock()
		c.command = strings.TrimSpace(command)
		c.mu.Unlock()
		// Model a server whose native path lookup accepts alternate case.
		c.pending = bytes.ReplaceAll(line, []byte("EXACT"), []byte("Exact"))
	}
	n := copy(p, c.pending)
	c.pending = c.pending[n:]
	return n, nil
}

func (c *control) Write(p []byte) (int, error) {
	line := string(p)
	if c.service.legacy {
		line = strings.ReplaceAll(line, " MLST type*;size*;modify*;\r\n", "")
		if strings.HasPrefix(line, "229 ") {
			line = "502 EPSV unavailable\r\n"
		}
		if strings.HasPrefix(line, "227 ") {
			line = strings.Replace(line, "127,0,0,1,", "192,0,2,1,", 1)
		}
	}
	c.mu.Lock()
	command := c.command
	c.mu.Unlock()
	if command == "RETR" && strings.HasPrefix(line, "226 ") {
		switch c.service.finalFault.Swap(0) {
		case 1:
			line = "426 Synthetic transfer confirmation failure\r\n"
		case 2:
			return len(p), nil
		}
	}
	n, err := io.WriteString(c.Conn, line)
	if err == nil && n != len(line) {
		err = io.ErrShortWrite
	}
	if err != nil {
		return 0, err
	}
	return len(p), nil
}

func upstream(t *testing.T, security driver.Security, legacy bool, authorize core.Authorizer) *service {
	t.Helper()
	f := &service{root: t.TempDir(), legacy: legacy}
	c := files(t, func(ctx context.Context, p core.Principal, a core.Action, target core.Address) error {
		f.requests.Add(1)
		if authorize != nil {
			return authorize(ctx, p, a, target)
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
	o := export.Options{Root: core.Path("/files"), CursorTTL: time.Minute, DataConnectTimeout: time.Second,
		Limits: export.Limits{CommandBytes: 4096, TransferBufferBytes: 4096, PageEntries: 2, PageNameBytes: 8192},
		Authenticate: func(_ context.Context, l export.Login) (core.Principal, error) {
			if l.Username != "account" || l.Password != "synthetic" || (l.TLS != nil) != (security != driver.Plain) {
				return "", errors.New("invalid synthetic credentials")
			}
			return "reader", nil
		},
		ListenPassive: func(ctx context.Context, _, _ net.Addr) (net.Listener, netip.AddrPort, error) {
			l, err := (&net.ListenConfig{}).Listen(ctx, "tcp4", "127.0.0.1:0")
			if err != nil {
				return nil, netip.AddrPort{}, err
			}
			return l, l.Addr().(*net.TCPAddr).AddrPort(), nil
		},
	}
	var serverTLS, clientTLS *tls.Config
	if security != driver.Plain {
		certificate := httptest.NewTLSServer(nil)
		t.Cleanup(certificate.Close)
		serverTLS = &tls.Config{Certificates: certificate.TLS.Certificates}
		roots := x509.NewCertPool()
		roots.AddCert(certificate.Certificate())
		clientTLS = &tls.Config{RootCAs: roots, ServerName: "127.0.0.1"}
		o.TLSConfig = serverTLS
	}
	s, err := export.New(c, o)
	if err != nil {
		t.Fatal(err)
	}
	l, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	f.options = driver.Options{Address: l.Addr().String(), RootPath: "/", Username: "account", Password: "synthetic",
		Security: security, TLSConfig: clientTLS, MaxConnections: 4, PageEntries: 2, CleanupTimeout: 150 * time.Millisecond}
	ctx, cancel := context.WithCancel(context.Background())
	var mu sync.Mutex
	connections := map[net.Conn]struct{}{}
	var workers sync.WaitGroup
	workers.Add(1)
	go func() {
		defer workers.Done()
		for {
			conn, err := l.Accept()
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
				var controlConn net.Conn = conn
				if security == driver.ImplicitTLS {
					controlConn = tls.Server(conn, serverTLS)
				}
				if security == driver.Plain {
					controlConn = &control{Conn: conn, service: f, reader: bufio.NewReader(conn)}
				}
				_ = s.ServeConn(ctx, controlConn)
			}()
		}
	}()
	t.Cleanup(func() {
		cancel()
		_ = l.Close()
		mu.Lock()
		for conn := range connections {
			_ = conn.Close()
		}
		mu.Unlock()
		workers.Wait()
	})
	return f
}

func mount(t *testing.T, o driver.Options) (*core.Core, *core.Mount) {
	t.Helper()
	d, err := driver.Open(context.Background(), o)
	if err != nil {
		if d != nil {
			_ = d.Close()
		}
		t.Fatal(err)
	}
	c := files(t, nil)
	m, err := c.Install("/remote", d)
	if err != nil {
		_ = d.Close()
		t.Fatal(err)
	}
	return c, m
}

func TestSourceThroughExistingExport(t *testing.T) {
	for _, security := range []driver.Security{driver.Plain, driver.ExplicitTLS, driver.ImplicitTLS} {
		t.Run(fmt.Sprint(security), func(t *testing.T) {
			f := upstream(t, security, false, nil)
			payload := bytes.Repeat([]byte("0123456789abcdef"), 4097)
			if err := os.WriteFile(filepath.Join(f.root, "space % &"), payload, 0600); err != nil {
				t.Fatal(err)
			}
			c, m := mount(t, f.options)
			ctx := context.Background()
			if err := c.BindAlias(ctx, "/alias", m, "/"); err != nil {
				t.Fatal(err)
			}
			before := f.connections.Load()
			entry, err := c.Stat(ctx, "reader", core.Path("/alias/space % &"))
			if err != nil || entry.Kind != core.File {
				t.Fatalf("stat: %+v %v", entry, err)
			}
			opened, err := c.Open(ctx, "reader", core.Path("/alias/space % &"), core.OpenOptions{Range: &core.Range{Start: 3, End: 9}})
			if err != nil {
				t.Fatal(err)
			}
			got, readErr := io.ReadAll(opened.Body)
			if err := errors.Join(readErr, opened.Body.Close()); err != nil || !bytes.Equal(got, payload) || opened.Range != nil || opened.Length != nil {
				t.Fatalf("full-body fallback: bytes=%d range=%+v length=%v error=%v", len(got), opened.Range, opened.Length, err)
			}
			if f.connections.Load() != before {
				t.Fatal("healthy connection was not reused")
			}
			if change, err := c.Mkdir(ctx, "writer", core.Path("/remote/folder")); err != nil || change.Effect != core.Applied {
				t.Fatalf("mkdir: %+v %v", change, err)
			}
			for _, name := range []string{"one", "two", "three"} {
				if err := os.WriteFile(filepath.Join(f.root, "folder", name), nil, 0600); err != nil {
					t.Fatal(err)
				}
			}
			page, err := c.List(ctx, "reader", core.ListRequest{Query: core.Children(core.Path("/remote/folder")), Limit: 1, CursorTTL: time.Minute})
			listed := len(page.Items)
			for err == nil && page.NextCursor != nil {
				r := core.ListRequest{Query: core.Children(core.Path("/remote/folder")), Limit: 1, Cursor: *page.NextCursor}
				page, err = c.List(ctx, "reader", r)
				listed += len(page.Items)
			}
			if err != nil || listed != 3 {
				t.Fatalf("listing count=%d error=%v", listed, err)
			}
			destination := t.TempDir()
			localDriver, err := local.New(destination)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := c.Install("/local", localDriver); err != nil {
				t.Fatal(err)
			}
			if change, err := c.Copy(ctx, "writer", core.Path("/remote/space % &"), core.Path("/local/copied")); err != nil || change.Effect != core.Applied {
				t.Fatalf("copy: %+v %v", change, err)
			}
			if got, err := os.ReadFile(filepath.Join(destination, "copied")); err != nil || !bytes.Equal(got, payload) {
				t.Fatalf("copied bytes=%d error=%v", len(got), err)
			}
			before = f.requests.Load()
			if _, err := c.Open(ctx, "reader", core.Path("/remote/space % &"), core.OpenOptions{NoFollow: true}); core.CodeOf(err) != core.UnsupportedOperation || f.requests.Load() != before {
				t.Fatalf("NoFollow performed native I/O: %v", err)
			}
			if change, err := c.Write(ctx, "writer", core.Path("/remote/new"), strings.NewReader("unread"), nil); core.CodeOf(err) != core.UnsupportedOperation || change.Effect != core.Unchanged {
				t.Fatalf("unsupported create: %+v %v", change, err)
			}
		})
	}
}

func TestLegacyListingAndExactNames(t *testing.T) {
	f := upstream(t, driver.Plain, true, nil)
	if err := os.WriteFile(filepath.Join(f.root, "Exact"), []byte("content"), 0600); err != nil {
		t.Fatal(err)
	}
	c, _ := mount(t, f.options)
	ctx := context.Background()
	if _, err := c.Stat(ctx, "reader", core.Path("/remote/EXACT")); core.CodeOf(err) != core.NotFound {
		t.Fatalf("alternate case was admitted: %v", err)
	}
	opened, err := c.Open(ctx, "reader", core.Path("/remote/Exact"), core.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	got, readErr := io.ReadAll(opened.Body)
	if err := errors.Join(readErr, opened.Body.Close()); err != nil || string(got) != "content" {
		t.Fatalf("LIST/PASV fallback: %q %v", got, err)
	}
}

func TestFinalReceiptsAndReturnedOwners(t *testing.T) {
	for _, fault := range []int32{1, 2} {
		t.Run(fmt.Sprint(fault), func(t *testing.T) {
			f := upstream(t, driver.Plain, false, nil)
			if err := os.WriteFile(filepath.Join(f.root, "data"), []byte("content"), 0600); err != nil {
				t.Fatal(err)
			}
			c, _ := mount(t, f.options)
			ctx := context.Background()
			f.finalFault.Store(fault)
			opened, err := c.Open(ctx, "reader", core.Path("/remote/data"), core.OpenOptions{})
			if err != nil {
				t.Fatal(err)
			}
			got, readErr := io.ReadAll(opened.Body)
			closeErr := opened.Body.Close()
			if string(got) != "content" || errors.Join(readErr, closeErr) == nil {
				t.Fatalf("missing final confirmation treated as EOF: %q %v %v", got, readErr, closeErr)
			}
			var native *textproto.Error
			if fault == 1 && (!errors.As(errors.Join(readErr, closeErr), &native) || native.Code != 426) {
				t.Fatalf("native cause lost: %v %v", readErr, closeErr)
			}
			before := f.connections.Load()
			next, err := c.Open(ctx, "reader", core.Path("/remote/data"), core.OpenOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if f.connections.Load() <= before {
				t.Fatal("unconfirmed session reused")
			}
			if err := opened.Body.Close(); err != nil {
				t.Fatal(err)
			}
			got, readErr = io.ReadAll(next.Body)
			if err := errors.Join(readErr, next.Body.Close()); err != nil || string(got) != "content" {
				t.Fatalf("old owner affected successor: %q %v", got, err)
			}
		})
	}
}

func TestCancellationCapacityAndEarlyClose(t *testing.T) {
	entered := make(chan struct{}, 1)
	f := upstream(t, driver.Plain, false, func(ctx context.Context, _ core.Principal, a core.Action, target core.Address) error {
		if a == core.ReadAction && target.Value() == "/files/stalled" {
			select {
			case entered <- struct{}{}:
			default:
			}
			<-ctx.Done()
			return ctx.Err()
		}
		return nil
	})
	for _, name := range []string{"stalled", "other"} {
		if err := os.WriteFile(filepath.Join(f.root, name), []byte("healthy"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	f.options.MaxConnections = 2
	c, _ := mount(t, f.options)
	ctx, cancel := context.WithCancel(context.Background())
	blocked, err := c.Open(ctx, "reader", core.Path("/remote/stalled"), core.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	readDone := make(chan error, 1)
	go func() { _, err := io.ReadAll(blocked.Body); readDone <- err }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("native read did not start")
	}
	other, err := c.Open(context.Background(), "reader", core.Path("/remote/other"), core.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Stat(context.Background(), "reader", core.Path("/remote/other")); core.CodeOf(err) != core.CapacityExceeded {
		t.Fatalf("occupied capacity did not fail immediately: %v", err)
	}
	cancel()
	select {
	case err := <-readDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancel cause: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("cancel did not interrupt data I/O")
	}
	if err := blocked.Body.Close(); err != nil {
		t.Fatal(err)
	}
	got, readErr := io.ReadAll(other.Body)
	if err := errors.Join(readErr, other.Body.Close()); err != nil || string(got) != "healthy" {
		t.Fatalf("cancel affected independent activity: %q %v", got, err)
	}
	early, err := c.Open(context.Background(), "reader", core.Path("/remote/other"), core.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := early.Body.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Stat(context.Background(), "reader", core.Path("/remote/other")); err != nil {
		t.Fatal(err)
	}
}
