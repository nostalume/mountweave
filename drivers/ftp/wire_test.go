package ftp

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	core "github.com/nostalume/mountweave"
)

type failFirstClose struct {
	net.Conn
	failed bool
	err    error
	called chan struct{}
}

func (c *failFirstClose) Close() error {
	if !c.failed {
		c.failed = true
		close(c.called)
		return c.err
	}
	return c.Conn.Close()
}

func TestWireAndOwnershipEdges(t *testing.T) {
	for _, tc := range []struct {
		name, input string
		code        core.Code
	}{
		{"line budget", strings.Repeat("x", 32) + "\r\n", core.ResponseBudgetExceeded},
		{"reply budget", "220-start\r\n0123456789012345\r\n220 end\r\n", core.ResponseBudgetExceeded},
		{"bare newline", "220 hello\n", core.InvalidUpstreamResponse},
		{"truncated reply", "220-start\r\n", core.UpstreamFailure},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client, peer := net.Pipe()
			defer client.Close()
			defer peer.Close()
			c := &connection{socket: client, control: client, reader: bufio.NewReader(strings.NewReader(tc.input)), options: Options{MaxLineBytes: 24, MaxResponseBytes: 32}}
			if _, _, err := c.reply(); core.CodeOf(err) != tc.code || !c.closed.Load() {
				t.Fatalf("unbounded or ambiguous reply retained: code=%s error=%v", core.CodeOf(err), err)
			}
		})
	}
	for _, tc := range []struct {
		line, name string
		machine    bool
		code       core.Code
	}{
		{"type=file;size=; bad", "bad", true, core.InvalidUpstreamResponse},
		{"type=file;type=dir; bad", "bad", true, core.InvalidUpstreamResponse},
		{"type=OS.unix=symlink; special", "special", true, core.UnsupportedOperation},
		{"+r,s3,\t leading % name", " leading % name", false, ""},
		{"-rw-r--r-- 1 user group 3 Jan 01 2026 space  % &", "space  % &", false, ""},
	} {
		m, skip, err := parseEntry(tc.line, tc.machine)
		if m.Name != tc.name || skip || err != nil && core.CodeOf(err) != tc.code || err == nil && tc.code != "" {
			t.Fatalf("listing lost name or failure: %+v skip=%v error=%v", m, skip, err)
		}
		if tc.code == "" && (m.Kind != core.File || m.Size == nil || *m.Size != 3) {
			t.Fatalf("listing facts: %+v", m)
		}
	}
	t.Run("cancel cleanup cause survives retry", func(t *testing.T) {
		socket, peer := net.Pipe()
		defer socket.Close()
		defer peer.Close()
		cause := errors.New("synthetic first socket close failure")
		wrapped := &failFirstClose{Conn: socket, err: cause, called: make(chan struct{})}
		c := &connection{socket: wrapped, control: wrapped}
		d := &Driver{all: map[*connection]struct{}{c: {}}, count: 1}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		s := &stream{driver: d, conn: c, ctx: ctx, end: c.scope(ctx)}
		cancel()
		select {
		case <-wrapped.called:
		case <-time.After(time.Second):
			t.Fatal("cancellation did not retire its connection")
		}
		if err := s.Close(); !errors.Is(err, cause) {
			t.Fatalf("cleanup retry erased the first failure: %v", err)
		}
		if d.count != 0 || len(d.all) != 0 || !s.released {
			t.Fatal("successful retry did not release ownership")
		}
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
		if _, err := peer.Write([]byte("closed")); !errors.Is(err, io.ErrClosedPipe) {
			t.Fatalf("socket remained open after retry: %v", err)
		}
	})
}
