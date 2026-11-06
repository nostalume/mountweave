package ftp

import (
	"bufio"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/netip"
	"strconv"
	"sync"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/protocols"
	"github.com/nostalume/mountweave/protocols/internal/fileview"
)

// A transfer owns only this command's data endpoint and opened content. Its
// cancellation closes actual I/O before waiting for Core and native cleanup.
type transfer struct {
	x               *session
	ctx             context.Context
	cancel          context.CancelFunc
	done            chan struct{}
	fatal           error // published by closing done
	command         string
	address         core.Address
	actor           core.Principal
	binary, private bool
	offset          int64
	active          netip.AddrPort
	mu              sync.Mutex
	closed          bool
	completing      bool
	listener        net.Listener
	data            net.Conn
	content         io.Closer
	cleanupErr      error
	phase           string
	change          core.Change
}

func (x *session) startTransfer(cmd, arg string) error {
	if x.passive == nil && !x.active.IsValid() {
		return x.reply(425, "Data endpoint required")
	}
	if cmd == "APPE" && x.restart != 0 {
		return x.reply(503, "APPE cannot use REST")
	}
	a, _, err := x.view.Resolve(x.cwd, arg)
	if err != nil {
		return x.failure(cmd, "request", err)
	}
	ctx, cancel := context.WithCancel(x.ctx)
	t := &transfer{x: x, ctx: ctx, cancel: cancel, done: make(chan struct{}), command: cmd, address: a, actor: x.actor,
		binary: x.binary, private: x.private, offset: x.restart, listener: x.passive, active: x.active,
		change: core.Change{Effect: core.Unchanged}}
	x.passive = nil
	x.active = netip.AddrPort{}
	x.restart = 0
	x.transfer = t
	if err := x.reply(150, "Opening data connection"); err != nil {
		t.shutdown()
		cancel()
		t.fatal = errors.Join(err, t.cleanupErr)
		x.reportFailure(t.failure("reply", t.fatal))
		close(t.done)
		return err
	}
	go t.run()
	return nil
}

func (t *transfer) run() {
	defer close(t.done)
	defer t.cancel()
	hookDone := make(chan struct{})
	stop := context.AfterFunc(t.ctx, func() { t.shutdown(); close(hookDone) })
	err, closeErr := t.execute()
	t.shutdown()
	if !stop() {
		<-hookDone
	}
	t.mu.Lock()
	cleanup := errors.Join(closeErr, t.cleanupErr)
	t.completing = true
	t.mu.Unlock()
	uncertain := t.change.Effect == core.Unknown
	if err == nil && uncertain {
		err = core.Fail(core.InvalidUpstreamResponse, nil)
	}
	canceled := t.ctx.Err()
	if canceled != nil && !errors.Is(err, canceled) {
		err = errors.Join(err, canceled)
	}
	phase := t.phase
	if err == nil && cleanup != nil {
		phase = "cleanup"
	}
	failure := t.failure(phase, errors.Join(err, cleanup))
	if failure.Err != nil {
		t.x.reportFailure(failure)
	}
	if uncertain || cleanup != nil {
		t.fatal = failure.Err
		t.x.closeTransport()
		return
	}
	code, message := 226, "Transfer complete"
	if err != nil {
		code, message = 550, failure.Message()
		if phase == "connect" {
			code = 425
		}
	}
	if canceled != nil {
		code = 426
	}
	if replyErr := t.x.reply(code, message); replyErr != nil {
		if failure.Err == nil {
			t.x.reportFailure(t.failure("reply", replyErr))
		}
		t.fatal = replyErr
		t.x.closeTransport()
	}
}

func (t *transfer) failure(phase string, err error) protocols.Failure {
	return protocols.Failure{Operation: t.command, Phase: phase, Err: err, Change: t.change,
		HasChange: t.command == "STOR" || t.command == "APPE"}
}

func (t *transfer) shutdown() {
	t.mu.Lock()
	l, d, c := t.listener, t.data, t.content
	t.listener = nil
	t.data = nil
	t.content = nil
	t.closed = true
	t.mu.Unlock()
	var err error
	if l != nil {
		err = errors.Join(err, l.Close())
	}
	if d != nil {
		if closeErr := d.Close(); !errors.Is(closeErr, net.ErrClosed) {
			err = errors.Join(err, closeErr)
		}
	}
	if c != nil {
		err = errors.Join(err, c.Close())
	}
	t.mu.Lock()
	t.cleanupErr = errors.Join(t.cleanupErr, err)
	t.mu.Unlock()
}

func (t *transfer) adopt(c io.Closer, data bool) error {
	t.mu.Lock()
	closed := t.closed || t.ctx.Err() != nil
	if !closed {
		if data {
			t.data = c.(net.Conn)
		} else {
			t.content = c
		}
	}
	t.mu.Unlock()
	if closed {
		err := c.Close()
		t.mu.Lock()
		t.cleanupErr = errors.Join(t.cleanupErr, err)
		t.mu.Unlock()
		return context.Canceled
	}
	return nil
}

func (t *transfer) connect() (_ net.Conn, err error) {
	ctx, cancel := context.WithTimeout(t.ctx, t.x.options.DataConnectTimeout)
	defer cancel()
	// Closing the owned listener/connection also interrupts Accept and TLS I/O.
	done := make(chan struct{})
	stop := context.AfterFunc(ctx, func() { t.shutdown(); close(done) })
	defer func() {
		if !stop() {
			<-done
		}
		if cause := ctx.Err(); cause != nil && !errors.Is(err, cause) {
			err = errors.Join(err, cause)
		}
	}()
	var conn net.Conn
	t.mu.Lock()
	l := t.listener
	t.mu.Unlock()
	if l != nil {
		conn, err = l.Accept()
		t.mu.Lock()
		if t.listener == l {
			t.listener = nil
			closeErr := l.Close()
			t.cleanupErr = errors.Join(t.cleanupErr, closeErr)
		}
		t.mu.Unlock()
	} else if t.active.IsValid() && t.x.options.DialActive != nil {
		conn, err = t.x.options.DialActive(ctx, t.x.conn.RemoteAddr(), t.active)
	} else {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return nil, core.Fail(core.InvalidRequest, nil)
	}
	if conn != nil {
		if ownedErr := t.adopt(conn, true); err == nil {
			err = ownedErr
		}
	}
	if err != nil {
		return nil, err
	}
	if conn == nil {
		return nil, core.Fail(core.InvalidUpstreamResponse, nil)
	}
	if t.private {
		secured := tls.Server(conn, t.x.options.TLSConfig)
		if err := secured.HandshakeContext(ctx); err != nil {
			return nil, err
		}
		// The raw connection remains the cancellation owner; TLS holds no other
		// native resource. A failed send cannot wait for a peer close_notify.
		return secured, nil
	}
	return conn, nil
}

func (t *transfer) execute() (err, cleanup error) {
	t.phase = "connect"
	data, err := t.connect()
	if err != nil {
		return err, nil
	}
	if secured, ok := data.(*tls.Conn); ok {
		defer func() {
			if closeErr := secured.Close(); !errors.Is(closeErr, net.ErrClosed) {
				cleanup = errors.Join(cleanup, closeErr)
			}
		}()
	}
	switch t.command {
	case "RETR":
		t.phase = "open"
		o := core.OpenOptions{}
		if t.offset != 0 {
			o.Range = &core.Range{Start: t.offset, End: -1}
		}
		content, err := t.x.files.Open(t.ctx, t.actor, t.address, o)
		if err != nil {
			var failure *core.Error
			if errors.As(err, &failure) && failure.Code == core.RangeNotSatisfiable && failure.Total != nil && t.offset == *failure.Total {
				return nil, nil
			}
			return err, nil
		}
		if err := t.adopt(content.Body, false); err != nil {
			return err, nil
		}
		var output io.Writer = data
		t.phase = "send"
		if !t.binary {
			output = &asciiWriter{output: data, limit: t.x.options.Limits.TransferBufferBytes}
		}
		if native, ok := content.Body.(io.WriterTo); ok {
			_, err = native.WriteTo(output)
		} else {
			_, err = io.CopyBuffer(output, content.Body, make([]byte, t.x.options.Limits.TransferBufferBytes))
		}
		return err, nil
	case "STOR", "APPE":
		var input io.Reader = data
		if !t.binary {
			input = &asciiReader{input: bufio.NewReaderSize(data, t.x.options.Limits.TransferBufferBytes)}
		}
		if t.command == "STOR" && t.offset == 0 && t.x.options.StorePolicy == CompletePublish {
			t.phase = "publish"
			change, err := t.x.files.Write(t.ctx, t.actor, t.address, input, nil, core.WriteOptions{Policy: core.Publish()})
			t.change = change
			return err, nil
		}
		t.phase = "edit-open"
		defer func() {
			if err != nil && t.change.Effect == core.Applied {
				t.change.Effect = core.Incomplete
			}
		}()
		o := core.OpenOptions{Mode: core.LiveEdit, Creation: core.OpenOrCreate, Truncate: t.command == "STOR" && t.offset == 0, AppendOnly: t.command == "APPE"}
		content, err := t.x.files.Open(t.ctx, t.actor, t.address, o)
		t.change = content.Change
		if err != nil {
			return err, nil
		}
		if err := t.adopt(content.File, false); err != nil {
			return err, nil
		}
		if content.Change.Effect == core.Unknown {
			return core.Fail(core.InvalidUpstreamResponse, nil), nil
		}
		t.phase = "transfer"
		position := t.offset
		buffer := make([]byte, t.x.options.Limits.TransferBufferBytes)
		for {
			n, readErr := input.Read(buffer)
			if n > 0 {
				if int64(n) > math.MaxInt64-position {
					return core.Fail(core.InvalidRange, nil), nil
				}
				var written int
				var change core.Change
				if o.AppendOnly {
					written, change, err = content.File.Append(t.ctx, buffer[:n])
				} else {
					written, change, err = content.File.WriteAt(t.ctx, buffer[:n], position)
				}
				position += int64(written)
				switch change.Effect {
				case core.Applied, core.Incomplete, core.Unknown:
					t.change.Effect, t.change.Evidence = change.Effect, change.Evidence
				}
				if err != nil {
					t.phase = "write"
					return err, nil
				}
				if change.Effect == core.Unknown {
					t.phase = "write"
					return core.Fail(core.InvalidUpstreamResponse, nil), nil
				}
				if written != n {
					return io.ErrShortWrite, nil
				}
			}
			if readErr == io.EOF {
				return t.ctx.Err(), nil
			}
			if readErr != nil {
				t.phase = "receive"
				return readErr, nil
			}
			if err := t.ctx.Err(); err != nil {
				return err, nil
			}
		}
	case "MLSD", "NLST", "LIST":
		t.phase = "list"
		return t.list(data)
	default:
		return core.Fail(core.UnsupportedOperation, nil), nil
	}
}

func (t *transfer) list(output io.Writer) (err, fatal error) {
	cursor := ""
	defer func() {
		if cursor != "" {
			if cleanup := t.x.files.ReleaseCursor(context.WithoutCancel(t.ctx), t.actor, cursor); cleanup != nil && core.CodeOf(cleanup) != core.CursorUnavailable {
				fatal = errors.Join(fatal, cleanup)
			}
		}
	}()
	for {
		p, err := t.x.files.List(t.ctx, t.actor, core.ListRequest{Query: core.Children(t.address), Cursor: cursor, CursorTTL: t.x.options.CursorTTL, Limit: t.x.options.Limits.PageEntries})
		if err != nil {
			return err, nil
		}
		if p.NextCursor != nil {
			cursor = *p.NextCursor
		}
		remaining := t.x.options.Limits.PageNameBytes
		for _, e := range p.Items {
			n := fileview.NameBytes(e)
			if n > remaining {
				return core.Fail(core.ResponseBudgetExceeded, nil), nil
			}
			remaining -= n
		}
		for _, e := range p.Items {
			name := e.Name
			switch t.command {
			case "MLSD":
				name = facts(e) + " " + name
			case "LIST":
				name = listFacts(e) + "\t" + name
			}
			if _, err := fmt.Fprintf(output, "%s\r\n", name); err != nil {
				return err, nil
			}
		}
		if p.NextCursor == nil {
			return nil, nil
		}
	}
}

// EPLF can express type and optional size without inventing Unix attributes.
func listFacts(e core.Entry) string {
	if e.Kind == core.Directory {
		return "+/,"
	}
	if e.Size == nil {
		return "+r,"
	}
	return "+r,s" + strconv.FormatInt(*e.Size, 10) + ","
}
