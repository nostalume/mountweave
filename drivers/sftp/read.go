package sftp

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"time"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/internal/upload"
	sshfx "github.com/pkg/sftp/v2/encoding/ssh/filexfer"
)

// closeHandle consumes a handle once. SFTP invalidates it when CLOSE is sent,
// even if the reply is lost; subsequent cleanup can retire only the connection.
func closeHandle(c *connection, handle **string, timeout time.Duration) (bool, error) {
	if *handle == nil {
		return false, nil
	}
	h := *handle
	*handle = nil
	if c.closed.Load() || c.wire.broken {
		return false, c.Close()
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	end := c.scope(ctx)
	err := c.wire.status(&sshfx.ClosePacket{Handle: *h})
	err = errors.Join(err, end())
	if err != nil {
		return false, errors.Join(err, c.Close())
	}
	return true, nil
}

func windowBytes(requests int, length *int64) int64 {
	size := int64(packetBytes) * int64(requests)
	if length != nil && *length < size {
		size = max(*length, 1)
	}
	return size
}

func (d *Driver) Open(ctx context.Context, p core.NativeAddress, options core.OpenOptions) (core.Opened, error) {
	if err := options.Validate(); err != nil {
		return core.Opened{}, err
	}
	when := options.Selection.When
	if options.Mode != core.StreamRead || options.NoFollow || options.Selection.Version != "" ||
		(!when.Empty() && !when.IsPresent() && !when.IsAbsent()) {
		return core.Opened{}, core.Fail(core.UnsupportedOperation, nil)
	}
	if err := d.admit(p); err != nil {
		return core.Opened{}, err
	}
	if p.Value() == "/" {
		return core.Opened{}, core.Fail(core.NotFile, nil)
	}
	c, err := d.borrow(ctx)
	if err != nil {
		return core.Opened{}, err
	}
	b := &body{driver: d, connection: c, ctx: ctx, end: c.scope(ctx), remaining: -1}
	opened := core.Opened{Body: b}
	target, observed, err := d.resolve(c, p.Value(), false)
	if err != nil {
		return opened, err
	}
	if observed.Kind != core.File {
		return opened, core.Fail(core.NotFile, nil)
	}
	handle, err := c.wire.handle(&sshfx.OpenPacket{Filename: target, PFlags: sshfx.FlagRead})
	if err != nil {
		return opened, errors.Join(err, ctx.Err())
	}
	b.handle = &handle
	attrs, err := c.wire.attrs(&sshfx.FStatPacket{Handle: handle})
	if err != nil {
		return opened, errors.Join(err, ctx.Err())
	}
	opened.Metadata, err = metadata(observed.Name, attrs)
	if err == nil && opened.Metadata.Kind != core.File {
		err = core.Fail(core.NotFile, nil)
	}
	if err == nil {
		err = options.Selection.Check(opened.Metadata.Evidence)
	}
	if err != nil {
		return opened, err
	}
	if opened.Metadata.Size != nil {
		length := *opened.Metadata.Size
		opened.Length, b.remaining = &length, length
	}
	if options.Range != nil {
		if opened.Metadata.Size == nil {
			return opened, core.Fail(core.UnsupportedOperation, errors.New("SFTP did not report a size for range resolution"))
		}
		actual, failure := options.Range.Resolve(*opened.Metadata.Size)
		if failure != nil {
			return opened, failure
		}
		length := actual.End - actual.Start + 1
		opened.Range, opened.Length = &actual, &length
		b.offset, b.remaining = actual.Start, length
	}
	if b.remaining != 0 {
		b.lease, err = d.buffer.Acquire(ctx, windowBytes(d.options.MaxRequests, opened.Length), 1)
		if err != nil {
			return opened, core.Fail(core.CapacityExceeded, err)
		}
	}
	return opened, ctx.Err()
}

// body owns one opened file and its exclusive connection. Buffered read-ahead
// is bounded by its payload lease; no future request can access a returned lease.
type body struct {
	driver     *Driver
	connection *connection
	ctx        context.Context
	end        func() error
	handle     *string
	lease      *upload.Lease
	mu         sync.Mutex
	closeMu    sync.Mutex
	closing    atomic.Bool
	finished   bool
	released   bool
	offset     int64
	remaining  int64
	pending    []byte
	terminal   error
}

func (b *body) Read(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closing.Load() {
		return 0, io.ErrClosedPipe
	}
	if len(p) == 0 {
		return 0, nil
	}
	if err := b.ctx.Err(); err != nil {
		return 0, err
	}
	if len(b.pending) == 0 {
		if b.terminal != nil {
			return 0, b.terminal
		}
		if b.remaining == 0 {
			return 0, io.EOF
		}
		buf := b.lease.Buffer()
		if b.remaining > 0 && int64(len(buf)) > b.remaining {
			buf = buf[:int(b.remaining)]
		}
		n, err := b.connection.wire.readAt(*b.handle, buf, b.offset, b.driver.options.MaxRequests)
		b.offset += int64(n)
		if b.remaining >= 0 {
			b.remaining -= int64(n)
			if err == io.EOF && b.remaining > 0 {
				err = io.ErrUnexpectedEOF
			}
		}
		if err == nil && n == 0 {
			err = io.ErrNoProgress
		}
		if cause := b.ctx.Err(); cause != nil {
			err = errors.Join(err, cause)
		}
		b.pending, b.terminal = buf[:n], err
		if b.remaining == 0 && b.terminal == nil {
			b.terminal = io.EOF
		}
	}
	n := copy(p, b.pending)
	b.pending = b.pending[n:]
	if len(b.pending) == 0 {
		return n, b.terminal
	}
	return n, nil
}

func (b *body) Close() error {
	b.closing.Store(true)
	b.closeMu.Lock()
	defer b.closeMu.Unlock()
	if b.finished {
		if b.released {
			return nil
		}
		err := b.driver.put(b.connection) // Retry local cleanup, never native CLOSE.
		b.released = err == nil
		return err
	}
	if !b.mu.TryLock() {
		if err := b.connection.Close(); err != nil {
			return err
		}
		b.mu.Lock()
	}
	defer b.mu.Unlock()
	_, err := closeHandle(b.connection, &b.handle, b.driver.options.CleanupTimeout)
	endErr := b.end()
	if b.ctx.Err() != nil {
		// Cancellation is an operation cause. Cleanup reports only actual
		// connection-close errors; the caller already observes ctx through Read.
		endErr = nil
		if b.connection.closed.Load() {
			endErr = b.connection.Close()
		}
	}
	if b.lease != nil {
		b.pending = nil
		b.lease.Release()
		b.lease = nil
	}
	b.finished = true
	failure := b.driver.put(b.connection)
	b.released = failure == nil
	return errors.Join(err, endErr, failure)
}
