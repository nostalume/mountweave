package smb

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"

	core "github.com/nostalume/mountweave"
)

// resource is the single owner shared by the Body and Iterator adapters.
// Only Close returns a session; reads never release another borrower's resource.
type resource struct {
	driver   *Driver
	conn     *connection
	handle   *handle
	end      func() error
	mu       sync.Mutex
	closeMu  sync.Mutex
	closing  atomic.Bool
	finished bool
	released bool
}

func (r *resource) Close() error {
	r.closing.Store(true)
	r.closeMu.Lock()
	defer r.closeMu.Unlock()
	if r.finished {
		if r.released {
			return nil
		}
		err := r.driver.put(r.conn)
		r.released = err == nil
		return err
	}
	if !r.mu.TryLock() {
		if err := r.conn.Close(); err != nil {
			return err
		}
		r.mu.Lock()
	}
	defer r.mu.Unlock()
	_, err := r.driver.closeHandle(r.conn, r.handle)
	r.handle = nil
	if r.end != nil {
		err = errors.Join(err, r.end())
		r.end = nil
	}
	r.finished = true
	putErr := r.driver.put(r.conn)
	r.released = putErr == nil
	return errors.Join(err, putErr)
}

func (d *Driver) Open(ctx context.Context, p core.NativeAddress, options core.OpenOptions) (core.Opened, error) {
	if err := options.Validate(); err != nil {
		return core.Opened{}, err
	}
	if options.Mode != core.StreamRead || options.NoFollow {
		return core.Opened{}, core.Fail(core.UnsupportedOperation, nil)
	}
	selected, err := selection([]core.ReadSelection{options.Selection})
	if err != nil {
		return core.Opened{}, err
	}
	if err = d.admit(p); err != nil {
		return core.Opened{}, err
	}
	if p.Value() == "/" {
		return core.Opened{}, core.Fail(core.NotFile, nil)
	}
	c, err := d.borrow(ctx)
	if err != nil {
		return core.Opened{}, err
	}
	b := &body{resource: &resource{driver: d, conn: c, end: c.scope(ctx)}, ctx: ctx}
	opened := core.Opened{Body: b}
	target, observed, err := d.locate(ctx, c, p.Value(), false)
	if err != nil {
		return opened, err
	}
	if observed.Kind != core.File {
		return opened, core.Fail(core.NotFile, nil)
	}
	b.handle, opened.Metadata, err = c.open(ctx, target, false, false)
	opened.Metadata.Name = observed.Name
	if err == nil {
		err = selected.Check(opened.Metadata.Evidence)
	}
	if err != nil {
		return opened, err
	}
	if opened.Metadata.Size == nil || *opened.Metadata.Size < 0 {
		return opened, core.Fail(core.InvalidUpstreamResponse, errors.New("SMB open did not supply a valid file size"))
	}
	length := *opened.Metadata.Size
	if options.Range != nil {
		actual, failure := options.Range.Resolve(length)
		if failure != nil {
			return opened, failure
		}
		opened.Range = &actual
		b.offset = actual.Start
		length = actual.End - actual.Start + 1
	}
	opened.Length, b.remaining = &length, length
	return opened, ctx.Err()
}

type body struct {
	*resource
	ctx       context.Context
	offset    int64
	remaining int64
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
	if b.remaining == 0 {
		return 0, io.EOF
	}
	p = p[:min(int64(len(p)), int64(b.driver.options.BlockBytes), b.remaining)]
	n, err := b.handle.readAt(b.ctx, p, b.offset)
	b.offset += int64(n)
	b.remaining -= int64(n)
	if err == io.EOF && b.remaining > 0 {
		err = io.ErrUnexpectedEOF
	}
	if err == nil && b.remaining == 0 {
		err = io.EOF
	}
	if cause := b.ctx.Err(); cause != nil {
		err = errors.Join(err, cause)
	}
	return n, err
}
