package ftp

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"

	core "github.com/nostalume/mountweave"
)

// stream owns one control/data pair until native completion or retirement.
// Body and Iterator share this resource owner; they do not share a scheduler.
type stream struct {
	driver   *Driver
	conn     *connection
	data     net.Conn
	ctx      context.Context
	end      func() error
	mu       sync.Mutex
	closeMu  sync.Mutex
	handoff  sync.Mutex
	closing  atomic.Bool
	done     bool
	released bool
	terminal error
}

// finish runs with mu held. All cancellation hooks stop/join before pool return.
func (s *stream) finish(cause error, end func() error) error {
	err := cause
	if cause == io.EOF {
		err = s.conn.finish()
	} else {
		err = s.conn.fail(err)
	}
	if end != nil {
		err = errors.Join(err, end())
	}
	if s.end != nil {
		err = errors.Join(err, s.end(), s.ctx.Err())
		s.end = nil
	}
	putErr := s.release()
	err = errors.Join(err, putErr)
	if err == nil {
		err = io.EOF
	}
	s.done, s.terminal = true, err
	return err
}

func (s *stream) release() error {
	s.handoff.Lock()
	defer s.handoff.Unlock()
	err := s.driver.put(s.conn)
	s.released = err == nil
	return err
}

func (s *stream) Read(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closing.Load() {
		return 0, io.ErrClosedPipe
	}
	if len(p) == 0 {
		return 0, nil
	}
	if s.done {
		return 0, s.terminal
	}
	if err := s.ctx.Err(); err != nil {
		return 0, s.finish(err, nil)
	}
	n, err := s.data.Read(p)
	if err != nil {
		return n, s.finish(err, nil)
	}
	return n, nil
}

func (s *stream) Close() error {
	s.closing.Store(true)
	s.closeMu.Lock()
	defer s.closeMu.Unlock()
	s.handoff.Lock()
	if s.released {
		s.handoff.Unlock()
		return nil
	}
	if !s.mu.TryLock() {
		err := s.conn.Close()
		s.handoff.Unlock()
		if err != nil {
			return err
		}
		s.mu.Lock()
	} else {
		s.handoff.Unlock()
	}
	defer s.mu.Unlock()
	if s.done {
		if s.released {
			return nil
		}
		return s.release()
	}
	// Early Close does not drain a file/directory or manufacture an ABOR success.
	err := s.conn.Close()
	if s.end != nil {
		err = errors.Join(err, s.end())
		s.end = nil
	}
	putErr := s.release()
	s.done, s.terminal = true, io.ErrClosedPipe
	return errors.Join(err, putErr)
}

func (d *Driver) Open(ctx context.Context, p core.NativeAddress, options core.OpenOptions) (core.Opened, error) {
	if err := options.Validate(); err != nil {
		return core.Opened{}, err
	}
	if options.Mode != core.StreamRead || options.NoFollow {
		return core.Opened{}, core.Fail(core.UnsupportedOperation, nil)
	}
	selection, err := readSelection([]core.ReadSelection{options.Selection})
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
	s := &stream{driver: d, conn: c, ctx: ctx, end: c.scope(ctx)}
	opened := core.Opened{Body: s}
	meta, err := d.locate(ctx, c, p.Value(), false)
	if err == nil && meta.Kind != core.File {
		err = core.Fail(core.NotFile, nil)
	}
	if err == nil {
		err = selection.Check(meta.Evidence)
	}
	if err != nil {
		return opened, err
	}
	code, message, err := c.command("TYPE", "I")
	if err == nil {
		err = expect(code, message, 200)
	}
	if err == nil {
		s.data, err = c.start(ctx, "RETR", meta.Name)
	}
	// A prior listing/SIZE does not describe the opened transfer's actual size.
	// Range=nil explicitly returns a full-body fallback, without tail draining.
	opened.Metadata = core.Metadata{Name: meta.Name, Kind: core.File}
	return opened, errors.Join(err, ctx.Err())
}

func (d *Driver) Mkdir(ctx context.Context, p core.NativeAddress) (result core.Change, err error) {
	result.Effect = core.Unchanged
	if err = d.admit(p); err != nil {
		return result, err
	}
	if p.Value() == "/" {
		return result, core.Fail(core.AlreadyExists, nil)
	}
	c, err := d.borrow(ctx)
	if err != nil {
		return result, err
	}
	end := c.scope(ctx)
	defer func() { err = errors.Join(err, end(), ctx.Err(), d.put(c)) }()
	meta, err := d.locate(ctx, c, p.Value(), true)
	if err != nil {
		return result, err
	}
	code, message, err := c.command("MKD", meta.Name)
	if err != nil {
		if core.CodeOf(err) != core.InvalidRequest {
			result.Effect = core.Unknown
		}
		return result, err
	}
	err = expect(code, message, 257)
	if err == nil {
		result.Effect = core.Applied
	} else if code < 400 || code == 451 {
		result.Effect = core.Unknown
	}
	return result, err
}

var (
	_ core.Driver         = (*Driver)(nil)
	_ core.DirectoryMaker = (*Driver)(nil)
)
