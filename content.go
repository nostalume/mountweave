package core

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"time"
)

func (c *Core) Open(ctx context.Context, p Principal, target string, wanted *Range) (Content, error) {
	return c.open(ctx, p, target, wanted, nil, nil)
}
func (c *Core) open(ctx context.Context, p Principal, target string, wanted *Range, expected *location, scope DirectoryScope) (Content, error) {
	if wanted != nil && !wanted.Valid() {
		return Content{}, Fail(InvalidRange, nil)
	}
	u, _, err := c.admit(ctx, p, ReadAction, target, expected)
	if err != nil {
		return Content{}, err
	}
	if u == nil {
		return Content{}, Fail(NotFile, nil)
	}
	var phase sync.Mutex
	adopted, timedOut := false, false
	var timer *time.Timer
	if c.opts.OpenTimeout > 0 {
		timer = time.AfterFunc(c.opts.OpenTimeout, func() {
			phase.Lock()
			defer phase.Unlock()
			if !adopted {
				timedOut = true
				u.cancel()
			}
		})
	}
	rel := u.path
	var opened Opened
	var openErr error
	if scope == nil {
		opened, openErr = u.mount.driver.Open(u.ctx, rel, wanted)
	} else {
		opened, openErr = scope.OpenFile(u.ctx, rel[strings.LastIndexByte(rel, '/')+1:])
	}
	phase.Lock()
	if timer != nil {
		timer.Stop()
	}
	adopted = true
	expired := timedOut
	phase.Unlock()
	if openErr == nil {
		openErr = u.ctx.Err()
	}
	if expired {
		openErr = errors.Join(context.DeadlineExceeded, openErr)
	}
	return c.adoptOpened(u, target, rel, wanted, opened, openErr)
}

func (c *Core) adoptOpened(u *use, target, rel string, wanted *Range, opened Opened, openErr error) (Content, error) {
	if openErr == nil {
		openErr = u.ctx.Err()
	}
	if openErr == nil {
		openErr = validateOpened(opened, rel, wanted)
	}
	if openErr != nil {
		if opened.Body != nil {
			if closeErr := opened.Body.Close(); closeErr != nil {
				u.failed(closeErr, opened.Body)
				return Content{}, errors.Join(openErr, closeErr)
			}
		}
		u.release()
		return Content{}, openErr
	}
	entry, err := project(target, opened.Metadata)
	if err != nil {
		closeErr := opened.Body.Close()
		if closeErr == nil {
			u.release()
		} else {
			u.failed(closeErr, opened.Body)
		}
		return Content{}, errors.Join(err, closeErr)
	}
	b := &ownedBody{inner: opened.Body, use: u, done: make(chan struct{}), remaining: -1}
	if opened.Length != nil {
		b.remaining = *opened.Length
	}
	b.mu.Lock()
	b.stop = context.AfterFunc(u.ctx, func() { _ = b.Close() })
	b.mu.Unlock()
	var length *int64
	if opened.Length != nil {
		n := *opened.Length
		length = &n
	}
	var actual *ContentRange
	if opened.Range != nil {
		r := *opened.Range
		actual = &r
	}
	return Content{Entry: entry, Body: b, Length: length, Range: actual}, nil
}
func validateOpened(o Opened, rel string, wanted *Range) error {
	if o.Body == nil {
		return Fail(InvalidUpstreamResponse, errors.New("opened content has no body"))
	}
	if o.Metadata.Kind == Directory {
		return Fail(NotFile, nil)
	}
	if _, err := project("/", o.Metadata); err != nil {
		return err
	}
	if err := checkName(o.Metadata, rel); err != nil {
		return err
	}
	if o.Length != nil && *o.Length < 0 {
		return Fail(InvalidUpstreamResponse, nil)
	}
	if o.Range != nil {
		if wanted == nil || o.Length == nil {
			return Fail(InvalidUpstreamResponse, nil)
		}
		expected, err := wanted.Resolve(o.Range.Total)
		if err != nil || expected != *o.Range || *o.Length != o.Range.End-o.Range.Start+1 {
			return Fail(InvalidUpstreamResponse, err)
		}
	}
	return nil
}

// ownedBody is the single owner of the body and mount use. A completed Close
// alone cannot release that use while a Read is still executing.
type ownedBody struct {
	inner              io.ReadCloser
	use                *use
	mu                 sync.Mutex
	stop               func() bool
	closing, finished  bool
	readers            int
	remaining          int64
	terminal, closeErr error
	done               chan struct{}
}

// begin and finish are shared by Read and WriteTo so cancellation, length
// accounting and release acknowledgement have one owner.
func (b *ownedBody) begin() (int64, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closing {
		err := b.terminal
		if err == nil {
			err = io.ErrClosedPipe
		}
		return 0, err
	}
	b.readers++
	return b.remaining, nil
}
func (b *ownedBody) finish(n int64, err error, complete bool) error {
	if cause := b.use.ctx.Err(); cause != nil {
		if err == nil || err == io.EOF {
			err = cause
		} else {
			err = errors.Join(err, cause)
		}
	}
	b.mu.Lock()
	b.readers--
	if b.remaining >= 0 {
		b.remaining -= n
		switch {
		case b.remaining < 0:
			err = Fail(InvalidUpstreamResponse, errors.New("body exceeds declared length"))
		case b.remaining > 0 && (err == io.EOF || (complete && err == nil)):
			err = io.ErrUnexpectedEOF
		}
	}
	terminal := complete || err != nil || b.remaining == 0
	if terminal {
		b.terminal = err
		if err == nil {
			b.terminal = io.EOF
		}
	}
	release := b.finished && b.closeErr == nil && b.readers == 0
	b.mu.Unlock()
	if release {
		b.use.release()
	}
	if terminal {
		if closeErr := b.Close(); closeErr != nil {
			return errors.Join(err, closeErr)
		}
	}
	return err
}
func (b *ownedBody) Read(p []byte) (int, error) {
	want, err := b.begin()
	if err != nil {
		return 0, err
	}
	var n int
	switch {
	case b.use.ctx.Err() != nil:
		err = b.use.ctx.Err()
	case want == 0:
		err = io.EOF
	default:
		if want > 0 && int64(len(p)) > want {
			p = p[:int(want)]
		}
		n, err = b.inner.Read(p)
	}
	return n, b.finish(int64(n), err, false)
}
func (b *ownedBody) Close() error {
	b.mu.Lock()
	if b.closing {
		done := b.done
		b.mu.Unlock()
		<-done
		b.mu.Lock()
		err := b.closeErr
		b.mu.Unlock()
		return err
	}
	b.closing = true
	if b.terminal == nil {
		if err := b.use.ctx.Err(); err != nil {
			b.terminal = err
		} else {
			b.terminal = io.ErrClosedPipe
		}
	}
	stop := b.stop
	b.mu.Unlock()
	if stop != nil {
		stop()
	}
	err := b.inner.Close()
	b.mu.Lock()
	b.closeErr = err
	b.finished = true
	release := err == nil && b.readers == 0
	close(b.done)
	b.mu.Unlock()
	if err != nil {
		b.use.failed(err, b.inner)
	} else if release {
		b.use.release()
	}
	return err
}

// WriteTo preserves compatible native copy paths while retaining the mount use
// until the transfer and underlying close have both completed.
func (b *ownedBody) WriteTo(w io.Writer) (int64, error) {
	want, err := b.begin()
	if err == io.EOF {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	var n int64
	if cause := b.use.ctx.Err(); cause != nil {
		err = cause
	} else {
		var reader io.Reader = b.inner
		if want >= 0 {
			reader = io.LimitReader(reader, want)
		}
		n, err = io.Copy(w, reader)
	}
	return n, b.finish(n, err, true)
}
