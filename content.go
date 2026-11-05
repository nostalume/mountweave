package core

import (
	"errors"
	"io"
	"sync"
)

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
	order              *OrderTurn
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
		b.finishOrder()
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
	b.finishOrder()
	return err
}

func (b *ownedBody) finishOrder() {
	b.mu.Lock()
	t := b.order
	ready := b.finished && (b.closeErr != nil || b.readers == 0)
	uncertain := b.closeErr != nil
	if ready {
		b.order = nil
	}
	b.mu.Unlock()
	if ready {
		t.finish(uncertain)
	}
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
