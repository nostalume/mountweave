package upload

import (
	"context"
	"errors"
	"io"
	"sync"

	core "github.com/nostalume/mountweave"
)

// Body borrows a stream for one native request. Finish waits for an active Read
// and revokes later reads; neither Finish nor Close closes the borrowed input.
type Body struct {
	mu        sync.Mutex
	ctx       context.Context
	input     io.Reader
	remaining int64
	maximum   int64
	payload   int64
	closed    bool
	complete  bool
	err       error
}

// NewBody checks a declared length including normal EOF. A nil length streams
// until EOF without buffering the complete input. An optional positive maximum
// bounds payload bytes independently of length. The caller still owns input.
func NewBody(ctx context.Context, input io.Reader, length *int64, maximum ...int64) *Body {
	b := &Body{ctx: ctx, input: input, remaining: -1, maximum: -1}
	if input == nil || length != nil && *length < 0 || len(maximum) > 1 || len(maximum) == 1 && maximum[0] <= 0 {
		b.err = core.Fail(core.InvalidRequest, nil)
		return b
	}
	if len(maximum) != 0 {
		b.maximum = maximum[0]
	}
	if length != nil {
		b.remaining = *length
		if b.maximum > 0 && *length > b.maximum {
			b.err = core.Fail(core.CapacityExceeded, errors.New("input exceeds maximum length"))
		}
	}
	return b
}

func (b *Body) Read(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(p) == 0 {
		return 0, nil
	}
	if b.closed {
		return 0, io.ErrClosedPipe
	}
	if b.err != nil {
		return 0, b.err
	}
	if b.complete {
		return 0, io.EOF
	}
	if err := b.ctx.Err(); err != nil {
		b.err = err
		return 0, err
	}
	quota := b.remaining
	if b.maximum > 0 && (quota < 0 || b.maximum-b.payload < quota) {
		quota = b.maximum - b.payload
	}
	if quota >= 0 && int64(len(p)) > quota {
		p = p[:int(quota)]
	}
	var n int
	var err error
	if len(p) != 0 {
		n, err = ReadChunk(b.ctx, b.input, p)
		b.payload += int64(n)
		if b.remaining >= 0 {
			b.remaining -= int64(n)
			if err == io.EOF && b.remaining != 0 {
				err = io.ErrUnexpectedEOF
			}
		}
	}
	if err == nil && (b.remaining == 0 || b.maximum > 0 && b.payload == b.maximum) {
		var extra [1]byte
		count, end := ReadChunk(b.ctx, b.input, extra[:])
		if count != 0 {
			if b.remaining == 0 {
				err = core.Fail(core.InvalidRequest, errors.Join(ErrSize, end))
			} else {
				err = core.Fail(core.CapacityExceeded, errors.Join(errors.New("input exceeds maximum length"), end))
			}
		} else {
			err = end
		}
	}
	if err == io.EOF {
		b.complete = true
	} else if err != nil {
		b.err = err
	}
	return n, err
}

// PayloadBytes counts bytes returned by Read, excluding any extra EOF/limit
// probe. It does not measure transport delivery or remote acknowledgement.
func (b *Body) PayloadBytes() int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.payload
}

// Finish preserves the observed input outcome while ending transport access.
func (b *Body) Finish() (bool, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.closed = true
	b.input = nil
	return b.complete, b.err
}

func (b *Body) Close() error {
	_, err := b.Finish()
	return err
}
