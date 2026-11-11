// Package upload provides bounded input chunking for native Driver uploads.
package upload

import (
	"bytes"
	"context"
	"errors"
	"io"
	"math"
	"sync"

	core "github.com/nostalume/mountweave"
)

var (
	ErrCapacity = errors.New("upload buffer capacity reached")
	ErrSize     = errors.New("input exceeds declared length")
	ErrParts    = errors.New("native part limit exceeded")
	ErrRead     = errors.New("invalid input Read result")
)

// Borrow exposes immutable data without copying. Close waits for active access
// and revokes later reads and seeks before the caller reuses the data.
func Borrow(data []byte) io.ReadSeekCloser {
	return &borrowedBody{reader: bytes.NewReader(data)}
}

type borrowedBody struct {
	mu     sync.Mutex
	reader *bytes.Reader
}

func (b *borrowedBody) Read(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.reader == nil {
		return 0, io.ErrClosedPipe
	}
	return b.reader.Read(p)
}

func (b *borrowedBody) Seek(offset int64, whence int) (int64, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.reader == nil {
		return 0, io.ErrClosedPipe
	}
	return b.reader.Seek(offset, whence)
}

func (b *borrowedBody) Close() error {
	b.mu.Lock()
	b.reader = nil
	b.mu.Unlock()
	return nil
}

// Budget counts all buffers held by active calls. Requests bound only send
// callbacks; SDK storage and native receipts are outside this payload budget.
type Budget struct {
	mu          sync.Mutex
	used, limit int64
	requests    chan struct{}
}

// Validate checks payload settings without acquiring or allocating a budget.
func Validate(size, limit int64, concurrency int) error {
	if size < 1 || size > int64(math.MaxInt) || limit < size || concurrency < 0 {
		return errors.New("positive part size and sufficient buffer budget are required")
	}
	return nil
}

func New(limit int64, concurrency int) (*Budget, error) {
	if limit < 1 || concurrency < 0 {
		return nil, errors.New("positive buffer budget and nonnegative request concurrency are required")
	}
	if concurrency == 0 {
		concurrency = 1
	}
	return &Budget{limit: limit, requests: make(chan struct{}, concurrency)}, nil
}

type Lease struct {
	budget  *Budget
	size    int
	buffers [][]byte
}

func (b *Budget) Acquire(ctx context.Context, size int64, parts int) (*Lease, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if size < 1 || size > int64(math.MaxInt) {
		return nil, errors.New("a positive representable part size is required")
	}
	if size > b.limit {
		return nil, ErrCapacity
	}
	window := int(min(int64(cap(b.requests)), b.limit/size))
	if parts > 0 {
		window = min(window, parts)
	}
	n := size * int64(window)
	b.mu.Lock()
	if n > b.limit-b.used {
		b.mu.Unlock()
		return nil, ErrCapacity
	}
	b.used += n
	b.mu.Unlock()
	l := &Lease{budget: b, size: int(size), buffers: make([][]byte, window)}
	for i := range l.buffers {
		l.buffers[i] = make([]byte, l.size)
	}
	return l, nil
}
func (l *Lease) Buffer() []byte { return l.buffers[0] }
func (l *Lease) Send(ctx context.Context, send func() error) error {
	select {
	case l.budget.requests <- struct{}{}:
		defer func() { <-l.budget.requests }()
		if err := ctx.Err(); err != nil {
			return err
		}
		return send()
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (l *Lease) Release() {
	l.budget.mu.Lock()
	l.budget.used -= int64(l.size) * int64(len(l.buffers))
	l.budget.mu.Unlock()
	l.buffers = nil
}

// ReadChunk preserves non-EOF errors even when they accompany the last byte.
func ReadChunk(ctx context.Context, input io.Reader, buffer []byte) (n int, err error) {
	empty := 0
	for n < len(buffer) {
		if err = ctx.Err(); err != nil {
			return
		}
		count, cause := input.Read(buffer[n:])
		if count < 0 || count > len(buffer)-n {
			return n, ErrRead
		}
		n += count
		if cause != nil {
			return n, cause
		}
		if count == 0 {
			empty++
			if empty == 100 {
				return n, io.ErrNoProgress
			}
		} else {
			empty = 0
		}
	}
	return
}
func ReadExact(ctx context.Context, input io.Reader, output []byte) error {
	n, err := ReadChunk(ctx, input, output)
	if err != nil && err != io.EOF {
		return err
	}
	if n != len(output) {
		return io.ErrUnexpectedEOF
	}
	if err == io.EOF {
		return nil
	}
	var extra [1]byte
	n, err = ReadChunk(ctx, input, extra[:])
	if n != 0 {
		return errors.Join(ErrSize, err)
	}
	if err != io.EOF {
		return err
	}
	return nil
}

// ReadRange verifies the entire requested source range before exposing its
// buffered bytes to a native request. It closes the range once and returns any close error.
func ReadRange(ctx context.Context, source core.Replayable, offset int64, output []byte) error {
	body, err := source.OpenRange(ctx, offset, int64(len(output)))
	if err == nil {
		if body == nil {
			err = core.Fail(core.InvalidUpstreamResponse, errors.New("source omitted its range body"))
		} else {
			err = ReadExact(ctx, body, output)
		}
	}
	if body != nil {
		err = errors.Join(err, body.Close())
	}
	return err
}

// Run borrows input synchronously and keeps each chunk until send returns.
// Empty input is sent as one final zero-length part. A failed send stops further
// reading/dispatch; blocked input and in-flight callbacks must actually return.
// Total and count measure read/attempted dispatch, not remote acknowledgement.
func (l *Lease) Run(ctx context.Context, input io.Reader, length *int64, maxParts int, send func(context.Context, int, []byte) error) (total int64, count int, err error) {
	life := ctx
	var cancel context.CancelFunc
	type part struct {
		index int
		data  []byte
	}
	var free chan []byte
	var jobs chan part
	var wg sync.WaitGroup
	var once sync.Once
	var sendErr error
	if len(l.buffers) > 1 {
		life, cancel = context.WithCancel(ctx)
		defer cancel()
		free, jobs = make(chan []byte, len(l.buffers)), make(chan part)
		for _, b := range l.buffers {
			free <- b
		}
		for range l.buffers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for p := range jobs {
					if e := l.Send(life, func() error { return send(life, p.index, p.data) }); e != nil {
						once.Do(func() { sendErr = e; cancel() })
					}
					free <- p.data[:l.size]
				}
			}()
		}
	}
	for {
		buffer := l.buffers[0]
		if jobs != nil {
			select {
			case buffer = <-free:
			case <-life.Done():
				err = life.Err()
			}
		}
		if err != nil {
			break
		}
		n, end := ReadChunk(life, input, buffer)
		if end != nil && end != io.EOF {
			err = end
			break
		}
		if length != nil && int64(n) > *length-total {
			err = ErrSize
			break
		}
		if n == 0 && count > 0 {
			break
		}
		if count == maxParts {
			err = ErrParts
			break
		}
		total += int64(n)
		count++
		if jobs == nil {
			err = l.Send(life, func() error { return send(life, count, buffer[:n]) })
		} else {
			select {
			case jobs <- part{count, buffer[:n]}:
			case <-life.Done():
				err = life.Err()
			}
		}
		if err != nil || end == io.EOF {
			break
		}
	}
	if err == nil && length != nil && total != *length {
		err = io.ErrUnexpectedEOF
	}
	if jobs != nil {
		if err != nil {
			cancel()
		}
		close(jobs)
		wg.Wait()
		// Only discard a pure cancellation induced by the other local failure.
		if sendErr != nil && err == context.Canceled && ctx.Err() == nil {
			err = nil
		}
		if err != nil && sendErr == context.Canceled && ctx.Err() == nil {
			sendErr = nil
		}
	}
	return total, count, errors.Join(sendErr, err, ctx.Err())
}
