// Package upload provides bounded input chunking for native Driver uploads.
package upload

import (
	"context"
	"errors"
	"io"
	"math"
	"sync"
)

var (
	ErrCapacity = errors.New("upload buffer capacity reached")
	ErrSize     = errors.New("input exceeds declared length")
	ErrParts    = errors.New("native part limit exceeded")
	ErrRead     = errors.New("invalid input Read result")
)

// Budget counts all buffers held by active calls. Requests bound only send
// callbacks; SDK storage and native receipts are outside this payload budget.
type Budget struct {
	mu           sync.Mutex
	used, limit  int64
	size, window int
	requests     chan struct{}
}

func New(size, limit int64, concurrency int) (*Budget, error) {
	if size < 1 || size > int64(math.MaxInt) || limit < size || concurrency < 0 {
		return nil, errors.New("positive part size and sufficient buffer budget are required")
	}
	if concurrency == 0 {
		concurrency = 1
	}
	window := int(min(int64(concurrency), limit/size))
	return &Budget{size: int(size), limit: limit, window: window, requests: make(chan struct{}, concurrency)}, nil
}

type Lease struct {
	budget  *Budget
	buffers [][]byte
}

func (b *Budget) Acquire(ctx context.Context, parts int) (*Lease, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	window := b.window
	if parts > 0 {
		window = min(window, parts)
	}
	n := int64(b.size) * int64(window)
	b.mu.Lock()
	if n > b.limit-b.used {
		b.mu.Unlock()
		return nil, ErrCapacity
	}
	b.used += n
	b.mu.Unlock()
	l := &Lease{budget: b, buffers: make([][]byte, window)}
	for i := range l.buffers {
		l.buffers[i] = make([]byte, b.size)
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
	l.budget.used -= int64(l.budget.size) * int64(len(l.buffers))
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

// Run borrows input synchronously and keeps each chunk until send returns.
// Empty input is sent as one final zero-length part. A failed send stops further
// reading/dispatch; blocked input and in-flight callbacks must actually return.
func (l *Lease) Run(ctx context.Context, input io.Reader, length *int64, maxParts int, send func(context.Context, int, []byte) error) (total int64, count int, err error) {
	life, cancel := context.WithCancel(ctx)
	defer cancel()
	type part struct {
		index int
		data  []byte
	}
	free, jobs := make(chan []byte, len(l.buffers)), make(chan part)
	for _, b := range l.buffers {
		free <- b
	}
	var wg sync.WaitGroup
	var once sync.Once
	var sendErr error
	for range l.buffers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for p := range jobs {
				if e := l.Send(life, func() error { return send(life, p.index, p.data) }); e != nil {
					once.Do(func() { sendErr = e; cancel() })
				}
				free <- p.data[:l.budget.size]
			}
		}()
	}
	for {
		var buffer []byte
		select {
		case buffer = <-free:
		case <-life.Done():
			err = life.Err()
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
		select {
		case jobs <- part{count, buffer[:n]}:
		case <-life.Done():
			err = life.Err()
		}
		if err != nil || end == io.EOF {
			break
		}
	}
	if err == nil && length != nil && total != *length {
		err = io.ErrUnexpectedEOF
	}
	if err != nil {
		cancel()
	}
	close(jobs)
	wg.Wait()
	if sendErr != nil && errors.Is(err, context.Canceled) && ctx.Err() == nil {
		err = nil
	}
	if err != nil && errors.Is(sendErr, context.Canceled) && ctx.Err() == nil {
		sendErr = nil
	}
	return total, count, errors.Join(sendErr, err, ctx.Err())
}
