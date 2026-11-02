package task

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"slices"
	"sync"
)

var (
	ErrFull   = errors.New("task capacity reached")
	ErrClosed = errors.New("task queue is closed")
)

type State string

const (
	Queued    State = "queued"
	Running   State = "running"
	Finishing State = "finishing"
	Finished  State = "finished"
)

// Published Progress and Result values must not be mutated after publication.
// Their types and effect semantics belong to the task, not the scheduler.
type Snapshot struct {
	ID                             string
	State                          State
	CancelRequested                bool
	Progress, Result               any
	Err, CleanupErr, CompletionErr error
}

type Work struct {
	Run func(context.Context, func(any)) (any, error)
	// Key is interpreted only by Options.Conflicts, when supplied.
	Key any
	// Cleanup transfers on successful Submit and runs exactly once, even if
	// cancelled before Run. Rejection leaves all ownership with the caller.
	Cleanup func() error
	// Complete may save a terminal receipt. It must not mutate its values.
	// Failure is separate from Run's result and never replays the operation.
	Complete func(context.Context, Snapshot) error
}

type Options struct {
	Concurrency, Capacity int
	// Conflicts must be pure, symmetric, nonblocking and non-reentrant.
	// Earlier conflicting submissions keep order; independent work may pass.
	// No policy means independent tasks. This is not a cross-queue lock.
	Conflicts func(a, b any) bool
}

type Queue struct {
	mu      sync.Mutex
	cond    *sync.Cond
	opts    Options
	pending []*Handle
	active  map[*Handle]struct{}
	count   int
	closing bool
	life    context.Context
	cancel  context.CancelFunc
	workers sync.WaitGroup
	work    sync.WaitGroup
	done    chan struct{}
	// Retain failed cleanup owners instead of claiming a successful shutdown.
	failed     []*Handle
	cleanupErr error
}

// Handle owns its completed result; the queue maintains no completed history.
// A host can retain/index handles or save selected receipts as needed.
type Handle struct {
	queue    *Queue
	ctx      context.Context
	cancel   context.CancelFunc
	done     chan struct{}
	work     Work
	snapshot Snapshot
}

func New(opts Options) (*Queue, error) {
	if opts.Concurrency <= 0 || opts.Capacity <= 0 {
		return nil, errors.New("positive concurrency and capacity are required")
	}
	ctx, cancel := context.WithCancel(context.Background())
	q := &Queue{opts: opts, active: make(map[*Handle]struct{}), life: ctx, cancel: cancel, done: make(chan struct{})}
	q.cond = sync.NewCond(&q.mu)
	q.workers.Add(opts.Concurrency)
	for range opts.Concurrency {
		go q.worker()
	}
	return q, nil
}

// Submit's context controls admission only. Accepted work gets a queue-owned
// context independent of the submitting request; Cancel/Close request its stop.
// Capacity includes queued, running, finalizing and failed-cleanup owners, not
// ordinary completed handles. An unresolved cleanup still consumes its slot.
func (q *Queue) Submit(ctx context.Context, work Work) (*Handle, error) {
	if work.Run == nil {
		return nil, errors.New("task run function is required")
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if q.closing {
		return nil, ErrClosed
	}
	if q.count >= q.opts.Capacity {
		return nil, ErrFull
	}
	taskCtx, cancel := context.WithCancel(q.life)
	h := &Handle{queue: q, ctx: taskCtx, cancel: cancel, done: make(chan struct{}), work: work,
		snapshot: Snapshot{ID: rand.Text(), State: Queued}}
	q.pending = append(q.pending, h)
	q.count++
	q.work.Add(1)
	q.cond.Broadcast()
	return h, nil
}

func (h *Handle) Snapshot() Snapshot {
	q := h.queue
	q.mu.Lock()
	defer q.mu.Unlock()
	return h.snapshot
}

func (h *Handle) Wait(ctx context.Context) (Snapshot, error) {
	select {
	case <-h.done:
		return h.Snapshot(), nil
	default:
	}
	select {
	case <-h.done:
		return h.Snapshot(), nil
	case <-ctx.Done():
		return h.Snapshot(), ctx.Err()
	}
}

func (h *Handle) Cancel() { q := h.queue; q.mu.Lock(); defer q.mu.Unlock(); q.cancelLocked(h) }

func (q *Queue) cancelLocked(h *Handle) {
	if h.snapshot.State == Finished || h.snapshot.State == Finishing {
		return
	}
	h.snapshot.CancelRequested = true
	h.cancel()
	if h.snapshot.State != Queued {
		return
	}
	for i, pending := range q.pending {
		if pending == h {
			q.pending = slices.Delete(q.pending, i, i+1)
			h.snapshot.State, h.snapshot.Err = Finishing, context.Canceled
			// The admission slot stays occupied until finalization finishes.
			go q.finish(h)
			q.cond.Broadcast()
			return
		}
	}
}

func (q *Queue) conflicts(a, b *Handle) bool {
	return q.opts.Conflicts != nil && q.opts.Conflicts(a.work.Key, b.work.Key)
}

func (q *Queue) next() *Handle {
	for i, candidate := range q.pending {
		blocked := false
		for active := range q.active {
			if q.conflicts(candidate, active) {
				blocked = true
				break
			}
		}
		if !blocked {
			for _, earlier := range q.pending[:i] {
				if q.conflicts(candidate, earlier) {
					blocked = true
					break
				}
			}
		}
		if blocked {
			continue
		}
		q.pending = slices.Delete(q.pending, i, i+1)
		q.active[candidate] = struct{}{}
		candidate.snapshot.State = Running
		return candidate
	}
	return nil
}

func (q *Queue) worker() {
	defer q.workers.Done()
	for {
		q.mu.Lock()
		h := q.next()
		for h == nil && !q.closing {
			q.cond.Wait()
			h = q.next()
		}
		if h == nil {
			q.mu.Unlock()
			return
		}
		q.mu.Unlock()
		result, err := invoke(func() (any, error) { return h.work.Run(h.ctx, h.report) })
		q.mu.Lock()
		h.snapshot.Result, h.snapshot.Err, h.snapshot.State = result, err, Finishing
		q.mu.Unlock()
		q.finish(h)
	}
}

func invoke(f func() (any, error)) (value any, err error) {
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("task callback panic: %v", p)
		}
	}()
	return f()
}

func (h *Handle) report(progress any) {
	q := h.queue
	q.mu.Lock()
	defer q.mu.Unlock()
	if h.snapshot.State == Running {
		h.snapshot.Progress = progress
	}
}

func (q *Queue) finish(h *Handle) {
	var cleanupErr, completionErr error
	if h.work.Cleanup != nil {
		_, cleanupErr = invoke(func() (any, error) { return nil, h.work.Cleanup() })
	}
	q.mu.Lock()
	h.snapshot.CleanupErr = cleanupErr
	receipt := h.snapshot
	receipt.State = Finished
	q.mu.Unlock()
	if h.work.Complete != nil {
		_, completionErr = invoke(func() (any, error) { return nil, h.work.Complete(context.WithoutCancel(h.ctx), receipt) })
	}
	h.cancel()
	q.mu.Lock()
	h.snapshot.CompletionErr, h.snapshot.State = completionErr, Finished
	if cleanupErr != nil {
		q.failed = append(q.failed, h)
		q.cleanupErr = errors.Join(q.cleanupErr, cleanupErr)
	} else {
		h.work.Cleanup = nil
	}
	h.work.Run, h.work.Key, h.work.Complete = nil, nil, nil
	delete(q.active, h)
	if cleanupErr == nil {
		q.count--
	}
	close(h.done)
	q.cond.Broadcast()
	q.mu.Unlock()
	q.work.Done()
}

// Close stops admission, cancels queued/running work and waits for its actual
// return, cleanup and completion acknowledgements. Context bounds only waiting.
// Completion callbacks receive an uncancelled context and own their I/O timeout.
func (q *Queue) Close(ctx context.Context) error {
	q.mu.Lock()
	if !q.closing {
		q.closing = true
		q.cancel()
		for len(q.pending) > 0 {
			q.cancelLocked(q.pending[0])
		}
		for h := range q.active {
			q.cancelLocked(h)
		}
		q.cond.Broadcast()
		go func() { q.workers.Wait(); q.work.Wait(); close(q.done) }()
	}
	q.mu.Unlock()
	select {
	case <-q.done:
		q.mu.Lock()
		defer q.mu.Unlock()
		return q.cleanupErr
	default:
	}
	select {
	case <-q.done:
		q.mu.Lock()
		defer q.mu.Unlock()
		return q.cleanupErr
	case <-ctx.Done():
		return ctx.Err()
	}
}
