package core

import (
	"context"
	"errors"
	"sync"
)

var ErrOrderStopped = errors.New("order scope stopped")

var ErrOrderUncertain = errors.New("order scope has an unconfirmed operation")

// OrderScope serializes explicitly reserved root operations. The trusted
// receiver reserves in request-receive order before dispatching goroutines.
// Capacity bounds active plus waiting turns; there is no worker or task record.
// Other scopes and calls without a turn are independent, including external I/O.
type OrderScope struct {
	mu              sync.Mutex
	capacity, count int
	head, tail      *OrderTurn
	err             error
}

func NewOrderScope(capacity int) (*OrderScope, error) {
	if capacity <= 0 {
		return nil, Fail(InvalidRequest, nil)
	}
	return &OrderScope{capacity: capacity}, nil
}

// OrderTurn is one root call's execution lifetime. Context is passed to the
// ordinary Core/File API. Cancel abandons an unused turn or requests cancellation
// of a started call; only that call's resource owner can acknowledge completion.
type OrderTurn struct {
	scope         *OrderScope
	prev, next    *OrderTurn
	ctx           context.Context
	cancel        context.CancelFunc
	stop          func() bool
	ready         chan struct{}
	claimed, done bool
}

type orderTurnKey struct{}

type orderActiveKey struct{}

func (s *OrderScope) Reserve(ctx context.Context) (*OrderTurn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return nil, s.err
	}
	if s.count == s.capacity {
		return nil, Fail(CapacityExceeded, nil)
	}
	life, cancel := context.WithCancel(ctx)
	t := &OrderTurn{scope: s, ctx: life, cancel: cancel, ready: make(chan struct{}), prev: s.tail}
	if s.tail != nil {
		s.tail.next = t
	} else {
		s.head = t
		close(t.ready)
	}
	s.tail = t
	s.count++
	t.stop = context.AfterFunc(life, t.Cancel)
	return t, nil
}

// Context is the execution lifetime, ended on completion as well as cancellation.
// Keep the original request context for response delivery after the Core call.
func (t *OrderTurn) Context() context.Context { return context.WithValue(t.ctx, orderTurnKey{}, t) }

func (t *OrderTurn) Cancel() {
	t.cancel()
	t.scope.mu.Lock()
	if !t.claimed {
		t.finishLocked(false)
	}
	t.scope.mu.Unlock()
}

// Stop cancels admission and pending/active call contexts. It does not close
// Files or acknowledge native I/O; those remain with their original owners.
func (s *OrderScope) Stop() { s.mu.Lock(); s.stopLocked(ErrOrderStopped); s.mu.Unlock() }

func (s *OrderScope) stopLocked(cause error) {
	if s.err == nil {
		s.err = cause
	}
	for t := s.head; t != nil; t = t.next {
		t.cancel()
	}
}

func (t *OrderTurn) finish(uncertain bool) {
	if t == nil {
		return
	}
	t.scope.mu.Lock()
	t.finishLocked(uncertain)
	t.scope.mu.Unlock()
}

func (t *OrderTurn) finishLocked(uncertain bool) {
	if t.done {
		return
	}
	t.done = true
	s := t.scope
	if uncertain {
		s.stopLocked(ErrOrderUncertain)
	}
	if t.prev != nil {
		t.prev.next = t.next
	} else {
		s.head = t.next
	}
	if t.next != nil {
		t.next.prev = t.prev
	} else {
		s.tail = t.prev
	}
	t.prev, t.next = nil, nil
	s.count--
	if t.stop != nil {
		t.stop()
	}
	t.cancel()
	if s.head != nil && s.err == nil {
		select {
		case <-s.head.ready:
		default:
			close(s.head.ready)
		}
	}
}

func orderScope(ctx context.Context) *OrderScope {
	if t, _ := ctx.Value(orderTurnKey{}).(*OrderTurn); t != nil {
		return t.scope
	}
	return nil
}

// enterOrder consumes one public call, while nested Core composition retains
// that call's already admitted context. It never requeues inner Open/Write.
func enterOrder(ctx context.Context, expected *OrderScope) (context.Context, *OrderTurn, error) {
	t, _ := ctx.Value(orderTurnKey{}).(*OrderTurn)
	if t == nil {
		if expected != nil {
			return ctx, nil, Fail(InvalidRequest, errors.New("a fresh turn from the opening scope is required"))
		}
		return ctx, nil, nil
	}
	if expected != nil && t.scope != expected {
		return ctx, nil, Fail(InvalidRequest, nil)
	}
	if active, _ := ctx.Value(orderActiveKey{}).(*OrderTurn); active == t {
		return ctx, nil, nil
	}
	select {
	case <-t.ready:
	case <-ctx.Done():
		t.Cancel()
		return ctx, nil, ctx.Err()
	}
	t.scope.mu.Lock()
	defer t.scope.mu.Unlock()
	if t.scope.err != nil {
		return ctx, nil, t.scope.err
	}
	if err := ctx.Err(); err != nil {
		if !t.claimed {
			t.finishLocked(false)
		}
		return ctx, nil, err
	}
	if t.done || t.claimed {
		return ctx, nil, Fail(InvalidRequest, nil)
	}
	t.claimed = true
	return context.WithValue(ctx, orderActiveKey{}, t), t, nil
}
