package task_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nostalume/mountweave/task"
)

func wait(t *testing.T, h *task.Handle) task.Snapshot {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s, err := h.Wait(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return s
}
func submit(t *testing.T, q *task.Queue, w task.Work) *task.Handle {
	t.Helper()
	h, err := q.Submit(context.Background(), w)
	if err != nil {
		t.Fatal(err)
	}
	return h
}
func signal(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatal("task did not reach boundary")
	}
}

func TestSchedulingOwnershipAndReceipts(t *testing.T) {
	q, err := task.New(task.Options{Concurrency: 2, Capacity: 4, Conflicts: func(a, b any) bool { return a != nil && a == b }})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := q.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	entered, release := make(chan struct{}), make(chan struct{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	request, cancelRequest := context.WithCancel(context.Background())
	a, err := q.Submit(request, task.Work{Key: "same", Run: func(ctx context.Context, report func(any)) (any, error) {
		report("waiting")
		close(entered)
		<-release
		return 7, ctx.Err()
	}})
	if err != nil {
		t.Fatal(err)
	}
	signal(t, entered)
	cancelRequest()
	if a.Snapshot().Progress != "waiting" {
		t.Fatal("latest progress lost")
	}
	started := make(chan struct{})
	b := submit(t, q, task.Work{Key: "same", Run: func(context.Context, func(any)) (any, error) { close(started); return "ordered", nil }})
	// The second worker must pass the blocked item for unrelated, non-file work.
	c := submit(t, q, task.Work{Run: func(context.Context, func(any)) (any, error) { return 9, nil }})
	if s := wait(t, c); s.Result != 9 || s.Err != nil {
		t.Fatalf("independent: %+v", s)
	}
	select {
	case <-started:
		t.Fatal("conflict bypassed earlier task")
	default:
	}
	var ran, cleaned atomic.Int32
	d := submit(t, q, task.Work{Key: "same", Run: func(context.Context, func(any)) (any, error) { ran.Add(1); return nil, nil }, Cleanup: func() error { cleaned.Add(1); return nil }})
	d.Cancel()
	if s := wait(t, d); !errors.Is(s.Err, context.Canceled) || !s.CancelRequested || ran.Load() != 0 || cleaned.Load() != 1 {
		t.Fatalf("queued cancellation: %+v", s)
	}
	close(release)
	if s := wait(t, a); s.Err != nil || s.Result != 7 {
		t.Fatalf("request cancellation leaked: %+v", s)
	}
	if s := wait(t, b); s.Result != "ordered" {
		t.Fatalf("ordered result: %+v", s)
	}
	a.Cancel()
	if a.Snapshot().CancelRequested {
		t.Fatal("late cancel rewrote terminal receipt")
	}
	storeErr := errors.New("receipt save failed")
	e := submit(t, q, task.Work{Run: func(context.Context, func(any)) (any, error) { return "effect confirmed", nil }, Complete: func(ctx context.Context, s task.Snapshot) error {
		if ctx.Err() != nil || s.State != task.Finished || s.Result != "effect confirmed" {
			t.Error("invalid completion receipt")
		}
		return storeErr
	}})
	if s := wait(t, e); s.Err != nil || !errors.Is(s.CompletionErr, storeErr) || s.Result != "effect confirmed" {
		t.Fatalf("receipt failure replaced effect: %+v", s)
	}
}

func TestStopWaitsForWorkAndRetainsFailedOwner(t *testing.T) {
	q, _ := task.New(task.Options{Concurrency: 1, Capacity: 1})
	entered, ack := make(chan struct{}), make(chan struct{})
	failure := errors.New("resource close failed")
	var cleaned atomic.Int32
	h := submit(t, q, task.Work{Run: func(ctx context.Context, _ func(any)) (any, error) {
		close(entered)
		<-ctx.Done()
		<-ack
		return "partial", ctx.Err()
	}, Cleanup: func() error { cleaned.Add(1); return failure }})
	signal(t, entered)
	if _, err := q.Submit(context.Background(), task.Work{Run: func(context.Context, func(any)) (any, error) { return nil, nil }}); !errors.Is(err, task.ErrFull) {
		t.Fatalf("capacity: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := q.Close(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("close ignored outstanding work: %v", err)
	}
	if h.Snapshot().State != task.Running {
		t.Fatal("cancel manufactured completion")
	}
	close(ack)
	s := wait(t, h)
	if s.Result != "partial" || !errors.Is(s.Err, context.Canceled) || !errors.Is(s.CleanupErr, failure) || cleaned.Load() != 1 {
		t.Fatalf("lost result/owner: %+v", s)
	}
	if err := q.Close(context.Background()); !errors.Is(err, failure) {
		t.Fatalf("close hid cleanup: %v", err)
	}
	if _, err := q.Submit(context.Background(), task.Work{Run: func(context.Context, func(any)) (any, error) { return nil, nil }}); !errors.Is(err, task.ErrClosed) {
		t.Fatal(err)
	}
	// Cleanup failure occupies capacity even before the host closes the queue.
	q2, _ := task.New(task.Options{Concurrency: 1, Capacity: 1})
	p := submit(t, q2, task.Work{Run: func(context.Context, func(any)) (any, error) { panic("controlled") }, Cleanup: func() error { return failure }})
	if s := wait(t, p); s.Err == nil || !errors.Is(s.CleanupErr, failure) {
		t.Fatalf("panic cleanup: %+v", s)
	}
	if _, err := q2.Submit(context.Background(), task.Work{Run: func(context.Context, func(any)) (any, error) { return nil, nil }}); !errors.Is(err, task.ErrFull) {
		t.Fatalf("failed owner lost capacity: %v", err)
	}
	if err := q2.Close(context.Background()); !errors.Is(err, failure) {
		t.Fatal(err)
	}
}
