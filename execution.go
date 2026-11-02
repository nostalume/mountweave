package core

import (
	"context"
	"errors"
)

// ExecutionOptions applies within one finite call, not across independent
// requests or external writers. Zero and one select serial execution.
type ExecutionOptions struct{ Concurrency int }

func concurrency(options []ExecutionOptions) (int, error) {
	if len(options) > 1 {
		return 0, Fail(InvalidRequest, nil)
	}
	n := 1
	if len(options) == 1 {
		if options[0].Concurrency < 0 {
			return 0, Fail(InvalidRequest, nil)
		}
		if options[0].Concurrency > 0 {
			n = options[0].Concurrency
		}
	}
	return n, nil
}

type access struct {
	at    *location
	write bool
}
type work[T any] struct {
	accesses []access
	run      func() (T, error)
}

func conflict(a, b []access) bool {
	for _, x := range a {
		for _, y := range b {
			if x.at == nil || y.at == nil {
				return true
			}
			if x.at.binding.mount == y.at.binding.mount && (x.write || y.write) &&
				(Within(x.at.path, y.at.path) || Within(y.at.path, x.at.path)) {
				return true
			}
		}
	}
	return false
}

func observedCause(ctx context.Context, err error) error {
	if cause := ctx.Err(); cause != nil && !errors.Is(err, cause) {
		return errors.Join(err, cause)
	}
	return err
}

// execute keeps a contiguous started prefix and never bypasses a blocked head.
// All endpoint ranges belong to one work item and are admitted together.
func execute[T any](ctx context.Context, count, limit int, results []T, prepare func(int) work[T]) ([]T, error) {
	base := len(results)
	if count == 0 {
		return results, ctx.Err()
	}
	limit = min(limit, count)
	if limit == 1 {
		for i := 0; i < count; i++ {
			if err := ctx.Err(); err != nil {
				return results, err
			}
			value, err := prepare(i).run()
			results = append(results, value)
			if err != nil {
				return results, observedCause(ctx, err)
			}
		}
		return results, ctx.Err()
	}
	type completed struct {
		index int
		value T
		err   error
	}
	done := make(chan completed, limit)
	active := make(map[int][]access)
	var stop error
	finish := func(item completed) {
		results[item.index] = item.value
		delete(active, item.index)
		if item.err != nil {
			stop = errors.Join(stop, item.err)
		}
	}
	var pending *work[T]
	next := 0
	cancel := ctx.Done()
	for next < count || len(active) != 0 {
		// Observe completions before admitting more work.
	drain:
		for {
			select {
			case item := <-done:
				finish(item)
			default:
				break drain
			}
		}
		stop = observedCause(ctx, stop)
		if stop != nil {
			if len(active) == 0 {
				break
			}
			finish(<-done)
			continue
		}
		if next < count && len(active) < limit {
			if pending == nil {
				item := prepare(next)
				pending = &item
			}
			blocked := false
			for _, ranges := range active {
				if conflict(pending.accesses, ranges) {
					blocked = true
					break
				}
			}
			if !blocked {
				index, item := base+next, *pending
				var zero T
				results = append(results, zero)
				active[index] = item.accesses
				next++
				pending = nil
				go func() {
					value, err := item.run()
					done <- completed{index: index, value: value, err: err}
				}()
				continue
			}
		}
		if len(active) == 0 {
			break
		}
		select {
		case item := <-done:
			finish(item)
		case <-cancel:
			stop = observedCause(ctx, stop)
			cancel = nil
		}
	}
	return results, observedCause(ctx, stop)
}
