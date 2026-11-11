// Package childscan works with native directory listings.
package childscan

import (
	"context"
	"errors"
	"io"
	"sync"

	core "github.com/nostalume/mountweave"
)

// Unique retains one candidate until native enumeration proves it unique.
// Only literal io.EOF confirms a complete scan and may accompany final entries.
// The caller owns page progress, validation, budgets and response cleanup.
func Unique[T any](ctx context.Context, name string, next func(context.Context) ([]T, error), identify func(T) (id, name string)) (found T, exists bool, err error) {
	var foundID string
	for {
		if err := ctx.Err(); err != nil {
			return found, exists, err
		}
		batch, err := next(ctx)
		if err != nil && err != io.EOF {
			return found, exists, err
		}
		for _, child := range batch {
			id, observed := identify(child)
			if observed != name {
				continue
			}
			if exists {
				if id == foundID {
					return found, exists, core.Fail(core.InvalidUpstreamResponse, errors.New("native child identity repeated"))
				}
				return found, exists, core.Fail(core.PathAmbiguous, nil)
			}
			found, exists, foundID = child, true, id
		}
		if err == io.EOF {
			return found, exists, ctx.Err()
		}
	}
}

// Items transfers an already-read list to an iterator. Next returns slices of
// that list, and Close releases the remaining entries.
func Items(items []core.Metadata) core.Iterator {
	return &itemsIterator{items: items}
}

type itemsIterator struct {
	mu     sync.Mutex
	items  []core.Metadata
	closed bool
}

func (i *itemsIterator) Next(ctx context.Context, n int) ([]core.Metadata, error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.closed {
		return nil, io.ErrClosedPipe
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if n < 1 {
		return nil, core.Fail(core.InvalidRequest, nil)
	}
	n = min(n, len(i.items))
	items := i.items[:n:n]
	if n == len(i.items) {
		i.items = nil
		return items, io.EOF
	}
	i.items = i.items[n:]
	return items, nil
}

func (i *itemsIterator) Close() error {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.closed, i.items = true, nil
	return nil
}
