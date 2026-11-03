package input

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"unicode/utf8"
)

// Hold durably retains a sealed source for an opaque owner key. Repeating an
// existing hold is idempotent, even after Release. New owners cannot retain a
// retired input. The owner must eventually Unhold after its durable terminal fact.
func (s *Store) Hold(ctx context.Context, id, key string) error { return s.hold(ctx, id, key, true) }

// Unhold is idempotent. It spends pre-reserved journal bytes, so a full Store
// cannot prevent an owner from relinquishing its retention.
func (s *Store) Unhold(ctx context.Context, id, key string) error { return s.hold(ctx, id, key, false) }
func (s *Store) hold(ctx context.Context, id, key string, retain bool) (err error) {
	if key == "" || !utf8.ValidString(key) {
		return ErrInvalid
	}
	e, finish, err := s.operation(ctx, id)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, finish()) }()
	s.mu.Lock()
	current, failed := e.record, e.failed
	_, exists := e.holds[key]
	s.mu.Unlock()
	if failed != nil {
		return errors.Join(ErrUncertain, failed)
	}
	if !current.Sealed {
		return ErrState
	}
	if exists == retain {
		return nil
	}
	if retain && current.Released {
		return ErrState
	}
	cost := frameSize + int64(len(key))
	if retain {
		s.mu.Lock()
		if cost > (s.opts.MaxBytes-s.used)/2 {
			s.mu.Unlock()
			return ErrCapacity
		}
		s.used += 2 * cost
		e.charge += 2 * cost
		s.mu.Unlock()
	}
	refund := func() {
		if retain {
			s.mu.Lock()
			s.used -= 2 * cost
			e.charge -= 2 * cost
			s.mu.Unlock()
		}
	}
	f, err := s.root.OpenFile(id, os.O_RDWR, 0)
	if err != nil {
		refund()
		return err
	}
	defer func() { err = errors.Join(err, s.closeFile(f, e)) }()
	if _, err = f.Seek(e.end, io.SeekStart); err != nil {
		refund()
		return err
	}
	kind := kindUnhold
	if retain {
		kind = kindHold
	}
	receipt, err := writeFrame(ctx, s.life, f, e.sum, kind, current.Offset, int64(len(key)), bytes.NewBufferString(key))
	if err != nil {
		if rollbackErr := f.Truncate(e.end); rollbackErr != nil {
			s.block(e, rollbackErr)
			return errors.Join(err, ErrUncertain, rollbackErr)
		}
		if rollbackErr := f.Sync(); rollbackErr != nil {
			s.block(e, rollbackErr)
			return errors.Join(err, ErrUncertain, rollbackErr)
		}
		refund()
		return err
	}
	if err = f.Sync(); err != nil {
		s.block(e, err)
		return errors.Join(ErrUncertain, err)
	}
	s.mu.Lock()
	e.end += receipt.size
	e.sum = receipt.sum
	if retain {
		if e.holds == nil {
			e.holds = make(map[string]struct{})
		}
		e.holds[key] = struct{}{}
	} else {
		delete(e.holds, key)
	}
	s.mu.Unlock()
	return check(ctx, s.life)
}
