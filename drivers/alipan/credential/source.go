// Package credential serializes one rotating token's external and persistence effects.
package credential

import (
	"context"
	"errors"
	"sync"
	"time"
)

var (
	ErrClosed           = errors.New("credential source is closed")
	ErrRefreshUncertain = errors.New("refresh outcome requires recovery")
	ErrPersistence      = errors.New("rotated credential is not durably saved")
	ErrUnusableAccess   = errors.New("refreshed access token is unusable")
)

type Token struct {
	Access, Refresh string
	ExpiresAt       time.Time
}

// A nonempty returned Refresh reports a known credential even on error;
// Source retains and saves it before reporting the associated failure.
type Refresh func(context.Context, string) (Token, error)

// Record is a credential checkpoint, not a product configuration format.
// Pending marks a rotation whose known result is not durably confirmed.
type Record struct {
	Token   Token
	Pending bool
}

// Save succeeds only after a complete, durable replacement. The host owns
// encoding, exclusive writer admission, and storage; loads must not fall back
// from an incomplete or pending record to an older confirmed credential.
type Save func(context.Context, Record) error

// Source owns one refresh writer. Cancellation of a waiter does not cancel a
// shared rotation. Close cancels and waits for that writer's acknowledgement.
type Source struct {
	mu             sync.Mutex
	token          Token
	refresh        Refresh
	save           Save
	life           context.Context
	cancel         context.CancelFunc
	pending        chan struct{}
	failed         error
	refreshFailure error
	dirty, closed  bool
}

// New accepts the latest durably confirmed checkpoint under one writer.
// An unresolved checkpoint requires explicit reconciliation or new authorization.
func New(initial Record, refresh Refresh, save Save) (*Source, error) {
	if initial.Pending {
		return nil, ErrRefreshUncertain
	}
	if initial.Token.Refresh == "" || refresh == nil || save == nil {
		return nil, errors.New("an initial refresh token, refresh operation, and durable save operation are required")
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Source{token: initial.Token, refresh: refresh, save: save, life: ctx, cancel: cancel}, nil
}
func (s *Source) Access(ctx context.Context) (string, error) { return s.access(ctx, "") }
func (s *Source) Reject(ctx context.Context, rejected string) (string, error) {
	if rejected == "" {
		return "", errors.New("a rejected access token is required")
	}
	return s.access(ctx, rejected)
}
func (s *Source) access(ctx context.Context, rejected string) (string, error) {
	for {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			return "", ErrClosed
		}
		if s.failed != nil {
			err := s.failed
			s.mu.Unlock()
			return "", err
		}
		if s.pending == nil && s.token.Access != "" && time.Now().Before(s.token.ExpiresAt) && rejected != s.token.Access {
			token := s.token.Access
			s.mu.Unlock()
			return token, nil
		}
		if s.pending == nil {
			s.startLocked(false)
		}
		done := s.pending
		s.mu.Unlock()
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-done:
			rejected = ""
		}
	}
}
func (s *Source) startLocked(saveOnly bool) {
	s.pending = make(chan struct{})
	token := s.token
	go func() {
		var err error
		if saveOnly {
			s.mu.Lock()
			err = s.refreshFailure
			s.mu.Unlock()
		}
		known := saveOnly
		if !saveOnly {
			if commitErr := s.save(s.life, Record{Token: token, Pending: true}); commitErr != nil {
				// No external refresh was invoked. Reload/reconcile the checkpoint;
				// RetrySave is reserved for a known rotated credential.
				err = errors.Join(ErrPersistence, commitErr)
			} else if cause := s.life.Err(); cause != nil {
				err = cause
			} else {
				token, err = s.refresh(s.life, token.Refresh)
				known = token.Refresh != ""
				if err == nil && !known {
					err = errors.New("refresh returned an unusable token")
				}
				if err != nil {
					err = errors.Join(ErrRefreshUncertain, err)
				}
				s.mu.Lock()
				s.refreshFailure = err
				s.mu.Unlock()
			}
		}
		if known {
			// Preserve the new refresh token even when persistence fails. Never retry the old token.
			s.mu.Lock()
			s.token = token
			s.dirty = true
			s.mu.Unlock()
			if saveErr := s.save(s.life, Record{Token: token}); saveErr != nil {
				err = errors.Join(err, ErrPersistence, saveErr)
			} else {
				s.mu.Lock()
				s.dirty = false
				s.mu.Unlock()
				if !saveOnly && (token.Access == "" || !time.Now().Before(token.ExpiresAt)) {
					err = errors.Join(err, ErrUnusableAccess)
				}
			}
		}
		s.mu.Lock()
		s.failed = err
		if err == nil {
			s.dirty = false
		}
		close(s.pending)
		s.pending = nil
		s.mu.Unlock()
	}()
}

// RetrySave retries only persistence of the retained rotated credential.
// An uncertain refresh cannot be retried by this operation.
func (s *Source) RetrySave(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			return ErrClosed
		}
		if s.pending != nil {
			done := s.pending
			s.mu.Unlock()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-done:
			}
			continue
		}
		if !s.dirty || !errors.Is(s.failed, ErrPersistence) {
			err := s.failed
			s.mu.Unlock()
			return err
		}
		s.startLocked(true)
		done := s.pending
		s.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-done:
			s.mu.Lock()
			err := s.failed
			s.mu.Unlock()
			return err
		}
	}
}
func (s *Source) Close() error {
	s.mu.Lock()
	s.closed = true
	s.cancel()
	done := s.pending
	s.mu.Unlock()
	if done != nil {
		<-done
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.dirty || errors.Is(s.failed, ErrRefreshUncertain) {
		return s.failed
	}
	return nil
}
