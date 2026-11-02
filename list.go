package core

import (
	"context"
	"crypto/rand"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"time"
)

type ListRequest struct {
	Path   string
	Limit  int
	Cursor string
	// CursorTTL is required on the first request; zero inherits on continuation.
	CursorTTL time.Duration
}
type Page struct {
	Items           []Entry    `json:"items"`
	NextCursor      *string    `json:"next_cursor"`
	CursorExpiresAt *time.Time `json:"cursor_expires_at"`
}
type session struct {
	core                 *Core
	principal            Principal
	target               string
	limit                int
	ttl                  time.Duration
	mount                *Mount
	binding              *binding
	use                  *use
	version              uint64
	revision             uint64
	life                 context.Context
	cancel               context.CancelFunc
	mu                   sync.Mutex
	iter                 Iterator
	pending              []Metadata
	eof, iterClosed      bool
	closeErr             error
	lastInput, nextToken string
	replay               Page
	haveReplay           bool
	expires              time.Time
	timer                *time.Timer
	invalid              atomic.Bool
	cleaned              chan struct{}
}

func (c *Core) List(ctx context.Context, p Principal, r ListRequest) (Page, error) {
	if err := ValidatePath(r.Path); err != nil {
		return Page{}, err
	}
	if r.Cursor != "" {
		c.mu.Lock()
		s := c.tokens[r.Cursor]
		c.mu.Unlock()
		if s == nil || s.principal != p || s.target != r.Path || (r.Limit != 0 && r.Limit != s.limit) || (r.CursorTTL != 0 && r.CursorTTL != s.ttl) {
			return Page{}, Fail(CursorUnavailable, nil)
		}
		if err := c.authorize(ctx, p, ListAction, r.Path); err != nil {
			return Page{}, err
		}
		return s.next(ctx, r.Cursor)
	}
	if r.Limit == 0 {
		r.Limit = DefaultPage
	}
	if r.Limit < 1 || r.Limit > MaxPage || r.CursorTTL <= 0 {
		return Page{}, Fail(InvalidRequest, nil)
	}
	s, err := c.beginList(ctx, p, r.Path, r.Limit, r.CursorTTL)
	if err != nil {
		return Page{}, err
	}
	return s.next(ctx, "")
}
func (c *Core) beginList(ctx context.Context, p Principal, target string, limit int, ttl time.Duration) (*session, error) {
	if err := c.authorizePath(ctx, p, ListAction, target); err != nil {
		return nil, err
	}
	expected, err := c.constrained(ctx, target, nil)
	if err != nil {
		return nil, err
	}
	s := &session{core: c, principal: p, target: target, limit: limit, cleaned: make(chan struct{}), ttl: ttl, expires: time.Now().Add(ttl)}
	s.mu.Lock()
	c.mu.Lock()
	if c.opts.MaxCursors > 0 && len(c.sessions) >= c.opts.MaxCursors {
		c.mu.Unlock()
		s.mu.Unlock()
		return nil, Fail(CapacityExceeded, nil)
	}
	u, names, err := c.locateLocked(context.Background(), target, expected)
	if err != nil {
		c.mu.Unlock()
		s.mu.Unlock()
		return nil, err
	}
	s.use = u
	s.version = c.version
	if u != nil {
		s.mount = u.mount
		s.binding = u.binding
		s.revision = u.mount.revision
		s.life = u.ctx
		s.cancel = u.cancel
	} else {
		s.life, s.cancel = context.WithCancel(context.Background())
	}
	c.sessions[s] = struct{}{}
	c.mu.Unlock()
	s.timer = time.AfterFunc(time.Until(s.expires), s.invalidate)
	call, end := requestScope(ctx, s.life)
	if u != nil {
		err = c.authorizeBinding(call, p, ListAction, u.binding, u.path)
		if err == nil {
			s.iter, err = u.mount.driver.Enumerate(call, u.path)
		}
	} else {
		items := make([]Metadata, 0, len(names))
		for _, name := range names {
			if c.authorize(call, p, StatAction, childPath(target, name)) == nil {
				items = append(items, Metadata{Name: name, Kind: Directory})
			}
		}
		err = call.Err()
		s.iter = &virtualIterator{items: items}
	}
	end()
	if err == nil && s.iter == nil {
		err = Fail(InvalidUpstreamResponse, errors.New("enumeration has no iterator"))
	}
	if err != nil {
		s.invalidate()
	}
	s.mu.Unlock()
	if err != nil {
		return nil, err
	}
	return s, nil
}
func requestScope(ctx, lifetime context.Context) (context.Context, func()) {
	child, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(lifetime, cancel)
	if lifetime.Err() != nil {
		cancel()
	}
	return child, func() { stop(); cancel() }
}
func (s *session) next(ctx context.Context, token string) (Page, error) {
	if !s.mu.TryLock() {
		return Page{}, Fail(CapacityExceeded, nil)
	}
	defer s.mu.Unlock()
	c := s.core
	c.mu.Lock()
	valid := s.validLocked()
	c.mu.Unlock()
	if !valid {
		s.invalidate()
		return Page{}, Fail(CursorUnavailable, nil)
	}
	if s.haveReplay && token == s.lastInput {
		return clonePage(s.replay), nil
	}
	if token != s.nextToken {
		return Page{}, Fail(CursorUnavailable, nil)
	}
	page, more, err := s.advance(ctx)
	if err != nil {
		s.invalidate()
		return Page{}, err
	}
	return s.publish(token, page, more)
}

// validLocked is evaluated under the core lock at admission and publication.
func (s *session) validLocked() bool {
	return !s.core.closed && !s.invalid.Load() && time.Now().Before(s.expires) &&
		((s.mount != nil && !s.mount.retiring && s.binding.active && s.revision == s.mount.revision) || (s.mount == nil && s.version == s.core.version))
}

// advance owns only native progress and projection, under the session lock.
func (s *session) advance(ctx context.Context) (Page, bool, error) {
	call, end := requestScope(ctx, s.life)
	defer end()
	if len(s.pending) == 0 && !s.eof {
		batch, err := s.iter.Next(call, s.limit)
		if err != nil && !errors.Is(err, io.EOF) {
			return Page{}, false, err
		}
		if err := call.Err(); err != nil {
			return Page{}, false, err
		}
		s.pending = batch
		s.eof = errors.Is(err, io.EOF)
	}
	page := Page{Items: make([]Entry, 0, s.limit)}
	for len(s.pending) > 0 && len(page.Items) < s.limit {
		meta := s.pending[0]
		s.pending[0] = Metadata{}
		s.pending = s.pending[1:]
		if !ValidName(meta.Name) {
			return Page{}, false, Fail(UnrepresentableName, nil)
		}
		target := childPath(s.target, meta.Name)
		if ValidatePath(target) != nil {
			return Page{}, false, Fail(UnrepresentableName, nil)
		}
		entry, err := project(target, meta)
		if err != nil {
			return Page{}, false, err
		}
		if s.mount == nil {
			entry.Virtual = true
		}
		page.Items = append(page.Items, entry)
	}
	more := len(s.pending) > 0 || !s.eof
	if !more {
		if err := s.closeIterator(); err != nil {
			return Page{}, false, err
		}
	}
	return page, more, nil
}

// publish commits progress and replay aliases atomically with invalidation.
func (s *session) publish(token string, page Page, more bool) (Page, error) {
	c := s.core
	c.mu.Lock()
	valid := s.validLocked()
	if !valid {
		c.mu.Unlock()
		s.invalidate()
		return Page{}, Fail(CursorUnavailable, nil)
	}
	if s.lastInput != "" {
		delete(c.tokens, s.lastInput)
	}
	if s.nextToken != "" {
		delete(c.tokens, s.nextToken)
	}
	s.lastInput = token
	s.nextToken = ""
	if more {
		next := rand.Text()
		s.nextToken = next
		c.tokens[next] = s
		page.NextCursor = &next
		expires := s.expires.UTC()
		page.CursorExpiresAt = &expires
	}
	if token != "" {
		c.tokens[token] = s
		s.haveReplay = true
		s.replay = clonePage(page)
	}
	c.mu.Unlock()
	if !more && token == "" {
		s.invalidate()
	}
	return page, nil
}
func (s *session) closeIterator() error {
	if s.iterClosed {
		return s.closeErr
	}
	s.iterClosed = true
	if s.iter != nil {
		s.closeErr = s.iter.Close()
	}
	if s.use != nil {
		if s.closeErr == nil {
			s.use.release()
		} else {
			s.use.failed(s.closeErr, s.iter)
		}
	}
	return s.closeErr
}
func (s *session) invalidate() {
	if !s.invalid.CompareAndSwap(false, true) {
		return
	}
	s.cancel()
	c := s.core
	c.mu.Lock()
	if c.tokens[s.lastInput] == s {
		delete(c.tokens, s.lastInput)
	}
	if c.tokens[s.nextToken] == s {
		delete(c.tokens, s.nextToken)
	}
	c.mu.Unlock()
	go func() {
		s.mu.Lock()
		if s.timer != nil {
			s.timer.Stop()
		}
		err := s.closeIterator()
		s.pending = nil
		s.replay = Page{}
		s.mu.Unlock()
		c.mu.Lock()
		if err == nil {
			delete(c.sessions, s)
		}
		close(s.cleaned)
		c.mu.Unlock()
	}()
}
func (s *session) cleanupError() error { s.mu.Lock(); defer s.mu.Unlock(); return s.closeErr }
func clonePage(p Page) Page {
	q := Page{Items: make([]Entry, len(p.Items))}
	copy(q.Items, p.Items)
	for i := range q.Items {
		if q.Items[i].Size != nil {
			n := *q.Items[i].Size
			q.Items[i].Size = &n
		}
		if q.Items[i].ModifiedAt != nil {
			t := *q.Items[i].ModifiedAt
			q.Items[i].ModifiedAt = &t
		}
	}
	if p.NextCursor != nil {
		v := *p.NextCursor
		q.NextCursor = &v
	}
	if p.CursorExpiresAt != nil {
		v := *p.CursorExpiresAt
		q.CursorExpiresAt = &v
	}
	return q
}

type virtualIterator struct{ items []Metadata }

func (v *virtualIterator) Next(ctx context.Context, n int) ([]Metadata, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if n > len(v.items) {
		n = len(v.items)
	}
	items := v.items[:n]
	v.items = v.items[n:]
	if len(v.items) == 0 {
		return items, io.EOF
	}
	return items, nil
}
func (v *virtualIterator) Close() error { v.items = nil; return nil }

// CheckCursor validates a continuation without advancing it, for conditional
// responses. No I/O or extra enumeration is performed.
func (c *Core) CheckCursor(ctx context.Context, p Principal, r ListRequest) error {
	if err := ValidatePath(r.Path); err != nil {
		return err
	}
	if r.Cursor == "" {
		return c.authorize(ctx, p, ListAction, r.Path)
	}
	c.mu.Lock()
	s := c.tokens[r.Cursor]
	valid := s != nil && !c.closed
	if valid {
		valid = s.principal == p && s.target == r.Path && (r.Limit == 0 || r.Limit == s.limit) && (r.CursorTTL == 0 || r.CursorTTL == s.ttl) && !s.invalid.Load() && time.Now().Before(s.expires)
		if s.mount != nil {
			valid = valid && !s.mount.retiring && s.binding.active && s.revision == s.mount.revision
		} else {
			valid = valid && s.version == c.version
		}
	}
	c.mu.Unlock()
	if !valid {
		return Fail(CursorUnavailable, nil)
	}
	return c.authorize(ctx, p, ListAction, r.Path)
}

// ReleaseCursor closes an owned enumeration, including in-flight work. A revoked
// read grant does not prevent cleanup of the principal's existing resource.
func (c *Core) ReleaseCursor(ctx context.Context, p Principal, token string) error {
	c.mu.Lock()
	s := c.tokens[token]
	valid := s != nil && p != "" && s.principal == p
	c.mu.Unlock()
	if !valid {
		return Fail(CursorUnavailable, nil)
	}
	s.invalidate()
	select {
	case <-s.cleaned:
		return s.cleanupError()
	case <-ctx.Done():
		return ctx.Err()
	}
}
