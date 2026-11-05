package core

import (
	"context"
	"errors"
	"sync"
	"time"
)

// Options supplies runtime policy, not a configuration-file format.
// Cursor lifetime belongs to each enumeration request.
type Options struct {
	Authorize   Authorizer
	MaxCursors  int
	OpenTimeout time.Duration
}

type Core struct {
	mu       sync.Mutex
	opts     Options
	mounts   map[string]*Mount
	bindings map[string]*binding
	version  uint64
	closed   bool
	tokens   map[string]*session
	sessions map[*session]struct{}
}

func New(opts Options) (*Core, error) {
	if opts.Authorize == nil || opts.MaxCursors < 0 || opts.OpenTimeout < 0 {
		return nil, errors.New("an authorizer and nonnegative optional policies are required")
	}
	return &Core{opts: opts, mounts: make(map[string]*Mount), bindings: make(map[string]*binding), tokens: make(map[string]*session), sessions: make(map[*session]struct{})}, nil
}

func (c *Core) Install(p string, d Driver) (*Mount, error) {
	if err := ValidatePath(p); err != nil {
		return nil, err
	}
	if d == nil {
		return nil, errors.New("a prepared driver is required")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, Fail(MountUnavailable, nil)
	}
	for old := range c.bindings {
		if Within(old, p) || Within(p, old) {
			return nil, Fail(PathAmbiguous, errors.New("mounts must be unique and non-nested"))
		}
	}
	c.version++
	m := &Mount{core: c, path: p, driver: d, uses: make(map[*use]struct{}), done: make(chan struct{})}
	m.keys = supportsNamespace(d, KeyNamespace)
	m.projection, _ = d.(KeyProjection)
	c.mounts[p] = m
	c.bindings[p] = &binding{prefix: p, mount: m, base: "/", active: true, keySpace: m.keys}
	return m, nil
}

func (c *Core) Close(ctx context.Context) error {
	c.mu.Lock()
	c.closed = true
	mounts := make([]*Mount, 0, len(c.mounts))
	for _, m := range c.mounts {
		mounts = append(mounts, m)
	}
	sessions := make([]*session, 0, len(c.sessions))
	for s := range c.sessions {
		sessions = append(sessions, s)
	}
	c.mu.Unlock()
	for _, s := range sessions {
		s.invalidate()
	}
	for _, m := range mounts {
		m.beginRetire()
	}
	var result error
	for _, m := range mounts {
		result = errors.Join(result, m.wait(ctx))
		if ctx.Err() != nil {
			return errors.Join(result, ctx.Err())
		}
	}
	for _, s := range sessions {
		select {
		case <-s.cleaned:
			result = errors.Join(result, s.cleanupError())
		case <-ctx.Done():
			return errors.Join(result, ctx.Err())
		}
	}
	return result
}
