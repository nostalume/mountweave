package core

import (
	"context"
	"errors"
	"sort"
	"strings"
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
	c.mounts[p] = m
	c.bindings[p] = &binding{prefix: p, mount: m, base: "/", active: true}
	return m, nil
}
func (c *Core) authorizePath(ctx context.Context, p Principal, a Action, target string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := ValidatePath(target); err != nil {
		return err
	}
	if p == "" {
		return Fail(Unauthenticated, nil)
	}
	if err := c.opts.Authorize(ctx, p, a, target); err != nil {
		return Fail(PermissionDenied, err)
	}
	return nil
}

// locateLocked registers a use in the same critical section as resolution.
func (c *Core) locateLocked(ctx context.Context, p string, expected *location) (*use, []string, error) {
	expected, err := c.constrained(ctx, p, expected)
	if err != nil {
		return nil, nil, err
	}
	at, names, err := c.resolveLocked(p, expected)
	if err != nil || at == nil {
		return nil, names, err
	}
	u := at.binding.mount.borrowLocked(ctx)
	u.binding, u.path = at.binding, at.path
	return u, nil, nil
}

// resolveLocked checks captured locations without borrowing runtime resources.
func (c *Core) resolveLocked(p string, expected *location) (*location, []string, error) {
	if c.closed {
		return nil, nil, Fail(MountUnavailable, nil)
	}
	at := expected
	if at == nil || !at.binding.active || c.bindings[at.binding.prefix] != at.binding ||
		!Within(p, at.binding.prefix) || at.path != joinPath(at.binding.base, relative(p, at.binding.prefix)) {
		at = c.locationLocked(p)
	}
	if at != nil {
		if at.binding.mount.retiring {
			return nil, nil, Fail(MountUnavailable, nil)
		}
		if expected != nil && (expected.binding != at.binding || expected.path != at.path) {
			return nil, nil, Fail(MountUnavailable, nil)
		}
		return at, nil, nil
	}
	if expected != nil {
		return nil, nil, Fail(MountUnavailable, nil)
	}
	names := make(map[string]bool)
	for root, b := range c.bindings {
		if !b.active || b.mount.retiring {
			continue
		}
		prefix := p
		if prefix != "/" {
			prefix += "/"
		}
		if strings.HasPrefix(root, prefix) {
			rest := strings.TrimPrefix(root, prefix)
			if rest != "" {
				names[strings.SplitN(rest, "/", 2)[0]] = true
			}
		}
	}
	if p != "/" && len(names) == 0 {
		return nil, nil, Fail(NotFound, nil)
	}
	children := make([]string, 0, len(names))
	for name := range names {
		children = append(children, name)
	}
	sort.Strings(children)
	return nil, children, nil
}
func (c *Core) Stat(ctx context.Context, p Principal, target string) (Entry, error) {
	return c.Inspect(ctx, p, StatAction, target)
}

// Inspect checks metadata under the requested action without opening content.
func (c *Core) Inspect(ctx context.Context, p Principal, action Action, target string) (Entry, error) {
	if action != StatAction && action != ListAction && action != ReadAction {
		return Entry{}, Fail(InvalidRequest, nil)
	}
	u, _, err := c.admit(ctx, p, action, target, nil)
	if err != nil {
		return Entry{}, err
	}
	if u == nil {
		return virtualEntry(target), nil
	}
	defer u.release()
	rel := u.path
	meta, err := u.mount.driver.Stat(u.ctx, rel)
	if err != nil {
		return Entry{}, err
	}
	if err = u.ctx.Err(); err != nil {
		return Entry{}, err
	}
	if err = checkName(meta, rel); err != nil {
		return Entry{}, err
	}
	return project(target, meta)
}
func checkName(m Metadata, rel string) error {
	if rel != "/" {
		name := rel[strings.LastIndexByte(rel, '/')+1:]
		if !ValidName(m.Name) {
			return Fail(UnrepresentableName, nil)
		}
		if name != m.Name {
			return Fail(InvalidUpstreamResponse, errors.New("resolved name differs from the requested name"))
		}
	}
	return nil
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
