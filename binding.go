package core

import (
	"context"
	"errors"
)

// binding is an immutable directory mapping. active is guarded by Core.mu.
type binding struct {
	prefix string
	mount  *Mount
	base   string
	active bool
}

type location struct {
	*binding
	path string
}

// A location is one immutable captured access route and its rooted address.
// Visible and primary policy paths are projections, never independent inputs.
func (at *location) visible() string { return joinPath(at.prefix, relative(at.path, at.base)) }
func (at *location) primary() string { return joinPath(at.mount.path, at.path) }

// descend receives a relative path admitted by selection or bounded discovery.
func (at *location) descend(rel string) *location {
	return &location{binding: at.binding, path: joinPath(at.path, rel)}
}

func joinPath(root, rel string) string {
	if rel == "/" {
		return root
	}
	if root == "/" {
		return rel
	}
	return root + rel
}

// BindAlias exposes a directory of an installed mount under a new, non-nested
// prefix. It owns no Driver and grants no permission beyond the primary path.
func (c *Core) BindAlias(ctx context.Context, prefix string, mount *Mount, base string) error {
	if c == nil || mount == nil || mount.core != c {
		return Fail(InvalidRequest, nil)
	}
	if err := ValidatePath(prefix); err != nil {
		return err
	}
	if err := ValidatePath(base); err != nil {
		return err
	}
	if err := ValidatePath(joinPath(mount.path, base)); err != nil {
		return err
	}
	c.mu.Lock()
	if c.closed || mount.retiring || c.mounts[mount.path] != mount {
		c.mu.Unlock()
		return Fail(MountUnavailable, nil)
	}
	u := mount.borrowLocked(ctx, &location{binding: c.bindings[mount.path], path: base})
	c.mu.Unlock()
	defer u.release()
	meta, err := mount.driver.Stat(u.ctx, base)
	if err != nil {
		return err
	}
	if err = checkName(meta, base); err != nil {
		return err
	}
	if meta.Kind != Directory {
		return Fail(NotDirectory, nil)
	}
	if err = u.ctx.Err(); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || mount.retiring {
		return Fail(MountUnavailable, nil)
	}
	for old := range c.bindings {
		if Within(old, prefix) || Within(prefix, old) {
			return Fail(PathAmbiguous, nil)
		}
	}
	c.bindings[prefix] = &binding{prefix: prefix, mount: mount, base: base, active: true}
	c.version++
	return nil
}

// UnbindAlias removes only an alias and its cursors. Open bodies keep their
// captured mount use. Context bounds cleanup waiting, not native cleanup work.
func (c *Core) UnbindAlias(ctx context.Context, prefix string) error {
	if c == nil {
		return Fail(InvalidRequest, nil)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	c.mu.Lock()
	b := c.bindings[prefix]
	if b == nil || b.prefix == b.mount.path {
		c.mu.Unlock()
		return Fail(InvalidRequest, nil)
	}
	b.active = false
	delete(c.bindings, prefix)
	c.version++
	var cursors []*session
	for s := range c.sessions {
		if s.binding == b {
			cursors = append(cursors, s)
		}
	}
	c.mu.Unlock()
	for _, s := range cursors {
		s.invalidate()
	}
	var err error
	for _, s := range cursors {
		select {
		case <-s.cleaned:
			err = errors.Join(err, s.cleanupError())
		case <-ctx.Done():
			return errors.Join(err, ctx.Err())
		}
	}
	return err
}

func (c *Core) locationLocked(target string) *location {
	for prefix, b := range c.bindings {
		if Within(target, prefix) && b.active {
			return &location{binding: b, path: joinPath(b.base, relative(target, prefix))}
		}
	}
	return nil
}

func (c *Core) capture(ctx context.Context, target string) *location {
	if c == nil || ValidatePath(target) != nil {
		return nil
	}
	return c.captureChecked(ctx, target)
}
func (c *Core) captureChecked(ctx context.Context, target string) *location {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil
	}
	expected, err := c.constrained(ctx, target, nil)
	if err != nil {
		return nil
	}
	at, _, err := c.resolveLocked(target, expected)
	if err != nil {
		return nil
	}
	return at
}

// authorize retains the visible grant on continuations and virtual projections.
func (c *Core) authorize(ctx context.Context, p Principal, action Action, target string) error {
	if err := c.authorizePath(ctx, p, action, target); err != nil {
		return err
	}
	expected, err := c.constrained(ctx, target, nil)
	if err != nil {
		return err
	}
	c.mu.Lock()
	at, _, err := c.resolveLocked(target, expected)
	c.mu.Unlock()
	if err != nil {
		return err
	}
	if at != nil {
		return c.authorizeBinding(ctx, p, action, at)
	}
	return ctx.Err()
}

func (c *Core) authorizeBinding(ctx context.Context, p Principal, action Action, at *location) error {
	if at.prefix != at.mount.path {
		if err := c.authorizePath(ctx, p, action, at.primary()); err != nil {
			return err
		}
	}
	c.mu.Lock()
	valid := !c.closed && !at.mount.retiring && at.active && c.bindings[at.prefix] == at.binding
	c.mu.Unlock()
	if !valid {
		return Fail(MountUnavailable, nil)
	}
	return ctx.Err()
}

func (c *Core) admit(ctx context.Context, p Principal, action Action, target string) (*use, []string, error) {
	if c == nil {
		return nil, nil, Fail(InvalidRequest, nil)
	}
	if err := c.authorizePath(ctx, p, action, target); err != nil {
		return nil, nil, err
	}
	expected, err := c.constrained(ctx, target, nil)
	if err != nil {
		return nil, nil, err
	}
	c.mu.Lock()
	u, names, err := c.locateLocked(ctx, target, expected)
	c.mu.Unlock()
	if err != nil || u == nil {
		return u, names, err
	}
	if err = c.authorizeBinding(ctx, p, action, u.location); err != nil {
		u.release()
		return nil, nil, err
	}
	return u, names, nil
}

// admitAt renews dynamic authority for the captured address without resolving
// paths or consulting a second selection. A stale generation cannot retarget.
func (c *Core) admitAt(ctx context.Context, p Principal, action Action, at *location) (*use, error) {
	if c == nil || at == nil {
		return nil, Fail(InvalidRequest, nil)
	}
	if err := c.authorizeChecked(ctx, p, action, at.visible()); err != nil {
		return nil, err
	}
	c.mu.Lock()
	u, err := c.borrowAtLocked(ctx, at)
	c.mu.Unlock()
	if err != nil {
		return nil, err
	}
	if err = c.authorizeBinding(ctx, p, action, at); err != nil {
		u.release()
		return nil, err
	}
	return u, nil
}
func (c *Core) borrowAtLocked(ctx context.Context, at *location) (*use, error) {
	if c.closed || at.mount.core != c || at.mount.retiring || !at.active || c.bindings[at.prefix] != at.binding {
		return nil, Fail(MountUnavailable, nil)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return at.mount.borrowLocked(ctx, at), nil
}
