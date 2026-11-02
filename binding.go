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
	binding *binding
	path    string
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
	u := mount.borrowLocked(ctx)
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

func (c *Core) capture(target string) *location {
	if c == nil || ValidatePath(target) != nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil
	}
	at := c.locationLocked(target)
	if at != nil && at.binding.mount.retiring {
		return nil
	}
	return at
}

// authorize retains the visible grant on continuations and virtual projections.
func (c *Core) authorize(ctx context.Context, p Principal, action Action, target string) error {
	if err := c.authorizePath(ctx, p, action, target); err != nil {
		return err
	}
	at := c.capture(target)
	if _, constrained := ctx.Value(selectionKey{}).(*Selection); constrained {
		c.mu.Lock()
		expected, err := c.constrained(ctx, target, nil)
		if err == nil {
			at, _, err = c.resolveLocked(target, expected)
		}
		c.mu.Unlock()
		if err != nil {
			return err
		}
	}
	if at != nil && at.binding.prefix != at.binding.mount.path {
		return c.authorizePath(ctx, p, action, joinPath(at.binding.mount.path, at.path))
	}
	return nil
}

func (c *Core) authorizeBinding(ctx context.Context, p Principal, action Action, b *binding, relative string) error {
	if b.prefix != b.mount.path {
		if err := c.authorizePath(ctx, p, action, joinPath(b.mount.path, relative)); err != nil {
			return err
		}
	}
	c.mu.Lock()
	valid := !c.closed && !b.mount.retiring && b.active && c.bindings[b.prefix] == b
	c.mu.Unlock()
	if !valid {
		return Fail(MountUnavailable, nil)
	}
	return ctx.Err()
}

func (c *Core) admit(ctx context.Context, p Principal, action Action, target string, expected *location) (*use, []string, error) {
	if c == nil {
		return nil, nil, Fail(InvalidRequest, nil)
	}
	if err := c.authorizePath(ctx, p, action, target); err != nil {
		return nil, nil, err
	}
	c.mu.Lock()
	u, names, err := c.locateLocked(ctx, target, expected)
	c.mu.Unlock()
	if err != nil || u == nil {
		return u, names, err
	}
	if err = c.authorizeBinding(ctx, p, action, u.binding, u.path); err != nil {
		u.release()
		return nil, nil, err
	}
	return u, names, nil
}
