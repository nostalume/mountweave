package core

import (
	"context"
	"errors"
	"path"
	"sort"
	"strings"
)

// binding is an immutable directory mapping. active is guarded by Core.mu.
type binding struct {
	prefix    string
	mount     *Mount
	base      string
	keyPrefix string
	keySpace  bool
	active    bool
}

type location struct {
	*binding
	path        string
	namespace   Namespace
	prefixRange bool
}

func (b *binding) keyBase() string {
	return b.keyPrefix
}

func (at *location) address() Address {
	if at.namespace == TreeNamespace {
		return Path(at.visible())
	}
	a := Key(at.prefix, strings.TrimPrefix(at.path, at.keyBase()))
	a.prefix = at.prefixRange
	return a
}

func (at *location) primaryAddress() Address {
	if at.namespace == TreeNamespace {
		return Path(at.primary())
	}
	a := Key(at.mount.path, at.path)
	a.prefix = at.prefixRange
	return a
}

func (at *location) native() NativeAddress {
	return NativeAddress{namespace: at.namespace, value: at.path}
}

func (at *location) validExtent() bool {
	return at.namespace == KeyNamespace || pathExtent(at.primary())
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
	meta, err := mount.driver.Stat(u.ctx, NativePath(base))
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
	b := &binding{prefix: prefix, mount: mount, base: base, active: true, keySpace: mount.keys && base == "/"}
	if mount.keys && base != "/" && mount.projection != nil {
		b.keyPrefix, b.keySpace = mount.projection.KeyRoot(NativePath(base))
	}
	c.bindings[prefix] = b
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
	for prefix := target; ; prefix = path.Dir(prefix) {
		if b := c.bindings[prefix]; b != nil && b.active {
			return &location{binding: b, path: joinPath(b.base, relative(target, prefix))}
		}
		if prefix == "/" {
			break
		}
	}
	return nil
}

func (c *Core) capture(ctx context.Context, target Address) *location {
	if c == nil || c.validateInput(ctx, target) != nil {
		return nil
	}
	return c.captureChecked(ctx, target)
}

func (c *Core) captureChecked(ctx context.Context, target Address) *location {
	c.mu.Lock()
	defer c.mu.Unlock()
	at, _, err := c.resolveRequestLocked(ctx, target)
	if err != nil || at == nil || !c.validAtLocked(at) || !at.validExtent() {
		return nil
	}
	return at
}

// resolveLocked reads the live namespace. Selected requests use their own map.
func (c *Core) resolveLocked(p string) (*location, []string, error) {
	if c.closed {
		return nil, nil, Fail(MountUnavailable, nil)
	}
	if at := c.locationLocked(p); at != nil {
		if at.mount.retiring {
			return nil, nil, Fail(MountUnavailable, nil)
		}
		return at, nil, nil
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

// BindingInfo is an observation, never an execution credential.
type BindingInfo struct {
	Mount *Mount
	Base  string
}

func (c *Core) Binding(prefix string) (BindingInfo, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	b := c.bindings[prefix]
	if c.closed || b == nil || !b.active || b.mount.retiring {
		return BindingInfo{}, false
	}
	return BindingInfo{Mount: b.mount, Base: b.base}, true
}
