package core

import (
	"context"
	"errors"
)

type Principal string

type Action string

const (
	StatAction       Action = "stat"
	ListAction       Action = "list"
	ReadAction       Action = "read"
	CreateAction     Action = "create"
	WriteAction      Action = "write" // Explicit create-or-replace permission.
	RemoveAction     Action = "remove"
	RemoveTreeAction Action = "remove_tree" // Grants deletion and traversal of a whole subtree.
)

type Authorizer func(context.Context, Principal, Action, Address) error

func (c *Core) authorizeChecked(ctx context.Context, p Principal, a Action, target Address) error {
	if err := ctx.Err(); err != nil {
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

// authorize retains the visible grant on continuations and virtual projections.
func (c *Core) authorize(ctx context.Context, p Principal, action Action, target Address) error {
	if err := c.authorizeChecked(ctx, p, action, target); err != nil {
		return err
	}
	c.mu.Lock()
	at, _, err := c.resolveRequestLocked(ctx, target)
	c.mu.Unlock()
	if err != nil {
		return err
	}
	if at != nil {
		if !at.validExtent() {
			return Fail(InvalidPath, nil)
		}
		if err := c.authorizeBinding(ctx, p, action, at); err != nil {
			return err
		}
		c.mu.Lock()
		valid := c.validAtLocked(at)
		c.mu.Unlock()
		if !valid {
			return Fail(MountUnavailable, nil)
		}
	}
	return ctx.Err()
}

func (c *Core) authorizeBinding(ctx context.Context, p Principal, action Action, at *location) error {
	if at.prefix != at.mount.path {
		if err := c.authorizeChecked(ctx, p, action, at.primaryAddress()); err != nil {
			return err
		}
	}
	return ctx.Err()
}

func (c *Core) admit(ctx context.Context, p Principal, action Action, target Address, lifetime ...context.Context) (*use, []string, error) {
	if c == nil {
		return nil, nil, Fail(InvalidRequest, nil)
	}
	if err := c.validateInput(ctx, target); err != nil {
		return nil, nil, err
	}
	return c.admitChecked(ctx, p, action, target, lifetime...)
}

// admitChecked consumes a path already checked at the public boundary.
func (c *Core) admitChecked(ctx context.Context, p Principal, action Action, target Address, lifetime ...context.Context) (*use, []string, error) {
	if err := c.authorizeChecked(ctx, p, action, target); err != nil {
		return nil, nil, err
	}
	c.mu.Lock()
	at, names, err := c.resolveRequestLocked(ctx, target)
	c.mu.Unlock()
	if err != nil || at == nil {
		return nil, names, observedCause(ctx, err)
	}
	if !at.validExtent() {
		return nil, nil, Fail(InvalidPath, nil)
	}
	if at.prefixRange {
		return nil, nil, Fail(UnsupportedOperation, nil)
	}
	u, err := c.borrowAuthorized(ctx, p, action, at, lifetime...)
	return u, nil, err
}

// admitAt renews dynamic authority for the captured address without resolving
// paths or consulting a second selection. A stale generation cannot retarget.
func (c *Core) admitAt(ctx context.Context, p Principal, action Action, at *location, lifetime ...context.Context) (*use, error) {
	if c == nil || at == nil {
		return nil, Fail(InvalidRequest, nil)
	}
	if at.prefixRange {
		return nil, Fail(UnsupportedOperation, nil)
	}
	if err := c.authorizeChecked(ctx, p, action, at.address()); err != nil {
		return nil, err
	}
	return c.borrowAuthorized(ctx, p, action, at, lifetime...)
}

func (c *Core) borrowAuthorized(ctx context.Context, p Principal, action Action, at *location, lifetime ...context.Context) (*use, error) {
	if err := c.authorizeBinding(ctx, p, action, at); err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.borrowAtLocked(ctx, at, lifetime...)
}

func (c *Core) validAtLocked(at *location) bool {
	return !c.closed && at.mount.core == c && !at.mount.retiring && at.active && c.bindings[at.prefix] == at.binding
}

func (c *Core) borrowAtLocked(ctx context.Context, at *location, lifetime ...context.Context) (*use, error) {
	if !c.validAtLocked(at) {
		return nil, Fail(MountUnavailable, nil)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	// Only persistent opens separate admission cancellation from adopted lifetime.
	// Validation/authority still use ctx, and the original mount cancels the use.
	if len(lifetime) != 0 {
		ctx = lifetime[0]
	}
	return at.mount.borrowLocked(ctx, at), nil
}

func (c *Core) beginChange(ctx context.Context, p Principal, action Action, target Address) (*use, error) {
	u, _, err := c.admit(ctx, p, action, target)
	return changeUse(u, err)
}

func (c *Core) beginChangeAt(ctx context.Context, p Principal, action Action, at *location) (*use, error) {
	return changeUse(c.admitAt(ctx, p, action, at))
}

func changeUse(u *use, err error) (*use, error) {
	if err != nil {
		return nil, err
	}
	if u == nil {
		return nil, Fail(InvalidRequest, errors.New("a real mounted target is required"))
	}
	if err = u.changeTarget(); err != nil {
		u.release()
		return nil, err
	}
	return u, nil
}

// Preflight renews authority without borrowing resources. Execution still owns
// its admission, using exactly this captured location.
func (c *Core) preflightAt(ctx context.Context, p Principal, action Action, at *location) error {
	if err := c.authorizeChecked(ctx, p, action, at.address()); err != nil {
		return err
	}
	if err := c.authorizeBinding(ctx, p, action, at); err != nil {
		return err
	}
	c.mu.Lock()
	valid := c.validAtLocked(at)
	c.mu.Unlock()
	if !valid {
		return Fail(MountUnavailable, nil)
	}
	return at.changeTarget()
}

func (at *location) changeTarget() error {
	if at.prefixRange {
		return Fail(UnsupportedOperation, nil)
	}
	if at.namespace == KeyNamespace {
		return nil
	}
	return checkChangeTarget(at.visible(), at.path, at.prefix)
}

func checkChangeTarget(target, relative, prefix string) error {
	if relative == "/" || target == prefix {
		return Fail(InvalidRequest, errors.New("mount roots cannot be changed"))
	}
	return nil
}
