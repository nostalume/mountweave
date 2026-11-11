package core

import (
	"context"
	"errors"
)

// Effect describes target mutation independently of an operation's error.
// The zero value is conservative: no absence of effects has been established.
type Effect uint8

const (
	Unknown Effect = iota
	Unchanged
	Applied
	Incomplete
)

func (e Effect) String() string {
	switch e {
	case Unchanged:
		return "unchanged"
	case Applied:
		return "applied"
	case Incomplete:
		return "incomplete"
	default:
		return "unknown"
	}
}

type Change struct {
	Effect   Effect
	Before   TargetPresence
	Evidence Evidence
}

// Optional capabilities share the installed Driver's lifetime. An unsupported
// mode must be refused before consuming input or changing the target.
type DirectoryMaker interface {
	Mkdir(context.Context, NativeAddress) (Change, error)
}

// Remover removes one native entry. Tree ancestors must not follow links, and
// a final link is removed itself. Type and condition constraints must hold at
// the native effect point; a preceding Stat cannot establish them. Native calls
// finish before return, and failed cleanup remains owned by the Driver.
type Remover interface {
	Remove(context.Context, NativeAddress, ...RemoveOptions) (Change, error)
}

type RemoveTarget string

const (
	RemoveNonDirectory   RemoveTarget = "non_directory"
	RemoveEmptyDirectory RemoveTarget = "empty_directory"
)

type RemoveOptions struct {
	When Condition `json:"when"`
	// Target constrains the native deletion. Zero allows either kind.
	Target RemoveTarget `json:"target,omitempty"`
}

func (c *Core) Mkdir(ctx context.Context, p Principal, target Address) (result Change, err error) {
	result.Effect = Unchanged
	ctx, turn, err := enterOrder(ctx, nil)
	if err != nil {
		return result, err
	}
	defer func() { turn.finish(result.Effect == Unknown) }()
	return c.mkdirUse(c.beginChange(ctx, p, CreateAction, target))
}

func (c *Core) mkdirAt(ctx context.Context, p Principal, at *location) (Change, error) {
	return c.mkdirUse(c.beginChangeAt(ctx, p, CreateAction, at))
}

func (c *Core) mkdirUse(u *use, err error) (Change, error) {
	if err != nil {
		return Change{Effect: Unchanged}, err
	}
	defer u.release()
	if u.namespace != TreeNamespace {
		return Change{Effect: Unchanged}, Fail(UnsupportedOperation, nil)
	}
	maker, ok := u.mount.driver.(DirectoryMaker)
	if !ok {
		return Change{Effect: Unchanged}, Fail(UnsupportedOperation, nil)
	}
	if err = u.ctx.Err(); err != nil {
		return Change{Effect: Unchanged}, err
	}
	result, err := maker.Mkdir(u.ctx, u.native())
	return c.finishChange(u, result, err)
}

func (c *Core) Remove(ctx context.Context, p Principal, target Address, policies ...RemoveOptions) (result Change, err error) {
	result.Effect = Unchanged
	ctx, turn, err := enterOrder(ctx, nil)
	if err != nil {
		return result, err
	}
	defer func() { turn.finish(result.Effect == Unknown) }()
	if len(policies) > 1 {
		return Change{Effect: Unchanged}, Fail(InvalidRequest, nil)
	}
	var policy RemoveOptions
	if len(policies) == 1 {
		policy = policies[0]
	}
	if policy.Target != "" && policy.Target != RemoveNonDirectory && policy.Target != RemoveEmptyDirectory {
		return result, Fail(InvalidRequest, nil)
	}
	if err := policy.When.Validate(); err != nil {
		return Change{Effect: Unchanged}, err
	}
	u, err := c.beginChange(ctx, p, RemoveAction, target)
	return c.removeUse(u, policy, err)
}

func (c *Core) removeAt(ctx context.Context, p Principal, at *location) (Change, error) {
	u, err := c.beginChangeAt(ctx, p, RemoveAction, at)
	return c.removeUse(u, RemoveOptions{}, err)
}

func (c *Core) removeUse(u *use, options RemoveOptions, err error) (Change, error) {
	if err != nil {
		return Change{Effect: Unchanged}, err
	}
	defer u.release()
	remover, ok := u.mount.driver.(Remover)
	if !ok {
		return Change{Effect: Unchanged}, Fail(UnsupportedOperation, nil)
	}
	if err := u.ctx.Err(); err != nil {
		return Change{Effect: Unchanged}, err
	}
	result, err := remover.Remove(u.ctx, u.native(), options)
	return c.finishChange(u, result, err)
}

// finishChange preserves receipts after cancellation and makes cursor admission
// and publication observe one mount-local mutation boundary.
func (c *Core) finishChange(u *use, result Change, err error, allowIdle ...bool) (Change, error) {
	idle := len(allowIdle) != 0 && allowIdle[0] && result.Effect == Unchanged
	if result.Evidence.Validator != (Validator{}) && !result.Evidence.Validator.Valid() {
		result.Evidence.Validator = Validator{}
		err = errors.Join(err, Fail(InvalidUpstreamResponse, errors.New("invalid native mutation validator")))
	}
	if result.Effect > Incomplete || result.Before > TargetPresent || (result.Effect == Unchanged && err == nil && !idle) {
		result.Effect = Unknown
		err = errors.Join(err, Fail(InvalidUpstreamResponse, errors.New("invalid mutation receipt")))
	}
	if err == nil && result.Effect != Applied && !idle {
		err = Fail(InvalidUpstreamResponse, errors.New("mutation did not confirm completion"))
	}
	if cause := u.ctx.Err(); cause != nil && !errors.Is(err, cause) {
		err = errors.Join(err, cause)
	}
	if result.Effect != Unchanged {
		c.mu.Lock()
		u.mount.revision++
		var stale []*session
		for s := range c.sessions {
			if s.mount == u.mount {
				stale = append(stale, s)
			}
		}
		c.mu.Unlock()
		for _, s := range stale {
			s.invalidate()
		}
	}
	return result, err
}
