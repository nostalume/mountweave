package core

import (
	"context"
	"errors"
	"io"
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

type Change struct{ Effect Effect }

// Optional capabilities share the installed Driver's lifetime. An unsupported
// mode must be refused before consuming input or changing the target.
type DirectoryMaker interface {
	Mkdir(context.Context, string) (Change, error)
}
type Remover interface {
	Remove(context.Context, string) (Change, error)
}
type FileCreator interface {
	// Create borrows input synchronously; it must not close or retain it.
	// Length, when supplied, describes the entire input, including its normal EOF.
	Create(context.Context, string, io.Reader, *int64) (Change, error)
}

// WriteOptions leaves existing targets untouched unless Replace is explicit.
// Replace publishes complete input at a name, creating it when absent.
type WriteOptions struct{ Replace bool }

// FilePublisher is optional. Publish validates complete input before changing
// the target name, creates or replaces it, and borrows input only until return.
// It does not promise conditional versions, metadata preservation or durability.
type FilePublisher interface {
	Publish(context.Context, string, io.Reader, *int64) (Change, error)
}

func writeOptions(options []WriteOptions) (WriteOptions, error) {
	if len(options) > 1 {
		return WriteOptions{}, Fail(InvalidRequest, nil)
	}
	if len(options) == 1 {
		return options[0], nil
	}
	return WriteOptions{}, nil
}

func (c *Core) beginChangeAt(ctx context.Context, p Principal, action Action, target string, expected *location) (*use, string, error) {
	u, _, err := c.admit(ctx, p, action, target, expected)
	if err != nil {
		return nil, "", err
	}
	if u == nil {
		return nil, "", Fail(InvalidRequest, errors.New("a real mounted target is required"))
	}
	rel := u.path
	if err := checkChangeTarget(target, rel, u.binding.prefix); err != nil {
		u.release()
		return nil, "", err
	}
	return u, rel, nil
}

// preflightChange checks the current grant and binding, not future permission.
// Execution must still acquire its own use and revalidate admission.
func (c *Core) preflightChange(ctx context.Context, p Principal, action Action, target string, expected *location) error {
	if c == nil {
		return Fail(InvalidRequest, nil)
	}
	if err := c.authorizePath(ctx, p, action, target); err != nil {
		return err
	}
	var err error
	expected, err = c.constrained(ctx, target, expected)
	if err != nil {
		return err
	}
	c.mu.Lock()
	at, _, err := c.resolveLocked(target, expected)
	c.mu.Unlock()
	if err != nil {
		return err
	}
	if at == nil {
		return Fail(InvalidRequest, errors.New("a real mounted target is required"))
	}
	if err := c.authorizeBinding(ctx, p, action, at.binding, at.path); err != nil {
		return err
	}
	return checkChangeTarget(target, at.path, at.binding.prefix)
}

func checkChangeTarget(target, relative, prefix string) error {
	if relative == "/" || target == prefix {
		return Fail(InvalidRequest, errors.New("mount roots cannot be changed"))
	}
	return nil
}

func (c *Core) Mkdir(ctx context.Context, p Principal, target string) (Change, error) {
	return c.mkdir(ctx, p, target, nil)
}
func (c *Core) mkdir(ctx context.Context, p Principal, target string, expected *location) (Change, error) {
	u, rel, err := c.beginChangeAt(ctx, p, CreateAction, target, expected)
	if err != nil {
		return Change{Unchanged}, err
	}
	defer u.release()
	maker, ok := u.mount.driver.(DirectoryMaker)
	if !ok {
		return Change{Unchanged}, Fail(UnsupportedOperation, nil)
	}
	if err = u.ctx.Err(); err != nil {
		return Change{Unchanged}, err
	}
	result, err := maker.Mkdir(u.ctx, rel)
	return c.finishChange(u, result, err)
}

func (c *Core) Remove(ctx context.Context, p Principal, target string) (Change, error) {
	return c.remove(ctx, p, target, nil)
}
func (c *Core) remove(ctx context.Context, p Principal, target string, expected *location) (Change, error) {
	u, rel, err := c.beginChangeAt(ctx, p, RemoveAction, target, expected)
	if err != nil {
		return Change{Unchanged}, err
	}
	defer u.release()
	remover, ok := u.mount.driver.(Remover)
	if !ok {
		return Change{Unchanged}, Fail(UnsupportedOperation, nil)
	}
	if err = u.ctx.Err(); err != nil {
		return Change{Unchanged}, err
	}
	result, err := remover.Remove(u.ctx, rel)
	return c.finishChange(u, result, err)
}

// Create writes a new file without replacing an existing target. The caller
// owns input and keeps it valid until this synchronous call returns.
func (c *Core) Create(ctx context.Context, p Principal, target string, input io.Reader, length *int64) (Change, error) {
	return c.Write(ctx, p, target, input, length)
}

// Write shares Create's input ownership. The default creates a new file;
// explicit Replace requires WriteAction and complete-publication capability.
func (c *Core) Write(ctx context.Context, p Principal, target string, input io.Reader, length *int64, options ...WriteOptions) (Change, error) {
	write, err := writeOptions(options)
	if err != nil {
		return Change{Unchanged}, err
	}
	return c.write(ctx, p, target, input, length, write, nil)
}

func (c *Core) write(ctx context.Context, p Principal, target string, input io.Reader, length *int64, write WriteOptions, expected *location) (Change, error) {
	if input == nil || (length != nil && *length < 0) {
		return Change{Unchanged}, Fail(InvalidRequest, nil)
	}
	if length != nil {
		n := *length
		length = &n
	}
	action := CreateAction
	if write.Replace {
		action = WriteAction
	}
	u, rel, err := c.beginChangeAt(ctx, p, action, target, expected)
	if err != nil {
		return Change{Unchanged}, err
	}
	defer u.release()
	if write.Replace {
		publisher, ok := u.mount.driver.(FilePublisher)
		if !ok {
			return Change{Unchanged}, Fail(UnsupportedOperation, nil)
		}
		if err = u.ctx.Err(); err != nil {
			return Change{Unchanged}, err
		}
		result, err := publisher.Publish(u.ctx, rel, input, length)
		if result.Effect == Incomplete {
			result.Effect = Unknown
			err = Fail(InvalidUpstreamResponse, errors.Join(errors.New("publisher reported a partial target"), err))
		}
		return c.finishChange(u, result, err)
	}
	creator, ok := u.mount.driver.(FileCreator)
	if !ok {
		return Change{Unchanged}, Fail(UnsupportedOperation, nil)
	}
	if err = u.ctx.Err(); err != nil {
		return Change{Unchanged}, err
	}
	result, err := creator.Create(u.ctx, rel, input, length)
	return c.finishChange(u, result, err)
}

// finishChange preserves receipts after cancellation and makes cursor admission
// and publication observe one mount-local mutation boundary.
func (c *Core) finishChange(u *use, result Change, err error) (Change, error) {
	if result.Effect > Incomplete || (result.Effect == Unchanged && err == nil) {
		result.Effect = Unknown
		err = errors.Join(err, Fail(InvalidUpstreamResponse, errors.New("invalid mutation receipt")))
	}
	if err == nil && result.Effect != Applied {
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
