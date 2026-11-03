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
type WriteOptions struct {
	Replace      bool
	Continuation *WriteContinuation
}

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

func (c *Core) beginChange(ctx context.Context, p Principal, action Action, target string) (*use, error) {
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
	if err = checkChangeTarget(u.visible(), u.path, u.prefix); err != nil {
		u.release()
		return nil, err
	}
	return u, nil
}

// Preflight renews authority without borrowing resources. Execution still owns
// its admission, using exactly this captured location.
func (c *Core) preflightAt(ctx context.Context, p Principal, action Action, at *location) error {
	if err := c.authorizeChecked(ctx, p, action, at.visible()); err != nil {
		return err
	}
	if err := c.authorizeBinding(ctx, p, action, at); err != nil {
		return err
	}
	return checkChangeTarget(at.visible(), at.path, at.prefix)
}

func checkChangeTarget(target, relative, prefix string) error {
	if relative == "/" || target == prefix {
		return Fail(InvalidRequest, errors.New("mount roots cannot be changed"))
	}
	return nil
}

func (c *Core) Mkdir(ctx context.Context, p Principal, target string) (Change, error) {
	return c.mkdirUse(c.beginChange(ctx, p, CreateAction, target))
}
func (c *Core) mkdirAt(ctx context.Context, p Principal, at *location) (Change, error) {
	return c.mkdirUse(c.beginChangeAt(ctx, p, CreateAction, at))
}
func (c *Core) mkdirUse(u *use, err error) (Change, error) {
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
	result, err := maker.Mkdir(u.ctx, u.path)
	return c.finishChange(u, result, err)
}
func (c *Core) Remove(ctx context.Context, p Principal, target string) (Change, error) {
	return c.removeUse(c.beginChange(ctx, p, RemoveAction, target))
}
func (c *Core) removeAt(ctx context.Context, p Principal, at *location) (Change, error) {
	return c.removeUse(c.beginChangeAt(ctx, p, RemoveAction, at))
}
func (c *Core) removeUse(u *use, err error) (Change, error) {
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
	result, err := remover.Remove(u.ctx, u.path)
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
	if input == nil || (length != nil && *length < 0) {
		return Change{Unchanged}, Fail(InvalidRequest, nil)
	}
	u, err := c.beginChange(ctx, p, write.action(), target)
	return c.writeUse(u, input, length, write, err)
}

func (o WriteOptions) action() Action {
	if o.Replace {
		return WriteAction
	}
	return CreateAction
}
func (c *Core) writeAt(ctx context.Context, p Principal, at *location, input io.Reader, length *int64, write WriteOptions) (Change, error) {
	u, err := c.beginChangeAt(ctx, p, write.action(), at)
	return c.writeUse(u, input, length, write, err)
}
func (c *Core) writeUse(u *use, input io.Reader, length *int64, write WriteOptions, err error) (Change, error) {
	if err != nil {
		return Change{Unchanged}, err
	}
	defer u.release()
	if length != nil {
		n := *length
		length = &n
	}
	rel := u.path
	if write.Continuation != nil {
		writer, ok := u.mount.driver.(ContinuableWriter)
		if !ok {
			return Change{Unchanged}, Fail(UnsupportedOperation, nil)
		}
		source, ok := input.(Replayable)
		if !ok || length == nil || write.Continuation.Append == nil || source.SourceVersion() == "" {
			return Change{Unchanged}, Fail(InvalidRequest, nil)
		}
		continuation := *write.Continuation
		continuation.Checkpoint = append([]byte(nil), continuation.Checkpoint...)
		if err = u.ctx.Err(); err != nil {
			return Change{Unchanged}, err
		}
		result, err := writer.ContinueWrite(u.ctx, rel, source, *length, write.Replace, continuation)
		if continuation.Abort && result.Effect == Unchanged && err == nil {
			return result, u.ctx.Err()
		}
		if result.Effect == Incomplete {
			result.Effect = Unknown
			err = Fail(InvalidUpstreamResponse, errors.Join(err, errors.New("continuation reported a partial target")))
		}
		return c.finishChange(u, result, err)
	}
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
