package core

import (
	"context"
	"errors"
	"io"
)

type FileCreator interface {
	// Create borrows input synchronously; it must not close or retain it.
	// Length, when supplied, describes the entire input, including its normal EOF.
	Create(context.Context, NativeAddress, io.Reader, *int64) (Change, error)
}

// WriteOptions defaults to direct exclusive creation. Policy selects complete
// publication independently of the target's required presence.
type WriteOptions struct {
	Policy        WritePolicy        `json:"policy"`
	ObserveTarget bool               `json:"observe_target,omitempty"`
	Continuation  *WriteContinuation `json:"-"`
}

// FilePublisher is optional. Publish validates complete input before changing
// the target name and enforces options.When at publication. ObserveTarget must
// produce a known Before on success, or be refused before input is consumed.
// Input is borrowed only until return. When is enforced at the native commit;
// observation alone does not imply a version guard or durability.
type FilePublisher interface {
	Publish(context.Context, NativeAddress, io.Reader, *int64, PublicationOptions) (Change, error)
}

func writeOptions(options []WriteOptions) (WriteOptions, error) {
	if len(options) > 1 {
		return WriteOptions{}, Fail(InvalidRequest, nil)
	}
	if len(options) == 1 {
		o := options[0]
		if o.Policy.Validate() != nil || (o.ObserveTarget && !o.Policy.Complete()) {
			return WriteOptions{}, Fail(InvalidRequest, nil)
		}
		return o, nil
	}
	return WriteOptions{}, nil
}

// Create writes a new file without replacing an existing target. The caller
// owns input and keeps it valid until this synchronous call returns.
func (c *Core) Create(ctx context.Context, p Principal, target Address, input io.Reader, length *int64) (Change, error) {
	return c.Write(ctx, p, target, input, length)
}

// Write shares Create's input ownership. The default creates a new file;
// Publish() requires WriteAction; Publish(IfAbsent()) retains CreateAction.
func (c *Core) Write(ctx context.Context, p Principal, target Address, input io.Reader, length *int64, options ...WriteOptions) (result Change, err error) {
	result.Effect = Unchanged
	ctx, turn, err := enterOrder(ctx, nil)
	if err != nil {
		return result, err
	}
	defer func() { turn.finish(result.Effect == Unknown) }()
	write, err := writeOptions(options)
	if err != nil {
		return Change{Effect: Unchanged}, err
	}
	if input == nil || (length != nil && *length < 0) {
		return Change{Effect: Unchanged}, Fail(InvalidRequest, nil)
	}
	u, err := c.beginChange(ctx, p, write.action(), target)
	return c.writeUse(u, input, length, write, err)
}

func (o WriteOptions) action() Action {
	if o.Policy.Complete() && !o.Policy.When().IsAbsent() {
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
		return Change{Effect: Unchanged}, err
	}
	defer u.release()
	if length != nil {
		n := *length
		length = &n
	}
	rel := u.native()
	if write.Continuation != nil {
		writer, ok := u.mount.driver.(ContinuableWriter)
		if !ok {
			return Change{Effect: Unchanged}, Fail(UnsupportedOperation, nil)
		}
		source, ok := input.(Replayable)
		if !ok || length == nil || write.Continuation.Append == nil || source.SourceVersion() == "" {
			return Change{Effect: Unchanged}, Fail(InvalidRequest, nil)
		}
		continuation := *write.Continuation
		continuation.Checkpoint = append([]byte(nil), continuation.Checkpoint...)
		if err = u.ctx.Err(); err != nil {
			return Change{Effect: Unchanged}, err
		}
		publication := PublicationOptions{When: write.Policy.When(), ObserveTarget: write.ObserveTarget}
		if !write.Policy.Complete() {
			publication.When = IfAbsent()
		}
		result, err := writer.ContinueWrite(u.ctx, rel, source, *length, publication, continuation)
		if continuation.Abort && result.Effect == Unchanged && err == nil {
			return result, u.ctx.Err()
		}
		if result.Effect == Incomplete {
			result.Effect = Unknown
			err = Fail(InvalidUpstreamResponse, errors.Join(err, errors.New("continuation reported a partial target")))
		}
		return c.finishChange(u, result, err)
	}
	if write.Policy.Complete() {
		publisher, ok := u.mount.driver.(FilePublisher)
		if !ok {
			return Change{Effect: Unchanged}, Fail(UnsupportedOperation, nil)
		}
		if err = u.ctx.Err(); err != nil {
			return Change{Effect: Unchanged}, err
		}
		result, err := publisher.Publish(u.ctx, rel, input, length, PublicationOptions{When: write.Policy.When(), ObserveTarget: write.ObserveTarget})
		if result.Effect == Unchanged && write.Policy.When().IsAbsent() && CodeOf(err) == AlreadyExists {
			err = Fail(PreconditionFailed, errors.Join(&ConditionFailure{Negative: true, Evidence: result.Evidence}, err))
		}
		if result.Effect == Applied && write.Policy.When().IsAbsent() {
			result.Before = TargetMissing
		}
		if result.Effect == Applied && write.ObserveTarget && result.Before == TargetUnknown && err == nil {
			err = Fail(InvalidUpstreamResponse, errors.New("publisher omitted requested target observation"))
		}
		if result.Effect == Incomplete {
			result.Effect = Unknown
			err = Fail(InvalidUpstreamResponse, errors.Join(errors.New("publisher reported a partial target"), err))
		}
		return c.finishChange(u, result, err)
	}
	creator, ok := u.mount.driver.(FileCreator)
	if !ok {
		return Change{Effect: Unchanged}, Fail(UnsupportedOperation, nil)
	}
	if err = u.ctx.Err(); err != nil {
		return Change{Effect: Unchanged}, err
	}
	result, err := creator.Create(u.ctx, rel, input, length)
	return c.finishChange(u, result, err)
}
