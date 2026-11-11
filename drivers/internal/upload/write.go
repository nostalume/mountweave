package upload

import (
	"context"
	"errors"
	"time"

	core "github.com/nostalume/mountweave"
)

type DeliveryKind uint8

const (
	NoReceipt DeliveryKind = iota
	FileResult
	ContentReady
)

// Delivery carries native facts, never a response Body. NoReceipt
// preserves the preparation effect on failure. A FileResult replaces it even
// with an error; ContentReady still needs a separate file confirmation.
type Delivery struct {
	Kind   DeliveryKind
	Change core.Change
}

// Attempt owns the native facts of one synchronous write. Retire confirms
// private resource cleanup independently of any explicitly updated file effect.
// Retain must keep only owned native identity and causes, never borrowed input.
type Attempt interface {
	Prepare(context.Context) (core.Change, error)
	Deliver(context.Context) (Delivery, error)
	Retire(context.Context, core.Change) (bool, core.Change, error)
	Retain(error)
}

// Write shares the complete prepare/deliver/confirm/retire lifetime. A nil
// confirm is valid for a native file result; it never invents another commit.
func Write(ctx context.Context, attempt Attempt, confirm func(context.Context) (core.Change, error), cleanupTimeout time.Duration) (change core.Change, err error) {
	if err = ctx.Err(); err != nil {
		return core.Change{Effect: core.Unchanged}, err
	}
	change, err = attempt.Prepare(ctx)
	if err == nil && change.Effect != core.Applied {
		if err = ctx.Err(); err == nil {
			var delivery Delivery
			delivery, err = attempt.Deliver(ctx)
			switch delivery.Kind {
			case FileResult:
				change = delivery.Change
			case ContentReady:
				if err == nil && ctx.Err() == nil {
					if confirm == nil {
						err = core.Fail(core.InvalidUpstreamResponse, errors.New("content has no file confirmation"))
					} else {
						change, err = confirm(ctx)
					}
				}
			default:
				if err == nil {
					err = core.Fail(core.InvalidUpstreamResponse, errors.New("upload omitted its receipt"))
				}
			}
		}
	}
	err = errors.Join(err, ctx.Err())
	if change.Effect == core.Applied {
		return
	}
	cleanup := context.WithoutCancel(ctx)
	if cleanupTimeout > 0 {
		var cancel context.CancelFunc
		cleanup, cancel = context.WithTimeout(cleanup, cleanupTimeout)
		defer cancel()
	}
	retired, current, failure := attempt.Retire(cleanup, change)
	change, err = current, errors.Join(err, failure)
	if !retired {
		attempt.Retain(err)
	}
	return
}
