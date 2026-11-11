package core

import (
	"context"
	"errors"
	"strings"
)

// MoveSource supplies a strong deletion validator for the exact file returned
// by Open. Remove must enforce it and RemoveNonDirectory against the current
// source name at the native commit.
type MoveSource interface {
	Remover
	MoveFileValidator(NativeAddress, Evidence) (Validator, error)
}

func (c *Core) moveAcross(p Principal, from, to *use, options MoveOptions) (MoveResult, error) {
	defer from.release()
	defer to.release()
	result := MoveResult{Source: Change{Effect: Unchanged}, Target: Change{Effect: Unchanged}}
	source, ok := from.mount.driver.(MoveSource)
	if options.NoCopy || !ok || options.Existing == Replace || !options.Target.Empty() && !options.Target.IsAbsent() {
		return result, Fail(UnsupportedOperation, nil)
	}
	ctx, cancel := context.WithCancel(from.ctx)
	defer cancel()
	stop := context.AfterFunc(to.ctx, cancel)
	defer stop()
	if err := to.ctx.Err(); err != nil {
		return result, err
	}
	if options.Tree.MaxEntries > 0 || options.Tree.MaxPlanBytes > 0 {
		entry, err := c.observe(from, "")
		if err != nil {
			return result, err
		}
		if entry.Kind == Directory {
			return c.moveTree(ctx, p, from.location, to.location, source, options)
		}
	}
	if _, ok := to.mount.driver.(FilePublisher); !ok {
		return result, Fail(UnsupportedOperation, nil)
	}
	opened, err := c.openAt(ctx, p, from.location, OpenOptions{NoFollow: true, Selection: ReadSelection{When: options.Source}})
	if err != nil {
		if CodeOf(err) == NotFile && options.Tree.MaxEntries == 0 && options.Tree.MaxPlanBytes == 0 {
			if entry, observeErr := c.observe(from, ""); observeErr == nil && entry.Kind == Directory {
				err = Fail(InvalidRequest, errors.New("cross-mount directory moves require tree budgets"))
			}
		}
		return result, &OperationError{Side: SourceSide, Phase: ReadPhase, Cause: err}
	}
	guard, err := moveFileGuard(source, from.location, opened)
	if err != nil {
		return result, err
	}
	result.Target, err = c.copyContent(ctx, p, to.location, opened, WriteOptions{Policy: Publish(IfAbsent())})
	if err != nil {
		return result, err
	}
	if err := to.ctx.Err(); err != nil {
		return result, err
	}
	result.Source, err = c.removeMoved(ctx, p, from.location, guard, RemoveNonDirectory)
	if err != nil {
		err = &OperationError{Side: SourceSide, Phase: RemovePhase, Cause: err}
	}
	return result, err
}

func moveValidator(v Validator) bool {
	return v.Domain != "" && v.Valid() && !strings.HasPrefix(v.Tag, "W/")
}

func moveFileGuard(source MoveSource, at *location, opened Content) (Validator, error) {
	guard, err := source.MoveFileValidator(at.native(), opened.Entry.Evidence)
	if err == nil && !moveValidator(guard) {
		err = Fail(InvalidUpstreamResponse, errors.New("missing strong move validator"))
	}
	if err != nil {
		err = &OperationError{Side: SourceSide, Phase: ReadPhase, Cause: errors.Join(err, opened.Body.Close())}
	}
	return guard, err
}

func (c *Core) removeMoved(ctx context.Context, p Principal, at *location, guard Validator, kind RemoveTarget) (Change, error) {
	u, err := c.beginChangeAt(ctx, p, RemoveAction, at)
	if err != nil {
		return Change{Effect: Unchanged}, err
	}
	// Resolve the source name in the native deletion, so a moved parent
	// cannot redirect the effect through a previously opened scope.
	return c.removeUse(u, RemoveOptions{When: IfMatch(guard), Target: kind}, nil)
}
