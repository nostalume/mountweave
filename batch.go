package core

import (
	"context"
	"errors"
)

// ItemResult records one attempted input occurrence, including repeated inputs.
// An error does not erase a confirmed or uncertain Change.
type ItemResult struct {
	Change Change
	Err    error
}

type CopyRequest struct {
	Source string
	Target string
	WriteOptions
}

// RemoveBatch retains input order and duplicates. Execution is serial unless an
// explicit concurrency option is supplied. On failure no more items start;
// already started items finish and retain their receipts in the returned prefix.
func (c *Core) RemoveBatch(ctx context.Context, p Principal, targets []string, options ...ExecutionOptions) ([]ItemResult, error) {
	if c == nil {
		return nil, Fail(InvalidRequest, errors.New("core is required"))
	}
	limit, err := concurrency(options)
	if err != nil {
		return nil, err
	}
	return execute(ctx, len(targets), limit, nil, func(i int) work[ItemResult] {
		target := targets[i]
		at := c.capture(ctx, target)
		return work[ItemResult]{accesses: []access{{at: at, write: true}}, run: func() (ItemResult, error) {
			var change Change
			var err error
			if at == nil {
				change, err = c.Remove(ctx, p, target)
			} else {
				change, err = c.removeAt(ctx, p, at)
			}
			return ItemResult{Change: change, Err: err}, err
		}}
	})
}

// CopyBatch follows RemoveBatch's prefix and stop contract. Its conflict ranges
// include both source reads and target writes after explicit alias resolution.
// Unknown physical aliases and external mutations are not transactionally locked.
func (c *Core) CopyBatch(ctx context.Context, p Principal, requests []CopyRequest, options ...ExecutionOptions) ([]ItemResult, error) {
	if c == nil {
		return nil, Fail(InvalidRequest, errors.New("core is required"))
	}
	limit, err := concurrency(options)
	if err != nil {
		return nil, err
	}
	return execute(ctx, len(requests), limit, nil, func(i int) work[ItemResult] {
		request := requests[i]
		from, to := c.capture(ctx, request.Source), c.capture(ctx, request.Target)
		return work[ItemResult]{accesses: []access{{at: from}, {at: to, write: true}}, run: func() (ItemResult, error) {
			var change Change
			var err error
			if from == nil || to == nil {
				change, err = c.Copy(ctx, p, request.Source, request.Target, request.WriteOptions)
			} else {
				change, err = c.copyAt(ctx, p, from, to, request.WriteOptions)
			}
			return ItemResult{Change: change, Err: err}, err
		}}
	})
}

type MoveRequest struct {
	Source string
	Target string
}

// MoveItemResult retains both endpoint receipts for one attempted occurrence.
type MoveItemResult struct {
	MoveResult
	Err error
}

// MoveBatch follows the same prefix and stop contract as CopyBatch. Both
// endpoints are writes, including their descendants, within this finite call.
func (c *Core) MoveBatch(ctx context.Context, p Principal, requests []MoveRequest, options ...ExecutionOptions) ([]MoveItemResult, error) {
	if c == nil {
		return nil, Fail(InvalidRequest, errors.New("core is required"))
	}
	limit, err := concurrency(options)
	if err != nil {
		return nil, err
	}
	return execute(ctx, len(requests), limit, nil, func(i int) work[MoveItemResult] {
		request := requests[i]
		from, to := c.capture(ctx, request.Source), c.capture(ctx, request.Target)
		return work[MoveItemResult]{accesses: []access{{at: from, write: true}, {at: to, write: true}}, run: func() (MoveItemResult, error) {
			var result MoveResult
			var err error
			if from == nil || to == nil {
				result, err = c.Move(ctx, p, request.Source, request.Target)
			} else {
				result, err = c.moveAt(ctx, p, from, to)
			}
			return MoveItemResult{MoveResult: result, Err: err}, err
		}}
	})
}
