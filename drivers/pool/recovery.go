package pool

import (
	"context"
	"errors"

	core "github.com/nostalume/mountweave"
)

// PrivateUpload names a never-reused immutable attempt. It contains no secrets.
type PrivateUpload struct {
	Path   string
	Size   int64
	SHA256 [32]byte
}

// UploadResolver is an optional member capability for native reconciliation.
// Applied proves exact bytes/hash and terminal creation; Unchanged proves final
// absence. Both MUST exclude any delayed writer creating/changing this key later.
// A list, Stat or hash without that native termination proof is insufficient.
// Unknown/Incomplete leaves the original receipt and charge intact. A confirmed
// terminal receipt survives an accompanying error, including cancellation.
// The member owns native operation IDs/checkpoints needed to establish this fact.
type UploadResolver interface {
	ResolveUpload(context.Context, PrivateUpload) (core.Change, error)
}

func (d *Driver) resolveUpload(ctx context.Context, r shardRecord) (core.Effect, error) {
	u, err := d.borrowMember(ctx, r.Member)
	if err != nil {
		return core.Unknown, errors.Join(ErrUncertain, err)
	}
	defer u.finish()
	resolver, ok := u.native.(UploadResolver)
	if !ok {
		return core.Unknown, ErrUncertain
	}
	change, err := resolver.ResolveUpload(u.ctx, PrivateUpload{Path: r.Path, Size: int64(d.layout.ShardBytes), SHA256: r.Hash})
	if change.Effect != core.Applied && change.Effect != core.Unchanged {
		return core.Unknown, errors.Join(ErrUncertain, err, u.ctx.Err())
	}
	return change.Effect, errors.Join(err, u.ctx.Err())
}

func (d *Driver) removeShard(ctx context.Context, r shardRecord) (core.Change, error) {
	u, err := d.borrowMember(ctx, r.Member)
	if err != nil {
		return core.Change{Effect: core.Unchanged}, err
	}
	defer u.finish()
	remover, ok := u.native.(core.Remover)
	if !ok {
		return core.Change{Effect: core.Unchanged}, core.Fail(core.UnsupportedOperation, nil)
	}
	change, err := remover.Remove(u.ctx, r.Path)
	return change, errors.Join(err, u.ctx.Err())
}
