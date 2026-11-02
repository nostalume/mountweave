package pool

import (
	"context"
	"errors"

	core "github.com/nostalume/mountweave"
	bolt "go.etcd.io/bbolt"
)

// MemberInfo describes attached resources and private-byte accounting. Attached
// is not a claim that the provider is healthy. MaxBytes zero adds no pool cap.
type MemberInfo struct {
	ID, Identity, FailureDomain string
	MaxBytes, UsedBytes         int64
	Draining, Removed, Attached bool
	Retiring                    bool
}
type MemberPolicy struct {
	MaxBytes int64
	Draining bool
}

func (d *Driver) Members(ctx context.Context) (items []MemberInfo, err error) {
	o, err := d.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer o.finish()
	d.state.Lock()
	defer d.state.Unlock()
	err = d.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(membersKey).ForEach(func(_, raw []byte) error {
			if err := o.ctx.Err(); err != nil {
				return err
			}
			var m memberRecord
			if err := decode(raw, &m); err != nil {
				return err
			}
			runtime := d.members[m.ID]
			items = append(items, MemberInfo{ID: m.ID, Identity: m.Identity, FailureDomain: m.FailureDomain, MaxBytes: m.MaxBytes, UsedBytes: m.UsedBytes, Draining: m.Draining, Removed: m.Removed, Attached: d.availableMember(m.ID) != nil, Retiring: runtime != nil && runtime.retiring})
			return nil
		})
	})
	return
}

// AddMember transfers Driver ownership only when Effect is Applied, including
// Applied with a cancellation error. Any other result leaves it with the caller;
// a metadata confirmation failure freezes writes without adopting the instance.
func (d *Driver) AddMember(ctx context.Context, m Member) (core.Change, error) {
	if m.MaxBytes < 0 || !core.ValidName(m.ID) || len(m.ID) > 128 || m.Identity == "" || len(m.Identity) > 1024 || m.FailureDomain == "" || len(m.FailureDomain) > 256 {
		return core.Change{Effect: core.Unchanged}, core.Fail(core.InvalidRequest, nil)
	}
	o, err := d.begin(ctx)
	if err != nil {
		return core.Change{Effect: core.Unchanged}, err
	}
	defer o.finish()
	d.state.Lock()
	defer d.state.Unlock()
	change, err := d.commitLocked(o.ctx, func(tx *bolt.Tx) error {
		b := tx.Bucket(membersKey)
		if b.Get([]byte(m.ID)) != nil {
			return core.Fail(core.AlreadyExists, nil)
		}
		if err := b.ForEach(func(_, raw []byte) error {
			var old memberRecord
			if err := decode(raw, &old); err != nil {
				return err
			}
			if !old.Removed && old.Identity == m.Identity {
				return core.Fail(core.AlreadyExists, nil)
			}
			return nil
		}); err != nil {
			return err
		}
		if err := b.Put([]byte(m.ID), encode(memberRecord{ID: m.ID, Identity: m.Identity, FailureDomain: m.FailureDomain, MaxBytes: m.MaxBytes})); err != nil {
			return err
		}
		return bumpMembership(tx)
	})
	if change.Effect == core.Applied {
		d.members[m.ID] = newMemberRuntime(m.Driver)
	}
	return change, err
}
func bumpMembership(tx *bolt.Tx) error {
	var cfg layout
	if err := decode(tx.Bucket(configKey).Get(headerKey), &cfg); err != nil {
		return err
	}
	cfg.Membership++
	return tx.Bucket(configKey).Put(headerKey, encode(cfg))
}

// ConfigureMember persists quota and placement policy. Draining disables new
// reservations but preserves existing reservations and all historical reads.
func (d *Driver) ConfigureMember(ctx context.Context, id string, policy MemberPolicy) (core.Change, error) {
	if policy.MaxBytes < 0 {
		return core.Change{Effect: core.Unchanged}, core.Fail(core.InvalidRequest, nil)
	}
	o, err := d.begin(ctx)
	if err != nil {
		return core.Change{Effect: core.Unchanged}, err
	}
	defer o.finish()
	return d.commit(o.ctx, func(tx *bolt.Tx) error {
		b := tx.Bucket(membersKey)
		if b.Get([]byte(id)) == nil {
			return core.Fail(core.NotFound, nil)
		}
		var m memberRecord
		if err := decode(b.Get([]byte(id)), &m); err != nil {
			return err
		}
		if m.Removed {
			return core.Fail(core.MountUnavailable, nil)
		}
		if m.Draining != policy.Draining {
			m.Policy++
		}
		m.MaxBytes, m.Draining = policy.MaxBytes, policy.Draining
		if err := b.Put([]byte(id), encode(m)); err != nil {
			return err
		}
		return bumpMembership(tx)
	})
}

// RemoveMember requires draining and zero private reservations. Tombstones are
// retained so another account cannot reuse the old identity. Native Close runs
// once after removal and its failure remains owned, independently of the receipt.
func (d *Driver) RemoveMember(ctx context.Context, id string) (core.Change, error) {
	o, err := d.begin(ctx)
	if err != nil {
		return core.Change{Effect: core.Unchanged}, err
	}
	defer o.finish()
	d.state.Lock()
	if m := d.members[id]; m != nil && m.changing {
		d.state.Unlock()
		return core.Change{Effect: core.Unchanged}, core.Fail(core.UpstreamThrottled, nil)
	}
	change, err := d.commitLocked(o.ctx, func(tx *bolt.Tx) error {
		b := tx.Bucket(membersKey)
		if b.Get([]byte(id)) == nil {
			return core.Fail(core.NotFound, nil)
		}
		var m memberRecord
		if err := decode(b.Get([]byte(id)), &m); err != nil {
			return err
		}
		if m.Removed {
			return core.Fail(core.AlreadyExists, nil)
		}
		if !m.Draining || m.UsedBytes != 0 {
			return core.Fail(core.CapacityExceeded, errors.New("member still owns private objects or reservations"))
		}
		m.Removed = true
		if err := b.Put([]byte(id), encode(m)); err != nil {
			return err
		}
		return bumpMembership(tx)
	})
	var native *memberRuntime
	if change.Effect == core.Applied {
		native = d.members[id]
		if native != nil {
			native.retiring, native.changing = true, true
			native.cancel()
		}
	}
	d.state.Unlock()
	if native != nil {
		// Zero reservations excludes shard users. Reconnect is excluded above.
		closeErr := native.close()
		d.state.Lock()
		native.changing = false
		d.state.Unlock()
		err = errors.Join(err, closeErr)
	}
	return change, err
}
