package pool

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"

	core "github.com/nostalume/mountweave"
	bolt "go.etcd.io/bbolt"
)

// ErrUncertain requires provider-specific reconciliation of an upload, not a
// delete based on an absent Stat. Its reservation remains charged.
var ErrUncertain = errors.New("unconfirmed private upload requires reconciliation")

// Maintenance reports bounded, partial work. Visited counts stripe/boundary records;
// Changed counts reclaimed objects or published placements. Bytes counts their
// private bytes. More can remain true with no progress (pins or uncertain work).
type Maintenance struct {
	Visited, Changed int
	Bytes            int64
	More             bool
}
type scanCursor struct {
	ID    string
	Index int64
}
type drainState struct {
	Cursor scanCursor
	Policy uint64
}
type stripeWork struct {
	ID     string
	Index  int64
	Stripe stripeRecord
}

// nextStripe uses a short transaction and a runtime cursor, not a whole-pool plan.
// A generation boundary is returned with Index=-1 for intent cleanup.
func (d *Driver) nextStripe(cursor *scanCursor, garbage bool) (work stripeWork, found bool, err error) {
	err = d.db.View(func(tx *bolt.Tx) error {
		root := tx.Bucket(uploadsKey)
		if garbage {
			root = tx.Bucket(garbageKey)
		}
		c := root.Cursor()
		id, _ := c.Seek([]byte(cursor.ID))
		if id == nil {
			*cursor = scanCursor{}
			return nil
		}
		work.ID = string(id)
		b := tx.Bucket(uploadsKey).Bucket(id)
		if b == nil {
			return core.Fail(core.InvalidUpstreamResponse, nil)
		}
		index := int64(0)
		if work.ID == cursor.ID {
			index = cursor.Index
		}
		k, v := b.Cursor().Seek(stripeKey(index))
		if len(k) == 8 {
			work.Index = int64(binary.BigEndian.Uint64(k))
			if work.Index < 0 {
				return core.Fail(core.InvalidUpstreamResponse, nil)
			}
			if err := decode(v, &work.Stripe); err != nil {
				return err
			}
			*cursor = scanCursor{ID: work.ID, Index: work.Index + 1}
		} else {
			work.Index = -1
			*cursor = scanCursor{ID: work.ID + "\x00"}
		}
		found = true
		return nil
	})
	return
}
func (d *Driver) prune(tx *bolt.Tx, id string) error {
	b := tx.Bucket(uploadsKey).Bucket([]byte(id))
	if b == nil {
		return nil
	}
	var u uploadRecord
	if err := decode(b.Get(headerKey), &u); err != nil {
		return err
	}
	if d.active[id] != nil || d.pins[id] != 0 {
		return nil
	}
	if tx.Bucket(refsKey).Get([]byte(id)) == nil && u.Objects == 0 {
		if err := tx.Bucket(uploadsKey).DeleteBucket([]byte(id)); err != nil {
			return err
		}
		if err := tx.Bucket(filesKey).Delete([]byte(id)); err != nil {
			return err
		}
		return tx.Bucket(garbageKey).Delete([]byte(id))
	}
	return queueGarbage(tx, id, u)
}
func (d *Driver) maintenanceStart(ctx context.Context, limit int) (*operation, error) {
	if limit < 1 {
		return nil, core.Fail(core.InvalidRequest, nil)
	}
	o, err := d.begin(ctx)
	if err != nil {
		return nil, err
	}
	if !d.maintenance.TryLock() {
		o.finish()
		return nil, core.Fail(core.UpstreamThrottled, nil)
	}
	return o, nil
}

// Collect reclaims only inactive, unreferenced generations or abandoned placement
// attempts with no old readers. Unconfirmed uploads require terminal native
// reconciliation or remain charged. The roots are exclusive with never-reused keys.
func (d *Driver) Collect(ctx context.Context, limit int) (result Maintenance, err error) {
	o, err := d.maintenanceStart(ctx, limit)
	if err != nil {
		return result, err
	}
	defer o.finish()
	defer d.maintenance.Unlock()
	return d.collect(o, limit, &d.gcCursor)
}

func (d *Driver) collect(o *operation, limit int, cursor *scanCursor) (result Maintenance, err error) {
	result.More = true
	for result.Visited < limit {
		if cause := o.ctx.Err(); cause != nil {
			err = errors.Join(err, cause)
			break
		}
		d.state.Lock()
		work, found, scanErr := d.nextStripe(cursor, true)
		if scanErr != nil || !found {
			d.state.Unlock()
			err = errors.Join(err, scanErr)
			break
		}
		result.Visited++
		if d.active[work.ID] != nil || d.pins[work.ID] != 0 {
			*cursor = scanCursor{ID: work.ID + "\x00"}
			d.state.Unlock()
			continue
		}
		if work.Index < 0 {
			scanErr = d.update(func(tx *bolt.Tx) error { return d.prune(tx, work.ID) })
			d.state.Unlock()
			if scanErr != nil {
				err = errors.Join(err, scanErr)
				break
			}
			continue
		}
		live := false
		scanErr = d.db.View(func(tx *bolt.Tx) error { live = tx.Bucket(refsKey).Get([]byte(work.ID)) != nil; return nil })
		d.state.Unlock()
		if scanErr != nil {
			err = errors.Join(err, scanErr)
			break
		}
		s := work.Stripe
		lists := [][]shardRecord{s.Abandoned}
		if !live {
			lists = append(lists, s.Shards)
		}
		for _, list := range lists {
			for j := range list {
				r := &list[j]
				if r.Reclaimed || r.Effect == core.Unchanged {
					continue
				}
				if cause := o.ctx.Err(); cause != nil {
					err = errors.Join(err, cause)
					break
				}
				if r.Effect != core.Applied {
					resolved, resolveErr := d.resolveUpload(o.ctx, *r)
					if resolved == core.Applied || resolved == core.Unchanged {
						r.Effect = resolved
						if saveErr := d.saveStripe(work.ID, work.Index, s); saveErr != nil {
							return result, errors.Join(err, resolveErr, saveErr)
						}
					}
					err = errors.Join(err, resolveErr)
					if r.Effect == core.Unchanged {
						result.Changed++
						result.Bytes += int64(d.layout.ShardBytes)
						continue
					}
					if r.Effect != core.Applied {
						continue
					}
				}
				change, removeErr := d.removeShard(o.ctx, *r)
				if removeErr == nil && change.Effect != core.Applied {
					removeErr = core.Fail(core.InvalidUpstreamResponse, nil)
				}
				if change.Effect == core.Applied || (change.Effect == core.Unchanged && core.CodeOf(removeErr) == core.NotFound) {
					r.Reclaimed = true
					result.Changed++
					result.Bytes += int64(d.layout.ShardBytes)
					if core.CodeOf(removeErr) == core.NotFound {
						removeErr = nil
					}
				}
				err = errors.Join(err, removeErr)
				// Persist every acknowledged release; a lost delete confirmation is
				// safe to repeat because confirmed uploads cannot recreate this key.
				if saveErr := d.saveStripe(work.ID, work.Index, s); saveErr != nil {
					return result, errors.Join(err, saveErr)
				}
			}
		}
		kept := s.Abandoned[:0]
		for _, r := range s.Abandoned {
			if !r.Reclaimed && r.Effect != core.Unchanged {
				kept = append(kept, r)
			}
		}
		s.Abandoned = kept
		d.state.Lock()
		saveErr := d.update(func(tx *bolt.Tx) error {
			if err := d.putStripe(tx, work.ID, work.Index, s); err != nil {
				return err
			}
			return d.prune(tx, work.ID)
		})
		d.state.Unlock()
		if saveErr != nil {
			err = errors.Join(err, saveErr)
			break
		}
	}
	stateErr := d.db.View(func(tx *bolt.Tx) error {
		k, _ := tx.Bucket(garbageKey).Cursor().First()
		result.More = k != nil
		return nil
	})
	return result, errors.Join(err, stateErr, o.ctx.Err())
}

// Drain stops new placement, then moves at most limit stripe records. It copies
// immutable shards, verifies the destination, and atomically switches locations.
// Old readers keep their source objects until Collect can prove them unused.
func (d *Driver) Drain(ctx context.Context, id string, limit int) (result Maintenance, err error) {
	o, err := d.maintenanceStart(ctx, limit)
	if err != nil {
		return result, err
	}
	defer o.finish()
	defer d.maintenance.Unlock()
	saved := d.drainCursors[id]
	result, err = d.drain(o, id, limit, &saved)
	d.drainCursors[id] = saved
	return
}

func (d *Driver) drain(o *operation, id string, limit int, saved *drainState) (result Maintenance, err error) {
	var policy uint64
	d.state.Lock()
	_, err = d.commitLocked(o.ctx, func(tx *bolt.Tx) error {
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
		if !m.Draining {
			m.Draining = true
			m.Policy++
			if err := b.Put([]byte(id), encode(m)); err != nil {
				return err
			}
			if err := bumpMembership(tx); err != nil {
				return err
			}
		}
		policy = m.Policy
		return nil
	})
	cursor := saved.Cursor
	if saved.Policy != policy {
		cursor = scanCursor{}
	}
	d.state.Unlock()
	if err != nil {
		return result, err
	}
	var buffers [][]byte
	defer func() {
		if buffers != nil {
			d.release()
		}
	}()
	for result.Visited < limit {
		if err = o.ctx.Err(); err != nil {
			break
		}
		d.state.Lock()
		work, found, scanErr := d.nextStripe(&cursor, false)
		if scanErr != nil || !found {
			d.state.Unlock()
			err = scanErr
			break
		}
		result.Visited++
		active := d.active[work.ID] != nil
		live := false
		scanErr = d.db.View(func(tx *bolt.Tx) error { live = tx.Bucket(refsKey).Get([]byte(work.ID)) != nil; return nil })
		d.state.Unlock()
		if scanErr != nil {
			err = scanErr
			break
		}
		if active || !live || work.Index < 0 {
			cursor = scanCursor{ID: work.ID + "\x00"}
			continue
		}
		slot := -1
		for i, r := range work.Stripe.Shards {
			if r.Member == id {
				slot = i
				break
			}
		}
		if slot < 0 {
			continue
		}
		if buffers == nil {
			if err = d.reserve(o.ctx); err != nil {
				break
			}
			buffers = d.buffers()
		}
		reader := body{op: o}
		moveErr := reader.fetch(work.Stripe.Shards[slot], buffers[slot])
		if moveErr != nil && o.ctx.Err() == nil {
			var cleanup *cleanupFailure
			if !errors.As(moveErr, &cleanup) {
				_, recoverErr := d.loadVerified(o, work.Stripe, buffers, slot)
				if recoverErr == nil {
					moveErr = nil
				} else {
					moveErr = errors.Join(moveErr, recoverErr)
				}
			}
		}
		changed := false
		if moveErr == nil {
			changed, moveErr = d.placeShard(o, &work, slot, buffers[slot], nil, true)
		}
		if changed {
			result.Changed++
			result.Bytes += int64(d.layout.ShardBytes)
		}
		if moveErr != nil {
			err = moveErr
			break
		}
	}
	if err != nil {
		cursor = scanCursor{}
	}
	d.state.Lock()
	var current memberRecord
	stateErr := d.db.View(func(tx *bolt.Tx) error { return decode(tx.Bucket(membersKey).Get([]byte(id)), &current) })
	if stateErr == nil && current.Policy != policy {
		cursor = scanCursor{}
		err = errors.Join(err, core.Fail(core.InvalidRequest, errors.New("drain policy changed")))
	}
	err = errors.Join(err, stateErr)
	*saved = drainState{Cursor: cursor, Policy: current.Policy}
	result.More = cursor.ID != "" || err != nil
	for _, members := range d.active {
		result.More = result.More || members[id]
	}
	d.state.Unlock()
	return result, errors.Join(err, o.ctx.Err())
}

func (d *Driver) placeShard(o *operation, work *stripeWork, slot int, payload []byte, exclude map[string]bool, draining bool) (bool, error) {
	s := work.Stripe
	old := s.Shards[slot]
	used := make(map[string]bool)
	d.state.Lock()
	err := d.update(func(tx *bolt.Tx) error {
		var source memberRecord
		if err := decode(tx.Bucket(membersKey).Get([]byte(old.Member)), &source); err != nil {
			return err
		}
		if draining && !source.Draining {
			return core.Fail(core.InvalidRequest, errors.New("drain policy changed"))
		}
		var cfg layout
		if err := decode(tx.Bucket(configKey).Get(headerKey), &cfg); err != nil {
			return err
		}
		s.Membership = cfg.Membership
		for i, r := range s.Shards {
			if i == slot {
				continue
			}
			var m memberRecord
			if err := decode(tx.Bucket(membersKey).Get([]byte(r.Member)), &m); err != nil {
				return err
			}
			used[m.FailureDomain] = true
		}
		eligible, err := d.candidates(tx)
		if err != nil {
			return err
		}
		kept := eligible[:0]
		for _, m := range eligible {
			if !exclude[m.ID] {
				kept = append(kept, m)
			}
		}
		m, err := chooseMember(kept, used, slot)
		if err != nil {
			return err
		}
		next := shardRecord{Member: m.ID, Path: shardPath(work.ID, work.Index, slot, s.NextAttempt), Attempt: s.NextAttempt, Hash: old.Hash}
		s.NextAttempt++
		s.Abandoned = append(s.Abandoned, next)
		return d.putStripe(tx, work.ID, work.Index, s)
	})
	d.state.Unlock()
	if err != nil {
		return false, err
	}
	next := &s.Abandoned[len(s.Abandoned)-1]
	reader := body{op: o}
	change, createErr := d.createShard(o.ctx, *next, bytes.NewReader(payload), int64(len(payload)))
	next.Effect, err = change.Effect, createErr
	if err == nil && change.Effect != core.Applied {
		err = core.Fail(core.InvalidUpstreamResponse, nil)
	}
	if err == nil {
		err = reader.fetch(*next, payload)
	}
	var changed bool
	d.state.Lock()
	saveErr := d.update(func(tx *bolt.Tx) error {
		if err == nil && o.ctx.Err() == nil && tx.Bucket(refsKey).Get([]byte(work.ID)) != nil {
			s.Shards[slot] = *next
			s.Abandoned[len(s.Abandoned)-1] = old
			changed = true
		}
		return d.putStripe(tx, work.ID, work.Index, s)
	})
	d.state.Unlock()
	if saveErr == nil {
		work.Stripe = s
	}
	return changed && saveErr == nil, errors.Join(err, saveErr, o.ctx.Err())
}
