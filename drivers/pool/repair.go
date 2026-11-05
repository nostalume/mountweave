package pool

import (
	"context"
	"crypto/sha256"
	"errors"

	core "github.com/nostalume/mountweave"
	bolt "go.etcd.io/bbolt"
)

// Repair verifies committed stripes and restores missing/corrupt shards onto
// eligible private members. It never publishes an interrupted user upload.
// Every switched location has been read back and verified; old objects remain
// owned until Collect, including unreachable or uncertain locations.
func (d *Driver) Repair(ctx context.Context, limit int) (Maintenance, error) {
	o, err := d.maintenanceStart(ctx, limit)
	if err != nil {
		return Maintenance{}, err
	}
	defer o.finish()
	defer d.maintenance.Unlock()
	return d.repair(o, limit, &d.repairCursor)
}

func (d *Driver) repair(o *operation, limit int, cursor *scanCursor) (result Maintenance, err error) {
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
		work, found, scanErr := d.nextStripe(cursor, false)
		live, active := false, d.active[work.ID] != nil
		if scanErr == nil && found {
			scanErr = d.db.View(func(tx *bolt.Tx) error { live = tx.Bucket(refsKey).Get([]byte(work.ID)) != nil; return nil })
		}
		d.state.Unlock()
		if scanErr != nil || !found {
			err = scanErr
			break
		}
		result.Visited++
		if !live || active || work.Index < 0 {
			continue
		}
		// Validate recorded placement before making native requests.
		var f fileRecord
		err = d.db.View(func(tx *bolt.Tx) error { return decode(tx.Bucket(filesKey).Get([]byte(work.ID)), &f) })
		if err != nil {
			break
		}
		work.Stripe, err = d.stripe(f, work.Index)
		if err != nil {
			break
		}
		if buffers == nil {
			if err = d.reserve(o.ctx); err != nil {
				break
			}
			buffers = d.buffers()
		}
		failures, loadErr := d.loadVerified(o, work.Stripe, buffers, -1)
		if loadErr != nil {
			err = loadErr
			break
		}
		exclude := make(map[string]bool)
		for slot, failure := range failures {
			// Missing/corrupt objects do not alone make an attached root unwritable.
			if failure != nil && core.CodeOf(failure) != core.NotFound && core.CodeOf(failure) != core.InvalidUpstreamResponse {
				exclude[work.Stripe.Shards[slot].Member] = true
			}
		}
		for slot, failure := range failures {
			if failure == nil {
				continue
			}
			changed, placeErr := d.placeShard(o, &work, slot, buffers[slot], exclude, false)
			if changed {
				result.Changed++
				result.Bytes += int64(d.layout.ShardBytes)
			}
			if placeErr != nil {
				err = placeErr
				break
			}
		}
		if err != nil {
			break
		}
	}
	if err != nil {
		*cursor = scanCursor{}
	}
	result.More = cursor.ID != "" || err != nil
	return result, errors.Join(err, o.ctx.Err())
}

// loadVerified reuses the caller's stripe budget. Missing slots have zero length
// but retain capacity, so the codec reconstructs into the same buffers.
// skip is an already-failed source, avoiding its repeated native request in Drain.
func (d *Driver) loadVerified(o *operation, s stripeRecord, buffers [][]byte, skip int) ([]error, error) {
	shards, failures := make([][]byte, len(s.Shards)), make([]error, len(s.Shards))
	reader := body{op: o}
	fetch := func(slot int) {
		shards[slot] = buffers[slot][:0]
		if slot == skip {
			failures[slot] = core.Fail(core.MountUnavailable, nil)
			return
		}
		failures[slot] = reader.fetch(reader.op.ctx, s.Shards[slot], buffers[slot])
		if failures[slot] == nil {
			shards[slot] = buffers[slot]
		}
	}
	if skip < 0 {
		parallel(len(s.Shards), d.opts.Concurrency, fetch)
	} else {
		for slot := range shards {
			shards[slot] = buffers[slot][:0]
		}
		failures[skip] = core.Fail(core.MountUnavailable, nil)
		// A known lost source needs k verified inputs, not a whole scrub. Batches
		// preserve the configured native concurrency without over-fetching parity.
		for next, valid := 0, 0; next < len(shards) && valid < d.layout.Data; {
			var slots []int
			for next < len(shards) && len(slots) < min(d.opts.Concurrency, d.layout.Data-valid) {
				slot := next
				next++
				if slot != skip {
					slots = append(slots, slot)
				}
			}
			parallel(len(slots), d.opts.Concurrency, func(j int) { fetch(slots[j]) })
			for _, slot := range slots {
				if failures[slot] == nil {
					valid++
				}
			}
			if o.ctx.Err() != nil {
				break
			}
		}
	}
	valid := 0
	for i, failure := range failures {
		var cleanup *cleanupFailure
		if errors.As(failure, &cleanup) {
			return nil, failure
		}
		if len(shards[i]) != 0 {
			valid++
		}
	}
	if err := o.ctx.Err(); err != nil {
		return nil, err
	}
	if valid < d.layout.Data {
		return nil, core.Fail(core.UpstreamFailure, errors.Join(errors.New("insufficient verified shards for repair"), errors.Join(failures...)))
	}
	if valid != len(shards) {
		if err := d.codec.Reconstruct(shards); err != nil {
			return nil, core.Fail(core.InvalidUpstreamResponse, err)
		}
		for slot := range shards {
			if sha256.Sum256(shards[slot]) != s.Shards[slot].Hash {
				return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("reconstructed shard checksum mismatch"))
			}
		}
	}
	return failures, nil
}
