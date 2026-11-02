package pool

import (
	"context"
	"errors"

	core "github.com/nostalume/mountweave"
	bolt "go.etcd.io/bbolt"
)

type MaintenanceKind string

const (
	RepairWork  MaintenanceKind = "repair"
	DrainWork   MaintenanceKind = "drain"
	CollectWork MaintenanceKind = "collect"
)

type MaintenanceRequest struct {
	Kind   MaintenanceKind
	Member string // Required only for drain.
}

// MaintenanceJob is the durable intent and latest acknowledged batch receipt.
// Last is not an exactly-once cumulative counter: a crash may repeat a batch.
// Done marks one finite pass, not a promise that future writes need no repair.
type MaintenanceJob struct {
	ID      string
	Request MaintenanceRequest
	Done    bool
	Last    Maintenance
}
type maintenanceRecord struct {
	Version int
	MaintenanceJob
	Scan drainState
}

// PlanMaintenance durably accepts a caller-chosen ID. Repeating the same ID and
// request is unchanged and does not replay a completed pass; another request at
// that ID is rejected. No work is started and no queue becomes an authority.
func (d *Driver) PlanMaintenance(ctx context.Context, id string, request MaintenanceRequest) (core.Change, error) {
	if !core.ValidName(id) || len(id) > 128 ||
		(request.Kind != RepairWork && request.Kind != DrainWork && request.Kind != CollectWork) ||
		(request.Kind == DrainWork && !core.ValidName(request.Member)) ||
		(request.Kind != DrainWork && request.Member != "") {
		return core.Change{Effect: core.Unchanged}, core.Fail(core.InvalidRequest, nil)
	}
	o, err := d.begin(ctx)
	if err != nil {
		return core.Change{Effect: core.Unchanged}, err
	}
	defer o.finish()
	repeated := false
	change, err := d.commit(o.ctx, func(tx *bolt.Tx) error {
		b := tx.Bucket(jobsKey)
		if raw := b.Get([]byte(id)); raw != nil {
			var old maintenanceRecord
			if err := decode(raw, &old); err != nil {
				return err
			}
			if old.Version != 1 || old.Request != request {
				return core.Fail(core.AlreadyExists, nil)
			}
			repeated = true
			return nil
		}
		if request.Kind == DrainWork {
			var m memberRecord
			raw := tx.Bucket(membersKey).Get([]byte(request.Member))
			if raw == nil {
				return core.Fail(core.NotFound, nil)
			}
			if err := decode(raw, &m); err != nil {
				return err
			}
			if m.Removed {
				return core.Fail(core.MountUnavailable, nil)
			}
		}
		job := maintenanceRecord{Version: 1, MaintenanceJob: MaintenanceJob{ID: id, Request: request}}
		return b.Put([]byte(id), encode(job))
	})
	if repeated && change.Effect == core.Applied {
		change.Effect = core.Unchanged
	}
	return change, err
}

// MaintenanceJobs returns up to limit intents ordered by ID, strictly after
// after. The host retains/removes terminal receipts explicitly, and may submit
// unfinished IDs to the existing task.Queue with RunMaintenance as its closure.
func (d *Driver) MaintenanceJobs(ctx context.Context, after string, limit int) (jobs []MaintenanceJob, err error) {
	if limit < 1 {
		return nil, core.Fail(core.InvalidRequest, nil)
	}
	o, err := d.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer o.finish()
	err = d.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(jobsKey)
		key := []byte(after)
		for len(jobs) < limit {
			if err := o.ctx.Err(); err != nil {
				return err
			}
			k, raw := nextAfter(b, key)
			if k == nil {
				break
			}
			var record maintenanceRecord
			if err := decode(raw, &record); err != nil {
				return err
			}
			if record.Version != 1 {
				return core.Fail(core.InvalidUpstreamResponse, nil)
			}
			jobs = append(jobs, record.MaintenanceJob)
			key = k
		}
		return nil
	})
	return
}

// RunMaintenance resumes one bounded batch from its persisted checkpoint. Native
// receipts are saved before progress. A crash between them replays only safe,
// conditional work; it never publishes an incomplete borrowed user stream.
func (d *Driver) RunMaintenance(ctx context.Context, id string, limit int) (job MaintenanceJob, err error) {
	o, err := d.maintenanceStart(ctx, limit)
	if err != nil {
		return job, err
	}
	defer o.finish()
	defer d.maintenance.Unlock()
	var record maintenanceRecord
	err = d.db.View(func(tx *bolt.Tx) error {
		raw := tx.Bucket(jobsKey).Get([]byte(id))
		if raw == nil {
			return core.Fail(core.NotFound, nil)
		}
		return decode(raw, &record)
	})
	if err != nil {
		return job, err
	}
	if record.Version != 1 {
		return job, core.Fail(core.InvalidUpstreamResponse, nil)
	}
	if record.Done {
		return record.MaintenanceJob, nil
	}
	switch record.Request.Kind {
	case RepairWork:
		record.Last, err = d.repair(o, limit, &record.Scan.Cursor)
	case DrainWork:
		record.Last, err = d.drain(o, record.Request.Member, limit, &record.Scan)
	case CollectWork:
		record.Last, err = d.collect(o, limit, &record.Scan.Cursor)
	default:
		return record.MaintenanceJob, core.Fail(core.InvalidUpstreamResponse, nil)
	}
	record.Done = err == nil && !record.Last.More
	d.state.Lock()
	saveErr := d.update(func(tx *bolt.Tx) error { return tx.Bucket(jobsKey).Put([]byte(id), encode(record)) })
	d.state.Unlock()
	return record.MaintenanceJob, errors.Join(err, saveErr, o.ctx.Err())
}

// ForgetMaintenance drops an inactive intent/receipt without undoing effects.
// Private object ownership remains in the shard ledger. A running batch must
// first acknowledge cancellation/return. Reusing the ID creates a new pass.
func (d *Driver) ForgetMaintenance(ctx context.Context, id string) (core.Change, error) {
	o, err := d.maintenanceStart(ctx, 1)
	if err != nil {
		return core.Change{Effect: core.Unchanged}, err
	}
	defer o.finish()
	defer d.maintenance.Unlock()
	return d.commit(o.ctx, func(tx *bolt.Tx) error {
		b := tx.Bucket(jobsKey)
		raw := b.Get([]byte(id))
		if raw == nil {
			return core.Fail(core.NotFound, nil)
		}
		var record maintenanceRecord
		if err := decode(raw, &record); err != nil {
			return err
		}
		if record.Version != 1 {
			return core.Fail(core.InvalidUpstreamResponse, nil)
		}
		return b.Delete([]byte(id))
	})
}
