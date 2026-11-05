package pool

import (
	"context"
	"errors"
	"io"
	"sync"

	core "github.com/nostalume/mountweave"
	bolt "go.etcd.io/bbolt"
)

// memberRuntime owns one native instance, including its outstanding bodies.
// Its mutable admission fields are protected by Driver.state.
type memberRuntime struct {
	native               core.Driver
	life                 context.Context
	cancel               context.CancelFunc
	uses                 int
	idle                 chan struct{}
	retiring, changing   bool
	closeOnce            sync.Once
	closeErr, cleanupErr error
}

func newMemberRuntime(native core.Driver) *memberRuntime {
	ctx, cancel := context.WithCancel(context.Background())
	idle := make(chan struct{})
	close(idle)
	return &memberRuntime{native: native, life: ctx, cancel: cancel, idle: idle}
}
func (m *memberRuntime) close() error {
	if m == nil {
		return nil
	}
	m.closeOnce.Do(func() {
		m.cancel()
		if m.native != nil {
			m.closeErr = m.native.Close()
		}
		m.closeErr = errors.Join(m.closeErr, m.cleanupErr)
	})
	return m.closeErr
}

// availableMember is only an admission hint; callers hold state. Native I/O
// must acquire a use so retirement also waits for returned bodies to close.
func (d *Driver) availableMember(id string) core.Driver {
	m := d.members[id]
	if m == nil || m.retiring {
		return nil
	}
	return m.native
}

type memberUse struct {
	ctx        context.Context
	native     core.Driver
	d          *Driver
	owner      *memberRuntime
	cancel     context.CancelFunc
	stop       func() bool
	cleanupErr error
}

func (u *memberUse) finish() {
	u.stop()
	u.cancel()
	u.d.state.Lock()
	u.owner.cleanupErr = errors.Join(u.owner.cleanupErr, u.cleanupErr)
	u.owner.uses--
	if u.owner.uses == 0 {
		close(u.owner.idle)
	}
	u.d.state.Unlock()
}
func (d *Driver) borrowMember(ctx context.Context, id string) (*memberUse, error) {
	d.state.Lock()
	defer d.state.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m := d.members[id]
	if m == nil || m.retiring || m.native == nil {
		return nil, core.Fail(core.MountUnavailable, nil)
	}
	if m.uses == 0 {
		m.idle = make(chan struct{})
	}
	m.uses++
	life, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(m.life, cancel)
	return &memberUse{ctx: life, native: m.native, d: d, owner: m, cancel: cancel, stop: stop}, nil
}

func (d *Driver) createShard(ctx context.Context, shard shardRecord, input io.Reader, length int64) (core.Change, error) {
	u, err := d.borrowMember(ctx, shard.Member)
	if err != nil {
		return core.Change{Effect: core.Unchanged}, err
	}
	defer u.finish()
	creator, ok := u.native.(core.FileCreator)
	if !ok {
		return core.Change{Effect: core.Unchanged}, core.Fail(core.UnsupportedOperation, nil)
	}
	change, err := creator.Create(u.ctx, core.NativePath(shard.Path), input, &length)
	return change, errors.Join(err, u.ctx.Err())
}

// ReconnectMember retires the old instance before invoking build. The host must
// assert the stored account/root identity and load its latest credential state
// inside build, never prepare a second refresh writer in advance. A nil build
// leaves the member offline. A non-nil returned Driver transfers ownership even
// with a build error; it is then closed, with any cleanup failure retained.
// Applied means retirement began; cancellation may leave it offline/retiring.
// Cancellation bounds waiting, not native Close or a blocked build function.
func (d *Driver) ReconnectMember(ctx context.Context, id, identity string, build func(context.Context) (core.Driver, error)) (core.Change, error) {
	unchanged := core.Change{Effect: core.Unchanged}
	o, err := d.begin(ctx)
	if err != nil {
		return unchanged, err
	}
	defer o.finish()
	d.state.Lock()
	err = d.db.View(func(tx *bolt.Tx) error {
		raw := tx.Bucket(membersKey).Get([]byte(id))
		if raw == nil {
			return core.Fail(core.NotFound, nil)
		}
		var m memberRecord
		if err := decode(raw, &m); err != nil {
			return err
		}
		if m.Removed || m.Identity != identity {
			return core.Fail(core.InvalidRequest, nil)
		}
		return nil
	})
	old := d.members[id]
	if err == nil && old != nil && old.changing {
		err = core.Fail(core.UpstreamThrottled, nil)
	}
	if err != nil {
		d.state.Unlock()
		return unchanged, err
	}
	if old == nil {
		old = newMemberRuntime(nil)
		d.members[id] = old
	}
	old.retiring, old.changing = true, true
	old.cancel()
	idle := old.idle
	d.state.Unlock()
	defer func() { d.state.Lock(); old.changing = false; d.state.Unlock() }()
	applied := core.Change{Effect: core.Applied}
	select {
	case <-idle:
	case <-o.ctx.Done():
		return applied, o.ctx.Err()
	}
	if err = old.close(); err != nil {
		return applied, err
	}
	if err = o.ctx.Err(); err != nil {
		return applied, err
	}
	if build == nil {
		return applied, nil
	}
	native, err := build(o.ctx)
	if err == nil && native == nil {
		err = core.Fail(core.InvalidUpstreamResponse, nil)
	}
	if err != nil {
		if native != nil {
			candidate := newMemberRuntime(native)
			candidate.retiring = true
			err = errors.Join(err, candidate.close())
			d.state.Lock()
			d.members[id] = candidate
			d.state.Unlock()
		}
		return applied, err
	}
	d.state.Lock()
	d.members[id] = newMemberRuntime(native)
	d.state.Unlock()
	return applied, o.ctx.Err()
}
