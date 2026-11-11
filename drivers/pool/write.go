package pool

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"io"
	"math"

	core "github.com/nostalume/mountweave"
	bolt "go.etcd.io/bbolt"
)

func (d *Driver) Create(ctx context.Context, pAddress core.NativeAddress, input io.Reader, length *int64) (core.Change, error) {
	if pAddress.Namespace() != core.TreeNamespace {
		return core.Change{Effect: core.Unchanged}, core.Fail(core.UnsupportedOperation,
			nil)
	}
	p := pAddress.Value()

	return d.write(ctx, p, input, length, core.PublicationOptions{When: core.IfAbsent()}, core.Validator{})
}
func (d *Driver) Publish(ctx context.Context, pAddress core.NativeAddress, input io.Reader, length *int64, options core.PublicationOptions) (core.Change, error) {
	if pAddress.Namespace() != core.TreeNamespace {
		return core.Change{Effect: core.Unchanged}, core.Fail(core.UnsupportedOperation,
			nil)
	}
	p := pAddress.Value()

	return d.write(ctx, p, input, length, options, core.Validator{})
}

func (d *Driver) PublishForMove(ctx context.Context, p core.NativeAddress, parent core.Validator, input io.Reader, length *int64) (core.Change, error) {
	if p.Namespace() != core.TreeNamespace || parent == (core.Validator{}) {
		return core.Change{Effect: core.Unchanged}, core.Fail(core.UnsupportedOperation, nil)
	}
	return d.write(ctx, p.Value(), input, length, core.PublicationOptions{When: core.IfAbsent()}, parent)
}

// The parent identity is captured before input. The final name is resolved in
// that same directory, even after a rename; an unlinked parent cannot publish.
func (d *Driver) write(ctx context.Context, p string, input io.Reader, length *int64, options core.PublicationOptions, expected core.Validator) (result core.Change, err error) {
	replace := !options.When.IsAbsent()
	if err := options.When.ForDomain("pool-generation"); err != nil {
		return core.Change{Effect: core.Unchanged}, err
	}
	result.Effect = core.Unchanged
	if err = core.ValidatePath(p); err != nil {
		return
	}
	if p == "/" || input == nil || (length != nil && *length < 0) {
		return result, core.Fail(core.InvalidRequest, nil)
	}
	if length != nil {
		n := *length
		length = &n
	}
	o, err := d.begin(ctx)
	if err != nil {
		return result, err
	}
	defer o.finish()
	defer func() { err = errors.Join(err, o.ctx.Err()) }()
	var parent, name string
	err = d.db.View(func(tx *bolt.Tx) error {
		var e error
		parent, name, e = parentAt(tx, p)
		if e != nil {
			return e
		}
		if expected != (core.Validator{}) && expected != directoryValidator(parent) {
			return core.Fail(core.PreconditionFailed, nil)
		}
		if raw := tx.Bucket(entriesKey).Get(entryKey(parent, name)); raw != nil {
			var old entryRecord
			if e := decode(raw, &old); e != nil {
				return e
			}
			if old.Directory {
				return core.Fail(core.NotFile, nil)
			}
			if !replace {
				return core.Fail(core.AlreadyExists, nil)
			}
		}
		return nil
	})
	if err != nil {
		return
	}
	key := string(entryKey(parent, name))
	d.mu.Lock()
	if d.writes[key] {
		d.mu.Unlock()
		return result, core.Fail(core.UpstreamThrottled, nil)
	}
	d.writes[key] = true
	d.mu.Unlock()
	defer func() { d.mu.Lock(); delete(d.writes, key); d.mu.Unlock() }()
	d.state.Lock()
	err = d.db.View(func(tx *bolt.Tx) error { return d.coverage(tx) })
	d.state.Unlock()
	if err != nil {
		return
	}
	if err = d.reserve(o.ctx); err != nil {
		return
	}
	defer d.release()
	shards := d.buffers()
	f := fileRecord{ID: rand.Text()}
	d.state.Lock()
	d.active[f.ID] = make(map[string]bool)
	err = d.update(func(tx *bolt.Tx) error {
		b, e := tx.Bucket(uploadsKey).CreateBucket([]byte(f.ID))
		if e != nil {
			return e
		}
		u := uploadRecord{Path: p}
		if e = b.Put(headerKey, encode(u)); e != nil {
			return e
		}
		return queueGarbage(tx, f.ID, u)
	})
	d.state.Unlock()
	defer func() { d.state.Lock(); delete(d.active, f.ID); d.state.Unlock() }()
	if err != nil {
		return
	}
	for {
		if err = o.ctx.Err(); err != nil {
			return
		}
		for _, b := range shards {
			clear(b)
		}
		size, endErr := readStripe(o.ctx, input, shards[:d.layout.Data])
		if endErr != nil && endErr != io.EOF {
			return result, endErr
		}
		if f.Length > math.MaxInt64-int64(size) || (length != nil && int64(size) > *length-f.Length) {
			return result, core.Fail(core.InvalidRequest, errors.New("input exceeds declared length"))
		}
		if size > 0 {
			if err = d.codec.Encode(shards); err != nil {
				return
			}
			if err = d.writeStripe(o.ctx, f.ID, f.Stripes, size, shards); err != nil {
				return
			}
			f.Length += int64(size)
			f.Stripes++
		}
		if endErr == io.EOF {
			break
		}
	}
	if length != nil && f.Length != *length {
		return result, io.ErrUnexpectedEOF
	}
	before := core.TargetUnknown
	result, err = d.commit(o.ctx, func(tx *bolt.Tx) error {
		if expected != (core.Validator{}) {
			current, _, err := parentAt(tx, p)
			if err != nil {
				return err
			}
			if current != parent {
				return core.Fail(core.PreconditionFailed, nil)
			}
		}
		if tx.Bucket(dirsKey).Get([]byte(parent)) == nil {
			return core.Fail(core.NotFound, nil)
		}
		entry := entryRecord{ID: f.ID, Generation: f.ID}
		before = core.TargetMissing
		var previous core.Evidence
		if raw := tx.Bucket(entriesKey).Get([]byte(key)); raw != nil {
			before = core.TargetPresent
			var old entryRecord
			if e := decode(raw, &old); e != nil {
				return e
			}
			if old.Directory {
				return core.Fail(core.NotFile, nil)
			}
			if !replace {
				return core.Fail(core.AlreadyExists, nil)
			}
			previous = metadata(name, old, fileRecord{ID: old.Generation}).Evidence
			if err := options.When.Check(previous, true); err != nil {
				return err
			}
			entry.ID = old.ID
			if e := markUnreferenced(tx, old.Generation); e != nil {
				return e
			}
		} else if err := options.When.Check(previous, false); err != nil {
			return err
		}
		if e := tx.Bucket(filesKey).Put([]byte(f.ID), encode(f)); e != nil {
			return e
		}
		if e := tx.Bucket(entriesKey).Put([]byte(key), encode(entry)); e != nil {
			return e
		}
		if e := tx.Bucket(refsKey).Put([]byte(f.ID), []byte(entry.ID)); e != nil {
			return e
		}
		b := tx.Bucket(uploadsKey).Bucket([]byte(f.ID))
		var u uploadRecord
		if e := decode(b.Get(headerKey), &u); e != nil {
			return e
		}
		u.Complete = true
		if e := b.Put(headerKey, encode(u)); e != nil {
			return e
		}
		return queueGarbage(tx, f.ID, u)
	})
	result.Before = before
	if result.Effect == core.Applied {
		result.Evidence = metadata(name, entryRecord{}, f).Evidence
	}
	return
}

func readStripe(ctx context.Context, input io.Reader, data [][]byte) (total int, err error) {
	empty := 0
	for _, b := range data {
		for offset := 0; offset < len(b); {
			if err = ctx.Err(); err != nil {
				return
			}
			n, cause := input.Read(b[offset:])
			if n < 0 || n > len(b)-offset {
				return total, core.Fail(core.InvalidUpstreamResponse, nil)
			}
			total += n
			offset += n
			if cause != nil {
				return total, cause
			}
			if n == 0 {
				empty++
				if empty == 100 {
					return total, io.ErrNoProgress
				}
			} else {
				empty = 0
			}
		}
	}
	return
}

func (d *Driver) saveStripe(id string, index int64, s stripeRecord) error {
	d.state.Lock()
	defer d.state.Unlock()
	return d.update(func(tx *bolt.Tx) error { return d.putStripe(tx, id, index, s) })
}
func (d *Driver) candidates(tx *bolt.Tx) ([]memberRecord, error) {
	var eligible []memberRecord
	err := tx.Bucket(membersKey).ForEach(func(_, raw []byte) error {
		var m memberRecord
		if err := decode(raw, &m); err != nil {
			return err
		}
		if m.Removed || m.Draining {
			return nil
		}
		if _, ok := d.availableMember(m.ID).(core.FileCreator); !ok {
			return nil
		}
		if m.MaxBytes > 0 && int64(d.layout.ShardBytes) > m.MaxBytes-m.UsedBytes {
			return nil
		}
		eligible = append(eligible, m)
		return nil
	})
	return eligible, err
}
func chooseMember(eligible []memberRecord, used map[string]bool, offset int) (memberRecord, error) {
	least, count := int64(math.MaxInt64), 0
	for _, m := range eligible {
		if used[m.FailureDomain] {
			continue
		}
		if m.UsedBytes < least {
			least, count = m.UsedBytes, 0
		}
		if m.UsedBytes == least {
			count++
		}
	}
	if count == 0 {
		return memberRecord{}, core.Fail(core.CapacityExceeded, errors.New("no eligible private shard capacity"))
	}
	chosen := offset % count
	for _, m := range eligible {
		if used[m.FailureDomain] || m.UsedBytes != least {
			continue
		}
		if chosen == 0 {
			used[m.FailureDomain] = true
			return m, nil
		}
		chosen--
	}
	return memberRecord{}, core.Fail(core.InternalError, nil)
}
func (d *Driver) coverage(tx *bolt.Tx) error {
	domains, readOnly := make(map[string]bool), false
	err := tx.Bucket(membersKey).ForEach(func(_, raw []byte) error {
		var m memberRecord
		if err := decode(raw, &m); err != nil {
			return err
		}
		if m.Removed || m.Draining || d.availableMember(m.ID) == nil {
			return nil
		}
		if _, ok := d.availableMember(m.ID).(core.FileCreator); !ok {
			readOnly = true
			return nil
		}
		if m.MaxBytes == 0 || int64(d.layout.ShardBytes) <= m.MaxBytes-m.UsedBytes {
			domains[m.FailureDomain] = true
		}
		return nil
	})
	if err != nil {
		return err
	}
	if len(domains) < d.layout.Data+d.layout.Parity {
		code := core.CapacityExceeded
		if readOnly {
			code = core.UnsupportedOperation
		}
		return core.Fail(code, errors.New("full writable redundancy is unavailable"))
	}
	return nil
}
func (d *Driver) writeStripe(ctx context.Context, id string, index int64, size int, data [][]byte) error {
	s := stripeRecord{Bytes: size, NextAttempt: 1, Shards: make([]shardRecord, len(data))}
	used := make(map[string]bool)
	d.state.Lock()
	err := d.update(func(tx *bolt.Tx) error {
		var cfg layout
		if err := decode(tx.Bucket(configKey).Get(headerKey), &cfg); err != nil {
			return err
		}
		s.Membership = cfg.Membership
		eligible, err := d.candidates(tx)
		if err != nil {
			return err
		}
		for i := range data {
			m, err := chooseMember(eligible, used, int(index%256)+i)
			if err != nil {
				return err
			}
			s.Shards[i] = shardRecord{Member: m.ID, Path: shardPath(id, index, i, 0), Hash: sha256.Sum256(data[i])}
		}
		return d.putStripe(tx, id, index, s)
	})
	d.state.Unlock()
	if err != nil {
		return err
	}
	results := make([]error, len(data))
	write := func(i int) {
		r := &s.Shards[i]
		if err := ctx.Err(); err != nil {
			r.Effect = core.Unchanged
			results[i] = err
			return
		}
		n := int64(len(data[i]))
		change, err := d.createShard(ctx, *r, bytes.NewReader(data[i]), n)
		r.Effect = change.Effect
		if change.Effect != core.Applied || err != nil {
			if err == nil {
				err = core.Fail(core.InvalidUpstreamResponse, nil)
			}
			results[i] = err
		}
	}
	parallel(len(data), d.opts.Concurrency, write)
	for i := range data {
		for results[i] != nil && ctx.Err() == nil {
			d.state.Lock()
			err = d.update(func(tx *bolt.Tx) error {
				eligible, err := d.candidates(tx)
				if err != nil {
					return err
				}
				m, err := chooseMember(eligible, used, i)
				if err != nil {
					return err
				}
				next := shardRecord{Member: m.ID, Path: shardPath(id, index, i, s.NextAttempt), Attempt: s.NextAttempt, Hash: sha256.Sum256(data[i])}
				s.NextAttempt++
				s.Abandoned = append(s.Abandoned, s.Shards[i])
				s.Shards[i] = next
				var cfg layout
				if err := decode(tx.Bucket(configKey).Get(headerKey), &cfg); err != nil {
					return err
				}
				s.Membership = cfg.Membership
				return d.putStripe(tx, id, index, s)
			})
			d.state.Unlock()
			if err != nil {
				break
			}
			results[i] = nil
			write(i)
		}
	}
	return errors.Join(errors.Join(results...), d.saveStripe(id, index, s), ctx.Err())
}

var _ core.FilePublisher = (*Driver)(nil)
