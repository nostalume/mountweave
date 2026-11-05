package pool

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"path"

	core "github.com/nostalume/mountweave"
	bolt "go.etcd.io/bbolt"
)

var configKey = []byte("config")
var filesKey = []byte("files")
var uploadsKey = []byte("uploads")
var headerKey = []byte("header")
var membersKey = []byte("members")
var entriesKey = []byte("entries")
var dirsKey = []byte("directories")
var refsKey = []byte("references")
var garbageKey = []byte("garbage")
var jobsKey = []byte("maintenance")

type memberRecord struct {
	ID, Identity, FailureDomain string
	MaxBytes, UsedBytes         int64
	Draining, Removed           bool
	Policy                      uint64
}
type layout struct {
	Version, Data, Parity, ShardBytes int
	Codec                             string
	Membership                        uint64
	Members                           []memberRecord `json:",omitempty"` // Version-one import only.
}
type fileRecord struct {
	ID              string
	Length, Stripes int64
}
type uploadRecord struct {
	Path             string
	Complete         bool
	Objects, Garbage int64
}
type shardRecord struct {
	Member, Path string
	Attempt      int
	Hash         [32]byte
	Effect       core.Effect
	Reclaimed    bool `json:",omitempty"`
}
type stripeRecord struct {
	Bytes       int
	Membership  uint64
	NextAttempt int
	Shards      []shardRecord
	Abandoned   []shardRecord `json:",omitempty"`
}

func encode(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}
func decode(b []byte, v any) error {
	if b == nil {
		return core.Fail(core.InvalidUpstreamResponse, errors.New("missing pool metadata"))
	}
	if err := json.Unmarshal(b, v); err != nil {
		return core.Fail(core.InvalidUpstreamResponse, err)
	}
	return nil
}
func stripeKey(index int64) []byte {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], uint64(index))
	return b[:]
}
func stripeCount(length, size int64) int64 {
	n := length / size
	if length%size != 0 {
		n++
	}
	return n
}
func shardPath(id string, stripe int64, slot, attempt int) string {
	return fmt.Sprintf("/%s-%x-%x-%x", id, stripe, slot, attempt)
}

func (d *Driver) initialize() error {
	return d.db.Update(func(tx *bolt.Tx) error {
		for _, key := range [][]byte{configKey, filesKey, uploadsKey, membersKey, entriesKey, dirsKey, refsKey, garbageKey, jobsKey} {
			if _, err := tx.CreateBucketIfNotExists(key); err != nil {
				return err
			}
		}
		if err := tx.Bucket(dirsKey).Put([]byte("/"), []byte("/")); err != nil {
			return err
		}
		cfg := tx.Bucket(configKey)
		want := layout{Version: 2, Codec: "reedsolomon-vandermonde-gf256-v1", Membership: 1, Data: d.opts.DataShards, Parity: d.opts.ParityShards, ShardBytes: d.opts.ShardBytes}
		if raw := cfg.Get(headerKey); raw != nil {
			if err := decode(raw, &d.layout); err != nil {
				return err
			}
			if (d.layout.Version != 1 && d.layout.Version != 2) || d.layout.Codec != want.Codec || d.layout.Membership == 0 || d.layout.Data != want.Data || d.layout.Parity != want.Parity || d.layout.ShardBytes != want.ShardBytes {
				return core.Fail(core.InvalidRequest, errors.New("stored pool layout must match"))
			}
			if d.layout.Version == 1 {
				if err := d.importVersionOne(tx); err != nil {
					return err
				}
			}
			for _, m := range d.opts.Members {
				var stored memberRecord
				if err := decode(tx.Bucket(membersKey).Get([]byte(m.ID)), &stored); err != nil {
					return err
				}
				if stored.Removed || stored.Identity != m.Identity || stored.FailureDomain != m.FailureDomain {
					return core.Fail(core.InvalidRequest, errors.New("stored member identity must match"))
				}
			}
			return nil
		}
		if len(d.opts.Members) < want.Data+want.Parity {
			return core.Fail(core.InvalidRequest, errors.New("initial membership cannot satisfy the layout"))
		}
		for _, m := range d.opts.Members {
			r := memberRecord{ID: m.ID, Identity: m.Identity, FailureDomain: m.FailureDomain, MaxBytes: m.MaxBytes}
			if err := tx.Bucket(membersKey).Put([]byte(m.ID), encode(r)); err != nil {
				return err
			}
		}
		d.layout = want
		return cfg.Put(headerKey, encode(want))
	})
}

// The single metadata transaction imports the flat version-one namespace and
// derives conservative private-byte reservations from its existing intents.
func (d *Driver) importVersionOne(tx *bolt.Tx) error {
	for _, m := range d.layout.Members {
		m.UsedBytes = 0
		if err := tx.Bucket(membersKey).Put([]byte(m.ID), encode(m)); err != nil {
			return err
		}
	}
	files := tx.Bucket(filesKey)
	for {
		k, v := files.Cursor().First()
		if len(k) == 0 || k[0] != '/' {
			break
		}
		var f fileRecord
		if err := decode(v, &f); err != nil {
			return err
		}
		key := append([]byte(nil), k...)
		if err := tx.Bucket(entriesKey).Put(entryKey("/", path.Base(string(key))), encode(entryRecord{ID: f.ID, Generation: f.ID})); err != nil {
			return err
		}
		if err := tx.Bucket(refsKey).Put([]byte(f.ID), []byte(f.ID)); err != nil {
			return err
		}
		if err := files.Put([]byte(f.ID), encode(f)); err != nil {
			return err
		}
		if err := files.Delete(key); err != nil {
			return err
		}
	}
	var lastID []byte
	for {
		id, _ := nextAfter(tx.Bucket(uploadsKey), lastID)
		if id == nil {
			break
		}
		lastID = bytes.Clone(id)
		b := tx.Bucket(uploadsKey).Bucket(id)
		var u uploadRecord
		if err := decode(b.Get(headerKey), &u); err != nil {
			return err
		}
		u.Objects, u.Garbage = 0, 0
		var lastKey []byte
		for {
			k, v := nextAfter(b, lastKey)
			if k == nil {
				break
			}
			lastKey = bytes.Clone(k)
			if len(k) != 8 {
				continue
			}
			var s stripeRecord
			if err := decode(v, &s); err != nil {
				return err
			}
			for _, r := range append(append([]shardRecord(nil), s.Shards...), s.Abandoned...) {
				s.NextAttempt = max(s.NextAttempt, r.Attempt+1)
				if r.Effect == core.Unchanged {
					continue
				}
				if err := changeUsage(tx, r.Member, int64(d.layout.ShardBytes)); err != nil {
					return err
				}
			}
			_, objects, garbage := outstanding(s)
			u.Objects += objects
			u.Garbage += garbage
			if err := b.Put(k, encode(s)); err != nil {
				return err
			}
		}
		if err := b.Put(headerKey, encode(u)); err != nil {
			return err
		}
		if err := queueGarbage(tx, string(id), u); err != nil {
			return err
		}
	}
	d.layout.Version, d.layout.Members = 2, nil
	return tx.Bucket(configKey).Put(headerKey, encode(d.layout))
}

// Re-seek after mutation; bbolt forbids modifying a bucket inside ForEach.
func nextAfter(b *bolt.Bucket, key []byte) ([]byte, []byte) {
	c := b.Cursor()
	if key == nil {
		return c.First()
	}
	k, v := c.Seek(key)
	if bytes.Equal(k, key) {
		return c.Next()
	}
	return k, v
}

func (d *Driver) update(run func(*bolt.Tx) error) error {
	d.mu.Lock()
	stopped := d.writeErr
	d.mu.Unlock()
	if stopped != nil {
		return stopped
	}
	var callbackErr error
	err := d.db.Update(func(tx *bolt.Tx) error { callbackErr = run(tx); return callbackErr })
	if err != nil && callbackErr == nil {
		d.mu.Lock()
		d.writeErr = core.Fail(core.UpstreamFailure, err)
		d.mu.Unlock()
	}
	return err
}
func changeUsage(tx *bolt.Tx, id string, delta int64) error {
	var m memberRecord
	b := tx.Bucket(membersKey)
	if err := decode(b.Get([]byte(id)), &m); err != nil {
		return err
	}
	if delta > 0 && delta > math.MaxInt64-m.UsedBytes {
		return core.Fail(core.CapacityExceeded, nil)
	}
	if delta > 0 && (m.Removed || (m.MaxBytes > 0 && delta > m.MaxBytes-m.UsedBytes)) {
		return core.Fail(core.CapacityExceeded, nil)
	}
	if delta < -m.UsedBytes {
		return core.Fail(core.InvalidUpstreamResponse, nil)
	}
	m.UsedBytes += delta
	return b.Put([]byte(id), encode(m))
}
func outstanding(s stripeRecord) (map[string]int64, int64, int64) {
	counts := make(map[string]int64)
	var total, garbage int64
	for _, r := range s.Shards {
		if !r.Reclaimed && r.Effect != core.Unchanged {
			counts[r.Member]++
			total++
		}
	}
	for _, r := range s.Abandoned {
		if !r.Reclaimed && r.Effect != core.Unchanged {
			counts[r.Member]++
			total++
			garbage++
		}
	}
	return counts, total, garbage
}
func queueGarbage(tx *bolt.Tx, id string, u uploadRecord) error {
	if !u.Complete || u.Garbage > 0 || tx.Bucket(refsKey).Get([]byte(id)) == nil {
		return tx.Bucket(garbageKey).Put([]byte(id), []byte{1})
	}
	return tx.Bucket(garbageKey).Delete([]byte(id))
}
func (d *Driver) putStripe(tx *bolt.Tx, id string, index int64, s stripeRecord) error {
	b := tx.Bucket(uploadsKey).Bucket([]byte(id))
	if b == nil {
		return core.Fail(core.InvalidUpstreamResponse, nil)
	}
	var old stripeRecord
	if raw := b.Get(stripeKey(index)); raw != nil {
		if err := decode(raw, &old); err != nil {
			return err
		}
	}
	before, oldObjects, oldGarbage := outstanding(old)
	after, objects, garbage := outstanding(s)
	for member, count := range before {
		after[member] -= count
	}
	for member, delta := range after {
		if delta != 0 {
			if err := changeUsage(tx, member, delta*int64(d.layout.ShardBytes)); err != nil {
				return err
			}
		}
	}
	var u uploadRecord
	if err := decode(b.Get(headerKey), &u); err != nil {
		return err
	}
	u.Objects += objects - oldObjects
	u.Garbage += garbage - oldGarbage
	if u.Objects < 0 || u.Garbage < 0 {
		return core.Fail(core.InvalidUpstreamResponse, nil)
	}
	if members := d.active[id]; members != nil {
		for _, list := range [][]shardRecord{s.Shards, s.Abandoned} {
			for _, r := range list {
				if !r.Reclaimed && r.Effect != core.Unchanged {
					members[r.Member] = true
				}
			}
		}
	}
	if err := b.Put(stripeKey(index), encode(s)); err != nil {
		return err
	}
	if err := b.Put(headerKey, encode(u)); err != nil {
		return err
	}
	return queueGarbage(tx, id, u)
}

func (d *Driver) file(p string) (f fileRecord, err error) {
	err = d.db.View(func(tx *bolt.Tx) error {
		e, err := lookup(tx, p)
		if err != nil {
			return err
		}
		if e.Directory {
			return core.Fail(core.NotFile, nil)
		}
		return decode(tx.Bucket(filesKey).Get([]byte(e.Generation)), &f)
	})
	return
}
func metadata(name string, e entryRecord, f fileRecord) core.Metadata {
	if e.Directory {
		return core.Metadata{Name: name, Kind: core.Directory}
	}
	n := f.Length
	return core.Metadata{Name: name, Kind: core.File, Size: &n, Evidence: core.Evidence{Validator: core.Validator{Domain: "pool-generation", Tag: `"` + f.ID + `"`}, Version: core.Version(f.ID)}}
}
func (d *Driver) stripe(f fileRecord, index int64) (s stripeRecord, err error) {
	err = d.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(uploadsKey).Bucket([]byte(f.ID))
		if b == nil {
			return core.Fail(core.InvalidUpstreamResponse, nil)
		}
		if err := decode(b.Get(stripeKey(index)), &s); err != nil {
			return err
		}
		n, size := d.layout.Data+d.layout.Parity, int64(d.layout.Data)*int64(d.layout.ShardBytes)
		if index < 0 || index >= f.Stripes || int64(s.Bytes) != min(size, f.Length-index*size) || len(s.Shards) != n || s.Membership == 0 {
			return core.Fail(core.InvalidUpstreamResponse, nil)
		}
		domains := make(map[string]bool)
		for slot, shard := range s.Shards {
			var member memberRecord
			if err := decode(tx.Bucket(membersKey).Get([]byte(shard.Member)), &member); err != nil {
				return err
			}
			if member.Removed || domains[member.FailureDomain] || shard.Reclaimed || shard.Effect != core.Applied || shard.Attempt < 0 || shard.Path != shardPath(f.ID, index, slot, shard.Attempt) {
				return core.Fail(core.InvalidUpstreamResponse, errors.New("invalid shard placement"))
			}
			domains[member.FailureDomain] = true
		}
		return nil
	})
	return
}
