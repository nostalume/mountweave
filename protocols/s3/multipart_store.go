package s3

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/input"
	"github.com/nostalume/mountweave/protocols"
	bolt "go.etcd.io/bbolt"
)

// MultipartOptions enables durable S3 multipart uploads. The host owns a
// synchronized private database and input store, with one active handler using
// them for multipart. Drain it before rebuilding it or closing either store.
type MultipartOptions struct {
	DB              *bolt.DB
	Inputs          *input.Store
	MaxUploads      int
	MaxPendingParts int
	MaxObjectBytes  int64
	// MaxCheckpointBytes enables native continuation for new uploads when
	// positive. Bind must resolve a ContinuableWriter for those uploads.
	// This budget limits appends to each upload's protected native log.
	MaxCheckpointBytes int
	Retention          time.Duration
	// Bind authorizes staging and resolves the exact target under host binding
	// activation. An empty expected identity captures the current generation.
	// Otherwise it must verify that identity before returning a fresh Selection.
	// Core separately authorizes publication under that selection.
	Bind func(ctx context.Context, actor core.Principal, bucket string, target core.Address, expected string) (identity string, selection *core.Selection, err error)
}

const (
	receiving  = "receiving"
	committing = "committing"
	paused     = "paused"
	completed  = "completed"
	aborted    = "aborted"
	unknown    = "unknown"
)

var (
	manifestRoot = []byte("mountweave.s3.multipart.v1")
	uploadTable  = []byte("uploads")
	uploadMeta   = []byte("meta")
	uploadFrozen = []byte("frozen")
	partTable    = []byte("parts")
	pendingTable = []byte("pending")
	uploadCount  = []byte("upload-count")
	pendingCount = []byte("pending-count")
)

type uploadRecord struct {
	Actor   core.Principal
	Bucket  string
	Address core.Address
	Binding string
	Phase   string
	Expires time.Time

	LegacyFrozen           *completeRequest `json:"Frozen,omitempty"`
	hasFrozen              bool
	Digest                 [32]byte
	Change                 core.Change
	Native, AbortRequested bool
}

type partRecord struct {
	ID, Owner, Version string
	Size               int64
	Tag, CRC           string
	Modified           *time.Time `json:",omitempty"`
}

type partGroup struct {
	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}
	active int
}

type multipart struct {
	MultipartOptions
	gate  sync.RWMutex // Recovery runs with no live HTTP operations.
	mu    sync.Mutex
	parts map[string]*partGroup
	stops map[string]bool
}

func newMultipart(o MultipartOptions) (*multipart, error) {
	if o.DB == nil || o.DB.NoSync || o.Inputs == nil || o.Bind == nil || o.MaxUploads <= 0 ||
		o.MaxPendingParts <= 0 || o.MaxObjectBytes <= 0 || o.MaxCheckpointBytes < 0 || o.Retention <= 0 {
		return nil, core.Fail(core.InvalidRequest, nil)
	}
	m := &multipart{MultipartOptions: o, parts: make(map[string]*partGroup), stops: make(map[string]bool)}
	if err := o.DB.Update(func(tx *bolt.Tx) error {
		root, err := tx.CreateBucketIfNotExists(manifestRoot)
		if err != nil {
			return err
		}
		_, err = root.CreateBucketIfNotExists(uploadTable)
		return err
	}); err != nil {
		return nil, err
	}
	return m, nil
}

// RecoverMultipart reclaims expired uploads and resumes private cleanup. It
// requires a quiescent handler. Interrupted publications retain their parts
// and become unknown instead of being replayed or implicitly aborted.
func (h *Handler) RecoverMultipart(ctx context.Context) error {
	if h.multipart == nil {
		return core.Fail(core.UnsupportedOperation, nil)
	}
	m := h.multipart
	if !m.gate.TryLock() {
		return uploadConflict()
	}
	unresolved, err := m.recover(ctx)
	m.gate.Unlock()
	if report := h.options.OnFailure; report != nil {
		for _, item := range unresolved {
			report(protocols.Failure{Operation: "CompleteMultipartUpload", Phase: "recovery",
				Err: fmt.Errorf("multipart upload %s has unknown publication effect", item.id), HasChange: true, Change: item.change})
		}
	}
	return err
}

func noUpload() error          { return fault(http.StatusNotFound, "NoSuchUpload") }
func uploadConflict() error    { return fault(http.StatusConflict, "OperationAborted") }
func multipartCapacity() error { return fault(http.StatusServiceUnavailable, "SlowDown") }

func addCount(root *bolt.Bucket, key []byte, delta int) error {
	n := count(root, key) + delta
	if n < 0 {
		return errors.New("invalid multipart manifest count")
	}
	return root.Put(key, binary.BigEndian.AppendUint64(nil, uint64(n)))
}

func count(root *bolt.Bucket, key []byte) int {
	v := root.Get(key)
	if len(v) == 0 {
		return 0
	}
	return int(binary.BigEndian.Uint64(v))
}

func putRecord(b *bolt.Bucket, key []byte, v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return b.Put(key, data)
}

func readUpload(tx *bolt.Tx, id string) (*bolt.Bucket, uploadRecord, error) {
	b := tx.Bucket(manifestRoot).Bucket(uploadTable).Bucket([]byte(id))
	if b == nil {
		return nil, uploadRecord{}, noUpload()
	}
	var r uploadRecord
	err := json.Unmarshal(b.Get(uploadMeta), &r)
	r.hasFrozen = b.Get(uploadFrozen) != nil
	return b, r, err
}

type uploadAccess struct {
	id, bucket, key string
	actor           core.Principal
}

func (a uploadAccess) read(tx *bolt.Tx) (*bolt.Bucket, uploadRecord, error) {
	b, r, err := readUpload(tx, a.id)
	if err == nil && (r.Actor != a.actor || r.Bucket != a.bucket || r.Address.Value() != a.key) {
		err = noUpload()
	}
	return b, r, err
}

func (m *multipart) create(ctx context.Context, a uploadAccess, address core.Address) (string, error) {
	binding, selected, err := m.Bind(ctx, a.actor, a.bucket, address, "")
	if err != nil {
		return "", err
	}
	if binding == "" || selected == nil {
		return "", core.Fail(core.InvalidRequest, nil)
	}
	id := rand.Text()
	r := uploadRecord{Actor: a.actor, Bucket: a.bucket, Address: address, Binding: binding, Phase: receiving, Expires: time.Now().Add(m.Retention), Native: m.MaxCheckpointBytes > 0}
	err = m.DB.Update(func(tx *bolt.Tx) error {
		root := tx.Bucket(manifestRoot)
		if err := ctx.Err(); err != nil {
			return err
		}
		if count(root, uploadCount) >= m.MaxUploads {
			return multipartCapacity()
		}
		b, err := root.Bucket(uploadTable).CreateBucket([]byte(id))
		if err != nil {
			return err
		}
		if _, err = b.CreateBucket(partTable); err != nil {
			return err
		}
		if _, err = b.CreateBucket(pendingTable); err != nil {
			return err
		}
		if err = putRecord(b, uploadMeta, r); err != nil {
			return err
		}
		return addCount(root, uploadCount, 1)
	})
	return id, err
}

func (m *multipart) bound(ctx context.Context, r uploadRecord, address core.Address) (*core.Selection, error) {
	if r.Address != address {
		return nil, core.Fail(core.MountUnavailable, nil)
	}
	actual, selected, err := m.Bind(ctx, r.Actor, r.Bucket, address, r.Binding)
	if err == nil && (actual != r.Binding || selected == nil) {
		err = core.Fail(core.MountUnavailable, nil)
	}
	return selected, err
}

func (m *multipart) partContext(ctx context.Context, id string) (context.Context, func()) {
	m.mu.Lock()
	g := m.parts[id]
	if g == nil {
		g = &partGroup{done: make(chan struct{})}
		g.ctx, g.cancel = context.WithCancel(context.Background())
		m.parts[id] = g
	}
	g.active++
	m.mu.Unlock()
	ctx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(g.ctx, cancel)
	return ctx, func() {
		stop()
		cancel()
		m.mu.Lock()
		defer m.mu.Unlock()
		g.active--
		if g.active == 0 {
			g.cancel()
			delete(m.parts, id)
			close(g.done)
		}
	}
}

func (m *multipart) drain(ctx context.Context, id string) error {
	m.mu.Lock()
	g := m.parts[id]
	if g != nil {
		g.cancel()
	}
	m.mu.Unlock()
	if g != nil {
		select {
		case <-g.done:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func (m *multipart) release(ctx context.Context, id, owner string, owners map[string]string) error {
	if id == "" && owners != nil {
		id = owners[owner]
		if id == "" {
			return nil
		}
	}
	if id == "" {
		// A crash between Create and recording its ID leaves the durable owner
		// on the input itself. Only this attempt can claim that name.
		records, err := m.Inputs.List(ctx)
		if err != nil {
			return err
		}
		for _, r := range records {
			if r.Owner == owner {
				id = r.ID
				break
			}
		}
		if id == "" {
			return nil
		}
	}
	r, err := m.Inputs.Status(ctx, id)
	if errors.Is(err, input.ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	if r.Owner != owner {
		return errors.New("multipart input ownership mismatch")
	}
	if !r.Released {
		if _, err = m.Inputs.Release(ctx, id); err != nil {
			return err
		}
	}
	if r.Sealed {
		if err = m.Inputs.Unhold(ctx, id, owner); err != nil && !errors.Is(err, input.ErrNotFound) {
			return err
		}
	}
	_, err = m.Inputs.Status(ctx, id)
	if errors.Is(err, input.ErrNotFound) {
		return nil
	}
	if err == nil {
		return errors.New("multipart input still retained")
	}
	return err
}

// discard acts only on a durable pending entry. A failed/uncertain install
// cannot cause a candidate that became current to be discarded.
func (m *multipart) discard(ctx context.Context, upload, owner string, owners map[string]string) error {
	var id string
	var found bool
	err := m.DB.View(func(tx *bolt.Tx) error {
		b, _, err := readUpload(tx, upload)
		if err != nil {
			return err
		}
		v := b.Bucket(pendingTable).Get([]byte(owner))
		id, found = string(v), v != nil
		if id == unlinkedInput {
			id = ""
		}
		return nil
	})
	if err != nil || !found {
		return err
	}
	if err := m.release(ctx, id, owner, owners); err != nil {
		return err
	}
	return m.DB.Update(func(tx *bolt.Tx) error {
		b, _, err := readUpload(tx, upload)
		if err != nil {
			return err
		}
		pending := b.Bucket(pendingTable)
		if pending.Get([]byte(owner)) == nil {
			return nil
		}
		if err := pending.Delete([]byte(owner)); err != nil {
			return err
		}
		return addCount(tx.Bucket(manifestRoot), pendingCount, -1)
	})
}

func (m *multipart) clean(ctx context.Context, upload string, terminal bool, owners map[string]string) error {
	type retained struct {
		key  string
		part partRecord
	}
	var pending []string
	var parts []retained
	if err := m.DB.View(func(tx *bolt.Tx) error {
		b, _, err := readUpload(tx, upload)
		if err != nil {
			return err
		}
		err = b.Bucket(pendingTable).ForEach(func(k, _ []byte) error {
			pending = append(pending, string(k))
			return nil
		})
		if err != nil || !terminal {
			return err
		}
		return b.Bucket(partTable).ForEach(func(k, v []byte) error {
			var p partRecord
			if err := json.Unmarshal(v, &p); err != nil {
				return err
			}
			parts = append(parts, retained{key: string(k), part: p})
			return nil
		})
	}); err != nil {
		return err
	}
	var result error
	for _, owner := range pending {
		if err := ctx.Err(); err != nil {
			return errors.Join(result, err)
		}
		result = errors.Join(result, m.discard(ctx, upload, owner, owners))
	}
	var released []string
	for _, entry := range parts {
		if ctx.Err() != nil {
			result = errors.Join(result, ctx.Err())
			break
		}
		if err := m.release(ctx, entry.part.ID, entry.part.Owner, owners); err != nil {
			result = errors.Join(result, err)
		} else {
			released = append(released, entry.key)
		}
	}
	if len(released) > 0 {
		result = errors.Join(result, m.DB.Update(func(tx *bolt.Tx) error {
			b, _, err := readUpload(tx, upload)
			if err != nil {
				return err
			}
			for _, k := range released {
				if err := b.Bucket(partTable).Delete([]byte(k)); err != nil {
					return err
				}
			}
			return nil
		}))
	}
	return result
}

type unresolvedUpload struct {
	id     string
	change core.Change
}

func (m *multipart) recover(ctx context.Context) (unresolved []unresolvedUpload, result error) {
	var uploads []string
	if err := m.DB.View(func(tx *bolt.Tx) error {
		return tx.Bucket(manifestRoot).Bucket(uploadTable).ForEach(func(k, v []byte) error {
			if v == nil {
				uploads = append(uploads, string(k))
			}
			return nil
		})
	}); err != nil {
		return nil, err
	}
	records, err := m.Inputs.List(ctx)
	if err != nil {
		return nil, err
	}
	owners := make(map[string]string)
	for _, record := range records {
		owners[record.Owner] = record.ID
	}
	for _, id := range uploads {
		if err := ctx.Err(); err != nil {
			return unresolved, errors.Join(result, err)
		}
		var r uploadRecord
		now := time.Now()
		err := m.DB.Update(func(tx *bolt.Tx) error {
			b, record, err := readUpload(tx, id)
			if err != nil {
				return err
			}
			r = record
			legacy := r.LegacyFrozen != nil
			if legacy {
				if err := putRecord(b, uploadFrozen, r.LegacyFrozen); err != nil {
					return err
				}
				r.LegacyFrozen = nil
			}
			switch {
			case r.Phase == committing:
				r.Phase = unknown
				r.Change.Effect = core.Unknown
			case r.Phase == receiving && !now.Before(r.Expires):
				r.Phase = aborted
				r.Expires = now.Add(m.Retention)
			default:
				if !legacy {
					return nil
				}
			}
			return putRecord(b, uploadMeta, r)
		})
		if err != nil {
			result = errors.Join(result, err)
			continue
		}
		if r.Phase == unknown {
			unresolved = append(unresolved, unresolvedUpload{id: id, change: r.Change})
		}
		terminal := r.Phase == completed || r.Phase == aborted
		if err := m.clean(ctx, id, terminal, owners); err != nil {
			result = errors.Join(result, err)
			continue
		}
		if terminal && !now.Before(r.Expires) {
			result = errors.Join(result, m.DB.Update(func(tx *bolt.Tx) error {
				root := tx.Bucket(manifestRoot)
				if err := root.Bucket(uploadTable).DeleteBucket([]byte(id)); err != nil {
					return err
				}
				return addCount(root, uploadCount, -1)
			}))
		}
	}
	return unresolved, result
}
