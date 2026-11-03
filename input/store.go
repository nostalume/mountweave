// Package input owns durable sequential input independently of file targets,
// Drivers and task execution. The host exclusively owns the private directory.
package input

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"sort"
	"sync"
	"unicode/utf8"
)

var (
	ErrInvalid   = errors.New("invalid input request")
	ErrNotFound  = errors.New("input not found")
	ErrConflict  = errors.New("input offset or content conflict")
	ErrState     = errors.New("input state does not permit this operation")
	ErrBusy      = errors.New("input mutation already active")
	ErrCapacity  = errors.New("input storage capacity reached")
	ErrClosed    = errors.New("input store is closed")
	ErrCorrupt   = errors.New("input journal is corrupt")
	ErrUncertain = errors.New("input requires storage reconciliation")
)

// Options bounds retained logical journal bytes (including metadata and reserved
// seal/release records), input/block counts and each append. It does not measure disk
// allocation blocks or free space. All limits are host-selected and positive.
type Options struct {
	MaxBytes       int64
	MaxInputs      int
	MaxAppendBytes int64
	MaxBlocks      int
}

type Record struct {
	ID, Owner                 string
	Offset                    int64
	Sealed, Released, Blocked bool
	Version                   string
}

type identity struct{ ID, Owner string }

type entry struct {
	op          sync.Mutex
	record      Record
	end, charge int64
	sum         [32]byte
	chunks      []chunk
	holds       map[string]struct{}
	refs        int
	failed      error
}

type Store struct {
	mu                 sync.Mutex
	root               *os.Root
	opts               Options
	items              map[string]*entry
	used               int64
	creating, users    int
	blocks             int
	closed, rootClosed bool
	life               context.Context
	cancel             context.CancelFunc
	changed            chan struct{}
	closeErr           error
	failedFiles        []*os.File
}

// Open recovers one exclusively owned private directory. It verifies committed
// records and truncates incomplete tails; checksum corruption is never discarded.
// There is no cross-process lock. File.Sync precedes acknowledgements; physical
// power-loss guarantees, directory persistence and backups depend on the host FS.
func Open(ctx context.Context, directory string, opts Options) (_ *Store, err error) {
	if opts.MaxBytes <= 2*frameSize || opts.MaxInputs <= 0 || opts.MaxBlocks <= 0 || opts.MaxAppendBytes <= 0 || opts.MaxAppendBytes > opts.MaxBytes-frameSize {
		return nil, ErrInvalid
	}
	root, err := os.OpenRoot(directory)
	if err != nil {
		return nil, err
	}
	life, cancel := context.WithCancel(context.Background())
	s := &Store{root: root, opts: opts, items: make(map[string]*entry), life: life, cancel: cancel, changed: make(chan struct{})}
	defer func() {
		if err != nil {
			cancel()
			err = errors.Join(err, root.Close())
		}
	}()
	dir, err := root.Open(".")
	if err != nil {
		return nil, err
	}
	defer func() { err = errors.Join(err, dir.Close()) }()
	for {
		if err = ctx.Err(); err != nil {
			return nil, err
		}
		batch, end := dir.ReadDir(128)
		if end != nil && end != io.EOF {
			return nil, end
		}
		for _, item := range batch {
			if !validID(item.Name()) || !item.Type().IsRegular() {
				return nil, ErrCorrupt
			}
			e, readErr := s.recover(ctx, item.Name())
			if readErr != nil {
				return nil, readErr
			}
			if e == nil {
				continue
			}
			if len(s.items) >= opts.MaxInputs {
				return nil, ErrCapacity
			}
			if e.charge > opts.MaxBytes-s.used {
				return nil, ErrCapacity
			}
			s.items[e.record.ID], s.used = e, s.used+e.charge
			s.blocks += len(e.chunks)
		}
		if end == io.EOF {
			break
		}
	}
	return s, nil
}

func validID(id string) bool {
	if len(id) != 32 {
		return false
	}
	for _, c := range id {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}

func (s *Store) start(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	s.users++
	return nil
}
func (s *Store) endUser() {
	s.mu.Lock()
	s.users--
	close(s.changed)
	s.changed = make(chan struct{})
	s.mu.Unlock()
}
func (s *Store) snapshot(e *entry) Record {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := e.record
	r.Blocked = e.failed != nil
	return r
}

func (s *Store) operation(ctx context.Context, id string) (*entry, func() error, error) {
	if !validID(id) {
		return nil, nil, ErrInvalid
	}
	if err := s.start(ctx); err != nil {
		return nil, nil, err
	}
	s.mu.Lock()
	e := s.items[id]
	var err error
	if e == nil {
		err = ErrNotFound
	} else if !e.op.TryLock() {
		err = ErrBusy
	}
	s.mu.Unlock()
	if err != nil {
		s.endUser()
		return nil, nil, err
	}
	return e, func() error { e.op.Unlock(); err := s.reap(e); s.endUser(); return err }, nil
}

// Create publishes an empty receiving input. Owner is opaque metadata; the host
// authorizes access. Unacknowledged creations can be found with List.
func (s *Store) Create(ctx context.Context, owner string) (record Record, err error) {
	if owner == "" || !utf8.ValidString(owner) {
		return record, ErrInvalid
	}
	if err = s.start(ctx); err != nil {
		return
	}
	defer s.endUser()
	var random [16]byte
	if _, err = rand.Read(random[:]); err != nil {
		return
	}
	id := hex.EncodeToString(random[:])
	header, _ := json.Marshal(identity{ID: id, Owner: owner})
	cost := int64(len(magic)+len(header)) + 3*frameSize
	s.mu.Lock()
	if len(s.items)+s.creating >= s.opts.MaxInputs || cost > s.opts.MaxBytes-s.used {
		s.mu.Unlock()
		return record, ErrCapacity
	}
	s.creating++
	s.used += cost
	s.mu.Unlock()
	e := &entry{record: Record{ID: id, Owner: owner}, charge: cost}
	f, openErr := s.root.OpenFile(id, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0600)
	if openErr != nil {
		s.mu.Lock()
		s.creating--
		s.used -= cost
		s.mu.Unlock()
		return record, openErr
	}
	_, err = f.Write([]byte(magic))
	var receipt frame
	if err == nil {
		receipt, err = writeFrame(ctx, s.life, f, initialSum(), kindCreate, 0, int64(len(header)), bytes.NewReader(header))
		e.sum, e.end = receipt.sum, receipt.size
	}
	if err == nil {
		err = f.Sync()
	}
	err = errors.Join(err, s.closeFile(f, e))
	removeErr := e.failed
	if err != nil && removeErr == nil {
		// Compensate only this private identity, without blocking other inputs.
		removeErr = s.root.Remove(id)
		err = errors.Join(err, removeErr)
	}
	s.mu.Lock()
	s.creating--
	if err == nil {
		e.end += int64(len(magic))
		s.items[id] = e
	} else {
		if removeErr == nil {
			s.used -= cost
		} else {
			e.failed = err
			s.items[id] = e
		}
	}
	s.mu.Unlock()
	if err == nil {
		record = s.snapshot(e)
		err = check(ctx, s.life)
	}
	return
}

func (s *Store) Status(ctx context.Context, id string) (Record, error) {
	if !validID(id) {
		return Record{}, ErrInvalid
	}
	if err := s.start(ctx); err != nil {
		return Record{}, err
	}
	defer s.endUser()
	s.mu.Lock()
	defer s.mu.Unlock()
	e := s.items[id]
	if e == nil {
		return Record{}, ErrNotFound
	}
	r := e.record
	r.Blocked = e.failed != nil
	if e.failed != nil {
		return r, errors.Join(ErrUncertain, e.failed)
	}
	return r, nil
}

// List is a bounded owner snapshot for host retention/recovery policy. No expiry
// or task history is inferred from it.
func (s *Store) List(ctx context.Context) ([]Record, error) {
	if err := s.start(ctx); err != nil {
		return nil, err
	}
	defer s.endUser()
	s.mu.Lock()
	defer s.mu.Unlock()
	rows := make([]Record, 0, len(s.items))
	for _, e := range s.items {
		r := e.record
		r.Blocked = e.failed != nil
		rows = append(rows, r)
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].ID < rows[j].ID })
	return rows, nil
}

// Seal fixes one immutable input version at the expected confirmed offset.
func (s *Store) Seal(ctx context.Context, id string, offset int64) (Record, error) {
	return s.transition(ctx, id, kindSeal, offset)
}

// Release durably retires admission. Existing references may still open/read the
// sealed input. Bytes remain charged until the final reference and body close.
func (s *Store) Release(ctx context.Context, id string) (Record, error) {
	return s.transition(ctx, id, kindRelease, -1)
}

func (s *Store) transition(ctx context.Context, id string, kind byte, offset int64) (record Record, err error) {
	e, finish, err := s.operation(ctx, id)
	if err != nil {
		return record, err
	}
	defer func() { err = errors.Join(err, finish()); record = s.snapshot(e) }()
	s.mu.Lock()
	failed := e.failed
	current := e.record
	s.mu.Unlock()
	if failed != nil {
		return record, errors.Join(ErrUncertain, failed)
	}
	if kind == kindSeal && offset != current.Offset {
		return record, ErrConflict
	}
	if current.Released {
		if kind == kindRelease {
			return current, nil
		}
		return record, ErrState
	}
	if kind == kindSeal && current.Sealed {
		return current, nil
	}
	f, err := s.root.OpenFile(id, os.O_RDWR, 0)
	if err != nil {
		return record, err
	}
	defer func() { err = errors.Join(err, s.closeFile(f, e)) }()
	if _, err = f.Seek(e.end, io.SeekStart); err != nil {
		return record, err
	}
	receipt, err := writeFrame(ctx, s.life, f, e.sum, kind, current.Offset, 0, bytes.NewReader(nil))
	if err != nil {
		return record, s.rollback(f, e, 0, err)
	}
	if err = f.Sync(); err != nil {
		s.block(e, err)
		return record, errors.Join(ErrUncertain, err)
	}
	s.mu.Lock()
	e.end += receipt.size
	e.sum = receipt.sum
	if kind == kindSeal {
		e.record.Sealed = true
		e.record.Version = hex.EncodeToString(receipt.sum[:])
	} else {
		e.record.Released = true
	}
	s.mu.Unlock()
	return record, check(ctx, s.life)
}

// Limits returns the immutable host-selected admission limits.
func (s *Store) Limits() Options { return s.opts }

func (s *Store) block(e *entry, err error) {
	s.mu.Lock()
	e.failed = errors.Join(e.failed, err)
	s.mu.Unlock()
}
func (s *Store) closeFile(f *os.File, e *entry) error {
	err := f.Close()
	if err != nil {
		s.mu.Lock()
		e.failed = errors.Join(e.failed, err)
		s.closeErr = errors.Join(s.closeErr, err)
		s.failedFiles = append(s.failedFiles, f)
		s.mu.Unlock()
	}
	return err
}
func (s *Store) reap(e *entry) error {
	if !e.op.TryLock() {
		return nil
	}
	defer e.op.Unlock()
	s.mu.Lock()
	ready := s.items[e.record.ID] == e && e.record.Released && e.refs == 0 && len(e.holds) == 0 && e.failed == nil
	s.mu.Unlock()
	if !ready {
		return nil
	}
	err := s.root.Remove(e.record.ID)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		s.block(e, err)
		s.mu.Lock()
		s.closeErr = errors.Join(s.closeErr, err)
		s.mu.Unlock()
		return err
	}
	s.mu.Lock()
	delete(s.items, e.record.ID)
	s.used -= e.charge
	s.blocks -= len(e.chunks)
	e.chunks = nil
	s.mu.Unlock()
	return nil
}

// Close stops admission and requests active input reads to stop. It waits for
// actual operations, references and bodies; the context limits waiting only.
// Persisted inputs are retained. Close errors keep failed cleanup owned here.
func (s *Store) Close(ctx context.Context) error {
	s.mu.Lock()
	s.closed = true
	s.cancel()
	for s.users != 0 {
		changed := s.changed
		s.mu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
			return ctx.Err()
		}
		s.mu.Lock()
	}
	defer s.mu.Unlock()
	if !s.rootClosed {
		s.closeErr = errors.Join(s.closeErr, s.root.Close())
		s.rootClosed = true
		for _, e := range s.items {
			e.chunks = nil
			e.holds = nil
		}
		s.blocks = 0
	}
	return s.closeErr
}
