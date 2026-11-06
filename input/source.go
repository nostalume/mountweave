package input

import (
	"context"
	"crypto/sha256"
	"errors"
	"hash"
	"io"
	"os"
	"sort"
	"sync"
)

// Reference pins one sealed version without opening a file or borrowing Core
// resources. Open uses the execution context, not the acquisition request context.
// Each returned body owns a separate pin and must also be closed.
type Reference struct {
	mu     sync.Mutex
	store  *Store
	entry  *entry
	record Record
	closed bool
	err    error
}

func (s *Store) Acquire(ctx context.Context, id string) (*Reference, error) {
	return s.acquire(ctx, id, "")
}

// AcquireHeld also admits a retired input retained by this durable owner key.
func (s *Store) AcquireHeld(ctx context.Context, id, key string) (*Reference, error) {
	if key == "" {
		return nil, ErrInvalid
	}
	return s.acquire(ctx, id, key)
}
func (s *Store) acquire(ctx context.Context, id, key string) (*Reference, error) {
	if !validID(id) {
		return nil, ErrInvalid
	}
	if err := s.start(ctx); err != nil {
		return nil, err
	}
	s.mu.Lock()
	e := s.items[id]
	var err error
	if e == nil {
		err = ErrNotFound
	} else if e.failed != nil {
		err = errors.Join(ErrUncertain, e.failed)
	} else if _, held := e.holds[key]; !e.record.Sealed || (key != "" && !held) || (e.record.Released && !held) {
		err = ErrState
	}
	if err != nil {
		s.mu.Unlock()
		s.endUser()
		return nil, err
	}
	e.refs++
	r := &Reference{store: s, entry: e, record: e.record}
	s.mu.Unlock()
	return r, nil
}

func (r *Reference) Record() Record { return r.record }

func (r *Reference) Open(ctx context.Context) (io.ReadCloser, error) {
	return r.OpenRange(ctx, 0, r.record.Offset)
}

// OpenRange reads and verifies a range of this sealed version through EOF.
// The body owns its pin and must be closed, even after the reference is closed.
func (r *Reference) OpenRange(ctx context.Context, offset, length int64) (io.ReadCloser, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil, ErrClosed
	}
	if offset < 0 || length < 0 || offset > r.record.Offset || length > r.record.Offset-offset {
		return nil, ErrInvalid
	}
	s := r.store
	if err := s.start(ctx); err != nil {
		return nil, err
	}
	s.mu.Lock()
	r.entry.refs++
	s.mu.Unlock()
	f, err := s.root.Open(r.record.ID)
	if err != nil {
		return nil, errors.Join(ErrCorrupt, err, s.drop(r.entry))
	}
	first := sort.Search(len(r.entry.chunks), func(i int) bool { c := r.entry.chunks[i]; return c.offset+c.length > offset })
	last := first
	end := offset + length
	if length > 0 {
		last = sort.Search(len(r.entry.chunks), func(i int) bool { return r.entry.chunks[i].offset >= end })
	}
	b := &body{file: f, store: s, entry: r.entry, ctx: ctx, hash: sha256.New(), index: first, limit: last, record: r.record}
	if length == 0 {
		return &rangeBody{body: b}, nil
	}
	prefix := offset - r.entry.chunks[first].offset
	if _, err = io.CopyN(io.Discard, b, prefix); err != nil {
		return nil, errors.Join(err, b.Close())
	}
	suffix := r.entry.chunks[last-1].offset + r.entry.chunks[last-1].length - end
	return &rangeBody{body: b, remaining: length, suffix: suffix}, nil
}
func (r *Reference) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.closed {
		r.closed = true
		r.err = r.store.drop(r.entry)
	}
	return r.err
}
func (s *Store) drop(e *entry) error {
	s.mu.Lock()
	e.refs--
	s.mu.Unlock()
	err := s.reap(e)
	s.endUser()
	return err
}

type body struct {
	mu       sync.Mutex
	file     *os.File
	store    *Store
	entry    *entry
	ctx      context.Context
	index    int
	position int64
	hash     hash.Hash
	closed   bool
	err      error
	limit    int
	record   Record
}

func (b *body) Read(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return 0, io.ErrClosedPipe
	}
	if err := check(b.ctx, b.store.life); err != nil {
		return 0, err
	}
	if len(p) == 0 {
		return 0, nil
	}
	if b.index == b.limit {
		return 0, io.EOF
	}
	c := b.entry.chunks[b.index]
	request := p[:min(int64(len(p)), c.length-b.position)]
	n, err := b.file.ReadAt(request, c.position+b.position)
	if n > 0 {
		b.hash.Write(request[:n])
		b.position += int64(n)
	}
	if err != nil || n != len(request) {
		return n, errors.Join(ErrCorrupt, err)
	}
	if b.position == c.length {
		var actual [32]byte
		copy(actual[:], b.hash.Sum(nil))
		if actual != c.sum {
			return n, ErrCorrupt
		}
		b.index++
		b.position = 0
		b.hash.Reset()
	}
	return n, nil
}

// rangeBody verifies both boundary blocks before returning normal EOF. It may
// read their prefixes/suffixes, but never scans preceding source blocks.
type rangeBody struct {
	*body
	remaining, suffix int64
}

func (b *rangeBody) SourceVersion() string { return b.record.Version }
func (b *rangeBody) OpenRange(ctx context.Context, offset, length int64) (io.ReadCloser, error) {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil, ErrClosed
	}
	// A temporary pin protects the immutable chunk index while opening the range.
	s := b.store
	if err := s.start(ctx); err != nil {
		b.mu.Unlock()
		return nil, err
	}
	s.mu.Lock()
	b.entry.refs++
	s.mu.Unlock()
	r := &Reference{store: s, entry: b.entry, record: b.record}
	b.mu.Unlock()
	opened, err := r.OpenRange(ctx, offset, length)
	return opened, errors.Join(err, r.Close())
}
func (b *rangeBody) Read(p []byte) (int, error) {
	// Reader calls, like other io.Readers, must be serialized by their consumer.
	if len(p) == 0 {
		return b.body.Read(p)
	}
	if b.remaining > 0 {
		n, err := b.body.Read(p[:min(int64(len(p)), b.remaining)])
		b.remaining -= int64(n)
		return n, err
	}
	if b.suffix > 0 {
		n, err := io.CopyN(io.Discard, b.body, b.suffix)
		b.suffix -= n
		if err != nil {
			return 0, err
		}
	}
	return b.body.Read(p[:1])
}
func (b *body) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.closed {
		b.closed = true
		b.err = b.store.closeFile(b.file, b.entry)
		b.err = errors.Join(b.err, b.store.drop(b.entry))
	}
	return b.err
}
