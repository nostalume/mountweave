package input

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"sort"
)

const (
	magic            = "MWINPUT1"
	fieldSize        = 17
	headSize         = fieldSize + sha256.Size
	frameSize        = int64(headSize + 2*sha256.Size)
	kindCreate  byte = 1
	kindData    byte = 2
	kindSeal    byte = 3
	kindRelease byte = 4
	kindHold    byte = 5
	kindUnhold  byte = 6
)

type chunk struct {
	offset, length, position int64
	sum                      [32]byte
}
type frame struct {
	sum, payload [32]byte
	size         int64
}

func initialSum() [32]byte { return sha256.Sum256([]byte(magic)) }
func frameHeader(kind byte, offset, length int64) []byte {
	b := make([]byte, headSize)
	b[0] = kind
	binary.LittleEndian.PutUint64(b[1:9], uint64(offset))
	binary.LittleEndian.PutUint64(b[9:17], uint64(length))
	sum := sha256.Sum256(b[:fieldSize])
	copy(b[fieldSize:], sum[:])
	return b
}
func chain(prev [32]byte, head []byte, payload [32]byte) [32]byte {
	h := sha256.New()
	h.Write(prev[:])
	h.Write(head)
	h.Write(payload[:])
	var sum [32]byte
	copy(sum[:], h.Sum(nil))
	return sum
}
func check(ctx, life context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return life.Err()
}

// copyExact borrows input directly. Final-byte errors and excess data reject the
// append, even when its declared byte count has already been received.
func copyExact(ctx, life context.Context, input io.Reader, length int64, output io.Writer) error {
	buffer := make([]byte, min(length+1, 32<<10))
	remaining, empty := length, 0
	for {
		if err := check(ctx, life); err != nil {
			return err
		}
		next := buffer[:min(int64(len(buffer)), max(remaining, 1))]
		n, err := input.Read(next)
		if stopped := check(ctx, life); stopped != nil {
			if err == io.EOF {
				return stopped
			}
			return errors.Join(stopped, err)
		}
		if n < 0 || n > len(next) {
			return ErrInvalid
		}
		if int64(n) > remaining {
			return ErrInvalid
		}
		if n > 0 {
			if _, writeErr := output.Write(next[:n]); writeErr != nil {
				return writeErr
			}
			remaining -= int64(n)
			empty = 0
		} else {
			empty++
		}
		if err != nil {
			if err == io.EOF {
				if remaining != 0 {
					return errors.Join(ErrInvalid, io.ErrUnexpectedEOF)
				}
				return check(ctx, life)
			}
			if errors.Is(err, io.ErrUnexpectedEOF) {
				return errors.Join(ErrInvalid, err)
			}
			return err
		}
		if empty == 100 {
			return io.ErrNoProgress
		}
	}
}
func writeFrame(ctx, life context.Context, f *os.File, prev [32]byte, kind byte, offset, length int64, input io.Reader) (frame, error) {
	var result frame
	if err := check(ctx, life); err != nil {
		return result, err
	}
	head := frameHeader(kind, offset, length)
	if _, err := f.Write(head); err != nil {
		return result, err
	}
	h := sha256.New()
	if err := copyExact(ctx, life, input, length, io.MultiWriter(f, h)); err != nil {
		return result, err
	}
	copy(result.payload[:], h.Sum(nil))
	result.sum = chain(prev, head, result.payload)
	result.size = frameSize + length
	footer := append(result.payload[:], result.sum[:]...)
	_, err := f.Write(footer)
	return result, err
}

// Append commits one positive-length block, or verifies an exact retry of a
// committed block. Rechunked overlaps and gaps are conflicts. The input is never
// closed/replayed and a busy input refuses before consuming it.
func (s *Store) Append(ctx context.Context, id string, offset int64, input io.Reader, length int64) (record Record, err error) {
	if offset < 0 || length <= 0 || length > s.opts.MaxAppendBytes || input == nil {
		return record, ErrInvalid
	}
	e, finish, err := s.operation(ctx, id)
	if err != nil {
		return record, err
	}
	defer func() { err = errors.Join(err, finish()); record = s.snapshot(e) }()
	s.mu.Lock()
	current, failed := e.record, e.failed
	s.mu.Unlock()
	if failed != nil {
		return record, errors.Join(ErrUncertain, failed)
	}
	if current.Released {
		return record, ErrState
	}
	if offset != current.Offset {
		i := sort.Search(len(e.chunks), func(i int) bool { return e.chunks[i].offset >= offset })
		if i == len(e.chunks) || e.chunks[i].offset != offset || e.chunks[i].length != length {
			return record, ErrConflict
		}
		h := sha256.New()
		if err = copyExact(ctx, s.life, input, length, h); err != nil {
			return record, err
		}
		var sum [32]byte
		copy(sum[:], h.Sum(nil))
		if sum != e.chunks[i].sum {
			return record, ErrConflict
		}
		return record, check(ctx, s.life)
	}
	if current.Sealed {
		return record, ErrState
	}
	cost := frameSize + length
	s.mu.Lock()
	if cost > s.opts.MaxBytes-s.used || s.blocks >= s.opts.MaxBlocks {
		s.mu.Unlock()
		return record, ErrCapacity
	}
	s.used += cost
	e.charge += cost
	s.blocks++
	s.mu.Unlock()
	f, err := s.root.OpenFile(id, os.O_RDWR, 0)
	if err != nil {
		s.refund(e, cost)
		return record, err
	}
	defer func() { err = errors.Join(err, s.closeFile(f, e)) }()
	if _, err = f.Seek(e.end, io.SeekStart); err != nil {
		s.refund(e, cost)
		return record, err
	}
	receipt, err := writeFrame(ctx, s.life, f, e.sum, kindData, offset, length, input)
	if err != nil {
		return record, s.rollback(f, e, cost, err)
	}
	if err = f.Sync(); err != nil {
		s.block(e, err)
		return record, errors.Join(ErrUncertain, err)
	}
	s.mu.Lock()
	e.chunks = append(e.chunks, chunk{offset: offset, length: length, position: e.end + headSize, sum: receipt.payload})
	e.end += receipt.size
	e.sum = receipt.sum
	e.record.Offset += length
	s.mu.Unlock()
	return record, check(ctx, s.life)
}
func (s *Store) refund(e *entry, n int64) {
	s.mu.Lock()
	s.used -= n
	e.charge -= n
	if n > 0 {
		s.blocks--
	}
	s.mu.Unlock()
}
func (s *Store) rollback(f *os.File, e *entry, cost int64, cause error) error {
	err := f.Truncate(e.end)
	if err == nil {
		err = f.Sync()
	}
	if err != nil {
		s.block(e, err)
		return errors.Join(cause, ErrUncertain, err)
	}
	s.refund(e, cost)
	return cause
}

// Recovery verifies the metadata checksum chain without re-reading all payload.
// Each payload digest is verified when a sealed source is consumed.
func (s *Store) recover(ctx context.Context, id string) (result *entry, err error) {
	f, err := s.root.OpenFile(id, os.O_RDWR, 0)
	if err != nil {
		return nil, err
	}
	closed := false
	defer func() {
		if !closed {
			err = errors.Join(err, f.Close())
		}
	}()
	discard := func() error {
		closed = true
		if err := f.Close(); err != nil {
			return err
		}
		return s.root.Remove(id)
	}
	stat, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if !stat.Mode().IsRegular() || stat.Size() > s.opts.MaxBytes {
		return nil, ErrCapacity
	}
	var prefix [len(magic)]byte
	if _, err = f.ReadAt(prefix[:], 0); err != nil && err != io.EOF {
		return nil, err
	}
	if stat.Size() < int64(len(magic)) {
		return nil, discard()
	}
	if string(prefix[:]) != magic {
		return nil, ErrCorrupt
	}
	e := &entry{end: int64(len(magic)), sum: initialSum()}
	var lastKind byte
	for e.end < stat.Size() {
		if err = ctx.Err(); err != nil {
			return nil, err
		}
		remaining := stat.Size() - e.end
		if remaining < frameSize {
			break
		}
		var head [headSize]byte
		if _, err = f.ReadAt(head[:], e.end); err != nil {
			return nil, err
		}
		headerSum := sha256.Sum256(head[:fieldSize])
		if !bytes.Equal(headerSum[:], head[fieldSize:]) {
			return nil, ErrCorrupt
		}
		kind := head[0]
		offset := int64(binary.LittleEndian.Uint64(head[1:9]))
		length := int64(binary.LittleEndian.Uint64(head[9:17]))
		if offset < 0 || length < 0 {
			return nil, ErrCorrupt
		}
		if length > remaining-frameSize {
			break
		}
		var footer [2 * sha256.Size]byte
		if _, err = f.ReadAt(footer[:], e.end+headSize+length); err != nil {
			return nil, err
		}
		var payload, committed [32]byte
		copy(payload[:], footer[:32])
		copy(committed[:], footer[32:])
		if chain(e.sum, head[:], payload) != committed {
			return nil, ErrCorrupt
		}
		if e.record.Released && kind != kindUnhold {
			return nil, ErrCorrupt
		}
		switch kind {
		case kindCreate:
			if e.record.ID != "" || offset != 0 {
				return nil, ErrCorrupt
			}
			data := make([]byte, length)
			if _, err = f.ReadAt(data, e.end+headSize); err != nil {
				return nil, err
			}
			var who identity
			if sha256.Sum256(data) != payload || json.Unmarshal(data, &who) != nil || who.ID != id || who.Owner == "" {
				return nil, ErrCorrupt
			}
			e.record.ID, e.record.Owner = who.ID, who.Owner
		case kindData:
			if e.record.ID == "" || e.record.Sealed || length == 0 || offset != e.record.Offset {
				return nil, ErrCorrupt
			}
			if len(e.chunks) >= s.opts.MaxBlocks-s.blocks {
				return nil, ErrCapacity
			}
			e.chunks = append(e.chunks, chunk{offset: offset, length: length, position: e.end + headSize, sum: payload})
			e.record.Offset += length
		case kindSeal, kindRelease:
			if e.record.ID == "" || length != 0 || offset != e.record.Offset || payload != sha256.Sum256(nil) {
				return nil, ErrCorrupt
			}
			if kind == kindSeal {
				if e.record.Sealed {
					return nil, ErrCorrupt
				}
				e.record.Sealed = true
				e.record.Version = hex.EncodeToString(committed[:])
			} else {
				e.record.Released = true
			}
		case kindHold, kindUnhold:
			if !e.record.Sealed || offset != e.record.Offset || length == 0 {
				return nil, ErrCorrupt
			}
			data := make([]byte, length)
			if _, err = f.ReadAt(data, e.end+headSize); err != nil {
				return nil, err
			}
			if sha256.Sum256(data) != payload {
				return nil, ErrCorrupt
			}
			key := string(data)
			_, exists := e.holds[key]
			if kind == kindHold {
				if exists {
					return nil, ErrCorrupt
				}
				if e.holds == nil {
					e.holds = make(map[string]struct{})
				}
				e.holds[key] = struct{}{}
			} else {
				if !exists {
					return nil, ErrCorrupt
				}
				delete(e.holds, key)
			}
		default:
			return nil, ErrCorrupt
		}
		e.end += frameSize + length
		e.sum = committed
		lastKind = kind
	}
	if e.record.ID == "" {
		return nil, discard()
	} // Incomplete, never acknowledged creation.
	// A writer stops on an uncertain sync. Only its last complete data frame
	// could precede an unacknowledged flush; older frames had confirmed barriers.
	if lastKind == kindData {
		c := e.chunks[len(e.chunks)-1]
		h := sha256.New()
		if err = copyExact(ctx, s.life, io.NewSectionReader(f, c.position, c.length), c.length, h); err != nil {
			return nil, errors.Join(ErrCorrupt, err)
		}
		var actual [32]byte
		copy(actual[:], h.Sum(nil))
		if actual != c.sum {
			return nil, ErrCorrupt
		}
	}
	if e.end != stat.Size() {
		if err = f.Truncate(e.end); err != nil {
			return nil, err
		}
	}
	// The final valid record might have survived before its original Sync/ack.
	if err = f.Sync(); err != nil {
		return nil, errors.Join(ErrUncertain, err)
	}
	if e.record.Released && len(e.holds) == 0 {
		return nil, discard()
	}
	reserve := frameSize
	if e.record.Released {
		reserve = 0
	}
	if !e.record.Sealed {
		reserve += frameSize
	}
	for key := range e.holds {
		cost := frameSize + int64(len(key))
		if cost > s.opts.MaxBytes-e.end-reserve {
			return nil, ErrCapacity
		}
		reserve += cost
	}
	if reserve > s.opts.MaxBytes-e.end {
		return nil, ErrCapacity
	}
	e.charge = e.end + reserve
	return e, nil
}
