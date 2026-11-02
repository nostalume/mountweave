package pool

import (
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"path"
	"sync"

	core "github.com/nostalume/mountweave"
	bolt "go.etcd.io/bbolt"
)

func (d *Driver) Open(ctx context.Context, p string, wanted *core.Range) (core.Opened, error) {
	if err := core.ValidatePath(p); err != nil {
		return core.Opened{}, err
	}
	return d.open(ctx, path.Base(p), wanted, func(tx *bolt.Tx) (entryRecord, error) { return lookup(tx, p) })
}

func (d *Driver) open(ctx context.Context, name string, wanted *core.Range, resolve func(*bolt.Tx) (entryRecord, error)) (core.Opened, error) {
	if wanted != nil && !wanted.Valid() {
		return core.Opened{}, core.Fail(core.InvalidRange, nil)
	}
	o, err := d.begin(ctx)
	if err != nil {
		return core.Opened{}, err
	}
	var f fileRecord
	d.state.Lock()
	err = d.db.View(func(tx *bolt.Tx) error {
		e, err := resolve(tx)
		if err != nil {
			return err
		}
		f, err = fileAt(tx, e)
		return err
	})
	if err == nil && (!core.ValidName(f.ID) || f.Length < 0 || f.Stripes != stripeCount(f.Length, int64(d.layout.Data)*int64(d.layout.ShardBytes))) {
		err = core.Fail(core.InvalidUpstreamResponse, nil)
	}
	if err == nil {
		d.pins[f.ID]++
	}
	d.state.Unlock()
	if err != nil {
		o.finish()
		return core.Opened{}, err
	}
	start, end, length := int64(0), f.Length-1, f.Length
	var actual *core.ContentRange
	if wanted != nil {
		r, err := wanted.Resolve(f.Length)
		if err != nil {
			d.unpin(f.ID)
			o.finish()
			return core.Opened{}, err
		}
		actual, start, end, length = &r, r.Start, r.End, r.End-r.Start+1
	}
	b := &body{op: o, file: f, position: start, end: end, loaded: -1}
	b.mu.Lock()
	b.stop = context.AfterFunc(o.ctx, func() { _ = b.Close() })
	b.mu.Unlock()
	return core.Opened{Metadata: metadata(name, entryRecord{}, f), Body: b, Length: &length, Range: actual}, nil
}

func (d *Driver) unpin(id string) {
	d.state.Lock()
	d.pins[id]--
	if d.pins[id] == 0 {
		delete(d.pins, id)
	}
	d.state.Unlock()
}

type body struct {
	mu                    sync.Mutex
	op                    *operation
	file                  fileRecord
	position, end, loaded int64
	buffers, shards       [][]byte
	reserved, closed      bool
	stop                  func() bool
	terminal              error
}

func (b *body) Close() error {
	b.op.cancel()
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.closed {
		b.closed = true
		b.stop()
		b.buffers, b.shards = nil, nil
		if b.reserved {
			b.op.d.release()
		}
		b.op.d.unpin(b.file.ID)
		b.op.finish()
	}
	return nil
}
func (b *body) Read(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(p) == 0 {
		return 0, nil
	}
	if err := b.op.ctx.Err(); err != nil {
		return 0, err
	}
	if b.closed {
		return 0, io.ErrClosedPipe
	}
	if b.terminal != nil {
		return 0, b.terminal
	}
	if b.position > b.end {
		return 0, io.EOF
	}
	d := b.op.d
	if !b.reserved {
		if err := d.reserve(b.op.ctx); err != nil {
			return 0, err
		}
		b.reserved = true
		b.buffers = d.buffers()
		b.shards = make([][]byte, len(b.buffers))
	}
	stripeBytes := int64(d.layout.Data) * int64(d.layout.ShardBytes)
	index := b.position / stripeBytes
	if b.loaded != index {
		if err := b.load(index); err != nil {
			b.terminal = err
			return 0, err
		}
		b.loaded = index
	}
	offset := b.position % stripeBytes
	slot, within := int(offset/int64(d.layout.ShardBytes)), int(offset%int64(d.layout.ShardBytes))
	n := copy(p, b.shards[slot][within:within+int(min(int64(d.layout.ShardBytes-within), b.end-b.position+1))])
	b.position += int64(n)
	return n, nil
}

func (b *body) load(index int64) error {
	d := b.op.d
	s, err := d.stripe(b.file, index)
	if err != nil {
		return err
	}
	for i := range b.shards {
		b.shards[i] = b.buffers[i][:0]
	}
	size := int64(d.layout.Data) * int64(d.layout.ShardBytes)
	first := int((b.position - index*size) / int64(d.layout.ShardBytes))
	last := int((min(b.end, index*size+int64(s.Bytes)-1) - index*size) / int64(d.layout.ShardBytes))
	attempted, failures := make([]bool, len(s.Shards)), make([]error, len(s.Shards))
	fetch := func(i int) {
		attempted[i] = true
		if err := b.fetch(s.Shards[i], b.buffers[i]); err != nil {
			failures[i] = err
		} else {
			b.shards[i] = b.buffers[i]
		}
	}
	parallel(last-first+1, d.opts.Concurrency, func(j int) { fetch(first + j) })
	for _, failure := range failures {
		var cleanup *cleanupFailure
		if errors.As(failure, &cleanup) {
			return failure
		}
	}
	missing := false
	for i := first; i <= last; i++ {
		missing = missing || len(b.shards[i]) == 0
	}
	if !missing {
		return b.op.ctx.Err()
	}
	valid := 0
	for _, shard := range b.shards {
		if len(shard) > 0 {
			valid++
		}
	}
	for i := range s.Shards {
		if valid >= d.layout.Data {
			break
		}
		if attempted[i] {
			continue
		}
		fetch(i)
		var cleanup *cleanupFailure
		if errors.As(failures[i], &cleanup) {
			return failures[i]
		}
		if len(b.shards[i]) > 0 {
			valid++
		}
	}
	if valid < d.layout.Data {
		return core.Fail(core.UpstreamFailure, errors.Join(errors.New("insufficient verified shards"), errors.Join(failures...), b.op.ctx.Err()))
	}
	if err := d.codec.ReconstructData(b.shards); err != nil {
		return core.Fail(core.InvalidUpstreamResponse, err)
	}
	for i := first; i <= last; i++ {
		if sha256.Sum256(b.shards[i]) != s.Shards[i].Hash {
			return core.Fail(core.InvalidUpstreamResponse, errors.New("reconstructed data checksum mismatch"))
		}
	}
	return b.op.ctx.Err()
}

func (b *body) fetch(shard shardRecord, output []byte) error {
	d := b.op.d
	u, err := d.borrowMember(b.op.ctx, shard.Member)
	if err != nil {
		return err
	}
	defer u.finish()
	opened, openErr := u.native.Open(u.ctx, shard.Path, nil)
	if opened.Body == nil {
		if openErr == nil {
			openErr = core.Fail(core.InvalidUpstreamResponse, nil)
		}
		return openErr
	}
	if openErr == nil && (opened.Metadata.Kind != core.File || opened.Range != nil ||
		(opened.Length != nil && *opened.Length != int64(len(output))) ||
		(opened.Metadata.Size != nil && *opened.Metadata.Size != int64(len(output)))) {
		openErr = core.Fail(core.InvalidUpstreamResponse, nil)
	}
	if openErr == nil {
		count, end := readStripe(u.ctx, opened.Body, [][]byte{output})
		if count != len(output) {
			openErr = errors.Join(io.ErrUnexpectedEOF, end)
		} else if end != nil && end != io.EOF {
			openErr = end
		}
		if openErr == nil && end != io.EOF {
			var extra [1]byte
			n, end := readStripe(u.ctx, opened.Body, [][]byte{extra[:]})
			if n != 0 || end != io.EOF {
				openErr = core.Fail(core.InvalidUpstreamResponse, end)
			}
		}
		if openErr == nil && sha256.Sum256(output) != shard.Hash {
			openErr = core.Fail(core.InvalidUpstreamResponse, errors.New("shard checksum mismatch"))
		}
	}
	closeErr := opened.Body.Close()
	u.cleanupErr = closeErr
	d.retain(closeErr, opened.Body)
	if closeErr != nil {
		closeErr = &cleanupFailure{closeErr}
	}
	return errors.Join(openErr, closeErr, u.ctx.Err())
}

type cleanupFailure struct{ error }

func (e *cleanupFailure) Unwrap() error { return e.error }
