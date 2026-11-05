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

func (d *Driver) Open(ctx context.Context, pAddress core.NativeAddress, options core.OpenOptions) (core.Opened, error) {
	if options.Mode == core.LiveEdit {
		return core.Opened{Change: core.Change{Effect: core.Unchanged}}, core.Fail(core.UnsupportedOperation, nil)
	}
	if err := options.Validate(); err != nil {
		return core.Opened{}, err
	}
	if pAddress.Namespace() != core.TreeNamespace {
		return core.Opened{}, core.Fail(core.UnsupportedOperation, nil)
	}
	p := pAddress.Value()

	if err := core.ValidatePath(p); err != nil {
		return core.Opened{}, err
	}
	return d.open(ctx, path.Base(p), options, func(tx *bolt.Tx) (entryRecord, error) { return lookup(tx, p) })
}

func (d *Driver) open(ctx context.Context, name string, options core.OpenOptions, resolve func(*bolt.Tx) (entryRecord, error)) (core.Opened, error) {
	wanted, selection, bound := options.Range, options.Selection, options.Mode == core.BoundRead
	if wanted != nil && !wanted.Valid() {
		return core.Opened{}, core.Fail(core.InvalidRange, nil)
	}
	lifetime := ctx
	if bound {
		lifetime = context.WithoutCancel(ctx)
	}
	o, err := d.begin(lifetime)
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
		if err == nil {
			err = selection.Check(metadata(name, e, f).Evidence)
		}
		return err
	})
	if err == nil && (!core.ValidName(f.ID) || f.Length < 0 || f.Stripes != stripeCount(f.Length, int64(d.layout.Data)*int64(d.layout.ShardBytes))) {
		err = core.Fail(core.InvalidUpstreamResponse, nil)
	}
	if err == nil {
		err = ctx.Err()
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
	if bound {
		return core.Opened{Metadata: metadata(name, entryRecord{}, f), File: &boundFile{body: b, name: name}, Binding: core.PinnedVersion}, nil
	}
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

type boundFile struct {
	*body
	name string
}

func (f *boundFile) Stat(ctx context.Context) (core.Metadata, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return core.Metadata{}, err
	}
	if f.closed {
		return core.Metadata{}, io.ErrClosedPipe
	}
	return metadata(f.name, entryRecord{}, f.file), f.op.ctx.Err()
}

func (f *boundFile) ReadAt(ctx context.Context, p []byte, offset int64) (n int, err error) {
	if offset < 0 {
		return 0, core.Fail(core.InvalidRange, nil)
	}
	call, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(f.op.ctx, cancel)
	defer func() { stop(); cancel() }()
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.op.ctx.Err() != nil {
		cancel()
	}
	f.position, f.end, f.terminal = offset, f.file.Length-1, nil
	// Reuse the stripe decoder, but do not retain scarce window credits while
	// a persistent File is idle or at EOF. Its generation pin remains held.
	defer func() {
		if f.reserved {
			f.op.d.release()
			f.reserved = false
		}
		f.buffers, f.shards = nil, nil
		f.loaded = -1
	}()
	for n < len(p) {
		var count int
		count, err = f.read(call, p[n:])
		n += count
		if err != nil {
			return n, err
		}
	}
	return n, call.Err()
}
func (b *body) Read(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.read(b.op.ctx, p)
}

func (b *body) read(ctx context.Context, p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if err := ctx.Err(); err != nil {
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
		if err := d.reserve(ctx); err != nil {
			return 0, err
		}
		b.reserved = true
		b.buffers = d.buffers()
		b.shards = make([][]byte, len(b.buffers))
	}
	stripeBytes := int64(d.layout.Data) * int64(d.layout.ShardBytes)
	index := b.position / stripeBytes
	if b.loaded != index {
		if err := b.load(ctx, index); err != nil {
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

func (b *body) load(ctx context.Context, index int64) error {
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
		if err := b.fetch(ctx, s.Shards[i], b.buffers[i]); err != nil {
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
		return ctx.Err()
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
		return core.Fail(core.UpstreamFailure, errors.Join(errors.New("insufficient verified shards"), errors.Join(failures...), ctx.Err()))
	}
	if err := d.codec.ReconstructData(b.shards); err != nil {
		return core.Fail(core.InvalidUpstreamResponse, err)
	}
	for i := first; i <= last; i++ {
		if sha256.Sum256(b.shards[i]) != s.Shards[i].Hash {
			return core.Fail(core.InvalidUpstreamResponse, errors.New("reconstructed data checksum mismatch"))
		}
	}
	return ctx.Err()
}

func (b *body) fetch(ctx context.Context, shard shardRecord, output []byte) error {
	d := b.op.d
	u, err := d.borrowMember(ctx, shard.Member)
	if err != nil {
		return err
	}
	defer u.finish()
	opened, openErr := u.native.Open(u.ctx, core.NativePath(shard.Path), core.OpenOptions{})
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
