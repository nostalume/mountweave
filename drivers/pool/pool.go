// Package pool stores logical files as verified erasure-coded stripes on private Drivers.
package pool

import (
	"context"
	"errors"
	"io"
	"math"
	"path/filepath"
	"sync"
	"time"

	"github.com/klauspost/reedsolomon"
	core "github.com/nostalume/mountweave"
	bolt "go.etcd.io/bbolt"
)

// Member identifies one private, exclusively owned storage root. Identity and
// FailureDomain are host assertions without secrets; aliases of one account/root
// must not claim independent capacity or failure domains. A nil Driver is offline.
type Member struct {
	ID, Identity, FailureDomain string
	Driver                      core.Driver
	// MaxBytes is an additional private-byte cap; zero adds no pool cap.
	MaxBytes int64
}

// Options has no inferred layout or memory allowance. MetadataPath must be a
// private local database outside member roots, on a filesystem supported by bbolt.
// LockTimeout bounds database-lock acquisition; zero waits without a timeout.
// Concurrency defaults to one and bounds member requests within each operation.
type Options struct {
	MetadataPath                         string
	LockTimeout                          time.Duration
	DataShards, ParityShards, ShardBytes int
	MaxBufferedBytes                     int64
	Concurrency                          int
	Members                              []Member
}

type Driver struct {
	opts                 Options
	db                   *bolt.DB
	codec                reedsolomon.Encoder
	members              map[string]*memberRuntime
	layout               layout
	life                 context.Context
	cancel               context.CancelFunc
	mu                   sync.Mutex
	closing              bool
	used                 int64
	writes               map[string]bool
	writeErr, cleanupErr error
	failed               []io.Closer
	wg                   sync.WaitGroup
	closeOnce            sync.Once
	closeErr             error
	state                sync.Mutex
	maintenance          sync.Mutex
	active               map[string]map[string]bool
	pins                 map[string]int
	gcCursor             scanCursor
	drainCursors         map[string]drainState
	repairCursor         scanCursor
}

// New acquires the metadata writer lock. Only success transfers non-nil member
// Drivers; on failure the caller still owns all of them. Reopening must preserve
// member identities and the stored layout. Missing runtime members remain offline.
func New(opts Options) (*Driver, error) {
	n := opts.DataShards + opts.ParityShards
	if opts.DataShards < 1 || opts.DataShards > 255 || opts.ParityShards < 1 || opts.ParityShards > 255 || n > 256 || n < 2 ||
		opts.ShardBytes < 1 || opts.ShardBytes > math.MaxInt/n ||
		opts.MaxBufferedBytes < int64(n)*int64(opts.ShardBytes) ||
		!filepath.IsAbs(opts.MetadataPath) || opts.LockTimeout < 0 || opts.Concurrency < 0 {
		return nil, core.Fail(core.InvalidRequest, errors.New("explicit layout, absolute metadata path, and at least one stripe buffer are required"))
	}
	if opts.Concurrency == 0 {
		opts.Concurrency = 1
	}
	opts.Members = append([]Member(nil), opts.Members...)
	d := &Driver{opts: opts, members: make(map[string]*memberRuntime), writes: make(map[string]bool), active: make(map[string]map[string]bool), pins: make(map[string]int), drainCursors: make(map[string]drainState)}
	identities := make(map[string]bool)
	for _, m := range opts.Members {
		if m.MaxBytes < 0 || !core.ValidName(m.ID) || len(m.ID) > 128 || m.Identity == "" || len(m.Identity) > 1024 ||
			m.FailureDomain == "" || len(m.FailureDomain) > 256 || identities[m.Identity] {
			return nil, core.Fail(core.InvalidRequest, errors.New("unique stable member identities and failure domains are required"))
		}
		if _, exists := d.members[m.ID]; exists {
			return nil, core.Fail(core.InvalidRequest, nil)
		}
		identities[m.Identity] = true
		d.members[m.ID] = newMemberRuntime(m.Driver)
	}
	var err error
	d.codec, err = reedsolomon.New(opts.DataShards, opts.ParityShards,
		reedsolomon.WithMaxGoroutines(1), reedsolomon.WithInversionCache(false))
	if err != nil {
		return nil, err
	}
	d.db, err = bolt.Open(opts.MetadataPath, 0600, &bolt.Options{Timeout: opts.LockTimeout})
	if err != nil {
		return nil, err
	}
	if err = d.initialize(); err != nil {
		return nil, errors.Join(err, d.db.Close())
	}
	d.life, d.cancel = context.WithCancel(context.Background())
	return d, nil
}

type operation struct {
	ctx    context.Context
	cancel context.CancelFunc
	stop   func() bool
	d      *Driver
	once   sync.Once
}

func (d *Driver) begin(ctx context.Context) (*operation, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closing {
		return nil, core.Fail(core.MountUnavailable, nil)
	}
	d.wg.Add(1)
	life, cancel := context.WithCancel(ctx)
	return &operation{ctx: life, cancel: cancel, stop: context.AfterFunc(d.life, cancel), d: d}, nil
}
func (o *operation) finish() { o.once.Do(func() { o.stop(); o.cancel(); o.d.wg.Done() }) }

func (d *Driver) reserve(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	n := int64(d.layout.Data+d.layout.Parity) * int64(d.layout.ShardBytes)
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.cleanupErr != nil {
		return core.Fail(core.UpstreamFailure, d.cleanupErr)
	}
	// Never wait while borrowing an arbitrary reader: it may need this same
	// pool's budget (Core.Copy is one example). Refusal preserves bounded work.
	if n > d.opts.MaxBufferedBytes-d.used {
		return core.Fail(core.CapacityExceeded, nil)
	}
	d.used += n
	return nil
}
func (d *Driver) release() {
	d.mu.Lock()
	d.used -= int64(d.layout.Data+d.layout.Parity) * int64(d.layout.ShardBytes)
	d.mu.Unlock()
}
func (d *Driver) buffers() [][]byte {
	n, size := d.layout.Data+d.layout.Parity, d.layout.ShardBytes
	data, shards := make([]byte, n*size), make([][]byte, n)
	for i := range shards {
		shards[i] = data[i*size : (i+1)*size : (i+1)*size]
	}
	return shards
}
func (d *Driver) retain(err error, body io.Closer) {
	if err == nil {
		return
	}
	d.mu.Lock()
	d.cleanupErr = errors.Join(d.cleanupErr, err)
	if body != nil {
		d.failed = append(d.failed, body)
	}
	d.mu.Unlock()
}

// Close is terminal: cancel, wait for actual I/O acknowledgement, then close owned
// members and the database. A failed native Close remains reported and owned.
func (d *Driver) Close() error {
	d.closeOnce.Do(func() {
		d.mu.Lock()
		d.closing = true
		d.cancel()
		d.mu.Unlock()
		d.wg.Wait()
		for _, m := range d.members {
			d.closeErr = errors.Join(d.closeErr, m.close())
		}
		d.closeErr = errors.Join(d.closeErr, d.db.Close(), d.cleanupErr)
	})
	return d.closeErr
}

// Backup writes a consistent metadata snapshot, including member mappings and
// unfinished intents. It does not back up shards or credentials. The host owns
// the writer and must durably save its bytes; a blocked Write is not interrupted.
func (d *Driver) Backup(ctx context.Context, output io.Writer) error {
	if output == nil {
		return core.Fail(core.InvalidRequest, nil)
	}
	o, err := d.begin(ctx)
	if err != nil {
		return err
	}
	defer o.finish()
	return d.db.View(func(tx *bolt.Tx) error { _, err := tx.WriteTo(contextWriter{o.ctx, output}); return err })
}

type contextWriter struct {
	ctx context.Context
	w   io.Writer
}

func (w contextWriter) Write(p []byte) (int, error) {
	if err := w.ctx.Err(); err != nil {
		return 0, err
	}
	return w.w.Write(p)
}

func parallel(count, limit int, run func(int)) {
	if limit <= 1 || count <= 1 {
		for i := 0; i < count; i++ {
			run(i)
		}
		return
	}
	jobs := make(chan int)
	var wg sync.WaitGroup
	for i := 0; i < min(count, limit); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range jobs {
				run(j)
			}
		}()
	}
	for i := 0; i < count; i++ {
		jobs <- i
	}
	close(jobs)
	wg.Wait()
}

var _ core.Driver = (*Driver)(nil)
var _ core.FileCreator = (*Driver)(nil)
