package pool

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/local"
	"github.com/nostalume/mountweave/task"
)

type gatedMember struct {
	*memberFixture
	entered, closing, release chan struct{}
	reads, closes             atomic.Int32
}

func (m *gatedMember) Open(ctx context.Context, p string, wanted *core.Range) (core.Opened, error) {
	o, err := m.memberFixture.Open(ctx, p, wanted)
	if o.Body != nil && m.reads.Add(1) == 1 {
		o.Body = &gatedBody{ReadCloser: o.Body, ctx: ctx, member: m}
	}
	return o, err
}
func (m *gatedMember) Close() error { m.closes.Add(1); return m.memberFixture.Close() }

type gatedBody struct {
	io.ReadCloser
	ctx    context.Context
	member *gatedMember
}

func (b *gatedBody) Read([]byte) (int, error) {
	close(b.member.entered)
	<-b.ctx.Done()
	return 0, b.ctx.Err()
}
func (b *gatedBody) Close() error {
	close(b.member.closing)
	<-b.member.release
	return b.ReadCloser.Close()
}
func await(t *testing.T, signal <-chan struct{}) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(5 * time.Second):
		t.Fatal("missing acknowledgement")
	}
}

func TestMemberReconnectionWaitsForBodyAcknowledgement(t *testing.T) {
	ctx := context.Background()
	opts, members, roots := rig(t, 3)
	gate := &gatedMember{memberFixture: members[0], entered: make(chan struct{}), closing: make(chan struct{}), release: make(chan struct{})}
	opts.Members[0].Driver = gate
	d, c := attach(t, opts)
	t.Cleanup(func() {
		select {
		case <-gate.release:
		default:
			close(gate.release)
		}
	})
	content := []byte("member handoff preserves the logical file")
	if _, err := c.Write(ctx, "user", "/pool/file", bytes.NewReader(content), nil); err != nil {
		t.Fatal(err)
	}
	opened, err := c.Open(ctx, "user", "/pool/file", nil)
	if err != nil {
		t.Fatal(err)
	}
	readDone := make(chan error, 1)
	go func() {
		b, err := io.ReadAll(opened.Body)
		if !bytes.Equal(b, content) {
			err = errors.Join(err, errors.New("wrong content after recovery"))
		}
		readDone <- errors.Join(err, opened.Body.Close())
	}()
	await(t, gate.entered)
	reconnectCtx, cancel := context.WithCancel(ctx)
	var builds atomic.Int32
	reconnected := make(chan error, 1)
	go func() {
		change, err := d.ReconnectMember(reconnectCtx, "A", "private-root-A", func(context.Context) (core.Driver, error) {
			builds.Add(1)
			return nil, errors.New("unexpected early build")
		})
		if change.Effect != core.Applied {
			err = errors.Join(err, errors.New("missing retirement receipt"))
		}
		reconnected <- err
	}()
	await(t, gate.closing)
	if builds.Load() != 0 || gate.closes.Load() != 0 {
		t.Fatal("native body acknowledgement was skipped")
	}
	if _, err := d.ReconnectMember(ctx, "A", "private-root-A", nil); core.CodeOf(err) != core.UpstreamThrottled {
		t.Fatal(err)
	}
	// Namespace and a separate degraded reader progress while the old native Body
	// has not acknowledged Close. No new request borrows the retiring instance.
	if got, err := read(t, c, "/pool/file", nil); err != nil || !bytes.Equal(got, content) {
		t.Fatal(string(got), err)
	}
	cancel()
	if err := <-reconnected; !errors.Is(err, context.Canceled) || builds.Load() != 0 {
		t.Fatal(err)
	}
	close(gate.release)
	if err := <-readDone; err != nil {
		t.Fatal(err)
	}
	if _, err := d.ReconnectMember(ctx, "A", "wrong-root", nil); core.CodeOf(err) != core.InvalidRequest {
		t.Fatal(err)
	}
	buildErr := errors.New("checkpoint unavailable")
	if _, err := d.ReconnectMember(ctx, "A", "private-root-A", func(context.Context) (core.Driver, error) {
		if gate.closes.Load() != 1 {
			t.Fatal("old credential owner is still active")
		}
		return nil, buildErr
	}); !errors.Is(err, buildErr) {
		t.Fatal(err)
	}
	if _, err := d.ReconnectMember(ctx, "A", "private-root-A", func(context.Context) (core.Driver, error) { return local.New(roots[0]) }); err != nil {
		t.Fatal(err)
	}
	if gate.closes.Load() != 1 {
		t.Fatal("native Close repeated")
	}
	if got, err := read(t, c, "/pool/file", nil); err != nil || !bytes.Equal(got, content) {
		t.Fatal(err)
	}
}

func TestRepairAndDurableMaintenanceResume(t *testing.T) {
	ctx := context.Background()
	opts, _, roots := rig(t, 3)
	d, c := attach(t, opts)
	content := []byte("0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJK")
	if _, err := c.Write(ctx, "user", "/pool/file", bytes.NewReader(content), nil); err != nil {
		t.Fatal(err)
	}
	f, err := d.file("/file")
	if err != nil {
		t.Fatal(err)
	}
	for index, slot := range []int{0, 2, 1} {
		s, err := d.stripe(f, int64(index))
		if err != nil {
			t.Fatal(err)
		}
		r := s.Shards[slot]
		p := filepath.Join(roots[int(r.Member[0]-'A')], r.Path[1:])
		if index == 1 {
			err = os.WriteFile(p, []byte("corrupt!"), 0600)
		} else {
			err = os.Remove(p)
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	request := MaintenanceRequest{Kind: RepairWork}
	if change, err := d.PlanMaintenance(ctx, "repair-1", request); err != nil || change.Effect != core.Applied {
		t.Fatal(change, err)
	}
	if change, err := d.PlanMaintenance(ctx, "repair-1", request); err != nil || change.Effect != core.Unchanged {
		t.Fatal(change, err)
	}
	if _, err := d.PlanMaintenance(ctx, "repair-1", MaintenanceRequest{Kind: CollectWork}); core.CodeOf(err) != core.AlreadyExists {
		t.Fatal(err)
	}
	job, err := d.RunMaintenance(ctx, "repair-1", 1)
	if err != nil || job.Last.Changed != 1 || job.Done {
		t.Fatal(job, err)
	}
	if _, err := d.PlanMaintenance(ctx, "abandoned", request); err != nil {
		t.Fatal(err)
	}
	if _, err := d.ForgetMaintenance(ctx, "abandoned"); err != nil {
		t.Fatal(err)
	}
	if err := c.Close(ctx); err != nil {
		t.Fatal(err)
	}
	for i, root := range roots {
		native, err := local.New(root)
		if err != nil {
			t.Fatal(err)
		}
		opts.Members[i].Driver = native
	}
	d, c = attach(t, opts)
	jobs, err := d.MaintenanceJobs(ctx, "", 1)
	if err != nil || len(jobs) != 1 || jobs[0].Done || jobs[0].Last.Changed != 1 {
		t.Fatal(jobs, err)
	}
	queue, err := task.New(task.Options{Concurrency: 1, Capacity: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer queue.Close(ctx)
	changed := 0
	handle, err := queue.Submit(ctx, task.Work{Run: func(ctx context.Context, report func(any)) (any, error) {
		for {
			job, err := d.RunMaintenance(ctx, "repair-1", 1)
			changed += job.Last.Changed
			report(job)
			if err != nil || job.Done {
				return job, err
			}
		}
	}})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := handle.Wait(ctx)
	if err != nil || snapshot.Err != nil || changed != 2 {
		t.Fatal(snapshot, err, changed)
	}
	if got, err := read(t, c, "/pool/file", nil); err != nil || !bytes.Equal(got, content) {
		t.Fatal(err)
	}
	// Beyond the erasure budget no placement is published and no valid slot is
	// destroyed. Restoring the two test objects makes the same manifest readable.
	s, err := d.stripe(f, 0)
	if err != nil {
		t.Fatal(err)
	}
	var paths []string
	var saved [][]byte
	for _, r := range s.Shards[:2] {
		p := filepath.Join(roots[int(r.Member[0]-'A')], r.Path[1:])
		b, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		paths, saved = append(paths, p), append(saved, b)
		if err := os.Remove(p); err != nil {
			t.Fatal(err)
		}
	}
	if result, err := d.Repair(ctx, 1); err == nil || result.Changed != 0 {
		t.Fatal(result, err)
	}
	for i, p := range paths {
		if err := os.WriteFile(p, saved[i], 0600); err != nil {
			t.Fatal(err)
		}
	}
	_, spare, _ := addLocal(t, d, "D")
	if _, err := d.ReconnectMember(ctx, "A", "private-root-A", nil); err != nil {
		t.Fatal(err)
	}
	// Cancellation after native creation retains the new private object but must
	// not switch the logical placement or forget the pending maintenance intent.
	cancelCtx, cancel := context.WithCancel(ctx)
	spare.create = func(ctx context.Context, p string, r io.Reader, n *int64) (core.Change, error) {
		change, err := spare.Driver.Create(ctx, p, r, n)
		cancel()
		return change, err
	}
	if _, err := d.PlanMaintenance(ctx, "cancelled", request); err != nil {
		t.Fatal(err)
	}
	job, err = d.RunMaintenance(cancelCtx, "cancelled", 1)
	if !errors.Is(err, context.Canceled) || job.Done || job.Last.Changed != 0 {
		t.Fatal(job, err)
	}
	spare.mu.Lock()
	spare.create = nil
	spare.mu.Unlock()
	if _, err := d.ConfigureMember(ctx, "D", MemberPolicy{MaxBytes: 8}); err != nil {
		t.Fatal(err)
	}
	if result, err := d.Drain(ctx, "A", 1); core.CodeOf(err) != core.CapacityExceeded || result.Changed != 0 {
		t.Fatal(result, err)
	}
	if _, err := d.ConfigureMember(ctx, "D", MemberPolicy{MaxBytes: 80}); err != nil {
		t.Fatal(err)
	}
	drainAll(t, d, "A") // No source instance: rebuild each required shard.
	if got, err := read(t, c, "/pool/file", nil); err != nil || !bytes.Equal(got, content) {
		t.Fatal(err)
	}
	if _, err := d.Collect(ctx, 100); err == nil {
		t.Fatal("offline garbage was reported reclaimed")
	}
	if _, err := d.ReconnectMember(ctx, "A", "private-root-A", func(context.Context) (core.Driver, error) { return local.New(roots[0]) }); err != nil {
		t.Fatal(err)
	}
	collectAll(t, d)
	if _, err := d.RemoveMember(ctx, "A"); err != nil {
		t.Fatal(err)
	}
	if _, err := d.ForgetMaintenance(ctx, "repair-1"); err != nil {
		t.Fatal(err)
	}
	if job, err = d.RunMaintenance(ctx, "cancelled", 100); err != nil || !job.Done {
		t.Fatal(job, err)
	}
	if _, err := d.ForgetMaintenance(ctx, "cancelled"); err != nil {
		t.Fatal(err)
	}
	if jobs, err := d.MaintenanceJobs(ctx, "", 1); err != nil || len(jobs) != 0 {
		t.Fatal(jobs, err)
	}

	// With 1+2 coding, one valid input suffices even when both data and parity
	// are lost. The Drain fallback and scrub must reconstruct parity as well.
	parityOpts, parityMembers, parityRoots := rig(t, 3)
	parityOpts.DataShards, parityOpts.ParityShards = 1, 2
	parityPool, parityCore := attach(t, parityOpts)
	if _, err := parityCore.Write(ctx, "user", "/pool/file", bytes.NewBufferString("parity"), nil); err != nil {
		t.Fatal(err)
	}
	pf, err := parityPool.file("/file")
	if err != nil {
		t.Fatal(err)
	}
	ps, err := parityPool.stripe(pf, 0)
	if err != nil {
		t.Fatal(err)
	}
	parityMembers[0].mu.Lock()
	parityMembers[0].offline = true
	parityMembers[0].mu.Unlock()
	bad := ps.Shards[1]
	if err := os.WriteFile(filepath.Join(parityRoots[int(bad.Member[0]-'A')], bad.Path[1:]), []byte("corrupt!"), 0600); err != nil {
		t.Fatal(err)
	}
	addLocal(t, parityPool, "D")
	if result, err := parityPool.Drain(ctx, "A", 4); err != nil || result.Changed != 1 || result.More {
		t.Fatal(result, err)
	}
	if result, err := parityPool.Repair(ctx, 4); err != nil || result.Changed != 1 || result.More {
		t.Fatal(result, err)
	}
	if got, err := read(t, parityCore, "/pool/file", nil); err != nil || string(got) != "parity" {
		t.Fatal(string(got), err)
	}
}

// This resolver is valid only after the test has waited for its local writer
// process to exit. That quiescence, not Stat/hash alone, fences delayed creates.
type settledLocal struct {
	*local.Driver
	lostDelete    *atomic.Bool
	resolutionErr error
}

func (m *settledLocal) ResolveUpload(ctx context.Context, intent PrivateUpload) (core.Change, error) {
	o, err := m.Driver.Open(ctx, intent.Path, nil)
	if core.CodeOf(err) == core.NotFound {
		return core.Change{Effect: core.Unchanged}, m.resolutionErr
	}
	if err != nil {
		return core.Change{}, err
	}
	b, err := io.ReadAll(io.LimitReader(o.Body, intent.Size+1))
	err = errors.Join(err, o.Body.Close())
	if err != nil || int64(len(b)) != intent.Size || sha256.Sum256(b) != intent.SHA256 {
		return core.Change{}, errors.Join(err, ErrUncertain)
	}
	return core.Change{Effect: core.Applied}, m.resolutionErr
}
func (m *settledLocal) Remove(ctx context.Context, p string) (core.Change, error) {
	change, err := m.Driver.Remove(ctx, p)
	if err == nil && m.lostDelete != nil && m.lostDelete.CompareAndSwap(false, true) {
		return core.Change{}, core.Fail(core.UpstreamTimeout, nil)
	}
	return change, err
}

func TestPoolRecoveryAfterProcessExit(t *testing.T) {
	ctx := context.Background()
	base := os.Getenv("MOUNTWEAVE_POOL_CRASH_TEST")
	child := base != ""
	crash := func(mode string, code int) {
		binary, err := os.Executable()
		if err != nil {
			t.Fatal(err)
		}
		runCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		command := exec.CommandContext(runCtx, binary, "-test.run=^TestPoolRecoveryAfterProcessExit$", "-test.timeout=20s")
		command.Env = append(os.Environ(), "MOUNTWEAVE_POOL_CRASH_TEST="+base, "MOUNTWEAVE_POOL_CRASH_MODE="+mode)
		output, err := command.CombinedOutput()
		var exit *exec.ExitError
		if !errors.As(err, &exit) || exit.ExitCode() != code {
			t.Fatalf("writer did not reach crash point: %v %s", err, output)
		}
	}
	if !child {
		base = t.TempDir()
		for _, id := range []string{"A", "B", "C", "D"} {
			if err := os.Mkdir(filepath.Join(base, id), 0700); err != nil {
				t.Fatal(err)
			}
		}
		crash("upload", 47)
	}
	opts := Options{MetadataPath: filepath.Join(base, "pool.db"), LockTimeout: time.Second, DataShards: 2, ParityShards: 1, ShardBytes: 8, MaxBufferedBytes: 48, Concurrency: 1}
	for _, id := range []string{"A", "B", "C"} {
		native, err := local.New(filepath.Join(base, id))
		if err != nil {
			t.Fatal(err)
		}
		opts.Members = append(opts.Members, Member{ID: id, Identity: "private-root-" + id, FailureDomain: id, Driver: &memberFixture{Driver: native, quota: 1024}})
	}
	content := []byte("0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJK")
	if child {
		d, err := New(opts)
		if err != nil {
			t.Fatal(err)
		}
		if os.Getenv("MOUNTWEAVE_POOL_CRASH_MODE") == "repair" {
			f, err := d.file("/committed")
			if err != nil {
				t.Fatal(err)
			}
			s, err := d.stripe(f, 0)
			if err != nil {
				t.Fatal(err)
			}
			r := s.Shards[0]
			if err := os.Remove(filepath.Join(base, r.Member, r.Path[1:])); err != nil {
				t.Fatal(err)
			}
			native, err := local.New(filepath.Join(base, "D"))
			if err != nil {
				t.Fatal(err)
			}
			spare := &memberFixture{Driver: native, quota: 1024}
			if _, err := d.AddMember(ctx, Member{ID: "D", Identity: "private-root-D", FailureDomain: "D", Driver: spare}); err != nil {
				t.Fatal(err)
			}
			if _, err := d.PlanMaintenance(ctx, "repair-crash", MaintenanceRequest{Kind: RepairWork}); err != nil {
				t.Fatal(err)
			}
			spare.create = func(ctx context.Context, p string, r io.Reader, n *int64) (core.Change, error) {
				if _, err := spare.Driver.Create(ctx, p, r, n); err != nil {
					t.Fatal(err)
				}
				os.Exit(48) // Repair target exists; placement/receipt remain unconfirmed.
				return core.Change{}, nil
			}
			d.RunMaintenance(ctx, "repair-crash", 1)
			t.Fatal("repair crash hook not reached")
		}
		if _, err := d.Create(ctx, "/committed", bytes.NewReader(content), nil); err != nil {
			t.Fatal(err)
		}
		if _, err := d.PlanMaintenance(ctx, "gc", MaintenanceRequest{Kind: CollectWork}); err != nil {
			t.Fatal(err)
		}
		m := opts.Members[0].Driver.(*memberFixture)
		m.create = func(ctx context.Context, p string, r io.Reader, n *int64) (core.Change, error) {
			if _, err := m.Driver.Create(ctx, p, r, n); err != nil {
				t.Fatal(err)
			}
			os.Exit(47) // Real process exit after native creation, before its receipt.
			return core.Change{}, nil
		}
		d.Create(ctx, "/interrupted", bytes.NewBufferString("uncommitted"), nil)
		t.Fatal("crash hook not reached")
	}
	d, c := attach(t, opts)
	if got, err := read(t, c, "/pool/committed", nil); err != nil || !bytes.Equal(got, content) {
		t.Fatal(err)
	}
	if _, err := c.Stat(ctx, "user", "/pool/interrupted"); core.CodeOf(err) != core.NotFound {
		t.Fatal(err)
	}
	if result, err := d.RunMaintenance(ctx, "gc", 100); !errors.Is(err, ErrUncertain) || result.Done {
		t.Fatal(result, err)
	}
	var lostDelete atomic.Bool
	resolutionWarning := errors.New("terminal absence confirmed with an accompanying warning")
	for _, id := range []string{"A", "B", "C"} {
		if _, err := d.ReconnectMember(ctx, id, "private-root-"+id, func(context.Context) (core.Driver, error) {
			native, err := local.New(filepath.Join(base, id))
			if err != nil {
				return nil, err
			}
			settled := &settledLocal{Driver: native, lostDelete: &lostDelete}
			if id == "C" {
				settled.resolutionErr = resolutionWarning
			}
			return settled, nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	job, err := d.RunMaintenance(ctx, "gc", 100)
	if core.CodeOf(err) != core.UpstreamTimeout || !errors.Is(err, resolutionWarning) || job.Done || !lostDelete.Load() {
		t.Fatal(job, err)
	}
	for i := 0; i < 10 && !job.Done; i++ {
		job, err = d.RunMaintenance(ctx, "gc", 2)
		if err != nil {
			t.Fatal(job, err)
		}
	}
	if !job.Done {
		t.Fatal("recovery did not finish")
	}
	members, err := d.Members(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range members {
		if m.UsedBytes != 24 {
			t.Fatal("unconfirmed quota was lost or released twice", members)
		}
	}
	if got, err := read(t, c, "/pool/committed", nil); err != nil || !bytes.Equal(got, content) {
		t.Fatal(err)
	}
	if err := c.Close(ctx); err != nil {
		t.Fatal(err)
	}
	crash("repair", 48)
	// Reopen the interrupted repair, retain its unknown target and use a fresh
	// immutable attempt. Reconciliation then removes the original duplicate.
	opts.Members = nil
	for _, id := range []string{"A", "B", "C", "D"} {
		native, err := local.New(filepath.Join(base, id))
		if err != nil {
			t.Fatal(err)
		}
		opts.Members = append(opts.Members, Member{ID: id, Identity: "private-root-" + id, FailureDomain: id, Driver: &settledLocal{Driver: native}})
	}
	d, c = attach(t, opts)
	for i := 0; i < 10; i++ {
		job, err = d.RunMaintenance(ctx, "repair-crash", 1)
		if err != nil {
			t.Fatal(job, err)
		}
		if job.Done {
			break
		}
	}
	if !job.Done {
		t.Fatal("interrupted repair did not resume")
	}
	collectAll(t, d)
	members, err = d.Members(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(members) != 4 || members[0].UsedBytes != 16 || members[3].UsedBytes != 8 {
		t.Fatal("repair retry lost its original object ownership", members)
	}
	if got, err := read(t, c, "/pool/committed", nil); err != nil || !bytes.Equal(got, content) {
		t.Fatal(err)
	}
}
