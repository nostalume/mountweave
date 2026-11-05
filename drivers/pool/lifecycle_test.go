package pool

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/local"
	bolt "go.etcd.io/bbolt"
)

func collectAll(t *testing.T, d *Driver) {
	t.Helper()
	for i := 0; i < 100; i++ {
		result, err := d.Collect(context.Background(), 2)
		if err != nil {
			t.Fatal(result, err)
		}
		if !result.More {
			return
		}
	}
	t.Fatal("collector made no bounded progress")
}
func drainAll(t *testing.T, d *Driver, id string) {
	t.Helper()
	for i := 0; i < 100; i++ {
		result, err := d.Drain(context.Background(), id, 2)
		if err != nil {
			t.Fatal(result, err)
		}
		if !result.More {
			return
		}
	}
	t.Fatal("drain made no bounded progress")
}
func addLocal(t *testing.T, d *Driver, id string) (Member, *memberFixture, string) {
	t.Helper()
	root := t.TempDir()
	native, err := local.New(root)
	if err != nil {
		t.Fatal(err)
	}
	fixture := &memberFixture{Driver: native, quota: 1024}
	m := Member{ID: id, Identity: "private-root-" + id, FailureDomain: id, Driver: fixture, MaxBytes: 80}
	if change, err := d.AddMember(context.Background(), m); err != nil || change.Effect != core.Applied {
		_ = fixture.Close()
		t.Fatal(change, err)
	}
	return m, fixture, root
}

func TestPoolFileManagementAndMembershipLifecycle(t *testing.T) {
	opts, _, roots := rig(t, 3)
	opts.MaxBufferedBytes = 72
	for i := range opts.Members {
		opts.Members[i].MaxBytes = 80
	}
	d, c := attach(t, opts)
	ctx := context.Background()
	for _, p := range []string{"/pool/docs", "/pool/docs/sub"} {
		if _, err := c.Mkdir(ctx, "user", core.Path(p)); err != nil {
			t.Fatal(err)
		}
	}
	old := []byte("0123456789abcdefghijklmnopqrstuvwxyz")
	if change, err := c.Write(ctx, "user", core.Path("/pool/docs/sub/movie"), bytes.NewReader(old), nil, core.WriteOptions{Policy: core.Publish(core.IfAbsent())}); err != nil || change.Before != core.TargetMissing {
		t.Fatal(change, err)
	}
	oldFile, err := d.file("/docs/sub/movie")
	if err != nil {
		t.Fatal(err)
	}
	oldStripe, err := d.stripe(oldFile, 0)
	if err != nil {
		t.Fatal(err)
	}
	oldShard := oldStripe.Shards[0]
	oldPath := filepath.Join(roots[int(oldShard.Member[0]-'A')], oldShard.Path[1:])
	opened, err := c.Open(ctx, "user", core.Path("/pool/docs/sub/movie"), core.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	prefix := make([]byte, 1)
	if opened, err := c.Open(ctx, "user", core.Path("/pool/docs/sub/movie"), core.OpenOptions{Mode: core.LiveEdit, Truncate: true}); core.CodeOf(err) != core.UnsupportedOperation || opened.Change.Effect != core.Unchanged {
		t.Fatal(opened, err)
	}
	opening, cancelOpening := context.WithCancel(ctx)
	fixed, err := c.Open(opening, "user", core.Path("/pool/docs/sub/movie"), core.OpenOptions{Mode: core.BoundRead, Binding: core.PinnedVersion})
	if err != nil {
		t.Fatal(err)
	}
	defer fixed.File.Close()
	cancelOpening()
	if _, err = io.ReadFull(opened.Body, prefix); err != nil {
		t.Fatal(err)
	}
	if change, err := c.Write(ctx, "user", core.Path("/pool/docs/sub/movie"), &faultInput{}, nil, core.WriteOptions{Policy: core.Publish()}); err == nil || change.Effect != core.Unchanged {
		t.Fatal(change, err)
	}
	fresh := []byte("new complete content")
	oldTag := opened.Entry.Evidence.Validator
	if !oldTag.Valid() {
		t.Fatal("generation validator missing")
	}
	other := core.Validator{Domain: oldTag.Domain, Tag: `"other"`}
	when := core.All(core.IfMatch(oldTag, other), core.IfNotMatch(other))
	if change, err := c.Write(ctx, "user", core.Path("/pool/docs/sub/movie"), bytes.NewReader(fresh), nil, core.WriteOptions{Policy: core.Publish(when), ObserveTarget: true}); err != nil || change.Before != core.TargetPresent || !change.Evidence.Validator.Valid() {
		t.Fatal(change, err)
	}
	prepared, err := c.Stat(ctx, "user", core.Path("/pool/docs/sub/movie"))
	if err != nil {
		t.Fatal(err)
	}
	paused := &pausedInput{started: make(chan struct{}), release: make(chan struct{}), data: strings.NewReader("stale prepared input")}
	t.Cleanup(func() {
		select {
		case <-paused.release:
		default:
			close(paused.release)
		}
	})
	guarded := make(chan error, 1)
	go func() {
		change, err := c.Write(ctx, "user", core.Path("/pool/docs/sub/movie"), paused, nil, core.WriteOptions{Policy: core.Publish(core.IfMatch(prepared.Evidence.Validator))})
		if change.Effect != core.Unchanged {
			err = errors.Join(err, errors.New("stale writer changed target"))
		}
		guarded <- err
	}()
	select {
	case <-paused.started:
	case <-time.After(5 * time.Second):
		t.Fatal("guarded writer did not start")
	}
	// Two writers to one name are already excluded by the upload owner. A
	// competing namespace replacement can still occur while its input is paused.
	if _, err := c.Write(ctx, "user", core.Path("/pool/peer"), bytes.NewReader(fresh), nil); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Move(ctx, "user", prepared.Address, core.Path("/pool/previous"), core.MoveOptions{Source: core.IfMatch(prepared.Evidence.Validator), Target: core.IfAbsent()}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Move(ctx, "user", core.Path("/pool/peer"), prepared.Address); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Remove(ctx, "user", core.Path("/pool/previous")); err != nil {
		t.Fatal(err)
	}
	close(paused.release)
	if err := <-guarded; core.CodeOf(err) != core.PreconditionFailed {
		t.Fatal("commit condition was not rechecked", err)
	}
	stale := core.IfMatch(oldTag)
	if change, err := c.Remove(ctx, "user", core.Path("/pool/docs/sub/movie"), core.RemoveOptions{When: stale}); core.CodeOf(err) != core.PreconditionFailed || change.Effect != core.Unchanged {
		t.Fatal(change, err)
	}
	if result, err := c.Move(ctx, "user", core.Path("/pool/docs/sub/movie"), core.Path("/pool/stale"), core.MoveOptions{Source: stale}); core.CodeOf(err) != core.PreconditionFailed || result.Source.Effect != core.Unchanged || result.Target.Effect != core.Unchanged {
		t.Fatal(result, err)
	}
	if _, err := c.Open(ctx, "user", core.Path("/pool/docs/sub/movie"), core.OpenOptions{Selection: core.ReadSelection{Version: opened.Entry.Evidence.Version}}); core.CodeOf(err) != core.NotFound {
		t.Fatal("version silently rebound", err)
	}
	current, err := c.Stat(ctx, "user", core.Path("/pool/docs/sub/movie"))
	if err != nil {
		t.Fatal(err)
	}
	weak := current.Evidence.Validator
	weak.Tag = "W/" + weak.Tag
	if _, err := c.Open(ctx, "user", current.Address, core.OpenOptions{Selection: core.ReadSelection{When: core.IfNotMatch(weak)}}); core.CodeOf(err) != core.PreconditionFailed {
		t.Fatal("weak negative comparison", err)
	}
	if got, err := read(t, c, "/pool/docs/sub/movie", nil); err != nil || !bytes.Equal(got, fresh) {
		t.Fatal(got, err)
	}
	if _, err := d.Collect(ctx, 32); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(oldPath); err != nil {
		t.Fatal("collector removed a pinned generation", err)
	}
	rest, err := io.ReadAll(opened.Body)
	if err != nil || !bytes.Equal(append(prefix, rest...), old) {
		t.Fatal("old reader changed generation", rest, err)
	}
	if err := opened.Body.Close(); err != nil {
		t.Fatal(err)
	}
	boundData := make([]byte, len(old)+1)
	if n, err := fixed.File.ReadAt(ctx, boundData, 0); n != len(old) || err != io.EOF || !bytes.Equal(boundData[:n], old) {
		t.Fatal(n, err, string(boundData))
	}
	d.mu.Lock()
	retained := d.used
	d.mu.Unlock()
	if retained != 0 {
		t.Fatal("idle File retained stripe-window credit", retained)
	}
	if _, err := d.Collect(ctx, 32); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(oldPath); err != nil {
		t.Fatal("EOF released pin", err)
	}
	if n, err := fixed.File.ReadAt(ctx, boundData[:2], 1); n != 2 || err != nil || !bytes.Equal(boundData[:2], old[1:3]) {
		t.Fatal(n, err, boundData)
	}
	state, err := fixed.File.Stat(ctx)
	if err != nil || state.Evidence.Version != opened.Entry.Evidence.Version {
		t.Fatal(state, err)
	}
	if err := fixed.File.Close(); err != nil {
		t.Fatal(err)
	}
	collectAll(t, d)
	if _, err := os.Stat(oldPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("obsolete generation was not reclaimed", err)
	}
	// The directory scope keeps its ID across a metadata-only directory move.
	scope, err := d.OpenDirectory(ctx, core.NativePath("/docs/sub"))
	if err != nil {
		t.Fatal(err)
	}
	if items, err := scope.Next(ctx, 10); err != io.EOF || len(items) != 1 {
		t.Fatal(items, err)
	}
	before, err := d.Members(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if result, err := c.Move(ctx, "user", core.Path("/pool/docs"), core.Path("/pool/moved")); err != nil || result.Target.Effect != core.Applied {
		t.Fatal(result, err)
	}
	bound, err := scope.OpenFile(ctx, "movie")
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(bound.Body)
	if err != nil || !bytes.Equal(got, fresh) {
		t.Fatal(got, err)
	}
	if err := bound.Body.Close(); err != nil {
		t.Fatal(err)
	}
	if err := scope.Close(); err != nil {
		t.Fatal(err)
	}
	after, err := d.Members(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for i := range before {
		if before[i].UsedBytes != after[i].UsedBytes {
			t.Fatal("rename moved content")
		}
	}
	tree := core.TreeOptions{MaxEntries: 16, MaxPlanBytes: 1024}
	if _, err := c.CopyTree(ctx, "user", core.Path("/pool/moved"), core.Path("/pool/duplicate"), tree); err != nil {
		t.Fatal(err)
	}
	if _, err := c.RemoveTree(ctx, "user", core.Path("/pool/duplicate"), tree); err != nil {
		t.Fatal(err)
	}
	collectAll(t, d)
	_, _, rootD := addLocal(t, d, "D")
	roots = append(roots, rootD)
	if _, err := c.Write(ctx, "user", core.Path("/pool/added"), strings.NewReader("joined"), nil); err != nil {
		t.Fatal(err)
	}
	if members, err := d.Members(ctx); err != nil {
		t.Fatal(err)
	} else {
		for _, member := range members {
			if member.ID == "D" && member.UsedBytes == 0 {
				t.Fatal("small-file placement ignored the added capacity")
			}
		}
	}
	candidate, err := local.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if result, err := d.AddMember(ctx, Member{ID: "D", Identity: "another-root", FailureDomain: "D", Driver: candidate}); core.CodeOf(err) != core.AlreadyExists || result.Effect != core.Unchanged {
		t.Fatal(result, err)
	}
	if _, err := candidate.Stat(ctx, core.NativePath("/")); err != nil {
		t.Fatal("rejected member was adopted", err)
	}
	if err := candidate.Close(); err != nil {
		t.Fatal(err)
	}
	reader, err := c.Open(ctx, "user", core.Path("/pool/moved/sub/movie"), core.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadFull(reader.Body, make([]byte, 1)); err != nil {
		t.Fatal(err)
	}
	drainAll(t, d, "A")
	if _, err := d.Collect(ctx, 32); err != nil {
		t.Fatal(err)
	}
	if result, err := d.RemoveMember(ctx, "A"); core.CodeOf(err) != core.CapacityExceeded || result.Effect != core.Unchanged {
		t.Fatal("removed member held by an old reader", result, err)
	}
	if _, err := io.Copy(io.Discard, reader.Body); err != nil {
		t.Fatal(err)
	}
	if err := reader.Body.Close(); err != nil {
		t.Fatal(err)
	}
	collectAll(t, d)
	if result, err := d.RemoveMember(ctx, "A"); err != nil || result.Effect != core.Applied {
		t.Fatal(result, err)
	}
	if entries, err := os.ReadDir(roots[0]); err != nil || len(entries) != 0 {
		t.Fatal("drained root still has objects", entries, err)
	}
	info, err := d.Members(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var usedD int64
	for _, m := range info {
		if m.ID == "D" {
			usedD = m.UsedBytes
		}
	}
	if usedD == 0 {
		t.Fatal("new member did not receive content")
	}
	if _, err := d.ConfigureMember(ctx, "D", MemberPolicy{MaxBytes: usedD}); err != nil {
		t.Fatal(err)
	}
	untouched := &faultInput{}
	if result, err := c.Write(ctx, "user", core.Path("/pool/full"), untouched, nil); core.CodeOf(err) != core.CapacityExceeded || result.Effect != core.Unchanged || untouched.reads != 0 {
		t.Fatal(result, err, untouched.reads)
	}
	if err := c.Close(ctx); err != nil {
		t.Fatal(err)
	}
	opts.Members = nil
	for _, m := range info {
		if m.Removed {
			continue
		}
		native, err := local.New(roots[int(m.ID[0]-'A')])
		if err != nil {
			t.Fatal(err)
		}
		opts.Members = append(opts.Members, Member{ID: m.ID, Identity: m.Identity, FailureDomain: m.FailureDomain, Driver: native})
	}
	_, restored := attach(t, opts)
	if got, err := read(t, restored, "/pool/moved/sub/movie", nil); err != nil || !bytes.Equal(got, fresh) {
		t.Fatal("restored membership/namespace", got, err)
	}
	untouched = &faultInput{}
	if result, err := restored.Write(ctx, "user", core.Path("/pool/still-full"), untouched, nil); core.CodeOf(err) != core.CapacityExceeded || result.Effect != core.Unchanged || untouched.reads != 0 {
		t.Fatal("quota policy was reset on reopen", result, err)
	}
}

type pausedInput struct {
	started, release chan struct{}
	once             sync.Once
	data             *strings.Reader
}

func (r *pausedInput) Read(p []byte) (int, error) {
	r.once.Do(func() { close(r.started) })
	<-r.release
	return r.data.Read(p)
}

func TestPoolParentCompetitionAndPartialDrain(t *testing.T) {
	opts, _, roots := rig(t, 3)
	d, c := attach(t, opts)
	ctx := context.Background()
	if _, err := c.Write(ctx, "user", core.Path("/pool/legacy"), strings.NewReader("legacy-state"), nil); err != nil {
		t.Fatal(err)
	}
	f, err := d.file("/legacy")
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Close(ctx); err != nil {
		t.Fatal(err)
	}
	// Encode the original version-one disk schema, retaining actual private
	// shard bytes. Reopen must atomically import names and derive reservations.
	db, err := bolt.Open(opts.MetadataPath, 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	err = db.Update(func(tx *bolt.Tx) error {
		var cfg layout
		if err := decode(tx.Bucket(configKey).Get(headerKey), &cfg); err != nil {
			return err
		}
		cfg.Version = 1
		for _, m := range opts.Members {
			cfg.Members = append(cfg.Members, memberRecord{ID: m.ID, Identity: m.Identity, FailureDomain: m.FailureDomain})
		}
		if err := tx.Bucket(configKey).Put(headerKey, encode(cfg)); err != nil {
			return err
		}
		if err := tx.Bucket(filesKey).Delete([]byte(f.ID)); err != nil {
			return err
		}
		if err := tx.Bucket(filesKey).Put([]byte("/legacy"), encode(f)); err != nil {
			return err
		}
		if err := tx.Bucket(uploadsKey).Bucket([]byte(f.ID)).Put(headerKey, encode(uploadRecord{Path: "/legacy", Complete: true})); err != nil {
			return err
		}
		for _, key := range [][]byte{membersKey, entriesKey, dirsKey, refsKey, garbageKey} {
			if err := tx.DeleteBucket(key); err != nil {
				return err
			}
		}
		return nil
	})
	if closeErr := db.Close(); err != nil || closeErr != nil {
		t.Fatal(err, closeErr)
	}
	for i := range opts.Members {
		native, err := local.New(roots[i])
		if err != nil {
			t.Fatal(err)
		}
		opts.Members[i].Driver = native
	}
	d, c = attach(t, opts)
	if data, err := read(t, c, "/pool/legacy", nil); err != nil || string(data) != "legacy-state" {
		t.Fatal("version-one import", data, err)
	}
	if members, err := d.Members(ctx); err != nil || len(members) != 3 || members[0].UsedBytes != 8 {
		t.Fatal("legacy reservations", members, err)
	}
	if _, err := c.Mkdir(ctx, "user", core.Path("/pool/parent")); err != nil {
		t.Fatal(err)
	}
	// A moved parent remains the publication target; a deleted/recreated parent
	// has another ID and must not receive the old upload.
	for pass := 0; pass < 2; pass++ {
		r := &pausedInput{started: make(chan struct{}), release: make(chan struct{}), data: strings.NewReader("parent-bound content")}
		t.Cleanup(func() {
			select {
			case <-r.release:
			default:
				close(r.release)
			}
		})
		finished := make(chan error, 1)
		go func() {
			change, err := c.Write(ctx, "user", core.Path("/pool/parent/file"), r, nil)
			if pass == 0 && change.Effect != core.Applied {
				err = errors.Join(err, errors.New("moved parent did not publish"))
			}
			if pass == 1 && change.Effect != core.Unchanged {
				err = errors.Join(err, errors.New("recreated parent received upload"))
			}
			finished <- err
		}()
		select {
		case <-r.started:
		case <-time.After(5 * time.Second):
			t.Fatal("input did not start")
		}
		if pass == 0 {
			if _, err := c.Move(ctx, "user", core.Path("/pool/parent"), core.Path("/pool/renamed")); err != nil {
				t.Fatal(err)
			}
		} else {
			if _, err := c.Remove(ctx, "user", core.Path("/pool/parent")); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := c.Mkdir(ctx, "user", core.Path("/pool/parent")); err != nil {
			t.Fatal(err)
		}
		close(r.release)
		if err := <-finished; (pass == 0 && err != nil) || (pass == 1 && core.CodeOf(err) != core.NotFound) {
			t.Fatal(pass, err)
		}
	}
	if _, err := c.Stat(ctx, "user", core.Path("/pool/parent/file")); core.CodeOf(err) != core.NotFound {
		t.Fatal(err)
	}
	collectAll(t, d)
	_, memberD, _ := addLocal(t, d, "D")
	active, cancel := context.WithCancel(ctx)
	defer cancel()
	calls := 0
	memberD.create = func(ctx context.Context, p string, input io.Reader, length *int64) (core.Change, error) {
		calls++
		if calls == 2 {
			cancel()
			return core.Change{Effect: core.Unchanged}, ctx.Err()
		}
		return memberD.Driver.Create(ctx, core.NativePath(p), input, length)
	}
	result, err := d.Drain(active, "A", 32)
	if !errors.Is(err, context.Canceled) || result.Changed != 1 || !result.More {
		t.Fatal("partial drain receipt", result, err)
	}
	if got, err := read(t, c, "/pool/renamed/file", nil); err != nil || string(got) != "parent-bound content" {
		t.Fatal(got, err)
	}
	toggle := true
	memberD.create = func(ctx context.Context, p string, input io.Reader, length *int64) (core.Change, error) {
		if toggle {
			toggle = false
			for _, draining := range []bool{false, true} {
				if _, err := d.ConfigureMember(ctx, "A", MemberPolicy{Draining: draining}); err != nil {
					return core.Change{Effect: core.Unchanged}, err
				}
			}
		}
		return memberD.Driver.Create(ctx, core.NativePath(p), input, length)
	}
	if result, err := d.Drain(ctx, "A", 32); err == nil || !result.More || !strings.Contains(err.Error(), "drain policy changed") {
		t.Fatal("stale drain cursor survived a policy change", result, err)
	}
	drainAll(t, d, "A")
	collectAll(t, d)
	if _, err := d.RemoveMember(ctx, "A"); err != nil {
		t.Fatal(err)
	}
}

func TestAtomicMemberReservationsDuringHotAddition(t *testing.T) {
	opts, members, _ := rig(t, 3)
	for i := range opts.Members {
		opts.Members[i].MaxBytes = 8
	}
	started, release := make(chan struct{}), make(chan struct{})
	members[0].create = func(ctx context.Context, p string, input io.Reader, length *int64) (core.Change, error) {
		close(started)
		<-release
		return members[0].Driver.Create(ctx, core.NativePath(p), input, length)
	}
	d, c := attach(t, opts)
	ctx := context.Background()
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})
	finished := make(chan error, 1)
	go func() {
		_, err := c.Write(ctx, "user", core.Path("/pool/slow"), strings.NewReader("pending"), nil)
		finished <- err
	}()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("native write did not start")
	}
	input := &faultInput{}
	if change, err := c.Write(ctx, "user", core.Path("/pool/full"), input, nil); core.CodeOf(err) != core.CapacityExceeded || change.Effect != core.Unchanged || input.reads != 0 {
		t.Fatal("in-flight quota was oversubscribed", change, err, input.reads)
	}
	_, _, _ = addLocal(t, d, "D")
	for _, id := range []string{"B", "C"} {
		if _, err := d.ConfigureMember(ctx, id, MemberPolicy{MaxBytes: 16}); err != nil {
			t.Fatal(err)
		}
	}
	if change, err := c.Write(ctx, "user", core.Path("/pool/new"), strings.NewReader("next"), nil); err != nil || change.Effect != core.Applied {
		t.Fatal("hot addition waited for unrelated native I/O", change, err)
	}
	if got, err := read(t, c, "/pool/new", nil); err != nil || string(got) != "next" {
		t.Fatal(got, err)
	}
	close(release)
	if err := <-finished; err != nil {
		t.Fatal(err)
	}
	info, err := d.Members(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range info {
		if m.UsedBytes < 0 || (m.MaxBytes > 0 && m.UsedBytes > m.MaxBytes) {
			t.Fatal("invalid persisted quota", m)
		}
	}
}
