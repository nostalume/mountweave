package core_test

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/local"
)

func TestBoundObjectAcrossPublicationAndAuthority(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	write(t, root, "item", []byte("original"))
	write(t, root, "peer", []byte("new"))
	var denyStat, denyPrimary atomic.Bool
	c := newCore(t, core.Options{Authorize: func(ctx context.Context, p core.Principal, a core.Action, address core.Address) error {
		if a == core.StatAction && denyStat.Load() || a == core.ReadAction && address == core.Path("/files/item") && denyPrimary.Load() {
			return errors.New("revoked")
		}
		return allow(ctx, p, a, address)
	}})
	defer closeCore(t, c)
	d, err := local.New(root, local.Options{StagingDirectory: t.TempDir(), MaxStagingBytes: 128})
	if err != nil {
		t.Fatal(err)
	}
	m := installCopyMount(t, c, "/files", d)
	if err := c.BindAlias(ctx, "/view", m, "/"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Open(ctx, "alice", core.Path("/view/item"), core.OpenOptions{Mode: core.BoundRead, Binding: core.PinnedVersion}); core.CodeOf(err) != core.UnsupportedOperation {
		t.Fatal(err)
	}
	opening, cancel := context.WithCancel(ctx)
	opened, err := c.Open(opening, "alice", core.Path("/view/item"), core.OpenOptions{Mode: core.BoundRead})
	if err != nil {
		t.Fatal(err)
	}
	defer opened.File.Close()
	if opened.Body != nil || opened.File == nil || opened.Binding != core.BoundObject {
		t.Fatal(opened)
	}
	cancel() // Successful handoff detaches the opening call, not the File.
	p := make([]byte, 12)
	if n, err := opened.File.ReadAt(ctx, p, 0); n != 8 || err != io.EOF || string(p[:n]) != "original" {
		t.Fatal(n, err, string(p))
	}
	if result, err := c.CopyTree(ctx, "alice", core.Path("/files/peer"), core.Path("/files/item"), core.TreeOptions{}, core.DirectoryCopyOptions{Existing: core.Replace}); err != nil || result.Change.Effect != core.Applied {
		t.Fatal(result, err)
	}
	// The old fd survives replacement; a new ordinary read sees the new object.
	if n, err := opened.File.ReadAt(ctx, p[:8], 0); n != 8 || err != nil || string(p[:8]) != "original" {
		t.Fatal(n, err, string(p))
	}
	current, err := c.Open(ctx, "alice", core.Path("/files/item"), core.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if current.File != nil {
		t.Fatal("stream path constructed a File")
	}
	data, readErr := io.ReadAll(current.Body)
	if err := errors.Join(readErr, current.Body.Close()); err != nil || string(data) != "new" {
		t.Fatal(string(data), err)
	}
	state, err := opened.File.Stat(ctx)
	if err != nil || state.Size == nil || *state.Size != 8 || state.Address != core.Path("/view/item") {
		t.Fatal(state, err)
	}
	denyStat.Store(true)
	if _, err := opened.File.Stat(ctx); core.CodeOf(err) != core.PermissionDenied {
		t.Fatal(err)
	}
	denyPrimary.Store(true)
	if _, err := opened.File.ReadAt(ctx, p, 0); core.CodeOf(err) != core.PermissionDenied {
		t.Fatal(err)
	}
	denyPrimary.Store(false)
	call, stop := context.WithCancel(ctx)
	stop()
	if _, err := opened.File.ReadAt(call, p, 0); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if n, err := opened.File.ReadAt(ctx, p[:3], 0); n != 3 || err != nil || string(p[:3]) != "ori" {
		t.Fatal(n, err, string(p))
	}
	if err := c.UnbindAlias(ctx, "/view"); err != nil {
		t.Fatal(err)
	}
	if err := c.BindAlias(ctx, "/view", m, "/"); err != nil {
		t.Fatal(err)
	}
	if _, err := opened.File.ReadAt(ctx, p, 0); core.CodeOf(err) != core.MountUnavailable {
		t.Fatal("stale handle rebound", err)
	}
	if err := opened.File.Close(); err != nil {
		t.Fatal(err)
	}
	if err := opened.File.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := opened.File.ReadAt(ctx, p, 0); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatal(err)
	}
}

func TestLiveEditAndOrderedNames(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	root := t.TempDir()
	write(t, root, "item", []byte("original"))
	var denyRead, denyWrite atomic.Bool
	c := newCore(t, core.Options{Authorize: func(ctx context.Context, p core.Principal, a core.Action, address core.Address) error {
		if a == core.ReadAction && denyRead.Load() || a == core.WriteAction && denyWrite.Load() {
			return errors.New("revoked")
		}
		return allow(ctx, p, a, address)
	}})
	defer closeCore(t, c)
	d, err := local.New(root, local.Options{StagingDirectory: t.TempDir(), MaxStagingBytes: 128})
	if err != nil {
		t.Fatal(err)
	}
	installCopyMount(t, c, "/files", d)
	scope, err := core.NewOrderScope(4)
	if err != nil {
		t.Fatal(err)
	}
	defer scope.Stop()
	reserve := func() *core.OrderTurn {
		t.Helper()
		turn, err := scope.Reserve(ctx)
		if err != nil {
			t.Fatal(err)
		}
		return turn
	}
	opening := reserve()
	opened, err := c.Open(opening.Context(), "alice", core.Path("/files/item"), core.OpenOptions{Mode: core.LiveEdit, Read: true})
	if err != nil || opened.Change.Effect != core.Unchanged {
		t.Fatal(opened, err)
	}
	defer opened.File.Close()
	// Dispatch backwards; receive order still controls a handle edit and names.
	first, second, third := reserve(), reserve(), reserve()
	unused := reserve()
	if _, err := scope.Reserve(ctx); core.CodeOf(err) != core.CapacityExceeded {
		t.Fatal(err)
	}
	unused.Cancel()
	results := make(chan error, 2)
	go func() {
		_, err := c.Copy(third.Context(), "alice", core.Path("/files/moved"), core.Path("/files/copy"))
		results <- err
	}()
	go func() {
		n, change, err := opened.File.WriteAt(second.Context(), []byte("new"), 0)
		if n != 3 || change.Effect != core.Applied {
			err = errors.Join(err, errors.New("missing write receipt"))
		}
		results <- err
	}()
	if _, err := c.Move(first.Context(), "alice", core.Path("/files/item"), core.Path("/files/moved")); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
	if data, err := os.ReadFile(filepath.Join(root, "copy")); err != nil || string(data) != "newginal" {
		t.Fatal(string(data), err)
	}
	if _, err := c.Write(ctx, "alice", core.Path("/files/item"), strings.NewReader("peer"), nil); err != nil {
		t.Fatal(err)
	}
	truncating := reserve()
	if change, err := opened.File.Truncate(truncating.Context(), 4); err != nil || change.Effect != core.Applied {
		t.Fatal(change, err)
	}
	if data, err := os.ReadFile(filepath.Join(root, "moved")); err != nil || string(data) != "newg" {
		t.Fatal(string(data), err)
	}
	syncing := reserve()
	if err := opened.File.Sync(syncing.Context()); err != nil {
		t.Fatal(err)
	}
	if data, err := os.ReadFile(filepath.Join(root, "item")); err != nil || string(data) != "peer" {
		t.Fatal("editor rebound", string(data), err)
	}
	denyWrite.Store(true)
	revoked := reserve()
	if n, change, err := opened.File.WriteAt(revoked.Context(), []byte("XX"), 0); n != 0 || change.Effect != core.Unchanged || core.CodeOf(err) != core.PermissionDenied {
		t.Fatal(n, change, err)
	}
	denyWrite.Store(false)
	denyRead.Store(true)
	if o, err := c.Open(ctx, "alice", core.Path("/files/item"), core.OpenOptions{Mode: core.LiveEdit, Read: true, Truncate: true}); core.CodeOf(err) != core.PermissionDenied || o.Change.Effect != core.Unchanged {
		t.Fatal(o, err)
	}
	if data, err := os.ReadFile(filepath.Join(root, "item")); err != nil || string(data) != "peer" {
		t.Fatal("denied read-open changed content", string(data), err)
	}
	writeOnly, err := c.Open(ctx, "alice", core.Path("/files/item"), core.OpenOptions{Mode: core.LiveEdit})
	if err != nil {
		t.Fatal(err)
	}
	if err := writeOnly.File.Close(); err != nil {
		t.Fatal(err)
	}
	denyRead.Store(false)
	truncated, err := c.Open(ctx, "alice", core.Path("/files/item"), core.OpenOptions{Mode: core.LiveEdit, Truncate: true})
	if err != nil || truncated.Change.Effect != core.Applied || truncated.Change.Before != core.TargetPresent || truncated.Entry.Size == nil || *truncated.Entry.Size != 0 {
		t.Fatal(truncated, err)
	}
	if err := truncated.File.Close(); err != nil {
		t.Fatal(err)
	}
	// Abandoning an unused head makes the next ordinary call runnable.
	abandoned, next := reserve(), reserve()
	abandoned.Cancel()
	if _, err := c.Stat(next.Context(), "alice", core.Path("/files/item")); err != nil {
		t.Fatal(err)
	}
	// Stream ownership remains in the same sequence until real EOF/Close.
	reading := reserve()
	body, err := c.Open(reading.Context(), "alice", core.Path("/files/moved"), core.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer body.Body.Close()
	deleting := reserve()
	extra1, extra2 := reserve(), reserve()
	if _, err := scope.Reserve(ctx); core.CodeOf(err) != core.CapacityExceeded {
		t.Fatal("stream released order before EOF", err)
	}
	extra1.Cancel()
	extra2.Cancel()
	go func() { _, err := c.Remove(deleting.Context(), "alice", core.Path("/files/moved")); results <- err }()
	data, err := io.ReadAll(body.Body)
	if err != nil || string(data) != "newg" {
		t.Fatal(string(data), err)
	}
	if err := <-results; err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, "moved")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}
	// Cleanup bypasses both the ordered queue and revoked namespace bindings.
	if err := opened.File.Close(); err != nil {
		t.Fatal(err)
	}
	a, err := c.Open(ctx, "alice", core.Path("/files/append"), core.OpenOptions{Mode: core.LiveEdit, Creation: core.OpenNew, AppendOnly: true, Read: true})
	if err != nil || a.Change.Effect != core.Applied || a.Change.Before != core.TargetMissing {
		t.Fatal(a, err)
	}
	defer a.File.Close()
	b, err := c.Open(ctx, "alice", core.Path("/files/append"), core.OpenOptions{Mode: core.LiveEdit, Creation: core.OpenOrCreate, AppendOnly: true})
	if err != nil || b.Change.Effect != core.Unchanged {
		t.Fatal(b, err)
	}
	defer b.File.Close()
	if _, err := b.File.ReadAt(ctx, make([]byte, 1), 0); core.CodeOf(err) != core.UnsupportedOperation {
		t.Fatal(err)
	}
	for _, item := range []struct {
		file *core.BoundFile
		text string
	}{{a.File, "A"}, {b.File, "B"}, {a.File, "C"}} {
		if n, change, err := item.file.Append(ctx, []byte(item.text)); n != 1 || change.Effect != core.Applied || err != nil {
			t.Fatal(n, change, err)
		}
	}
	if data, err := os.ReadFile(filepath.Join(root, "append")); err != nil || string(data) != "ABC" {
		t.Fatal("append used stale position", string(data), err)
	}
	if _, _, err := a.File.WriteAt(ctx, []byte("x"), 0); core.CodeOf(err) != core.UnsupportedOperation {
		t.Fatal(err)
	}
	if n, change, err := a.File.Append(ctx, nil); n != 0 || change.Effect != core.Unchanged || err != nil {
		t.Fatal(n, change, err)
	}
	if change, err := a.File.Truncate(ctx, 2); change.Effect != core.Applied || err != nil {
		t.Fatal(change, err)
	}
	if err := a.File.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Open(ctx, "alice", core.Path("/files/append"), core.OpenOptions{Mode: core.LiveEdit, Creation: core.OpenNew, Truncate: true}); core.CodeOf(err) != core.AlreadyExists {
		t.Fatal(err)
	}
	if data, err := os.ReadFile(filepath.Join(root, "append")); err != nil || string(data) != "AB" {
		t.Fatal("failed open truncated", string(data), err)
	}
}

type partialEditDriver struct {
	*local.Driver
	unknown atomic.Bool
	fault   error
}

func (d *partialEditDriver) Open(ctx context.Context, address core.NativeAddress, options core.OpenOptions) (core.Opened, error) {
	opened, err := d.Driver.Open(ctx, address, options)
	if err == nil && options.Mode == core.LiveEdit {
		opened.File = &partialEditor{EditFile: opened.File.(core.EditFile), owner: d}
	}
	return opened, err
}

type partialEditor struct {
	core.EditFile
	owner *partialEditDriver
}

func (f *partialEditor) WriteAt(ctx context.Context, p []byte, offset int64) (int, core.Change, error) {
	n, change, err := f.EditFile.WriteAt(ctx, p[:1], offset)
	if err != nil {
		return n, change, err
	}
	if f.owner.unknown.Load() {
		return 0, core.Change{Effect: core.Unknown}, f.owner.fault
	}
	change.Effect = core.Incomplete
	return n, change, f.owner.fault
}

func TestOrderedEditPartialCancellationAndCleanup(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	root := t.TempDir()
	write(t, root, "item", []byte("base"))
	native, err := local.New(root)
	if err != nil {
		t.Fatal(err)
	}
	d := &partialEditDriver{Driver: native, fault: errors.New("lost native acknowledgment")}
	c := newCore(t, core.Options{})
	defer closeCore(t, c)
	installCopyMount(t, c, "/files", d)
	scope, err := core.NewOrderScope(2)
	if err != nil {
		t.Fatal(err)
	}
	defer scope.Stop()
	reserve := func() *core.OrderTurn {
		t.Helper()
		turn, err := scope.Reserve(ctx)
		if err != nil {
			t.Fatal(err)
		}
		return turn
	}
	open := func() core.Content {
		t.Helper()
		turn := reserve()
		o, err := c.Open(turn.Context(), "alice", core.Path("/files/item"), core.OpenOptions{Mode: core.LiveEdit})
		if err != nil {
			t.Fatal(err)
		}
		return o
	}
	first := open()
	head, queued := reserve(), reserve()
	done := make(chan error, 1)
	go func() { _, _, err := first.File.WriteAt(queued.Context(), []byte("QQ"), 0); done <- err }()
	if err := first.File.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("closed queued editor ran")
		}
	case <-ctx.Done():
		t.Fatal("Close did not cancel the order wait")
	}
	head.Cancel()
	if data, err := os.ReadFile(filepath.Join(root, "item")); err != nil || string(data) != "base" {
		t.Fatal(string(data), err)
	}
	opened := open()
	defer opened.File.Close()
	partial := reserve()
	n, change, err := opened.File.WriteAt(partial.Context(), []byte("XY"), 0)
	if n != 1 || change.Effect != core.Incomplete || !errors.Is(err, d.fault) {
		t.Fatal(n, change, err)
	}
	if data, err := os.ReadFile(filepath.Join(root, "item")); err != nil || string(data) != "Xase" {
		t.Fatal(string(data), err)
	}
	known := reserve()
	if _, err := c.Stat(known.Context(), "alice", core.Path("/files/item")); err != nil {
		t.Fatal("known partial effect poisoned order", err)
	}
	d.unknown.Store(true)
	uncertain := reserve()
	n, change, err = opened.File.WriteAt(uncertain.Context(), []byte("ZY"), 0)
	if n != 0 || change.Effect != core.Unknown || !errors.Is(err, d.fault) {
		t.Fatal(n, change, err)
	}
	if data, err := os.ReadFile(filepath.Join(root, "item")); err != nil || string(data) != "Zase" {
		t.Fatal("unknown was treated as unchanged", string(data), err)
	}
	if _, err := scope.Reserve(ctx); !errors.Is(err, core.ErrOrderUncertain) {
		t.Fatal(err)
	}
	if err := opened.File.Close(); err != nil {
		t.Fatal("stopped scope blocked cleanup", err)
	}
}
