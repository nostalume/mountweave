package core_test

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"testing/iotest"
	"time"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/local"
)

func TestTreeRootsReplacementAndMovePhases(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	d, err := local.New(root, local.Options{StagingDirectory: t.TempDir(), MaxStagingBytes: 1024})
	if err != nil {
		t.Fatal(err)
	}
	c := newCore(t, core.Options{Authorize: func(_ context.Context, p core.Principal, a core.Action, _ core.Address) error {
		if p == "create-only" && (a == core.RemoveAction || a == core.RemoveTreeAction) {
			return errors.New("deletion not granted")
		}
		if p == "file-user" && (a == core.StatAction || a == core.ListAction) {
			return errors.New("metadata/list not granted")
		}
		return nil
	}})
	defer closeCore(t, c)
	installCopyMount(t, c, "/files", d)
	for _, name := range []string{"source", "source/empty", "target"} {
		if err := os.Mkdir(filepath.Join(root, name), 0700); err != nil {
			t.Fatal(err)
		}
	}
	write(t, root, "source/a", []byte("new"))
	write(t, root, "target/old", []byte("old"))
	limits := core.TreeOptions{MaxEntries: 32, MaxPlanBytes: 1024}
	// An absent replacement needs no deletion grant; an existing one does.
	replacement := core.DirectoryCopyOptions{Existing: core.Replace, Depth: core.Self}
	if r, err := c.CopyTree(ctx, "create-only", core.Path("/files/source"), core.Path("/files/created"), limits, replacement); err != nil || r.Change.Effect != core.Applied {
		t.Fatal(r, err)
	}
	if r, err := c.CopyTree(ctx, "create-only", core.Path("/files/source"), core.Path("/files/created"), limits, replacement); core.CodeOf(err) != core.PermissionDenied || r.Change.Effect != core.Unchanged {
		t.Fatal(r, err)
	}
	// A file leaf keeps ordinary file authority and does not require traversal.
	r, err := c.CopyTree(ctx, "file-user", core.Path("/files/source/a"), core.Path("/files/leaf"), core.TreeOptions{}, core.DirectoryCopyOptions{WriteOptions: core.WriteOptions{Policy: core.Publish()}})
	if err != nil || r.Change.Effect != core.Applied || r.Change.Before != core.TargetMissing || len(r.Items) != 1 {
		t.Fatal(r, err)
	}
	if r, err = c.RemoveTree(ctx, "file-user", core.Path("/files/leaf"), core.TreeOptions{}); err != nil || r.Change.Effect != core.Applied {
		t.Fatal(r, err)
	}
	policy := core.DirectoryCopyOptions{Existing: core.Replace, WriteOptions: core.WriteOptions{Policy: core.Publish()}}
	if r, err = c.CopyTree(ctx, "alice", core.Path("/files/source"), core.Path("/files/target"), core.TreeOptions{MaxEntries: 1, MaxPlanBytes: 1}, policy); core.CodeOf(err) != core.CapacityExceeded || r.Change.Effect != core.Unchanged {
		t.Fatal(r, err)
	}
	if data, err := os.ReadFile(filepath.Join(root, "target/old")); err != nil || string(data) != "old" {
		t.Fatal("planning failure deleted old target", err)
	}
	if r, err = c.CopyTree(ctx, "alice", core.Path("/files/source"), core.Path("/files/target"), limits, policy); err != nil || r.Change.Effect != core.Applied || r.Change.Before != core.TargetPresent || r.Items[0].Phase != core.RemovePhase {
		t.Fatal(r, err)
	}
	if _, err := os.Stat(filepath.Join(root, "target/old")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("replace merged old membership", err)
	}
	policy.Depth = core.Self
	if r, err = c.CopyTree(ctx, "alice", core.Path("/files/source"), core.Path("/files/target"), limits, policy); err != nil || r.Change.Effect != core.Applied {
		t.Fatal(r, err)
	}
	if entries, err := os.ReadDir(filepath.Join(root, "target")); err != nil || len(entries) != 0 {
		t.Fatal("Self copied descendants", entries, err)
	}
	// Failed source EOF cannot replace an old file. The receipt retains target
	// effect and source failure provenance without a second body wrapper.
	fault := errors.New("source receipt failed")
	bad := &fakeDriver{stat: func(context.Context, string) (core.Metadata, error) {
		return core.Metadata{Name: "leaf", Kind: core.File, Size: ptr(3)}, nil
	}, open: func(context.Context, string, *core.Range) (core.Opened, error) {
		return core.Opened{Metadata: core.Metadata{Name: "leaf", Kind: core.File, Size: ptr(3)}, Length: ptr(3), Body: io.NopCloser(io.MultiReader(strings.NewReader("b"), iotest.ErrReader(fault)))}, nil
	}}
	installCopyMount(t, c, "/broken", bad)
	write(t, root, "retained", []byte("old"))
	if r, err = c.CopyTree(ctx, "alice", core.Path("/broken/leaf"), core.Path("/files/retained"), limits, policy); !errors.Is(err, fault) || r.Change.Effect != core.Unchanged || r.Items[0].ErrorSide != core.SourceSide {
		t.Fatal(r, err)
	}
	if data, err := os.ReadFile(filepath.Join(root, "retained")); err != nil || string(data) != "old" {
		t.Fatal(string(data), err)
	}
	// File/directory replacement shares Move, retaining its separate pre-delete.
	moved, err := c.Move(ctx, "alice", core.Path("/files/source"), core.Path("/files/retained"), core.MoveOptions{Existing: core.Replace, Tree: limits})
	if err != nil || moved.Source.Effect != core.Applied || moved.Removed == nil || moved.Removed.Change.Effect != core.Applied || moved.Target.Before != core.TargetPresent {
		t.Fatal(moved, err)
	}
	if _, err = c.Move(ctx, "alice", core.Path("/files/retained/a"), core.Path("/files/retained"), core.MoveOptions{Existing: core.Replace, Tree: limits}); core.CodeOf(err) != core.InvalidRequest {
		t.Fatal("replacement removed a source ancestor", err)
	}
	// A peer created after target removal must survive the final exclusive move.
	raceRoot := t.TempDir()
	write(t, raceRoot, "from", []byte("source"))
	write(t, raceRoot, "to", []byte("old"))
	native, err := local.New(raceRoot)
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	mover := &relocatingDriver{Driver: native, move: func(ctx context.Context, from, to string) (core.Change, error) {
		calls++
		if calls == 2 {
			if err := os.WriteFile(filepath.Join(raceRoot, "to"), []byte("peer"), 0600); err != nil {
				return core.Change{Effect: core.Unchanged}, err
			}
		}
		return native.Relocate(ctx, core.NativePath(from), core.NativePath(to))
	}}
	installCopyMount(t, c, "/race", &struct {
		core.Driver
		core.Relocator
		core.Remover
		core.DirectoryReader
	}{mover, mover, native, native})
	moved, err = c.Move(ctx, "alice", core.Path("/race/from"), core.Path("/race/to"), core.MoveOptions{Existing: core.Replace})
	if core.CodeOf(err) != core.AlreadyExists || moved.Source.Effect != core.Unchanged || moved.Target.Effect != core.Unchanged || moved.Removed == nil || moved.Removed.Change.Effect != core.Applied || calls != 2 {
		t.Fatal(moved, err, calls)
	}
	if data, err := os.ReadFile(filepath.Join(raceRoot, "to")); err != nil || string(data) != "peer" {
		t.Fatal("move overwrote peer", string(data), err)
	}
	if data, err := os.ReadFile(filepath.Join(raceRoot, "from")); err != nil || string(data) != "source" {
		t.Fatal(string(data), err)
	}
	if err := os.Symlink("retained/a", filepath.Join(root, "link")); err == nil {
		if r, err = c.CopyTree(ctx, "alice", core.Path("/files/link"), core.Path("/files/protected"), limits, policy); err == nil || r.Change.Effect != core.Unchanged {
			t.Fatal("leaf followed a link", r, err)
		}
	}
}

func TestAliasesAndTreeCopy(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	for _, dir := range []string{"source", "source/empty", "source/nested"} {
		if err := os.Mkdir(filepath.Join(root, filepath.FromSlash(dir)), 0700); err != nil {
			t.Fatal(err)
		}
	}
	write(t, root, "source/a", []byte("alpha"))
	write(t, root, "source/z", []byte("gamma"))
	write(t, root, "source/nested/b", []byte("beta"))
	deny, deleting, merging := false, false, false
	c := newCore(t, core.Options{Authorize: func(ctx context.Context, p core.Principal, a core.Action, targetAddress core.Address) error {
		target := targetAddress.Value()
		if merging && strings.HasPrefix(target, "/files/combined") && (a == core.StatAction || a == core.ListAction) {
			return errors.New("target confirmation cannot require reading it")
		}
		if deleting && (a == core.ReadAction || (a == core.ListAction && target == "/files")) {
			return errors.New("deletion cannot read content or list outside its target")
		}
		if deny && a == core.ReadAction && target == "/files/source/a" {
			return errors.New("primary denied")
		}
		return allow(ctx, p, a, core.Path(target))
	}})
	defer closeCore(t, c)
	storage, err := local.New(root, local.Options{StagingDirectory: t.TempDir(), MaxStagingBytes: 64, MaxConcurrentCommits: 1})
	if err != nil {
		t.Fatal(err)
	}
	m := installCopyMount(t, c, "/files", storage)
	if err := c.BindAlias(ctx, "/view", m, "/source"); err != nil {
		t.Fatal(err)
	}
	deny = true
	_, err = c.Open(ctx, "alice", core.Path("/view/a"), core.OpenOptions{})
	code(t, err, core.PermissionDenied)
	deny = false
	change, err := c.Remove(ctx, "alice", core.Path("/view"))
	if change.Effect != core.Unchanged || core.CodeOf(err) != core.InvalidRequest {
		t.Fatal(change, err)
	}
	code(t, c.BindAlias(ctx, "/view/child", m, "/"), core.PathAmbiguous)
	change, err = c.Copy(ctx, "alice", core.Path("/view/a"), core.Path("/files/source/a"))
	if change.Effect != core.Unchanged || core.CodeOf(err) != core.InvalidRequest {
		t.Fatal(change, err)
	}
	page, err := c.List(ctx, "alice", core.ListRequest{Query: core.Children(core.Path("/view")), Limit: 1, CursorTTL: time.Minute})
	if err != nil || page.NextCursor == nil {
		t.Fatal(page, err)
	}
	options := core.TreeCopyOptions{ExecutionOptions: core.ExecutionOptions{Concurrency: 2}, MaxEntries: 30, MaxPlanBytes: 1024}
	rows, err := c.CopyTree(ctx, "alice", core.Path("/view"), core.Path("/files/output"), options)
	if core.CodeOf(err) == core.UnsupportedOperation && len(rows.Items) == 0 {
		t.Skip("native no-redirect directory access unavailable")
	}
	if err != nil || len(rows.Items) != 6 {
		t.Fatal(rows, err)
	}
	for _, row := range rows.Items {
		if row.Change.Effect != core.Applied || row.Err != nil {
			t.Fatal(row)
		}
	}
	for name, want := range map[string]string{"a": "alpha", "z": "gamma", "nested/b": "beta"} {
		data, err := os.ReadFile(filepath.Join(root, "output", filepath.FromSlash(name)))
		if err != nil || string(data) != want {
			t.Fatal(name, string(data), err)
		}
	}
	// Equal child names in distinct bound parents must keep their provenance.
	if err = os.Mkdir(filepath.Join(root, "other"), 0700); err != nil {
		t.Fatal(err)
	}
	write(t, root, "other/a", []byte("another-parent"))
	if err = c.BindAlias(ctx, "/other-view", m, "/other"); err != nil {
		t.Fatal(err)
	}
	otherRows, otherErr := c.CopyTree(ctx, "alice", core.Path("/other-view"), core.Path("/files/other-output"), options)
	otherData, otherReadErr := os.ReadFile(filepath.Join(root, "other-output", "a"))
	if otherErr != nil || len(otherRows.Items) != 2 || otherReadErr != nil || string(otherData) != "another-parent" {
		t.Fatal(otherRows, otherErr, string(otherData), otherReadErr)
	}
	// An enumerated alias child still renews its primary Read grant at execution.
	deny = true
	_, deniedErr := c.CopyTree(ctx, "alice", core.Path("/view"), core.Path("/files/denied-output"), options)
	deny = false
	if core.CodeOf(deniedErr) != core.PermissionDenied {
		t.Fatalf("scoped child bypassed primary grant: %v", deniedErr)
	}
	if _, statErr := os.Stat(filepath.Join(root, "denied-output", "a")); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("denied child was copied: %v", statErr)
	}
	if info, err := os.Stat(filepath.Join(root, "output", "empty")); err != nil || !info.IsDir() {
		t.Fatal(info, err)
	}
	code(t, c.CheckCursor(ctx, "alice", core.ListRequest{Query: core.Children(core.Path("/view")), Cursor: *page.NextCursor}), core.CursorUnavailable)
	rows, err = c.CopyTree(ctx, "alice", core.Path("/view"), core.Path("/files/source/inside"), options)
	if len(rows.Items) != 0 || core.CodeOf(err) != core.InvalidRequest {
		t.Fatal(rows, err)
	}
	if err := os.MkdirAll(filepath.Join(root, "combined", "nested"), 0700); err != nil {
		t.Fatal(err)
	}
	write(t, root, "combined/keep", []byte("retained"))
	write(t, root, "combined/nested/old", []byte("retained"))
	rows, err = c.CopyTree(ctx, "alice", core.Path("/view"), core.Path("/files/combined"), options)
	if core.CodeOf(err) != core.AlreadyExists || len(rows.Items) != 1 || rows.Items[0].Change.Effect != core.Unchanged {
		t.Fatal("default tree copy merged a directory", rows, err)
	}
	merge := core.DirectoryCopyOptions{Existing: core.Merge}
	rows, err = c.CopyTree(ctx, "alice", core.Path("/view"), core.Path("/files/combined"), options, merge, merge)
	if core.CodeOf(err) != core.InvalidRequest || len(rows.Items) != 0 {
		t.Fatal("multiple directory policies were accepted", rows, err)
	}
	merging = true
	rows, err = c.CopyTree(ctx, "alice", core.Path("/view"), core.Path("/files/combined"), options, merge)
	merging = false
	if err != nil || len(rows.Items) != 6 {
		t.Fatal(rows, err)
	}
	for _, row := range rows.Items {
		want := core.Applied
		if row.RelativePath == "/" || row.RelativePath == "/nested" {
			want = core.Unchanged
		}
		if row.Change.Effect != want || row.Err != nil {
			t.Fatal("merged and created directories lost their distinct receipts", row)
		}
	}
	for name, want := range map[string]string{"keep": "retained", "nested/old": "retained", "a": "alpha", "z": "gamma", "nested/b": "beta"} {
		if data, err := os.ReadFile(filepath.Join(root, "combined", filepath.FromSlash(name))); err != nil || string(data) != want {
			t.Fatal(name, string(data), err)
		}
	}
	change, err = c.Mkdir(ctx, "alice", core.Path("/files/combined"))
	if change.Effect != core.Unchanged || core.CodeOf(err) != core.AlreadyExists {
		t.Fatal("tree policy changed ordinary Mkdir", change, err)
	}
	publish := core.DirectoryCopyOptions{WriteOptions: core.WriteOptions{Policy: core.Publish()}}
	rows, err = c.CopyTree(ctx, "alice", core.Path("/view"), core.Path("/files/combined"), options, publish)
	if core.CodeOf(err) != core.AlreadyExists || len(rows.Items) != 1 || rows.Items[0].Change.Effect != core.Unchanged {
		t.Fatal("file publication implicitly merged directories", rows, err)
	}
	rows, err = c.CopyTree(ctx, "alice", core.Path("/view"), core.Path("/files/published"), options, publish)
	if err != nil || len(rows.Items) != 6 {
		t.Fatal("publication could not create a fresh tree", rows, err)
	}
	for _, row := range rows.Items {
		if row.Change.Effect != core.Applied || row.Err != nil {
			t.Fatal(row)
		}
	}
	write(t, root, "combined/a", []byte("stale"))
	if err := os.Remove(filepath.Join(root, "combined", "z")); err != nil {
		t.Fatal(err)
	}
	publish.Existing = core.Merge
	rows, err = c.CopyTree(ctx, "alice", core.Path("/view"), core.Path("/files/combined"), options, publish)
	if err != nil || len(rows.Items) != 6 {
		t.Fatal(rows, err)
	}
	for _, row := range rows.Items {
		want := core.Applied
		if row.RelativePath == "/" || row.RelativePath == "/empty" || row.RelativePath == "/nested" {
			want = core.Unchanged
		}
		if row.Change.Effect != want || row.Err != nil {
			t.Fatal("directory and file policies interfered", row)
		}
	}
	for name, want := range map[string]string{"keep": "retained", "nested/old": "retained", "a": "alpha", "z": "gamma", "nested/b": "beta"} {
		if data, err := os.ReadFile(filepath.Join(root, "combined", filepath.FromSlash(name))); err != nil || string(data) != want {
			t.Fatal(name, string(data), err)
		}
	}
	options.MaxEntries = 1
	rows, err = c.CopyTree(ctx, "alice", core.Path("/view"), core.Path("/files/budget"), options)
	if len(rows.Items) != 0 || core.CodeOf(err) != core.CapacityExceeded {
		t.Fatal(rows, err)
	}
	if _, err := os.Stat(filepath.Join(root, "budget")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}

	// Delete the copy while an already opened body remains borrowed.
	body, err := c.Open(ctx, "alice", core.Path("/files/output/a"), core.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer body.Body.Close()
	deleting = true
	for _, target := range []string{"/files", "/view"} {
		removed, err := c.RemoveTree(ctx, "alice", core.Path(target), core.TreeOptions{MaxEntries: 30, MaxPlanBytes: 1024})
		if len(removed.Items) != 0 || core.CodeOf(err) != core.InvalidRequest {
			t.Fatal(target, removed, err)
		}
	}
	removed, err := c.RemoveTree(ctx, "alice", core.Path("/files/output"), core.TreeOptions{MaxEntries: 1, MaxPlanBytes: 1024})
	if len(removed.Items) != 0 || core.CodeOf(err) != core.CapacityExceeded {
		t.Fatal(removed, err)
	}
	if _, err := os.Stat(filepath.Join(root, "output", "a")); err != nil {
		t.Fatal("discovery failure deleted content", err)
	}
	removed, err = c.RemoveTree(ctx, "alice", core.Path("/files/output"), core.TreeOptions{
		ExecutionOptions: core.ExecutionOptions{Concurrency: 2}, MaxEntries: 30, MaxPlanBytes: 1024,
	})
	deleting = false
	if err != nil || len(removed.Items) != 6 || removed.Items[len(removed.Items)-1].RelativePath != "/" {
		t.Fatal(removed, err)
	}
	for _, row := range removed.Items {
		if row.Change.Effect != core.Applied || row.Err != nil {
			t.Fatal(row)
		}
	}
	if _, err := os.Stat(filepath.Join(root, "output")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("tree name survived confirmed deletion", err)
	}
	payload, err := io.ReadAll(body.Body)
	if err != nil || string(payload) != "alpha" {
		t.Fatal("unlink invalidated a borrowed body", string(payload), err)
	}

	opened, err := c.Open(ctx, "alice", core.Path("/view/a"), core.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer opened.Body.Close()
	page, err = c.List(ctx, "alice", core.ListRequest{Query: core.Children(core.Path("/view")), Limit: 1, CursorTTL: time.Minute})
	if err != nil || page.NextCursor == nil {
		t.Fatal(page, err)
	}
	if err := c.UnbindAlias(ctx, "/view"); err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(opened.Body)
	if err != nil || string(data) != "alpha" {
		t.Fatal(string(data), err)
	}
	code(t, c.CheckCursor(ctx, "alice", core.ListRequest{Query: core.Children(core.Path("/view")), Cursor: *page.NextCursor}), core.CursorUnavailable)
	if err := c.BindAlias(ctx, "/view", m, "/source"); err != nil {
		t.Fatal(err)
	}
	code(t, c.CheckCursor(ctx, "alice", core.ListRequest{Query: core.Children(core.Path("/view")), Cursor: *page.NextCursor}), core.CursorUnavailable)
	if err := m.Retire(ctx); err != nil {
		t.Fatal(err)
	}
	_, err = c.Stat(ctx, "alice", core.Path("/view"))
	code(t, err, core.NotFound)
}

type treeWriter struct {
	*copyWriter
	core.DirectoryMaker
}

func TestTreeCopyRefusalAndPartialEffects(t *testing.T) {
	from, to := t.TempDir(), t.TempDir()
	write(t, from, "a", []byte("a"))
	write(t, from, "b", []byte("b"))
	c := newCore(t, core.Options{})
	defer closeCore(t, c)
	source, err := local.New(from)
	if err != nil {
		t.Fatal(err)
	}
	installCopyMount(t, c, "/from", source)
	target, err := local.New(to)
	if err != nil {
		t.Fatal(err)
	}
	fault := errors.New("target acknowledged with a completion fault")
	calls := 0
	writer := &treeWriter{DirectoryMaker: target, copyWriter: &copyWriter{Driver: target, create: func(ctx context.Context, name string, input io.Reader, length *int64) (core.Change, error) {
		calls++
		result, err := target.Create(ctx, core.NativePath(name), input, length)
		return result, errors.Join(err, fault)
	}}}
	installCopyMount(t, c, "/to", &struct {
		core.Driver
		core.DirectoryMaker
		core.FileCreator
	}{writer, writer, writer})
	options := core.TreeCopyOptions{MaxEntries: 30, MaxPlanBytes: 1024}
	rows, err := c.CopyTree(context.Background(), "alice", core.Path("/from"), core.Path("/to/output"), options, core.DirectoryCopyOptions{WriteOptions: core.WriteOptions{Policy: core.Publish()}})
	if len(rows.Items) != 0 || core.CodeOf(err) != core.UnsupportedOperation || calls != 0 {
		t.Fatal("missing publication capability changed the target", rows, err, calls)
	}
	rows, err = c.CopyTree(context.Background(), "alice", core.Path("/from"), core.Path("/to/output"), options, core.DirectoryCopyOptions{Existing: core.Merge})
	if len(rows.Items) != 0 || core.CodeOf(err) != core.UnsupportedOperation || calls != 0 {
		t.Fatal("missing target confirmation capability changed the target", rows, err, calls)
	}
	rows, err = c.CopyTree(context.Background(), "alice", core.Path("/from"), core.Path("/to/output"), options)
	if core.CodeOf(err) == core.UnsupportedOperation && len(rows.Items) == 0 {
		t.Skip("native no-redirect directory access unavailable")
	}
	if len(rows.Items) != 2 || calls != 1 || !errors.Is(err, fault) ||
		rows.Items[0].Change.Effect != core.Applied || rows.Items[0].Err != nil ||
		rows.Items[1].Change.Effect != core.Applied || !errors.Is(rows.Items[1].Err, fault) {
		t.Fatal(rows, err, calls)
	}
	copied := filepath.Join(to, "output", strings.TrimPrefix(rows.Items[1].RelativePath, "/"))
	if _, err := os.Stat(copied); err != nil {
		t.Fatal("confirmed partial target lost", err)
	}
	if err := os.Symlink("a", filepath.Join(from, "link")); err != nil {
		t.Skipf("symlink creation unavailable: %v", err)
	}
	rows, err = c.CopyTree(context.Background(), "alice", core.Path("/from"), core.Path("/to/refused"), options)
	if len(rows.Items) != 0 || err == nil {
		t.Fatal(rows, err)
	}
	if _, err := os.Stat(filepath.Join(to, "refused")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("discovery refusal changed target", err)
	}
	if err := os.Remove(filepath.Join(from, "link")); err != nil {
		t.Fatal(err)
	}
	scope, err := source.OpenDirectory(context.Background(), core.NativePath("/"))
	if err != nil {
		t.Fatal(err)
	}
	defer scope.Close()
	for {
		_, err = scope.Next(context.Background(), core.MaxPage)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	write(t, from, "a", []byte("new length after enumeration"))
	fresh, err := scope.OpenFile(context.Background(), "a")
	if err != nil {
		t.Fatal(err)
	}
	data, readErr := io.ReadAll(fresh.Body)
	closeErr := fresh.Body.Close()
	if readErr != nil || closeErr != nil || fresh.Length == nil || *fresh.Length != int64(len(data)) ||
		string(data) != "new length after enumeration" || fresh.Metadata.Size == nil || *fresh.Metadata.Size != *fresh.Length {
		t.Fatal("opened metadata reused stale discovery state", fresh, string(data), readErr, closeErr)
	}
	if err := os.Rename(filepath.Join(from, "a"), filepath.Join(from, "retained")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("b", filepath.Join(from, "a")); err != nil {
		t.Fatal(err)
	}
	opened, err := scope.OpenFile(context.Background(), "a")
	if opened.Body != nil {
		_ = opened.Body.Close()
	}
	if err == nil {
		t.Fatal("a link installed after enumeration was followed")
	}
}

type removalReader struct {
	core.Driver
	open func(context.Context, string) (core.DirectoryScope, error)
}

func (d *removalReader) OpenDirectory(ctx context.Context, nameAddress core.NativeAddress) (core.DirectoryScope, error) {
	if nameAddress.Namespace() != core.TreeNamespace {
		return nil, core.Fail(core.UnsupportedOperation, nil)
	}
	name := nameAddress.Value()

	return d.open(ctx, name)
}

type removalScope struct {
	core.DirectoryScope
	remove   func(context.Context, string, core.Kind) (core.Change, error)
	closeErr error
}

func (s *removalScope) RemoveChild(ctx context.Context, name string, kind core.Kind) (core.Change, error) {
	if s.remove != nil {
		return s.remove(ctx, name, kind)
	}
	return s.DirectoryScope.(core.ChildRemover).RemoveChild(ctx, name, kind)
}

func (s *removalScope) Close() error {
	return errors.Join(s.DirectoryScope.Close(), s.closeErr)
}

func TestTreeRemovalBoundaries(t *testing.T) {
	for _, replacement := range []string{"file", "link"} {
		t.Run("name-intent-"+replacement, func(t *testing.T) {
			root := t.TempDir()
			if err := os.Mkdir(filepath.Join(root, "tree"), 0700); err != nil {
				t.Fatal(err)
			}
			write(t, root, "tree/a", []byte("old"))
			write(t, root, "tree/b", []byte("b"))
			if replacement == "link" {
				if err := os.Symlink("tree/b", filepath.Join(root, "probe")); err != nil {
					t.Skipf("symlink unavailable: %v", err)
				}
				if err := os.Remove(filepath.Join(root, "probe")); err != nil {
					t.Fatal(err)
				}
			}
			storage, err := local.New(root)
			if err != nil {
				t.Fatal(err)
			}
			c := newCore(t, core.Options{})
			defer closeCore(t, c)
			opens := 0
			installCopyMount(t, c, "/files", &removalReader{Driver: storage, open: func(ctx context.Context, name string) (core.DirectoryScope, error) {
				if name == "/tree" {
					opens++
					if opens == 2 { // Execution, after discovery has closed.
						if err := os.Rename(filepath.Join(root, "tree", "a"), filepath.Join(root, "retained")); err != nil {
							return nil, err
						}
						if replacement == "link" {
							if err := os.Symlink("../retained", filepath.Join(root, "tree", "a")); err != nil {
								return nil, err
							}
						} else {
							write(t, root, "tree/a", []byte("replacement"))
						}
						write(t, root, "tree/new", []byte("new"))
					}
				}
				return storage.OpenDirectory(ctx, core.NativePath(name))
			}})
			rows, err := c.RemoveTree(context.Background(), "alice", core.Path("/files/tree"), core.TreeOptions{MaxEntries: 10, MaxPlanBytes: 128})
			if core.CodeOf(err) != core.DirectoryNotEmpty || opens != 2 || len(rows.Items) != 3 || rows.Items[2].RelativePath != "/" {
				t.Fatal(rows, err, opens)
			}
			for i, row := range rows.Items {
				want := core.Applied
				if i == 2 {
					want = core.Unchanged
				}
				if row.Change.Effect != want {
					t.Fatal(row)
				}
			}
			for name, want := range map[string]string{"retained": "old", "tree/new": "new"} {
				data, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(name)))
				if err != nil || string(data) != want {
					t.Fatal(name, string(data), err)
				}
			}
			if _, err := os.Lstat(filepath.Join(root, "tree", "a")); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("selected name survived", err)
			}
		})
	}
	t.Run("ancestor-link", func(t *testing.T) {
		root := t.TempDir()
		for _, name := range []string{"tree/nested", "other"} {
			if err := os.MkdirAll(filepath.Join(root, filepath.FromSlash(name)), 0700); err != nil {
				t.Fatal(err)
			}
		}
		write(t, root, "tree/nested/a", []byte("selected"))
		write(t, root, "other/a", []byte("outside"))
		if err := os.Symlink("../other", filepath.Join(root, "tree", "link")); err != nil {
			t.Skipf("symlink unavailable: %v", err)
		}
		storage, err := local.New(root)
		if err != nil {
			t.Fatal(err)
		}
		c := newCore(t, core.Options{})
		defer closeCore(t, c)
		armed, opens := false, 0
		installCopyMount(t, c, "/files", &removalReader{Driver: storage, open: func(ctx context.Context, name string) (core.DirectoryScope, error) {
			if armed && name == "/tree/nested" {
				opens++
				if opens == 2 {
					if err := os.Rename(filepath.Join(root, "tree", "nested"), filepath.Join(root, "retained")); err != nil {
						return nil, err
					}
					if err := os.Symlink("../other", filepath.Join(root, "tree", "nested")); err != nil {
						return nil, err
					}
				}
			}
			return storage.OpenDirectory(ctx, core.NativePath(name))
		}})
		options := core.TreeOptions{MaxEntries: 10, MaxPlanBytes: 128}
		if rows, err := c.RemoveTree(context.Background(), "alice", core.Path("/files/tree"), options); len(rows.Items) != 0 || err == nil {
			t.Fatal("known link admitted", rows, err)
		}
		if err := os.Remove(filepath.Join(root, "tree", "link")); err != nil {
			t.Fatal(err)
		}
		armed = true
		if rows, err := c.RemoveTree(context.Background(), "alice", core.Path("/files/tree"), options); len(rows.Items) != 1 || rows.Items[0].Phase != core.ReadPhase || rows.Change.Effect != core.Unchanged || err == nil || opens != 2 {
			t.Fatal("ancestor redirection admitted", rows, err, opens)
		}
		for name, want := range map[string]string{"retained/a": "selected", "other/a": "outside"} {
			data, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(name)))
			if err != nil || string(data) != want {
				t.Fatal(name, string(data), err)
			}
		}
	})
}

func TestSubtreeRemovalAuthorityAndFallback(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "tree", "nested"), 0700); err != nil {
		t.Fatal(err)
	}
	write(t, root, "tree/nested/a", []byte("selected"))
	write(t, root, "outside", []byte("retained"))
	whole := false
	c := newCore(t, core.Options{Authorize: func(ctx context.Context, p core.Principal, a core.Action, nameAddress core.Address) error {
		name := nameAddress.Value()
		if p == "alice" && (name == "/view/tree" || name == "/files/tree" || name == "/files" || name == "/view") {
			if a == core.RemoveAction || (whole && a == core.RemoveTreeAction) {
				return nil
			}
		}
		return errors.New("no child/list grant")
	}})
	defer closeCore(t, c)
	d, err := local.New(root)
	if err != nil {
		t.Fatal(err)
	}
	m := installCopyMount(t, c, "/files", d)
	if err := c.BindAlias(ctx, "/view", m, "/"); err != nil {
		t.Fatal(err)
	}
	limits := core.TreeOptions{ExecutionOptions: core.ExecutionOptions{Concurrency: 2}, MaxEntries: 10, MaxPlanBytes: 128}
	policy := core.DirectoryRemoveOptions{Scope: core.Subtree}
	if rows, err := c.RemoveTree(ctx, "alice", core.Path("/view/tree"), limits); len(rows.Items) != 0 || core.CodeOf(err) != core.PermissionDenied {
		t.Fatal(rows, err)
	}
	if rows, err := c.RemoveTree(ctx, "alice", core.Path("/view/tree"), limits, policy); len(rows.Items) != 0 || core.CodeOf(err) != core.PermissionDenied {
		t.Fatal(rows, err)
	}
	if _, err := os.Stat(filepath.Join(root, "tree", "nested", "a")); err != nil {
		t.Fatal(err)
	}
	whole = true
	if rows, err := c.RemoveTree(ctx, "alice", core.Path("/view/tree"), limits, policy); err != nil || len(rows.Items) != 3 || rows.Items[2].Change.Effect != core.Applied {
		t.Fatal(rows, err)
	}
	if _, err := os.Stat(filepath.Join(root, "tree")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}
	for _, target := range []string{"/files", "/view"} {
		if _, err := c.RemoveTree(ctx, "alice", core.Path(target), limits, policy); core.CodeOf(err) != core.InvalidRequest {
			t.Fatal(target, err)
		}
	}
	if _, err := c.RemoveTree(ctx, "alice", core.Path("/files/outside"), limits, core.DirectoryRemoveOptions{Scope: "auto"}); core.CodeOf(err) != core.InvalidRequest {
		t.Fatal(err)
	}
	if data, err := os.ReadFile(filepath.Join(root, "outside")); err != nil || string(data) != "retained" {
		t.Fatal(string(data), err)
	}
}

func TestTreeRemovalEffectsAndCleanup(t *testing.T) {
	for _, mode := range []string{"cancel", "retire", "close-fault", "prepare", "read-only", "permission", "rebind"} {
		t.Run(mode, func(t *testing.T) {
			root := t.TempDir()
			if err := os.Mkdir(filepath.Join(root, "tree"), 0700); err != nil {
				t.Fatal(err)
			}
			names := []string{"a", "b"}
			concurrent := mode == "cancel" || mode == "retire" || mode == "close-fault" || mode == "prepare"
			if concurrent {
				names = []string{"left/a", "right/b"}
				for _, name := range []string{"left", "right"} {
					if err := os.Mkdir(filepath.Join(root, "tree", name), 0700); err != nil {
						t.Fatal(err)
					}
				}
			}
			for _, name := range names {
				write(t, root, "tree/"+name, []byte(name))
			}
			storage, err := local.New(root)
			if err != nil {
				t.Fatal(err)
			}
			fault := errors.New("directory preparation or close not acknowledged")
			var c *core.Core
			var m *core.Mount
			c = newCore(t, core.Options{Authorize: func(ctx context.Context, p core.Principal, a core.Action, targetAddress core.Address) error {
				target := targetAddress.Value()
				if mode == "permission" && a == core.RemoveAction && target == "/files/tree/b" {
					return errors.New("child removal denied")
				}
				if mode == "rebind" && a == core.RemoveAction && target == "/files/tree/a" {
					if err := c.UnbindAlias(ctx, "/view"); err != nil {
						return err
					}
					return c.BindAlias(ctx, "/view", m, "/")
				}
				return allow(ctx, p, a, core.Path(target))
			}})
			if mode == "close-fault" {
				defer storage.Close() // The injected failure deliberately retains Core ownership.
			} else {
				defer closeCore(t, c)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			acknowledged := make(chan string, 2)
			release := make(chan struct{}, 2)
			defer close(release)
			opens, prepared := make(map[string]int), 0
			var discovered []string
			m = installCopyMount(t, c, "/files", &removalReader{Driver: storage, open: func(ctx context.Context, name string) (core.DirectoryScope, error) {
				scope, err := storage.OpenDirectory(ctx, core.NativePath(name))
				if err != nil {
					return scope, err
				}
				if mode == "read-only" {
					return struct{ core.DirectoryScope }{scope}, nil
				}
				if name != "/tree/left" && name != "/tree/right" {
					return scope, nil
				}
				opens[name]++
				if opens[name] == 1 {
					discovered = append(discovered, strings.TrimPrefix(name, "/tree"))
				}
				wrapped := &removalScope{DirectoryScope: scope}
				if opens[name] == 2 {
					prepared++
					switch mode {
					case "prepare":
						if prepared == 2 {
							return wrapped, fault
						}
					case "close-fault":
						wrapped.closeErr = fault
					case "cancel", "retire":
						parent := name
						wrapped.remove = func(ctx context.Context, name string, kind core.Kind) (core.Change, error) {
							change, err := scope.(core.ChildRemover).RemoveChild(ctx, name, kind)
							acknowledged <- parent + "/" + name
							<-ctx.Done()
							<-release
							return change, errors.Join(err, ctx.Err())
						}
					}
				}
				return wrapped, nil
			}})
			target := "/files/tree"
			if mode == "rebind" {
				if err := c.BindAlias(ctx, "/view", m, "/"); err != nil {
					t.Fatal(err)
				}
				target = "/view/tree"
			}
			type outcome struct {
				rows core.TreeResult
				err  error
			}
			done := make(chan outcome, 1)
			go func() {
				rows, err := c.RemoveTree(ctx, "alice", core.Path(target), core.TreeOptions{
					ExecutionOptions: core.ExecutionOptions{Concurrency: 2}, MaxEntries: 10, MaxPlanBytes: 128,
				})
				done <- outcome{rows, err}
			}()
			if mode == "cancel" || mode == "retire" {
				started := make(map[string]bool)
				for i := 0; i < 2; i++ {
					select {
					case name := <-acknowledged:
						started[name] = true
					case <-time.After(3 * time.Second):
						t.Fatal("independent parents did not overlap")
					}
				}
				if !started["/tree/left/a"] || !started["/tree/right/b"] {
					t.Fatal("unexpected concurrent targets", started)
				}
				if mode == "cancel" {
					cancel()
				}
				wait, stop := context.WithTimeout(context.Background(), 10*time.Millisecond)
				err := m.Retire(wait)
				stop()
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatal("retirement acknowledged unfinished operations", err)
				}
				if mode == "retire" && ctx.Err() != nil {
					t.Fatal("retirement canceled the caller context")
				}
				release <- struct{}{}
				release <- struct{}{}
			}
			var got outcome
			select {
			case got = <-done:
			case <-time.After(3 * time.Second):
				t.Fatal("tree deletion did not finish")
			}
			if mode == "read-only" || mode == "permission" || mode == "rebind" || mode == "prepare" {
				want := core.UnsupportedOperation
				switch mode {
				case "permission":
					want = core.PermissionDenied
				case "rebind":
					want = core.MountUnavailable
				case "prepare":
					want = core.CodeOf(fault)
				}
				wantRows := 0
				if mode == "prepare" {
					wantRows = 1
				}
				if len(got.rows.Items) != wantRows || core.CodeOf(got.err) != want || (mode == "prepare" && (!errors.Is(got.err, fault) || got.rows.Items[0].Phase != core.ReadPhase)) {
					t.Fatal(got)
				}
				for _, name := range names {
					if _, err := os.Stat(filepath.Join(root, "tree", filepath.FromSlash(name))); err != nil {
						t.Fatal("preparation failure changed planned entries", err)
					}
				}
				return
			}
			wantRows := 2
			if mode == "close-fault" {
				wantRows = 4
			}
			if len(got.rows.Items) != wantRows || got.rows.Items[0].RelativePath == got.rows.Items[1].RelativePath {
				t.Fatal(got)
			}
			for i, row := range got.rows.Items[:2] {
				if filepath.ToSlash(filepath.Dir(row.RelativePath)) != discovered[i] {
					t.Fatal("concurrent receipts changed discovery order", got.rows, discovered)
				}
				if row.Change.Effect != core.Applied ||
					((mode == "cancel" || mode == "retire") && !errors.Is(row.Err, context.Canceled)) ||
					(mode == "close-fault" && row.Err != nil) {
					t.Fatal(row)
				}
			}
			if mode == "close-fault" {
				for _, row := range got.rows.Items[2:] {
					if row.Phase != core.ReadPhase || row.Change.Effect != core.Unchanged || !errors.Is(row.Err, fault) {
						t.Fatal(row)
					}
				}
			}
			if info, err := os.Stat(filepath.Join(root, "tree")); err != nil || !info.IsDir() {
				t.Fatal("unstarted root removal took effect", info, err)
			}
			if mode == "cancel" || mode == "retire" {
				if !errors.Is(got.err, context.Canceled) {
					t.Fatal(got)
				}
			} else {
				if !errors.Is(got.err, fault) {
					t.Fatal(got)
				}
				wait, stop := context.WithTimeout(context.Background(), 10*time.Millisecond)
				defer stop()
				if err := c.Close(wait); !errors.Is(err, fault) || !errors.Is(err, context.DeadlineExceeded) {
					t.Fatal("failed cleanup was released", err)
				}
			}
		})
	}
}

func TestTreeMergeBoundaries(t *testing.T) {
	for _, mode := range []string{"directory-type", "file-type", "file-conflict", "link", "cancel", "confirm-cancel", "joined-error", "close-error"} {
		t.Run(mode, func(t *testing.T) {
			from, to := t.TempDir(), t.TempDir()
			for _, name := range []string{filepath.Join(from, "folder"), filepath.Join(to, "output")} {
				if err := os.Mkdir(name, 0700); err != nil {
					t.Fatal(err)
				}
			}
			for _, name := range []string{"a", "b", "folder/child"} {
				write(t, from, name, []byte(name))
			}
			write(t, to, "output/keep", []byte("retained"))
			outside := t.TempDir()
			switch mode {
			case "directory-type":
				write(t, to, "output/folder", []byte("retained"))
			case "file-type", "file-conflict":
				for _, name := range []string{"a", "b"} {
					if mode == "file-type" {
						if err := os.Mkdir(filepath.Join(to, "output", name), 0700); err != nil {
							t.Fatal(err)
						}
					} else {
						write(t, to, "output/"+name, []byte("retained"))
					}
				}
			case "link":
				if err := os.Symlink(outside, filepath.Join(to, "output", "folder")); err != nil {
					t.Skipf("symlink creation unavailable: %v", err)
				}
			}
			c := newCore(t, core.Options{Authorize: func(ctx context.Context, p core.Principal, action core.Action, targetAddress core.Address) error {
				target := targetAddress.Value()
				if strings.HasPrefix(target, "/to/") && (action == core.ListAction || action == core.StatAction) {
					return errors.New("target confirmation must use create permission")
				}
				return allow(ctx, p, action, core.Path(target))
			}})
			retained := false
			defer func() {
				if !retained {
					closeCore(t, c)
				}
			}()
			source, err := local.New(from)
			if err != nil {
				t.Fatal(err)
			}
			installCopyMount(t, c, "/from", source)
			target, err := local.New(to)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if retained {
					_ = target.Close() // The injected failure retains its Core use.
				}
			}()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			fault := errors.New("directory confirmation or creation cleanup failed")
			calls, confirmations := 0, 0
			writer := &treeWriter{DirectoryMaker: target, copyWriter: &copyWriter{Driver: target, create: func(ctx context.Context, name string, input io.Reader, length *int64) (core.Change, error) {
				calls++
				change, err := target.Create(ctx, core.NativePath(name), input, length)
				if mode == "cancel" {
					cancel()
				}
				return change, err
			}}}
			if mode == "joined-error" {
				writer.DirectoryMaker = &directoryDriver{fakeDriver: &fakeDriver{}, mkdir: func(ctx context.Context, name string) (core.Change, error) {
					change, err := target.Mkdir(ctx, core.NativePath(name))
					return change, errors.Join(err, fault)
				}}
			}
			installCopyMount(t, c, "/to", &struct {
				*treeWriter
				core.DirectoryReader
			}{writer, &removalReader{Driver: target, open: func(ctx context.Context, name string) (core.DirectoryScope, error) {
				confirmations++
				scope, err := target.OpenDirectory(ctx, core.NativePath(name))
				if mode == "confirm-cancel" {
					cancel()
				}
				if scope != nil && mode == "close-error" {
					retained = true
					return &removalScope{DirectoryScope: scope, closeErr: fault}, err
				}
				return scope, err
			}}})
			rows, err := c.CopyTree(ctx, "alice", core.Path("/from"), core.Path("/to/output"), core.TreeOptions{MaxEntries: 10, MaxPlanBytes: 128}, core.DirectoryCopyOptions{Existing: core.Merge})
			if core.CodeOf(err) == core.UnsupportedOperation && len(rows.Items) == 0 {
				t.Skip("native no-redirect directory access unavailable")
			}
			if err == nil || len(rows.Items) == 0 || rows.Items[len(rows.Items)-1].Err == nil {
				t.Fatal("merge hid a failure", rows, err)
			}
			last := rows.Items[len(rows.Items)-1]
			switch mode {
			case "directory-type", "link":
				if len(rows.Items) != 2 || calls != 0 || last.Change.Effect != core.Unchanged || last.RelativePath != "/folder" ||
					(mode == "directory-type" && core.CodeOf(err) != core.NotDirectory) {
					t.Fatal(rows, err, calls)
				}
			case "file-type", "file-conflict", "cancel":
				if len(rows.Items) != 3 || calls != 1 || rows.Items[0].Change.Effect != core.Unchanged || rows.Items[0].Err != nil ||
					rows.Items[1].Change.Effect != core.Applied || rows.Items[1].Err != nil {
					t.Fatal("partial directory effects changed", rows, err, calls)
				}
				if mode == "cancel" {
					if last.Change.Effect != core.Applied || !errors.Is(last.Err, context.Canceled) || !errors.Is(err, context.Canceled) {
						t.Fatal("cancellation erased a confirmed file", rows, err)
					}
				} else if last.Change.Effect != core.Unchanged || core.CodeOf(err) != core.AlreadyExists {
					t.Fatal("a file collision was accepted", rows, err)
				}
			case "confirm-cancel":
				if len(rows.Items) != 1 || last.Change.Effect != core.Unchanged || calls != 0 || !errors.Is(err, context.Canceled) {
					t.Fatal("directory confirmation swallowed cancellation", rows, err, calls)
				}
			case "joined-error", "close-error":
				if len(rows.Items) != 1 || last.Change.Effect != core.Unchanged || calls != 0 || !errors.Is(err, fault) ||
					(mode == "joined-error" && confirmations != 0) {
					t.Fatal("an additional failure was swallowed", rows, err, confirmations)
				}
			}
			if data, err := os.ReadFile(filepath.Join(to, "output", "keep")); err != nil || string(data) != "retained" {
				t.Fatal("extra target content changed", string(data), err)
			}
			for _, name := range []string{"a", "b", "folder/child"} {
				if mode == "file-type" && name != "folder/child" {
					if info, err := os.Stat(filepath.Join(to, "output", name)); err != nil || !info.IsDir() {
						t.Fatal("target directory was replaced", info, err)
					}
					continue
				}
				data, readErr := os.ReadFile(filepath.Join(to, "output", filepath.FromSlash(name)))
				switch {
				case mode == "file-conflict" && name != "folder/child":
					if readErr != nil || string(data) != "retained" {
						t.Fatal("existing file was replaced", string(data), readErr)
					}
				case mode == "cancel" && last.RelativePath == "/"+name:
					if readErr != nil || string(data) != name {
						t.Fatal("confirmed file was lost", string(data), readErr)
					}
				default:
					if readErr == nil {
						t.Fatal("unstarted copy took effect", name)
					}
				}
			}
			if entries, err := os.ReadDir(outside); err != nil || len(entries) != 0 {
				t.Fatal("merge traversed a target link", entries, err)
			}
			if mode == "close-error" {
				wait, stop := context.WithTimeout(context.Background(), 10*time.Millisecond)
				defer stop()
				if err := c.Close(wait); !errors.Is(err, fault) || !errors.Is(err, context.DeadlineExceeded) {
					t.Fatal("failed confirmation cleanup was released", err)
				}
			}
		})
	}
}

func TestTreePublicationEffects(t *testing.T) {
	if runtime.GOOS != "windows" && runtime.GOOS != "linux" {
		t.Skip("native publication is unavailable on this platform")
	}
	for _, mode := range []string{"permission", "input", "quota", "cancel", "post-commit"} {
		t.Run(mode, func(t *testing.T) {
			from, to, staging := t.TempDir(), t.TempDir(), t.TempDir()
			for _, directory := range []string{filepath.Join(from, "empty"), filepath.Join(to, "output")} {
				if err := os.Mkdir(directory, 0700); err != nil {
					t.Fatal(err)
				}
			}
			for _, name := range []string{"a", "b"} {
				write(t, from, name, []byte("new"))
				write(t, to, "output/"+name, []byte("old"))
			}
			c := newCore(t, core.Options{Authorize: func(ctx context.Context, p core.Principal, action core.Action, nameAddress core.Address) error {
				name := nameAddress.Value()
				if mode == "permission" && action == core.WriteAction {
					return errors.New("create permission does not grant publication")
				}
				return allow(ctx, p, action, core.Path(name))
			}})
			defer closeCore(t, c)
			source, err := local.New(from)
			if err != nil {
				t.Fatal(err)
			}
			installCopyMount(t, c, "/from", source)
			budget := int64(6)
			if mode == "quota" {
				budget = 1
			}
			target, err := local.New(to, local.Options{StagingDirectory: staging, MaxStagingBytes: budget, MaxConcurrentCommits: 1})
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			fault := errors.New("input or confirmed-publication failure")
			calls := 0
			var probe *copyBody
			publisher := &copyWriter{Driver: target, publish: func(ctx context.Context, name string, input io.Reader, length *int64) (core.Change, error) {
				calls++
				if mode == "input" && calls == 2 {
					input = io.MultiReader(input, iotest.ErrReader(fault))
				}
				probe = &copyBody{Reader: input}
				change, err := target.Publish(ctx, core.NativePath(name), probe, length, core.PublicationOptions{})
				if mode == "cancel" {
					cancel()
				}
				if mode == "post-commit" {
					err = errors.Join(err, fault)
				}
				return change, err
			}}
			// Expose publication without FileCreator: mode selection must not
			// depend on the native implementation's other optional capabilities.
			installCopyMount(t, c, "/to", &struct {
				core.Driver
				core.DirectoryMaker
				core.DirectoryReader
				core.FilePublisher
			}{target, target, target, publisher})
			limits := core.TreeOptions{MaxEntries: 10, MaxPlanBytes: 128}
			rows, err := c.CopyTree(ctx, "alice", core.Path("/from"), core.Path("/to/default"), limits)
			if len(rows.Items) != 0 || core.CodeOf(err) != core.UnsupportedOperation || calls != 0 {
				t.Fatal("default mode silently used publication", rows, err, calls)
			}
			rows, err = c.CopyTree(ctx, "alice", core.Path("/from"), core.Path("/to/output"), limits, core.DirectoryCopyOptions{
				Existing: core.Merge, WriteOptions: core.WriteOptions{Policy: core.Publish()},
			})
			if err == nil || len(rows.Items) < 3 || rows.Items[0].RelativePath != "/" || rows.Items[0].Change.Effect != core.Unchanged ||
				rows.Items[1].RelativePath != "/empty" || rows.Items[1].Change.Effect != core.Applied {
				t.Fatal("earlier directory receipts were lost", rows, err)
			}
			last := rows.Items[len(rows.Items)-1]
			switch mode {
			case "permission", "quota":
				want, wantCalls := core.PermissionDenied, 0
				if mode == "quota" {
					want, wantCalls = core.CapacityExceeded, 1
				}
				if len(rows.Items) != 3 || last.Change.Effect != core.Unchanged || core.CodeOf(err) != want || calls != wantCalls ||
					(probe != nil && probe.reads.Load() != 0) {
					t.Fatal("known rejection consumed input or changed a file", rows, err, calls)
				}
			case "input":
				if len(rows.Items) != 4 || calls != 2 || rows.Items[2].Change.Effect != core.Applied || rows.Items[2].Err != nil ||
					last.Change.Effect != core.Unchanged || !errors.Is(err, fault) {
					t.Fatal("input failure lost previous effects", rows, err, calls)
				}
			case "cancel", "post-commit":
				want := error(fault)
				if mode == "cancel" {
					want = context.Canceled
				}
				if len(rows.Items) != 3 || calls != 1 || last.Change.Effect != core.Applied || !errors.Is(last.Err, want) || !errors.Is(err, want) {
					t.Fatal("confirmed publication was erased or replayed", rows, err, calls)
				}
			}
			for _, name := range []string{"a", "b"} {
				want := "old"
				for _, row := range rows.Items {
					if row.RelativePath == "/"+name && row.Change.Effect == core.Applied {
						want = "new"
					}
				}
				if data, err := os.ReadFile(filepath.Join(to, "output", name)); err != nil || string(data) != want {
					t.Fatal("file content disagreed with its receipt", name, string(data), err, want)
				}
			}
			if info, err := os.Stat(filepath.Join(to, "output", "empty")); err != nil || !info.IsDir() {
				t.Fatal("earlier directory effect was rolled back", info, err)
			}
			if entries, err := os.ReadDir(staging); err != nil || len(entries) != 0 || (probe != nil && probe.closes.Load() != 0) {
				t.Fatal("publication cleanup or input ownership failed", entries, err)
			}
		})
	}
}
