package core_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	core "github.com/nostalume/mountweave"
)

func TestLocalMove(t *testing.T) {
	directory := t.TempDir()
	c := newCore(t, core.Options{Authorize: func(ctx context.Context, p core.Principal, a core.Action, targetAddress core.Address) error {
		target := targetAddress.Value()
		if core.Within(target, "/files/folder") && (a == core.ReadAction || a == core.ListAction) {
			return errors.New("directory move needs no descendant read grant")
		}
		return allow(ctx, p, a, core.Path(target))
	}})
	defer closeCore(t, c)
	m := mountLocal(t, c, directory)
	if err := c.BindAlias(context.Background(), "/alias", m, "/"); err != nil {
		t.Fatal(err)
	}
	write(t, directory, "a", []byte("source"))
	write(t, directory, "existing", []byte("keep"))
	if err := os.Mkdir(filepath.Join(directory, "folder"), 0700); err != nil {
		t.Fatal(err)
	}
	page, err := c.List(context.Background(), "alice", core.ListRequest{Query: core.Children(core.Path("/files")), Limit: 1, CursorTTL: time.Minute})
	if err != nil || page.NextCursor == nil {
		t.Fatal(page, err)
	}
	change, err := c.Move(context.Background(), "alice", core.Path("/files/a"), core.Path("/files/existing"))
	if change.Source.Effect == core.Unchanged && change.Target.Effect == core.Unchanged && core.CodeOf(err) == core.UnsupportedOperation {
		t.Skip("native no-replace relocation unavailable on this filesystem")
	}
	if change.Source.Effect != core.Unchanged || change.Target.Effect != core.Unchanged || core.CodeOf(err) != core.AlreadyExists {
		t.Fatal(change, err)
	}
	if err := c.CheckCursor(context.Background(), "alice", core.ListRequest{Query: core.Children(core.Path("/files")), Cursor: *page.NextCursor}); err != nil {
		t.Fatal("refusal invalidated the cursor", err)
	}
	change, err = c.Move(context.Background(), "alice", core.Path("/files/a"), core.Path("/files/folder/b"))
	if core.CodeOf(err) == core.UnsupportedOperation && change.Source.Effect == core.Unchanged && change.Target.Effect == core.Unchanged {
		t.Skip("native no-replace relocation unavailable on this filesystem")
	}
	if change.Source.Effect != core.Applied || change.Target.Effect != core.Applied || err != nil {
		t.Fatal(change, err)
	}
	if _, err := os.Stat(filepath.Join(directory, "a")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("source name survived", err)
	}
	for name, want := range map[string]string{"folder/b": "source", "existing": "keep"} {
		data, err := os.ReadFile(filepath.Join(directory, filepath.FromSlash(name)))
		if err != nil || string(data) != want {
			t.Fatal(name, string(data), err)
		}
	}
	code(t, c.CheckCursor(context.Background(), "alice", core.ListRequest{Query: core.Children(core.Path("/files")), Cursor: *page.NextCursor}), core.CursorUnavailable)
	change, err = c.Move(context.Background(), "alice", core.Path("/files/folder/b"), core.Path("/files/folder/c"))
	if change.Source.Effect != core.Applied || change.Target.Effect != core.Applied || err != nil {
		t.Fatal(change, err)
	}
	if err := os.MkdirAll(filepath.Join(directory, "folder", "nested", "empty"), 0700); err != nil {
		t.Fatal(err)
	}
	write(t, directory, "folder/nested/payload", []byte("nested"))
	child, err := os.Open(filepath.Join(directory, "folder", "nested", "payload"))
	if err != nil {
		t.Fatal(err)
	}
	before, err := child.Stat()
	if closeErr := child.Close(); err != nil || closeErr != nil {
		t.Fatal(err, closeErr)
	}
	if err := os.Mkdir(filepath.Join(directory, "occupied"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := c.BindAlias(context.Background(), "/view", m, "/folder"); err != nil {
		t.Fatal(err)
	}
	page, err = c.List(context.Background(), "alice", core.ListRequest{Query: core.Children(core.Path("/files")), Limit: 1, CursorTTL: time.Minute})
	if err != nil || page.NextCursor == nil {
		t.Fatal(page, err)
	}
	change, err = c.Move(context.Background(), "alice", core.Path("/files/folder"), core.Path("/alias/occupied"))
	if change.Source.Effect != core.Unchanged || change.Target.Effect != core.Unchanged || core.CodeOf(err) != core.AlreadyExists {
		t.Fatal(change, err)
	}
	if err := c.CheckCursor(context.Background(), "alice", core.ListRequest{Query: core.Children(core.Path("/files")), Cursor: *page.NextCursor}); err != nil {
		t.Fatal("refused directory move invalidated cursor", err)
	}
	for _, target := range []string{"/alias/folder", "/alias/folder/nested/inside"} {
		change, err = c.Move(context.Background(), "alice", core.Path("/files/folder"), core.Path(target))
		if change.Source.Effect != core.Unchanged || change.Target.Effect != core.Unchanged || core.CodeOf(err) != core.InvalidRequest {
			t.Fatal(change, err)
		}
	}
	change, err = c.Move(context.Background(), "alice", core.Path("/files/folder"), core.Path("/alias/renamed"))
	if change.Source.Effect != core.Applied || change.Target.Effect != core.Applied || err != nil {
		t.Fatal(change, err)
	}
	after, err := os.Stat(filepath.Join(directory, "renamed", "nested", "payload"))
	if err != nil || !os.SameFile(before, after) {
		t.Fatal("directory relocation did not preserve child identity", after, err)
	}
	if info, err := os.Stat(filepath.Join(directory, "renamed", "nested", "empty")); err != nil || !info.IsDir() {
		t.Fatal(info, err)
	}
	code(t, c.CheckCursor(context.Background(), "alice", core.ListRequest{Query: core.Children(core.Path("/files")), Cursor: *page.NextCursor}), core.CursorUnavailable)
	_, err = c.Stat(context.Background(), "alice", core.Path("/view"))
	code(t, err, core.NotFound) // Aliases retain paths, not directory identities.
	rows, err := c.MoveBatch(context.Background(), "alice", []core.MoveRequest{
		{Source: core.Path("/files/renamed"), Target: core.Path("/files/final")},
		{Source: core.Path("/alias/final/nested/payload"), Target: core.Path("/files/result")},
		{Source: core.Path("/files/final/c"), Target: core.Path("/files/result")},
		{Source: core.Path("/files/final/c"), Target: core.Path("/files/not-started")},
	})
	if len(rows) != 3 || core.CodeOf(err) != core.AlreadyExists {
		t.Fatal(rows, err)
	}
	for i, row := range rows {
		want := core.Applied
		if i == 2 {
			want = core.Unchanged
		}
		if row.Source.Effect != want || row.Target.Effect != want {
			t.Fatal(i, row)
		}
	}
	data, err := os.ReadFile(filepath.Join(directory, "result"))
	if err != nil || string(data) != "nested" {
		t.Fatal(string(data), err)
	}
	if _, err := os.Stat(filepath.Join(directory, "not-started")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}
}

type relocatingDriver struct {
	core.Driver
	move func(context.Context, string, string) (core.Change, error)
}

func (d *relocatingDriver) Relocate(ctx context.Context, sourceAddress, targetAddress core.NativeAddress, _ ...core.MoveOptions) (core.Change, error) {
	if sourceAddress.Namespace() != core.TreeNamespace {
		return core.Change{Effect: core.Unchanged}, core.Fail(core.UnsupportedOperation, nil)
	}
	source := sourceAddress.Value()
	if targetAddress.Namespace() != core.TreeNamespace {
		return core.Change{Effect: core.Unchanged}, core.Fail(core.UnsupportedOperation, nil)
	}
	target := targetAddress.Value()

	return d.move(ctx, source, target)
}

func TestMoveAdmissionAndReceipt(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	denied, calls := true, 0
	c := newCore(t, core.Options{Authorize: func(_ context.Context, _ core.Principal, a core.Action, targetAddress core.Address) error {
		if denied && a == core.CreateAction {
			return errors.New("target denied")
		}
		return nil
	}})
	defer closeCore(t, c)
	fault := errors.New("completion fault")
	d := &relocatingDriver{Driver: &fakeDriver{}, move: func(_ context.Context, source, target string) (core.Change, error) {
		calls++
		if source != "/a" || target != "/b" {
			t.Fatal(source, target)
		}
		cancel()
		return core.Change{Effect: core.Applied}, fault
	}}
	if _, err := c.Install("/files", d); err != nil {
		t.Fatal(err)
	}
	change, err := c.Move(ctx, "alice", core.Path("/files/a"), core.Path("/files/b"))
	if change.Source.Effect != core.Unchanged || change.Target.Effect != core.Unchanged || core.CodeOf(err) != core.PermissionDenied || calls != 0 {
		t.Fatal(change, err, calls)
	}
	denied = false
	if _, err := c.Install("/other", &fakeDriver{}); err != nil {
		t.Fatal(err)
	}
	change, err = c.Move(ctx, "alice", core.Path("/files/a"), core.Path("/other/b"))
	if change.Source.Effect != core.Unchanged || change.Target.Effect != core.Unchanged || core.CodeOf(err) != core.UnsupportedOperation || calls != 0 {
		t.Fatal(change, err, calls)
	}
	change, err = c.Move(ctx, "alice", core.Path("/files/a"), core.Path("/files/b"))
	if change.Source.Effect != core.Applied || change.Target.Effect != core.Applied || !errors.Is(err, fault) || !errors.Is(err, context.Canceled) || calls != 1 {
		t.Fatal(change, err, calls)
	}

	for _, tc := range []struct {
		effect core.Effect
		want   core.Effect
	}{
		{core.Unknown, core.Unknown},
		{core.Incomplete, core.Unknown},
	} {
		d.move = func(context.Context, string, string) (core.Change, error) {
			return core.Change{Effect: tc.effect}, fault
		}
		result, err := c.Move(context.Background(), "alice", core.Path("/files/a"), core.Path("/files/b"))
		if result.Source.Effect != tc.want || result.Target.Effect != tc.want || !errors.Is(err, fault) {
			t.Fatal(result, err)
		}
		if tc.effect == core.Incomplete && core.CodeOf(err) != core.InvalidUpstreamResponse {
			t.Fatal("partial effects violated the atomic capability", result, err)
		}
	}
	for _, tc := range []struct {
		c              *core.Core
		source, target string
		code           core.Code
	}{
		{nil, "/files/a", "/files/b", core.InvalidRequest},
		{c, "/files/a", "/files/a", core.InvalidRequest},
		{c, "/files/../a", "/files/b", core.InvalidPath},
		{c, "/files/a", "/files/../b", core.InvalidPath},
		{c, "/files", "/files/b", core.InvalidRequest},
	} {
		result, err := tc.c.Move(context.Background(), "alice", core.Path(tc.source), core.Path(tc.target))
		if result.Source.Effect != core.Unchanged || result.Target.Effect != core.Unchanged || core.CodeOf(err) != tc.code {
			t.Fatal(result, err)
		}
	}

	for _, endpoint := range []string{"source", "target"} {
		t.Run("rebound-"+endpoint, func(t *testing.T) {
			var c *core.Core
			var m *core.Mount
			c = newCore(t, core.Options{Authorize: func(ctx context.Context, _ core.Principal, _ core.Action, targetAddress core.Address) error {
				target := targetAddress.Value()
				if core.Within(target, "/alias") {
					if err := c.UnbindAlias(ctx, "/alias"); err != nil {
						return err
					}
					return c.BindAlias(ctx, "/alias", m, "/")
				}
				return nil
			}})
			defer closeCore(t, c)
			nativeCalls := 0
			m = installCopyMount(t, c, "/files", &relocatingDriver{
				Driver: &fakeDriver{stat: func(context.Context, string) (core.Metadata, error) {
					return core.Metadata{Kind: core.Directory}, nil
				}},
				move: func(context.Context, string, string) (core.Change, error) {
					nativeCalls++
					return core.Change{Effect: core.Applied}, nil
				},
			})
			if err := c.BindAlias(context.Background(), "/alias", m, "/"); err != nil {
				t.Fatal(err)
			}
			request := core.MoveRequest{Source: core.Path("/files/a"), Target: core.Path("/files/b")}
			if endpoint == "source" {
				request.Source = core.Path("/alias/a")
			} else {
				request.Target = core.Path("/alias/b")
			}
			rows, err := c.MoveBatch(context.Background(), "alice", []core.MoveRequest{request})
			if len(rows) != 1 || nativeCalls != 0 || core.CodeOf(err) != core.MountUnavailable ||
				rows[0].Source.Effect != core.Unchanged || rows[0].Target.Effect != core.Unchanged || core.CodeOf(rows[0].Err) != core.MountUnavailable {
				t.Fatal(rows, err, nativeCalls)
			}
		})
	}
}

type directoryRelocatingDriver struct {
	core.Driver
	move func(context.Context, core.NativeAddress, core.NativeAddress) (string, core.Change, error)
}

func (d *directoryRelocatingDriver) RelocateInto(ctx context.Context, source, directory core.NativeAddress) (string, core.Change, error) {
	return d.move(ctx, source, directory)
}

func TestMoveIntoAdmissionAndReceipt(t *testing.T) {
	denyPrimary, calls := true, 0
	c := newCore(t, core.Options{Authorize: func(_ context.Context, _ core.Principal, action core.Action, at core.Address) error {
		switch {
		case action == core.RemoveAction && at == core.Path("/files/a"):
			return nil
		case action == core.CreateChildAction && (at == core.Path("/alias") || !denyPrimary && at == core.Path("/files/dir")):
			return nil
		default:
			return errors.New("denied")
		}
	}})
	defer closeCore(t, c)
	d := &directoryRelocatingDriver{Driver: &fakeDriver{stat: func(_ context.Context, path string) (core.Metadata, error) {
		return core.Metadata{Name: "dir", Kind: core.Directory}, nil
	}}}
	m, err := c.Install("/files", d)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.BindAlias(context.Background(), "/alias", m, "/dir"); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	from, directory := core.Path("/files/a"), core.Path("/alias")
	for _, tc := range []struct {
		name, leaf   string
		deny, cancel bool
		wantCode     core.Code
		effect       core.Effect
		destination  core.Address
		wantCalls    int
	}{
		{name: "deny primary", deny: true, wantCode: core.PermissionDenied, effect: core.Unchanged},
		{name: "reject native path", leaf: "../elsewhere", wantCode: core.InvalidUpstreamResponse, effect: core.Unknown, wantCalls: 1},
		{name: "preserve confirmed placement", leaf: "a (2)", cancel: true, effect: core.Applied, destination: core.Path("/alias/a (2)"), wantCalls: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			denyPrimary, calls = tc.deny, 0
			d.move = func(_ context.Context, from, parent core.NativeAddress) (string, core.Change, error) {
				calls++
				if from != core.NativePath("/a") || parent != core.NativePath("/dir") {
					t.Fatal(from, parent)
				}
				if tc.cancel {
					cancel()
				}
				return tc.leaf, core.Change{Effect: core.Applied}, nil
			}
			result, err := c.MoveInto(ctx, "alice", from, directory)
			if result.Source.Effect != tc.effect || result.Target.Effect != tc.effect || result.Destination != tc.destination || calls != tc.wantCalls {
				t.Fatal(result, err, calls)
			}
			if tc.cancel {
				if !errors.Is(err, context.Canceled) {
					t.Fatal(err)
				}
			} else if core.CodeOf(err) != tc.wantCode {
				t.Fatal(err)
			}
		})
	}
}
