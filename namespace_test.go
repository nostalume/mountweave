package core_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/local"
)

func TestVisibleNamespaceOperations(t *testing.T) {
	ctx := context.Background()
	a, b, output := t.TempDir(), t.TempDir(), t.TempDir()
	if err := os.Mkdir(filepath.Join(a, "nested"), 0700); err != nil {
		t.Fatal(err)
	}
	write(t, a, "same", []byte("a"))
	write(t, a, "other", []byte("a2"))
	write(t, a, "nested/same", []byte("nested"))
	write(t, b, "same", []byte("b"))
	deny := ""
	var cancelSecond context.CancelFunc
	c := newCore(t, core.Options{Authorize: func(ctx context.Context, p core.Principal, action core.Action, targetAddress core.Address) error {
		target := targetAddress.Value()
		if action == core.StatAction && core.Within(target, "/work/hidden") {
			return errors.New("hidden")
		}
		if action == core.RemoveTreeAction && target == "/work/team" {
			return errors.New("subtree traversal uses its root grant")
		}
		if target == deny {
			return errors.New("revoked")
		}
		if cancelSecond != nil && action == core.RemoveAction && target == "/work/b/again" {
			if _, err := os.Stat(filepath.Join(a, "again")); errors.Is(err, os.ErrNotExist) {
				cancelSecond()
			}
		}
		return allow(ctx, p, action, core.Path(target))
	}})
	defer closeCore(t, c)
	opens := make(map[string]int)
	install := func(prefix, root string) *core.Mount {
		d, err := local.New(root)
		if err != nil {
			t.Fatal(err)
		}
		if prefix == "/work/a" {
			return installCopyMount(t, c, prefix, &removalReader{Driver: d, open: func(ctx context.Context, name string) (core.DirectoryScope, error) {
				opens[name]++
				return d.OpenDirectory(ctx, core.NativePath(name))
			}})
		}
		return installCopyMount(t, c, prefix, d)
	}
	mA := install("/work/a", a)
	install("/work/b", b)
	install("/out", output)
	install("/work/hidden", t.TempDir())
	if err := c.BindAlias(ctx, "/work/alias", mA, "/nested"); err != nil {
		t.Fatal(err)
	}
	selected, err := c.Capture(ctx, core.AddressAccess{Address: core.Path("/work")}, core.AddressAccess{Address: core.Path("/out/snapshot"), Write: true})
	if err != nil {
		t.Fatal(err)
	}
	install("/work/team/new", t.TempDir())
	call := selected.Context(ctx)
	page, err := c.List(call, "alice", core.ListRequest{Query: core.Children(core.Path("/work")), CursorTTL: time.Minute})
	if err != nil || len(page.Items) != 3 {
		t.Fatal("selection broadened or hid a visible member", page, err)
	}
	writer, err := c.Capture(ctx, core.AddressAccess{Address: core.Path("/work/a/nested/same"), Write: true})
	if err != nil || !selected.Conflicts(writer) || !writer.Conflicts(selected) {
		t.Fatal("alias conflict was lost", err)
	}
	ranges, err := c.Capture(ctx, core.AddressAccess{Address: core.Path("/work/a/a"), Write: true}, core.AddressAccess{Address: core.Path("/work/a/a-"), Write: true})
	nested, err2 := c.Capture(ctx, core.AddressAccess{Address: core.Path("/work/a/a/z")})
	if err != nil || err2 != nil || !ranges.Conflicts(nested) {
		t.Fatal("component order lost an ancestor conflict", err, err2)
	}
	limits := core.TreeOptions{ExecutionOptions: core.ExecutionOptions{Concurrency: 3}, MaxEntries: 30, MaxPlanBytes: 1024}
	tiny := limits
	tiny.MaxEntries = 3
	if rows, err := c.CopyTree(call, "alice", core.Path("/work"), core.Path("/out/snapshot"), tiny); len(rows.Items) != 0 || core.CodeOf(err) != core.CapacityExceeded {
		t.Fatal(rows, err)
	}
	if _, err := os.Stat(filepath.Join(output, "snapshot")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("discovery changed target", err)
	}
	rows, err := c.CopyTree(call, "alice", core.Path("/work"), core.Path("/out/snapshot"), limits)
	if err != nil {
		t.Fatal(rows, err)
	}
	if opens["/"] != 2 || opens["/nested"] != 4 {
		t.Fatal("directory acquisition grew per file or crossed visible occurrences", opens)
	}
	for name, want := range map[string]string{"a/same": "a", "a/nested/same": "nested", "alias/same": "nested", "b/same": "b"} {
		data, err := os.ReadFile(filepath.Join(output, "snapshot", filepath.FromSlash(name)))
		if err != nil || string(data) != want {
			t.Fatal(name, string(data), err)
		}
	}
	for _, name := range []string{"team", "hidden"} {
		if _, err := os.Stat(filepath.Join(output, "snapshot", name)); !errors.Is(err, os.ErrNotExist) {
			t.Fatal(name, err)
		}
	}
	if _, err := c.Stat(call, "alice", core.Path("/work/a/../same")); core.CodeOf(err) != core.InvalidPath {
		t.Fatal("range membership bypassed syntax", err)
	}
	if rows, err := c.CopyTree(ctx, "alice", core.Path("/work"), core.Path("/work/a/inside"), limits); len(rows.Items) != 0 || core.CodeOf(err) != core.InvalidRequest {
		t.Fatal("self copy admitted", rows, err)
	}
	if rows, err := c.RemoveTree(ctx, "alice", core.Path("/work"), limits); len(rows.Items) != 0 || core.CodeOf(err) != core.InvalidRequest {
		t.Fatal("implicit clear", rows, err)
	}
	clear := core.DirectoryRemoveOptions{ContentsOnly: true}
	// Explicit alias occurrences remain distinct. A later duplicate may fail, but
	// its native root must survive deletion through the primary visible branch.
	rows, err = c.RemoveTree(ctx, "alice", core.Path("/work"), limits, clear)
	if core.CodeOf(err) != core.NotFound || len(rows.Items) == 0 {
		t.Fatal("duplicate intent disappeared", rows, err)
	}
	if info, err := os.Stat(filepath.Join(a, "nested")); err != nil || !info.IsDir() {
		t.Fatal("selected alias anchor removed", info, err)
	}
	if err := c.UnbindAlias(ctx, "/work/alias"); err != nil {
		t.Fatal(err)
	}
	if err := c.BindAlias(ctx, "/work/alias", mA, "/nested"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Stat(call, "alice", core.Path("/work/alias")); core.CodeOf(err) != core.MountUnavailable {
		t.Fatal("selected alias retargeted", err)
	}
	if _, err := c.List(call, "alice", core.ListRequest{Query: core.Children(core.Path("/work")), CursorTTL: time.Minute}); core.CodeOf(err) != core.MountUnavailable {
		t.Fatal("stale member was silently hidden", err)
	}
	if err := c.UnbindAlias(ctx, "/work/alias"); err != nil {
		t.Fatal(err)
	}
	// Discovery denial cannot consume an earlier source, even with several members.
	write(t, a, "again", []byte("a"))
	write(t, b, "again", []byte("b"))
	deny = "/work/b/again"
	if rows, err := c.RemoveTree(ctx, "alice", core.Path("/work"), limits, clear); len(rows.Items) != 0 || core.CodeOf(err) != core.PermissionDenied {
		t.Fatal(rows, err)
	}
	if _, err := os.Stat(filepath.Join(a, "again")); err != nil {
		t.Fatal("preflight had effects", err)
	}
	deny = ""
	if rows, err := c.RemoveTree(ctx, "alice", core.Path("/work"), limits, clear); err != nil || len(rows.Items) == 0 {
		t.Fatal(rows, err)
	}
	for _, root := range []string{a, b} {
		entries, err := os.ReadDir(root)
		if err != nil || len(entries) != 0 {
			t.Fatal(entries, err)
		}
	}
	if info, err := c.Stat(ctx, "alice", core.Path("/work")); err != nil || !info.Virtual {
		t.Fatal(info, err)
	}
	if rows, err := c.RemoveTree(ctx, "alice", core.Path("/work"), limits, clear); err != nil || len(rows.Items) != 0 {
		t.Fatal("empty clear has fake effects", rows, err)
	}
	if rows, err := c.RemoveTree(ctx, "alice", core.Path("/work"), limits, core.DirectoryRemoveOptions{ContentsOnly: true, Scope: core.Subtree}); err != nil || len(rows.Items) != 0 {
		t.Fatal("virtual traversal required descendant subtree grants", rows, err)
	}
	write(t, a, "again", []byte("a"))
	write(t, b, "again", []byte("b"))
	cancelled, cancel := context.WithCancel(ctx)
	cancelSecond = cancel
	serial := limits
	serial.Concurrency = 1
	rows, err = c.RemoveTree(cancelled, "alice", core.Path("/work"), serial, clear)
	cancel()
	cancelSecond = nil
	if !errors.Is(err, context.Canceled) || len(rows.Items) != 2 || rows.Items[0].Change.Effect != core.Applied || rows.Items[1].Phase != core.ReadPhase || rows.Items[1].Change.Effect != core.Unchanged {
		t.Fatal("cancel lost the applied prefix", rows, err)
	}
	if _, err := os.Stat(filepath.Join(b, "again")); err != nil {
		t.Fatal("cancelled source was consumed", err)
	}
	// An admitted executor must still reject a captured generation retired in an
	// authorization callback, before the native mutation can consume the name.
	write(t, a, "retained", []byte("a"))
	var retired bool
	var retiring *core.Core
	var mount *core.Mount
	retiring = newCore(t, core.Options{Authorize: func(_ context.Context, _ core.Principal, action core.Action, nameAddress core.Address) error {
		name := nameAddress.Value()
		if action == core.RemoveAction && strings.HasSuffix(name, "/retained") && !retired {
			retired = true
			return mount.Retire(ctx)
		}
		return nil
	}})
	defer closeCore(t, retiring)
	d, err := local.New(a)
	if err != nil {
		t.Fatal(err)
	}
	mount = installCopyMount(t, retiring, "/source", d)
	selection, err := retiring.Capture(ctx, core.AddressAccess{Address: core.Path("/source/retained"), Write: true})
	if err != nil {
		t.Fatal(err)
	}
	change, err := retiring.Remove(selection.Context(ctx), "alice", core.Path("/source/retained"))
	if core.CodeOf(err) != core.MountUnavailable || change.Effect != core.Unchanged {
		t.Fatal(change, err)
	}
	if _, err := os.Stat(filepath.Join(a, "retained")); err != nil {
		t.Fatal(err)
	}
}
