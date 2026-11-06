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

type directoryDriver struct {
	*fakeDriver
	mkdir func(context.Context, string) (core.Change, error)
}

func (d *directoryDriver) Mkdir(ctx context.Context, pAddress core.NativeAddress) (core.Change, error) {
	if pAddress.Namespace() != core.TreeNamespace {
		return core.Change{Effect: core.Unchanged}, core.Fail(core.UnsupportedOperation, nil)
	}
	p := pAddress.Value()

	d.calls.Add(1)
	return d.mkdir(ctx, p)
}

func TestLocalChangesAndInputBoundaries(t *testing.T) {
	root := t.TempDir()
	c := newCore(t, core.Options{})
	defer closeCore(t, c)
	mountLocal(t, c, root)
	ctx := context.Background()
	changed, err := c.Mkdir(ctx, "alice", core.Path("/files/folder"))
	if err != nil || changed.Effect != core.Applied {
		t.Fatal(changed, err)
	}
	changed, err = c.Mkdir(ctx, "alice", core.Path("/files/folder"))
	code(t, err, core.AlreadyExists)
	if changed.Effect != core.Unchanged {
		t.Fatal(changed)
	}
	changed, err = c.Mkdir(ctx, "alice", core.Path("/files/missing/child"))
	code(t, err, core.NotFound)
	if changed.Effect != core.Unchanged {
		t.Fatal(changed)
	}
	cases := []struct {
		name, data string
		length     *int64
		effect     core.Effect
	}{
		{"complete", "abc", nil, core.Applied},
		{"empty", "", ptr(0), core.Applied},
		{"exact", "abc", ptr(3), core.Applied},
		{"short", "ab", ptr(3), core.Incomplete},
		{"long", "abcd", ptr(3), core.Incomplete},
	}
	for _, tc := range cases {
		changed, err = c.Create(ctx, "alice", core.Path("/files/folder/"+tc.name), strings.NewReader(tc.data), tc.length)
		if changed.Effect != tc.effect {
			t.Fatalf("%s: %+v %v", tc.name, changed, err)
		}
		if tc.effect == core.Applied && err != nil {
			t.Fatal(err)
		}
		if tc.effect == core.Incomplete {
			code(t, err, core.InvalidRequest)
		}
	}
	original, err := os.ReadFile(filepath.Join(root, "folder", "complete"))
	if err != nil {
		t.Fatal(err)
	}
	changed, err = c.Create(ctx, "alice", core.Path("/files/folder/complete"), strings.NewReader("replacement"), nil)
	code(t, err, core.AlreadyExists)
	after, readErr := os.ReadFile(filepath.Join(root, "folder", "complete"))
	if changed.Effect != core.Unchanged || readErr != nil || string(after) != string(original) {
		t.Fatal(changed, string(after), readErr)
	}
	changed, err = c.Create(ctx, "alice", core.Path("/files/folder/failed"), io.MultiReader(strings.NewReader("prefix"), iotest.ErrReader(io.ErrNoProgress)), nil)
	if changed.Effect != core.Incomplete || !errors.Is(err, io.ErrNoProgress) {
		t.Fatal(changed, err)
	}
	partial, err := os.ReadFile(filepath.Join(root, "folder", "failed"))
	if err != nil || string(partial) != "prefix" {
		t.Fatal(string(partial), err)
	}
	changed, err = c.Remove(ctx, "alice", core.Path("/files/folder"))
	code(t, err, core.DirectoryNotEmpty)
	if changed.Effect != core.Unchanged {
		t.Fatal(changed)
	}
	changed, err = c.Remove(ctx, "alice", core.Path("/files/folder/complete"))
	if err != nil || changed.Effect != core.Applied {
		t.Fatal(changed, err)
	}
	changed, err = c.Remove(ctx, "alice", core.Path("/files/folder/complete"))
	code(t, err, core.NotFound)
	if changed.Effect != core.Unchanged {
		t.Fatal(changed)
	}
	changed, err = c.Remove(ctx, "alice", core.Path("/files"))
	code(t, err, core.InvalidRequest)
	if changed.Effect != core.Unchanged {
		t.Fatal(changed)
	}
	results, err := c.RemoveBatch(ctx, "alice", []core.Address{
		core.Path("/files/folder/exact"), core.Path("/files/folder/exact"), core.Path("/files/folder/empty"),
	})
	if len(results) != 2 || core.CodeOf(err) != core.NotFound ||
		results[0].Change.Effect != core.Applied || results[0].Err != nil ||
		results[1].Change.Effect != core.Unchanged || core.CodeOf(results[1].Err) != core.NotFound {
		t.Fatal(results, err)
	}
	if _, err := os.Stat(filepath.Join(root, "folder", "empty")); err != nil {
		t.Fatal("unstarted suffix was removed", err)
	}

}
func ptr(n int64) *int64 { return &n }

func TestChangeReceiptsSurviveErrorsAndCancellation(t *testing.T) {
	fault := errors.New("cleanup not acknowledged")
	for _, effect := range []core.Effect{core.Unchanged, core.Applied, core.Incomplete, core.Unknown} {
		t.Run(effect.String(), func(t *testing.T) {
			c := newCore(t, core.Options{})
			defer closeCore(t, c)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			d := &directoryDriver{fakeDriver: &fakeDriver{}, mkdir: func(context.Context, string) (core.Change, error) {
				cancel()
				return core.Change{Effect: effect}, fault
			}}
			if _, err := c.Install("/files", d); err != nil {
				t.Fatal(err)
			}
			result, err := c.Mkdir(ctx, "alice", core.Path("/files/new"))
			if result.Effect != effect || !errors.Is(err, fault) || !errors.Is(err, context.Canceled) {
				t.Fatal(result, err)
			}
		})
	}
}

func TestMutationKeepsOtherMountsAndOpenedBodiesIndependent(t *testing.T) {
	c := newCore(t, core.Options{})
	defer closeCore(t, c)
	root, other := t.TempDir(), t.TempDir()
	for _, name := range []string{"a", "b", "c"} {
		write(t, root, name, []byte("abc"))
		write(t, other, name, nil)
	}
	mountLocal(t, c, root)
	d, err := local.New(other)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = c.Install("/other", d); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	first := func(target string, limit int) core.ListRequest {
		r := core.ListRequest{Query: core.Children(core.Path(target)), Limit: limit, CursorTTL: time.Minute}
		page, e := c.List(ctx, "alice", r)
		if e != nil || page.NextCursor == nil {
			t.Fatal(page, e)
		}
		r.Cursor = *page.NextCursor
		return r
	}
	current := first("/files", 1)
	eofReplay := first("/files", 2)
	for {
		final, err := c.List(ctx, "alice", eofReplay)
		if err != nil {
			t.Fatal(err)
		}
		if final.NextCursor == nil {
			break
		}
		eofReplay.Cursor = *final.NextCursor
	}
	peerCursor := first("/other", 1)
	opened, err := c.Open(ctx, "alice", core.Path("/files/a"), core.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer opened.Body.Close()
	result, err := c.Mkdir(ctx, "alice", core.Path("/files/new"))
	if err != nil || result.Effect != core.Applied {
		t.Fatal(result, err)
	}
	for _, r := range []core.ListRequest{current, eofReplay} {
		code(t, c.CheckCursor(ctx, "alice", r), core.CursorUnavailable)
		_, e := c.List(ctx, "alice", r)
		code(t, e, core.CursorUnavailable)
	}
	if err = c.CheckCursor(ctx, "alice", peerCursor); err != nil {
		t.Fatal(err)
	}
	bytes, err := io.ReadAll(opened.Body)
	if err != nil || string(bytes) != "abc" {
		t.Fatal(string(bytes), err)
	}
	fresh := first("/files", 1)
	_, err = c.Mkdir(ctx, "alice", core.Path("/files/new"))
	code(t, err, core.AlreadyExists)
	if err = c.CheckCursor(ctx, "alice", fresh); err != nil {
		t.Fatal(err)
	}
	result, err = c.Create(ctx, "alice", core.Path("/files/partial"), strings.NewReader("x"), ptr(2))
	if result.Effect != core.Incomplete {
		t.Fatal(result, err)
	}
	code(t, c.CheckCursor(ctx, "alice", fresh), core.CursorUnavailable)
}

func TestMutationWaitsForBorrowedReadAcknowledgment(t *testing.T) {
	for _, replace := range []bool{false, true} {
		t.Run(map[bool]string{false: "create", true: "publish"}[replace], func(t *testing.T) {
			if replace && runtime.GOOS != "windows" && runtime.GOOS != "linux" {
				t.Skip("native publication is unavailable on this platform")
			}
			input := &blockingBody{started: make(chan struct{}), release: make(chan struct{}), closed: make(chan struct{})}

			root := t.TempDir()
			write(t, root, "peer", []byte("peer"))
			c := newCore(t, core.Options{})
			defer closeCore(t, c)
			defer func() {
				select {
				case <-input.release:
				default:
					close(input.release)
				}
			}()
			storage, err := local.New(root, local.Options{StagingDirectory: t.TempDir(), MaxStagingBytes: 32})
			if err != nil {
				t.Fatal(err)
			}
			m := installCopyMount(t, c, "/files", storage)
			if replace {
				write(t, root, "new", []byte("old"))
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			type outcome struct {
				result core.Change
				err    error
			}
			done := make(chan outcome, 1)
			go func() {
				var r core.Change
				var e error
				if replace {
					r, e = c.Write(ctx, "alice", core.Path("/files/new"), input, nil, core.WriteOptions{Policy: core.Publish()})
				} else {
					r, e = c.Create(ctx, "alice", core.Path("/files/new"), input, nil)
				}
				done <- outcome{r, e}
			}()
			await(t, input.started)
			cancel()
			peer, err := c.Open(context.Background(), "alice", core.Path("/files/peer"), core.OpenOptions{})
			if err != nil {
				t.Fatal(err)
			}
			data, err := io.ReadAll(peer.Body)
			if err != nil || string(data) != "peer" {
				t.Fatal(string(data), err)
			}
			if input.closes.Load() != 0 {
				t.Fatal("borrowed input was closed")
			}
			wait, stop := context.WithTimeout(context.Background(), 20*time.Millisecond)
			err = m.Retire(wait)
			stop()
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatal(err)
			}
			if _, err = c.Install("/files", &fakeDriver{}); core.CodeOf(err) != core.PathAmbiguous {
				t.Fatal(err)
			}
			close(input.release)
			select {
			case got := <-done:
				want := core.Incomplete
				if replace {
					want = core.Unchanged
				}
				if got.result.Effect != want || !errors.Is(got.err, context.Canceled) {
					t.Fatal(got)
				}
			case <-time.After(time.Second):
				t.Fatal("write did not acknowledge input completion")
			}
			if replace {
				if data, err := os.ReadFile(filepath.Join(root, "new")); err != nil || string(data) != "old" {
					t.Fatal("canceled publication changed the old target", string(data), err)
				}
			}
			if input.closes.Load() != 0 {
				t.Fatal("input ownership changed")
			}
			if err = input.Close(); err != nil {
				t.Fatal(err)
			}
			if err = m.Retire(context.Background()); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestCompletePublication(t *testing.T) {
	if runtime.GOOS != "windows" && runtime.GOOS != "linux" {
		t.Skip("native publication is unavailable on this platform")
	}
	ctx := context.Background()
	root, staging := t.TempDir(), t.TempDir()
	opts := local.Options{StagingDirectory: staging, MaxStagingBytes: 8, MaxConcurrentCommits: 1}
	d, err := local.New(root, opts)
	if err != nil {
		t.Fatal(err)
	}
	c := newCore(t, core.Options{Authorize: func(_ context.Context, p core.Principal, a core.Action, _ core.Address) error {
		if p == "creator" && a == core.WriteAction {
			return errors.New("replacement denied")
		}
		return nil
	}})
	defer closeCore(t, c)
	m := installCopyMount(t, c, "/files", d)
	if err := c.BindAlias(ctx, "/alias", m, "/"); err != nil {
		t.Fatal(err)
	}
	replace := core.WriteOptions{Policy: core.Publish()}
	cases := []struct {
		name, data string
		length     *int64
		missing    bool
		fault      bool
		want       core.Code
	}{
		{"new", "new", ptr(3), true, false, ""},
		{"empty", "", ptr(0), false, false, ""},
		{"unknown", "new", nil, false, false, ""},
		{"short", "no", ptr(3), false, false, core.InvalidRequest},
		{"long", "long", ptr(3), false, false, core.InvalidRequest},
		{"input-error", "new", nil, false, true, core.UpstreamFailure},
		{"known-quota", "oversized", ptr(9), false, false, core.CapacityExceeded},
		{"unknown-quota", "oversized", nil, false, false, core.CapacityExceeded},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.missing {
				_ = os.Remove(filepath.Join(root, "item"))
			} else {
				write(t, root, "item", []byte("old"))
			}
			var reader io.Reader = strings.NewReader(tc.data)
			if tc.fault {
				reader = io.MultiReader(reader, iotest.ErrReader(io.ErrNoProgress))
			}
			input := &copyBody{Reader: reader}
			change, err := c.Write(ctx, "alice", core.Path("/alias/item"), input, tc.length, replace)
			want := tc.data
			if tc.want != "" {
				want = "old"
				if change.Effect != core.Unchanged || core.CodeOf(err) != tc.want {
					t.Fatal(change, err)
				}
			} else if change.Effect != core.Applied || err != nil {
				t.Fatal(change, err)
			}
			data, readErr := os.ReadFile(filepath.Join(root, "item"))
			if readErr != nil || string(data) != want || input.closes.Load() != 0 {
				t.Fatal(string(data), readErr, input.closes.Load())
			}
			if tc.name == "known-quota" && input.reads.Load() != 0 {
				t.Fatal("known over-budget input was consumed")
			}
			if entries, err := os.ReadDir(staging); err != nil || len(entries) != 0 {
				t.Fatal("temporary files survived successful cleanup", entries, err)
			}
		})
	}
	input := &copyBody{Reader: strings.NewReader("new")}
	change, err := c.Write(ctx, "creator", core.Path("/files/item"), input, ptr(3), replace)
	if change.Effect != core.Unchanged || core.CodeOf(err) != core.PermissionDenied || input.reads.Load() != 0 {
		t.Fatal("create permission granted replacement", change, err)
	}
	change, err = c.Write(ctx, "alice", core.Path("/files/item"), input, ptr(3))
	if change.Effect != core.Unchanged || core.CodeOf(err) != core.AlreadyExists || input.reads.Load() != 0 {
		t.Fatal("default write replaced an existing target", change, err)
	}
	if err := os.Mkdir(filepath.Join(root, "directory"), 0700); err != nil {
		t.Fatal(err)
	}
	change, err = c.Write(ctx, "alice", core.Path("/files/directory"), input, ptr(3), replace)
	if change.Effect != core.Unchanged || core.CodeOf(err) != core.NotFile || input.reads.Load() != 0 {
		t.Fatal(change, err)
	}
	plain, err := local.New(t.TempDir(), local.Options{MaxConcurrentCommits: 1})
	if err != nil {
		t.Fatal(err)
	}
	installCopyMount(t, c, "/plain", plain)
	change, err = c.Write(ctx, "alice", core.Path("/plain/new"), input, ptr(3), replace)
	if change.Effect != core.Unchanged || core.CodeOf(err) != core.UnsupportedOperation || input.reads.Load() != 0 {
		t.Fatal("unconfigured publication consumed input", change, err)
	}
	change, err = c.Write(ctx, "alice", core.Path("/files/new"), input, ptr(3), replace, replace)
	if change.Effect != core.Unchanged || core.CodeOf(err) != core.InvalidRequest {
		t.Fatal(change, err)
	}
	if upper, err := os.Stat(filepath.Join(root, "ITEM")); err == nil {
		actual, err := os.Stat(filepath.Join(root, "item"))
		if err == nil && os.SameFile(upper, actual) {
			probe := &copyBody{Reader: strings.NewReader("alias")}
			change, err := c.Write(ctx, "alice", core.Path("/files/ITEM"), probe, ptr(5), replace)
			if change.Effect != core.Unchanged || probe.reads.Load() != 0 {
				t.Fatal("a different stored name was replaced", change, err)
			}
		}
	}
	for _, options := range []local.Options{
		{StagingDirectory: root, MaxStagingBytes: 8},
		{MaxConcurrentCommits: -1},
	} {
		if invalid, err := local.New(root, options); core.CodeOf(err) != core.InvalidRequest {
			if invalid != nil {
				_ = invalid.Close()
			}
			t.Fatal("invalid Local configuration was accepted", options, err)
		}
	}
}

func TestPublicationCompetitionAndBudget(t *testing.T) {
	if runtime.GOOS != "windows" && runtime.GOOS != "linux" {
		t.Skip("native publication is unavailable on this platform")
	}
	root, staging := t.TempDir(), t.TempDir()
	d, err := local.New(root, local.Options{StagingDirectory: staging, MaxStagingBytes: 8})
	if err != nil {
		t.Fatal(err)
	}
	c := newCore(t, core.Options{})
	defer closeCore(t, c)
	installCopyMount(t, c, "/files", d)
	ctx := context.Background()
	replace := core.WriteOptions{Policy: core.Publish()}
	write(t, root, "item", []byte("old"))
	opened, err := c.Open(ctx, "alice", core.Path("/files/item"), core.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer opened.Body.Close()
	barrier := &blockingBody{started: make(chan struct{}), release: make(chan struct{}), closed: make(chan struct{})}
	defer func() {
		select {
		case <-barrier.release:
		default:
			close(barrier.release)
		}
	}()
	type outcome struct {
		change core.Change
		err    error
	}
	done := make(chan outcome, 1)
	go func() {
		change, err := c.Write(ctx, "alice", core.Path("/files/item"), io.MultiReader(strings.NewReader("new"), barrier), ptr(3), replace)
		done <- outcome{change, err}
	}()
	await(t, barrier.started) // All declared bytes arrived, but normal EOF has not.
	if data, err := os.ReadFile(filepath.Join(root, "item")); err != nil || string(data) != "old" {
		t.Fatal("input was published before normal EOF", string(data), err)
	}
	if entries, err := os.ReadDir(root); err != nil || len(entries) != 1 || entries[0].Name() != "item" {
		t.Fatal("private staging entered the served namespace", entries, err)
	}
	probe := &copyBody{Reader: strings.NewReader("oversized")}
	change, err := c.Write(ctx, "alice", core.Path("/files/peer"), probe, ptr(8), replace)
	if change.Effect != core.Unchanged || core.CodeOf(err) != core.CapacityExceeded || probe.reads.Load() != 0 {
		t.Fatal("separate calls did not share the byte budget", change, err)
	}
	change, err = c.Write(ctx, "alice", core.Path("/files/item"), strings.NewReader("fast"), ptr(4), replace)
	if err != nil || change.Effect != core.Applied {
		t.Fatal(change, err)
	}
	if data, err := io.ReadAll(opened.Body); err != nil || string(data) != "old" {
		t.Fatal("publication changed an already opened object", string(data), err)
	}
	outside := filepath.Join(t.TempDir(), "untouched")
	if err := os.WriteFile(outside, []byte("outside"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(root, "item")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(root, "item")); err != nil {
		// Keep the competition test active where native symlink creation is unavailable.
		write(t, root, "item", []byte("fast"))
	}
	close(barrier.release)
	got := <-done
	if got.err != nil || got.change.Effect != core.Applied {
		t.Fatal(got)
	}
	if data, err := os.ReadFile(filepath.Join(root, "item")); err != nil || string(data) != "new" {
		t.Fatal("later native publication did not win", string(data), err)
	}
	if data, err := os.ReadFile(outside); err != nil || string(data) != "outside" {
		t.Fatal("publication followed a raced final link", string(data), err)
	}
	change, err = c.Write(ctx, "alice", core.Path("/files/peer"), strings.NewReader("released"), ptr(8), replace)
	if err != nil || change.Effect != core.Applied {
		t.Fatal("completed calls did not release staging reservations", change, err)
	}
	if runtime.GOOS == "windows" {
		pause := &blockingBody{started: make(chan struct{}), release: make(chan struct{}), closed: make(chan struct{})}
		go func() {
			change, err := c.Write(ctx, "alice", core.Path("/files/draft"), io.MultiReader(strings.NewReader("x"), pause), ptr(1), replace)
			done <- outcome{change, err}
		}()
		await(t, pause.started)
		_, created := c.Write(ctx, "alice", core.Path("/files/Draft"), strings.NewReader("peer"), ptr(4))
		close(pause.release)
		got := <-done
		data, readErr := os.ReadFile(filepath.Join(root, "Draft"))
		if created != nil || readErr != nil || string(data) != "peer" || got.err == nil || got.change.Effect != core.Unchanged {
			t.Fatal("commit replaced a competing spelling", created, string(data), readErr, got)
		}
	}
}

func TestExclusivePublicationCompetition(t *testing.T) {
	if runtime.GOOS != "windows" && runtime.GOOS != "linux" {
		t.Skip("native publication unavailable")
	}
	root, staging := t.TempDir(), t.TempDir()
	d, err := local.New(root, local.Options{StagingDirectory: staging, MaxStagingBytes: 8})
	if err != nil {
		t.Fatal(err)
	}
	c := newCore(t, core.Options{})
	defer closeCore(t, c)
	m := installCopyMount(t, c, "/files", d)
	ctx := context.Background()
	if err := c.BindAlias(ctx, "/alias", m, "/"); err != nil {
		t.Fatal(err)
	}
	policy := core.WriteOptions{Policy: core.Publish(core.IfAbsent()), ObserveTarget: true}
	barrier := &blockingBody{started: make(chan struct{}), release: make(chan struct{}), closed: make(chan struct{})}
	defer func() {
		select {
		case <-barrier.release:
		default:
			close(barrier.release)
		}
	}()
	type outcome struct {
		change core.Change
		err    error
	}
	done := make(chan outcome, 1)
	go func() {
		change, err := c.Write(ctx, "alice", core.Path("/alias/new"), io.MultiReader(strings.NewReader("new"), barrier), ptr(3), policy)
		done <- outcome{change, err}
	}()
	await(t, barrier.started)
	if _, err := os.Stat(filepath.Join(root, "new")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("published before EOF", err)
	}
	_, createErr := c.Create(ctx, "alice", core.Path("/files/new"), strings.NewReader("peer"), ptr(4))
	close(barrier.release)
	got := <-done
	if createErr != nil || got.change.Effect != core.Unchanged || core.CodeOf(got.err) != core.PreconditionFailed {
		t.Fatal(createErr, got)
	}
	if data, err := os.ReadFile(filepath.Join(root, "new")); err != nil || string(data) != "peer" {
		t.Fatal(string(data), err)
	}
	probe := &copyBody{Reader: strings.NewReader("x")}
	change, err := c.Write(ctx, "alice", core.Path("/files/new"), probe, ptr(1), policy)
	if change.Effect != core.Unchanged || core.CodeOf(err) != core.PreconditionFailed || probe.reads.Load() != 0 {
		t.Fatal(change, err, probe.reads.Load())
	}
	if change, err = c.Write(ctx, "alice", core.Path("/files/failed"), strings.NewReader("x"), ptr(2), policy); err == nil || change.Effect != core.Unchanged {
		t.Fatal(change, err)
	}
	if _, err := os.Stat(filepath.Join(root, "failed")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("partial exclusive publication", err)
	}
	change, err = c.Write(ctx, "alice", core.Path("/files/complete"), strings.NewReader("complete"), ptr(8), policy)
	if err != nil || change.Effect != core.Applied || change.Before != core.TargetMissing {
		t.Fatal("reservation or commit leaked", change, err)
	}
	if entries, err := os.ReadDir(staging); err != nil || len(entries) != 0 {
		t.Fatal("private link leaked", entries, err)
	}
	if change, err = c.Write(ctx, "alice", core.Path("/files/invalid"), probe, ptr(1), core.WriteOptions{Policy: core.Publish(core.IfAbsent(), core.IfAbsent())}); core.CodeOf(err) != core.InvalidRequest || change.Effect != core.Unchanged || probe.reads.Load() != 0 {
		t.Fatal(change, err)
	}
}
