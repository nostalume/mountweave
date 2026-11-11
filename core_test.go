package core_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/local"
)

func allow(ctx context.Context, p core.Principal, a core.Action, targetAddress core.Address) error {
	if p != "alice" {
		return errors.New("denied")
	}
	return nil
}
func newCore(t *testing.T, opts core.Options) *core.Core {
	t.Helper()
	if opts.Authorize == nil {
		opts.Authorize = allow
	}
	c, err := core.New(opts)
	if err != nil {
		t.Fatal(err)
	}
	return c
}
func closeCore(t *testing.T, c *core.Core) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := c.Close(ctx); err != nil {
		t.Error(err)
	}
}
func mountLocal(t *testing.T, c *core.Core, root string) *core.Mount {
	t.Helper()
	d, err := local.New(root)
	if err != nil {
		t.Fatal(err)
	}
	m, err := c.Install("/files", d)
	if err != nil {
		_ = d.Close()
		t.Fatal(err)
	}
	return m
}
func code(t *testing.T, err error, want core.Code) {
	t.Helper()
	if err == nil || core.CodeOf(err) != want {
		t.Fatalf("error %v, want code %s", err, want)
	}
}
func await(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatal("resource did not complete")
	}
}
func write(t *testing.T, dir, name string, data []byte) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name), data, 0600); err != nil {
		t.Fatal(err)
	}
}

func TestPaths(t *testing.T) {
	for _, p := range []string{"", "relative", "//a", "/a/", "/a//b", "/.", "/a/..", "/a\\b", "/a\x00b", "/a\x7fb", "/\xff", strings.Repeat("/x", 65), "/" + strings.Repeat("x", 4096)} {
		if core.ValidatePath(p) == nil {
			t.Errorf("accepted %q", p)
		}
	}
	for _, p := range []string{"/", "/a", "/space % + &", "/utf8-\u6d4b\u8bd5-\U0001f680", "/literal%2e%2e"} {
		if err := core.ValidatePath(p); err != nil {
			t.Errorf("rejected %q: %v", p, err)
		}
	}
}
func TestIndependentLocalReadAndRange(t *testing.T) {
	root := t.TempDir()
	data := bytes.Repeat([]byte("0123456789"), 7000)
	write(t, root, "CaseName.bin", data)
	write(t, root, "empty", nil)
	c := newCore(t, core.Options{})
	defer closeCore(t, c)
	mountLocal(t, c, root)
	parent, err := c.Stat(context.Background(), "alice", core.Path("/"))
	if err != nil || !parent.Virtual || parent.Kind != core.Directory || parent.Size != nil {
		t.Fatalf("virtual root: %+v %v", parent, err)
	}
	entry, err := c.Stat(context.Background(), "alice", core.Path("/files/CaseName.bin"))
	if err != nil || entry.Size == nil || *entry.Size != int64(len(data)) {
		t.Fatalf("stat: %+v %v", entry, err)
	}
	_, err = c.Open(context.Background(), "alice", core.Path("/files/casename.bin"), core.OpenOptions{})
	code(t, err, core.NotFound)
	size := len(data)
	for _, test := range []struct {
		r          *core.Range
		start, end int
	}{
		{nil, 0, size},
		{&core.Range{Start: 65535, End: 65537}, 65535, 65538},
		{&core.Range{Start: int64(size - 7), End: -1}, size - 7, size},
		{&core.Range{Start: 17, End: -1, Suffix: true}, size - 17, size},
		{&core.Range{Start: int64(size - 7), End: int64(size + 7)}, size - 7, size},
		{&core.Range{Start: int64(size + 1), End: -1, Suffix: true}, 0, size},
	} {
		o, err := c.Open(context.Background(), "alice", entry.Address, core.OpenOptions{Range: test.r})
		if err != nil {
			t.Fatal(err)
		}
		got, err := io.ReadAll(o.Body)
		if err = errors.Join(err, o.Body.Close()); err != nil || !bytes.Equal(got, data[test.start:test.end]) {
			t.Fatalf("range %v returned %d bytes: %v", test.r, len(got), err)
		}
		want := core.ContentRange{Start: int64(test.start), End: int64(test.end - 1), Total: int64(size)}
		if test.r != nil && (o.Range == nil || *o.Range != want) {
			t.Fatal("range metadata", o.Range, want)
		}
	}
	empty, err := c.Open(context.Background(), "alice", core.Path("/files/empty"), core.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	b, err := io.ReadAll(empty.Body)
	if err != nil || len(b) != 0 {
		t.Fatalf("empty: %q %v", b, err)
	}
	_, err = c.Open(context.Background(), "alice", core.Path("/files/empty"), core.OpenOptions{Range: &core.Range{Start: 0, End: 0}})
	code(t, err, core.RangeNotSatisfiable)
	write(t, root, "CaseName.bin", []byte("new"))
	o, err := c.Open(context.Background(), "alice", core.Path(entry.Address.Value()), core.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if *o.Length != 3 {
		t.Fatal("used old metadata length")
	}
	_ = o.Body.Close()
	_, err = c.Open(context.Background(), "alice", core.Path("/files"), core.OpenOptions{})
	code(t, err, core.NotFile)
}
func TestLinksAndNames(t *testing.T) {
	base := t.TempDir()
	root := filepath.Join(base, "root")
	if err := os.Mkdir(root, 0700); err != nil {
		t.Fatal(err)
	}
	write(t, root, "Exact.txt", []byte("inside"))
	write(t, base, "outside.txt", []byte("outside"))
	write(t, root, "utf8-\u6d4b\u8bd5.txt", []byte("unicode"))
	c := newCore(t, core.Options{})
	defer closeCore(t, c)
	mountLocal(t, c, root)
	if err := os.Symlink("Exact.txt", filepath.Join(root, "inside-link")); err != nil {
		t.Skipf("symlink creation unavailable: %v", err)
	}
	if err := os.Symlink("../outside.txt", filepath.Join(root, "outside-link")); err != nil {
		t.Fatal(err)
	}
	if o, err := c.Open(context.Background(), "alice", core.Path("/files/inside-link"), core.OpenOptions{}); err == nil {
		_ = o.Body.Close()
		t.Fatal("inside link opened")
	}
	if _, err := c.Open(context.Background(), "alice", core.Path("/files/outside-link"), core.OpenOptions{}); err == nil {
		t.Fatal("outside link opened")
	}
	if _, err := c.Stat(context.Background(), "alice", core.Path("/files/utf8-\u6d4b\u8bd5.txt")); err != nil {
		t.Fatal(err)
	}
	changed, err := c.Remove(context.Background(), "alice", core.Path("/files/outside-link"))
	if err != nil || changed.Effect != core.Applied {
		t.Fatal(changed, err)
	}
	outside, err := os.ReadFile(filepath.Join(base, "outside.txt"))
	if err != nil || string(outside) != "outside" {
		t.Fatal("link removal changed its target", err)
	}
}
func TestPaginationReplayScopeAndRetirement(t *testing.T) {
	root := t.TempDir()
	remaining := map[string]bool{"a": true, "b": true, "c": true, "d": true, "e": true}
	for name := range remaining {
		write(t, root, name, nil)
	}
	c := newCore(t, core.Options{})
	defer closeCore(t, c)
	m := mountLocal(t, c, root)
	req := core.ListRequest{Query: core.Children(core.Path("/files")), Limit: 2, CursorTTL: time.Minute}
	first, err := c.List(context.Background(), "alice", req)
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Items) != 2 || first.NextCursor == nil {
		t.Fatalf("first page: %+v", first)
	}
	req.Cursor = *first.NextCursor
	second, err := c.List(context.Background(), "alice", req)
	if err != nil || len(second.Items) != 2 || second.NextCursor == nil {
		t.Fatal("second page", second, err)
	}
	saved := second.Items[0].Name
	next := *second.NextCursor
	second.Items[0].Name = "caller mutation"
	*second.NextCursor = "caller mutation"
	replay, err := c.List(context.Background(), "alice", req)
	if err != nil || len(replay.Items) != 2 || replay.Items[0].Name != saved || replay.NextCursor == nil || *replay.NextCursor != next {
		t.Fatalf("replay: %+v %v", replay, err)
	}
	_, err = c.List(context.Background(), "bob", req)
	code(t, err, core.CursorUnavailable)
	changed := req
	changed.Limit = 1
	_, err = c.List(context.Background(), "alice", changed)
	code(t, err, core.CursorUnavailable)
	changed = req
	changed.Query = core.Children(core.Path("/"))
	_, err = c.List(context.Background(), "alice", changed)
	code(t, err, core.CursorUnavailable)
	old := req.Cursor
	req.Cursor = next
	items := append(first.Items, replay.Items...)
	var terminal core.Page
	// A short native batch can precede the terminal EOF page.
	for range 2 {
		terminal, err = c.List(context.Background(), "alice", req)
		if err != nil {
			t.Fatal(err)
		}
		items = append(items, terminal.Items...)
		if terminal.NextCursor == nil {
			break
		}
		req.Cursor = *terminal.NextCursor
	}
	if terminal.NextCursor != nil {
		t.Fatal("enumeration did not reach EOF", terminal)
	}
	tail := req.Cursor
	for _, item := range items {
		if !remaining[item.Name] {
			t.Fatal("duplicate or unexpected entry", item.Name)
		}
		delete(remaining, item.Name)
	}
	if len(remaining) != 0 {
		t.Fatal("listing omitted entries", remaining)
	}
	req.Cursor = old
	_, err = c.List(context.Background(), "alice", req)
	code(t, err, core.CursorUnavailable)
	req.Cursor = tail
	final, err := c.List(context.Background(), "alice", req)
	if err != nil || !reflect.DeepEqual(final, terminal) {
		t.Fatal("last page replay", final, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err = m.Retire(ctx); err != nil {
		t.Fatal(err)
	}
	_, err = c.List(context.Background(), "alice", req)
	code(t, err, core.CursorUnavailable)
}
func TestCursorExpiryAndCapacity(t *testing.T) {
	root := t.TempDir()
	write(t, root, "a", nil)
	write(t, root, "b", nil)
	c := newCore(t, core.Options{MaxCursors: 1})
	defer closeCore(t, c)
	mountLocal(t, c, root)
	first, err := c.List(context.Background(), "alice", core.ListRequest{Query: core.Children(core.Path("/files")), Limit: 1, CursorTTL: 100 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.List(context.Background(), "alice", core.ListRequest{Query: core.Children(core.Path("/files")), Limit: 1, CursorTTL: 100 * time.Millisecond})
	code(t, err, core.CapacityExceeded)
	<-time.After(time.Until(*first.CursorExpiresAt) + 20*time.Millisecond)
	_, err = c.List(context.Background(), "alice", core.ListRequest{Query: core.Children(core.Path("/files")), Cursor: *first.NextCursor})
	code(t, err, core.CursorUnavailable)
	deadline := time.Now().Add(time.Second)
	for {
		_, err = c.List(context.Background(), "alice", core.ListRequest{Query: core.Children(core.Path("/files")), Limit: 1, CursorTTL: 100 * time.Millisecond})
		if err == nil {
			break
		}
		if core.CodeOf(err) != core.CapacityExceeded || time.Now().After(deadline) {
			t.Fatal(err)
		}
		time.Sleep(time.Millisecond)
	}
}

type fakeDriver struct {
	stat   func(context.Context, string) (core.Metadata, error)
	open   func(context.Context, string, *core.Range) (core.Opened, error)
	enum   func(context.Context, string) (core.Iterator, error)
	close  func() error
	calls  atomic.Int32
	closes atomic.Int32
}

func (d *fakeDriver) Stat(ctx context.Context, pAddress core.NativeAddress, selections ...core.ReadSelection) (meta core.Metadata, err error) {
	defer func() {
		if err == nil && len(selections) == 1 {
			err = selections[0].Check(meta.Evidence)
		}
	}()
	if pAddress.Namespace() != core.TreeNamespace {
		return core.Metadata{}, core.Fail(core.UnsupportedOperation, nil)
	}
	p := pAddress.Value()

	d.calls.Add(1)
	if d.stat != nil {
		return d.stat(ctx, p)
	}
	n := int64(3)
	return core.Metadata{Name: "a", Kind: core.File, Size: &n}, nil
}
func (d *fakeDriver) Open(ctx context.Context, pAddress core.NativeAddress, options core.OpenOptions) (opened core.Opened, err error) {
	r := options.Range
	defer func() {
		if err == nil {
			err = options.Selection.Check(opened.Metadata.Evidence)
		}
	}()
	if pAddress.Namespace() != core.TreeNamespace {
		return core.Opened{}, core.Fail(core.UnsupportedOperation, nil)
	}
	p := pAddress.Value()

	d.calls.Add(1)
	if d.open != nil {
		return d.open(ctx, p, r)
	}
	n := int64(3)
	return core.Opened{Metadata: core.Metadata{Name: "a", Kind: core.File, Size: &n}, Length: &n, Body: io.NopCloser(strings.NewReader("abc"))}, nil
}
func (d *fakeDriver) Enumerate(ctx context.Context, pAddress core.NativeQuery) (core.Iterator, error) {
	if pAddress.Address().Namespace() != core.TreeNamespace || pAddress.Delimiter() != "" || pAddress.StartAfter() != "" {
		return nil, core.Fail(core.UnsupportedOperation, nil)
	}
	p := pAddress.Address().Value()

	d.calls.Add(1)
	if d.enum != nil {
		return d.enum(ctx, p)
	}
	return nil, errors.New("unexpected enumeration")
}
func (d *fakeDriver) Close() error {
	d.closes.Add(1)
	if d.close != nil {
		return d.close()
	}
	return nil
}
func installFake(t *testing.T, c *core.Core, d *fakeDriver) *core.Mount {
	t.Helper()
	m, err := c.Install("/files", d)
	if err != nil {
		t.Fatal(err)
	}
	return m
}
func TestAuthorizationBeforeDriverAndMountIdentity(t *testing.T) {
	c := newCore(t, core.Options{})
	defer closeCore(t, c)
	d := &fakeDriver{}
	m := installFake(t, c, d)
	_, err := c.Open(context.Background(), "bob", core.Path("/files/a"), core.OpenOptions{})
	code(t, err, core.PermissionDenied)
	if d.calls.Load() != 0 {
		t.Fatal("I/O before authorization")
	}
	for _, action := range []func() (core.Change, error){func() (core.Change, error) { return c.Mkdir(context.Background(), "bob", core.Path("/files/new")) }, func() (core.Change, error) { return c.Remove(context.Background(), "bob", core.Path("/files/a")) }, func() (core.Change, error) {
		return c.Create(context.Background(), "bob", core.Path("/files/new"), strings.NewReader("x"), nil)
	}} {
		result, err := action()
		code(t, err, core.PermissionDenied)
		if result.Effect != core.Unchanged {
			t.Fatal(result)
		}
	}
	input := strings.NewReader("unread")
	result, err := c.Create(context.Background(), "alice", core.Path("/files/new"), input, nil)
	code(t, err, core.UnsupportedOperation)
	if result.Effect != core.Unchanged || input.Len() != 6 || d.calls.Load() != 0 {
		t.Fatal("unsupported mutation consumed input or called driver")
	}

	rejected := &fakeDriver{}
	_, err = c.Install("/files/child", rejected)
	code(t, err, core.PathAmbiguous)
	if rejected.closes.Load() != 0 {
		t.Fatal("core closed a rejected binding")
	}
	_ = rejected.Close()
	if err = m.Retire(context.Background()); err != nil {
		t.Fatal(err)
	}
	replacement := &fakeDriver{}
	installFake(t, c, replacement)
	if err = m.Retire(context.Background()); err != nil {
		t.Fatal(err)
	}
	o, err := c.Open(context.Background(), "alice", core.Path("/files/a"), core.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	_ = o.Body.Close()
	if replacement.closes.Load() != 0 {
		t.Fatal("old handle retired replacement")
	}
}

type blockingBody struct {
	started chan struct{}
	release chan struct{}
	closed  chan struct{}
	once    sync.Once
	closes  atomic.Int32
}

func (b *blockingBody) Read(p []byte) (int, error) {
	b.once.Do(func() { close(b.started) })
	<-b.release
	return 0, io.EOF
}
func (b *blockingBody) Close() error {
	if b.closes.Add(1) == 1 {
		close(b.closed)
	}
	return nil
}

type blockingFile struct{ *blockingBody }

func (f *blockingFile) WriteAt(ctx context.Context, p []byte, _ int64) (int, core.Change, error) {
	_, _ = f.Read(p)
	return 0, core.Change{Effect: core.Unchanged}, ctx.Err()
}
func (f *blockingFile) Append(ctx context.Context, p []byte) (int, core.Change, error) {
	return f.WriteAt(ctx, p, 0)
}
func (f *blockingFile) Truncate(context.Context, int64) (core.Change, error) {
	return core.Change{Effect: core.Unchanged}, core.Fail(core.UnsupportedOperation, nil)
}
func (f *blockingFile) Sync(context.Context) error { return nil }

func (f *blockingFile) ReadAt(_ context.Context, p []byte, _ int64) (int, error) { return f.Read(p) }
func (f *blockingFile) Stat(context.Context) (core.Metadata, error) {
	return core.Metadata{Name: "a", Kind: core.File}, nil
}
func TestRetirementWaitsForReadAcknowledgment(t *testing.T) {
	for _, mode := range []core.OpenMode{core.StreamRead, core.BoundRead, core.LiveEdit} {
		t.Run(string(mode), func(t *testing.T) {
			b := &blockingBody{started: make(chan struct{}), release: make(chan struct{}), closed: make(chan struct{})}
			var opened atomic.Int32
			d := &fakeDriver{open: func(context.Context, string, *core.Range) (core.Opened, error) {
				if opened.Add(1) == 1 {
					if mode != core.StreamRead {
						return core.Opened{Metadata: core.Metadata{Name: "a", Kind: core.File}, File: &blockingFile{b}, Binding: core.BoundObject, Change: core.Change{Effect: core.Unchanged}}, nil
					}
					return core.Opened{Metadata: core.Metadata{Name: "a", Kind: core.File}, Body: b}, nil
				}
				n := int64(3)
				return core.Opened{Metadata: core.Metadata{Name: "a", Kind: core.File}, Length: &n, Body: io.NopCloser(strings.NewReader("abc"))}, nil
			}}
			c := newCore(t, core.Options{})
			defer closeCore(t, c)
			m := installFake(t, c, d)
			defer func() {
				select {
				case <-b.release:
				default:
					close(b.release)
				}
			}()
			first, cancelFirst := context.WithCancel(context.Background())
			defer cancelFirst()
			o, err := c.Open(first, "alice", core.Path("/files/a"), core.OpenOptions{Mode: mode})
			if err != nil {
				t.Fatal(err)
			}
			readDone := make(chan struct{})
			call, cancelCall := context.WithCancel(context.Background())
			defer cancelCall()
			go func() {
				if mode != core.StreamRead {
					if mode == core.LiveEdit {
						_, _, _ = o.File.WriteAt(call, []byte("x"), 0)
					} else {
						_, _ = o.File.ReadAt(call, make([]byte, 1), 0)
					}
				} else {
					_, _ = o.Body.Read(make([]byte, 1))
				}
				close(readDone)
			}()
			await(t, b.started)
			peer, err := c.Open(context.Background(), "alice", core.Path("/files/a"), core.OpenOptions{})
			if err != nil {
				t.Fatal(err)
			}
			cancelFirst()
			if mode == core.StreamRead {
				await(t, b.closed)
			} else {
				cancelCall()
				select {
				case <-b.closed:
					t.Fatal("call cancellation closed File")
				default:
				}
			}
			data, err := io.ReadAll(peer.Body)
			if err != nil || string(data) != "abc" {
				t.Fatalf("independent read canceled: %q %v", data, err)
			}
			if d.closes.Load() != 0 {
				t.Fatal("shared driver closed before blocked read acknowledged")
			}
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
			defer cancel()
			if err = m.Retire(ctx); !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("retire: %v", err)
			}
			await(t, b.closed)
			if d.closes.Load() != 0 {
				t.Fatal("driver closed while read was still executing")
			}
			_, err = c.Open(context.Background(), "alice", core.Path("/files/a"), core.OpenOptions{})
			code(t, err, core.MountUnavailable)
			_, err = c.Install("/files", &fakeDriver{})
			code(t, err, core.PathAmbiguous)
			if b.closes.Load() != 1 {
				t.Fatal("duplicate close")
			}
			close(b.release)
			await(t, readDone)
			ctx2, stop := context.WithTimeout(context.Background(), time.Second)
			defer stop()
			if err = m.Retire(ctx2); err != nil {
				t.Fatal(err)
			}
			if d.closes.Load() != 1 {
				t.Fatal("driver was not closed exactly once")
			}

		})
	}
}

func TestRetirementDoesNotDuplicateBlockedDriverClose(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	d := &fakeDriver{close: func() error { close(started); <-release; return nil }}
	c := newCore(t, core.Options{})
	defer closeCore(t, c)
	m := installFake(t, c, d)
	for i := 0; i < 2; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		err := m.Retire(ctx)
		cancel()
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatal(err)
		}
	}
	await(t, started)
	if d.closes.Load() != 1 {
		t.Fatal("duplicate driver close")
	}
	close(release)
	if err := m.Retire(context.Background()); err != nil {
		t.Fatal(err)
	}
}
func TestOpenTimerDoesNotKillAdoptedBody(t *testing.T) {
	c := newCore(t, core.Options{OpenTimeout: 20 * time.Millisecond})
	defer closeCore(t, c)
	installFake(t, c, &fakeDriver{})
	o, err := c.Open(context.Background(), "alice", core.Path("/files/a"), core.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	<-time.After(50 * time.Millisecond)
	b, err := io.ReadAll(o.Body)
	if err != nil || string(b) != "abc" {
		t.Fatal(string(b), err)
	}
}
func TestOpenTimeoutCleansLateBody(t *testing.T) {
	for _, mode := range []core.OpenMode{core.StreamRead, core.BoundRead} {
		t.Run(string(mode), func(t *testing.T) {
			closed := atomic.Int32{}
			d := &fakeDriver{open: func(ctx context.Context, _ string, _ *core.Range) (core.Opened, error) {
				<-ctx.Done()
				resource := &countedBody{Reader: strings.NewReader("late"), closed: &closed}
				if mode == core.BoundRead {
					return core.Opened{File: &countedFile{resource}}, ctx.Err()
				}
				return core.Opened{Body: resource}, ctx.Err()
			}}
			c := newCore(t, core.Options{OpenTimeout: 10 * time.Millisecond})
			defer closeCore(t, c)
			installFake(t, c, d)
			_, err := c.Open(context.Background(), "alice", core.Path("/files/a"), core.OpenOptions{Mode: mode})
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatal(err)
			}
			if closed.Load() != 1 {
				t.Fatal("late body was not closed")
			}

		})
	}
}

type countedBody struct {
	io.Reader
	closed *atomic.Int32
	err    error
}

func (b *countedBody) Close() error { b.closed.Add(1); return b.err }

type countedFile struct{ *countedBody }

func (f *countedFile) ReadAt(_ context.Context, p []byte, offset int64) (int, error) {
	return f.Reader.(io.ReaderAt).ReadAt(p, offset)
}
func (f *countedFile) Stat(context.Context) (core.Metadata, error) {
	return core.Metadata{Name: "a", Kind: core.File}, nil
}
func TestShortReadAndCleanupErrorSurvive(t *testing.T) {
	for _, mode := range []core.OpenMode{core.StreamRead, core.BoundRead} {
		t.Run(string(mode), func(t *testing.T) {
			cleanup := errors.New("cleanup failed")
			closed := atomic.Int32{}
			d := &fakeDriver{open: func(context.Context, string, *core.Range) (core.Opened, error) {
				n := int64(9)
				resource := &countedBody{Reader: strings.NewReader("abc"), closed: &closed, err: cleanup}
				if mode == core.BoundRead {
					return core.Opened{Metadata: core.Metadata{Name: "a", Kind: core.File}, File: &countedFile{resource}, Binding: core.BoundObject}, nil
				}
				return core.Opened{Metadata: core.Metadata{Name: "a", Kind: core.File}, Length: &n, Body: resource}, nil
			}}
			c := newCore(t, core.Options{})
			m := installFake(t, c, d)
			o, err := c.Open(context.Background(), "alice", core.Path("/files/a"), core.OpenOptions{Mode: mode})
			if err != nil {
				t.Fatal(err)
			}
			short := io.ErrUnexpectedEOF
			if mode == core.BoundRead {
				n, readErr := o.File.ReadAt(context.Background(), make([]byte, 9), 0)
				if n != 3 || readErr != io.EOF || closed.Load() != 0 {
					t.Fatal(n, readErr, closed.Load())
				}
				short = io.EOF
				err = errors.Join(readErr, o.File.Close())
			} else {
				_, err = io.Copy(io.Discard, o.Body)
			}
			if !errors.Is(err, short) || !errors.Is(err, cleanup) {
				t.Fatalf("lost failure: %v", err)
			}
			if mode == core.BoundRead {
				_ = o.File.Close()
			} else {
				_ = o.Body.Close()
			}
			if closed.Load() != 1 {
				t.Fatal("duplicate close")
			}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
			defer cancel()
			err = m.Retire(ctx)
			if !errors.Is(err, cleanup) || !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("unacknowledged cleanup: %v", err)
			}
			if d.closes.Load() != 0 {
				t.Fatal("closed driver after unconfirmed cleanup")
			}

		})
	}
}

type cancelBody struct {
	started   chan struct{}
	released  chan struct{}
	once      sync.Once
	closeOnce sync.Once
}

func (b *cancelBody) Read(p []byte) (int, error) {
	b.once.Do(func() { close(b.started) })
	<-b.released
	return 0, io.EOF
}
func (b *cancelBody) Close() error { b.closeOnce.Do(func() { close(b.released) }); return nil }
func TestCancellationIsNotSuccessfulEOF(t *testing.T) {
	for _, copyPath := range []bool{false, true} {
		t.Run(fmt.Sprint(copyPath), func(t *testing.T) {
			b := &cancelBody{started: make(chan struct{}), released: make(chan struct{})}
			d := &fakeDriver{open: func(context.Context, string, *core.Range) (core.Opened, error) {
				return core.Opened{Metadata: core.Metadata{Name: "a", Kind: core.File}, Body: b}, nil
			}}
			c := newCore(t, core.Options{})
			defer closeCore(t, c)
			installFake(t, c, d)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			o, err := c.Open(ctx, "alice", core.Path("/files/a"), core.OpenOptions{})
			if err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() {
				var err error
				if copyPath {
					_, err = io.Copy(io.Discard, o.Body)
				} else {
					_, err = io.ReadAll(o.Body)
				}
				done <- err
			}()
			await(t, b.started)
			cancel()
			select {
			case err := <-done:
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("cancellation became successful EOF: %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("cooperative cancellation did not complete")
			}
		})
	}
}

type controlledIterator struct {
	calls        atomic.Int32
	closed       atomic.Int32
	next         func(context.Context, int) ([]core.Metadata, error)
	closeErr     error
	nextStarted  chan struct{}
	closeStarted chan struct{}
	releaseClose chan struct{}
}

func (i *controlledIterator) Next(ctx context.Context, n int) ([]core.Metadata, error) {
	call := i.calls.Add(1)
	if i.next != nil {
		return i.next(ctx, n)
	}
	if call == 1 {
		return []core.Metadata{{Name: "a", Kind: core.File}}, nil
	}
	close(i.nextStarted)
	<-ctx.Done()
	// Model a late native success after cancellation; it must not publish.
	return []core.Metadata{{Name: "late", Kind: core.File}}, nil
}
func (i *controlledIterator) Close() error {
	i.closed.Add(1)
	if i.closeStarted != nil {
		close(i.closeStarted)
	}
	if i.releaseClose != nil {
		<-i.releaseClose
	}
	return i.closeErr
}
func TestCursorDoesNotFreeUnacknowledgedClose(t *testing.T) {
	for _, trigger := range []string{"request", "mutation", "close-failure"} {
		t.Run(trigger, func(t *testing.T) {
			iterator := &controlledIterator{nextStarted: make(chan struct{}), closeStarted: make(chan struct{}), releaseClose: make(chan struct{})}
			if trigger == "close-failure" {
				iterator.closeErr = errors.New("iterator cleanup failed")
			}
			d := &fakeDriver{enum: func(context.Context, string) (core.Iterator, error) { return iterator, nil }}
			c := newCore(t, core.Options{MaxCursors: 1})
			if iterator.closeErr == nil {
				defer closeCore(t, c)
			}
			m, installErr := c.Install("/files", &directoryDriver{fakeDriver: d, mkdir: func(context.Context, string) (core.Change, error) { return core.Change{Effect: core.Applied}, nil }})
			if installErr != nil {
				t.Fatal(installErr)
			}
			first, err := c.List(context.Background(), "alice", core.ListRequest{Query: core.Children(core.Path("/files")), Limit: 1, CursorTTL: time.Minute})
			if err != nil {
				t.Fatal(err)
			}
			requestCtx, cancelRequest := context.WithCancel(context.Background())
			defer cancelRequest()
			nextDone := make(chan error, 1)
			go func() {
				_, err := c.List(requestCtx, "alice", core.ListRequest{Query: core.Children(core.Path("/files")), Cursor: *first.NextCursor})
				nextDone <- err
			}()
			await(t, iterator.nextStarted)
			_, err = c.List(context.Background(), "alice", core.ListRequest{Query: core.Children(core.Path("/files")), Cursor: *first.NextCursor})
			code(t, err, core.CapacityExceeded)
			if trigger != "mutation" {
				cancelRequest()
			} else {
				result, changeErr := c.Mkdir(context.Background(), "alice", core.Path("/files/new"))
				if changeErr != nil || result.Effect != core.Applied {
					t.Fatal(result, changeErr)
				}
			}
			select {
			case err := <-nextDone:
				if !errors.Is(err, context.Canceled) {
					t.Fatal(err)
				}
			case <-time.After(time.Second):
				t.Fatal("Next did not observe cancellation")
			}
			await(t, iterator.closeStarted)
			code(t, c.CheckCursor(context.Background(), "alice", core.ListRequest{Query: core.Children(core.Path("/files")), Cursor: *first.NextCursor}), core.CursorUnavailable)
			_, err = c.List(context.Background(), "alice", core.ListRequest{Query: core.Children(core.Path("/")), Limit: 1, CursorTTL: time.Minute})
			code(t, err, core.CapacityExceeded)
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
			err = m.Retire(ctx)
			cancel()
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatal(err)
			}
			if d.closes.Load() != 0 || iterator.closed.Load() != 1 {
				t.Fatal("lost pending iterator ownership")
			}
			close(iterator.releaseClose)
			if iterator.closeErr != nil {
				ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
				defer cancel()
				if err = c.Close(ctx); !errors.Is(err, iterator.closeErr) || !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("lost failed iterator ownership: %v", err)
				}
				if d.closes.Load() != 0 || iterator.closed.Load() != 1 {
					t.Fatal("failed cleanup released the driver or retried Close")
				}
				return
			}
			if err = m.Retire(context.Background()); err != nil {
				t.Fatal(err)
			}

		})
	}
}
func TestInvalidRangeResponseIsClosed(t *testing.T) {
	two, ten := int64(2), int64(10)
	for _, tt := range []struct {
		name         string
		size, length *int64
		actual       *core.ContentRange
		valid        bool
	}{
		{"shifted-range", nil, &two, &core.ContentRange{Start: 1, End: 2, Total: 10}, false},
		{"full-size", &ten, &two, nil, false},
		{"range-size", &two, &two, &core.ContentRange{Start: 0, End: 1, Total: 10}, false},
		{"unknown-length", &two, nil, nil, true},
		{"unknown-size", nil, &two, &core.ContentRange{Start: 0, End: 1, Total: 10}, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var closes atomic.Int32
			d := &fakeDriver{open: func(context.Context, string, *core.Range) (core.Opened, error) {
				return core.Opened{Metadata: core.Metadata{Name: "a", Kind: core.File, Size: tt.size}, Body: &countedBody{Reader: strings.NewReader("ab"), closed: &closes}, Length: tt.length, Range: tt.actual}, nil
			}}
			c := newCore(t, core.Options{})
			defer closeCore(t, c)
			installFake(t, c, d)
			opened, err := c.Open(context.Background(), "alice", core.Path("/files/a"), core.OpenOptions{Range: &core.Range{Start: 0, End: 1}})
			if tt.valid {
				if err != nil {
					t.Fatal(err)
				}
				data, err := io.ReadAll(opened.Body)
				if err != nil || string(data) != "ab" || !reflect.DeepEqual(opened.Length, tt.length) || !reflect.DeepEqual(opened.Range, tt.actual) {
					t.Fatal("body facts changed", opened, string(data), err)
				}
			} else {
				code(t, err, core.InvalidUpstreamResponse)
			}
			if closes.Load() != 1 {
				t.Fatal("body was leaked or closed twice")
			}
		})
	}
}

func TestCoreCloseNeverAcknowledgesPendingMount(t *testing.T) {
	c := newCore(t, core.Options{})
	release := make(chan struct{})
	closed := make(chan struct{})
	var starts atomic.Int32
	blocked := &fakeDriver{close: func() error { starts.Add(1); <-release; close(closed); return nil }}
	if _, err := c.Install("/blocked", blocked); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 8; i++ {
		if _, err := c.Install(fmt.Sprintf("/ready%d", i), &fakeDriver{}); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := c.Close(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("pending shutdown reported completion: %v", err)
	}
	close(release)
	if err := c.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	await(t, closed)
	if starts.Load() != 1 {
		t.Fatal("duplicate shutdown worker")
	}
}

func TestExplicitCursorRelease(t *testing.T) {
	root := t.TempDir()
	write(t, root, "a", nil)
	write(t, root, "b", nil)
	var revoked atomic.Bool
	c := newCore(t, core.Options{MaxCursors: 1, Authorize: func(context.Context, core.Principal, core.Action, core.Address) error {
		if revoked.Load() {
			return errors.New("revoked")
		}
		return nil
	}})
	defer closeCore(t, c)
	mountLocal(t, c, root)
	first, err := c.List(context.Background(), "alice", core.ListRequest{Query: core.Children(core.Path("/files")), Limit: 1, CursorTTL: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	code(t, c.ReleaseCursor(context.Background(), "bob", *first.NextCursor), core.CursorUnavailable)
	revoked.Store(true)
	if err = c.ReleaseCursor(context.Background(), "alice", *first.NextCursor); err != nil {
		t.Fatal(err)
	}
	revoked.Store(false)
	_, err = c.List(context.Background(), "alice", core.ListRequest{Query: core.Children(core.Path("/files")), Cursor: *first.NextCursor})
	code(t, err, core.CursorUnavailable)
	if _, err = c.List(context.Background(), "alice", core.ListRequest{Query: core.Children(core.Path("/files")), Limit: 1, CursorTTL: time.Minute}); err != nil {
		t.Fatal("released cursor still occupied capacity", err)
	}
}

func TestRenamedRootRemainsAnchored(t *testing.T) {
	base := t.TempDir()
	root := filepath.Join(base, "root")
	outside := filepath.Join(base, "outside")
	moved := filepath.Join(base, "moved")
	for _, p := range []string{root, outside} {
		if err := os.Mkdir(p, 0700); err != nil {
			t.Fatal(err)
		}
	}
	write(t, root, "item", []byte("real"))
	write(t, outside, "item", []byte("unrelated outside bytes"))
	c := newCore(t, core.Options{})
	defer closeCore(t, c)
	mountLocal(t, c, root)
	if err := os.Rename(root, moved); err != nil {
		t.Skipf("moving an open root unavailable: %v", err)
	}
	if err := os.Symlink(outside, root); err != nil {
		t.Skipf("root replacement link unavailable: %v", err)
	}
	page, err := c.List(context.Background(), "alice", core.ListRequest{Query: core.Children(core.Path("/files")), CursorTTL: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Items) != 1 || page.Items[0].Size == nil || *page.Items[0].Size != 4 {
		t.Fatalf("metadata escaped the opened root: %+v", page)
	}
	o, err := c.Open(context.Background(), "alice", core.Path("/files/item"), core.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(o.Body)
	if err != nil || string(body) != "real" {
		t.Fatal(string(body), err)
	}
}

func TestCursorLifetimeBelongsToEnumeration(t *testing.T) {
	c := newCore(t, core.Options{})
	defer closeCore(t, c)
	d := &fakeDriver{}
	installFake(t, c, d)
	if _, err := c.Stat(context.Background(), "alice", core.Path("/files/a")); err != nil {
		t.Fatal(err)
	}
	opened, err := c.Open(context.Background(), "alice", core.Path("/files/a"), core.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err = opened.Body.Close(); err != nil {
		t.Fatal(err)
	}
	before := d.calls.Load()
	_, err = c.List(context.Background(), "alice", core.ListRequest{Query: core.Children(core.Path("/files"))})
	code(t, err, core.InvalidRequest)
	if d.calls.Load() != before {
		t.Fatal("missing enumeration lifetime reached driver")
	}
	iterator := &controlledIterator{}
	iterator.next = func(ctx context.Context, n int) ([]core.Metadata, error) {
		if err := ctx.Err(); err != nil || n != 1 {
			t.Fatalf("invalid native call scope or hint: %v, %d", err, n)
		}
		if iterator.calls.Load() == 1 {
			return nil, nil // An empty native page still has progress.
		}
		return []core.Metadata{{Name: "a", Kind: core.File}, {Name: "b", Kind: core.File}, {Name: "c", Kind: core.File}}, io.EOF
	}
	d.enum = func(context.Context, string) (core.Iterator, error) { return iterator, nil }
	page, err := c.List(context.Background(), "alice", core.ListRequest{Query: core.Children(core.Path("/files")), Limit: 1, CursorTTL: time.Minute})
	if err != nil || len(page.Items) != 0 || page.NextCursor == nil {
		t.Fatalf("first page: %v %v", page, err)
	}
	req := core.ListRequest{Query: core.Children(core.Path("/files")), Cursor: *page.NextCursor, CursorTTL: time.Second}
	_, err = c.List(context.Background(), "alice", req)
	code(t, err, core.CursorUnavailable)
	if err = c.CheckCursor(context.Background(), "alice", req); core.CodeOf(err) != core.CursorUnavailable {
		t.Fatal(err)
	}
	req.CursorTTL = 0
	for _, name := range []string{"a", "b", "c"} {
		if err = c.CheckCursor(context.Background(), "alice", req); err != nil {
			t.Fatal(err)
		}
		page, err = c.List(context.Background(), "alice", req)
		if err != nil || len(page.Items) != 1 || page.Items[0].Name != name {
			t.Fatalf("native batch projection: %+v %v", page, err)
		}
		replay, err := c.List(context.Background(), "alice", req)
		if err != nil || !reflect.DeepEqual(page, replay) || iterator.calls.Load() != 2 {
			t.Fatalf("replay advanced the native iterator: %+v %v", replay, err)
		}
		if name != "c" {
			if page.NextCursor == nil {
				t.Fatal("lost native batch remainder")
			}
			req.Cursor = *page.NextCursor
		}
	}
	if page.NextCursor != nil || iterator.closed.Load() != 1 {
		t.Fatal("terminal native batch was not closed")
	}
	if err = c.ReleaseCursor(context.Background(), "alice", req.Cursor); err != nil || iterator.closed.Load() != 1 {
		t.Fatal("releasing terminal replay repeated Close", err)
	}
}
