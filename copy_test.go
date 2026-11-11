package core_test

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/local"
	"github.com/nostalume/mountweave/task"
)

func copyPermit(_ context.Context, p core.Principal, a core.Action, _ core.Address) error {
	if p == "caller" && (a == core.ReadAction || a == core.CreateAction) {
		return nil
	}
	return errors.New("denied")
}

func copyCore(t *testing.T, authorize core.Authorizer) *core.Core {
	t.Helper()
	c := newCore(t, core.Options{Authorize: authorize})
	t.Cleanup(func() { closeCore(t, c) })
	return c
}

func installCopyMount(t *testing.T, c *core.Core, p string, d core.Driver) *core.Mount {
	t.Helper()
	m, err := c.Install(p, d)
	if err != nil {
		t.Fatal(errors.Join(err, d.Close()))
	}
	return m
}

type copyWriter struct {
	core.Driver
	create  func(context.Context, string, io.Reader, *int64) (core.Change, error)
	publish func(context.Context, string, io.Reader, *int64) (core.Change, error)
}

func (d *copyWriter) Create(ctx context.Context, pAddress core.NativeAddress, r io.Reader, n *int64) (core.Change, error) {
	if pAddress.Namespace() != core.TreeNamespace {
		return core.Change{Effect: core.Unchanged}, core.Fail(core.UnsupportedOperation, nil)
	}
	p := pAddress.Value()

	return d.create(ctx, p, r, n)
}

func (d *copyWriter) Publish(ctx context.Context, pAddress core.NativeAddress, r io.Reader, n *int64, _ core.PublicationOptions) (core.Change, error) {
	if pAddress.Namespace() != core.TreeNamespace {
		return core.Change{Effect: core.Unchanged}, core.Fail(core.UnsupportedOperation, nil)
	}
	p := pAddress.Value()

	if d.publish == nil {
		return core.Change{Effect: core.Unchanged}, core.Fail(core.UnsupportedOperation, nil)
	}
	return d.publish(ctx, p, r, n)
}

type copyBody struct {
	io.Reader
	reads, closes atomic.Int32
	close         func() error
}

func (b *copyBody) Read(p []byte) (int, error) { b.reads.Add(1); return b.Reader.Read(p) }
func (b *copyBody) Close() error {
	b.closes.Add(1)
	if b.close != nil {
		return b.close()
	}
	return nil
}

func copySource(b *copyBody, length *int64) *fakeDriver {
	return &fakeDriver{stat: func(context.Context, string) (core.Metadata, error) {
		return core.Metadata{}, errors.New("unexpected stat")
	}, open: func(_ context.Context, p string, r *core.Range) (core.Opened, error) {
		if p != "/a" || r != nil {
			return core.Opened{}, errors.New("unexpected source request")
		}
		size := int64(99)
		if length != nil {
			size = *length
		}
		return core.Opened{Metadata: core.Metadata{Name: "a", Kind: core.File, Size: &size}, Body: b, Length: length}, nil
	}}
}

func TestCopyLocal(t *testing.T) {
	from, to := t.TempDir(), t.TempDir()
	c := copyCore(t, func(ctx context.Context, p core.Principal, a core.Action, targetAddress core.Address) error {
		target := targetAddress.Value()
		if p == "writer" && (a == core.ReadAction || a == core.CreateAction || a == core.WriteAction) {
			return nil
		}
		return copyPermit(ctx, p, a, core.Path(target))
	})
	for _, binding := range []struct{ path, directory string }{{"/from", from}, {"/to", to}} {
		d, err := local.New(binding.directory, local.Options{StagingDirectory: t.TempDir(), MaxStagingBytes: 32})
		if err != nil {
			t.Fatal(err)
		}
		installCopyMount(t, c, binding.path, d)
	}
	for name, data := range map[string]string{"a": "complete content", "empty": ""} {
		if err := os.WriteFile(filepath.Join(from, name), []byte(data), 0600); err != nil {
			t.Fatal(err)
		}
		change, err := c.Copy(context.Background(), "caller", core.Path("/from/"+name), core.Path("/to/"+name))
		if err != nil || change.Effect != core.Applied {
			t.Fatal(change, err)
		}
		for _, directory := range []string{from, to} {
			got, err := os.ReadFile(filepath.Join(directory, name))
			if err != nil || string(got) != data {
				t.Fatalf("unexpected file content: %q, %v", got, err)
			}
		}
	}
	results, err := c.CopyBatch(context.Background(), "caller", []core.CopyRequest{
		{Source: core.Path("/to/a"), Target: core.Path("/to/batch-b")},
		{Source: core.Path("/to/batch-b"), Target: core.Path("/to/batch-c")},
		{Source: core.Path("/to/a"), Target: core.Path("/to/batch-b")},
		// This conflict blocks the suffix while the failing item runs.
		{Source: core.Path("/to/a"), Target: core.Path("/to/batch-b")},
		{Source: core.Path("/to/a"), Target: core.Path("/to/not-started")},
	}, core.ExecutionOptions{Concurrency: 2})
	if len(results) != 3 || core.CodeOf(err) != core.AlreadyExists {
		t.Fatal(results, err)
	}
	for i, result := range results[:2] {
		if result.Change.Effect != core.Applied || result.Err != nil {
			t.Fatal(i, result)
		}
	}
	if results[2].Change.Effect != core.Unchanged || core.CodeOf(results[2].Err) != core.AlreadyExists {
		t.Fatal(results[2])
	}
	data, err := os.ReadFile(filepath.Join(to, "batch-c"))
	if err != nil || string(data) != "complete content" {
		t.Fatal("dependent input was not executed in order", string(data), err)
	}
	if _, err := os.Stat(filepath.Join(to, "not-started")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("unstarted suffix had an effect", err)
	}

	if runtime.GOOS != "windows" && runtime.GOOS != "linux" {
		return // The preceding default-create checks apply on other platforms too.
	}
	write(t, from, "a", []byte("updated"))
	replace := core.WriteOptions{Policy: core.Publish()}
	if change, err := c.Copy(context.Background(), "writer", core.Path("/from/a"), core.Path("/to/a"), core.CopyOptions{Target: replace}); err != nil || change.Effect != core.Applied {
		t.Fatal(change, err)
	}
	published, err := c.CopyBatch(context.Background(), "writer", []core.CopyRequest{
		{Source: core.Path("/from/a"), Target: core.Path("/to/a"), Options: core.CopyOptions{Target: replace}},
		{Source: core.Path("/to/a"), Target: core.Path("/to/upsert"), Options: core.CopyOptions{Target: replace}},
	}, core.ExecutionOptions{Concurrency: 2})
	if err != nil || len(published) != 2 || published[0].Change.Effect != core.Applied || published[1].Change.Effect != core.Applied {
		t.Fatal(published, err)
	}
	if data, err := os.ReadFile(filepath.Join(to, "upsert")); err != nil || string(data) != "updated" {
		t.Fatal("batch did not consume the shared write policy", string(data), err)
	}

}

func TestCopyValidation(t *testing.T) {
	c := copyCore(t, copyPermit)
	for _, tc := range []struct {
		core           *core.Core
		source, target string
		code           core.Code
	}{
		{nil, "/from/a", "/to/a", core.InvalidRequest},
		{c, "/from/a", "/from/a", core.InvalidRequest},
		{c, "/from/../a", "/to/a", core.InvalidPath},
		{c, "/from/a", "/to/../a", core.InvalidPath},
	} {
		change, err := tc.core.Copy(context.Background(), "caller", core.Path(tc.source), core.Path(tc.target))
		if change.Effect != core.Unchanged || core.CodeOf(err) != tc.code {
			t.Fatal(change, err)
		}
	}
}

func TestCopyOpenedLength(t *testing.T) {
	n, longer := int64(3), int64(4)
	for _, tc := range []struct {
		name   string
		length *int64
		effect core.Effect
	}{{"known", &n, core.Applied}, {"unknown", nil, core.Applied}, {"short", &longer, core.Incomplete}} {
		t.Run(tc.name, func(t *testing.T) {
			directory := t.TempDir()
			c := copyCore(t, copyPermit)
			b := &copyBody{Reader: strings.NewReader("abc")}
			installCopyMount(t, c, "/from", copySource(b, tc.length))
			d, err := local.New(directory)
			if err != nil {
				t.Fatal(err)
			}
			installCopyMount(t, c, "/to", d)
			change, err := c.Copy(context.Background(), "caller", core.Path("/from/a"), core.Path("/to/new"))
			if change.Effect != tc.effect || (tc.effect == core.Applied && err != nil) ||
				(tc.effect == core.Incomplete && !errors.Is(err, io.ErrUnexpectedEOF)) {
				t.Fatal(change, err)
			}
			got, readErr := os.ReadFile(filepath.Join(directory, "new"))
			if readErr != nil || string(got) != "abc" || b.closes.Load() != 1 {
				t.Fatalf("body or target was lost: %q, %v, closes=%d", got, readErr, b.closes.Load())
			}
		})
	}
}

func TestCopyRefusalClosesSource(t *testing.T) {
	fault := errors.New("source open failed")
	for _, mode := range []string{"source", "permission", "capability"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			authorize := func(ctx context.Context, p core.Principal, a core.Action, targetAddress core.Address) error {
				target := targetAddress.Value()
				if mode == "permission" && a == core.CreateAction {
					return errors.New("target denied")
				}
				return copyPermit(ctx, p, a, core.Path(target))
			}
			c := copyCore(t, authorize)
			b := &copyBody{Reader: strings.NewReader("abc")}
			d := copySource(b, nil)
			wantCode := core.UnsupportedOperation
			if mode == "source" {
				d.open = func(context.Context, string, *core.Range) (core.Opened, error) {
					cancel()
					return core.Opened{}, core.Fail(core.UpstreamFailure, fault)
				}
				wantCode = core.UpstreamFailure
			} else if mode == "permission" {
				wantCode = core.PermissionDenied
			}
			installCopyMount(t, c, "/from", d)
			installCopyMount(t, c, "/to", copySource(&copyBody{Reader: strings.NewReader("")}, nil))
			change, err := c.Copy(ctx, "caller", core.Path("/from/a"), core.Path("/to/new"))
			if mode == "source" && (!errors.Is(err, fault) || !errors.Is(err, context.Canceled)) {
				t.Fatal("source failure lost its concurrent cancellation", err)
			}
			wantClose := int32(1)
			if mode == "source" {
				wantClose = 0
			}
			if change.Effect != core.Unchanged || core.CodeOf(err) != wantCode ||
				b.reads.Load() != 0 || b.closes.Load() != wantClose {
				t.Fatal(change, err, b.reads.Load(), b.closes.Load())
			}
		})
	}
}

func TestCopyPreservesTargetReceipt(t *testing.T) {
	fault := errors.New("target confirmation failed")
	for _, effect := range []core.Effect{core.Applied, core.Unknown} {
		t.Run(effect.String(), func(t *testing.T) {
			c := copyCore(t, copyPermit)
			b := &copyBody{Reader: strings.NewReader("abc")}
			d := copySource(b, nil)
			installCopyMount(t, c, "/from", d)
			installCopyMount(t, c, "/to", &copyWriter{Driver: &fakeDriver{}, create: func(_ context.Context, _ string, input io.Reader, _ *int64) (core.Change, error) {
				if _, err := io.Copy(io.Discard, input); err != nil {
					t.Fatal(err)
				}
				return core.Change{Effect: effect}, fault
			}})
			change, err := c.Copy(context.Background(), "caller", core.Path("/from/a"), core.Path("/to/new"))
			if change.Effect != effect || !errors.Is(err, fault) || b.closes.Load() != 1 {
				t.Fatal(change, err, b.closes.Load())
			}
		})
	}
	c := newCore(t, core.Options{})
	defer closeCore(t, c)
	installCopyMount(t, c, "/invalid", &copyWriter{Driver: &fakeDriver{}, publish: func(context.Context, string, io.Reader, *int64) (core.Change, error) {
		return core.Change{Effect: core.Incomplete}, core.Fail(core.UpstreamFailure, fault)
	}})
	change, err := c.Write(context.Background(), "alice", core.Path("/invalid/file"), strings.NewReader("x"), ptr(1), core.WriteOptions{Policy: core.Publish()})
	if change.Effect != core.Unknown || core.CodeOf(err) != core.InvalidUpstreamResponse || !errors.Is(err, fault) {
		t.Fatal("partial publication did not preserve the contract violation and cause", change, err)
	}

}

func TestCopyWaitsForSourceClose(t *testing.T) {
	targetFault, closeFault := errors.New("target refused"), errors.New("source close failed")
	closing, release := make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	t.Cleanup(unblock)
	b := &copyBody{Reader: strings.NewReader("abc"), close: func() error {
		close(closing)
		<-release
		return closeFault
	}}
	c, err := core.New(core.Options{Authorize: copyPermit})
	if err != nil {
		t.Fatal(err)
	}

	// This in-memory failure deliberately leaves cleanup owned by Core.
	t.Cleanup(func() {
		unblock()
		stopped, stop := context.WithCancel(context.Background())
		stop()
		_ = c.Close(stopped)
	})
	d := copySource(b, nil)
	installCopyMount(t, c, "/from", d)
	installCopyMount(t, c, "/to", &copyWriter{Driver: &fakeDriver{}, create: func(context.Context, string, io.Reader, *int64) (core.Change, error) {
		return core.Change{Effect: core.Unchanged}, targetFault
	}})
	jobs, err := task.New(task.Options{Concurrency: 1, Capacity: 1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { unblock(); _ = jobs.Close(context.Background()) })
	h, err := jobs.Submit(context.Background(), task.Work{Run: func(ctx context.Context, _ func(any)) (any, error) {
		return c.Copy(ctx, "caller", core.Path("/from/a"), core.Path("/to/new"))
	}})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-closing:
	case <-time.After(3 * time.Second):
		t.Fatal("source close did not start")
	}
	h.Cancel()
	if h.Snapshot().State != task.Running {
		t.Fatal("task finished without source close acknowledgement")
	}
	unblock()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	receipt, waitErr := h.Wait(ctx)
	if waitErr != nil {
		t.Fatal(waitErr)
	}
	change := receipt.Result.(core.Change)
	err = receipt.Err
	if change.Effect != core.Unchanged || !errors.Is(err, targetFault) ||
		!errors.Is(err, closeFault) || !errors.Is(err, context.Canceled) || b.closes.Load() != 1 {
		t.Fatal(change, err, b.closes.Load())
	}
}
