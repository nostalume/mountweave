package core_test

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/local"
)

func TestBatchStopAndCancellation(t *testing.T) {
	fault := errors.New("completion fault")
	for _, cancelItem := range []bool{false, true} {
		t.Run(map[bool]string{false: "applied-error", true: "cancel-in-item"}[cancelItem], func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			c := copyCore(t, copyPermit)
			body := &copyBody{Reader: strings.NewReader("abc")}
			installCopyMount(t, c, "/from", copySource(body, nil))
			calls := 0
			installCopyMount(t, c, "/to", &copyWriter{Driver: &fakeDriver{}, create: func(_ context.Context, _ string, input io.Reader, _ *int64) (core.Change, error) {
				calls++
				if _, err := io.Copy(io.Discard, input); err != nil {
					t.Fatal(err)
				}
				if cancelItem {
					cancel()
				}
				return core.Change{Effect: core.Applied}, fault
			}})
			results, err := c.CopyBatch(ctx, "caller", []core.CopyRequest{
				{Source: core.Path("/from/a"), Target: core.Path("/to/a")},
				{Source: core.Path("/from/a"), Target: core.Path("/to/b")},
			})
			if len(results) != 1 || calls != 1 || body.closes.Load() != 1 ||
				results[0].Change.Effect != core.Applied || !errors.Is(results[0].Err, fault) || !errors.Is(err, fault) {
				t.Fatal(results, err, calls, body.closes.Load())
			}
			if cancelItem && (!errors.Is(results[0].Err, context.Canceled) || !errors.Is(err, context.Canceled)) {
				t.Fatal("cancellation or item receipt was lost", results, err)
			}
		})
	}
	c := newCore(t, core.Options{Authorize: func(context.Context, core.Principal, core.Action, core.Address) error {
		t.Fatal("empty or canceled batch attempted authorization")
		return nil
	}})
	defer closeCore(t, c)
	if results, err := c.RemoveBatch(context.Background(), "alice", nil); len(results) != 0 || err != nil {
		t.Fatal(results, err)
	}
	if results, err := c.CopyBatch(context.Background(), "alice", nil); len(results) != 0 || err != nil {
		t.Fatal(results, err)
	}
	if results, err := c.MoveBatch(context.Background(), "alice", nil); len(results) != 0 || err != nil {
		t.Fatal(results, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for _, requests := range [][]core.MoveRequest{nil, {{Source: core.Path("/a"), Target: core.Path("/b")}}} {
		if results, err := c.MoveBatch(ctx, "alice", requests); len(results) != 0 || !errors.Is(err, context.Canceled) {
			t.Fatal(results, err)
		}
	}
	for _, targets := range [][]core.Address{nil, {core.Path("/not-started")}} {
		if results, err := c.RemoveBatch(ctx, "alice", targets); len(results) != 0 || !errors.Is(err, context.Canceled) {
			t.Fatal(results, err)
		}
	}
	for _, requests := range [][]core.CopyRequest{nil, {{Source: core.Path("/a"), Target: core.Path("/b")}}} {
		if results, err := c.CopyBatch(ctx, "alice", requests); len(results) != 0 || !errors.Is(err, context.Canceled) {
			t.Fatal(results, err)
		}
	}
}

func TestBoundedBatches(t *testing.T) {
	t.Run("alias-conflict-order", func(t *testing.T) {
		from, to := t.TempDir(), t.TempDir()
		write(t, from, "a", []byte("payload"))
		c := copyCore(t, copyPermit)
		source, err := local.New(from)
		if err != nil {
			t.Fatal(err)
		}
		target, err := local.New(to)
		if err != nil {
			t.Fatal(err)
		}
		installCopyMount(t, c, "/from", source)
		first, second, duplicate := make(chan struct{}), make(chan struct{}), make(chan struct{})
		releaseFirst, releaseSecond, secondDone := make(chan struct{}), make(chan struct{}), make(chan struct{})
		unblockFirst, unblockSecond := sync.OnceFunc(func() { close(releaseFirst) }), sync.OnceFunc(func() { close(releaseSecond) })
		t.Cleanup(unblockFirst)
		t.Cleanup(unblockSecond)
		var xCalls atomic.Int32
		m := installCopyMount(t, c, "/to", &copyWriter{Driver: target, create: func(ctx context.Context, name string, input io.Reader, length *int64) (core.Change, error) {
			switch name {
			case "/x":
				if xCalls.Add(1) == 1 {
					close(first)
					<-releaseFirst
				} else {
					close(duplicate)
				}
			case "/y":
				close(second)
				<-releaseSecond
			}
			change, err := target.Create(ctx, core.NativePath(name), input, length)
			if name == "/y" {
				close(secondDone)
			}
			return change, err
		}})
		if err := c.BindAlias(context.Background(), "/alias", m, "/"); err != nil {
			t.Fatal(err)
		}
		type outcome struct {
			rows []core.ItemResult
			err  error
		}
		done := make(chan outcome, 1)
		go func() {
			rows, err := c.CopyBatch(context.Background(), "caller", []core.CopyRequest{
				{Source: core.Path("/from/a"), Target: core.Path("/to/x")},
				{Source: core.Path("/from/a"), Target: core.Path("/to/y")},
				{Source: core.Path("/from/a"), Target: core.Path("/alias/x")},
				{Source: core.Path("/from/a"), Target: core.Path("/to/x")},
				{Source: core.Path("/from/a"), Target: core.Path("/to/not-started")},
			}, core.ExecutionOptions{Concurrency: 2})
			done <- outcome{rows, err}
		}()
		await(t, first)
		await(t, second) // Independent work overlaps the blocked first request.
		unblockSecond()
		await(t, secondDone)
		select {
		case <-duplicate:
			t.Fatal("alias conflict bypassed the unfinished first item")
		case <-time.After(30 * time.Millisecond):
		}
		unblockFirst()
		select {
		case got := <-done:
			if len(got.rows) != 3 || core.CodeOf(got.err) != core.AlreadyExists ||
				got.rows[0].Change.Effect != core.Applied || got.rows[1].Change.Effect != core.Applied ||
				got.rows[2].Change.Effect != core.Unchanged {
				t.Fatal(got)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("bounded execution did not finish")
		}
		if _, err := os.Stat(filepath.Join(to, "not-started")); !errors.Is(err, os.ErrNotExist) {
			t.Fatal(err)
		}
	})
	t.Run("cancel-keeps-started-effects", func(t *testing.T) {
		from, to := t.TempDir(), t.TempDir()
		write(t, from, "a", []byte("payload"))
		c := copyCore(t, copyPermit)
		source, err := local.New(from)
		if err != nil {
			t.Fatal(err)
		}
		target, err := local.New(to)
		if err != nil {
			t.Fatal(err)
		}
		installCopyMount(t, c, "/from", source)
		acknowledged := make(chan struct{}, 2)
		installCopyMount(t, c, "/to", &copyWriter{Driver: target, create: func(ctx context.Context, name string, input io.Reader, length *int64) (core.Change, error) {
			change, err := target.Create(ctx, core.NativePath(name), input, length)
			acknowledged <- struct{}{}
			<-ctx.Done()
			return change, errors.Join(err, ctx.Err())
		}})
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		type outcome struct {
			rows []core.ItemResult
			err  error
		}
		done := make(chan outcome, 1)
		go func() {
			rows, err := c.CopyBatch(ctx, "caller", []core.CopyRequest{
				{Source: core.Path("/from/a"), Target: core.Path("/to/x")},
				{Source: core.Path("/from/a"), Target: core.Path("/to/y")},
				{Source: core.Path("/from/a"), Target: core.Path("/to/not-started")},
			}, core.ExecutionOptions{Concurrency: 2})
			done <- outcome{rows, err}
		}()
		for i := 0; i < 2; i++ {
			select {
			case <-acknowledged:
			case <-time.After(3 * time.Second):
				t.Fatal("independent item did not start")
			}
		}
		cancel()
		select {
		case got := <-done:
			if len(got.rows) != 2 || !errors.Is(got.err, context.Canceled) {
				t.Fatal(got)
			}
			for _, row := range got.rows {
				if row.Change.Effect != core.Applied || !errors.Is(row.Err, context.Canceled) {
					t.Fatal(row)
				}
			}
		case <-time.After(3 * time.Second):
			t.Fatal("canceled batch lost in-flight work")
		}
		for _, name := range []string{"x", "y"} {
			data, err := os.ReadFile(filepath.Join(to, name))
			if err != nil || string(data) != "payload" {
				t.Fatal(name, string(data), err)
			}
		}
		if _, err := os.Stat(filepath.Join(to, "not-started")); !errors.Is(err, os.ErrNotExist) {
			t.Fatal(err)
		}
	})

	for _, child := range []string{"tree/child", "moved/child"} {
		t.Run("move-conflict-"+child, func(t *testing.T) {
			c := newCore(t, core.Options{})
			defer closeCore(t, c)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			first, second, secondDone := make(chan struct{}), make(chan struct{}), make(chan struct{})
			releaseSecond, unexpected := make(chan struct{}), make(chan string, 2)
			unblockSecond := sync.OnceFunc(func() { close(releaseSecond) })
			defer unblockSecond()
			m := installCopyMount(t, c, "/files", &relocatingDriver{
				Driver: &fakeDriver{stat: func(context.Context, string) (core.Metadata, error) {
					return core.Metadata{Kind: core.Directory}, nil
				}},
				move: func(ctx context.Context, source, target string) (core.Change, error) {
					switch source {
					case "/tree":
						close(first)
						<-ctx.Done()
						return core.Change{Effect: core.Applied}, ctx.Err()
					case "/other":
						close(second)
						<-releaseSecond
						close(secondDone)
						return core.Change{Effect: core.Applied}, nil
					default:
						unexpected <- source
						return core.Change{Effect: core.Unchanged}, errors.New("conflicting suffix started")
					}
				},
			})
			if err := c.BindAlias(ctx, "/alias", m, "/"); err != nil {
				t.Fatal(err)
			}
			type outcome struct {
				rows []core.MoveItemResult
				err  error
			}
			done := make(chan outcome, 1)
			go func() {
				rows, err := c.MoveBatch(ctx, "alice", []core.MoveRequest{
					{Source: core.Path("/files/tree"), Target: core.Path("/files/moved")},
					{Source: core.Path("/files/other"), Target: core.Path("/files/independent")},
					{Source: core.Path("/alias/" + child), Target: core.Path("/files/late")},
					{Source: core.Path("/files/tail"), Target: core.Path("/files/bypassed")},
				}, core.ExecutionOptions{Concurrency: 2})
				done <- outcome{rows, err}
			}()
			await(t, first)
			await(t, second)
			unblockSecond()
			await(t, secondDone)
			select {
			case source := <-unexpected:
				t.Fatal("move write conflict or blocked head bypassed", source)
			case <-time.After(30 * time.Millisecond):
			}
			cancel()
			select {
			case got := <-done:
				if len(got.rows) != 2 || !errors.Is(got.err, context.Canceled) {
					t.Fatal(got)
				}
				for i, row := range got.rows {
					if row.Source.Effect != core.Applied || row.Target.Effect != core.Applied ||
						(i == 0 && !errors.Is(row.Err, context.Canceled)) || (i == 1 && row.Err != nil) {
						t.Fatal(i, row)
					}
				}
			case <-time.After(3 * time.Second):
				t.Fatal("move batch did not join started work")
			}
		})
	}
}
