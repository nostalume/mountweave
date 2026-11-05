package local

import (
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
)

func TestPublicationRetainsFailedCleanup(t *testing.T) {
	if !directoryScopesSupported {
		t.Skip("native publication is unavailable")
	}
	root, staging := t.TempDir(), t.TempDir()
	d, err := New(root, Options{StagingDirectory: staging, MaxStagingBytes: 3})
	if err != nil {
		t.Fatal(err)
	}
	c, err := core.New(core.Options{Authorize: func(context.Context, core.Principal, core.Action, core.Address) error {
		return nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	m, err := c.Install("/files", d)
	if err != nil {
		t.Fatal(err)
	}
	file, _, err := openChild(d.stagingParent, "owned", stageChild)
	if err != nil {
		t.Fatal(err)
	}
	if err := d.reserveStaging(3); err != nil {
		t.Fatal(err)
	}
	stage := &stagedFile{driver: d, file: file, name: "owned", reserved: 3, fixed: true}
	// Make the owned close fail without adding production fault-injection hooks.
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if err := stage.cleanup(false); !errors.Is(err, os.ErrClosed) {
		t.Fatal("cleanup failure was hidden", err)
	}
	input := strings.NewReader("x")
	length := int64(1)
	change, err := c.Write(context.Background(), "writer", core.Path("/files/new"), input, &length, core.WriteOptions{Policy: core.Publish()})
	if change.Effect != core.Unchanged || core.CodeOf(err) != core.CapacityExceeded || input.Len() != 1 {
		t.Fatal("failed cleanup released its reservation", change, err)
	}
	if err := m.Retire(context.Background()); !errors.Is(err, os.ErrClosed) {
		t.Fatal("retirement acknowledged failed cleanup", err)
	}
	if _, err := c.Install("/files", d); core.CodeOf(err) != core.PathAmbiguous {
		t.Fatal("failed retirement allowed a replacement mount", err)
	}
}

func TestPublicationCommitWait(t *testing.T) {
	if !directoryScopesSupported {
		t.Skip("native publication is unavailable")
	}
	for _, retire := range []bool{false, true} {
		t.Run(map[bool]string{false: "cancel", true: "retire"}[retire], func(t *testing.T) {
			root, staging := t.TempDir(), t.TempDir()
			d, err := New(root, Options{StagingDirectory: staging, MaxStagingBytes: 3, MaxConcurrentCommits: 1})
			if err != nil {
				t.Fatal(err)
			}
			c, err := core.New(core.Options{Authorize: func(context.Context, core.Principal, core.Action, core.Address) error {
				return nil
			}})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				wait, stop := context.WithTimeout(context.Background(), time.Second)
				defer stop()
				if err := c.Close(wait); err != nil {
					t.Error(err)
				}
			})
			m, err := c.Install("/files", d)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(root, "item"), []byte("old"), 0600); err != nil {
				t.Fatal(err)
			}
			// Occupy the native commit slot without introducing a production hook.
			d.commits <- struct{}{}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			end := publicationEnd{done: make(chan struct{})}
			type outcome struct {
				change core.Change
				err    error
			}
			done := make(chan outcome, 1)
			length := int64(3)
			go func() {
				change, err := c.Write(ctx, "writer", core.Path("/files/item"), io.MultiReader(strings.NewReader("new"), &end), &length, core.WriteOptions{Policy: core.Publish()})
				done <- outcome{change, err}
			}()
			select {
			case <-end.done:
			case <-time.After(time.Second):
				t.Fatal("commit capacity blocked input transfer")
			}
			select {
			case got := <-done:
				t.Fatal("publication bypassed the occupied commit slot", got)
			case <-time.After(20 * time.Millisecond):
			}
			probe := strings.NewReader("x")
			one := int64(1)
			change, err := c.Write(ctx, "writer", core.Path("/files/peer"), probe, &one, core.WriteOptions{Policy: core.Publish()})
			if change.Effect != core.Unchanged || core.CodeOf(err) != core.CapacityExceeded || probe.Len() != 1 {
				t.Fatal("waiting publication released its staging reservation", change, err)
			}
			if retire {
				wait, stop := context.WithTimeout(context.Background(), time.Second)
				defer stop()
				if err := m.Retire(wait); err != nil {
					t.Fatal("retirement did not cancel the commit wait", err)
				}
			} else {
				cancel()
			}
			select {
			case got := <-done:
				if got.change.Effect != core.Unchanged || !errors.Is(got.err, context.Canceled) {
					t.Fatal(got)
				}
			case <-time.After(time.Second):
				t.Fatal("commit wait ignored cancellation")
			}
			if data, err := os.ReadFile(filepath.Join(root, "item")); err != nil || string(data) != "old" {
				t.Fatal("canceled commit changed its target", string(data), err)
			}
			if entries, err := os.ReadDir(staging); err != nil || len(entries) != 0 {
				t.Fatal("canceled commit leaked staged files", entries, err)
			}
			<-d.commits
			if !retire {
				change, err := c.Write(context.Background(), "writer", core.Path("/files/item"), strings.NewReader("new"), &length, core.WriteOptions{Policy: core.Publish()})
				if err != nil || change.Effect != core.Applied || len(d.commits) != 0 {
					t.Fatal("cancellation leaked staging bytes or commit capacity", change, err)
				}
			}
		})
	}
}

type publicationEnd struct {
	done chan struct{}
	once sync.Once
}

func (r *publicationEnd) Read([]byte) (int, error) {
	r.once.Do(func() { close(r.done) })
	return 0, io.EOF
}
