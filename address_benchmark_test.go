package core_test

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/local"
)

// BenchmarkTreeFlow compares identical tree semantics across address changes.
// Local I/O is intentional; it is not a universal throughput requirement.
func BenchmarkTreeFlow(b *testing.B) {
	ctx := context.Background()
	root := b.TempDir()
	data := bytes.Repeat([]byte("x"), 1<<20)
	if err := os.WriteFile(filepath.Join(root, "data"), data, 0600); err != nil {
		b.Fatal(err)
	}
	d, err := local.New(root)
	if err != nil {
		b.Fatal(err)
	}
	c, err := core.New(core.Options{Authorize: allow})
	if err != nil {
		b.Fatal(err)
	}
	if _, err = c.Install("/files", d); err != nil {
		b.Fatal(err)
	}
	defer c.Close(ctx)
	b.Run("Stat", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if _, err := c.Stat(ctx, "alice", core.Path("/files/data")); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("List", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if _, err := c.List(ctx, "alice", core.ListRequest{Query: core.Children(core.Path("/files")), CursorTTL: time.Minute}); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("Read", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(data)))
		for b.Loop() {
			o, err := c.Open(ctx, "alice", core.Path("/files/data"), core.OpenOptions{})
			if err != nil {
				b.Fatal(err)
			}
			_, err = io.Copy(io.Discard, o.Body)
			errClose := o.Body.Close()
			if err != nil || errClose != nil {
				b.Fatal(err, errClose)
			}
		}
	})
	b.Run("Create", func(b *testing.B) {
		b.ReportAllocs()
		body := data[:8192]
		for b.Loop() {
			if _, err := c.Write(ctx, "alice", core.Path("/files/new"), bytes.NewReader(body), nil); err != nil {
				b.Fatal(err)
			}
			if _, err := c.Remove(ctx, "alice", core.Path("/files/new")); err != nil {
				b.Fatal(err)
			}
		}
	})
}
