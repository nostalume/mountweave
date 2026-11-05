package local_test

import (
	"context"
	"path/filepath"
	"syscall"
	"testing"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/local"
)

func TestFIFOIsRejectedWithoutBlocking(t *testing.T) {
	root := t.TempDir()
	if err := syscall.Mkfifo(filepath.Join(root, "fifo"), 0600); err != nil {
		t.Fatal(err)
	}
	d, err := local.New(root)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	if _, err = d.Open(context.Background(), core.NativePath("/fifo"), core.OpenOptions{}); err == nil {
		t.Fatal("FIFO was accepted")
	}
}
