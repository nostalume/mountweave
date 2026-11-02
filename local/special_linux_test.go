package local_test

import (
	"context"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/nostalume/mountweave/local"
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
	if _, err = d.Open(context.Background(), "/fifo", nil); err == nil {
		t.Fatal("FIFO was accepted")
	}
}
