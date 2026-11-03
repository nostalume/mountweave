package input_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/input"
)

func TestDurableRetentionAndVerifiedRanges(t *testing.T) {
	ctx := context.Background()
	directory := t.TempDir()
	s := openStore(t, directory)
	r, err := s.Create(ctx, "owner")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.Append(ctx, r.ID, 0, bytes.NewBufferString("abcdefgh"), 8); err != nil {
		t.Fatal(err)
	}
	if _, err = s.Seal(ctx, r.ID, 8); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"job-a", "job-b", "job-a"} {
		if err = s.Hold(ctx, r.ID, key); err != nil {
			t.Fatal(err)
		}
	}
	if _, err = s.Release(ctx, r.ID); err != nil {
		t.Fatal(err)
	}
	if err = s.Close(ctx); err != nil {
		t.Fatal(err)
	}
	s = openStore(t, directory)
	defer func() {
		if err := s.Close(ctx); err != nil {
			t.Error(err)
		}
	}()
	if err = s.Hold(ctx, r.ID, "job-c"); !errors.Is(err, input.ErrState) {
		t.Fatalf("new retired owner: %v", err)
	}
	if err = s.Hold(ctx, r.ID, "job-a"); err != nil {
		t.Fatal(err)
	}
	if err = s.Unhold(ctx, r.ID, "job-a"); err != nil {
		t.Fatal(err)
	}
	if err = s.Unhold(ctx, r.ID, "job-a"); err != nil {
		t.Fatal(err)
	}
	ref, err := s.AcquireHeld(ctx, r.ID, "job-b")
	if err != nil {
		t.Fatal(err)
	}
	body, err := ref.Open(ctx)
	if err != nil {
		t.Fatal(err)
	}
	source, ok := body.(core.Replayable)
	if !ok {
		t.Fatal("source is not replayable")
	}
	part, err := source.OpenRange(ctx, 2, 3)
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(part)
	err = errors.Join(err, part.Close())
	if err != nil || string(data) != "cde" {
		t.Fatalf("range %q: %v", data, err)
	}
	if _, err = source.OpenRange(ctx, 7, 2); !errors.Is(err, input.ErrInvalid) {
		t.Fatalf("invalid range: %v", err)
	}
	for _, offset := range []int64{4, 8} {
		empty, e := source.OpenRange(ctx, offset, 0)
		if e != nil {
			t.Fatal(e)
		}
		if data, e = io.ReadAll(empty); len(data) != 0 || e != nil {
			t.Fatalf("empty range: %v", e)
		}
		if e = empty.Close(); e != nil {
			t.Fatal(e)
		}
	}
	// Damage outside the requested bytes, but inside its checksum boundary block.
	path := filepath.Join(directory, r.ID)
	stored, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	position := bytes.Index(stored, []byte("abcdefgh"))
	if position < 0 {
		t.Fatal("payload missing")
	}
	f, err := os.OpenFile(path, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	_, err = f.WriteAt([]byte("X"), int64(position+7))
	err = errors.Join(err, f.Close())
	if err != nil {
		t.Fatal(err)
	}
	part, err = source.OpenRange(ctx, 2, 2)
	if err != nil {
		t.Fatal(err)
	}
	_, err = io.ReadAll(part)
	if !errors.Is(err, input.ErrCorrupt) {
		t.Fatalf("suffix corruption ignored: %v", err)
	}
	if err = part.Close(); err != nil {
		t.Fatal(err)
	}
	// Fill the byte budget; reserved Unhold and Release still finish at capacity.
	for {
		_, err = s.Create(ctx, "fill")
		if errors.Is(err, input.ErrCapacity) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	if err = s.Unhold(ctx, r.ID, "job-b"); err != nil {
		t.Fatal(err)
	}
	if err = ref.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err = s.Status(ctx, r.ID); err != nil {
		t.Fatalf("body pin lost: %v", err)
	}
	if err = body.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err = s.Status(ctx, r.ID); !errors.Is(err, input.ErrNotFound) {
		t.Fatalf("retired bytes retained: %v", err)
	}
}
