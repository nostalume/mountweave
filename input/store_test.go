package input_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nostalume/mountweave/input"
)

func options() input.Options {
	return input.Options{MaxBytes: 4096, MaxInputs: 32, MaxAppendBytes: 1024, MaxBlocks: 2}
}
func openStore(t *testing.T, path string) *input.Store {
	t.Helper()
	s, err := input.Open(context.Background(), path, options())
	if err != nil {
		t.Fatal(err)
	}
	return s
}

type failedEOF struct {
	io.Reader
	err error
}

func (r failedEOF) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	if err == io.EOF {
		return n, r.err
	}
	return n, err
}

func TestInputReplaySealAndReferenceLifetime(t *testing.T) {
	ctx := context.Background()
	directory := t.TempDir()
	s := openStore(t, directory)
	t.Cleanup(func() {
		if err := s.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	r, err := s.Create(ctx, "owner")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.Acquire(ctx, r.ID); !errors.Is(err, input.ErrState) {
		t.Fatal(err)
	}
	if r, err = s.Append(ctx, r.ID, 0, strings.NewReader("abc"), 3); err != nil || r.Offset != 3 {
		t.Fatal(r, err)
	}
	if r, err = s.Append(ctx, r.ID, 0, strings.NewReader("abc"), 3); err != nil || r.Offset != 3 {
		t.Fatal(r, err)
	}
	if _, err = s.Append(ctx, r.ID, 0, strings.NewReader("bad"), 3); !errors.Is(err, input.ErrConflict) {
		t.Fatal(err)
	}
	untouched := strings.NewReader("x")
	if _, err = s.Append(ctx, r.ID, 4, untouched, 1); !errors.Is(err, input.ErrConflict) || untouched.Len() != 1 {
		t.Fatal(err)
	}
	fault := errors.New("last input read failed")
	if r, err = s.Append(ctx, r.ID, 3, failedEOF{strings.NewReader("def"), fault}, 3); !errors.Is(err, fault) || r.Offset != 3 {
		t.Fatal(r, err)
	}
	if _, err = s.Seal(ctx, r.ID, 5); !errors.Is(err, input.ErrConflict) {
		t.Fatal(err)
	}
	if r, err = s.Append(ctx, r.ID, 3, strings.NewReader("def"), 3); err != nil {
		t.Fatal(err)
	}
	untouched = strings.NewReader("z")
	if _, err = s.Append(ctx, r.ID, 6, untouched, 1); !errors.Is(err, input.ErrCapacity) || untouched.Len() != 1 {
		t.Fatal("block budget did not precede reads", err)
	}
	r, err = s.Seal(ctx, r.ID, 6)
	if err != nil || r.Version == "" {
		t.Fatal(r, err)
	}
	version := r.Version
	if r, err = s.Seal(ctx, r.ID, 6); err != nil || r.Version != version {
		t.Fatal(r, err)
	}
	if _, err = s.Append(ctx, r.ID, 0, strings.NewReader("abc"), 3); err != nil {
		t.Fatal("late exact retry", err)
	}
	ref, err := s.Acquire(ctx, r.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer ref.Close()
	body, err := ref.Open(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer body.Close()
	if r, err = s.Release(ctx, r.ID); err != nil || !r.Released {
		t.Fatal(r, err)
	}
	retired, err := os.ReadFile(filepath.Join(directory, r.ID))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.Acquire(ctx, r.ID); !errors.Is(err, input.ErrState) {
		t.Fatal(err)
	}
	if err = ref.Close(); err != nil {
		t.Fatal(err)
	}
	if err = ref.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err = s.Status(ctx, r.ID); err != nil {
		t.Fatal("body lost its independent pin", err)
	}
	data, err := io.ReadAll(body)
	if err != nil || string(data) != "abcdef" {
		t.Fatal(string(data), err)
	}
	if err = body.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err = s.Status(ctx, r.ID); !errors.Is(err, input.ErrNotFound) {
		t.Fatal(err)
	}
	if err = s.Close(ctx); err != nil {
		t.Fatal(err)
	}
	// Restore a retired journal to model process-local pins disappearing on crash.
	if err = os.WriteFile(filepath.Join(directory, r.ID), retired, 0600); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(directory, strings.Repeat("0", 32)), nil, 0600); err != nil {
		t.Fatal(err)
	}
	s = openStore(t, directory)
	if records, err := s.List(ctx); err != nil || len(records) != 0 {
		t.Fatal(records, err)
	}
	// Fill the byte budget, then finish/release using already reserved metadata.
	var records []input.Record
	for {
		next, createErr := s.Create(ctx, "owner")
		if errors.Is(createErr, input.ErrCapacity) {
			break
		}
		if createErr != nil {
			t.Fatal(createErr)
		}
		records = append(records, next)
	}
	if len(records) == 0 {
		t.Fatal("quota was not reclaimed")
	}
	untouched = strings.NewReader(strings.Repeat("x", 1024))
	if _, err = s.Append(ctx, records[0].ID, 0, untouched, 1024); !errors.Is(err, input.ErrCapacity) || untouched.Len() != 1024 {
		t.Fatal(err)
	}
	if _, err = s.Seal(ctx, records[0].ID, 0); err != nil {
		t.Fatal("seal space not reserved", err)
	}
	if _, err = s.Release(ctx, records[0].ID); err != nil {
		t.Fatal("release space not reserved", err)
	}
}

type heldInput struct {
	started, release chan struct{}
	once             sync.Once
}

func (r *heldInput) Read(p []byte) (int, error) {
	r.once.Do(func() { close(r.started); <-r.release })
	return 0, io.EOF
}

func TestInputCancellationAndCorruption(t *testing.T) {
	ctx := context.Background()
	directory := t.TempDir()
	s := openStore(t, directory)
	t.Cleanup(func() {
		wait, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		if err := s.Close(wait); err != nil {
			t.Error(err)
		}
	})
	r, err := s.Create(ctx, "owner")
	if err != nil {
		t.Fatal(err)
	}
	payload := "unique-payload-to-corrupt"
	if _, err = s.Append(ctx, r.ID, 0, strings.NewReader(payload), int64(len(payload))); err != nil {
		t.Fatal(err)
	}
	held := &heldInput{started: make(chan struct{}), release: make(chan struct{})}
	unblock := sync.OnceFunc(func() { close(held.release) })
	defer unblock()
	callCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := s.Append(callCtx, r.ID, int64(len(payload)), held, 1); done <- err }()
	select {
	case <-held.started:
	case <-time.After(5 * time.Second):
		t.Fatal("append did not start")
	}
	untouched := strings.NewReader("x")
	if _, err = s.Append(ctx, r.ID, int64(len(payload)), untouched, 1); !errors.Is(err, input.ErrBusy) || untouched.Len() != 1 {
		t.Fatal(err)
	}
	cancel()
	waitCtx, stop := context.WithCancel(ctx)
	stop()
	if err = s.Close(waitCtx); !errors.Is(err, context.Canceled) {
		t.Fatal("close ignored borrowed read", err)
	}
	select {
	case err := <-done:
		t.Fatal("returned while reader remained active", err)
	default:
	}
	unblock()
	if err = <-done; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err = s.Close(ctx); err != nil {
		t.Fatal(err)
	}
	s = openStore(t, directory)
	if r, err = s.Status(ctx, r.ID); err != nil || r.Offset != int64(len(payload)) {
		t.Fatal(r, err)
	}
	if _, err = s.Seal(ctx, r.ID, r.Offset); err != nil {
		t.Fatal(err)
	}
	ref, err := s.Acquire(ctx, r.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer ref.Close()
	file := filepath.Join(directory, r.ID)
	raw, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	at := bytes.Index(raw, []byte(payload))
	if at < 0 {
		t.Fatal("payload not retained")
	}
	f, err := os.OpenFile(file, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.WriteAt([]byte("X"), int64(at)); err != nil {
		t.Fatal(err)
	}
	if err = errors.Join(f.Sync(), f.Close()); err != nil {
		t.Fatal(err)
	}
	body, err := ref.Open(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer body.Close()
	if _, err = io.ReadAll(body); !errors.Is(err, input.ErrCorrupt) {
		t.Fatal(err)
	}
	if err = errors.Join(body.Close(), ref.Close(), s.Close(ctx)); err != nil {
		t.Fatal(err)
	}
	// A corrupt complete length header must not masquerade as a torn creation.
	raw[len("MWINPUT1")+16] ^= 0x40
	if err = os.WriteFile(file, raw, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err = input.Open(ctx, directory, options()); !errors.Is(err, input.ErrCorrupt) {
		t.Fatal(err)
	}
	if _, err = os.Stat(file); err != nil {
		t.Fatal("corrupt confirmed input was discarded", err)
	}
}

type crashInput struct {
	path  string
	first bool
}

func (r *crashInput) Read(p []byte) (int, error) {
	if !r.first {
		r.first = true
		return copy(p, "torn"), nil
	}
	if err := os.WriteFile(r.path, []byte("blocked"), 0600); err != nil {
		return 0, err
	}
	select {}
}

func TestInputProcessCrashRecovery(t *testing.T) {
	ctx := context.Background()
	if base := os.Getenv("MOUNTWEAVE_INPUT_CRASH"); base != "" {
		s := openStore(t, filepath.Join(base, "data"))
		r, err := s.Create(ctx, "owner")
		if err != nil {
			t.Fatal(err)
		}
		r, err = s.Append(ctx, r.ID, 0, strings.NewReader("first"), 5)
		if err != nil {
			t.Fatal(err)
		}
		ack, _ := json.Marshal(r)
		if err = os.WriteFile(filepath.Join(base, "ack"), ack, 0600); err != nil {
			t.Fatal(err)
		}
		_, err = s.Append(ctx, r.ID, 5, &crashInput{path: filepath.Join(base, "blocked")}, 100)
		t.Fatal("child did not block", err)
	}
	base := t.TempDir()
	directory := filepath.Join(base, "data")
	if err := os.Mkdir(directory, 0700); err != nil {
		t.Fatal(err)
	}
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(exe, "-test.run=^TestInputProcessCrashRecovery$")
	cmd.Env = append(os.Environ(), "MOUNTWEAVE_INPUT_CRASH="+base)
	var output bytes.Buffer
	cmd.Stdout = &output
	cmd.Stderr = &output
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cmd.Process.Kill(); _ = cmd.Wait() }()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err = os.Stat(filepath.Join(base, "blocked")); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("child did not reach torn append")
		}
		time.Sleep(time.Millisecond * 10)
	}
	if err = cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = cmd.Wait()
	ack, err := os.ReadFile(filepath.Join(base, "ack"))
	if err != nil {
		t.Fatal(err, output.String())
	}
	var r input.Record
	if err = json.Unmarshal(ack, &r); err != nil {
		t.Fatal(err)
	}
	s := openStore(t, directory)
	if r, err = s.Status(ctx, r.ID); err != nil || r.Offset != 5 {
		t.Fatal(r, err)
	}
	if _, err = s.Append(ctx, r.ID, 0, strings.NewReader("first"), 5); err != nil {
		t.Fatal(err)
	}
	if r, err = s.Append(ctx, r.ID, 5, strings.NewReader("second"), 6); err != nil || r.Offset != 11 {
		t.Fatal(r, err)
	}
	if r, err = s.Seal(ctx, r.ID, 11); err != nil {
		t.Fatal(err)
	}
	version := r.Version
	if err = s.Close(ctx); err != nil {
		t.Fatal(err)
	}
	s = openStore(t, directory)
	defer func() {
		if err := s.Close(ctx); err != nil {
			t.Error(err)
		}
	}()
	if r, err = s.Status(ctx, r.ID); err != nil || r.Version != version {
		t.Fatal(r, err)
	}
	ref, err := s.Acquire(ctx, r.ID)
	if err != nil {
		t.Fatal(err)
	}
	body, err := ref.Open(ctx)
	if err != nil {
		t.Fatal(err)
	}
	data, readErr := io.ReadAll(body)
	if err = errors.Join(readErr, body.Close(), ref.Close()); err != nil || string(data) != "firstsecond" {
		t.Fatal(string(data), err)
	}
}
