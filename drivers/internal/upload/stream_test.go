package upload

import (
	"context"
	"errors"
	"io"
	"strconv"
	"strings"
	"testing"
	"time"
)

type readFunc func([]byte) (int, error)

func (r readFunc) Read(p []byte) (int, error) { return r(p) }

func TestBorrowSeekAndRevoke(t *testing.T) {
	data := []byte("payload")
	body := Borrow(data)
	if end, err := body.Seek(0, io.SeekEnd); end != int64(len(data)) || err != nil {
		t.Fatal(end, err)
	}
	if _, err := body.Seek(0, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	if got, err := io.ReadAll(body); string(got) != string(data) || err != nil {
		t.Fatal(string(got), err)
	}
	if err := body.Close(); err != nil {
		t.Fatal(err)
	}
	data[0] = 'x'
	if _, err := body.Seek(0, io.SeekStart); err != io.ErrClosedPipe {
		t.Fatal("transport seek survived Close", err)
	}
	if _, err := body.Read(make([]byte, 1)); err != io.ErrClosedPipe {
		t.Fatal("transport read survived Close", err)
	}
}

func TestRunPreservesCombinedCancellationErrors(t *testing.T) {
	for _, joinedRead := range []bool{false, true} {
		t.Run(strconv.FormatBool(joinedRead), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			budget, _ := New(2, 2)
			lease, err := budget.Acquire(ctx, 1, 0)
			if err != nil {
				t.Fatal(err)
			}
			defer lease.Release()
			readErr, sendErr := errors.New("input failed"), errors.New("send failed")
			native, reading := make(chan context.Context, 1), make(chan struct{})
			first := true
			input := readFunc(func(p []byte) (int, error) {
				if first {
					first = false
					return copy(p, "x"), nil
				}
				var call context.Context
				select {
				case call = <-native:
				case <-ctx.Done():
					return 0, ctx.Err()
				}
				close(reading)
				if joinedRead {
					<-call.Done()
					return 0, errors.Join(context.Canceled, readErr)
				}
				return 0, readErr
			})
			_, _, err = lease.Run(ctx, input, nil, 2, func(call context.Context, _ int, _ []byte) error {
				native <- call
				<-reading
				if !joinedRead {
					<-call.Done()
					return errors.Join(context.Canceled, sendErr)
				}
				return sendErr
			})
			if ctx.Err() != nil || !errors.Is(err, readErr) || !errors.Is(err, sendErr) {
				t.Fatal("cancellation hid the real failure", err, ctx.Err())
			}
		})
	}
}

func TestRunWaitsForActualWindow(t *testing.T) {
	for _, window := range []int{1, 2} {
		t.Run(strconv.Itoa(window), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			budget, _ := New(int64(3*window), 4)
			lease, err := budget.Acquire(ctx, 1, window)
			if err != nil {
				t.Fatal(err)
			}
			_, invalid := budget.Acquire(ctx, 0, 1)
			_, oversized := budget.Acquire(ctx, int64(3*window+1), 1)
			if invalid == nil || !errors.Is(oversized, ErrCapacity) {
				t.Fatal(invalid, oversized)
			}
			other, err := budget.Acquire(ctx, int64(2*window), 1)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := budget.Acquire(ctx, 1, 1); !errors.Is(err, ErrCapacity) {
				t.Fatal("mixed leases exceeded the shared budget", err)
			}
			entered, release := make(chan struct{}, window), make(chan struct{})
			defer close(release)
			done := make(chan error, 1)
			go func() {
				_, _, err := lease.Run(ctx, strings.NewReader(strings.Repeat("x", window)), nil, window, func(call context.Context, _ int, data []byte) error {
					entered <- struct{}{}
					<-release
					if string(data) != "x" {
						t.Error("in-flight buffer changed")
					}
					return call.Err()
				})
				done <- err
			}()
			for range window {
				select {
				case <-entered:
				case <-ctx.Done():
					t.Fatal("send did not start", ctx.Err())
				}
			}
			cancel()
			select {
			case err := <-done:
				t.Fatal("returned before native sends ended", err)
			case <-time.After(10 * time.Millisecond):
			}
			for range window {
				release <- struct{}{}
			}
			if err := <-done; !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
			other.Release()
			lease.Release()
			_, cancelled := budget.Acquire(ctx, 1, 1)
			if budget.used != 0 || !errors.Is(cancelled, context.Canceled) {
				t.Fatal("mixed leases were not returned or cancellation was ignored", budget.used, cancelled)
			}
		})
	}
}
