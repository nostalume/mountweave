package upload

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	core "github.com/nostalume/mountweave"
)

func TestBodyInputBoundaries(t *testing.T) {
	size := func(n int64) *int64 { return &n }
	tail := errors.New("input tail failed")
	for _, test := range []struct {
		name    string
		input   io.Reader
		length  *int64
		maximum []int64
		want    error
		code    core.Code
		payload int64
	}{
		{"exact", strings.NewReader("abc"), size(3), nil, nil, "", 3},
		{"unknown", strings.NewReader("abc"), nil, nil, nil, "", 3},
		{"empty", strings.NewReader(""), size(0), nil, nil, "", 0},
		{"short", strings.NewReader("ab"), size(3), nil, io.ErrUnexpectedEOF, "", 2},
		{"long", strings.NewReader("abcd"), size(3), nil, ErrSize, "", 3},
		{"known tail", readFunc(func(p []byte) (int, error) { return copy(p, "abc"), tail }), size(3), nil, tail, "", 3},
		{"unknown tail", readFunc(func(p []byte) (int, error) { return copy(p, "abc"), tail }), nil, nil, tail, "", 3},
		{"no progress", readFunc(func([]byte) (int, error) { return 0, nil }), nil, nil, io.ErrNoProgress, "", 0},
		{"invalid read", readFunc(func(p []byte) (int, error) { return len(p) + 1, nil }), nil, nil, ErrRead, "", 0},
		{"bounded short", strings.NewReader("ab"), nil, []int64{3}, nil, "", 2},
		{"bounded exact", strings.NewReader("abc"), nil, []int64{3}, nil, "", 3},
		{"bounded extra", strings.NewReader("abcd"), nil, []int64{3}, nil, core.CapacityExceeded, 3},
		{"known beyond maximum", strings.NewReader("abcd"), size(4), []int64{3}, nil, core.CapacityExceeded, 0},
		{"known extra at maximum", strings.NewReader("abcd"), size(3), []int64{3}, ErrSize, "", 3},
		{"invalid maximum", strings.NewReader("abc"), nil, []int64{0}, nil, core.InvalidRequest, 0},
		{"multiple maxima", strings.NewReader("abc"), nil, []int64{3, 4}, nil, core.InvalidRequest, 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			reads := 0
			body := NewBody(context.Background(), readFunc(func(p []byte) (int, error) {
				reads++
				return test.input.Read(p)
			}), test.length, test.maximum...)
			data, readErr := io.ReadAll(body)
			closeErr := body.Close()
			complete, finishErr := body.Finish()
			matches := func(err error) bool {
				if test.code != "" {
					return core.CodeOf(err) == test.code
				}
				return errors.Is(err, test.want)
			}
			if !matches(readErr) || !matches(closeErr) || !matches(finishErr) || complete != (test.want == nil && test.code == "") {
				t.Fatal(complete, readErr, closeErr, finishErr)
			}
			if body.PayloadBytes() != test.payload || int64(len(data)) != test.payload {
				t.Fatal("payload count included validation probe or lost bytes", body.PayloadBytes(), len(data))
			}
			if test.code != "" && test.payload == 0 && reads != 0 {
				t.Fatal("invalid bounds consumed input", reads)
			}
			if _, err := body.Read(make([]byte, 1)); err != io.ErrClosedPipe || body.PayloadBytes() != test.payload {
				t.Fatal("transport access survived Finish", err)
			}
		})
	}
}
func TestBodyFinishWaitsAndRevokes(t *testing.T) {
	entered, release, readDone := make(chan struct{}), make(chan struct{}), make(chan struct{})
	reads := 0
	input := readFunc(func(p []byte) (int, error) {
		reads++
		close(entered)
		<-release
		return copy(p, "x"), io.EOF
	})
	body := NewBody(context.Background(), input, nil, 1)
	go func() { _, _ = body.Read(make([]byte, 1)); close(readDone) }()
	<-entered
	finished := make(chan bool, 1)
	go func() { complete, _ := body.Finish(); finished <- complete }()
	select {
	case <-finished:
		t.Fatal("Finish returned while input was borrowed")
	case <-time.After(10 * time.Millisecond):
	}
	close(release)
	<-readDone
	if !<-finished {
		t.Fatal("normal EOF was lost")
	}
	if _, err := body.Read(make([]byte, 1)); err != io.ErrClosedPipe || reads != 1 || body.PayloadBytes() != 1 {
		t.Fatal("late transport read accessed input", reads, err)
	}
	idleReads := 0
	idle := NewBody(context.Background(), readFunc(func([]byte) (int, error) { idleReads++; return 0, nil }), nil, 1)
	if complete, err := idle.Finish(); complete || err != nil || idleReads != 0 || idle.PayloadBytes() != 0 {
		t.Fatal("early Finish drained or completed unread input", complete, err, idleReads)
	}
	if err := idle.Close(); err != nil || idleReads != 0 {
		t.Fatal("Close drained unread input", err, idleReads)
	}
}
