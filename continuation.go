package core

import (
	"context"
	"io"
)

// Replayable is an immutable, versioned input with independent range readers.
// OpenRange returns exactly length bytes followed by normal EOF, verifies source
// integrity, and owns only its returned body. The caller closes each body. The
// source is borrowed only until Write returns; its durable retention is external.
type Replayable interface {
	io.Reader
	SourceVersion() string
	OpenRange(context.Context, int64, int64) (io.ReadCloser, error)
}

// WriteContinuation explicitly carries a protected native recovery log. Append
// must durably append complete records before returning nil, copy retained
// bytes, and serialize
// revisions for one executor. Native checkpoints are not untrusted client tokens.
// Append may receive a context detached from execution cancellation to record an
// acknowledged effect; it is synchronous and may not outlive Write. Its storage,
// deadlines and failures belong to the consumer. Canceling Write pauses a native
// attempt; Abort explicitly requests cleanup, without undoing published content.
// Checkpoint is the complete committed log for this execution. Append receives
// only new records; it must never rewrite the accumulated log per append.
type WriteContinuation struct {
	Checkpoint []byte
	Append     func(context.Context, []byte) error
	Abort      bool
}

// ContinuableWriter is an optional Driver mode reached through the same Write
// admission, authority, binding and resource lifetime. It must bind checkpoints
// to the native destination, source version, length and publication mode, retain
// uncertain effects, and join all source/native activity before returning. It
// cannot close or retain source. Confirmed Abort may return Unchanged with nil;
// all other results follow complete-publication semantics.
type ContinuableWriter interface {
	ContinueWrite(context.Context, NativeAddress, Replayable, int64, PublicationOptions, WriteContinuation) (Change, error)
}
