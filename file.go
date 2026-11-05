package core

import (
	"context"
	"io"
	"sync"
)

// ReadFile is an already-bound native resource, never a path-reopening adapter.
// ReadAt follows io.ReaderAt byte/EOF rules with a per-call context. EOF and call
// cancellation do not close it. Stat describes this resource, using the original
// opened name even after a rename. Close must be safe alongside in-flight calls
// and request their cancellation; Core waits for those calls before releasing use.
type ReadFile interface {
	ReadAt(context.Context, []byte, int64) (int, error)
	Stat(context.Context) (Metadata, error)
	Close() error
}

// EditFile is optional on a LiveEdit result, selected once at Open. Byte writes
// retain confirmed n and Change independently of error; no complete-publication
// or durability is implied. Append must use a native append mode, not Stat+WriteAt.
// Sync explicitly requests native file durability, separately from visible effects.
type EditFile interface {
	ReadFile
	WriteAt(context.Context, []byte, int64) (int, Change, error)
	Append(context.Context, []byte) (int, Change, error)
	Truncate(context.Context, int64) (Change, error)
	Sync(context.Context) error
}

// BoundFile owns a bound resource and one mount use. Calls renew the original
// address's authority/generation without resolving its current object. Calls on
// one File are serial and waiting respects the call context; different files
// are independent. An EOF or canceled call does not close the File.
type BoundFile struct {
	native               ReadFile
	editor               EditFile
	readable, appendOnly bool
	order                *OrderScope
	use                  *use
	principal            Principal
	gate                 chan struct{}
	mu                   sync.Mutex
	stop                 func() bool
	closing              bool
	done                 chan struct{}
	closeErr             error
}

func adoptFile(u *use, p Principal, native ReadFile, options OpenOptions) *BoundFile {
	f := &BoundFile{native: native, use: u, principal: p, gate: make(chan struct{}, 1), done: make(chan struct{})}
	f.readable = options.Mode == BoundRead || options.Read
	f.appendOnly = options.AppendOnly
	f.order = orderScope(u.ctx)
	if options.Mode == LiveEdit {
		f.editor = native.(EditFile)
	}
	f.mu.Lock()
	f.stop = context.AfterFunc(u.ctx, func() { _ = f.Close() })
	f.mu.Unlock()
	return f
}

func (f *BoundFile) begin(ctx context.Context, action Action) (context.Context, func(bool), error) {
	call, end := requestScope(ctx, f.use.ctx)
	call, turn, err := enterOrder(call, f.order)
	if err != nil {
		end()
		return nil, nil, err
	}
	finish := func(uncertain bool) { end(); turn.finish(uncertain) }
	f.mu.Lock()
	closing := f.closing
	f.mu.Unlock()
	if closing {
		finish(false)
		return nil, nil, io.ErrClosedPipe
	}
	select {
	case f.gate <- struct{}{}:
	case <-call.Done():
		finish(false)
		return nil, nil, call.Err()
	}
	finish = func(uncertain bool) { end(); <-f.gate; turn.finish(uncertain) }
	f.mu.Lock()
	closing = f.closing
	f.mu.Unlock()
	if closing {
		finish(false)
		return nil, nil, io.ErrClosedPipe
	}
	c := f.use.mount.core
	err = c.authorizeChecked(call, f.principal, action, f.use.address())
	if err == nil {
		err = c.authorizeBinding(call, f.principal, action, f.use.location)
	}
	if err == nil {
		c.mu.Lock()
		valid := c.validAtLocked(f.use.location)
		c.mu.Unlock()
		if !valid {
			err = Fail(MountUnavailable, nil)
		}
	}
	if err == nil {
		err = call.Err()
	}
	if err != nil {
		finish(false)
		return nil, nil, err
	}
	return call, finish, nil
}

func (f *BoundFile) ReadAt(ctx context.Context, p []byte, offset int64) (int, error) {
	call, end, err := f.begin(ctx, ReadAction)
	if err != nil {
		return 0, err
	}
	defer end(false)
	if !f.readable {
		return 0, Fail(UnsupportedOperation, nil)
	}
	if offset < 0 {
		return 0, Fail(InvalidRange, nil)
	}
	n, err := f.native.ReadAt(call, p, offset)
	if n < 0 || n > len(p) {
		return 0, Fail(InvalidUpstreamResponse, err)
	}
	if n < len(p) && err == nil {
		err = io.ErrUnexpectedEOF
	}
	return n, observedCause(call, err)
}

func (f *BoundFile) Stat(ctx context.Context) (Entry, error) {
	call, end, err := f.begin(ctx, StatAction)
	if err != nil {
		return Entry{}, err
	}
	defer end(false)
	meta, err := f.native.Stat(call)
	if err == nil {
		err = checkNativeName(meta, f.use.native())
	}
	if err == nil && meta.Kind != File {
		err = Fail(InvalidUpstreamResponse, nil)
	}
	if err != nil {
		return Entry{}, observedCause(call, err)
	}
	entry, err := project(f.use.address(), meta)
	return entry, observedCause(call, err)
}

// WriteAt modifies the captured object visibly, without staging or publication.
func (f *BoundFile) WriteAt(ctx context.Context, p []byte, offset int64) (int, Change, error) {
	return f.write(ctx, p, offset, false)
}

// Append requires the AppendOnly mode admitted at Open. It never derives an
// append position from Stat and never replaces the opened object by name.
func (f *BoundFile) Append(ctx context.Context, p []byte) (int, Change, error) {
	return f.write(ctx, p, 0, true)
}

func (f *BoundFile) write(ctx context.Context, p []byte, offset int64, appendMode bool) (n int, change Change, err error) {
	change.Effect = Unchanged
	call, end, err := f.begin(ctx, WriteAction)
	if err != nil {
		return 0, change, err
	}
	defer func() { end(change.Effect == Unknown) }()
	if offset < 0 {
		return 0, change, Fail(InvalidRange, nil)
	}
	if f.editor == nil || appendMode != f.appendOnly {
		return 0, change, Fail(UnsupportedOperation, nil)
	}
	if len(p) == 0 {
		return 0, change, nil
	}
	if appendMode {
		n, change, err = f.editor.Append(call, p)
	} else {
		n, change, err = f.editor.WriteAt(call, p, offset)
	}
	if n < 0 || n > len(p) {
		n = 0
		change.Effect = Unknown
		err = Fail(InvalidUpstreamResponse, err)
	}
	if n < len(p) && err == nil {
		err = io.ErrShortWrite
	}
	if change.Effect == Unchanged && n != 0 || change.Effect == Applied && n != len(p) || change.Effect == Incomplete && (n == 0 || n == len(p)) {
		change.Effect = Unknown
		err = Fail(InvalidUpstreamResponse, err)
	}
	change, err = f.use.mount.core.finishChange(f.use, change, err, true)
	return n, change, observedCause(call, err)
}

func (f *BoundFile) Truncate(ctx context.Context, size int64) (change Change, err error) {
	change.Effect = Unchanged
	call, end, err := f.begin(ctx, WriteAction)
	if err != nil {
		return change, err
	}
	defer func() { end(change.Effect == Unknown) }()
	if size < 0 {
		return change, Fail(InvalidRequest, nil)
	}
	if f.editor == nil {
		return change, Fail(UnsupportedOperation, nil)
	}
	change, err = f.editor.Truncate(call, size)
	change, err = f.use.mount.core.finishChange(f.use, change, err, true)
	return change, observedCause(call, err)
}

func (f *BoundFile) Sync(ctx context.Context) error {
	call, end, err := f.begin(ctx, WriteAction)
	if err != nil {
		return err
	}
	defer end(false)
	if f.editor == nil {
		return Fail(UnsupportedOperation, nil)
	}
	return observedCause(call, f.editor.Sync(call))
}

// Close stops admission, cancels calls and closes the native resource. It waits
// for both native Close and all started calls before releasing the mount use.
// A failed close remains owned by the original mount and cannot acknowledge
// retirement. Close is idempotent and needs no remaining file permission.
func (f *BoundFile) Close() error {
	f.mu.Lock()
	if f.closing {
		f.mu.Unlock()
		<-f.done
		f.mu.Lock()
		err := f.closeErr
		f.mu.Unlock()
		return err
	}
	f.closing = true
	stop := f.stop
	f.mu.Unlock()
	if stop != nil {
		stop()
	}
	f.use.cancel()
	err := f.native.Close()
	f.gate <- struct{}{} // A Close receipt alone cannot acknowledge a blocked call.
	if err != nil {
		f.use.failed(err, f.native)
	} else {
		f.use.release()
	}
	f.mu.Lock()
	f.closeErr = err
	close(f.done)
	f.mu.Unlock()
	return err
}
