// Package transfer owns durable file-upload intent, not task execution or Core
// file semantics. The host supplies protected persistence and stable bindings.
package transfer

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"unicode/utf8"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/input"
)

var (
	ErrNotFound = errors.New("transfer not found")
	ErrConflict = errors.New("transfer revision conflict")
	ErrBusy     = errors.New("transfer already executing")
	ErrInvalid  = errors.New("invalid transfer intent")
)

// Target is a stable host configuration identity, not a serialized Mount. Bind
// must reject changed native roots/accounts/revisions and return the Selection
// captured from that exact activation. Credential refresh facts stay elsewhere.
type Target struct {
	DefinitionID string
	Revision     uint64
	Path         string
}
type Request struct {
	InputID string
	Target  Target
	Replace bool
}
type State string

const (
	Preparing State = "preparing"
	Ready     State = "ready"
	Completed State = "completed"
	Aborted   State = "aborted"
)

type Record struct {
	ID, Namespace   string
	Owner           core.Principal
	Revision        uint64
	Request         Request
	SourceVersion   string
	State           State
	CheckpointBytes int
	Change          core.Change
}

// Store must return independent records, bound retained records/bytes, and
// durably CAS the previous revision (zero means absent). Commit nil confirms
// persistence, not just admission to a background writer. Errors may be uncertain;
// the next invocation reloads instead of assuming rollback. Records/checkpoints
// require host protection and a single service executor across processes.
// Load returns metadata only. Checkpoint returns independent, complete recovery
// bytes at the exact supplied intent revision, or ErrConflict. Commit atomically
// appends its delta and stores metadata; next.CheckpointBytes must match the total.
// A nil delta changes metadata only. Capacity counts the entire retained log.
// The store must append records, not rewrite all prior checkpoint bytes per part.
// Commit borrows delta until return; copy it if retained. Current-revision replay
// suffices: the contract does not require retaining historical full snapshots.
type Store interface {
	Load(context.Context, string) (Record, error)
	Checkpoint(context.Context, string, uint64) ([]byte, error)
	Commit(context.Context, Record, uint64, []byte) error
}
type Action string

const (
	CreateAction  Action = "create"
	ObserveAction Action = "observe"
	RunAction     Action = "run"
	AbortAction   Action = "abort"
)

type Options struct {
	Core      *core.Core
	Inputs    *input.Store
	Store     Store
	Namespace string
	// MaxCheckpointBytes bounds the entire retained recovery log per intent.
	MaxCheckpointBytes int
	Authorize          func(context.Context, core.Principal, Action, Record) error
	Bind               func(context.Context, core.Principal, Target) (*core.Selection, error)
}
type Service struct {
	opts   Options
	mu     sync.Mutex
	active map[string]bool
}

func New(opts Options) (*Service, error) {
	if opts.Core == nil || opts.Inputs == nil || opts.Store == nil || opts.Namespace == "" || !utf8.ValidString(opts.Namespace) || opts.MaxCheckpointBytes <= 0 || opts.Authorize == nil || opts.Bind == nil {
		return nil, ErrInvalid
	}
	return &Service{opts: opts, active: make(map[string]bool)}, nil
}
func (s *Service) enter(ctx context.Context, id string) (func(), error) {
	if id == "" || !utf8.ValidString(id) {
		return nil, ErrInvalid
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.active[id] {
		return nil, ErrBusy
	}
	s.active[id] = true
	return func() { s.mu.Lock(); delete(s.active, id); s.mu.Unlock() }, nil
}
func (s *Service) load(ctx context.Context, id string) (Record, error) {
	r, err := s.opts.Store.Load(ctx, id)
	if err != nil {
		return Record{}, err
	}
	if r.ID != id || r.Namespace != s.opts.Namespace || r.Revision == 0 || r.CheckpointBytes < 0 || r.CheckpointBytes > s.opts.MaxCheckpointBytes {
		return Record{}, ErrInvalid
	}
	return r, nil
}
func (s *Service) commit(ctx context.Context, r *Record, next Record, delta []byte) error {
	if len(delta) > s.opts.MaxCheckpointBytes-r.CheckpointBytes {
		return core.Fail(core.CapacityExceeded, nil)
	}
	next.CheckpointBytes = r.CheckpointBytes + len(delta)
	if r.Revision == ^uint64(0) {
		return ErrInvalid
	}
	next.Revision = r.Revision + 1
	if err := s.opts.Store.Commit(ctx, next, r.Revision, delta); err != nil {
		return err
	}
	*r = next
	return nil
}
func (s *Service) holdKey(id string) string {
	return strconv.Itoa(len(s.opts.Namespace)) + ":" + s.opts.Namespace + id
}
func (s *Service) unhold(ctx context.Context, r Record) error {
	err := s.opts.Inputs.Unhold(ctx, r.Request.InputID, s.holdKey(r.ID))
	if errors.Is(err, input.ErrNotFound) {
		return nil
	}
	return err
}

// Create uses a caller-chosen idempotency ID. A persisted preparation is visible
// even if source handoff fails. Retry the identical request or Abort that ID;
// never abandon an uncertain Commit and create another attempt implicitly.
func (s *Service) Create(ctx context.Context, p core.Principal, id string, request Request) (Record, error) {
	leave, err := s.enter(ctx, id)
	if err != nil {
		return Record{}, err
	}
	defer leave()
	r, err := s.load(ctx, id)
	if errors.Is(err, ErrNotFound) {
		if p == "" || request.InputID == "" || request.Target.DefinitionID == "" || request.Target.Revision == 0 || core.ValidatePath(request.Target.Path) != nil {
			return Record{}, ErrInvalid
		}
		r = Record{ID: id, Namespace: s.opts.Namespace, Owner: p, Request: request, State: Preparing, Change: core.Change{Effect: core.Unchanged}}
		if err = s.opts.Authorize(ctx, p, CreateAction, r); err != nil {
			return Record{}, err
		}
		source, sourceErr := s.opts.Inputs.Status(ctx, request.InputID)
		if sourceErr != nil {
			return Record{}, sourceErr
		}
		if !source.Sealed || source.Released {
			return Record{}, input.ErrState
		}
		r.SourceVersion = source.Version
		if err = s.commit(ctx, &r, r, nil); err != nil {
			return r, err
		}
	} else {
		if err != nil {
			return Record{}, err
		}
		if err = s.opts.Authorize(ctx, p, CreateAction, r); err != nil {
			return Record{}, err
		}
		if r.Owner != p || r.Request != request {
			return Record{}, ErrConflict
		}
	}
	if r.State == Preparing {
		if err = s.opts.Inputs.Hold(ctx, r.Request.InputID, s.holdKey(id)); err != nil {
			return r, err
		}
		next := r
		next.State = Ready
		err = s.commit(ctx, &r, next, nil)
	}
	return r, err
}
func (s *Service) Status(ctx context.Context, p core.Principal, id string) (Record, error) {
	r, err := s.load(ctx, id)
	if err != nil {
		return Record{}, err
	}
	if err = s.opts.Authorize(ctx, p, ObserveAction, r); err != nil {
		return Record{}, err
	}
	return r, nil
}

// Run and Abort are synchronous and may be called directly or from task.Work.
// Cancellation pauses Run. Abort is durable native cleanup, not cancellation of
// a worker that may still be active. No scheduler, goroutine or Mount is retained.
func (s *Service) Run(ctx context.Context, p core.Principal, id string) (Record, error) {
	return s.execute(ctx, p, id, false)
}
func (s *Service) Abort(ctx context.Context, p core.Principal, id string) (Record, error) {
	return s.execute(ctx, p, id, true)
}
func (s *Service) execute(ctx context.Context, p core.Principal, id string, abort bool) (Record, error) {
	leave, err := s.enter(ctx, id)
	if err != nil {
		return Record{}, err
	}
	defer leave()
	r, err := s.load(ctx, id)
	if err != nil {
		return Record{}, err
	}
	action := RunAction
	if abort {
		action = AbortAction
	}
	if err = s.opts.Authorize(ctx, p, action, r); err != nil {
		return Record{}, err
	}
	if r.State == Completed || r.State == Aborted {
		return r, s.unhold(ctx, r)
	}
	if r.State != Preparing && r.State != Ready {
		return r, ErrInvalid
	}
	if abort && r.CheckpointBytes == 0 {
		next := r
		next.State = Aborted
		if err = s.commit(ctx, &r, next, nil); err != nil {
			return r, err
		}
		return r, s.unhold(ctx, r)
	}
	if r.State == Preparing {
		if err = s.opts.Inputs.Hold(ctx, r.Request.InputID, s.holdKey(id)); err != nil {
			return r, err
		}
		next := r
		next.State = Ready
		if err = s.commit(ctx, &r, next, nil); err != nil {
			return r, err
		}
	}
	selected, err := s.opts.Bind(ctx, r.Owner, r.Request.Target)
	if err != nil {
		return r, err
	}
	if selected == nil {
		return r, ErrInvalid
	}
	ref, err := s.opts.Inputs.AcquireHeld(ctx, r.Request.InputID, s.holdKey(id))
	if err != nil {
		return r, err
	}

	checkpoint, err := s.opts.Store.Checkpoint(ctx, id, r.Revision)
	if err != nil {
		return r, errors.Join(err, ref.Close())
	}
	if len(checkpoint) != r.CheckpointBytes {
		return r, errors.Join(ErrInvalid, ref.Close())
	}
	var saved sync.Mutex
	var appendErr error
	appendRecord := func(saveCtx context.Context, delta []byte) error {
		saved.Lock()
		defer saved.Unlock()
		if appendErr != nil {
			return appendErr
		}
		if len(delta) == 0 {
			return ErrInvalid
		}
		appendErr = s.commit(saveCtx, &r, r, delta)
		return appendErr
	}
	change, runErr := func() (change core.Change, err error) {
		change.Effect = core.Unchanged
		defer func() { err = errors.Join(err, ref.Close()) }()
		source := ref.Record()
		if source.Version != r.SourceVersion {
			return change, ErrConflict
		}
		body, err := ref.Open(ctx)
		if err != nil {
			return change, err
		}
		defer func() { err = errors.Join(err, body.Close()) }()
		length := source.Offset
		return s.opts.Core.Write(selected.Context(ctx), r.Owner, r.Request.Target.Path, body, &length, core.WriteOptions{Replace: r.Request.Replace, Continuation: &core.WriteContinuation{Checkpoint: checkpoint, Append: appendRecord, Abort: abort}})
	}()
	previousEffect := r.Change.Effect
	r.Change = change
	if change.Effect == core.Applied || (abort && change.Effect == core.Unchanged && runErr == nil) {
		next := r
		next.State = Completed
		if change.Effect != core.Applied {
			next.State = Aborted
		}
		// Acknowledged effects must survive caller cancellation before retention ends.
		persistCtx := context.WithoutCancel(ctx)
		if err = s.commit(persistCtx, &r, next, nil); err != nil {
			return r, errors.Join(runErr, err)
		}
		runErr = errors.Join(runErr, s.unhold(persistCtx, r))
	} else if change.Effect != previousEffect {
		// Status must preserve an uncertain file effect after a failed invocation.
		runErr = errors.Join(runErr, s.commit(context.WithoutCancel(ctx), &r, r, nil))
	}
	return r, runErr
}
