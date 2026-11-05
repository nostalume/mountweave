// Package transfer owns durable file-upload intent, not task execution or Core
// file semantics. The host supplies protected persistence and stable bindings.
// New intents receive an immutable Generation; legacy records derive a stable
// reference from their stored immutable fields without query-time writes.
// Preserve every record field and the existing hold keys/checkpoint log. A
// terminal record may still owe retention release; Run/Abort can finish that
// handoff without opening source bytes, rebinding a Mount or replaying a log.
package transfer

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
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
	Address      core.Address
}

// UnmarshalJSON accepts the original tree-only record without rewriting its
// checkpoint or retention identity. New records always store tagged addresses.
func (t *Target) UnmarshalJSON(data []byte) error {
	var v struct {
		DefinitionID string
		Revision     uint64
		Address      *core.Address
		Path         *string
	}
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if err := d.Decode(&v); err != nil {
		return err
	}
	if (v.Address == nil) == (v.Path == nil) {
		return ErrInvalid
	}
	t.DefinitionID, t.Revision = v.DefinitionID, v.Revision
	if v.Address != nil {
		t.Address = *v.Address
	} else {
		t.Address = core.Path(*v.Path)
	}
	return t.Address.Validate()
}

type Request struct {
	InputID string
	Target  Target
	// A zero policy normalizes to complete exclusive publication. Explicit
	// Publish predicates are persisted unchanged and reused by every invocation.
	Policy core.WritePolicy
}

// UnmarshalJSON preserves original boolean intents while new records store the
// exact immutable publication predicate. Neither retry nor resume refreshes it.
func (r *Request) UnmarshalJSON(data []byte) error {
	var v struct {
		InputID string
		Target  Target
		Policy  *core.WritePolicy
		Replace *bool
	}
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if err := d.Decode(&v); err != nil {
		return err
	}
	if v.Policy != nil && v.Replace != nil {
		return ErrInvalid
	}
	*r = Request{InputID: v.InputID, Target: v.Target, Policy: core.Publish(core.IfAbsent())}
	if v.Policy != nil {
		r.Policy = *v.Policy
	} else if v.Replace != nil && *v.Replace {
		r.Policy = core.Publish()
	}
	return r.normalize()
}

func (r *Request) normalize() error {
	if r.Policy == (core.WritePolicy{}) {
		r.Policy = core.Publish(core.IfAbsent())
	}
	if !r.Policy.Complete() {
		return ErrInvalid
	}
	return r.Policy.Validate()
}

// Keep the original wire field order when deriving pre-Generation identities.
type legacyRequest struct {
	InputID string
	Target  legacyTarget
	Replace bool
}
type legacyTarget struct {
	DefinitionID string
	Revision     uint64
	Path         string
}
type State string

const (
	Preparing State = "preparing"
	Ready     State = "ready"
	Completed State = "completed"
	Aborted   State = "aborted"
)

type Record struct {
	ID, Namespace     string
	Generation        string
	Owner             core.Principal
	Revision          uint64
	Request           Request
	SourceVersion     string
	State             State
	CheckpointBytes   int
	Change            core.Change
	RetentionReleased bool
}

// CleanupPending describes durable input retention, not arbitrary runtime I/O.
func (r Record) CleanupPending() bool {
	return (r.State == Completed || r.State == Aborted) && !r.RetentionReleased
}

// ExecutionOptions optionally fences a delayed command against ID reuse.
// Omitting it retains direct ID-based calls. It grants no authority or lease.
// Run/Abort method values now have a variadic options parameter.
type ExecutionOptions struct{ ExpectedGeneration string }

// Lister is optional for execution, required for bounded recovery discovery.
// Return at most limit metadata records, in increasing ID order strictly after
// after. Do not load checkpoints. This is not a snapshot across pages.
type Lister interface {
	List(context.Context, string, int) ([]Record, error)
}
type Page struct {
	Items []Record
	Next  string
}

// SupportsListing reports whether this service can provide recovery discovery.
func (s *Service) SupportsListing() bool { _, ok := s.opts.Store.(Lister); return ok }

// Store must return independent records, bound retained records/bytes, and
// durably CAS the previous revision (zero means absent). Commit nil confirms
// persistence, not just admission to a background writer. Errors may be uncertain;
// the next invocation reloads instead of assuming rollback. Records/checkpoints
// require host protection and a single service executor across processes.
// Persist every Record field, including Generation and RetentionReleased; these
// belong to the same CAS as intent/progress, not to a separate job database.
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
	ListAction    Action = "list"
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
	return s.validate(r, id)
}
func (s *Service) validate(r Record, id string) (Record, error) {
	if r.ID != id || id == "" || r.Namespace != s.opts.Namespace || r.Revision == 0 || r.CheckpointBytes < 0 || r.CheckpointBytes > s.opts.MaxCheckpointBytes {
		return Record{}, ErrInvalid
	}
	if err := r.Request.normalize(); err != nil {
		return Record{}, err
	}
	if r.Generation == "" {
		when := r.Request.Policy.When()
		if r.Request.Target.Address.Namespace() != core.TreeNamespace || !when.Empty() && !when.IsAbsent() {
			return Record{}, ErrInvalid
		}
		// Legacy identities derive only from persisted immutable fields. A new
		// intent always receives a random generation; GET performs no migration.
		data, _ := json.Marshal(struct {
			ID, Namespace string
			Owner         core.Principal
			Request       legacyRequest
			Source        string
		}{r.ID, r.Namespace, r.Owner, legacyRequest{r.Request.InputID, legacyTarget{r.Request.Target.DefinitionID, r.Request.Target.Revision, r.Request.Target.Address.Value()}, when.Empty()}, r.SourceVersion})
		sum := sha256.Sum256(data)
		r.Generation = "legacy-" + hex.EncodeToString(sum[:])
	}
	return r, nil
}

func (s *Service) access(ctx context.Context, p core.Principal, action Action, r Record) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := s.opts.Authorize(ctx, p, action, r); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return core.Fail(core.PermissionDenied, err)
	}
	return ctx.Err()
}

// List bounds scanned metadata, including entries hidden by observation policy.
// An empty visible page can carry Next. It never calls Load per row or Checkpoint.
func (s *Service) List(ctx context.Context, p core.Principal, after string, limit int) (Page, error) {
	if limit < 1 {
		return Page{}, ErrInvalid
	}
	if err := s.access(ctx, p, ListAction, Record{Namespace: s.opts.Namespace}); err != nil {
		return Page{}, err
	}
	lister, ok := s.opts.Store.(Lister)
	if !ok {
		return Page{}, core.Fail(core.UnsupportedOperation, nil)
	}
	rows, err := lister.List(ctx, after, limit)
	if err != nil {
		return Page{}, err
	}
	if len(rows) > limit {
		return Page{}, ErrInvalid
	}
	page := Page{Items: make([]Record, 0, len(rows))}
	last := after
	for _, r := range rows {
		if r.ID <= last {
			return Page{}, ErrInvalid
		}
		r, err = s.validate(r, r.ID)
		if err != nil {
			return Page{}, err
		}
		last = r.ID
		if err = s.access(ctx, p, ObserveAction, r); err != nil {
			if ctx.Err() != nil {
				return Page{}, ctx.Err()
			}
			continue
		}
		page.Items = append(page.Items, r)
	}
	if len(rows) == limit {
		page.Next = last
	}
	return page, ctx.Err()
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

func (s *Service) releaseRetention(ctx context.Context, r *Record) error {
	if r.RetentionReleased {
		return nil
	}
	if err := s.unhold(ctx, *r); err != nil {
		return err
	}
	next := *r
	next.RetentionReleased = true
	return s.commit(ctx, r, next, nil)
}

// Create uses a caller-chosen idempotency ID. A persisted preparation is visible
// even if source handoff fails. Retry the identical request or Abort that ID;
// never abandon an uncertain Commit and create another attempt implicitly.
func (s *Service) Create(ctx context.Context, p core.Principal, id string, request Request) (Record, error) {
	if err := request.normalize(); err != nil {
		return Record{}, err
	}
	leave, err := s.enter(ctx, id)
	if err != nil {
		return Record{}, err
	}
	defer leave()
	r, err := s.load(ctx, id)
	if errors.Is(err, ErrNotFound) {
		if _, err := json.Marshal(request.Policy); err != nil {
			return Record{}, err
		}
		if p == "" || request.InputID == "" || request.Target.DefinitionID == "" || request.Target.Revision == 0 || request.Target.Address.Validate() != nil || request.Target.Address.IsPrefix() {
			return Record{}, ErrInvalid
		}
		r = Record{ID: id, Namespace: s.opts.Namespace, Generation: rand.Text(), Owner: p, Request: request, State: Preparing, Change: core.Change{Effect: core.Unchanged}}
		if err = s.access(ctx, p, CreateAction, r); err != nil {
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
		if err = s.access(ctx, p, CreateAction, r); err != nil {
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
	if err = s.access(ctx, p, ObserveAction, r); err != nil {
		return Record{}, err
	}
	return r, nil
}

// Run and Abort are synchronous and may be called directly or from task.Work.
// Cancellation pauses Run. Abort is durable native cleanup, not cancellation of
// a worker that may still be active. No scheduler, goroutine or Mount is retained.
func (s *Service) Run(ctx context.Context, p core.Principal, id string, options ...ExecutionOptions) (Record, error) {
	return s.execute(ctx, p, id, false, options)
}
func (s *Service) Abort(ctx context.Context, p core.Principal, id string, options ...ExecutionOptions) (Record, error) {
	return s.execute(ctx, p, id, true, options)
}
func (s *Service) execute(ctx context.Context, p core.Principal, id string, abort bool, options []ExecutionOptions) (Record, error) {
	if len(options) > 1 || (len(options) == 1 && options[0].ExpectedGeneration == "") {
		return Record{}, ErrInvalid
	}
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
	if err = s.access(ctx, p, action, r); err != nil {
		return Record{}, err
	}
	if len(options) == 1 && options[0].ExpectedGeneration != r.Generation {
		return Record{}, ErrConflict
	}
	if r.State == Completed || r.State == Aborted {
		err = s.releaseRetention(ctx, &r)
		return r, err
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
		err = s.releaseRetention(ctx, &r)
		return r, err
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
		return s.opts.Core.Write(selected.Context(ctx), r.Owner, r.Request.Target.Address, body, &length, core.WriteOptions{Policy: r.Request.Policy, Continuation: &core.WriteContinuation{Checkpoint: checkpoint, Append: appendRecord, Abort: abort}})
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
		runErr = errors.Join(runErr, s.releaseRetention(persistCtx, &r))
	} else if change.Effect != previousEffect {
		// Status must preserve an uncertain file effect after a failed invocation.
		runErr = errors.Join(runErr, s.commit(context.WithoutCancel(ctx), &r, r, nil))
	}
	return r, runErr
}
