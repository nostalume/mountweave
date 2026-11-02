package control

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	core "github.com/nostalume/mountweave"
)

var (
	ErrNotFound = errors.New("definition not found")
	ErrConflict = errors.New("definition revision conflict")
	ErrClosed   = errors.New("controller is closed")
)

type Alias struct{ Prefix, Base string }

// Config is native factory input; Secrets contains references, not secret values.
// Revision zero denotes an unsaved definition. Saving does not activate it.
type Definition struct {
	ID             string
	Revision       uint64
	Prefix, Driver string
	Config         json.RawMessage
	Secrets        map[string]string
	Enabled        bool
	Aliases        []Alias
}

// Store owns durability. Commit must atomically compare the current revision
// (zero for absence) with expected, then store the supplied next revision.
// Load/List must return independent values; absence is ErrNotFound.
type Store interface {
	Load(context.Context, string) (Definition, error)
	List(context.Context) ([]Definition, error)
	Commit(context.Context, Definition, uint64) error
}

// Validate must not acquire an exclusive refresh writer. Build runs only after
// the old instance has retired. On error it must return any still-owned Driver
// for cleanup; on success the returned Driver is exclusively transferred.
// Factories resolve native options and secrets, including latest checkpoints.
type Factory struct {
	Validate func(context.Context, Definition) error
	Build    func(context.Context, Definition) (core.Driver, error)
}

type Action string

const (
	SaveAction    Action = "save"
	ApplyAction   Action = "apply"
	ObserveAction Action = "observe"
	StopAction    Action = "stop"
	RestoreAction Action = "restore"
)

type Options struct {
	Core      *core.Core
	Store     Store
	Factories map[string]Factory
	Authorize func(context.Context, core.Principal, Action, string) error
}

type entry struct {
	gate     chan struct{}
	revision uint64
	prefix   string
	mount    *core.Mount
	aliases  []Alias
	pending  *cleanup
}

type cleanup struct {
	driver core.Driver
	done   chan struct{}
	err    error
}

func (e *entry) cleanupError() error {
	if e.pending == nil {
		return nil
	}
	select {
	case <-e.pending.done:
		if e.pending.err == nil {
			e.pending = nil
			return nil
		}
		return e.pending.err
	default:
		return nil
	}
}

func (e *entry) waitCleanup(ctx context.Context) error {
	if e.pending == nil {
		return nil
	}
	if err := e.cleanupError(); err != nil || e.pending == nil {
		return err
	}
	select {
	case <-e.pending.done:
		return e.cleanupError()
	case <-ctx.Done():
		return ctx.Err()
	}
}

type Controller struct {
	opts     Options
	mu       sync.Mutex
	entries  map[string]*entry
	closed   bool
	done     chan struct{}
	closeErr error
}

func New(opts Options) (*Controller, error) {
	if opts.Core == nil || opts.Store == nil || opts.Authorize == nil {
		return nil, errors.New("core, store and management authorizer are required")
	}
	factories := make(map[string]Factory, len(opts.Factories))
	for kind, factory := range opts.Factories {
		if kind == "" || factory.Build == nil {
			return nil, errors.New("factory kind and build are required")
		}
		factories[kind] = factory
	}
	opts.Factories = factories
	return &Controller{opts: opts, entries: make(map[string]*entry), done: make(chan struct{})}, nil
}

func clone(d Definition) Definition {
	d.Config = append(json.RawMessage(nil), d.Config...)
	d.Aliases = append([]Alias(nil), d.Aliases...)
	refs := make(map[string]string, len(d.Secrets))
	for k, v := range d.Secrets {
		refs[k] = v
	}
	d.Secrets = refs
	return d
}

func (c *Controller) validate(ctx context.Context, d Definition) (Factory, error) {
	f, ok := c.opts.Factories[d.Driver]
	if d.ID == "" || !ok {
		return Factory{}, errors.New("definition id and known driver kind are required")
	}
	if err := core.ValidatePath(d.Prefix); err != nil {
		return Factory{}, err
	}
	if len(d.Config) != 0 && !json.Valid(d.Config) {
		return Factory{}, errors.New("invalid native configuration JSON")
	}
	prefixes := []string{d.Prefix}
	for _, alias := range d.Aliases {
		if err := core.ValidatePath(alias.Prefix); err != nil {
			return Factory{}, err
		}
		if err := core.ValidatePath(alias.Base); err != nil {
			return Factory{}, err
		}
		for _, old := range prefixes {
			if core.Within(old, alias.Prefix) || core.Within(alias.Prefix, old) {
				return Factory{}, core.Fail(core.PathAmbiguous, nil)
			}
		}
		prefixes = append(prefixes, alias.Prefix)
	}
	if f.Validate != nil {
		if err := f.Validate(ctx, clone(d)); err != nil {
			return Factory{}, err
		}
	}
	return f, ctx.Err()
}

func (c *Controller) access(ctx context.Context, p core.Principal, action Action, id string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if p == "" {
		return core.Fail(core.Unauthenticated, nil)
	}
	if id == "" && action != RestoreAction && action != ListAction {
		return errors.New("definition id is required")
	}
	if err := c.opts.Authorize(ctx, p, action, id); err != nil {
		return core.Fail(core.PermissionDenied, err)
	}
	return nil
}

func (c *Controller) acquire(ctx context.Context, id string, allowClosed bool) (*entry, error) {
	c.mu.Lock()
	if c.closed && !allowClosed {
		c.mu.Unlock()
		return nil, ErrClosed
	}
	e := c.entries[id]
	if e == nil {
		e = &entry{gate: make(chan struct{}, 1)}
		c.entries[id] = e
	}
	c.mu.Unlock()
	select {
	case e.gate <- struct{}{}:
		c.mu.Lock()
		closed := c.closed
		c.mu.Unlock()
		if err := ctx.Err(); err != nil {
			release(e)
			return nil, err
		}
		if closed && !allowClosed {
			release(e)
			return nil, ErrClosed
		}
		return e, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func release(e *entry) { <-e.gate }

func (c *Controller) Save(ctx context.Context, p core.Principal, d Definition, expected uint64) (Definition, error) {
	if err := c.access(ctx, p, SaveAction, d.ID); err != nil {
		return Definition{}, err
	}
	d = clone(d)
	if _, err := c.validate(ctx, d); err != nil {
		return Definition{}, err
	}
	if expected == ^uint64(0) {
		return Definition{}, ErrConflict
	}
	e, err := c.acquire(ctx, d.ID, false)
	if err != nil {
		return Definition{}, err
	}
	defer release(e)
	d.Revision = expected + 1
	if err = c.opts.Store.Commit(ctx, clone(d), expected); err != nil {
		return Definition{}, err
	}
	return d, nil
}

// Activation describes observed runtime facts, independently of saved intent.
// A non-nil Mount may remain active after a later alias operation fails.
type Activation struct {
	ID     string
	Prefix string
	// RequestedRevision is set by Apply, even when activation fails.
	RequestedRevision uint64
	// Revision is the last installed or successfully disabled definition.
	Revision       uint64
	Mount          *core.Mount
	Runtime        core.MountStatus
	Aliases        []string
	PendingCleanup bool
}

func (c *Controller) observe(id string, e *entry) Activation {
	_ = e.cleanupError()
	a := Activation{ID: id, Prefix: e.prefix, Revision: e.revision, Mount: e.mount, PendingCleanup: e.pending != nil}
	if e.mount != nil {
		a.Runtime = e.mount.Status()
		for _, alias := range e.aliases {
			if b, ok := c.opts.Core.Binding(alias.Prefix); ok && b.Mount == e.mount && b.Base == alias.Base {
				a.Aliases = append(a.Aliases, alias.Prefix)
			}
		}
	}
	return a
}

func (c *Controller) Observe(ctx context.Context, p core.Principal, id string) (Activation, error) {
	if err := c.access(ctx, p, ObserveAction, id); err != nil {
		return Activation{}, err
	}
	e, err := c.acquire(ctx, id, true)
	if err != nil {
		return Activation{}, err
	}
	defer release(e)
	a := c.observe(id, e)
	return a, e.cleanupError()
}

func (c *Controller) stop(ctx context.Context, e *entry) error {
	if err := e.waitCleanup(ctx); err != nil {
		return err
	}
	if e.mount != nil {
		return e.mount.Retire(ctx)
	}
	return nil
}

// Stop does not change saved intent. Restore can enable the definition again.
func (c *Controller) Stop(ctx context.Context, p core.Principal, id string) (Activation, error) {
	if err := c.access(ctx, p, StopAction, id); err != nil {
		return Activation{}, err
	}
	e, err := c.acquire(ctx, id, true)
	if err != nil {
		return Activation{}, err
	}
	defer release(e)
	err = c.stop(ctx, e)
	return c.observe(id, e), err
}

func (c *Controller) Apply(ctx context.Context, p core.Principal, id string, revision uint64) (report Activation, resultErr error) {
	defer func() { report.ID = id; report.RequestedRevision = revision }()
	if err := c.access(ctx, p, ApplyAction, id); err != nil {
		return Activation{}, err
	}
	e, err := c.acquire(ctx, id, false)
	if err != nil {
		return Activation{}, err
	}
	defer release(e)
	d, err := c.opts.Store.Load(ctx, id)
	if err != nil {
		return c.observe(id, e), err
	}
	if d.ID != id || d.Revision != revision || revision == 0 {
		return c.observe(id, e), ErrConflict
	}
	f, err := c.validate(ctx, d)
	if err != nil {
		return c.observe(id, e), err
	}
	if !d.Enabled {
		err = c.stop(ctx, e)
		if err == nil {
			e.revision = revision
		}
		return c.observe(id, e), err
	}
	if err = e.waitCleanup(ctx); err != nil {
		return c.observe(id, e), err
	}
	if e.mount == nil || e.revision != revision || !e.mount.Status().Active {
		if err = c.stop(ctx, e); err != nil {
			return c.observe(id, e), err
		}
		if err = ctx.Err(); err != nil {
			return c.observe(id, e), err
		}
		driver, buildErr := f.Build(ctx, clone(d))
		if buildErr == nil && driver == nil {
			buildErr = errors.New("factory returned no driver")
		}
		if buildErr == nil {
			buildErr = ctx.Err()
		}
		var mount *core.Mount
		if buildErr == nil {
			mount, buildErr = c.opts.Core.Install(d.Prefix, driver)
		}
		if buildErr != nil {
			if driver != nil {
				owned := &cleanup{driver: driver, done: make(chan struct{})}
				e.pending = owned
				go func() { owned.err = owned.driver.Close(); close(owned.done) }()
				buildErr = errors.Join(buildErr, e.waitCleanup(ctx))
			}
			return c.observe(id, e), buildErr
		}
		e.mount, e.revision, e.aliases = mount, revision, append([]Alias(nil), d.Aliases...)
		e.prefix = d.Prefix
	}
	for _, alias := range d.Aliases {
		if b, ok := c.opts.Core.Binding(alias.Prefix); ok && b.Mount == e.mount && b.Base == alias.Base {
			continue
		}
		if err = c.opts.Core.BindAlias(ctx, alias.Prefix, e.mount, alias.Base); err != nil {
			return c.observe(id, e), err
		}
	}
	return c.observe(id, e), nil
}

type Result struct {
	Activation
	Err error
}

// Restore explicitly applies the latest desired definitions, with per-item
// results. It is not an automatic reconciler or an activation transaction.
func (c *Controller) Restore(ctx context.Context, p core.Principal) ([]Result, error) {
	if err := c.access(ctx, p, RestoreAction, ""); err != nil {
		return nil, err
	}
	definitions, err := c.opts.Store.List(ctx)
	if err != nil {
		return nil, err
	}
	results := make([]Result, 0, len(definitions))
	for _, d := range definitions {
		if err = ctx.Err(); err != nil {
			return results, err
		}
		a, applyErr := c.Apply(ctx, p, d.ID, d.Revision)
		results = append(results, Result{a, applyErr})
	}
	return results, nil
}

// Close stops management admission and retires only owned instances. A timeout
// bounds waiting, not native cleanup. Core and Store still belong to the host.
// Failed unadopted cleanup retains its owner and error; it is not retried blindly.
func (c *Controller) Close(ctx context.Context) error {
	c.mu.Lock()
	if !c.closed {
		c.closed = true
		ids := make([]string, 0, len(c.entries))
		for id := range c.entries {
			ids = append(ids, id)
		}
		go func() {
			var wg sync.WaitGroup
			for _, id := range ids {
				wg.Add(1)
				go func() {
					defer wg.Done()
					e, _ := c.acquire(context.Background(), id, true)
					err := c.stop(context.Background(), e)
					release(e)
					if err != nil {
						c.mu.Lock()
						c.closeErr = errors.Join(c.closeErr, fmt.Errorf("%s: %w", id, err))
						c.mu.Unlock()
					}
				}()
			}
			wg.Wait()
			close(c.done)
		}()
	}
	c.mu.Unlock()
	select {
	case <-c.done:
		c.mu.Lock()
		defer c.mu.Unlock()
		return c.closeErr
	default:
	}
	select {
	case <-c.done:
		c.mu.Lock()
		defer c.mu.Unlock()
		return c.closeErr
	case <-ctx.Done():
		return ctx.Err()
	}
}
