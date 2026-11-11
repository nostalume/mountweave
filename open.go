package core

import (
	"context"
	"errors"
	"io"
	"sync"
	"time"
)

// OpenOptions retains the ordinary sequential body path when zero.
type OpenOptions struct {
	Mode      OpenMode
	Binding   FileBinding // Minimum File binding; zero means BoundObject.
	Range     *Range
	Selection ReadSelection
	// NoFollow requires each native component to be free of links/redirects.
	NoFollow bool
	// LiveEdit opts into visible mutations; zero Creation opens an existing file.
	Read       bool
	Creation   FileCreation
	Truncate   bool
	AppendOnly bool
}

type OpenMode string

const (
	StreamRead OpenMode = ""
	BoundRead  OpenMode = "bound_read"
	LiveEdit   OpenMode = "live_edit"
)

type FileCreation string

const (
	OpenExisting FileCreation = ""
	OpenOrCreate FileCreation = "open_or_create"
	OpenNew      FileCreation = "open_new"
)

type FileBinding string

// BoundObject fixes resource identity, permitting in-place edits. PinnedVersion
// also fixes contents, but does not guarantee perpetual native availability.
const (
	BoundObject   FileBinding = "bound_object"
	PinnedVersion FileBinding = "pinned_version"
)

// Validate checks the requested mode, not the Driver's ability to supply it.
func (o OpenOptions) Validate() error {
	if o.Mode != StreamRead && o.Mode != BoundRead && o.Mode != LiveEdit ||
		o.Binding != "" && o.Binding != BoundObject && o.Binding != PinnedVersion ||
		o.Mode == StreamRead && o.Binding != "" || o.Mode != StreamRead && o.Range != nil ||
		o.Mode != LiveEdit && (o.Read || o.Creation != "" || o.Truncate || o.AppendOnly) ||
		o.Creation != OpenExisting && o.Creation != OpenOrCreate && o.Creation != OpenNew ||
		o.Mode == LiveEdit && (o.Binding == PinnedVersion || o.Selection.Version != "") {
		return Fail(InvalidRequest, nil)
	}
	if o.Range != nil && !o.Range.Valid() {
		return Fail(InvalidRange, nil)
	}
	return o.Selection.When.Validate()
}

// Opened transfers any returned Body or File even when Open also returns an
// error. For streams, Metadata.Size is the total object size; Length and Range
// describe the returned body. Unknown sizes remain nil.
type Opened struct {
	Change   Change // LiveEdit opening effects survive errors and cleanup.
	Metadata Metadata
	Body     io.ReadCloser
	File     ReadFile
	Binding  FileBinding
	Length   *int64
	Range    *ContentRange
}

type Content struct {
	Change  Change
	Entry   Entry
	Body    io.ReadCloser
	File    *BoundFile
	Binding FileBinding
	Length  *int64
	Range   *ContentRange
}

func (c *Core) Open(ctx context.Context, p Principal, target Address, options OpenOptions) (result Content, err error) {
	result.Change.Effect = Unchanged
	ctx, turn, err := enterOrder(ctx, nil)
	if err != nil {
		return result, err
	}
	handoff := false
	defer func() {
		if !handoff {
			turn.finish(options.Mode == LiveEdit && result.Change.Effect == Unknown)
		}
	}()
	if options.Range != nil && !options.Range.Valid() {
		return result, Fail(InvalidRange, nil)
	}
	var lifetime []context.Context
	if options.Mode != StreamRead {
		lifetime = []context.Context{context.WithoutCancel(ctx)}
	}
	action := ReadAction
	if options.Mode == LiveEdit {
		action = WriteAction
	}
	u, _, err := c.admit(ctx, p, action, target, lifetime...)
	result, err = c.openUse(ctx, p, u, options, err, nil, turn)
	handoff = err == nil && options.Mode == StreamRead && result.Body != nil
	return result, err
}

func (c *Core) openAt(ctx context.Context, p Principal, at *location, options OpenOptions) (Content, error) {
	var lifetime []context.Context
	if options.Mode == BoundRead {
		lifetime = []context.Context{context.WithoutCancel(ctx)}
	}
	u, err := c.admitAt(ctx, p, ReadAction, at, lifetime...)
	return c.openUse(ctx, p, u, options, err, nil)
}

// openUse owns timeout, adoption and cleanup for either a rooted open or a child
// open from an already bound parent. Only the parent constructs the latter.
func (c *Core) openUse(ctx context.Context, p Principal, u *use, options OpenOptions, err error, openChild func(context.Context) (Opened, error), bodyOrder ...*OrderTurn) (Content, error) {
	unchanged := Content{Change: Change{Effect: Unchanged}}
	if err != nil {
		return unchanged, err
	}
	if u == nil {
		return unchanged, Fail(NotFile, nil)
	}
	if err := options.Validate(); err != nil {
		u.release()
		return unchanged, err
	}
	if options.Mode == LiveEdit {
		if err = u.changeTarget(); err == nil && options.Read {
			err = c.authorizeChecked(ctx, p, ReadAction, u.address())
			if err == nil {
				err = c.authorizeBinding(ctx, p, ReadAction, u.location)
			}
		}
		if err != nil {
			u.release()
			return unchanged, err
		}
	}
	call := u.ctx
	if options.Mode != StreamRead {
		var end func()
		call, end = requestScope(ctx, u.ctx)
		defer end()
	}
	var phase sync.Mutex
	adopted, timedOut := false, false
	var timer *time.Timer
	if c.opts.OpenTimeout > 0 {
		timer = time.AfterFunc(c.opts.OpenTimeout, func() {
			phase.Lock()
			defer phase.Unlock()
			if !adopted {
				timedOut = true
				u.cancel()
			}
		})
	}
	rel := u.native()
	var opened Opened
	var openErr error
	if openChild == nil {
		opened, openErr = u.mount.driver.Open(call, rel, options)
	} else {
		opened, openErr = openChild(call)
	}
	phase.Lock()
	if timer != nil {
		timer.Stop()
	}
	adopted = true
	expired := timedOut
	phase.Unlock()
	if openErr == nil {
		openErr = call.Err()
	}
	if expired {
		openErr = errors.Join(context.DeadlineExceeded, openErr)
	}
	if options.Mode == LiveEdit {
		opened.Change, openErr = c.finishChange(u, opened.Change, openErr, true)
	}
	var owner *OrderTurn
	if len(bodyOrder) != 0 {
		owner = bodyOrder[0]
	}
	return c.adoptOpened(u, p, options, opened, openErr, owner)
}

func (c *Core) adoptOpened(u *use, p Principal, options OpenOptions, opened Opened, openErr error, owner *OrderTurn) (Content, error) {
	if options.Mode != LiveEdit {
		opened.Change = Change{Effect: Unchanged}
	}
	if openErr == nil {
		openErr = u.ctx.Err()
	}
	if openErr == nil {
		openErr = validateOpened(opened, u.native(), options)
	}
	var entry Entry
	if openErr == nil {
		entry, openErr = project(u.address(), opened.Metadata)
	}
	if openErr != nil {
		resources := openedResources{opened.Body, opened.File}
		closeErr := resources.Close()
		if closeErr == nil {
			u.release()
		} else {
			u.failed(closeErr, resources)
		}
		return Content{Change: opened.Change}, errors.Join(openErr, closeErr)
	}
	if options.Mode != StreamRead {
		return Content{Change: opened.Change, Entry: entry, File: adoptFile(u, p, opened.File, options), Binding: opened.Binding}, nil
	}
	b := &ownedBody{inner: opened.Body, use: u, done: make(chan struct{}), remaining: -1, order: owner}
	if opened.Length != nil {
		b.remaining = *opened.Length
	}
	b.mu.Lock()
	b.stop = context.AfterFunc(u.ctx, func() { _ = b.Close() })
	b.mu.Unlock()
	var length *int64
	if opened.Length != nil {
		n := *opened.Length
		length = &n
	}
	var actual *ContentRange
	if opened.Range != nil {
		r := *opened.Range
		actual = &r
	}
	return Content{Change: opened.Change, Entry: entry, Body: b, Length: length, Range: actual}, nil
}

type openedResources struct{ body, file io.Closer }

func (r openedResources) Close() error {
	var err error
	if r.body != nil {
		err = r.body.Close()
	}
	if r.file != nil {
		err = errors.Join(err, r.file.Close())
	}
	return err
}

func validateOpened(o Opened, rel NativeAddress, options OpenOptions) error {
	if o.Metadata.Kind == Directory {
		return Fail(NotFile, nil)
	}
	if err := checkNativeName(o.Metadata, rel); err != nil {
		return err
	}
	if options.Mode != StreamRead {
		if o.Metadata.Kind != File || o.File == nil || o.Body != nil || o.Range != nil || o.Length != nil ||
			(o.Binding != BoundObject && o.Binding != PinnedVersion) ||
			options.Binding == PinnedVersion && o.Binding != PinnedVersion {
			return Fail(InvalidUpstreamResponse, errors.New("invalid bound file response"))
		}
		if options.Mode == LiveEdit {
			if _, ok := o.File.(EditFile); !ok {
				return Fail(InvalidUpstreamResponse, nil)
			}
		}
		return nil
	}
	if o.Body == nil || o.File != nil || o.Binding != "" {
		return Fail(InvalidUpstreamResponse, errors.New("invalid stream response"))
	}
	wanted := options.Range
	if o.Length != nil && *o.Length < 0 {
		return Fail(InvalidUpstreamResponse, nil)
	}
	total := o.Length
	if o.Range != nil {
		if wanted == nil || o.Length == nil {
			return Fail(InvalidUpstreamResponse, nil)
		}
		expected, err := wanted.Resolve(o.Range.Total)
		if err != nil || expected != *o.Range || *o.Length != o.Range.End-o.Range.Start+1 {
			return Fail(InvalidUpstreamResponse, err)
		}
		total = &o.Range.Total
	}
	if total != nil && o.Metadata.Size != nil && *total != *o.Metadata.Size {
		return Fail(InvalidUpstreamResponse, errors.New("object size disagrees with the returned body"))
	}
	return nil
}
