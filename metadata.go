package core

import (
	"context"
	"errors"
	"strings"
	"time"
)

type Kind string

const (
	File         Kind = "file"
	Directory    Kind = "directory"
	PrefixGroup  Kind = "prefix"
	SymbolicLink Kind = "symlink" // Only returned by Lstat.
)

// Metadata contains observations, not an object identity or an owned handle.
type Metadata struct {
	Evidence   Evidence
	Name       string
	Kind       Kind
	Size       *int64
	ModifiedAt *time.Time
}

type Entry struct {
	Evidence   Evidence   `json:"evidence,omitzero"`
	Address    Address    `json:"address"`
	Name       string     `json:"name"`
	Kind       Kind       `json:"kind"`
	Size       *int64     `json:"size"`
	ModifiedAt *time.Time `json:"modified_at"`
	Virtual    bool       `json:"virtual"`
}

func virtualEntry(p Address) Entry {
	name := ""
	if p.value != "/" {
		name = p.value[strings.LastIndexByte(p.value, '/')+1:]
	}
	return Entry{Address: p, Name: name, Kind: Directory, Virtual: true}
}

func project(p Address, m Metadata) (Entry, error) {
	return projectMetadata(p, m, false)
}

func projectMetadata(p Address, m Metadata, leaf bool) (Entry, error) {
	if m.Evidence.Validator != (Validator{}) && !m.Evidence.Validator.Valid() {
		return Entry{}, Fail(InvalidUpstreamResponse, errors.New("invalid native validator"))
	}
	if m.Kind != File && m.Kind != Directory && !(p.prefix && m.Kind == PrefixGroup) && !(leaf && m.Kind == SymbolicLink) {
		return Entry{}, Fail(InvalidUpstreamResponse, nil)
	}
	if m.Size != nil && (*m.Size < 0 || m.Kind != File) {
		return Entry{}, Fail(InvalidUpstreamResponse, nil)
	}
	e := virtualEntry(p)
	if p.namespace == KeyNamespace {
		e.Name = p.value
	}
	e.Kind = m.Kind
	e.Evidence = m.Evidence
	e.Virtual = false
	if m.Size != nil {
		n := *m.Size
		e.Size = &n
	}
	if m.ModifiedAt != nil {
		t := m.ModifiedAt.UTC()
		if t.Year() < 1 || t.Year() > 9999 {
			return Entry{}, Fail(InvalidUpstreamResponse, errors.New("invalid metadata timestamp"))
		}
		e.ModifiedAt = &t
	}
	return e, nil
}

func (c *Core) Stat(ctx context.Context, p Principal, target Address, selections ...ReadSelection) (Entry, error) {
	return c.Inspect(ctx, p, StatAction, target, selections...)
}

// Lstat observes the final entry of a tree path without following a link.
// Intermediate links are refused. It uses the same authority as Stat and does
// not bind the entry for later operations.
func (c *Core) Lstat(ctx context.Context, p Principal, target Address) (Entry, error) {
	return c.inspect(ctx, p, StatAction, target, true)
}

// Inspect checks metadata under the requested action without opening content.
func (c *Core) Inspect(ctx context.Context, p Principal, action Action, target Address, selections ...ReadSelection) (Entry, error) {
	return c.inspect(ctx, p, action, target, false, selections...)
}

func (c *Core) inspect(ctx context.Context, p Principal, action Action, target Address, leaf bool, selections ...ReadSelection) (Entry, error) {
	ctx, turn, err := enterOrder(ctx, nil)
	if err != nil {
		return Entry{}, err
	}
	defer turn.finish(false)
	if len(selections) > 1 {
		return Entry{}, Fail(InvalidRequest, nil)
	}
	var selection ReadSelection
	if len(selections) == 1 {
		selection = selections[0]
	}
	if err := selection.When.Validate(); err != nil {
		return Entry{}, err
	}
	if action != StatAction && action != ListAction && action != ReadAction {
		return Entry{}, Fail(InvalidRequest, nil)
	}
	if leaf && target.Namespace() != TreeNamespace {
		return Entry{}, Fail(UnsupportedOperation, nil)
	}
	u, _, err := c.admit(ctx, p, action, target)
	if err != nil {
		return Entry{}, err
	}
	if u == nil {
		if action == ReadAction {
			return Entry{}, Fail(NotFile, nil)
		}
		entry := virtualEntry(target)
		return entry, selection.Check(entry.Evidence)
	}
	defer u.release()
	if leaf {
		driver, ok := u.mount.driver.(LeafStatter)
		if !ok {
			return Entry{}, Fail(UnsupportedOperation, errors.New("driver cannot observe the final entry without following links"))
		}
		meta, err := driver.Lstat(u.ctx, u.native())
		return observedEntry(u, meta, err, true)
	}
	// Stat already returns the selected observation. Compare it once here, after
	// validating the endpoint kind; never use this as a precheck for Open/commit.
	entry, err := c.observe(u, selection.Version)
	if err != nil {
		return Entry{}, err
	}
	if action == ReadAction && entry.Kind != File {
		return Entry{}, Fail(NotFile, nil)
	}
	return entry, selection.When.Check(entry.Evidence, true)
}

func (c *Core) observe(u *use, version Version) (Entry, error) {
	meta, err := u.mount.driver.Stat(u.ctx, u.native(), ReadSelection{Version: version})
	return observedEntry(u, meta, err, false)
}

func observedEntry(u *use, meta Metadata, err error, leaf bool) (Entry, error) {
	if err != nil {
		return Entry{}, err
	}
	if err = u.ctx.Err(); err != nil {
		return Entry{}, err
	}
	if err = checkNativeName(meta, u.native()); err != nil {
		return Entry{}, err
	}
	return projectMetadata(u.address(), meta, leaf)
}

func checkNativeName(m Metadata, rel NativeAddress) error {
	if rel.namespace == TreeNamespace {
		return checkName(m, rel.value)
	}
	if m.Kind != File || m.Name != rel.value {
		return Fail(InvalidUpstreamResponse, nil)
	}
	return nil
}

func checkName(m Metadata, rel string) error {
	if rel != "/" {
		name := rel[strings.LastIndexByte(rel, '/')+1:]
		if !ValidName(m.Name) {
			return Fail(UnrepresentableName, nil)
		}
		if name != m.Name {
			return Fail(InvalidUpstreamResponse, errors.New("resolved name differs from the requested name"))
		}
	}
	return nil
}
