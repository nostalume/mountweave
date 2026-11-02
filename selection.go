package core

import (
	"context"
	"errors"
)

// PathAccess describes a logical range for capture and optional conflict checks.
// Write is scheduling metadata, not an authorization grant.
type PathAccess struct {
	Path  string
	Write bool
}

type selectedPath struct {
	visible string
	at      location
	write   bool
}

// Selection records process-local bindings without borrowing their resources.
// It cannot be restored, grants no permission, and does not freeze file content.
type Selection struct {
	core  *Core
	paths []selectedPath
}

type selectionKey struct{}

// Capture selects existing real bindings, including paths not yet created.
// Authorization remains mandatory when the normal operation executes.
func (c *Core) Capture(ctx context.Context, accesses ...PathAccess) (*Selection, error) {
	if c == nil || len(accesses) == 0 {
		return nil, Fail(InvalidRequest, nil)
	}
	for _, access := range accesses {
		if err := ValidatePath(access.Path); err != nil {
			return nil, err
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s := &Selection{core: c, paths: make([]selectedPath, 0, len(accesses))}
	for _, access := range accesses {
		at, _, err := c.resolveLocked(access.Path, nil)
		if err != nil {
			return nil, err
		}
		if at == nil {
			return nil, Fail(UnsupportedOperation, nil)
		}
		s.paths = append(s.paths, selectedPath{access.Path, *at, access.Write})
	}
	return s, nil
}

// Context constrains ordinary calls to these bindings and their descendants.
// A retired/rebound selection fails at admission instead of resolving anew.
func (s *Selection) Context(ctx context.Context) context.Context {
	return context.WithValue(ctx, selectionKey{}, s)
}

// Conflicts compares the declared ranges, without accessing mutable mount state.
// Independent Core instances and undeclared physical aliases are not compared.
func (s *Selection) Conflicts(other *Selection) bool {
	if s == nil || other == nil || s.core != other.core {
		return false
	}
	for _, a := range s.paths {
		for _, b := range other.paths {
			if a.at.binding.mount == b.at.binding.mount && (a.write || b.write) &&
				(Within(a.at.path, b.at.path) || Within(b.at.path, a.at.path)) {
				return true
			}
		}
	}
	return false
}

func (c *Core) constrained(ctx context.Context, target string, expected *location) (*location, error) {
	s, ok := ctx.Value(selectionKey{}).(*Selection)
	if !ok {
		return expected, nil
	}
	if s == nil || s.core != c {
		return nil, Fail(InvalidRequest, errors.New("selection belongs to another core"))
	}
	var chosen *selectedPath
	for i := range s.paths {
		candidate := &s.paths[i]
		if Within(target, candidate.visible) && (chosen == nil || len(candidate.visible) > len(chosen.visible)) {
			chosen = candidate
		}
	}
	if chosen == nil {
		return nil, Fail(InvalidRequest, errors.New("path is outside the selected ranges"))
	}
	at := &location{binding: chosen.at.binding, path: joinPath(chosen.at.path, relative(target, chosen.visible))}
	if expected != nil && (expected.binding != at.binding || expected.path != at.path) {
		return nil, Fail(MountUnavailable, nil)
	}
	return at, nil
}

// BindingInfo is an observation, never an execution credential.
type BindingInfo struct {
	Mount *Mount
	Base  string
}

func (c *Core) Binding(prefix string) (BindingInfo, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	b := c.bindings[prefix]
	if c.closed || b == nil || !b.active || b.mount.retiring {
		return BindingInfo{}, false
	}
	return BindingInfo{Mount: b.mount, Base: b.base}, true
}

type MountStatus struct {
	Active, Retiring, Done bool
	Err                    error
}

// Status observes this exact generation, even after its prefix is replaced.
func (m *Mount) Status() MountStatus {
	c := m.core
	c.mu.Lock()
	defer c.mu.Unlock()
	s := MountStatus{Active: !c.closed && c.mounts[m.path] == m && !m.retiring,
		Retiring: m.retiring, Err: errors.Join(m.closeErr, m.cleanupErr)}
	select {
	case <-m.done:
		s.Done = true
	default:
	}
	return s
}
