package core

import (
	"context"
	"errors"
	"path"
	"sort"
	"strings"
)

// AddressAccess describes a logical range for capture and optional conflict checks.
// Write is scheduling metadata, not an authorization grant.
type AddressAccess struct {
	Address Address
	Write   bool
}

type mountRanges struct {
	read, write       []string
	keyRead, keyWrite []keyRange
}

type keyRange struct {
	value  string
	prefix bool
}

// Selection records process-local bindings without borrowing their resources.
// It cannot be restored, grants no permission, and does not freeze file content.
type Selection struct {
	core     *Core
	roots    map[Address]struct{}
	bindings map[string]*binding
	children map[string][]string
	ranges   map[*Mount]mountRanges
}

type selectionKey struct{}

// Capture selects real bindings or a virtual hierarchy and its existing members.
// New bindings never join this selection. Real paths need not exist yet.
// Authorization remains mandatory when the normal operation executes.
func (c *Core) Capture(ctx context.Context, accesses ...AddressAccess) (*Selection, error) {
	if c == nil || len(accesses) == 0 {
		return nil, Fail(InvalidRequest, nil)
	}
	for _, access := range accesses {
		if err := c.validateInput(ctx, access.Address); err != nil {
			return nil, err
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s := &Selection{core: c, roots: make(map[Address]struct{}), bindings: make(map[string]*binding),
		children: make(map[string][]string), ranges: make(map[*Mount]mountRanges)}
	bindings := c.bindings
	frozen := false
	if previous, ok := ctx.Value(selectionKey{}).(*Selection); ok && previous != nil {
		bindings = previous.bindings
		frozen = true
	}
	virtual := make(map[string]map[string]bool)
	add := func(at *location, write bool) {
		s.bindings[at.prefix] = at.binding
		r := s.ranges[at.mount]
		if at.mount.keys {
			keys, n := at.keyRanges()
			if write {
				r.keyWrite = append(r.keyWrite, keys[:n]...)
			} else {
				r.keyRead = append(r.keyRead, keys[:n]...)
			}
		} else if write {
			r.write = append(r.write, at.path)
		} else {
			r.read = append(r.read, at.path)
		}
		s.ranges[at.mount] = r
	}
	for _, access := range accesses {
		at, _, err := c.resolveRequestLocked(ctx, access.Address)
		if err != nil {
			return nil, err
		}
		s.roots[access.Address] = struct{}{}
		if at != nil {
			if !c.validAtLocked(at) {
				return nil, Fail(MountUnavailable, nil)
			}
			add(at, access.Write)
			continue
		}
		p := access.Address.Value()
		if virtual[p] == nil {
			virtual[p] = make(map[string]bool)
		}
		for prefix, b := range bindings {
			if !Within(prefix, p) || (!frozen && (!b.active || b.mount.retiring)) {
				continue
			}
			add(&location{binding: b, path: b.base}, access.Write)
			for child := prefix; child != p; child = path.Dir(child) {
				parent := path.Dir(child)
				if virtual[parent] == nil {
					virtual[parent] = make(map[string]bool)
				}
				virtual[parent][path.Base(child)] = true
			}
		}
	}
	for parent, names := range virtual {
		children := make([]string, 0, len(names))
		for name := range names {
			children = append(children, name)
		}
		sort.Strings(children)
		s.children[parent] = children
	}
	for m, r := range s.ranges {
		r.read, r.write = normalizeRanges(r.read), normalizeRanges(r.write)
		r.keyRead, r.keyWrite = normalizeKeys(r.keyRead), normalizeKeys(r.keyWrite)
		s.ranges[m] = r
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
	for m, a := range s.ranges {
		b := other.ranges[m]
		if rangesOverlap(a.write, b.read) || rangesOverlap(a.write, b.write) || rangesOverlap(a.read, b.write) ||
			keysOverlap(a.keyWrite, b.keyRead) || keysOverlap(a.keyWrite, b.keyWrite) || keysOverlap(a.keyRead, b.keyWrite) {
			return true
		}
	}
	return false
}

// validateInput reuses only an exact, already checked operand. A new descendant
// string is untrusted even when it is inside a selected range.
func (c *Core) validateInput(ctx context.Context, target Address) error {
	if s, ok := ctx.Value(selectionKey{}).(*Selection); ok && s != nil && s.core == c {
		if _, checked := s.roots[target]; checked {
			return nil
		}
	}
	return target.Validate()
}

func (c *Core) resolveRequestLocked(ctx context.Context, targetAddress Address) (*location, []string, error) {
	target := targetAddress.Value()

	if c.closed {
		return nil, nil, Fail(MountUnavailable, nil)
	}
	s, ok := ctx.Value(selectionKey{}).(*Selection)
	if ok && (s == nil || s.core != c) {
		return nil, nil, Fail(InvalidRequest, errors.New("selection belongs to another core"))
	}
	if targetAddress.namespace == KeyNamespace {
		bindings := c.bindings
		if ok {
			_, inside := s.roots[targetAddress]
			if !inside {
				for root := range s.roots {
					if root.namespace == KeyNamespace && root.space == targetAddress.space && root.prefix && strings.HasPrefix(target, root.value) {
						inside = true
						break
					}
				}
			}
			if !inside {
				return nil, nil, Fail(InvalidRequest, errors.New("address is outside the selected ranges"))
			}
			bindings = s.bindings
		}
		b := bindings[targetAddress.space]
		if b == nil {
			return nil, nil, Fail(NotFound, nil)
		}
		if !b.active || b.mount.retiring {
			return nil, nil, Fail(MountUnavailable, nil)
		}
		if !b.keySpace {
			return nil, nil, Fail(UnsupportedOperation, nil)
		}
		return &location{binding: b, path: b.keyBase() + target, namespace: KeyNamespace, prefixRange: targetAddress.prefix}, nil, nil
	}
	if !ok {
		return c.resolveLocked(target)
	}
	inside := false
	for p := target; ; p = path.Dir(p) {
		if _, exists := s.roots[Path(p)]; exists {
			inside = true
			break
		}
		if p == "/" {
			break
		}
	}
	if !inside {
		return nil, nil, Fail(InvalidRequest, errors.New("path is outside the selected ranges"))
	}
	for p := target; ; p = path.Dir(p) {
		if b := s.bindings[p]; b != nil {
			return &location{binding: b, path: joinPath(b.base, relative(target, p))}, nil, nil
		}
		if p == "/" {
			break
		}
	}
	if names, exists := s.children[target]; exists {
		return nil, names, nil
	}
	return nil, nil, Fail(NotFound, nil)
}

func (at *location) keyRanges() (r [2]keyRange, n int) {
	if at.namespace == KeyNamespace {
		r[0] = keyRange{at.path, at.prefixRange}
		return r, 1
	}
	if at.mount.projection == nil {
		r[0] = keyRange{"", true}
		return r, 1
	}
	e := at.mount.projection.KeyExtent(at.native())
	if !e.HasExact && !e.HasPrefix {
		r[0] = keyRange{"", true}
		return r, 1
	}
	if e.HasExact {
		r[n] = keyRange{e.Exact, false}
		n++
	}
	if e.HasPrefix {
		r[n] = keyRange{e.Prefix, true}
		n++
	}
	return r, n
}

func keyContains(a, b keyRange) bool {
	return a.prefix && strings.HasPrefix(b.value, a.value) || a == b
}

func normalizeKeys(r []keyRange) []keyRange {
	sort.Slice(r, func(i, j int) bool {
		return r[i].value < r[j].value || r[i].value == r[j].value && r[i].prefix && !r[j].prefix
	})
	n := 0
	for _, p := range r {
		if n == 0 || !keyContains(r[n-1], p) {
			r[n] = p
			n++
		}
	}
	return r[:n]
}

func keysOverlap(a, b []keyRange) bool {
	for len(a) > 0 && len(b) > 0 {
		if keyContains(a[0], b[0]) || keyContains(b[0], a[0]) {
			return true
		}
		if a[0].value < b[0].value {
			a = a[1:]
		} else {
			b = b[1:]
		}
	}
	return false
}

func locationsOverlap(a, b *location) bool {
	if a.mount != b.mount {
		return false
	}
	if !a.mount.keys {
		return Within(a.path, b.path) || Within(b.path, a.path)
	}
	xs, nx := a.keyRanges()
	ys, ny := b.keyRanges()
	for _, x := range xs[:nx] {
		for _, y := range ys[:ny] {
			if keyContains(x, y) || keyContains(y, x) {
				return true
			}
		}
	}
	return false
}

func sameName(a, b *location) bool {
	if a.mount != b.mount {
		return false
	}
	if a.native() == b.native() {
		return true
	}
	if a.mount.projection == nil {
		return false
	}
	x, y := a.mount.projection.KeyExtent(a.native()), a.mount.projection.KeyExtent(b.native())
	return x.HasExact && y.HasExact && x.Exact == y.Exact
}

// Slash-terminated keys keep a directory and its descendants contiguous even
// beside names such as "a-". These ranges are scheduling metadata only.
func rangeKey(p string) string {
	if p == "/" {
		return p
	}
	return p + "/"
}

func normalizeRanges(r []string) []string {
	sort.Slice(r, func(i, j int) bool { return rangeKey(r[i]) < rangeKey(r[j]) })
	n := 0
	for _, p := range r {
		if n == 0 || !Within(p, r[n-1]) {
			r[n] = p
			n++
		}
	}
	return r[:n]
}

func rangesOverlap(a, b []string) bool {
	for len(a) != 0 && len(b) != 0 {
		if Within(a[0], b[0]) || Within(b[0], a[0]) {
			return true
		}
		if rangeKey(a[0]) < rangeKey(b[0]) {
			a = a[1:]
		} else {
			b = b[1:]
		}
	}
	return false
}

func (c *Core) selectOperation(ctx context.Context, accesses ...AddressAccess) (context.Context, error) {
	if _, exists := ctx.Value(selectionKey{}).(*Selection); exists {
		return ctx, nil
	}
	s, err := c.Capture(ctx, accesses...)
	if err != nil {
		return ctx, err
	}
	return s.Context(ctx), nil
}
