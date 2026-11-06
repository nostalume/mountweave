package core

import (
	"context"
	"errors"
	"path"
)

// RemovalScope selects permitted effects, independently of execution strategy.
type RemovalScope string

const (
	Discovered RemovalScope = "discovered"
	Subtree    RemovalScope = "subtree"
)

// DirectoryRemoveOptions defaults to Discovered when omitted or empty.
// ContentsOnly preserves the requested root and namespace binding anchors while
// removing selected mutable contents. It also accepts mount and virtual roots.
type DirectoryRemoveOptions struct {
	Scope        RemovalScope
	ContentsOnly bool
	When         Condition
}

// SubtreeRemover optionally removes a directory and its native descendants.
// It must not follow redirects outside that subtree. Limits bound client discovery
// and planning, not the number of descendants a native recursive request deletes.
// They apply to each native invocation. A ContentsOnly operation additionally has
// one Core-wide visible-name plan budget; it is not a global native I/O quota.
// Applied requires confirmed completion, not asynchronous task acceptance. On
// cancellation, stop dispatch and await active client I/O; remote effects may
// continue and must remain Unknown when completion cannot be established.
type SubtreeRemover interface {
	RemoveSubtree(context.Context, NativeAddress, TreeOptions) (Change, error)
}

// RemoveTree discovers selected names before effects. ContentsOnly leaves the
// operation root and all virtual/binding anchors in place. Subtree permits native
// recursive effects beneath removable names and requires RemoveTreeAction.
// Results describe attempted mutations, never the preserved structure. Neither
// policy promises a snapshot, rollback or protection against later recreation.
// A file leaf reuses Remove and accepts zero traversal budgets. When is a root
// file predicate, never broadcast to descendants; nonempty directory predicates
// without a native commit proof are refused before child effects.
func (c *Core) RemoveTree(ctx context.Context, p Principal, targetAddress Address, options TreeOptions, policies ...DirectoryRemoveOptions) (result TreeResult, err error) {
	ctx, turn, err := enterOrder(ctx, nil)
	if err != nil {
		return TreeResult{Change: Change{Effect: Unchanged}}, err
	}
	defer func() { turn.finish(result.Change.Effect == Unknown) }()
	return c.removeTree(ctx, p, targetAddress, options, nil, policies...)
}

// A root observed by the same captured composite is only a routing fact. Native
// steps still enforce types/conditions; it is not cached identity or authority.
func (c *Core) removeTree(ctx context.Context, p Principal, targetAddress Address, options TreeOptions, observed *Entry, policies ...DirectoryRemoveOptions) (result TreeResult, err error) {
	result.Change.Effect = Unchanged
	complete, planned := false, -1
	defer func() {
		if planned >= 0 {
			complete = fulfilled(result.Items, "", planned)
		}
		result.Change.Effect = treeEffect(result.Items, complete)
		projectFailures(result.Items)
	}()
	if targetAddress.Namespace() != TreeNamespace {
		return result, Fail(UnsupportedOperation, nil)
	}
	target := targetAddress.Value()

	if len(policies) > 1 {
		return result, Fail(InvalidRequest, nil)
	}
	policy := DirectoryRemoveOptions{Scope: Discovered}
	if len(policies) == 1 {
		policy = policies[0]
		if policy.Scope == "" {
			policy.Scope = Discovered
		}
	}
	if policy.Scope != Discovered && policy.Scope != Subtree {
		return result, Fail(InvalidRequest, nil)
	}
	if err := policy.When.Validate(); err != nil {
		return result, err
	}
	limit, err := concurrency([]ExecutionOptions{options.ExecutionOptions})
	if err != nil {
		return result, err
	}
	if options.MaxEntries < 0 || options.MaxPlanBytes < 0 {
		return result, Fail(InvalidRequest, nil)
	}
	ctx, err = c.selectOperation(ctx, AddressAccess{Address: Path(target), Write: true})
	if err != nil {
		return result, err
	}
	action := RemoveAction
	if policy.Scope == Subtree {
		action = RemoveTreeAction
	}
	var root *use
	var entry Entry
	if observed == nil {
		root, entry, err = c.inspectTree(ctx, p, targetAddress, RemoveAction, action, "")
	} else {
		entry = *observed
		if entry.Address != targetAddress {
			return result, Fail(InvalidRequest, nil)
		}
		grant := RemoveAction
		if entry.Kind == Directory {
			grant = action
		}
		root, _, err = c.admit(ctx, p, grant, targetAddress)
	}
	if !policy.ContentsOnly {
		root, err = changeUse(root, err)
	}
	if err != nil {
		return result, err
	}
	if entry.Kind == File {
		if policy.ContentsOnly {
			root.release()
			return result, Fail(NotDirectory, nil)
		}
		change, removeErr := c.removeUse(root, RemoveOptions{When: policy.When}, nil)
		result.Items = []TreeItemResult{{RelativePath: "/", Side: SourceSide, Phase: RemovePhase, Change: change, Err: removeErr}}
		result.Change.Before = TargetPresent
		complete = change.Effect == Applied
		return result, removeErr
	}
	if options.MaxEntries <= 0 || options.MaxPlanBytes <= 0 {
		if root != nil {
			root.release()
		}
		return result, Fail(InvalidRequest, nil)
	}
	if !policy.When.Empty() {
		if root != nil {
			root.release()
		}
		if policy.When.IsAbsent() {
			return result, policy.When.Check(Evidence{}, true)
		}
		return result, Fail(UnsupportedOperation, nil)
	}
	var rootAt *location
	if root != nil {
		defer root.release()
		rootAt = root.location
		var end func()
		ctx, end = requestScope(ctx, root.ctx)
		defer end()
	}
	defer func() { err = observedCause(ctx, err) }()
	// Preserve selected binding targets and their necessary ancestors even when
	// another visible branch reaches them through the same mounted source.
	anchors := make(map[*Mount]map[string]bool)
	if policy.ContentsOnly {
		for _, b := range ctx.Value(selectionKey{}).(*Selection).bindings {
			if !Within(b.prefix, target) && !Within(target, b.prefix) {
				continue
			}
			if anchors[b.mount] == nil {
				anchors[b.mount] = make(map[string]bool)
			}
			for p := b.base; ; p = path.Dir(p) {
				anchors[b.mount][p] = true
				if p == "/" {
					break
				}
			}
		}
	}
	preserved := func(at *location) bool { return anchors[at.mount][at.path] }
	native := func(at *location) bool {
		if at == nil || policy.Scope != Subtree {
			return false
		}
		_, ok := at.mount.driver.(SubtreeRemover)
		return ok
	}
	removeNative := func(u *use, kind Kind) (Change, error) {
		if err := u.ctx.Err(); err != nil {
			return Change{Effect: Unchanged}, err
		}
		var result Change
		var err error
		if kind == Directory {
			result, err = u.mount.driver.(SubtreeRemover).RemoveSubtree(u.ctx, u.native(), options)
		} else {
			result, err = u.mount.driver.(Remover).Remove(u.ctx, u.native())
		}
		return c.finishChange(u, result, err)
	}
	if !policy.ContentsOnly && native(rootAt) {
		change, nativeErr := removeNative(root, Directory)
		result.Items = []TreeItemResult{{RelativePath: "/", Side: SourceSide, Phase: RemovePhase, Change: change, Err: nativeErr}}
		result.Change.Before = TargetPresent
		complete = change.Effect == Applied
		return result, nativeErr
	}
	// A whole-subtree grant covers client traversal and mutations. Each native
	// step renews the visible request and its real member/primary grant; it never
	// turns a virtual List grant into authority over another mounted source.
	begin := func(at *location) (*use, error) {
		if policy.Scope == Discovered {
			return c.beginChangeAt(ctx, p, RemoveAction, at)
		}
		grant := rootAt
		if grant == nil {
			if err := c.authorizeChecked(ctx, p, RemoveTreeAction, Path(target)); err != nil {
				return nil, err
			}
			grant = &location{binding: at.binding, path: at.base}
		}
		if err := c.authorizeChecked(ctx, p, RemoveTreeAction, Path(grant.visible())); err != nil {
			return nil, err
		}
		if err := c.authorizeBinding(ctx, p, RemoveTreeAction, grant); err != nil {
			return nil, err
		}
		c.mu.Lock()
		defer c.mu.Unlock()
		return c.borrowAtLocked(ctx, at)
	}
	openRemoval := func(at *location) (*directory, error) { return openRemovalDirectory(begin(at)) }
	if !policy.ContentsOnly {
		parent, openErr := openRemoval(rootAt)
		if openErr != nil {
			return result, openErr
		}
		if err = parent.close(); err != nil {
			return result, err
		}
	}
	discovery := treeDiscovery{virtualAction: ListAction}
	if policy.Scope == Subtree {
		discovery.virtualAction = RemoveTreeAction
	}
	discovery.open = func(at *location) (*directory, error) {
		var dir *directory
		var err error
		if policy.Scope == Discovered {
			dir, err = c.openDirectory(ctx, p, at)
		} else {
			var u *use
			u, err = begin(at)
			if err == nil {
				dir, err = openDirectoryAt(u, false)
			}
		}
		if err != nil {
			return nil, err
		}
		if !native(at) {
			if _, ok := dir.native.(ChildRemover); !ok {
				return nil, errors.Join(Fail(UnsupportedOperation, nil), dir.close())
			}
		}
		return dir, nil
	}
	discovery.visit = func(at *location, kind Kind) error {
		if preserved(at) {
			return nil
		}
		if policy.Scope == Discovered {
			return c.preflightAt(ctx, p, RemoveAction, at)
		}
		if native(at) && kind == File {
			if _, ok := at.mount.driver.(Remover); !ok {
				return Fail(UnsupportedOperation, nil)
			}
		}
		return nil
	}
	discovery.recurse = func(at *location) bool { return !native(at) || preserved(at) }
	directories, err := c.discoverTree(ctx, p, target, rootAt, options, discovery)
	if err != nil {
		return result, err
	}
	if policy.ContentsOnly {
		for i := range directories {
			if directories[i].at == nil {
				continue
			}
			mutable := directories[i].children[:0]
			for _, entry := range directories[i].children {
				if !preserved(directories[i].at.descend("/" + entry.name)) {
					mutable = append(mutable, entry)
				}
			}
			directories[i].children = mutable
		}
	}
	planned = 0
	if !policy.ContentsOnly {
		planned = 1
		result.Change.Before = TargetPresent
	}
	for _, dir := range directories {
		planned += len(dir.children)
	}
	child := func(dir treeDirectory, entry treeEntry) (*location, string) {
		// An empty name is used only for the final, explicitly requested root.
		if entry.name == "" {
			return dir.at, "/"
		}
		return dir.at.descend("/" + entry.name), joinPath(dir.path, "/"+entry.name)
	}
	removeGroups := func(groups []treeDirectory) error {
		var runErr error
		result.Items, runErr = executeDirectories(ctx, len(groups), limit, result.Items,
			func(i int) (*directory, int, error) {
				dir := groups[i]
				if dir.at == nil || len(dir.children) == 0 {
					return nil, 0, nil
				}
				if native(dir.at) {
					return nil, len(dir.children), nil
				}
				at, _ := child(dir, dir.children[0])
				scope, err := openRemoval(at)
				return scope, len(dir.children), err
			},
			func(i int, scope *directory, n int) work[TreeItemResult] {
				entry := groups[i].children[n]
				at, rel := child(groups[i], entry)
				return work[TreeItemResult]{accesses: []access{{at: at, write: true}}, run: func() (TreeItemResult, error) {
					row := TreeItemResult{RelativePath: rel, Side: SourceSide, Phase: RemovePhase, Change: Change{Effect: Unchanged}}
					u, err := begin(at)
					if err != nil {
						row.Err = err
					} else {
						defer u.release()
						if scope == nil {
							row.Change, row.Err = removeNative(u, entry.kind)
						} else {
							target := RemoveNonDirectory
							if entry.kind == Directory {
								target = RemoveEmptyDirectory
							}
							row.Change, row.Err = scope.removeChild(u, RemoveOptions{Target: target})
						}
					}
					return row, row.Err
				}}
			})
		directoryFailures(runErr, func(i int, cause error) {
			rel := groups[i].path
			if rel == "" {
				rel = "/"
			}
			result.Items = append(result.Items, TreeItemResult{RelativePath: rel, Side: SourceSide, Phase: ReadPhase, Change: Change{Effect: Unchanged}, Err: cause})
		})
		return runErr
	}
	// Discovery is breadth-first. Reverse depth bands, retaining sibling order.
	for end := len(directories); end > 0; {
		start := end - 1
		for start > 0 && directories[start-1].depth == directories[end-1].depth {
			start--
		}
		if err = removeGroups(directories[start:end]); err != nil {
			return result, err
		}
		end = start
	}
	if !policy.ContentsOnly {
		err = removeGroups([]treeDirectory{{at: rootAt, children: []treeEntry{{kind: Directory}}}})
	}
	complete = fulfilled(result.Items, "", planned)
	return result, err
}

// The caller has admitted a child mutation, including opening its parent.
func openRemovalDirectory(u *use, err error) (*directory, error) {
	if err != nil {
		return nil, err
	}
	scope, err := openDirectoryAt(u, true)
	if err != nil {
		return nil, err
	}
	if _, ok := scope.native.(ChildRemover); !ok {
		return nil, errors.Join(Fail(UnsupportedOperation, nil), scope.close())
	}
	return scope, nil
}

// removeTarget composes the existing bounded removal operation in the same
// captured selection. Missing names are not broadened into recursive effects.
func (c *Core) removeTarget(ctx context.Context, p Principal, target Address, options TreeOptions, when Condition, observed *Entry) (result TreeResult, err error) {
	defer func() {
		for i := range result.Items {
			item := &result.Items[i]
			item.Side = TargetSide
			if item.Err != nil {
				item.ErrorSide = TargetSide
				item.Err = &OperationError{Side: TargetSide, Phase: item.Phase, Cause: item.Err}
			}
		}
		if err != nil {
			err = &OperationError{Side: TargetSide, Phase: RemovePhase, Cause: err}
		}
	}()
	if observed == nil {
		// Missing targets require creation authority, not a needless delete grant.
		u, err := c.beginChange(ctx, p, CreateAction, target)
		if err != nil {
			return TreeResult{Change: Change{Effect: Unchanged}}, err
		}
		entry, err := c.observe(u, "")
		u.release()
		if err != nil {
			r := TreeResult{Change: Change{Effect: Unchanged}}
			if when.Empty() && CodeOf(err) == NotFound {
				r.Change.Before = TargetMissing
				return r, nil
			}
			return r, err
		}
		observed = &entry
	}
	r, err := c.removeTree(ctx, p, target, options, observed, DirectoryRemoveOptions{Scope: Subtree, When: when})
	if when.Empty() && r.Change.Effect == Unchanged && CodeOf(err) == NotFound {
		r.Change.Before = TargetMissing
		return r, nil
	}
	return r, err
}
