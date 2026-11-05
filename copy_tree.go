package core

import (
	"context"
	"errors"
)

// DirectoryCopyOptions selects directory and file policies independently.
// Existing controls the root/namespace policy. WriteOptions governs each file.
type DirectoryCopyOptions struct {
	WriteOptions
	Existing TargetPolicy
	Depth    TreeDepth
	Source   ReadSelection
}

type TargetPolicy string

const (
	CreateNew TargetPolicy = "create_new"
	Merge     TargetPolicy = "merge"
	Replace   TargetPolicy = "replace"
)

type TreeDepth string

const (
	Recursive TreeDepth = "recursive"
	Self      TreeDepth = "self"
)

// CopyTree selects a file leaf, directory itself, or finite recursive tree.
// Existing defaults to CreateNew; Merge retains extra members, whereas Replace
// discovers/adopts the source before target removal. Files use WriteOptions.
// There is no source deletion, snapshot, automatic replay or rollback.
// A virtual source traverses its captured bindings and aliases by visible names.
func (c *Core) CopyTree(ctx context.Context, p Principal, sourceAddress, targetAddress Address, options TreeOptions, policies ...DirectoryCopyOptions) (result TreeResult, err error) {
	result.Change.Effect = Unchanged
	ctx, turn, err := enterOrder(ctx, nil)
	if err != nil {
		return result, err
	}
	defer func() { turn.finish(result.Change.Effect == Unknown) }()
	complete := false
	planned := -1
	defer func() {
		if planned >= 0 {
			complete = fulfilled(result.Items, WritePhase, planned)
		}
		result.Change.Effect = treeEffect(result.Items, complete)
		projectFailures(result.Items)
	}()
	if sourceAddress.Namespace() != TreeNamespace || targetAddress.Namespace() != TreeNamespace {
		return result, Fail(UnsupportedOperation, nil)
	}
	if len(policies) > 1 {
		return result, Fail(InvalidRequest, nil)
	}
	var policy DirectoryCopyOptions
	if len(policies) == 1 {
		policy = policies[0]
	}
	if _, err := writeOptions([]WriteOptions{policy.WriteOptions}); err != nil {
		return result, err
	}
	if policy.Continuation != nil {
		return result, Fail(UnsupportedOperation, nil)
	}
	if policy.Existing == "" {
		policy.Existing = CreateNew
	}
	if policy.Existing != CreateNew && policy.Existing != Merge && policy.Existing != Replace {
		return result, Fail(InvalidRequest, nil)
	}
	if policy.Depth == "" {
		policy.Depth = Recursive
	}
	if err := policy.Source.When.Validate(); err != nil {
		return result, err
	}
	limit, err := concurrency([]ExecutionOptions{options.ExecutionOptions})
	if err != nil {
		return result, err
	}
	if options.MaxEntries < 0 || options.MaxPlanBytes < 0 {
		return result, Fail(InvalidRequest, nil)
	}
	source, target := sourceAddress.Value(), targetAddress.Value()
	ctx, err = c.selectOperation(ctx, AddressAccess{Address: sourceAddress}, AddressAccess{Address: targetAddress, Write: true})
	if err != nil {
		return result, err
	}
	from, sourceEntry, err := c.inspectTree(ctx, p, sourceAddress, ReadAction, ListAction, policy.Source.Version)
	if err != nil {
		return result, err
	}
	defer func() {
		if from != nil {
			from.release()
		}
	}()
	targetAction := CreateAction
	if sourceEntry.Kind == File {
		if policy.Existing == Replace && !policy.Policy.When().IsAbsent() {
			targetAction = WriteAction
		} else if policy.Existing == Merge {
			targetAction = policy.WriteOptions.action()
		}
	}
	to, _, err := c.admit(ctx, p, targetAction, targetAddress)
	if err != nil {
		return result, err
	}
	to, err = changeUse(to, nil)
	if err != nil {
		return result, err
	}
	defer to.release()
	selected := ctx.Value(selectionKey{}).(*Selection)
	for _, b := range selected.bindings {
		var at *location
		if Within(source, b.prefix) {
			at = &location{binding: b, path: joinPath(b.base, relative(source, b.prefix))}
		}
		if Within(b.prefix, source) {
			at = &location{binding: b, path: b.base}
		}
		if at != nil && at.mount == to.mount && (Within(to.path, at.path) || Within(at.path, to.path)) {
			return result, Fail(InvalidRequest, nil)
		}
	}
	ctx, endTarget := requestScope(ctx, to.ctx)
	defer endTarget()
	if sourceEntry.Kind == File {
		defer func() { err = observedCause(ctx, err) }()
		result, err = c.copyTreeFile(ctx, p, from, to, options, policy)
		complete = result.Change.Effect == Applied
		return result, err
	}
	if options.MaxEntries <= 0 || options.MaxPlanBytes <= 0 {
		return result, Fail(InvalidRequest, nil)
	}
	if policy.Depth != Recursive && policy.Depth != Self {
		return result, Fail(InvalidRequest, nil)
	}
	if policy.Source.Version != "" || (!policy.Source.When.Empty() && !policy.Source.When.IsAbsent() && !policy.Source.When.IsPresent()) {
		return result, Fail(UnsupportedOperation, nil)
	}
	if when := policy.Policy.When(); !when.Empty() && !when.IsAbsent() {
		return result, Fail(UnsupportedOperation, errors.New("a file validator cannot guard every tree member"))
	}
	if _, ok := to.mount.driver.(DirectoryMaker); !ok {
		return result, Fail(UnsupportedOperation, nil)
	}
	if policy.Existing == Merge {
		if _, ok := to.mount.driver.(DirectoryReader); !ok {
			return result, Fail(UnsupportedOperation, nil)
		}
	}
	var sourceAt *location
	if from != nil {
		sourceAt = from.location
		var endSource func()
		ctx, endSource = requestScope(ctx, from.ctx)
		defer endSource()
	}
	defer func() { err = observedCause(ctx, err) }()
	open := func(at *location) (*directory, error) {
		scope, openErr := c.openDirectory(ctx, p, at)
		if openErr == nil && at == sourceAt {
			if conditionErr := policy.Source.When.Check(Evidence{}, true); conditionErr != nil {
				return nil, errors.Join(conditionErr, scope.close())
			}
		}
		return scope, openErr
	}
	if sourceAt == nil {
		if err := policy.Source.When.Check(Evidence{}, true); err != nil {
			return result, err
		}
	}
	var directories []treeDirectory
	if policy.Depth == Self {
		if sourceAt != nil {
			scope, openErr := open(sourceAt)
			if openErr != nil {
				return result, openErr
			}
			if err = scope.close(); err != nil {
				return result, err
			}
		}
		directories = []treeDirectory{{path: "/", at: sourceAt}}
	} else {
		directories, err = c.discoverTree(ctx, p, source, sourceAt, options, treeDiscovery{open: open, virtualAction: ListAction, target: target})
		if err != nil {
			return result, err
		}
	}
	planned = len(directories)
	for _, dir := range directories {
		for _, child := range dir.children {
			if child.kind == File {
				planned++
			}
		}
	}
	if planned > len(directories) {
		if policy.Policy.Complete() {
			if _, ok := to.mount.driver.(FilePublisher); !ok {
				return result, Fail(UnsupportedOperation, nil)
			}
		} else if _, ok := to.mount.driver.(FileCreator); !ok {
			return result, Fail(UnsupportedOperation, nil)
		}
	}
	if policy.Existing == Replace {
		// Complete discovery and static operation admission precede destructive
		// replacement. Each actual step still renews authority and borrows uses.
		for _, dir := range directories {
			if err := c.preflightAt(ctx, p, CreateAction, to.location.descend(dir.path)); err != nil {
				return result, err
			}
			for _, child := range dir.children {
				if child.kind != File {
					continue
				}
				if err := c.preflightAt(ctx, p, ReadAction, dir.at.descend("/"+child.name)); err != nil {
					return result, err
				}
				if err := c.preflightAt(ctx, p, policy.WriteOptions.action(), to.location.descend(joinPath(dir.path, "/"+child.name))); err != nil {
					return result, err
				}
			}
		}
		// Removal already observes and routes the old target. Reuse that receipt
		// instead of performing another existence/type query before it.
		removed, removeErr := c.removeTarget(ctx, p, targetAddress, options, Condition{}, nil)
		result.Change.Before = removed.Change.Before
		result.Items = append(result.Items, removed.Items...)
		if removeErr != nil {
			return result, removeErr
		}
	}
	// Source enumerations are closed before namespace effects.
	result.Items, err = execute(ctx, len(directories), 1, result.Items, func(i int) work[TreeItemResult] {
		rel := directories[i].path
		at := to.location.descend(rel)
		return work[TreeItemResult]{accesses: []access{{at: at, write: true}}, run: func() (TreeItemResult, error) {
			change, err := c.copyDirectory(ctx, p, at, policy.Existing == Merge)
			if rel == "/" && result.Change.Before == TargetUnknown {
				if change.Effect == Applied {
					result.Change.Before = TargetMissing
				} else if directoryConflict(err) || err == nil {
					result.Change.Before = TargetPresent
				}
			}
			return TreeItemResult{RelativePath: rel, Side: TargetSide, Phase: CreatePhase, Change: change, Err: err}, err
		}}
	})
	if err != nil {
		return result, err
	}
	for i := range directories {
		files := directories[i].children[:0]
		for _, child := range directories[i].children {
			if child.kind == File {
				files = append(files, child)
			}
		}
		directories[i].children = files
	}
	result.Items, err = executeDirectories(ctx, len(directories), limit, result.Items,
		func(i int) (*directory, int, error) {
			dir := directories[i]
			if len(dir.children) == 0 {
				return nil, 0, nil
			}
			scope, err := c.readDirectory(ctx, p, dir.at, options, nil)
			return scope, len(dir.children), err
		},
		func(i int, scope *directory, item int) work[TreeItemResult] {
			dir := directories[i]
			name := dir.children[item].name
			rel := joinPath(dir.path, "/"+name)
			sourceAt, targetAt := dir.at.descend("/"+name), to.location.descend(rel)
			return work[TreeItemResult]{accesses: []access{{at: sourceAt}, {at: targetAt, write: true}}, run: func() (TreeItemResult, error) {
				row := TreeItemResult{RelativePath: rel, Side: TargetSide, Phase: WritePhase, Change: Change{Effect: Unchanged}}
				opened, openErr := scope.openFile(ctx, p, sourceAt)
				if openErr != nil {
					row.Side, row.Phase, row.Err = SourceSide, ReadPhase, openErr
					return row, openErr
				}
				row.Change, row.Err = c.copyContent(ctx, p, targetAt, opened, policy.WriteOptions)
				return row, row.Err
			}}
		})
	complete = fulfilled(result.Items, WritePhase, planned)
	directoryFailures(err, func(i int, cause error) {
		result.Items = append(result.Items, TreeItemResult{RelativePath: directories[i].path, Side: SourceSide, Phase: ReadPhase, Change: Change{Effect: Unchanged}, Err: cause})
	})
	return result, err
}

// copyDirectory accepts an existing directory only after an unchanged conflict
// and a no-follow open. The scope confirms this name now, not the whole future
// target tree. Confirmation uses CreateAction without consuming its iterator.
func (c *Core) copyDirectory(ctx context.Context, p Principal, at *location, merge bool) (Change, error) {
	change, err := c.mkdirAt(ctx, p, at)
	if !merge || change.Effect != Unchanged || !directoryConflict(err) {
		return change, err
	}
	u, err := c.beginChangeAt(ctx, p, CreateAction, at)
	if err != nil {
		return change, err
	}
	scope, err := openDirectoryAt(u, false)
	if err != nil {
		return change, err
	}
	return change, observedCause(ctx, scope.close())
}

// directoryConflict follows context wrappers but never hides another joined
// failure. A domain Error owns its native cause; separate failures stay separate.
func directoryConflict(err error) bool {
	for err != nil {
		if public, ok := err.(*Error); ok {
			return public.Code == AlreadyExists
		}
		if joined, ok := err.(interface{ Unwrap() []error }); ok {
			causes := joined.Unwrap()
			if len(causes) != 1 {
				return false
			}
			err = causes[0]
		} else {
			err = errors.Unwrap(err)
		}
	}
	return false
}

func (c *Core) copyTreeFile(ctx context.Context, p Principal, from, to *use, options TreeOptions, policy DirectoryCopyOptions) (result TreeResult, err error) {
	result.Change.Effect = Unchanged
	opened, err := c.openAt(ctx, p, from.location, OpenOptions{Selection: policy.Source, NoFollow: true})
	if err != nil {
		result.Items = []TreeItemResult{{RelativePath: "/", Side: SourceSide, Phase: ReadPhase, Change: Change{Effect: Unchanged}, Err: err}}
		return result, err
	}
	defer func() {
		if closeErr := opened.Body.Close(); closeErr != nil {
			failure := &OperationError{Side: SourceSide, Phase: ReadPhase, Cause: closeErr}
			result.Items = append(result.Items, TreeItemResult{RelativePath: "/", Side: SourceSide, Phase: ReadPhase, Change: Change{Effect: Unchanged}, Err: failure})
			err = errors.Join(err, failure)
		}
	}()
	ctx, end := requestScope(ctx, from.ctx)
	defer end()
	defer func() { err = observedCause(ctx, err) }()
	write := policy.WriteOptions
	if policy.Existing == CreateNew {
		if !write.Policy.When().Empty() && !write.Policy.When().IsAbsent() {
			return result, Fail(InvalidRequest, nil)
		}
		if write.Policy.Complete() {
			write.Policy = Publish(IfAbsent())
		}
	} else if policy.Existing == Replace {
		write.Policy = Publish(write.Policy.When())
		// The kind probe already supplies the requested response observation;
		// it is never substituted for the actual publication predicate.
		write.ObserveTarget = false
		if _, ok := to.mount.driver.(FilePublisher); !ok {
			return result, Fail(UnsupportedOperation, nil)
		}
		if !write.Policy.When().IsAbsent() {
			entry, inspectErr := c.observe(to, "")
			if inspectErr != nil && CodeOf(inspectErr) != NotFound {
				return result, inspectErr
			}
			result.Change.Before = TargetMissing
			if inspectErr == nil {
				result.Change.Before = TargetPresent
			}
			if inspectErr == nil && entry.Kind == Directory {
				if !write.Policy.When().Empty() {
					return result, Fail(UnsupportedOperation, nil)
				}
				removed, removeErr := c.removeTarget(ctx, p, to.address(), options, Condition{}, &entry)
				result.Items = append(result.Items, removed.Items...)
				result.Change.Before = TargetPresent
				if removeErr != nil {
					result.Change.Effect = treeEffect(result.Items, false)
					return result, removeErr
				}
				write.Policy = Publish(IfAbsent())
			}
		}
	}
	action := write.action()
	if policy.Existing == Replace && !policy.Policy.When().IsAbsent() {
		action = WriteAction
	}
	u, admissionErr := c.beginChangeAt(ctx, p, action, to.location)
	change, err := c.writeUse(u, opened.Body, opened.Length, write, admissionErr)
	err = copyError(opened.Body, err)
	if result.Change.Before == TargetUnknown {
		result.Change.Before = change.Before
	}
	if policy.Existing == CreateNew && change.Effect == Applied {
		result.Change.Before = TargetMissing
	}
	result.Change.Evidence = change.Evidence
	result.Items = append(result.Items, TreeItemResult{RelativePath: "/", Side: TargetSide, Phase: WritePhase, Change: change, Err: err})
	result.Change.Effect = treeEffect(result.Items, change.Effect == Applied)
	return result, err
}
