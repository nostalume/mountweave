package core

import (
	"context"
	"errors"
	"io"
	"path"
)

// DirectoryIdentity is optional on a DirectoryScope. Its validator identifies
// the directory independently of its contents. Remove must accept it for
// exact, atomic empty-directory removal.
type DirectoryIdentity interface {
	DirectoryValidator() Validator
}

// MoveDirectoryWriter creates directories and publishes files exclusively. A
// nonzero validator must match the current target parent at the native commit.
// MkdirForMove returns the new directory identity in Change.Evidence.
type MoveDirectoryWriter interface {
	MkdirForMove(context.Context, NativeAddress, Validator) (Change, error)
	PublishForMove(context.Context, NativeAddress, Validator, io.Reader, *int64) (Change, error)
}

type movedFile struct {
	at    *location
	guard Validator
	row   TreeItemResult
}

func (c *Core) moveTree(ctx context.Context, p Principal, from, to *location, source MoveSource, options MoveOptions) (result MoveResult, err error) {
	result = MoveResult{Source: Change{Effect: Unchanged}, Target: Change{Effect: Unchanged}}
	target, ok := to.mount.driver.(MoveDirectoryWriter)
	if !ok || from.namespace != TreeNamespace || to.namespace != TreeNamespace {
		return result, Fail(UnsupportedOperation, nil)
	}
	tree := options.Tree
	if tree.MaxEntries <= 0 || tree.MaxPlanBytes <= 0 {
		return result, Fail(InvalidRequest, nil)
	}
	sourceIDs := make(map[*location]Validator)
	dirs, err := c.discoverTree(ctx, p, from.visible(), from, tree, treeDiscovery{
		target: to.visible(),
		open: func(at *location) (*directory, error) {
			scope, err := c.openDirectory(ctx, p, at)
			if err != nil {
				return nil, err
			}
			identified, ok := scope.native.(DirectoryIdentity)
			if !ok {
				return nil, errors.Join(Fail(UnsupportedOperation, nil), scope.close())
			}
			id := identified.DirectoryValidator()
			if !moveValidator(id) {
				return nil, errors.Join(Fail(InvalidUpstreamResponse, nil), scope.close())
			}
			if at == from {
				if err := options.Source.Check(Evidence{Validator: id}, true); err != nil {
					return nil, errors.Join(err, scope.close())
				}
			}
			sourceIDs[at] = id
			return scope, nil
		},
	})
	if err != nil {
		return result, err
	}
	targetCount := len(dirs)
	for i := range dirs {
		dir := &dirs[i]
		if err := c.preflightAt(ctx, p, RemoveAction, dir.at); err != nil {
			return result, err
		}
		if err := c.preflightAt(ctx, p, CreateAction, to.descend(dir.path)); err != nil {
			return result, err
		}
		files := dir.children[:0]
		for _, child := range dir.children {
			if child.kind != File {
				continue
			}
			at := dir.at.descend("/" + child.name)
			for _, action := range []Action{ReadAction, RemoveAction} {
				if err := c.preflightAt(ctx, p, action, at); err != nil {
					return result, err
				}
			}
			if err := c.preflightAt(ctx, p, CreateAction, to.descend(joinPath(dir.path, "/"+child.name))); err != nil {
				return result, err
			}
			files = append(files, child)
		}
		dir.children = files
		targetCount += len(files)
	}
	created := 0
	result.Tree = &TreeResult{}
	record := func(row TreeItemResult) {
		result.Tree.Items = append(result.Tree.Items, row)
		end := &result.Source
		if row.Side == TargetSide {
			end = &result.Target
			if row.Change.Effect == Applied {
				created++
			}
		}
		switch row.Change.Effect {
		case Unknown:
			end.Effect = Unknown
		case Applied, Incomplete:
			if end.Effect == Unchanged {
				end.Effect = Incomplete
			}
		}
	}
	defer func() {
		if created == targetCount && result.Target.Effect != Unknown {
			result.Target.Effect = Applied
		}
		result.Tree.Change.Effect = treeEffect(result.Tree.Items, result.Source.Effect == Applied && result.Target.Effect == Applied)
	}()
	targetIDs := make(map[string]Validator, len(dirs))
	for _, dir := range dirs {
		u, err := c.beginChangeAt(ctx, p, CreateAction, to.descend(dir.path))
		change := Change{Effect: Unchanged}
		if err == nil {
			change, err = target.MkdirForMove(u.ctx, u.native(), targetIDs[path.Dir(dir.path)])
			change, err = c.finishChange(u, change, err)
			u.release()
		}
		if dir.path == "/" && change.Effect == Applied {
			result.Target.Before = TargetMissing
		}
		id := change.Evidence.Validator
		if err == nil && !moveValidator(id) {
			err = Fail(InvalidUpstreamResponse, errors.New("missing created directory identity"))
		}
		record(TreeItemResult{RelativePath: dir.path, Side: TargetSide, Phase: CreatePhase, Change: change, Err: err})
		if err != nil {
			return result, err
		}
		targetIDs[dir.path] = id
	}

	limit := min(max(tree.Concurrency, 1), len(dirs))
	// Close all readers in a wave before removing any of its source names.
	for start := 0; start < len(dirs); start += limit {
		group := dirs[start:min(start+limit, len(dirs))]
		files, err := executeDirectories(ctx, len(group), max(tree.Concurrency, 1), nil,
			func(i int) (*directory, int, error) {
				dir := group[i]
				if len(dir.children) == 0 {
					return nil, 0, nil
				}
				scope, err := c.openDirectory(ctx, p, dir.at)
				if err != nil {
					return nil, 0, err
				}
				id, ok := scope.native.(DirectoryIdentity)
				if !ok || id.DirectoryValidator() != sourceIDs[dir.at] {
					return nil, 0, errors.Join(Fail(PreconditionFailed, nil), scope.close())
				}
				if err := scope.scan(tree, nil); err != nil {
					return nil, 0, errors.Join(err, scope.close())
				}
				return scope, len(dir.children), nil
			},
			func(i int, scope *directory, n int) work[movedFile] {
				dir := group[i]
				name := dir.children[n].name
				rel := joinPath(dir.path, "/"+name)
				a, b := dir.at.descend("/"+name), to.descend(rel)
				return work[movedFile]{accesses: []access{{at: a}, {at: b, write: true}}, run: func() (movedFile, error) {
					file := movedFile{at: a, row: TreeItemResult{RelativePath: rel, Side: SourceSide, Phase: ReadPhase, Change: Change{Effect: Unchanged}}}
					opened, err := scope.openFile(ctx, p, a)
					if err == nil {
						file.guard, err = moveFileGuard(source, a, opened)
					}
					if err != nil {
						file.row.Err = err
						return file, err
					}
					file.row.Side, file.row.Phase = TargetSide, WritePhase
					file.row.Change, file.row.Err = copyOpened(ctx, opened, func(input io.Reader, length *int64) (Change, error) {
						u, err := c.beginChangeAt(ctx, p, CreateAction, b)
						if err != nil {
							return Change{Effect: Unchanged}, err
						}
						defer u.release()
						change, err := target.PublishForMove(u.ctx, u.native(), targetIDs[dir.path], input, length)
						return c.finishPublication(u, change, err, IfAbsent(), false)
					})
					var failure *OperationError
					if errors.As(file.row.Err, &failure) {
						file.row.ErrorSide = failure.Side
					}
					return file, file.row.Err
				}}
			})
		for _, file := range files {
			record(file.row)
		}
		if err != nil {
			directoryFailures(err, func(i int, cause error) {
				record(TreeItemResult{RelativePath: group[i].path, Side: SourceSide, Phase: ReadPhase, Change: Change{Effect: Unchanged}, Err: cause})
			})
			return result, err
		}
		rows, err := execute(ctx, len(files), max(tree.Concurrency, 1), nil, func(i int) work[TreeItemResult] {
			file := files[i]
			return work[TreeItemResult]{accesses: []access{{at: file.at, write: true}}, run: func() (TreeItemResult, error) {
				change, err := c.removeMoved(ctx, p, file.at, file.guard, RemoveNonDirectory)
				return TreeItemResult{RelativePath: file.row.RelativePath, Side: SourceSide, Phase: RemovePhase, Change: change, Err: err}, err
			}}
		})
		for _, row := range rows {
			record(row)
		}
		if err != nil {
			return result, err
		}
	}
	for i := len(dirs) - 1; i >= 0; i-- {
		dir := dirs[i]
		change, err := c.removeMoved(ctx, p, dir.at, sourceIDs[dir.at], RemoveEmptyDirectory)
		record(TreeItemResult{RelativePath: dir.path, Side: SourceSide, Phase: RemovePhase, Change: change, Err: err})
		if i == 0 && change.Effect == Applied {
			result.Source.Effect = Applied
		}
		if err != nil {
			return result, err
		}
	}
	return result, nil
}
