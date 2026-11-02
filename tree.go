package core

import (
	"context"
	"errors"
	"fmt"
	"io"
)

type TreeOptions struct {
	ExecutionOptions
	// Both budgets are caller-selected and positive. MaxPlanBytes counts stored
	// relative-path UTF-8 bytes, not native response bytes or file content.
	MaxEntries   int
	MaxPlanBytes int
}

// TreeCopyOptions remains an alias for existing CopyTree callers.
type TreeCopyOptions = TreeOptions

// DirectoryCopyOptions selects directory and file policies independently.
// MergeDirectories accepts existing directories. WriteOptions governs each file.
type DirectoryCopyOptions struct {
	WriteOptions
	MergeDirectories bool
}

type TreeItemResult struct {
	RelativePath string // "/" is the operation root.
	Change       Change
	Err          error
}

type treeDirectory struct {
	path  string
	files []string
}

// CopyTree discovers a finite source directory before writing its target tree.
// Existing directories require explicit MergeDirectories; file publication uses
// the selected WriteOptions. It preserves empty directories without source
// deletion, snapshots, retries or rollback. Supply at most one DirectoryCopyOptions.
func (c *Core) CopyTree(ctx context.Context, p Principal, source, target string, options TreeOptions, directoryOptions ...DirectoryCopyOptions) (results []TreeItemResult, err error) {
	if len(directoryOptions) > 1 {
		return nil, Fail(InvalidRequest, nil)
	}
	var policy DirectoryCopyOptions
	if len(directoryOptions) == 1 {
		policy = directoryOptions[0]
	}
	limit, err := concurrency([]ExecutionOptions{options.ExecutionOptions})
	if err != nil {
		return nil, err
	}
	if options.MaxEntries <= 0 || options.MaxPlanBytes <= 0 {
		return nil, Fail(InvalidRequest, nil)
	}
	from, _, err := c.admit(ctx, p, ListAction, source, nil)
	if err != nil {
		return nil, err
	}
	if from == nil {
		return nil, Fail(UnsupportedOperation, nil)
	}
	defer from.release()
	to, _, err := c.admit(ctx, p, CreateAction, target, nil)
	if err != nil {
		return nil, err
	}
	if to == nil {
		return nil, Fail(UnsupportedOperation, nil)
	}
	defer to.release()
	if target == to.binding.prefix || to.path == "/" ||
		(from.mount == to.mount && (Within(to.path, from.path) || Within(from.path, to.path))) {
		return nil, Fail(InvalidRequest, nil)
	}
	if _, ok := from.mount.driver.(DirectoryReader); !ok {
		return nil, Fail(UnsupportedOperation, nil)
	}
	if _, ok := to.mount.driver.(DirectoryMaker); !ok {
		return nil, Fail(UnsupportedOperation, nil)
	}
	if policy.Replace {
		if _, ok := to.mount.driver.(FilePublisher); !ok {
			return nil, Fail(UnsupportedOperation, nil)
		}
	} else if _, ok := to.mount.driver.(FileCreator); !ok {
		return nil, Fail(UnsupportedOperation, nil)
	}
	if policy.MergeDirectories {
		if _, ok := to.mount.driver.(DirectoryReader); !ok {
			return nil, Fail(UnsupportedOperation, nil)
		}
	}
	ctx, endSource := requestScope(ctx, from.ctx)
	defer endSource()
	ctx, endTarget := requestScope(ctx, to.ctx)
	defer endTarget()
	defer func() { err = observedCause(ctx, err) }()

	at := func(u *use, rel string) *location {
		return &location{binding: u.binding, path: joinPath(u.path, rel)}
	}
	directories := []treeDirectory{{path: "/"}}
	entries, pathBytes := 1, 1
	for i := 0; i < len(directories); i++ {
		directory := directories[i].path
		var files []string
		u, scope, readErr := c.readDirectory(ctx, p, joinPath(source, directory), at(from, directory), options, func(meta Metadata) error {
			rel := joinPath(directory, "/"+meta.Name)
			if ValidatePath(joinPath(source, rel)) != nil || ValidatePath(joinPath(target, rel)) != nil {
				return Fail(UnrepresentableName, nil)
			}
			entries++
			pathBytes += len(rel)
			if entries > options.MaxEntries || pathBytes > options.MaxPlanBytes {
				return Fail(CapacityExceeded, nil)
			}
			if meta.Kind == Directory {
				directories = append(directories, treeDirectory{path: rel})
			} else {
				files = append(files, meta.Name)
			}
			return nil
		})
		if readErr != nil {
			return nil, fmt.Errorf("discover %s: %w", directory, readErr)
		}
		if closeErr := closeDirectory(u, scope); closeErr != nil {
			return nil, fmt.Errorf("close discovery %s: %w", directory, closeErr)
		}
		directories[i].files = files
	}
	// No source enumeration remains open when namespace writes begin.
	results, err = execute(ctx, len(directories), 1, results, func(i int) work[TreeItemResult] {
		rel := directories[i].path
		targetAt := at(to, rel)
		return work[TreeItemResult]{accesses: []access{{at: targetAt, write: true}}, run: func() (TreeItemResult, error) {
			change, err := c.copyDirectory(ctx, p, joinPath(target, rel), targetAt, policy.MergeDirectories)
			return TreeItemResult{RelativePath: rel, Change: change, Err: err}, err
		}}
	})
	if err != nil {
		return results, err
	}
	results, err = executeDirectories(ctx, len(directories), limit, results,
		func(i int) (*use, DirectoryScope, int, error) {
			directory := directories[i]
			if len(directory.files) == 0 {
				return nil, nil, 0, nil
			}
			u, scope, openErr := c.readDirectory(ctx, p, joinPath(source, directory.path), at(from, directory.path), options, nil)
			if openErr != nil {
				openErr = fmt.Errorf("open source directory %s: %w", directory.path, openErr)
			}
			return u, scope, len(directory.files), openErr
		},
		func(i int, scope DirectoryScope, item int) work[TreeItemResult] {
			directory := directories[i]
			rel := joinPath(directory.path, "/"+directory.files[item])
			sourceAt, targetAt := at(from, rel), at(to, rel)
			return work[TreeItemResult]{accesses: []access{{at: sourceAt}, {at: targetAt, write: true}}, run: func() (TreeItemResult, error) {
				row := TreeItemResult{RelativePath: rel, Change: Change{Unchanged}}
				opened, err := c.open(ctx, p, joinPath(source, rel), nil, sourceAt, scope)
				if err != nil {
					row.Err = fmt.Errorf("open source: %w", err)
					return row, row.Err
				}
				row.Change, row.Err = c.copyContent(ctx, p, joinPath(target, rel), opened, policy.WriteOptions, targetAt)
				return row, row.Err
			}}
		})
	return results, err
}

// copyDirectory accepts an existing directory only after an unchanged conflict
// and a no-follow open. The scope confirms this name now, not the whole future
// target tree. Confirmation uses CreateAction without consuming its iterator.
func (c *Core) copyDirectory(ctx context.Context, p Principal, target string, expected *location, merge bool) (Change, error) {
	change, err := c.mkdir(ctx, p, target, expected)
	if !merge || change.Effect != Unchanged || !directoryConflict(err) {
		return change, err
	}
	u, _, err := c.beginChangeAt(ctx, p, CreateAction, target, expected)
	if err != nil {
		return change, err
	}
	scope, err := openDirectoryAt(u, u.path)
	if err != nil {
		return change, err
	}
	return change, observedCause(ctx, closeDirectory(u, scope))
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

// readDirectory returns an owned, fully enumerated scope. A second, bounded
// scan at execution observes current membership without rescanning ancestors per
// file. Additional entries are validated but do not extend the original plan.
func (c *Core) readDirectory(ctx context.Context, p Principal, target string, expected *location, options TreeOptions, visit func(Metadata) error) (*use, DirectoryScope, error) {
	u, scope, err := c.openDirectory(ctx, p, target, expected)
	if err != nil {
		return nil, nil, err
	}
	if err = scanDirectory(u, scope, target, options, visit); err != nil {
		return nil, nil, errors.Join(err, closeDirectory(u, scope))
	}
	return u, scope, nil
}

func scanDirectory(u *use, scope DirectoryScope, target string, options TreeOptions, visit func(Metadata) error) error {
	seen := make(map[string]bool)
	count, bytes := 0, 0
	for {
		if err := u.ctx.Err(); err != nil {
			return err
		}
		batch, nextErr := scope.Next(u.ctx, MaxPage)
		if nextErr != nil && !errors.Is(nextErr, io.EOF) {
			return nextErr
		}
		// Empty nonterminal pages consume work too; they cannot evade budgets.
		if len(batch) == 0 && !errors.Is(nextErr, io.EOF) {
			count++
		} else {
			count += len(batch)
		}
		if count > options.MaxEntries {
			return Fail(CapacityExceeded, nil)
		}
		for _, meta := range batch {
			if !ValidName(meta.Name) {
				return Fail(UnrepresentableName, nil)
			}
			if seen[meta.Name] {
				return Fail(PathAmbiguous, nil)
			}
			seen[meta.Name] = true
			bytes += len(meta.Name)
			if bytes > options.MaxPlanBytes {
				return Fail(CapacityExceeded, nil)
			}
			if _, err := project(joinPath(target, "/"+meta.Name), meta); err != nil {
				return err
			}
			if visit != nil {
				if err := visit(meta); err != nil {
					return err
				}
			}
		}
		if errors.Is(nextErr, io.EOF) {
			if err := u.ctx.Err(); err != nil {
				return err
			}
			return nil
		}
	}
}
