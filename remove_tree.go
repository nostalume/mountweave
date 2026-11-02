package core

import (
	"context"
	"errors"
	"fmt"
	"path"
)

type removalEntry struct {
	relative string
	kind     Kind
}

type removalDirectory struct {
	path     string
	depth    int
	children []removalEntry
}

// RemoveTree removes only the names in a bounded discovery of one directory.
// Compatible same-name replacements can be removed; unlisted new names are not
// chased. Directories are removed only when empty. Results are the started prefix
// in child-before-parent execution order, with the root last.
func (c *Core) RemoveTree(ctx context.Context, p Principal, target string, options TreeOptions) (results []TreeItemResult, err error) {
	limit, err := concurrency([]ExecutionOptions{options.ExecutionOptions})
	if err != nil {
		return nil, err
	}
	if options.MaxEntries <= 0 || options.MaxPlanBytes <= 0 {
		return nil, Fail(InvalidRequest, nil)
	}
	root, _, err := c.beginChangeAt(ctx, p, RemoveAction, target, nil)
	if err != nil {
		return nil, err
	}
	defer root.release()
	ctx, end := requestScope(ctx, root.ctx)
	defer end()
	defer func() { err = observedCause(ctx, err) }()
	at := func(relative string) *location {
		return &location{binding: root.binding, path: joinPath(root.path, relative)}
	}
	// Root removal needs its parent's mutation scope, without listing that parent.
	parent, scope, err := c.openRemovalDirectory(ctx, p, target, at("/"))
	if err != nil {
		return nil, err
	}
	if err = closeDirectory(parent, scope); err != nil {
		return nil, err
	}

	directories := []removalDirectory{{path: "/"}}
	entries, bytes := 1, 1
	for i := 0; i < len(directories); i++ {
		directory := directories[i].path
		u, scope, openErr := c.openDirectory(ctx, p, joinPath(target, directory), at(directory))
		if openErr != nil {
			return nil, fmt.Errorf("discover %s: %w", directory, openErr)
		}
		var scanErr error
		if _, ok := scope.(ChildRemover); !ok {
			scanErr = Fail(UnsupportedOperation, nil)
		} else {
			scanErr = scanDirectory(u, scope, joinPath(target, directory), options, func(meta Metadata) error {
				relative := joinPath(directory, "/"+meta.Name)
				if ValidatePath(joinPath(target, relative)) != nil {
					return Fail(UnrepresentableName, nil)
				}
				entries++
				bytes += len(relative)
				if entries > options.MaxEntries || bytes > options.MaxPlanBytes {
					return Fail(CapacityExceeded, nil)
				}
				if err := c.preflightChange(ctx, p, RemoveAction, joinPath(target, relative), at(relative)); err != nil {
					return err
				}
				directories[i].children = append(directories[i].children, removalEntry{relative, meta.Kind})
				if meta.Kind == Directory {
					directories = append(directories, removalDirectory{path: relative, depth: directories[i].depth + 1})
				}
				return nil
			})
		}
		closeErr := closeDirectory(u, scope)
		if scanErr != nil || closeErr != nil {
			return nil, fmt.Errorf("discover %s: %w", directory, errors.Join(scanErr, closeErr))
		}
	}

	removeGroups := func(groups []removalDirectory) error {
		var removeErr error
		results, removeErr = executeDirectories(ctx, len(groups), limit, results,
			func(i int) (*use, DirectoryScope, int, error) {
				children := groups[i].children
				if len(children) == 0 {
					return nil, nil, 0, nil
				}
				first := children[0].relative
				u, scope, openErr := c.openRemovalDirectory(ctx, p, joinPath(target, first), at(first))
				if openErr != nil {
					openErr = fmt.Errorf("open removal parent of %s: %w", first, openErr)
				}
				return u, scope, len(children), openErr
			},
			func(i int, scope DirectoryScope, item int) work[TreeItemResult] {
				entry := groups[i].children[item]
				expected := at(entry.relative)
				return work[TreeItemResult]{accesses: []access{{at: expected, write: true}}, run: func() (TreeItemResult, error) {
					row := TreeItemResult{RelativePath: entry.relative, Change: Change{Unchanged}}
					item, native, admissionErr := c.beginChangeAt(ctx, p, RemoveAction, joinPath(target, entry.relative), expected)
					if admissionErr != nil {
						row.Err = admissionErr
					} else {
						defer item.release()
						if row.Err = item.ctx.Err(); row.Err == nil {
							row.Change, row.Err = scope.(ChildRemover).RemoveChild(item.ctx, path.Base(native), entry.kind)
							row.Change, row.Err = c.finishChange(item, row.Change, row.Err)
						}
					}
					return row, row.Err
				}}
			})
		return removeErr
	}
	// Discovery is breadth-first. Reverse depth bands without reversing siblings.
	for end := len(directories); end > 0; {
		start := end - 1
		for start > 0 && directories[start-1].depth == directories[end-1].depth {
			start--
		}
		if err = removeGroups(directories[start:end]); err != nil {
			return results, err
		}
		end = start
	}
	err = removeGroups([]removalDirectory{{children: []removalEntry{{relative: "/", kind: Directory}}}})
	return results, err
}

// The child's RemoveAction authorizes opening its parent for mutation, not listing.
func (c *Core) openRemovalDirectory(ctx context.Context, p Principal, target string, expected *location) (*use, DirectoryScope, error) {
	u, native, err := c.beginChangeAt(ctx, p, RemoveAction, target, expected)
	if err != nil {
		return nil, nil, err
	}
	scope, err := openDirectoryAt(u, path.Dir(native))
	if err != nil {
		return nil, nil, err
	}
	if _, ok := scope.(ChildRemover); !ok {
		return nil, nil, errors.Join(Fail(UnsupportedOperation, nil), closeDirectory(u, scope))
	}
	return u, scope, nil
}
