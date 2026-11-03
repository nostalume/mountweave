package core

import (
	"context"
	"errors"
	"fmt"
)

// RemovalScope selects permitted effects, independently of execution strategy.
type RemovalScope string

const (
	Discovered RemovalScope = "discovered"
	Subtree    RemovalScope = "subtree"
)

// DirectoryRemoveOptions defaults to Discovered when omitted or empty.
type DirectoryRemoveOptions struct{ Scope RemovalScope }

// SubtreeRemover optionally removes a directory and its native descendants.
// It must not follow redirects outside that subtree. Limits bound client discovery
// and planning, not the number of descendants a native recursive request deletes.
// Applied requires confirmed completion, not asynchronous task acceptance. On
// cancellation, stop dispatch and await active client I/O; remote effects may
// continue and must remain Unknown when completion cannot be established.
type SubtreeRemover interface {
	RemoveSubtree(context.Context, string, TreeOptions) (Change, error)
}

type removalEntry struct {
	relative string
	kind     Kind
}

type removalDirectory struct {
	path     string
	depth    int
	children []removalEntry
}

// RemoveTree defaults to a bounded discovery: only selected names are removed,
// compatible same-name replacements may be removed, and directories must be empty.
// Explicit Subtree requires RemoveTreeAction and permits native recursive effects,
// including new descendants. Native execution returns one root receipt; otherwise
// the existing bounded planner/executor returns child-before-parent receipts.
// Neither mode promises a snapshot or protection against later recreation.
func (c *Core) RemoveTree(ctx context.Context, p Principal, target string, options TreeOptions, policies ...DirectoryRemoveOptions) (results []TreeItemResult, err error) {
	if len(policies) > 1 {
		return nil, Fail(InvalidRequest, nil)
	}
	scope := Discovered
	if len(policies) == 1 && policies[0].Scope != "" {
		scope = policies[0].Scope
	}
	if scope != Discovered && scope != Subtree {
		return nil, Fail(InvalidRequest, nil)
	}
	limit, err := concurrency([]ExecutionOptions{options.ExecutionOptions})
	if err != nil {
		return nil, err
	}
	if options.MaxEntries <= 0 || options.MaxPlanBytes <= 0 {
		return nil, Fail(InvalidRequest, nil)
	}
	action := RemoveAction
	if scope == Subtree {
		action = RemoveTreeAction
	}
	root, err := c.beginChange(ctx, p, action, target)
	if err != nil {
		return nil, err
	}
	defer root.release()
	ctx, end := requestScope(ctx, root.ctx)
	defer end()
	defer func() { err = observedCause(ctx, err) }()
	if scope == Subtree {
		if remover, ok := root.mount.driver.(SubtreeRemover); ok {
			if err = ctx.Err(); err != nil {
				return nil, err
			}
			change, nativeErr := remover.RemoveSubtree(root.ctx, root.path, options)
			change, nativeErr = c.finishChange(root, change, nativeErr)
			return []TreeItemResult{{RelativePath: "/", Change: change, Err: nativeErr}}, nativeErr
		}
	}
	at := func(relative string) *location {
		return root.location.descend(relative)
	}
	// Whole-subtree authority includes internal traversal and child mutation.
	// Renew that root grant for client steps while retaining the captured binding.
	begin := func(at *location) (*use, error) {
		if scope == Discovered {
			return c.beginChangeAt(ctx, p, RemoveAction, at)
		}
		if err := c.preflightAt(ctx, p, RemoveTreeAction, root.location); err != nil {
			return nil, err
		}
		c.mu.Lock()
		u, err := c.borrowAtLocked(ctx, at)
		c.mu.Unlock()
		return u, err
	}
	openRemoval := func(at *location) (*directory, error) { return openRemovalDirectory(begin(at)) }
	parent, err := openRemoval(at("/"))
	if err != nil {
		return nil, err
	}
	if err = parent.close(); err != nil {
		return nil, err
	}

	directories := []removalDirectory{{path: "/"}}
	entries, bytes := 1, 1
	for i := 0; i < len(directories); i++ {
		relativeDir := directories[i].path
		var directoryScope *directory
		var openErr error
		if scope == Discovered {
			directoryScope, openErr = c.openDirectory(ctx, p, at(relativeDir))
		} else {
			var u *use
			u, openErr = begin(at(relativeDir))
			if openErr == nil {
				directoryScope, openErr = openDirectoryAt(u, false)
			}
		}
		if openErr != nil {
			return nil, fmt.Errorf("discover %s: %w", relativeDir, openErr)
		}
		var scanErr error
		if _, ok := directoryScope.native.(ChildRemover); !ok {
			scanErr = Fail(UnsupportedOperation, nil)
		} else {
			scanErr = directoryScope.scan(options, func(meta Metadata) error {
				relative := joinPath(relativeDir, "/"+meta.Name)
				if ValidatePath(joinPath(target, relative)) != nil {
					return Fail(UnrepresentableName, nil)
				}
				entries++
				bytes += len(relative)
				if entries > options.MaxEntries || bytes > options.MaxPlanBytes {
					return Fail(CapacityExceeded, nil)
				}
				if scope == Discovered {
					if err := c.preflightAt(ctx, p, RemoveAction, at(relative)); err != nil {
						return err
					}
				}
				directories[i].children = append(directories[i].children, removalEntry{relative, meta.Kind})
				if meta.Kind == Directory {
					directories = append(directories, removalDirectory{path: relative, depth: directories[i].depth + 1})
				}
				return nil
			})
		}
		closeErr := directoryScope.close()
		if scanErr != nil || closeErr != nil {
			return nil, fmt.Errorf("discover %s: %w", relativeDir, errors.Join(scanErr, closeErr))
		}
	}

	removeGroups := func(groups []removalDirectory) error {
		var removeErr error
		results, removeErr = executeDirectories(ctx, len(groups), limit, results,
			func(i int) (*directory, int, error) {
				children := groups[i].children
				if len(children) == 0 {
					return nil, 0, nil
				}
				first := children[0].relative
				scope, openErr := openRemoval(at(first))
				if openErr != nil {
					openErr = fmt.Errorf("open removal parent of %s: %w", first, openErr)
				}
				return scope, len(children), openErr
			},
			func(i int, scope *directory, item int) work[TreeItemResult] {
				entry := groups[i].children[item]
				expected := at(entry.relative)
				return work[TreeItemResult]{accesses: []access{{at: expected, write: true}}, run: func() (TreeItemResult, error) {
					row := TreeItemResult{RelativePath: entry.relative, Change: Change{Unchanged}}
					item, admissionErr := begin(expected)
					if admissionErr != nil {
						row.Err = admissionErr
					} else {
						defer item.release()
						if row.Err = item.ctx.Err(); row.Err == nil {
							row.Change, row.Err = scope.removeChild(item, entry.kind)
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
