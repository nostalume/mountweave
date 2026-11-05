package core

import (
	"context"
	"errors"
	"fmt"
	"io"
)

type TreeOptions struct {
	ExecutionOptions
	// Directory plans require positive caller-selected budgets; a file leaf
	// accepts zero. Replacement bounds source and old-target plans separately.
	// MaxPlanBytes counts relative-path UTF-8 bytes, not responses or content.
	MaxEntries   int `json:"max_entries"`
	MaxPlanBytes int `json:"max_plan_bytes"`
}

// TreeCopyOptions remains an alias for existing CopyTree callers.
type TreeCopyOptions = TreeOptions

type treeDirectory struct {
	path     string
	at       *location // nil denotes a virtual hierarchy node, never a native path.
	depth    int
	children []treeEntry
}

type treeEntry struct {
	name string
	kind Kind
}

// treeDiscovery varies effect policy while sharing bounded namespace discovery.
// It owns no resources after discoverTree returns.
type treeDiscovery struct {
	open          func(*location) (*directory, error)
	visit         func(*location, Kind) error
	recurse       func(*location) bool
	virtualAction Action
	target        string
}

func (c *Core) discoverTree(ctx context.Context, p Principal, root string, at *location, options TreeOptions, policy treeDiscovery) ([]treeDirectory, error) {
	directories := []treeDirectory{{path: "/", at: at}}
	entries, bytes := 1, 1
	for i := 0; i < len(directories); i++ {
		dir := directories[i]
		visible := joinPath(root, dir.path)
		room := MaxPathBytes
		if policy.target != "" {
			room = childRoom(joinPath(policy.target, dir.path))
		}
		var children []treeEntry
		add := func(meta Metadata, child *location) error {
			if len(meta.Name) > room {
				return Fail(UnrepresentableName, nil)
			}
			rel := joinPath(dir.path, "/"+meta.Name)
			entries++
			bytes += len(rel)
			if entries > options.MaxEntries || bytes > options.MaxPlanBytes {
				return Fail(CapacityExceeded, nil)
			}
			if dir.at != nil && policy.visit != nil {
				if err := policy.visit(child, meta.Kind); err != nil {
					return err
				}
			}
			children = append(children, treeEntry{meta.Name, meta.Kind})
			if meta.Kind == Directory && (dir.at == nil || policy.recurse == nil || policy.recurse(child)) {
				directories = append(directories, treeDirectory{path: rel, at: child, depth: dir.depth + 1})
			}
			return nil
		}
		var err error
		if dir.at == nil {
			grant := visible
			if policy.virtualAction == RemoveTreeAction {
				grant = root
			}
			if err = c.authorize(ctx, p, policy.virtualAction, Path(grant)); err != nil {
				return nil, err
			}
			c.mu.Lock()
			_, names, resolveErr := c.resolveRequestLocked(ctx, Path(visible))
			c.mu.Unlock()
			if resolveErr != nil {
				return nil, resolveErr
			}
			for _, name := range names {
				target := childPath(visible, name)
				if err = c.authorize(ctx, p, StatAction, Path(target)); err != nil {
					if CodeOf(err) == PermissionDenied {
						err = nil
						continue
					}
					return nil, err
				}
				c.mu.Lock()
				child, _, resolveErr := c.resolveRequestLocked(ctx, Path(target))
				c.mu.Unlock()
				if resolveErr != nil {
					return nil, resolveErr
				}
				if err = add(Metadata{Name: name, Kind: Directory}, child); err != nil {
					break
				}
			}
		} else {
			var scope *directory
			scope, err = policy.open(dir.at)
			if err == nil {
				err = scope.scan(options, func(meta Metadata) error { return add(meta, dir.at.descend("/"+meta.Name)) })
				err = errors.Join(err, scope.close())
			}
		}
		if err != nil {
			return nil, fmt.Errorf("discover %s: %w", dir.path, err)
		}
		directories[i].children = children
	}
	return directories, ctx.Err()
}

// readDirectory returns an owned, fully enumerated scope. A second, bounded
// scan at execution observes current membership without rescanning ancestors per
// file. Additional entries are validated but do not extend the original plan.
func (c *Core) readDirectory(ctx context.Context, p Principal, at *location, options TreeOptions, visit func(Metadata) error) (*directory, error) {
	scope, err := c.openDirectory(ctx, p, at)
	if err != nil {
		return nil, err
	}
	if err = scope.scan(options, visit); err != nil {
		return nil, errors.Join(err, scope.close())
	}
	return scope, nil
}

func (d *directory) scan(options TreeOptions, visit func(Metadata) error) error {
	u, scope, target := d.owner, d.native, d.at.visible()
	room := min(childRoom(target), childRoom(d.at.primary()))
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
			if len(meta.Name) > room {
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
			if _, err := project(Path(joinPath(target, "/"+meta.Name)), meta); err != nil {
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

// inspectTree selects the actual branch under one of its operation grants. It
// adds no StatAction requirement. Execution keeps its own admission and native
// condition checks; this observation is never a later mutation's version guard.
func (c *Core) inspectTree(ctx context.Context, p Principal, target Address, fileAction, directoryAction Action, version Version) (*use, Entry, error) {
	u, _, err := c.admit(ctx, p, fileAction, target)
	action := fileAction
	if CodeOf(err) == PermissionDenied && fileAction != directoryAction {
		u, _, err = c.admit(ctx, p, directoryAction, target)
		action = directoryAction
	}
	if err != nil {
		return nil, Entry{}, err
	}
	fail := func(err error) (*use, Entry, error) {
		if u != nil {
			u.release()
		}
		return nil, Entry{}, err
	}
	entry := virtualEntry(target)
	if u != nil {
		entry, err = c.observe(u, version)
		if err != nil {
			return fail(err)
		}
	}
	wanted := fileAction
	if entry.Kind == Directory {
		wanted = directoryAction
	}
	if wanted != action {
		if err := c.authorizeChecked(ctx, p, wanted, target); err != nil {
			return fail(err)
		}
		if u != nil {
			if err := c.authorizeBinding(ctx, p, wanted, u.location); err != nil {
				return fail(err)
			}
		}
	}
	return u, entry, nil
}
