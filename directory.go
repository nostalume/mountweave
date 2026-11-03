package core

import (
	"context"
	"errors"
	"path"
)

// DirectoryReader opens a bound directory. Every path component is
// opened without following native links or redirects; "/" is the prepared root.
// Opening does not require Next. The returned scope is owned even on error.
type DirectoryReader interface {
	OpenDirectory(context.Context, string) (DirectoryScope, error)
}

// DirectoryScope enumerates one bound directory. Only after normal EOF may
// OpenFile open an observed, unambiguous regular child by a single name.
// OpenFile may run concurrently; Next has one caller. Bodies must finish before
// Close. Neither the directory nor file content is promised to be a snapshot.
type DirectoryScope interface {
	Iterator
	OpenFile(context.Context, string) (Opened, error)
}

// ChildRemover is an optional capability of a DirectoryScope, not of Driver.
// It removes one current child name without resolving the parent again. File
// means a non-directory unlink (never following a final link); Directory means
// empty-directory removal only. It does not bind a discovered object version.
// No enumeration is required. Calls may overlap each other, but must not overlap
// Next, OpenFile or Close. Native work finishes before return; failed cleanup
// stays owned by the scope/Driver. Applied confirms name removal, not storage
// reclamation.
type ChildRemover interface {
	RemoveChild(context.Context, string, Kind) (Change, error)
}

// directory owns its native scope and the exact captured parent address. The
// owner use may authorize that directory or a child mutation opening its parent.
type directory struct {
	owner  *use
	at     *location
	native DirectoryScope
}

func (d *directory) close() error {
	if err := d.native.Close(); err != nil {
		d.owner.failed(err, d.native)
		return err
	}
	d.owner.release()
	return nil
}
func (c *Core) openDirectory(ctx context.Context, p Principal, at *location) (*directory, error) {
	u, err := c.admitAt(ctx, p, ListAction, at)
	if err != nil {
		return nil, err
	}
	return openDirectoryAt(u, false)
}
func openDirectoryAt(u *use, parent bool) (*directory, error) {
	reader, ok := u.mount.driver.(DirectoryReader)
	if !ok {
		u.release()
		return nil, Fail(UnsupportedOperation, nil)
	}
	at := u.location
	if parent {
		at = &location{binding: at.binding, path: path.Dir(at.path)}
	}
	native, err := reader.OpenDirectory(u.ctx, at.path)
	if err == nil {
		err = u.ctx.Err()
	}
	if err == nil && native == nil {
		err = Fail(InvalidUpstreamResponse, nil)
	}
	if err != nil {
		if native != nil {
			if closeErr := native.Close(); closeErr != nil {
				u.failed(closeErr, native)
				return nil, errors.Join(err, closeErr)
			}
		}
		u.release()
		return nil, err
	}
	return &directory{owner: u, at: at, native: native}, nil
}
func (d *directory) child(name string) (*location, error) {
	if !ValidName(name) {
		return nil, Fail(UnrepresentableName, nil)
	}
	at := d.at.descend("/" + name)
	if err := ValidatePath(at.visible()); err != nil {
		return nil, err
	}
	return at, nil
}
func (d *directory) openFile(ctx context.Context, p Principal, name string) (Content, error) {
	at, err := d.child(name)
	if err != nil {
		return Content{}, err
	}
	c := at.mount.core
	u, err := c.admitAt(ctx, p, ReadAction, at)
	return c.openUse(u, nil, err, func(call context.Context) (Opened, error) { return d.native.OpenFile(call, name) })
}
func (d *directory) removeChild(u *use, kind Kind) (Change, error) {
	if u.binding != d.at.binding || path.Dir(u.path) != d.at.path {
		return Change{Unchanged}, Fail(InvalidRequest, nil)
	}
	remover, ok := d.native.(ChildRemover)
	if !ok {
		return Change{Unchanged}, Fail(UnsupportedOperation, nil)
	}
	if err := u.ctx.Err(); err != nil {
		return Change{Unchanged}, err
	}
	result, err := remover.RemoveChild(u.ctx, path.Base(u.path), kind)
	return u.mount.core.finishChange(u, result, err)
}

// executeDirectories owns bounded batches of prepared scopes around execute.
// open cleans up its failed acquisition; an empty group returns no scope.
// prepare follows group order and is called once per started or pending item.
func executeDirectories[T any](ctx context.Context, count, limit int, results []T,
	open func(int) (*directory, int, error),
	prepare func(int, *directory, int) work[T],
) ([]T, error) {
	type group struct {
		index, count int
		scope        *directory
	}
	for next := 0; next < count; {
		groups := make([]group, 0, min(limit, count-next))
		total := 0
		var prepareErr error
		for next < count && len(groups) < limit {
			if prepareErr = ctx.Err(); prepareErr != nil {
				break
			}
			index := next
			next++
			scope, n, err := open(index)
			if err != nil {
				prepareErr = err
				break
			}
			if n != 0 {
				groups = append(groups, group{index, n, scope})
				total += n
			}
		}
		var runErr error
		if prepareErr == nil {
			g, item := 0, 0
			results, runErr = execute(ctx, total, limit, results, func(int) work[T] {
				current := groups[g]
				work := prepare(current.index, current.scope, item)
				item++
				if item == current.count {
					g++
					item = 0
				}
				return work
			})
		}
		var closeErr error
		for _, current := range groups {
			closeErr = errors.Join(closeErr, current.scope.close())
		}
		if err := errors.Join(prepareErr, runErr, closeErr); err != nil {
			return results, err
		}
	}
	return results, ctx.Err()
}
