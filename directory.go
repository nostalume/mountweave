package core

import (
	"context"
	"errors"
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

func closeDirectory(u *use, scope DirectoryScope) error {
	if scope != nil {
		if err := scope.Close(); err != nil {
			u.failed(err, scope)
			return err
		}
	}
	u.release()
	return nil
}

func (c *Core) openDirectory(ctx context.Context, p Principal, target string, expected *location) (*use, DirectoryScope, error) {
	u, _, err := c.admit(ctx, p, ListAction, target, expected)
	if err != nil {
		return nil, nil, err
	}
	if u == nil {
		return nil, nil, Fail(UnsupportedOperation, nil)
	}
	scope, err := openDirectoryAt(u, u.path)
	if err != nil {
		return nil, nil, err
	}
	return u, scope, nil
}

// openDirectoryAt adopts a native scope under the already admitted use.
func openDirectoryAt(u *use, relative string) (DirectoryScope, error) {
	reader, ok := u.mount.driver.(DirectoryReader)
	if !ok {
		u.release()
		return nil, Fail(UnsupportedOperation, nil)
	}
	scope, err := reader.OpenDirectory(u.ctx, relative)
	if err == nil {
		err = u.ctx.Err()
	}
	if err == nil && scope == nil {
		err = Fail(InvalidUpstreamResponse, nil)
	}
	if err != nil {
		return nil, errors.Join(err, closeDirectory(u, scope))
	}
	return scope, nil
}

// executeDirectories owns bounded batches of prepared scopes around execute.
// open cleans up its failed acquisition; an empty group returns no scope.
// prepare follows group order and is called once per started or pending item.
func executeDirectories[T any](ctx context.Context, count, limit int, results []T,
	open func(int) (*use, DirectoryScope, int, error),
	prepare func(int, DirectoryScope, int) work[T],
) ([]T, error) {
	type group struct {
		index, count int
		use          *use
		scope        DirectoryScope
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
			u, scope, n, err := open(index)
			if err != nil {
				prepareErr = err
				break
			}
			if n != 0 {
				groups = append(groups, group{index, n, u, scope})
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
			closeErr = errors.Join(closeErr, closeDirectory(current.use, current.scope))
		}
		if err := errors.Join(prepareErr, runErr, closeErr); err != nil {
			return results, err
		}
	}
	return results, ctx.Err()
}
