package core

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path"
	"strings"
)

// availableChild selects an exact candidate under a captured directory binding.
// Its observation is advisory. The mutation must still create exclusively.
func (c *Core) availableChild(ctx context.Context, p Principal, directory Address, preferred string) (*location, error) {
	if directory.Namespace() != TreeNamespace || !ValidName(preferred) {
		return nil, Fail(InvalidRequest, nil)
	}
	parent, _, err := c.admit(ctx, p, StatAction, directory)
	if err != nil {
		return nil, err
	}
	if parent == nil {
		return nil, Fail(UnsupportedOperation, errors.New("destination must be a mounted directory"))
	}
	defer parent.release()
	entry, err := c.observe(parent, "")
	if err != nil {
		return nil, err
	}
	if entry.Kind != Directory {
		return nil, Fail(NotDirectory, nil)
	}
	extension := path.Ext(preferred)
	if extension == preferred {
		extension = ""
	}
	stem := strings.TrimSuffix(preferred, extension)
	name := preferred
	leaf, observesLeaf := parent.mount.driver.(LeafStatter)
	for i := 1; i <= 100; i++ {
		if i > 1 {
			name = fmt.Sprintf("%s (%d)%s", stem, i, extension)
		}
		at := parent.location.descend("/" + name)
		if !at.validExtent() || !pathExtent(at.visible()) {
			return nil, Fail(InvalidPath, nil)
		}
		candidate, err := c.admitAt(parent.ctx, p, StatAction, at)
		if err != nil {
			return nil, err
		}
		if observesLeaf {
			meta, nativeErr := leaf.Lstat(candidate.ctx, candidate.native())
			_, err = observedEntry(candidate, meta, nativeErr, true)
		} else {
			_, err = c.observe(candidate, "")
		}
		candidate.release()
		switch {
		case CodeOf(err) == NotFound:
			return at, nil
		case err != nil:
			return nil, err
		}
	}
	return nil, Fail(AlreadyExists, errors.New("could not find an available destination name"))
}

// WriteInto creates a new file inside an existing mounted directory. It uses the
// preferred name or an available numbered variant. A concurrent occupant is an
// exclusive-creation conflict. The caller retains input ownership.
func (c *Core) WriteInto(ctx context.Context, p Principal, directory Address, name string, input io.Reader, length *int64) (destination Address, change Change, err error) {
	change.Effect = Unchanged
	if input == nil || length != nil && *length < 0 {
		return destination, change, Fail(InvalidRequest, nil)
	}
	ctx, turn, err := enterOrder(ctx, nil)
	if err != nil {
		return destination, change, err
	}
	defer func() { turn.finish(change.Effect == Unknown) }()
	at, err := c.availableChild(ctx, p, directory, name)
	if err != nil {
		return destination, change, err
	}
	change, err = c.writeAt(ctx, p, at, input, length, WriteOptions{})
	if change.Effect == Applied || change.Effect == Incomplete {
		destination = at.address()
	}
	return destination, change, err
}

// CopyInto copies one file to an existing mounted directory using its source
// name or an available numbered variant. The chosen child is created exclusively.
func (c *Core) CopyInto(ctx context.Context, p Principal, source, directory Address) (destination Address, change Change, err error) {
	change.Effect = Unchanged
	ctx, turn, err := enterOrder(ctx, nil)
	if err != nil {
		return destination, change, err
	}
	defer func() { turn.finish(change.Effect == Unknown) }()
	ctx, at, err := c.copyTargetIn(ctx, p, source, directory)
	if err != nil {
		return destination, change, err
	}
	change, err = c.Copy(ctx, p, source, at.address())
	if change.Effect == Applied || change.Effect == Incomplete {
		destination = at.address()
	}
	return destination, change, err
}

// CopyTreeInto selects an exclusive child for a recursive copy. Its receipt
// retains the same item effects as CopyTree.
func (c *Core) CopyTreeInto(ctx context.Context, p Principal, source, directory Address, options TreeOptions) (destination Address, tree TreeResult, err error) {
	tree.Change.Effect = Unchanged
	ctx, turn, err := enterOrder(ctx, nil)
	if err != nil {
		return destination, tree, err
	}
	defer func() { turn.finish(tree.Change.Effect == Unknown) }()
	ctx, at, err := c.copyTargetIn(ctx, p, source, directory)
	if err != nil {
		return destination, tree, err
	}
	tree, err = c.CopyTree(ctx, p, source, at.address(), options)
	if tree.Change.Effect == Applied || tree.Change.Effect == Incomplete {
		destination = at.address()
	}
	return destination, tree, err
}

func (c *Core) copyTargetIn(ctx context.Context, p Principal, source, directory Address) (context.Context, *location, error) {
	if source.Namespace() != TreeNamespace || source.Value() == "/" {
		return ctx, nil, Fail(InvalidRequest, nil)
	}
	ctx, err := c.selectOperation(ctx, AddressAccess{Address: source}, AddressAccess{Address: directory, Write: true})
	if err != nil {
		return ctx, nil, err
	}
	at, err := c.availableChild(ctx, p, directory, path.Base(source.Value()))
	return ctx, at, err
}
