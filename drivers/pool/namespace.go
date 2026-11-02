package pool

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"io"
	"path"
	"strings"
	"sync"

	core "github.com/nostalume/mountweave"
	bolt "go.etcd.io/bbolt"
)

type entryRecord struct {
	ID, Generation string
	Directory      bool
}

func entryKey(parent, name string) []byte { return []byte(parent + "\x00" + name) }
func lookup(tx *bolt.Tx, p string) (entryRecord, error) {
	e := entryRecord{ID: "/", Directory: true}
	if p == "/" {
		return e, nil
	}
	for _, name := range strings.Split(p[1:], "/") {
		if !e.Directory {
			return e, core.Fail(core.NotDirectory, nil)
		}
		if tx.Bucket(dirsKey).Get([]byte(e.ID)) == nil {
			return e, core.Fail(core.NotFound, nil)
		}
		raw := tx.Bucket(entriesKey).Get(entryKey(e.ID, name))
		if raw == nil {
			return e, core.Fail(core.NotFound, nil)
		}
		if err := decode(raw, &e); err != nil {
			return e, err
		}
	}
	return e, nil
}
func parentAt(tx *bolt.Tx, p string) (string, string, error) {
	if p == "/" {
		return "", "", core.Fail(core.InvalidRequest, nil)
	}
	e, err := lookup(tx, path.Dir(p))
	if err != nil {
		return "", "", err
	}
	if !e.Directory {
		return "", "", core.Fail(core.NotDirectory, nil)
	}
	return e.ID, path.Base(p), nil
}
func fileAt(tx *bolt.Tx, e entryRecord) (f fileRecord, err error) {
	if e.Directory {
		return f, core.Fail(core.NotFile, nil)
	}
	err = decode(tx.Bucket(filesKey).Get([]byte(e.Generation)), &f)
	return
}
func markUnreferenced(tx *bolt.Tx, gen string) error {
	if err := tx.Bucket(refsKey).Delete([]byte(gen)); err != nil {
		return err
	}
	return tx.Bucket(garbageKey).Put([]byte(gen), []byte{1})
}

// commit serializes namespace changes with reader pinning and maintenance claims.
// No native I/O or borrowed input runs while state or a transaction is held.
func (d *Driver) commit(ctx context.Context, run func(*bolt.Tx) error) (core.Change, error) {
	d.state.Lock()
	defer d.state.Unlock()
	return d.commitLocked(ctx, run)
}
func (d *Driver) commitLocked(ctx context.Context, run func(*bolt.Tx) error) (core.Change, error) {
	var refusal error
	err := d.update(func(tx *bolt.Tx) error {
		if refusal = errors.Join(ctx.Err(), d.life.Err()); refusal != nil {
			return refusal
		}
		refusal = run(tx)
		return refusal
	})
	effect := core.Applied
	if err != nil {
		effect = core.Unknown
		if refusal != nil {
			effect = core.Unchanged
		}
	}
	return core.Change{Effect: effect}, errors.Join(err, ctx.Err())
}
func (d *Driver) Stat(ctx context.Context, p string) (meta core.Metadata, err error) {
	if err = core.ValidatePath(p); err != nil {
		return
	}
	o, err := d.begin(ctx)
	if err != nil {
		return meta, err
	}
	defer o.finish()
	err = d.db.View(func(tx *bolt.Tx) error {
		e, err := lookup(tx, p)
		if err != nil {
			return err
		}
		var f fileRecord
		if !e.Directory {
			if f, err = fileAt(tx, e); err != nil {
				return err
			}
		}
		meta = metadata(path.Base(p), e, f)
		return o.ctx.Err()
	})
	return
}
func (d *Driver) Mkdir(ctx context.Context, p string) (core.Change, error) {
	if err := core.ValidatePath(p); err != nil {
		return core.Change{Effect: core.Unchanged}, err
	}
	o, err := d.begin(ctx)
	if err != nil {
		return core.Change{Effect: core.Unchanged}, err
	}
	defer o.finish()
	return d.commit(o.ctx, func(tx *bolt.Tx) error {
		parent, name, err := parentAt(tx, p)
		if err != nil {
			return err
		}
		key := entryKey(parent, name)
		if tx.Bucket(entriesKey).Get(key) != nil {
			return core.Fail(core.AlreadyExists, nil)
		}
		e := entryRecord{ID: rand.Text(), Directory: true}
		if err := tx.Bucket(dirsKey).Put([]byte(e.ID), []byte(parent)); err != nil {
			return err
		}
		return tx.Bucket(entriesKey).Put(key, encode(e))
	})
}
func unlink(tx *bolt.Tx, parent, name string, expected core.Kind) error {
	if tx.Bucket(dirsKey).Get([]byte(parent)) == nil {
		return core.Fail(core.NotFound, nil)
	}
	key := entryKey(parent, name)
	raw := tx.Bucket(entriesKey).Get(key)
	if raw == nil {
		return core.Fail(core.NotFound, nil)
	}
	var e entryRecord
	if err := decode(raw, &e); err != nil {
		return err
	}
	if (expected == core.File && e.Directory) || (expected == core.Directory && !e.Directory) {
		return core.Fail(core.InvalidRequest, nil)
	}
	if e.Directory {
		prefix := entryKey(e.ID, "")
		k, _ := tx.Bucket(entriesKey).Cursor().Seek(prefix)
		if bytes.HasPrefix(k, prefix) {
			return core.Fail(core.DirectoryNotEmpty, nil)
		}
		if err := tx.Bucket(dirsKey).Delete([]byte(e.ID)); err != nil {
			return err
		}
	} else if err := markUnreferenced(tx, e.Generation); err != nil {
		return err
	}
	return tx.Bucket(entriesKey).Delete(key)
}
func (d *Driver) Remove(ctx context.Context, p string) (core.Change, error) {
	if err := core.ValidatePath(p); err != nil {
		return core.Change{Effect: core.Unchanged}, err
	}
	o, err := d.begin(ctx)
	if err != nil {
		return core.Change{Effect: core.Unchanged}, err
	}
	defer o.finish()
	return d.commit(o.ctx, func(tx *bolt.Tx) error {
		parent, name, err := parentAt(tx, p)
		if err != nil {
			return err
		}
		return unlink(tx, parent, name, "")
	})
}
func (d *Driver) Relocate(ctx context.Context, from, to string) (core.Change, error) {
	for _, p := range []string{from, to} {
		if err := core.ValidatePath(p); err != nil {
			return core.Change{Effect: core.Unchanged}, err
		}
	}
	o, err := d.begin(ctx)
	if err != nil {
		return core.Change{Effect: core.Unchanged}, err
	}
	defer o.finish()
	return d.commit(o.ctx, func(tx *bolt.Tx) error {
		oldParent, oldName, err := parentAt(tx, from)
		if err != nil {
			return err
		}
		newParent, newName, err := parentAt(tx, to)
		if err != nil {
			return err
		}
		oldKey, newKey := entryKey(oldParent, oldName), entryKey(newParent, newName)
		if bytes.Equal(oldKey, newKey) {
			return core.Fail(core.InvalidRequest, nil)
		}
		b := tx.Bucket(entriesKey)
		raw := b.Get(oldKey)
		if raw == nil {
			return core.Fail(core.NotFound, nil)
		}
		if b.Get(newKey) != nil {
			return core.Fail(core.AlreadyExists, nil)
		}
		var e entryRecord
		if err := decode(raw, &e); err != nil {
			return err
		}
		if e.Directory {
			for parent, depth := newParent, 0; ; depth++ {
				if parent == e.ID {
					return core.Fail(core.InvalidRequest, nil)
				}
				if parent == "/" {
					break
				}
				if depth > core.MaxPathSegments {
					return core.Fail(core.InvalidUpstreamResponse, nil)
				}
				parent = string(tx.Bucket(dirsKey).Get([]byte(parent)))
				if parent == "" {
					return core.Fail(core.InvalidUpstreamResponse, nil)
				}
			}
			if err := tx.Bucket(dirsKey).Put([]byte(e.ID), []byte(newParent)); err != nil {
				return err
			}
		}
		if err := b.Put(newKey, raw); err != nil {
			return err
		}
		return b.Delete(oldKey)
	})
}

type iterator struct {
	mu            sync.Mutex
	op            *operation
	parent, last  string
	seen          map[string]core.Kind
	closed, ended bool
	stop          func() bool
}

func (d *Driver) enumerate(ctx context.Context, p string, track bool) (*iterator, error) {
	if err := core.ValidatePath(p); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	o, err := d.begin(context.Background())
	if err != nil {
		return nil, err
	}
	var parent string
	err = d.db.View(func(tx *bolt.Tx) error {
		e, err := lookup(tx, p)
		if err != nil {
			return err
		}
		if !e.Directory {
			return core.Fail(core.NotDirectory, nil)
		}
		parent = e.ID
		return ctx.Err()
	})
	if err != nil {
		o.finish()
		return nil, err
	}
	i := &iterator{op: o, parent: parent}
	if track {
		i.seen = make(map[string]core.Kind)
	}
	i.mu.Lock()
	i.stop = context.AfterFunc(o.ctx, func() { _ = i.Close() })
	i.mu.Unlock()
	return i, nil
}
func (d *Driver) Enumerate(ctx context.Context, p string) (core.Iterator, error) {
	return d.enumerate(ctx, p, false)
}
func (d *Driver) OpenDirectory(ctx context.Context, p string) (core.DirectoryScope, error) {
	return d.enumerate(ctx, p, true)
}
func (i *iterator) Next(ctx context.Context, limit int) ([]core.Metadata, error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	if err := errors.Join(ctx.Err(), i.op.ctx.Err()); err != nil {
		return nil, err
	}
	if i.closed {
		return nil, io.ErrClosedPipe
	}
	if i.ended {
		return nil, io.EOF
	}
	if limit < 1 {
		return nil, core.Fail(core.InvalidRequest, nil)
	}
	var items []core.Metadata
	err := i.op.d.db.View(func(tx *bolt.Tx) error {
		if tx.Bucket(dirsKey).Get([]byte(i.parent)) == nil {
			return core.Fail(core.NotFound, nil)
		}
		prefix := entryKey(i.parent, "")
		c := tx.Bucket(entriesKey).Cursor()
		k, v := c.Seek(entryKey(i.parent, i.last))
		if i.last != "" && bytes.Equal(k, entryKey(i.parent, i.last)) {
			k, v = c.Next()
		}
		for ; bytes.HasPrefix(k, prefix) && len(items) < limit; k, v = c.Next() {
			if err := errors.Join(ctx.Err(), i.op.ctx.Err()); err != nil {
				return err
			}
			var e entryRecord
			if err := decode(v, &e); err != nil {
				return err
			}
			var f fileRecord
			var err error
			if !e.Directory {
				if f, err = fileAt(tx, e); err != nil {
					return err
				}
			}
			i.last = string(k[len(prefix):])
			meta := metadata(i.last, e, f)
			if i.seen != nil {
				i.seen[i.last] = meta.Kind
			}
			items = append(items, meta)
		}
		i.ended = !bytes.HasPrefix(k, prefix)
		return nil
	})
	if err == nil && i.ended {
		err = io.EOF
	}
	return items, err
}
func (i *iterator) OpenFile(ctx context.Context, name string) (core.Opened, error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.closed || !i.ended || i.seen[name] != core.File || !core.ValidName(name) {
		return core.Opened{}, core.Fail(core.InvalidRequest, nil)
	}
	return i.op.d.open(ctx, name, nil, func(tx *bolt.Tx) (entryRecord, error) {
		var e entryRecord
		if tx.Bucket(dirsKey).Get([]byte(i.parent)) == nil {
			return e, core.Fail(core.NotFound, nil)
		}
		raw := tx.Bucket(entriesKey).Get(entryKey(i.parent, name))
		if raw == nil {
			return e, core.Fail(core.NotFound, nil)
		}
		err := decode(raw, &e)
		return e, err
	})
}
func (i *iterator) RemoveChild(ctx context.Context, name string, kind core.Kind) (core.Change, error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.closed || !core.ValidName(name) || (kind != core.File && kind != core.Directory) {
		return core.Change{Effect: core.Unchanged}, core.Fail(core.InvalidRequest, nil)
	}
	return i.op.d.commit(ctx, func(tx *bolt.Tx) error { return unlink(tx, i.parent, name, kind) })
}
func (i *iterator) Close() error {
	i.mu.Lock()
	defer i.mu.Unlock()
	if !i.closed {
		i.closed = true
		i.stop()
		i.op.finish()
	}
	return nil
}

var _ core.DirectoryMaker = (*Driver)(nil)
var _ core.Remover = (*Driver)(nil)
var _ core.Relocator = (*Driver)(nil)
var _ core.DirectoryReader = (*Driver)(nil)
var _ core.ChildRemover = (*iterator)(nil)
