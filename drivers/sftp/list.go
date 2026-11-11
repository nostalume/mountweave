package sftp

import (
	"context"
	"errors"
	"io"
	"math"
	"path"
	"strings"
	"sync"
	"time"

	core "github.com/nostalume/mountweave"
	sshfx "github.com/pkg/sftp/v2/encoding/ssh/filexfer"
)

func childPath(parent, name string) string { return strings.TrimSuffix(parent, "/") + "/" + name }

// resolve admits exact stored components from NAME replies. ATTRS/FSTAT contain
// no name; attaching the request's basename would not detect server aliases.
// This is a path observation, not an atomic NoFollow or fixed-parent guarantee.
func (d *Driver) resolve(c *connection, p string, parent bool) (target string, meta core.Metadata, err error) {
	selected := p
	if parent {
		selected = path.Dir(p)
	}
	target, meta = d.root, core.Metadata{Kind: core.Directory}
	if selected == "/" && !parent {
		attrs, failure := c.wire.attrs(&sshfx.StatPacket{Path: target})
		if failure != nil {
			return target, meta, failure
		}
		meta, err = metadata("", attrs)
		return target, meta, err
	}
	if selected != "/" {
		for _, name := range strings.Split(strings.TrimPrefix(selected, "/"), "/") {
			if meta.Kind != core.Directory {
				return target, meta, core.Fail(core.NotDirectory, nil)
			}
			entry, failure := d.child(c, target, name)
			if failure != nil {
				return target, meta, failure
			}
			meta, err = metadata(entry.Filename, entry.Attrs)
			if err != nil {
				return target, meta, err
			}
			target = childPath(target, entry.Filename)
		}
	}
	if parent {
		if meta.Kind != core.Directory {
			return target, meta, core.Fail(core.NotDirectory, nil)
		}
		target = childPath(target, path.Base(p))
	}
	return target, meta, nil
}

func (d *Driver) child(c *connection, parent, name string) (entry *sshfx.NameEntry, err error) {
	handle, err := c.wire.handle(&sshfx.OpenDirPacket{Path: parent})
	if err != nil {
		return nil, err
	}
	owned := &handle
	defer func() {
		_, failure := closeHandle(c, &owned, d.options.CleanupTimeout)
		err = errors.Join(err, failure)
	}()
	for {
		items, failure := c.wire.names(&sshfx.ReadDirPacket{Handle: handle})
		if failure != nil {
			if failure == io.EOF {
				failure = core.Fail(core.NotFound, nil)
			}
			return nil, failure
		}
		for _, item := range items {
			if item.Filename == name {
				return item, nil
			}
		}
	}
}

func metadata(name string, attrs sshfx.Attributes) (core.Metadata, error) {
	m := core.Metadata{Name: name}
	if !attrs.HasPermissions() {
		return m, core.Fail(core.UnsupportedOperation, errors.New("SFTP did not supply the entry type"))
	}
	switch attrs.Permissions.Type() {
	case sshfx.ModeRegular:
		m.Kind = core.File
		if attrs.HasSize() {
			if attrs.Size > math.MaxInt64 {
				return m, core.Fail(core.InvalidUpstreamResponse, nil)
			}
			size := int64(attrs.Size)
			m.Size = &size
		}
	case sshfx.ModeDir:
		m.Kind = core.Directory
	default:
		return m, core.Fail(core.UnsupportedOperation, errors.New("SFTP entry is not a regular file or directory"))
	}
	if attrs.HasACModTime() {
		modified := time.Unix(int64(attrs.MTime), 0).UTC()
		m.ModifiedAt = &modified
	}
	return m, nil
}

func (d *Driver) Stat(ctx context.Context, p core.NativeAddress, selections ...core.ReadSelection) (meta core.Metadata, err error) {
	if len(selections) > 1 {
		return meta, core.Fail(core.InvalidRequest, nil)
	}
	var selection core.ReadSelection
	if len(selections) == 1 {
		selection = selections[0]
	}
	if selection.Version != "" {
		return meta, core.Fail(core.UnsupportedOperation, nil)
	}
	if err = selection.Check(core.Evidence{}); err != nil && core.CodeOf(err) != core.PreconditionFailed {
		return meta, err
	}
	err = d.admit(p)
	if err != nil {
		return meta, err
	}
	c, err := d.borrow(ctx)
	if err != nil {
		return meta, err
	}
	end := c.scope(ctx)
	defer func() { err = errors.Join(err, end(), d.put(c)) }()
	_, meta, err = d.resolve(c, p.Value(), false)
	if err == nil {
		err = selection.Check(meta.Evidence)
	}
	return meta, err
}

func (d *Driver) Enumerate(ctx context.Context, q core.NativeQuery) (result core.Iterator, err error) {
	if q.Delimiter() != "" || q.StartAfter() != "" {
		return nil, core.Fail(core.UnsupportedOperation, nil)
	}
	err = d.admit(q.Address())
	if err != nil {
		return nil, err
	}
	c, err := d.borrow(ctx)
	if err != nil {
		return nil, err
	}
	end := c.scope(ctx)
	target := d.root
	if q.Address().Value() != "/" {
		var meta core.Metadata
		target, meta, err = d.resolve(c, q.Address().Value(), false)
		if err == nil && meta.Kind != core.Directory {
			err = core.Fail(core.NotDirectory, nil)
		}
	}
	if err != nil {
		return nil, errors.Join(err, end(), d.put(c))
	}
	handle, err := c.wire.handle(&sshfx.OpenDirPacket{Path: target})
	err = errors.Join(err, end())
	if err != nil {
		_ = c.Close()
		return nil, errors.Join(err, d.put(c))
	}
	return &iterator{driver: d, conn: c, handle: &handle}, nil
}

type iterator struct {
	driver   *Driver
	conn     *connection
	handle   *string
	mu       sync.Mutex
	closeMu  sync.Mutex
	done     bool
	closing  bool
	released bool
}

func (i *iterator) Next(ctx context.Context, n int) (items []core.Metadata, err error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	if n < 1 {
		return nil, core.Fail(core.InvalidRequest, nil)
	}
	if i.closing {
		return nil, io.ErrClosedPipe
	}
	if i.done {
		return nil, io.EOF
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	end := i.conn.scope(ctx)
	defer func() {
		if failure := end(); failure != nil {
			err = errors.Join(err, failure)
		}
	}()
	names, err := i.conn.wire.names(&sshfx.ReadDirPacket{Handle: *i.handle})
	if err != nil {
		if err == io.EOF {
			i.done = true
		}
		return nil, err
	}
	items = make([]core.Metadata, 0, len(names))
	for _, entry := range names {
		if entry.Filename == "." || entry.Filename == ".." {
			continue
		}
		if !core.ValidName(entry.Filename) {
			return nil, core.Fail(core.UnrepresentableName, nil)
		}
		meta, failure := metadata(entry.Filename, entry.Attrs)
		if failure != nil {
			return nil, failure
		}
		items = append(items, meta)
	}
	return items, nil
}

func (i *iterator) Close() error {
	i.closeMu.Lock()
	defer i.closeMu.Unlock()
	if i.released {
		return nil
	}
	if !i.mu.TryLock() {
		if err := i.conn.Close(); err != nil {
			return err
		}
		i.mu.Lock()
	}
	defer i.mu.Unlock()
	if i.closing {
		if i.released {
			return nil
		}
		err := i.driver.put(i.conn)
		i.released = err == nil
		return err
	}
	i.closing = true
	_, err := closeHandle(i.conn, &i.handle, i.driver.options.CleanupTimeout)
	failure := i.driver.put(i.conn)
	i.released = failure == nil
	return errors.Join(err, failure)
}
