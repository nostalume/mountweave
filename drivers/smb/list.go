package smb

import (
	"context"
	"errors"
	"io"
	"strings"

	core "github.com/nostalume/mountweave"
)

func nativeChild(parent, name string) string {
	if parent == "" {
		return name
	}
	return parent + "\\" + name
}

func (d *Driver) closeHandle(c *connection, h *handle) (bool, error) {
	if h == nil {
		return false, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), d.options.CleanupTimeout)
	defer cancel()
	end := c.scope(ctx)
	ack, err := h.close(ctx)
	return ack, errors.Join(err, end(), ctx.Err())
}

func ordinary(m core.Metadata) error {
	if m.Kind != core.File && m.Kind != core.Directory {
		return core.Fail(core.UnsupportedOperation, errors.New("SMB entry is not an ordinary file or directory"))
	}
	return nil
}

// find narrows the native query with an admitted literal component, then
// verifies the stored spelling independently of the server's matching rules.
func (d *Driver) find(ctx context.Context, c *connection, parent, name string) (found core.Metadata, err error) {
	h, _, err := c.open(ctx, parent, true, false)
	if h != nil {
		defer func() {
			_, failure := d.closeHandle(c, h)
			err = errors.Join(err, failure)
		}()
	}
	if err != nil {
		return found, err
	}
	for {
		items, failure := h.page(ctx, name, uint32(d.options.PageBytes))
		if failure != nil {
			if failure == io.EOF {
				failure = core.Fail(core.NotFound, nil)
			}
			return found, failure
		}
		for _, item := range items {
			if item.Name == name {
				return item, ordinary(item)
			}
		}
	}
}

// locate returns a native share-relative path assembled from admitted names.
// It observes names, not a fixed-parent or all-component NoFollow guarantee.
func (d *Driver) locate(ctx context.Context, c *connection, p string, parent bool) (target string, meta core.Metadata, err error) {
	target = d.root
	if p == "/" {
		return target, core.Metadata{Kind: core.Directory}, nil
	}
	parts := strings.Split(p[1:], "/")
	for index, name := range parts {
		if parent && index == len(parts)-1 {
			return nativeChild(target, name), core.Metadata{Name: name}, nil
		}
		meta, err = d.find(ctx, c, target, name)
		if err != nil {
			return target, meta, err
		}
		if index < len(parts)-1 && meta.Kind != core.Directory {
			return target, meta, core.Fail(core.NotDirectory, nil)
		}
		target = nativeChild(target, meta.Name)
	}
	return target, meta, nil
}

func selection(values []core.ReadSelection) (core.ReadSelection, error) {
	if len(values) > 1 {
		return core.ReadSelection{}, core.Fail(core.InvalidRequest, nil)
	}
	var value core.ReadSelection
	if len(values) == 1 {
		value = values[0]
	}
	if err := value.When.Validate(); err != nil {
		return value, err
	}
	if value.Version != "" || !value.When.Empty() && !value.When.IsPresent() && !value.When.IsAbsent() {
		return value, core.Fail(core.UnsupportedOperation, nil)
	}
	return value, nil
}

func (d *Driver) Stat(ctx context.Context, p core.NativeAddress, values ...core.ReadSelection) (meta core.Metadata, err error) {
	selected, err := selection(values)
	if err != nil {
		return meta, err
	}
	if err = d.admit(p); err != nil {
		return meta, err
	}
	c, err := d.borrow(ctx)
	if err != nil {
		return meta, err
	}
	end := c.scope(ctx)
	defer func() { err = errors.Join(err, end(), ctx.Err(), d.put(c)) }()
	if p.Value() == "/" {
		var h *handle
		h, meta, err = c.open(ctx, d.root, true, false)
		_, closeErr := d.closeHandle(c, h)
		err = errors.Join(err, closeErr)
	} else {
		_, meta, err = d.locate(ctx, c, p.Value(), false)
	}
	if err == nil {
		err = selected.Check(meta.Evidence)
	}
	return meta, err
}

func (d *Driver) Enumerate(ctx context.Context, query core.NativeQuery) (result core.Iterator, err error) {
	if query.Delimiter() != "" || query.StartAfter() != "" {
		return nil, core.Fail(core.UnsupportedOperation, nil)
	}
	if err = d.admit(query.Address()); err != nil {
		return nil, err
	}
	c, err := d.borrow(ctx)
	if err != nil {
		return nil, err
	}
	end := c.scope(ctx)
	target, meta, err := d.locate(ctx, c, query.Address().Value(), false)
	if err == nil && meta.Kind != core.Directory {
		err = core.Fail(core.NotDirectory, nil)
	}
	var h *handle
	if err == nil {
		h, _, err = c.open(ctx, target, true, false)
	}
	err = errors.Join(err, end(), ctx.Err())
	if err != nil {
		_, closeErr := d.closeHandle(c, h)
		return nil, errors.Join(err, closeErr, d.put(c))
	}
	return &iterator{resource: &resource{driver: d, conn: c, handle: h}}, nil
}

type iterator struct {
	*resource
	eof bool
}

func (i *iterator) Next(ctx context.Context, n int) (items []core.Metadata, err error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	if n < 1 {
		return nil, core.Fail(core.InvalidRequest, nil)
	}
	if i.closing.Load() {
		return nil, io.ErrClosedPipe
	}
	if i.eof {
		return nil, io.EOF
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	end := i.conn.scope(ctx)
	defer func() {
		if failure := errors.Join(end(), ctx.Err()); failure != nil {
			err = errors.Join(err, failure)
		}
	}()
	items, err = i.handle.page(ctx, "*", uint32(i.driver.options.PageBytes))
	if err == io.EOF {
		i.eof = true
	}
	if err == nil {
		for _, item := range items {
			if err = ordinary(item); err != nil {
				return nil, err
			}
			if err = validatePath("/" + item.Name); err != nil {
				return nil, err
			}
		}
	}
	return items, err
}
