package imgbed

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"strings"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/internal/childscan"
	"github.com/nostalume/mountweave/drivers/internal/httpcontent"
)

func (d *Driver) resolve(ctx context.Context, address core.NativeAddress) (string, core.Metadata, error) {
	if err := d.check(ctx); err != nil {
		return "", core.Metadata{}, err
	}
	if address.Namespace() != core.TreeNamespace {
		return "", core.Metadata{}, core.Fail(core.UnsupportedOperation, nil)
	}
	if err := core.ValidatePath(address.Value()); err != nil {
		return "", core.Metadata{}, err
	}
	key, meta := d.root, core.Metadata{Kind: core.Directory}
	if address.Value() == "/" {
		return key, meta, nil
	}
	for _, name := range strings.Split(address.Value()[1:], "/") {
		if meta.Kind != core.Directory {
			return "", core.Metadata{}, core.Fail(core.NotDirectory, nil)
		}
		children := iterator{driver: d, parent: directoryPrefix(key)}
		item, found, err := childscan.Unique(ctx, name, func(ctx context.Context) ([]core.Metadata, error) {
			return children.Next(ctx, core.DefaultPage)
		}, func(item core.Metadata) (string, string) { return string(item.Kind), item.Name })
		if err != nil {
			return "", core.Metadata{}, err
		}
		if !found {
			return "", core.Metadata{}, core.Fail(core.NotFound, nil)
		}
		key, meta = children.parent+item.Name, item
	}
	return key, meta, nil
}

func (d *Driver) Stat(ctx context.Context, a core.NativeAddress, selections ...core.ReadSelection) (core.Metadata, error) {
	if len(selections) > 1 {
		return core.Metadata{}, core.Fail(core.InvalidRequest, nil)
	}
	if len(selections) == 1 && (selections[0].Version != "" || !selections[0].When.Empty()) {
		return core.Metadata{}, core.Fail(core.UnsupportedOperation, nil)
	}
	_, meta, err := d.resolve(ctx, a)
	return meta, err
}

func (d *Driver) Enumerate(ctx context.Context, q core.NativeQuery) (core.Iterator, error) {
	if q.Delimiter() != "" || q.StartAfter() != "" {
		return nil, core.Fail(core.UnsupportedOperation, nil)
	}
	key, meta, err := d.resolve(ctx, q.Address())
	if err != nil {
		return nil, err
	}
	if meta.Kind != core.Directory {
		return nil, core.Fail(core.NotDirectory, nil)
	}
	return &iterator{driver: d, parent: directoryPrefix(key)}, nil
}

func (d *Driver) Open(ctx context.Context, a core.NativeAddress, o core.OpenOptions) (core.Opened, error) {
	if err := o.Validate(); err != nil {
		return core.Opened{}, err
	}
	if o.Mode != core.StreamRead || o.Selection.Version != "" || !o.Selection.When.Empty() {
		return core.Opened{}, core.Fail(core.UnsupportedOperation, nil)
	}
	key, meta, err := d.resolve(ctx, a)
	if err != nil {
		return core.Opened{}, err
	}
	if meta.Kind != core.File {
		return core.Opened{}, core.Fail(core.NotFile, nil)
	}
	route := (&url.URL{Path: "/file/" + key}).EscapedPath()
	response, err := d.get(ctx, &d.content, route, httpcontent.Header(o.Range))
	if err != nil {
		return core.Opened{}, err
	}
	switch response.StatusCode {
	case http.StatusOK, http.StatusPartialContent, http.StatusRequestedRangeNotSatisfiable:
		return d.cleanup.OpenBody(response, o.Range, meta.Name)
	default:
		return core.Opened{}, errors.Join(responseError(response.StatusCode), d.cleanup.CloseBody(response.Body))
	}
}
