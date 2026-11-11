package autoindex

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/internal/childscan"
	"github.com/nostalume/mountweave/drivers/internal/httpcontent"
)

func (d *Driver) resolve(ctx context.Context, a core.NativeAddress) (string, core.Metadata, error) {
	if err := d.check(ctx, a); err != nil {
		return "", core.Metadata{}, err
	}
	target, meta := d.root, core.Metadata{Kind: core.Directory}
	if a.Value() == "/" {
		return target, meta, nil
	}
	for _, name := range strings.Split(a.Value()[1:], "/") {
		if meta.Kind != core.Directory {
			return "", core.Metadata{}, core.Fail(core.NotDirectory, nil)
		}
		items, err := d.list(ctx, target)
		if err != nil {
			return "", core.Metadata{}, err
		}
		f, found, err := childscan.Unique(ctx, name, func(context.Context) ([]item, error) {
			return items, io.EOF
		}, func(f item) (string, string) {
			return f.Name, f.Name
		})
		if err != nil {
			return "", core.Metadata{}, err
		}
		if !found {
			return "", core.Metadata{}, core.Fail(core.NotFound, nil)
		}
		if f.Type == "other" {
			return "", core.Metadata{}, core.Fail(core.UnsupportedOperation, errors.New("autoindex item is neither a directory nor a regular file"))
		}
		target += url.PathEscape(f.Name)
		meta = f.meta
		if meta.Kind == core.Directory {
			target += "/"
		}
	}
	return target, meta, nil
}

func (d *Driver) Stat(ctx context.Context, a core.NativeAddress, selections ...core.ReadSelection) (core.Metadata, error) {
	if len(selections) > 1 {
		return core.Metadata{}, core.Fail(core.InvalidRequest, nil)
	}
	if len(selections) == 1 && (selections[0].Version != "" || !selections[0].When.Empty()) {
		return core.Metadata{}, core.Fail(core.UnsupportedOperation, nil)
	}
	target, meta, err := d.resolve(ctx, a)
	if err == nil && a.Value() == "/" {
		_, err = d.list(ctx, target)
	}
	return meta, err
}

func (d *Driver) Enumerate(ctx context.Context, q core.NativeQuery) (core.Iterator, error) {
	if q.Delimiter() != "" || q.StartAfter() != "" {
		return nil, core.Fail(core.UnsupportedOperation, nil)
	}
	target, dir, err := d.resolve(ctx, q.Address())
	if err != nil {
		return nil, err
	}
	if dir.Kind != core.Directory {
		return nil, core.Fail(core.NotDirectory, nil)
	}
	native, err := d.list(ctx, target)
	if err != nil {
		return nil, err
	}
	items := make([]core.Metadata, 0, len(native))
	for _, f := range native {
		if f.Type != "other" {
			items = append(items, f.meta)
		}
	}
	return childscan.Items(items), nil
}

func (d *Driver) Open(ctx context.Context, a core.NativeAddress, o core.OpenOptions) (core.Opened, error) {
	if err := o.Validate(); err != nil {
		return core.Opened{}, err
	}
	if o.Mode != core.StreamRead || o.NoFollow || o.Selection.Version != "" || !o.Selection.When.Empty() {
		return core.Opened{}, core.Fail(core.UnsupportedOperation, nil)
	}
	target, f, err := d.resolve(ctx, a)
	if err != nil {
		return core.Opened{}, err
	}
	if f.Kind != core.File {
		return core.Opened{}, core.Fail(core.NotFile, nil)
	}
	response, err := d.get(ctx, target, "", httpcontent.Header(o.Range))
	if err != nil {
		return core.Opened{}, err
	}
	switch response.StatusCode {
	case http.StatusOK, http.StatusPartialContent, http.StatusRequestedRangeNotSatisfiable:
		return d.cleanup.OpenBody(response, o.Range, f.Name)
	default:
		return core.Opened{}, errors.Join(responseError(response.StatusCode), d.cleanup.CloseBody(response.Body))
	}
}
