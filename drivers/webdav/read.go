package webdav

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/internal/httpcontent"
)

func (d *Driver) stat(ctx context.Context, target *url.URL) (meta core.Metadata, err error) {
	i := d.enumeration(ctx, target, "0")
	defer func() { err = errors.Join(err, d.closeBody(i)) }()
	items, failure := i.Next(ctx, 2)
	if failure != nil && failure != io.EOF {
		return meta, failure
	}
	if failure != io.EOF || len(items) != 1 {
		return meta, core.Fail(core.InvalidUpstreamResponse, nil)
	}
	return items[0], nil
}

func (d *Driver) Stat(ctx context.Context, p core.NativeAddress, selections ...core.ReadSelection) (core.Metadata, error) {
	if len(selections) > 1 {
		return core.Metadata{}, core.Fail(core.InvalidRequest, nil)
	}
	var selection core.ReadSelection
	if len(selections) == 1 {
		selection = selections[0]
	}
	if selection.Version != "" {
		return core.Metadata{}, core.Fail(core.UnsupportedOperation, nil)
	}
	target, err := d.target(p)
	if err != nil {
		return core.Metadata{}, err
	}
	if err := selection.When.ForDomain(""); err != nil {
		return core.Metadata{}, err
	}
	meta, err := d.stat(ctx, target)
	if err == nil {
		err = selection.Check(meta.Evidence)
	}
	return meta, err
}

func (d *Driver) Open(ctx context.Context, p core.NativeAddress, options core.OpenOptions) (core.Opened, error) {
	if err := options.Validate(); err != nil {
		return core.Opened{}, err
	}
	if options.Mode != core.StreamRead || options.NoFollow || options.Selection.Version != "" {
		return core.Opened{}, core.Fail(core.UnsupportedOperation, nil)
	}
	target, err := d.target(p)
	if err != nil {
		return core.Opened{}, err
	}
	if err = options.Selection.When.ForDomain(""); err != nil {
		return core.Opened{}, err
	}
	meta, err := d.stat(ctx, target)
	if err != nil {
		return core.Opened{}, err
	}
	if meta.Kind != core.File {
		return core.Opened{}, core.Fail(core.NotFile, nil)
	}
	headers := make(http.Header)
	if options.Range != nil {
		headers.Set("Range", httpcontent.Header(options.Range))
	}
	if value := options.Selection.When.Match(); value != "" {
		headers.Set("If-Match", value)
	}
	if value := options.Selection.When.NotMatch(); value != "" {
		headers.Set("If-None-Match", value)
	}
	response, err := d.request(ctx, http.MethodGet, target, nil, headers)
	if err != nil {
		return core.Opened{}, err
	}
	opened := core.Opened{Metadata: core.Metadata{Name: meta.Name, Kind: core.File}, Body: response.Body}
	if response.StatusCode != 200 && response.StatusCode != 206 && response.StatusCode != 416 {
		if response.StatusCode == http.StatusNotModified {
			evidence := core.Evidence{Validator: core.Validator{Tag: response.Header.Get("ETag")}}
			return opened, core.Fail(core.PreconditionFailed, &core.ConditionFailure{Negative: true, Evidence: evidence})
		}
		return opened, statusError(http.MethodGet, response.StatusCode)
	}
	opened, err = httpcontent.Open(response, options.Range)
	opened.Metadata.Name = meta.Name
	if tag := response.Header.Get("ETag"); tag != "" {
		opened.Metadata.Evidence.Validator.Tag = tag
	}
	if err == nil {
		err = options.Selection.Check(opened.Metadata.Evidence)
	}
	return opened, err
}
