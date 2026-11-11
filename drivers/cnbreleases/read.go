package cnbreleases

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

type asset struct {
	ID, Name string
	Size     *int64
}

func (a asset) metadata() core.Metadata {
	return core.Metadata{Kind: core.File, Name: a.Name, Size: a.Size}
}

func (d *Driver) selectAsset(ctx context.Context, address core.NativeAddress) (asset, error) {
	name := address.Value()[1:]
	if strings.Contains(name, "/") {
		return asset{}, core.Fail(core.NotDirectory, nil)
	}
	items, err := d.assets(ctx)
	if err != nil {
		return asset{}, err
	}
	f, found, err := childscan.Unique(ctx, name, func(context.Context) ([]asset, error) {
		return items, io.EOF
	}, func(f asset) (string, string) {
		return f.ID, f.Name
	})
	if err != nil {
		return asset{}, err
	}
	if !found {
		return asset{}, core.Fail(core.NotFound, nil)
	}
	return f, nil
}

func (d *Driver) Stat(ctx context.Context, a core.NativeAddress, selections ...core.ReadSelection) (core.Metadata, error) {
	if len(selections) > 1 {
		return core.Metadata{}, core.Fail(core.InvalidRequest, nil)
	}
	if len(selections) == 1 && (selections[0].Version != "" || !selections[0].When.Empty()) {
		return core.Metadata{}, core.Fail(core.UnsupportedOperation, nil)
	}
	if err := d.check(ctx, a); err != nil {
		return core.Metadata{}, err
	}
	if a.Value() == "/" {
		if _, err := d.assets(ctx); err != nil {
			return core.Metadata{}, err
		}
		return core.Metadata{Kind: core.Directory}, nil
	}
	f, err := d.selectAsset(ctx, a)
	if err != nil {
		return core.Metadata{}, err
	}
	return f.metadata(), nil
}

func (d *Driver) Enumerate(ctx context.Context, q core.NativeQuery) (core.Iterator, error) {
	if q.Delimiter() != "" || q.StartAfter() != "" {
		return nil, core.Fail(core.UnsupportedOperation, nil)
	}
	a := q.Address()
	if err := d.check(ctx, a); err != nil {
		return nil, err
	}
	if a.Value() != "/" {
		if _, err := d.selectAsset(ctx, a); err != nil {
			return nil, err
		}
		return nil, core.Fail(core.NotDirectory, nil)
	}
	assets, err := d.assets(ctx)
	if err != nil {
		return nil, err
	}
	items := make([]core.Metadata, len(assets))
	for i, f := range assets {
		items[i] = f.metadata()
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
	if err := d.check(ctx, a); err != nil {
		return core.Opened{}, err
	}
	if a.Value() == "/" {
		return core.Opened{}, core.Fail(core.NotFile, nil)
	}
	f, err := d.selectAsset(ctx, a)
	if err != nil {
		return core.Opened{}, err
	}
	target := d.base + "/download/" + url.PathEscape(d.tag) + "/" + url.PathEscape(f.Name)
	byteRange := httpcontent.Header(o.Range)
	response, err := httpcontent.FollowDownload(target, func(target string, delegated bool) (*http.Response, error) {
		if delegated {
			return d.get(ctx, &d.content, target, "", byteRange)
		}
		reply, err := d.get(ctx, &d.api, target, "", "")
		if err != nil {
			return nil, err
		}
		if reply.StatusCode == http.StatusFound {
			return reply, nil
		}
		failure := responseError(reply.StatusCode)
		if reply.StatusCode >= 200 && reply.StatusCode < 400 {
			failure = core.Fail(core.InvalidUpstreamResponse, errors.New("CNB did not supply a download redirect"))
		}
		return nil, errors.Join(failure, d.cleanup.CloseBody(reply.Body))
	}, d.cleanup.CloseBody)
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
