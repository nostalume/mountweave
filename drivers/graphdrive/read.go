package graphdrive

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/internal/httpcontent"
)

func (d *Driver) getItem(ctx context.Context, route string) (item, error) {
	response, err := d.apiRequest(ctx, http.MethodGet, d.apiURL(route), nil, "", 0)
	if err != nil {
		return item{}, err
	}
	if response.StatusCode != http.StatusOK {
		return item{}, errors.Join(d.responseFailure(response))
	}
	var f item
	if _, err := d.decode(response, &f); err != nil {
		return item{}, err
	}
	if _, err := f.metadata(); err != nil {
		return item{}, err
	}
	if d.readOnly && !core.ValidName(f.ID) {
		return item{}, core.Fail(core.InvalidUpstreamResponse, errors.New("invalid Graph item identity"))
	}
	if f.Parent != nil && f.Parent.DriveID != "" && d.opts.DriveID != "" && f.Parent.DriveID != d.opts.DriveID {
		return item{}, core.Fail(core.InvalidUpstreamResponse, nil)
	}
	return f, nil
}

func (d *Driver) admit(ctx context.Context, p string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if d.closed.Load() {
		return core.Fail(core.MountUnavailable, nil)
	}
	return core.ValidatePath(p)
}

// Each native lookup may ignore case. Verify every stored component before
// using its ID, rather than validating only the last name of a whole path.
func (d *Driver) resolve(ctx context.Context, p string) (item, error) {
	if err := d.admit(ctx, p); err != nil {
		return item{}, err
	}
	current := d.root
	if p == "/" {
		return current, nil
	}
	for _, name := range strings.Split(strings.TrimPrefix(p, "/"), "/") {
		if current.Folder == nil {
			return item{}, core.Fail(core.NotDirectory, nil)
		}
		f, err := d.getItem(ctx, "/drives/"+url.PathEscape(d.opts.DriveID)+"/items/"+url.PathEscape(current.ID)+":/"+url.PathEscape(name))
		if err != nil {
			return item{}, err
		}
		if f.Name != name {
			return item{}, core.Fail(core.NotFound, nil)
		}
		if f.Parent != nil && f.Parent.ID != "" && f.Parent.ID != current.ID {
			return item{}, core.Fail(core.InvalidUpstreamResponse, nil)
		}
		current = f
	}
	return current, nil
}

func (d *Driver) Stat(ctx context.Context, p core.NativeAddress, selections ...core.ReadSelection) (core.Metadata, error) {
	if len(selections) > 1 {
		return core.Metadata{}, core.Fail(core.InvalidRequest, nil)
	}
	if p.Namespace() != core.TreeNamespace {
		return core.Metadata{}, core.Fail(core.UnsupportedOperation, nil)
	}
	if len(selections) == 1 && selections[0].Version != "" {
		return core.Metadata{}, core.Fail(core.UnsupportedOperation, nil)
	}
	f, err := d.resolve(ctx, p.Value())
	if err != nil {
		return core.Metadata{}, err
	}
	meta, err := f.metadata()
	if p.Value() == "/" {
		meta = core.Metadata{Kind: core.Directory}
	}
	if err == nil && len(selections) == 1 {
		err = selections[0].Check(meta.Evidence)
	}
	return meta, err
}

func (d *Driver) Enumerate(ctx context.Context, query core.NativeQuery) (core.Iterator, error) {
	if query.Address().Namespace() != core.TreeNamespace || query.Delimiter() != "" || query.StartAfter() != "" {
		return nil, core.Fail(core.UnsupportedOperation, nil)
	}
	f, err := d.resolve(ctx, query.Address().Value())
	if err != nil {
		return nil, err
	}
	if f.Folder == nil {
		return nil, core.Fail(core.NotDirectory, nil)
	}
	return &iterator{driver: d, parent: f.ID}, nil
}

type iterator struct {
	driver       *Driver
	parent, next string
	done, closed bool
}

func (i *iterator) Close() error { i.closed, i.next = true, ""; return nil }
func (i *iterator) Next(ctx context.Context, n int) ([]core.Metadata, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if i.closed {
		return nil, io.ErrClosedPipe
	}
	if i.done {
		return nil, io.EOF
	}
	if n < 1 {
		return nil, core.Fail(core.InvalidRequest, nil)
	}
	resource := i.driver.apiURL("/drives/" + url.PathEscape(i.driver.opts.DriveID) + "/items/" + url.PathEscape(i.parent) + "/children")
	target := i.next
	if target == "" {
		u, _ := url.Parse(resource)
		query := u.Query()
		query.Set("$top", strconv.Itoa(n))
		u.RawQuery = query.Encode()
		target = u.String()
	}
	response, err := i.driver.apiRequest(ctx, http.MethodGet, target, nil, "", 0)
	if err != nil {
		return nil, err
	}
	if response.StatusCode != http.StatusOK {
		return nil, errors.Join(i.driver.responseFailure(response))
	}
	var page struct {
		Items *[]item `json:"value"`
		Next  string  `json:"@odata.nextLink"`
	}
	if _, err := i.driver.decode(response, &page); err != nil {
		return nil, err
	}
	if page.Items == nil || (page.Next != "" && page.Next == target) {
		return nil, core.Fail(core.InvalidUpstreamResponse, nil)
	}
	if page.Next != "" {
		next, err := url.Parse(page.Next)
		base, _ := url.Parse(resource)
		if err != nil || next.Scheme != base.Scheme || !strings.EqualFold(next.Host, base.Host) || next.User != nil || next.Fragment != "" || next.EscapedPath() != base.EscapedPath() {
			return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("invalid children continuation"))
		}
	}
	items := make([]core.Metadata, 0, len(*page.Items))
	for _, f := range *page.Items {
		if !core.ValidName(f.Name) {
			return nil, core.Fail(core.UnrepresentableName, nil)
		}
		if f.Parent != nil && (f.Parent.ID != "" && f.Parent.ID != i.parent || f.Parent.DriveID != "" && f.Parent.DriveID != i.driver.opts.DriveID) {
			return nil, core.Fail(core.InvalidUpstreamResponse, nil)
		}
		meta, err := f.metadata()
		if err != nil {
			return nil, err
		}
		items = append(items, meta)
	}
	i.next, i.done = page.Next, page.Next == ""
	if i.done {
		return items, io.EOF
	}
	return items, nil
}

func (d *Driver) Open(ctx context.Context, p core.NativeAddress, options core.OpenOptions) (core.Opened, error) {
	if err := options.Validate(); err != nil {
		return core.Opened{}, err
	}
	if p.Namespace() != core.TreeNamespace || options.Mode != core.StreamRead || options.Selection.Version != "" {
		return core.Opened{}, core.Fail(core.UnsupportedOperation, nil)
	}
	selectionErr := options.Selection.Check(core.Evidence{})
	if selectionErr != nil && core.CodeOf(selectionErr) != core.PreconditionFailed {
		return core.Opened{}, selectionErr
	}
	f, err := d.resolve(ctx, p.Value())
	if err != nil {
		return core.Opened{}, err
	}
	if f.File == nil {
		return core.Opened{}, core.Fail(core.NotFile, nil)
	}
	if selectionErr != nil {
		return core.Opened{}, selectionErr
	}
	target := f.DownloadURL
	if target == "" {
		response, err := d.apiRequest(ctx, http.MethodGet, d.apiURL("/drives/"+url.PathEscape(d.opts.DriveID)+"/items/"+url.PathEscape(f.ID)+"/content"), nil, "", 0)
		if err != nil {
			return core.Opened{}, privateFailure(err)
		}
		if response.StatusCode != http.StatusFound {
			return core.Opened{}, errors.Join(d.responseFailure(response))
		}
		target = response.Header.Get("Location")
		if closeErr := d.privateResponses.CloseBody(response.Body); closeErr != nil {
			return core.Opened{}, closeErr
		}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil || req.URL.Scheme != "https" || req.URL.Hostname() == "" || req.URL.User != nil || req.URL.Fragment != "" {
		return core.Opened{}, core.Fail(core.InvalidUpstreamResponse, errors.New("invalid content target"))
	}
	req.Header.Set("Accept-Encoding", "identity")
	if options.Range != nil {
		req.Header.Set("Range", httpcontent.Header(options.Range))
	}
	response, err := d.content.Do(req)
	if err != nil {
		return core.Opened{}, privateFailure(networkError(err))
	}
	switch response.StatusCode {
	case http.StatusOK, http.StatusPartialContent, http.StatusRequestedRangeNotSatisfiable:
		return d.privateResponses.OpenBody(response, options.Range, f.Name)
	default:
		return core.Opened{}, errors.Join(statusError(response.StatusCode), d.privateResponses.CloseBody(response.Body))
	}
}

var _ core.Driver = (*Driver)(nil)
