package cloudreve

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"path"
	"strconv"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/internal/httpcontent"
)

func (d *Driver) getItem(ctx context.Context, uri string) (item, error) {
	u, err := parseURI(uri)
	if err != nil {
		return item{}, core.Fail(core.InvalidRequest, err)
	}
	// File info explicitly rejects the account root. Listing supplies its
	// real parent metadata without inventing a synthetic native object.
	if u.Path == "" {
		page, err := d.list(ctx, uri, 0, "", 1)
		if err != nil {
			return item{}, err
		}
		return page.Parent, nil
	}
	query := url.Values{"uri": {uri}}
	response, err := d.apiRequest(ctx, http.MethodGet, d.apiURL("/file/info")+"?"+query.Encode(), nil, "", 0)
	if err != nil {
		return item{}, err
	}
	var f item
	if _, err := d.decode(response, &f); err != nil {
		return item{}, err
	}
	if err := f.at(uri); err != nil {
		return item{}, err
	}
	return f, nil
}

func (f item) at(uri string) error {
	if _, err := f.metadata(); err != nil {
		return err
	}
	want, err := parseURI(uri)
	got, parseErr := parseURI(f.Path)
	if err != nil || parseErr != nil || want.Path != got.Path || (want.Path != "" && f.Name != path.Base(want.Path)) {
		return core.Fail(core.InvalidUpstreamResponse, errors.New("Cloudreve returned a different tree address"))
	}
	return nil
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

// nativeURI translates a path already admitted by the public operation.
func (d *Driver) nativeURI(p string) string {
	u, _ := url.Parse(d.opts.RootURI)
	if p != "/" {
		u.Path += p
		u.RawPath = ""
	}
	return u.String()
}

// The native account navigator refuses intermediate symbolic folders. Match
// its actual address and reject a final shared redirect instead of following it.
func (d *Driver) resolve(ctx context.Context, p string) (item, error) {
	if err := d.admit(ctx, p); err != nil {
		return item{}, err
	}
	return d.getItem(ctx, d.nativeURI(p))
}

type listing struct {
	Files      *[]item `json:"files"`
	Parent     item    `json:"parent"`
	Mixed      bool    `json:"mixed_type"`
	Single     bool    `json:"single_file_view"`
	Pagination *struct {
		Page     int    `json:"page"`
		Size     int    `json:"page_size"`
		Total    *int64 `json:"total_items"`
		Next     string `json:"next_token"`
		IsCursor bool   `json:"is_cursor"`
	} `json:"pagination"`
}

func (d *Driver) list(ctx context.Context, uri string, page int, token string, n int) (listing, error) {
	query := url.Values{"uri": {uri}, "page_size": {strconv.Itoa(n)}, "page": {strconv.Itoa(page)}}
	if token != "" {
		query.Set("next_page_token", token)
	}
	response, err := d.apiRequest(ctx, http.MethodGet, d.apiURL("/file")+"?"+query.Encode(), nil, "", 0)
	if err != nil {
		return listing{}, err
	}
	var result listing
	if _, err := d.decode(response, &result); err != nil {
		return listing{}, err
	}
	if result.Files == nil || result.Pagination == nil || result.Mixed || result.Single {
		return listing{}, core.Fail(core.InvalidUpstreamResponse, nil)
	}
	if err := result.Parent.at(uri); err != nil {
		return listing{}, err
	}
	if *result.Parent.Type != 1 {
		return listing{}, core.Fail(core.InvalidUpstreamResponse, nil)
	}
	return result, nil
}

func (d *Driver) Stat(ctx context.Context, address core.NativeAddress, selections ...core.ReadSelection) (core.Metadata, error) {
	if len(selections) > 1 {
		return core.Metadata{}, core.Fail(core.InvalidRequest, nil)
	}
	if address.Namespace() != core.TreeNamespace || (len(selections) == 1 && selections[0].Version != "") {
		return core.Metadata{}, core.Fail(core.UnsupportedOperation, nil)
	}
	f, err := d.resolve(ctx, address.Value())
	if err != nil {
		return core.Metadata{}, err
	}
	meta, err := f.metadata()
	if address.Value() == "/" {
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
	if *f.Type != 1 {
		return nil, core.Fail(core.NotDirectory, nil)
	}
	return &iterator{driver: d, parent: f}, nil
}

type iterator struct {
	driver       *Driver
	parent       item
	next         string
	page, size   int
	done, closed bool
}

func (i *iterator) Close() error {
	i.closed, i.next, i.parent = true, "", item{}
	return nil
}

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
	if i.size != 0 {
		n = i.size
	}
	result, err := i.driver.list(ctx, i.parent.Path, i.page, i.next, n)
	if err != nil {
		return nil, err
	}
	if result.Parent.ID != i.parent.ID {
		return nil, core.Fail(core.InvalidUpstreamResponse, nil)
	}
	p := result.Pagination
	done := p.Next == ""
	if p.IsCursor {
		if p.Next != "" && p.Next == i.next {
			return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("unchanged Cloudreve continuation"))
		}
	} else {
		if p.Page != i.page || p.Size < 1 || p.Total == nil || *p.Total < 0 || (i.size != 0 && i.size != p.Size) {
			return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("invalid Cloudreve page progress"))
		}
		done = *p.Total == 0 || int64(p.Page) >= (*p.Total-1)/int64(p.Size)
	}
	items := make([]core.Metadata, 0, len(*result.Files))
	base, _ := parseURI(i.parent.Path)
	for _, f := range *result.Files {
		if !core.ValidName(f.Name) {
			return nil, core.Fail(core.UnrepresentableName, nil)
		}
		child := *base
		child.Path, child.RawPath = base.Path+"/"+f.Name, ""
		if err := f.at(child.String()); err != nil {
			return nil, err
		}
		meta, _ := f.metadata()
		items = append(items, meta)
	}
	i.next, i.done = p.Next, done
	if !p.IsCursor && !done {
		i.page++
		i.size = p.Size
	}
	if done {
		return items, io.EOF
	}
	return items, nil
}

func (d *Driver) Open(ctx context.Context, address core.NativeAddress, options core.OpenOptions) (core.Opened, error) {
	if err := options.Validate(); err != nil {
		return core.Opened{}, err
	}
	if address.Namespace() != core.TreeNamespace || options.Mode != core.StreamRead || options.NoFollow || options.Selection.Version != "" {
		return core.Opened{}, core.Fail(core.UnsupportedOperation, nil)
	}
	selectionErr := options.Selection.Check(core.Evidence{})
	if selectionErr != nil && core.CodeOf(selectionErr) != core.PreconditionFailed {
		return core.Opened{}, selectionErr
	}
	f, err := d.resolve(ctx, address.Value())
	if err != nil {
		return core.Opened{}, err
	}
	if *f.Type != 0 {
		return core.Opened{}, core.Fail(core.NotFile, nil)
	}
	if selectionErr != nil {
		return core.Opened{}, selectionErr
	}
	data, _ := json.Marshal(struct {
		URIs     []string `json:"uris"`
		Download bool     `json:"download"`
	}{[]string{f.Path}, true})
	response, err := d.apiRequest(ctx, http.MethodPost, d.apiURL("/file/url"), bytes.NewReader(data), "application/json", int64(len(data)))
	if err != nil {
		return core.Opened{}, err
	}
	var urls struct {
		URLs []struct {
			URL string `json:"url"`
		} `json:"urls"`
	}
	if _, err := d.decode(response, &urls); err != nil {
		return core.Opened{}, err
	}
	if len(urls.URLs) != 1 {
		return core.Opened{}, core.Fail(core.InvalidUpstreamResponse, errors.New("expected one Cloudreve content URL"))
	}
	target := urls.URLs[0].URL
	if err := validContentURL(target); err != nil {
		return core.Opened{}, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return core.Opened{}, core.Fail(core.InvalidUpstreamResponse, err)
	}
	req.Header.Set("Accept-Encoding", "identity")
	if options.Range != nil {
		req.Header.Set("Range", httpcontent.Header(options.Range))
	}
	response, err = d.content.Do(req)
	if err != nil {
		return core.Opened{}, networkError(err)
	}
	if response.StatusCode != http.StatusOK && response.StatusCode != http.StatusPartialContent && response.StatusCode != http.StatusRequestedRangeNotSatisfiable {
		return core.Opened{Metadata: core.Metadata{Name: f.Name, Kind: core.File}, Body: response.Body}, httpFailure(response.StatusCode)
	}
	opened, err := httpcontent.Open(response, options.Range)
	opened.Metadata.Name = f.Name
	return opened, err
}

var _ core.Driver = (*Driver)(nil)
