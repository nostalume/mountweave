package misskey

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/internal/childscan"
	"github.com/nostalume/mountweave/drivers/internal/httpcontent"
)

const nativePage = 100

type item struct {
	ID, Name, URL      string
	ParentID, FolderID json.RawMessage
	kind               core.Kind
}

func (f item) metadata() core.Metadata {
	return core.Metadata{Name: f.Name, Kind: f.kind}
}

type listRequest struct {
	FolderID *string `json:"folderId"`
	UntilID  string  `json:"untilId,omitempty"`
	Limit    int     `json:"limit"`
}

func (d *Driver) page(ctx context.Context, parent, until string, limit int, files bool) ([]item, error) {
	request := listRequest{UntilID: until, Limit: limit}
	if parent != "" {
		request.FolderID = &parent
	}
	route, kind := "/folders", core.Directory
	if files {
		route, kind = "/files", core.File
	}
	var entries []item
	if err := d.list(ctx, route, request, &entries); err != nil {
		return nil, err
	}
	if entries == nil || len(entries) > limit {
		return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("invalid Misskey page"))
	}
	for index := range entries {
		f := &entries[index]
		ref := f.ParentID
		if files {
			ref = f.FolderID
		}
		var nativeParent *string
		if err := json.Unmarshal(ref, &nativeParent); err != nil ||
			(nativeParent == nil && parent != "") || (nativeParent != nil && (*nativeParent != parent || parent == "")) {
			return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("Misskey item left its requested parent"))
		}
		if f.ID == "" || f.ID == until || (index != 0 && f.ID == entries[index-1].ID) {
			return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("Misskey page did not advance"))
		}
		if !core.ValidName(f.Name) {
			return nil, core.Fail(core.UnrepresentableName, nil)
		}
		f.kind = kind
	}
	return entries, nil
}

type children struct {
	driver      *Driver
	parent      string
	until       string
	boundaries  map[string]struct{}
	files, done bool
	closed      bool
}

func (i *children) next(ctx context.Context, limit int) ([]item, error) {
	for !i.done {
		entries, err := i.driver.page(ctx, i.parent, i.until, limit, i.files)
		if err != nil {
			return nil, err
		}
		if len(entries) == limit {
			until := entries[len(entries)-1].ID
			if _, seen := i.boundaries[until]; seen {
				return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("Misskey page boundary repeated"))
			}
			if i.boundaries == nil {
				i.boundaries = make(map[string]struct{})
			}
			i.boundaries[until] = struct{}{}
			i.until = until
		} else if i.files {
			i.done, i.boundaries = true, nil
			return entries, io.EOF
		} else {
			i.files, i.until, i.boundaries = true, "", nil
		}
		if len(entries) != 0 {
			return entries, nil
		}
	}
	return nil, io.EOF
}

func (i *children) Next(ctx context.Context, n int) ([]core.Metadata, error) {
	if i.closed {
		return nil, io.ErrClosedPipe
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if n < 1 {
		return nil, core.Fail(core.InvalidRequest, nil)
	}
	entries, err := i.next(ctx, min(n, nativePage))
	if err != nil && err != io.EOF {
		return nil, err
	}
	metadata := make([]core.Metadata, len(entries))
	for k, f := range entries {
		metadata[k] = f.metadata()
	}
	return metadata, err
}

func (i *children) Close() error {
	i.closed, i.boundaries = true, nil
	return nil
}

func (d *Driver) resolve(ctx context.Context, a core.NativeAddress) (item, error) {
	if err := ctx.Err(); err != nil {
		return item{}, err
	}
	if d.closed.Load() {
		return item{}, core.Fail(core.MountUnavailable, nil)
	}
	if a.Namespace() != core.TreeNamespace {
		return item{}, core.Fail(core.UnsupportedOperation, nil)
	}
	if err := core.ValidatePath(a.Value()); err != nil {
		return item{}, err
	}
	current := item{ID: d.root, kind: core.Directory}
	if a.Value() == "/" {
		return current, nil
	}
	for _, name := range strings.Split(a.Value()[1:], "/") {
		if current.kind != core.Directory {
			return item{}, core.Fail(core.NotDirectory, nil)
		}
		scan := children{driver: d, parent: current.ID}
		found, exists, err := childscan.Unique(ctx, name, func(ctx context.Context) ([]item, error) {
			return scan.next(ctx, nativePage)
		}, func(f item) (string, string) { return string(f.kind) + ":" + f.ID, f.Name })
		if err != nil {
			return item{}, err
		}
		if !exists {
			return item{}, core.Fail(core.NotFound, nil)
		}
		current = found
	}
	return current, nil
}

func (d *Driver) Stat(ctx context.Context, a core.NativeAddress, selections ...core.ReadSelection) (core.Metadata, error) {
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
	if err := selection.When.ForDomain(""); err != nil {
		return core.Metadata{}, err
	}
	f, err := d.resolve(ctx, a)
	if err != nil {
		return core.Metadata{}, err
	}
	return f.metadata(), selection.Check(core.Evidence{})
}

func (d *Driver) Enumerate(ctx context.Context, q core.NativeQuery) (core.Iterator, error) {
	if q.Delimiter() != "" || q.StartAfter() != "" {
		return nil, core.Fail(core.UnsupportedOperation, nil)
	}
	parent, err := d.resolve(ctx, q.Address())
	if err != nil {
		return nil, err
	}
	if parent.kind != core.Directory {
		return nil, core.Fail(core.NotDirectory, nil)
	}
	return &children{driver: d, parent: parent.ID}, nil
}

func (d *Driver) Open(ctx context.Context, a core.NativeAddress, o core.OpenOptions) (core.Opened, error) {
	if err := o.Validate(); err != nil {
		return core.Opened{}, err
	}
	if o.Mode != core.StreamRead || o.NoFollow || o.Selection.Version != "" {
		return core.Opened{}, core.Fail(core.UnsupportedOperation, nil)
	}
	if err := o.Selection.When.ForDomain(""); err != nil {
		return core.Opened{}, err
	}
	f, err := d.resolve(ctx, a)
	if err != nil {
		return core.Opened{}, err
	}
	if f.kind != core.File {
		return core.Opened{}, core.Fail(core.NotFile, nil)
	}
	if err := o.Selection.Check(core.Evidence{}); err != nil {
		return core.Opened{}, err
	}
	if len(d.origins) == 0 {
		return core.Opened{}, core.Fail(core.UnsupportedOperation, errors.New("no Misskey content origin is configured"))
	}
	response, err := httpcontent.FollowDownload(f.URL, func(raw string, _ bool) (*http.Response, error) {
		target, err := url.Parse(raw)
		if err != nil || !httpsURL(target) {
			return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("invalid Misskey content URL"))
		}
		if _, allowed := d.origins[origin(target)]; !allowed {
			return nil, core.Fail(core.UpstreamAccessDenied, errors.New("Misskey content origin is not allowed"))
		}
		return d.request(ctx, &d.content, http.MethodGet, raw, nil, o.Range)
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
