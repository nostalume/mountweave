package huaweidrive

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/internal/childscan"
	"github.com/nostalume/mountweave/drivers/internal/httpcontent"
)

const (
	rootAlias  = "root"
	folderMIME = "application/vnd.huawei-apps.folder"
	listFields = "category,nextCursor,files/id,files/fileName,files/mimeType,files/parentFolder,files/recycled,files/containers,files/ownedByMe,files/size,files/editedTime"
)

type item struct {
	ID         string     `json:"id"`
	Name       string     `json:"fileName"`
	MIME       string     `json:"mimeType"`
	Parents    []string   `json:"parentFolder"`
	Containers []string   `json:"containers"`
	Recycled   bool       `json:"recycled"`
	Owned      *bool      `json:"ownedByMe"`
	Size       *int64     `json:"size"`
	Modified   *time.Time `json:"editedTime"`
	meta       core.Metadata
}

type listing struct {
	driver       *Driver
	parent, name string
	rootParent   string
	pageSize     int
	cursor       string
	seen         map[string]struct{}
	done, closed bool
}

func validID(id string) bool {
	return id != "" && id != "." && id != ".." && !strings.ContainsAny(id, "'/\\\r\n\x00")
}

func (l *listing) next(ctx context.Context) ([]item, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if l.done {
		return nil, io.EOF
	}
	filter := "'" + l.parent + "' in parentFolder and recycled = false"
	// Huawei documents quoted literals but no escape syntax. Names with a
	// quote still resolve through the same exact scan of the selected parent.
	if l.name != "" && !strings.ContainsAny(l.name, "'\\") {
		filter += " and fileName = '" + l.name + "'"
	}
	pageSize := l.pageSize
	if pageSize == 0 {
		pageSize = 100
	}
	q := url.Values{"containers": {"drive"}, "queryParam": {filter}, "pageSize": {strconv.Itoa(pageSize)}, "fields": {listFields}}
	if l.cursor != "" {
		q.Set("cursor", l.cursor)
	}
	var reply struct {
		Category   string
		NextCursor string
		Files      []item
	}
	if err := l.driver.getJSON(ctx, filesURL+"?"+q.Encode(), &reply); err != nil {
		return nil, err
	}
	if reply.Category != "drive#fileList" || len(reply.Files) > 100 {
		return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("invalid HUAWEI Drive page"))
	}
	if _, repeated := l.seen[reply.NextCursor]; reply.NextCursor != "" && repeated {
		return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("HUAWEI Drive repeated a list cursor"))
	}
	parent := l.parent
	if parent == rootAlias {
		parent = l.rootParent
	}
	for i := range reply.Files {
		child := &reply.Files[i]
		if !validID(child.ID) || child.ID == rootAlias || child.ID == "applicationData" || child.ID == l.parent ||
			child.MIME == "" || child.Recycled || len(child.Parents) == 0 ||
			len(child.Containers) != 0 && !slices.Contains(child.Containers, "drive") {
			return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("invalid HUAWEI Drive child"))
		}
		if parent == "" {
			if len(child.Parents) != 1 || !validID(child.Parents[0]) || child.Parents[0] == "applicationData" {
				return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("HUAWEI Drive did not identify the ordinary root parent"))
			}
			parent = child.Parents[0]
		}
		if child.ID == parent || !slices.Contains(child.Parents, parent) {
			return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("HUAWEI Drive returned an unrelated child"))
		}
		if !core.ValidName(child.Name) {
			return nil, core.Fail(core.UnrepresentableName, nil)
		}
		child.meta = core.Metadata{Name: child.Name, Kind: core.File, Size: child.Size, ModifiedAt: child.Modified}
		if child.MIME == folderMIME {
			child.meta.Kind, child.meta.Size = core.Directory, nil
		} else if child.Size != nil && *child.Size < 0 {
			return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("negative HUAWEI Drive file size"))
		}
	}
	if l.parent == rootAlias {
		l.rootParent = parent
	}
	l.cursor, l.done = reply.NextCursor, reply.NextCursor == ""
	if l.done {
		l.seen = nil
		return reply.Files, io.EOF
	}
	if l.seen == nil {
		l.seen = make(map[string]struct{})
	}
	l.seen[l.cursor] = struct{}{}
	return reply.Files, nil
}

func (l *listing) Next(ctx context.Context, n int) ([]core.Metadata, error) {
	if l.closed {
		return nil, io.ErrClosedPipe
	}
	if n < 1 {
		return nil, core.Fail(core.InvalidRequest, nil)
	}
	files, err := l.next(ctx)
	if err != nil && err != io.EOF {
		return nil, err
	}
	metadata := make([]core.Metadata, len(files))
	for i, file := range files {
		metadata[i] = file.meta
	}
	return metadata, err
}

func (l *listing) Close() error {
	l.closed, l.seen = true, nil
	return nil
}

func (d *Driver) resolve(ctx context.Context, address core.NativeAddress) (item, error) {
	if err := d.check(ctx); err != nil {
		return item{}, err
	}
	if address.Namespace() != core.TreeNamespace {
		return item{}, core.Fail(core.UnsupportedOperation, nil)
	}
	path := address.Value()
	if err := core.ValidatePath(path); err != nil {
		return item{}, err
	}
	current := item{ID: rootAlias, meta: core.Metadata{Kind: core.Directory}}
	if path == "/" {
		return current, nil
	}
	for _, name := range strings.Split(path[1:], "/") {
		if current.meta.Kind != core.Directory {
			return item{}, core.Fail(core.NotDirectory, nil)
		}
		scan := listing{driver: d, parent: current.ID, name: name, rootParent: d.rootParent}
		found, exists, err := childscan.Unique(ctx, name, scan.next, func(f item) (string, string) { return f.ID, f.Name })
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

func (f item) readURL(version core.Version) string {
	target := filesURL + "/" + url.PathEscape(f.ID)
	if version != "" {
		target += "/historyVersions/" + url.PathEscape(string(version))
	}
	return target
}

func (d *Driver) historyMetadata(ctx context.Context, target, name string, version core.Version) (core.Metadata, error) {
	var reply struct {
		Category   string
		ID         core.Version
		Size       *int64
		EditedTime *time.Time
	}
	if err := d.getJSON(ctx, target+"?fields=category,id,size,editedTime", &reply); err != nil {
		return core.Metadata{}, err
	}
	if reply.Category != "drive#historyVersion" || reply.ID != version || reply.Size != nil && *reply.Size < 0 {
		return core.Metadata{}, core.Fail(core.InvalidUpstreamResponse, errors.New("invalid HUAWEI Drive historical version"))
	}
	return core.Metadata{Name: name, Kind: core.File, Size: reply.Size, ModifiedAt: reply.EditedTime,
		Evidence: core.Evidence{Version: reply.ID}}, nil
}

func (d *Driver) Stat(ctx context.Context, address core.NativeAddress, selections ...core.ReadSelection) (core.Metadata, error) {
	if len(selections) > 1 {
		return core.Metadata{}, core.Fail(core.InvalidRequest, nil)
	}
	var selection core.ReadSelection
	if len(selections) == 1 {
		selection = selections[0]
	}
	if selection.Version != "" && !validID(string(selection.Version)) {
		return core.Metadata{}, core.Fail(core.InvalidRequest, nil)
	}
	if err := selection.When.ForDomain(""); err != nil {
		return core.Metadata{}, err
	}
	file, err := d.resolve(ctx, address)
	if err != nil {
		return core.Metadata{}, err
	}
	meta := file.meta
	if selection.Version != "" {
		if meta.Kind != core.File {
			return core.Metadata{}, core.Fail(core.NotFile, nil)
		}
		meta, err = d.historyMetadata(ctx, file.readURL(selection.Version), file.Name, selection.Version)
		if err != nil {
			return core.Metadata{}, err
		}
	}
	return meta, selection.Check(meta.Evidence)
}

func (d *Driver) Enumerate(ctx context.Context, q core.NativeQuery) (core.Iterator, error) {
	if q.Delimiter() != "" || q.StartAfter() != "" {
		return nil, core.Fail(core.UnsupportedOperation, nil)
	}
	parent, err := d.resolve(ctx, q.Address())
	if err != nil {
		return nil, err
	}
	if parent.meta.Kind != core.Directory {
		return nil, core.Fail(core.NotDirectory, nil)
	}
	return &listing{driver: d, parent: parent.ID, rootParent: d.rootParent}, nil
}

func (d *Driver) Open(ctx context.Context, address core.NativeAddress, o core.OpenOptions) (core.Opened, error) {
	if err := o.Validate(); err != nil {
		return core.Opened{}, err
	}
	if o.Mode != core.StreamRead || o.NoFollow {
		return core.Opened{}, core.Fail(core.UnsupportedOperation, nil)
	}
	version := o.Selection.Version
	if version != "" && !validID(string(version)) {
		return core.Opened{}, core.Fail(core.InvalidRequest, nil)
	}
	if err := o.Selection.When.ForDomain(""); err != nil {
		return core.Opened{}, err
	}
	file, err := d.resolve(ctx, address)
	if err != nil {
		return core.Opened{}, err
	}
	if file.meta.Kind != core.File {
		return core.Opened{}, core.Fail(core.NotFile, nil)
	}
	target := file.readURL(version)
	var revision core.Metadata
	if version != "" {
		revision, err = d.historyMetadata(ctx, target, file.Name, version)
		if err != nil {
			return core.Opened{}, err
		}
	}
	if err := o.Selection.Check(revision.Evidence); err != nil {
		return core.Opened{}, err
	}
	if version == "" && file.Owned != nil && !*file.Owned {
		return core.Opened{}, core.Fail(core.UpstreamAccessDenied, errors.New("HUAWEI Drive content requires ownership"))
	}
	delegated := false
	response, err := httpcontent.FollowDownload(target+"?form=content",
		func(target string, redirect bool) (*http.Response, error) {
			delegated = redirect
			client := &d.api
			if redirect {
				client = &d.content
			}
			return d.get(ctx, client, target, "", o.Range)
		}, d.cleanup.CloseBody)
	if err != nil {
		return core.Opened{}, err
	}
	switch response.StatusCode {
	case http.StatusOK, http.StatusPartialContent, http.StatusRequestedRangeNotSatisfiable:
		if version == "" {
			return d.cleanup.OpenBody(response, o.Range, file.Name)
		}
		var opened core.Opened
		if revision.Size == nil {
			opened, err = d.cleanup.OpenBody(response, o.Range, file.Name)
			revision.Size = opened.Metadata.Size
		} else {
			opened, err = d.cleanup.OpenSizedBody(response, o.Range, file.Name, *revision.Size)
		}
		if err == nil {
			opened.Metadata = revision
		}
		return opened, err
	default:
		return core.Opened{}, errors.Join(d.statusError(response, !delegated), d.cleanup.CloseBody(response.Body), ctx.Err())
	}
}
