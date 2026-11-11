package mediafire

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/internal/childscan"
	"github.com/nostalume/mountweave/drivers/internal/httpcontent"
)

type item struct {
	FolderKey, QuickKey, Name, Filename string
	ParentKey                           string `json:"parent_folderkey"`
	Size                                json.Number
	key                                 string
	meta                                core.Metadata
}

type listing struct {
	d      *Driver
	parent string
	chunk  int64
	files  bool
	done   bool
}

func (s *listing) next(ctx context.Context) ([]item, error) {
	if s.done {
		return nil, io.EOF
	}
	const chunkSize = 100
	kind := "folders"
	if s.files {
		kind = "files"
	}
	response, err := s.d.call(ctx, "folder/get_content", url.Values{
		"folder_key": {s.parent}, "content_type": {kind}, "chunk": {strconv.FormatInt(s.chunk, 10)},
		"chunk_size": {strconv.Itoa(chunkSize)},
	})
	if err != nil {
		return nil, err
	}
	page := response.FolderContent
	chunk, err := page.Chunk.Int64()
	if err != nil || chunk != s.chunk || page.Type != kind || page.More != "yes" && page.More != "no" ||
		s.files && len(page.Folders) != 0 || !s.files && len(page.Files) != 0 {
		return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("MediaFire returned an invalid content chunk"))
	}
	items := page.Folders
	if s.files {
		items = page.Files
	}
	if len(items) > chunkSize || len(items) == 0 && page.More == "yes" {
		return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("MediaFire returned an invalid chunk size"))
	}
	for k := range items {
		f := &items[k]
		f.key, f.meta = f.FolderKey, core.Metadata{Name: f.Name, Kind: core.Directory}
		if s.files {
			size, err := f.Size.Int64()
			if err != nil || size < 0 {
				return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("MediaFire omitted a valid file size"))
			}
			f.key, f.meta = f.QuickKey, core.Metadata{Name: f.Filename, Kind: core.File, Size: &size}
		}
		if !singleKey(f.key) || !s.files && f.key == s.parent || f.ParentKey != "" && f.ParentKey != s.parent {
			return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("MediaFire returned a child outside the selected folder"))
		}
		if !core.ValidName(f.meta.Name) {
			return nil, core.Fail(core.UnrepresentableName, nil)
		}
	}
	if page.More == "yes" {
		if s.chunk == math.MaxInt64 {
			return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("MediaFire chunk number cannot advance"))
		}
		s.chunk++
		return items, nil
	}
	if !s.files {
		s.files, s.chunk = true, 1
		return items, nil
	}
	s.done = true
	return items, io.EOF
}

func (d *Driver) lookup(ctx context.Context, a core.NativeAddress) (item, error) {
	if err := d.active(ctx); err != nil {
		return item{}, err
	}
	if a.Namespace() != core.TreeNamespace {
		return item{}, core.Fail(core.UnsupportedOperation, nil)
	}
	if err := core.ValidatePath(a.Value()); err != nil {
		return item{}, err
	}
	current := item{key: d.root, meta: core.Metadata{Kind: core.Directory}}
	if a.Value() == "/" {
		return current, nil
	}
	for _, name := range strings.Split(a.Value()[1:], "/") {
		if current.meta.Kind != core.Directory {
			return item{}, core.Fail(core.NotDirectory, nil)
		}
		scan := listing{d: d, parent: current.key, chunk: 1}
		found, exists, err := childscan.Unique(ctx, name, scan.next, func(f item) (string, string) {
			return string(f.meta.Kind) + ":" + f.key, f.meta.Name
		})
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
	if len(selections) == 1 && (selections[0].Version != "" || !selections[0].When.Empty()) {
		return core.Metadata{}, core.Fail(core.UnsupportedOperation, nil)
	}
	f, err := d.lookup(ctx, a)
	if err == nil && a.Value() == "/" {
		err = d.confirmRoot(ctx)
	}
	return f.meta, err
}

func (d *Driver) Enumerate(ctx context.Context, q core.NativeQuery) (core.Iterator, error) {
	if q.Delimiter() != "" || q.StartAfter() != "" {
		return nil, core.Fail(core.UnsupportedOperation, nil)
	}
	f, err := d.lookup(ctx, q.Address())
	if err != nil {
		return nil, err
	}
	if f.meta.Kind != core.Directory {
		return nil, core.Fail(core.NotDirectory, nil)
	}
	return &iterator{scan: listing{d: d, parent: f.key, chunk: 1}}, nil
}

type iterator struct {
	scan   listing
	closed bool
}

func (i *iterator) Next(ctx context.Context, n int) ([]core.Metadata, error) {
	if i.closed {
		return nil, io.ErrClosedPipe
	}
	if n < 1 {
		return nil, core.Fail(core.InvalidRequest, nil)
	}
	items, err := i.scan.next(ctx)
	if err != nil && err != io.EOF {
		return nil, err
	}
	result := make([]core.Metadata, len(items))
	for k, item := range items {
		result[k] = item.meta
	}
	return result, err
}

func (i *iterator) Close() error {
	i.closed = true
	return nil
}

func (d *Driver) Open(ctx context.Context, a core.NativeAddress, o core.OpenOptions) (core.Opened, error) {
	if err := o.Validate(); err != nil {
		return core.Opened{}, err
	}
	if o.Mode != core.StreamRead || o.NoFollow || o.Selection.Version != "" || !o.Selection.When.Empty() {
		return core.Opened{}, core.Fail(core.UnsupportedOperation, nil)
	}
	f, err := d.lookup(ctx, a)
	if err != nil {
		return core.Opened{}, err
	}
	if f.meta.Kind != core.File {
		return core.Opened{}, core.Fail(core.NotFile, nil)
	}
	target, err := d.directDownload(ctx, f.key)
	if err != nil {
		return core.Opened{}, err
	}
	byteRange := httpcontent.Header(o.Range)
	response, err := httpcontent.FollowDownload(target, func(target string, _ bool) (*http.Response, error) {
		return d.get(ctx, &d.content, target, byteRange)
	}, d.cleanup.CloseBody)
	if err != nil {
		return core.Opened{}, err
	}
	switch response.StatusCode {
	case http.StatusOK, http.StatusPartialContent, http.StatusRequestedRangeNotSatisfiable:
		opened, err := d.cleanup.OpenBody(response, o.Range, f.meta.Name)
		if err == nil && opened.Metadata.Size != nil && *opened.Metadata.Size != *f.meta.Size {
			return core.Opened{}, errors.Join(core.Fail(core.InvalidUpstreamResponse, errors.New("MediaFire body size disagrees with file metadata")), d.cleanup.CloseBody(opened.Body))
		}
		return opened, err
	default:
		return core.Opened{}, errors.Join(httpFailure(response.StatusCode), d.cleanup.CloseBody(response.Body))
	}
}

func (d *Driver) directDownload(ctx context.Context, key string) (string, error) {
	response, err := d.call(ctx, "file/get_links", url.Values{"quick_key": {key}, "link_type": {"direct_download"}})
	if err != nil {
		return "", err
	}
	if len(response.Links) != 1 || response.Links[0].QuickKey != key {
		return "", core.Fail(core.InvalidUpstreamResponse, errors.New("MediaFire did not confirm the selected download key"))
	}
	link := response.Links[0]
	if link.Error != "" {
		return "", core.Fail(core.UpstreamAccessDenied, errors.New("MediaFire denied the file link"))
	}
	if link.DownloadError != "" {
		value, _ := link.DownloadError.Int64()
		code := core.UnsupportedOperation
		switch value {
		case 41, 45:
			code = core.UpstreamAccessDenied
		case 44:
			code = core.UpstreamThrottled
		}
		return "", core.Fail(code, fmt.Errorf("MediaFire direct download error %d", value))
	}
	if link.DirectDownload == "" {
		return "", core.Fail(core.UnsupportedOperation, errors.New("MediaFire did not grant a direct download link"))
	}
	u, err := url.Parse(link.DirectDownload)
	if err != nil || u.Hostname() == "" || u.User != nil || u.Opaque != "" || u.Fragment != "" {
		return "", core.Fail(core.InvalidUpstreamResponse, errors.New("invalid MediaFire direct download URL"))
	}
	if u.Scheme != "https" {
		return "", core.Fail(core.UnsupportedOperation, errors.New("MediaFire did not grant an HTTPS direct download link"))
	}
	return link.DirectDownload, nil
}
