package tencentsmh

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/internal/httpcontent"
)

type item struct {
	Name       string
	Type       string
	Size       string
	Modified   string `json:"modificationTime"`
	ContentCAS string `json:"contentCas"`
}

func (f item) metadata() (core.Metadata, error) {
	meta := core.Metadata{Name: f.Name, Kind: core.File}
	switch f.Type {
	case "dir":
		meta.Kind = core.Directory
	case "file", "image", "video":
		if f.Size != "" {
			size, err := strconv.ParseInt(f.Size, 10, 64)
			if err != nil || size < 0 {
				return core.Metadata{}, core.Fail(core.InvalidUpstreamResponse, errors.New("invalid SMH file size"))
			}
			meta.Size = &size
		}
	case "symlink", "virtual":
		// Core List and Stat expose a name, with no claim of readable bytes.
	default:
		return core.Metadata{}, core.Fail(core.InvalidUpstreamResponse, errors.New("unrecognized SMH item type"))
	}
	if f.Modified != "" {
		modified, err := time.Parse(time.RFC3339Nano, f.Modified)
		if err != nil {
			return core.Metadata{}, core.Fail(core.InvalidUpstreamResponse, errors.New("invalid SMH modification time"))
		}
		meta.ModifiedAt = &modified
	}
	return meta, nil
}

func components(path string) []string {
	if path == "/" {
		return nil
	}
	return strings.Split(path[1:], "/")
}

func (d *Driver) item(ctx context.Context, path, token string, cas bool) (item, error) {
	query := url.Values{"info": {"1"}}
	if cas {
		query.Set("with_content_cas", "1")
	}
	var result struct {
		item
		Path []string
	}
	if err := d.json(ctx, "/directory", path, token, query, &result); err != nil {
		return item{}, err
	}
	if result.Path == nil || !slices.Equal(result.Path, components(path)) ||
		path != "/" && result.Name != path[strings.LastIndexByte(path, '/')+1:] {
		return item{}, core.Fail(core.InvalidUpstreamResponse, errors.New("SMH did not confirm the exact requested path"))
	}
	return result.item, nil
}

func checkSelections(selections []core.ReadSelection) error {
	if len(selections) > 1 {
		return core.Fail(core.InvalidRequest, nil)
	}
	if len(selections) == 1 && (selections[0].Version != "" || !selections[0].When.Empty()) {
		return core.Fail(core.UnsupportedOperation, nil)
	}
	return nil
}

func (d *Driver) Stat(ctx context.Context, address core.NativeAddress, selections ...core.ReadSelection) (core.Metadata, error) {
	if err := checkSelections(selections); err != nil {
		return core.Metadata{}, err
	}
	path, err := d.path(address)
	if err != nil {
		return core.Metadata{}, err
	}
	token, err := d.token(ctx)
	if err != nil {
		return core.Metadata{}, err
	}
	f, err := d.item(ctx, path, token, false)
	if err != nil {
		return core.Metadata{}, err
	}
	if address.Value() == "/" {
		f.Name = ""
	}
	return f.metadata()
}

func (d *Driver) Enumerate(ctx context.Context, q core.NativeQuery) (core.Iterator, error) {
	if q.Delimiter() != "" || q.StartAfter() != "" {
		return nil, core.Fail(core.UnsupportedOperation, nil)
	}
	path, err := d.path(q.Address())
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if d.closed.Load() {
		return nil, core.Fail(core.MountUnavailable, nil)
	}
	return &iterator{d: d, path: path}, nil
}

type iterator struct {
	d            *Driver
	path, marker string
	seen         map[string]struct{}
	closed, done bool
}

func (i *iterator) Next(ctx context.Context, n int) ([]core.Metadata, error) {
	if i.closed {
		return nil, io.ErrClosedPipe
	}
	if i.done {
		return nil, io.EOF
	}
	if n < 1 {
		return nil, core.Fail(core.InvalidRequest, nil)
	}
	token, err := i.d.token(ctx)
	if err != nil {
		return nil, err
	}
	query := url.Values{"by-marker": {"1"}, "limit": {strconv.Itoa(min(n, 1000))}}
	if i.marker != "" {
		query.Set("marker", i.marker)
	}
	var reply struct {
		Path       []string
		Contents   []item
		NextMarker json.RawMessage
	}
	if err := i.d.json(ctx, "/directory", i.path, token, query, &reply); err != nil {
		return nil, err
	}
	if reply.Path == nil || !slices.Equal(reply.Path, components(i.path)) || reply.Contents == nil {
		return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("SMH listing did not confirm the requested directory"))
	}
	marker, err := nextMarker(reply.NextMarker)
	if err != nil {
		return nil, err
	}
	if _, repeated := i.seen[marker]; repeated {
		return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("SMH repeated a directory marker"))
	}
	result := make([]core.Metadata, len(reply.Contents))
	for k, child := range reply.Contents {
		if !core.ValidName(child.Name) {
			return nil, core.Fail(core.UnrepresentableName, nil)
		}
		result[k], err = child.metadata()
		if err != nil {
			return nil, err
		}
	}
	if marker == "" {
		i.done, i.seen = true, nil
		return result, io.EOF
	}
	if i.seen == nil {
		i.seen = make(map[string]struct{})
	}
	i.marker = marker
	i.seen[marker] = struct{}{}
	return result, nil
}

func (i *iterator) Close() error {
	i.closed, i.seen = true, nil
	return nil
}

func nextMarker(raw json.RawMessage) (string, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return "", nil
	}
	if raw[0] == '"' {
		var value string
		if json.Unmarshal(raw, &value) == nil {
			return value, nil
		}
	} else {
		var value json.Number
		if json.Unmarshal(raw, &value) == nil && !strings.ContainsAny(value.String(), "eE.") {
			return value.String(), nil
		}
	}
	return "", core.Fail(core.InvalidUpstreamResponse, errors.New("invalid SMH directory marker"))
}

func checkOpen(o core.OpenOptions) error {
	if err := o.Validate(); err != nil {
		return err
	}
	if o.Mode != core.StreamRead || o.NoFollow || o.Selection.Version != "" || !o.Selection.When.Empty() {
		return core.Fail(core.UnsupportedOperation, nil)
	}
	return nil
}

func (d *Driver) Open(ctx context.Context, address core.NativeAddress, o core.OpenOptions) (core.Opened, error) {
	if err := checkOpen(o); err != nil {
		return core.Opened{}, err
	}
	path, err := d.path(address)
	if err != nil {
		return core.Opened{}, err
	}
	token, err := d.token(ctx)
	if err != nil {
		return core.Opened{}, err
	}
	f, err := d.item(ctx, path, token, true)
	if err != nil {
		return core.Opened{}, err
	}
	switch f.Type {
	case "dir":
		return core.Opened{}, core.Fail(core.NotFile, nil)
	case "symlink", "virtual":
		return core.Opened{}, core.Fail(core.UnsupportedOperation, nil)
	case "file", "image", "video":
	default:
		return core.Opened{}, core.Fail(core.InvalidUpstreamResponse, errors.New("unrecognized SMH file type"))
	}
	if f.ContentCAS == "" {
		return core.Opened{}, core.Fail(core.UnsupportedOperation, errors.New("SMH did not provide a content CAS for conditional download"))
	}
	var receipt struct {
		item
		COSURL string `json:"cosUrl"`
	}
	query := url.Values{"info": {"1"}, "content_cas": {f.ContentCAS}}
	if err := d.json(ctx, "/file", path, token, query, &receipt); err != nil {
		return core.Opened{}, err
	}
	if receipt.Type != f.Type || receipt.ContentCAS != "" && receipt.ContentCAS != f.ContentCAS {
		return core.Opened{}, core.Fail(core.InvalidUpstreamResponse, errors.New("SMH download receipt changed the selected file"))
	}
	receipt.Name = f.Name
	meta, err := receipt.item.metadata()
	if err != nil {
		return core.Opened{}, err
	}
	target, err := url.Parse(receipt.COSURL)
	if err != nil || target.Scheme != "https" || target.Hostname() == "" || target.User != nil ||
		target.Fragment != "" || target.Opaque != "" {
		return core.Opened{}, core.Fail(core.InvalidUpstreamResponse, errors.New("invalid SMH signed content address"))
	}
	byteRange := httpcontent.Header(o.Range)
	response, err := httpcontent.FollowDownload(target.String(), func(target string, _ bool) (*http.Response, error) {
		return d.request(ctx, &d.content, target, "", byteRange, nil)
	}, d.cleanup.CloseBody)
	if err != nil {
		return core.Opened{}, err
	}
	return d.openContent(ctx, response, o.Range, f.Name, meta.Size)
}
