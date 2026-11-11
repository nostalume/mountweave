package weiyun

import (
	"context"
	"encoding/hex"
	"errors"
	"io"
	"math"
	"net/http"
	"net/url"
	"strings"
	"time"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/internal/childscan"
	"github.com/nostalume/mountweave/drivers/internal/httpcontent"
)

const nativePageSize = 50

type directory struct {
	Key       string `json:"dir_key,omitempty"`
	ParentKey string `json:"pdir_key,omitempty"`
}

type entry struct {
	id string
	m  core.Metadata
}

type iterator struct {
	d            *Driver
	dir          directory
	offset       uint32
	observedKey  string
	done, closed bool
}

func validKey(key string) bool {
	if key == "" {
		return false
	}
	_, err := hex.DecodeString(key)
	return err == nil
}

func metadata(name string, size *int64, updated int64) core.Metadata {
	m := core.Metadata{Name: name, Kind: core.Directory, Size: size}
	if size != nil {
		m.Kind = core.File
	}
	if updated > 0 {
		t := time.UnixMilli(updated).UTC()
		m.ModifiedAt = &t
	}
	return m
}

func (i *iterator) next(ctx context.Context) ([]entry, error) {
	if i.done {
		return nil, io.EOF
	}
	request := struct {
		directory
		Offset    uint32       `json:"offset"`
		Limit     int          `json:"limit"`
		OrderBy   int          `json:"order_by"`
		Ascending bool         `json:"asc"`
		Header    clientHeader `json:"req_header"`
	}{i.dir, i.offset, nativePageSize, 1, true, caller}
	var result struct {
		nativeResult
		Key     string `json:"pdir_key"`
		Done    *bool  `json:"finish_flag"`
		Folders []struct {
			Key     string `json:"dir_key"`
			Name    string `json:"dir_name"`
			Updated int64  `json:"dir_mtime"`
		} `json:"dir_list"`
		Files []struct {
			ID      string `json:"file_id"`
			Name    string `json:"filename"`
			Size    *int64 `json:"file_size"`
			Updated int64  `json:"file_mtime"`
		} `json:"file_list"`
	}
	isError, err := i.d.call(ctx, "weiyun.list", request, &result)
	if err != nil {
		return nil, err
	}
	if err := result.failure(isError); err != nil {
		return nil, err
	}
	expected := i.dir.Key
	if expected == "" {
		expected = i.d.root
	}
	if !validKey(result.Key) || expected != "" && !strings.EqualFold(result.Key, expected) {
		return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("Weiyun did not confirm the selected directory"))
	}
	if result.Done == nil {
		return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("Weiyun omitted the list completion flag"))
	}
	count := len(result.Folders) + len(result.Files)
	if count > nativePageSize {
		return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("Weiyun exceeded its list page limit"))
	}
	// A full page advances by the same offset under either interpretation of a short page.
	if !*result.Done && (count != nativePageSize || i.offset > math.MaxUint32-nativePageSize) {
		return nil, core.Fail(core.UpstreamFailure, errors.New("Weiyun did not provide a verifiable list continuation"))
	}
	entries := make([]entry, 0, count)
	for _, folder := range result.Folders {
		if !validKey(folder.Key) || strings.EqualFold(folder.Key, result.Key) {
			return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("invalid Weiyun child directory key"))
		}
		if !core.ValidName(folder.Name) {
			return nil, core.Fail(core.UnrepresentableName, nil)
		}
		entries = append(entries, entry{folder.Key, metadata(folder.Name, nil, folder.Updated)})
	}
	for _, file := range result.Files {
		if file.ID == "" || file.Size == nil || *file.Size < 0 {
			return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("invalid Weiyun file identity or size"))
		}
		if !core.ValidName(file.Name) {
			return nil, core.Fail(core.UnrepresentableName, nil)
		}
		entries = append(entries, entry{file.ID, metadata(file.Name, file.Size, file.Updated)})
	}
	i.observedKey, i.done = result.Key, *result.Done
	if i.done {
		return entries, io.EOF
	}
	i.offset += nativePageSize
	return entries, nil
}

func (i *iterator) Next(ctx context.Context, n int) ([]core.Metadata, error) {
	if i.closed {
		return nil, io.ErrClosedPipe
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if n < 1 {
		return nil, core.Fail(core.InvalidRequest, nil)
	}
	entries, err := i.next(ctx)
	if err != nil && err != io.EOF {
		return nil, err
	}
	out := make([]core.Metadata, len(entries))
	for index, item := range entries {
		out[index] = item.m
	}
	return out, err
}

func (i *iterator) Close() error {
	i.closed = true
	return nil
}

func (d *Driver) resolve(ctx context.Context, address core.NativeAddress) (entry, string, error) {
	if err := d.check(ctx); err != nil {
		return entry{}, "", err
	}
	if address.Namespace() != core.TreeNamespace {
		return entry{}, "", core.Fail(core.UnsupportedOperation, nil)
	}
	if err := core.ValidatePath(address.Value()); err != nil {
		return entry{}, "", err
	}
	current := entry{m: core.Metadata{Kind: core.Directory}}
	var parent string
	if address.Value() == "/" {
		return current, parent, nil
	}
	for _, name := range strings.Split(address.Value()[1:], "/") {
		if current.m.Kind != core.Directory {
			return entry{}, "", core.Fail(core.NotDirectory, nil)
		}
		scan := iterator{d: d, dir: directory{current.id, parent}}
		found, exists, err := childscan.Unique(ctx, name, scan.next, func(f entry) (string, string) {
			if f.m.Kind == core.Directory {
				return "d:" + strings.ToLower(f.id), f.m.Name
			}
			return "f:" + f.id, f.m.Name
		})
		if err != nil {
			return entry{}, "", err
		}
		if !exists {
			return entry{}, "", core.Fail(core.NotFound, nil)
		}
		current, parent = found, scan.observedKey
	}
	return current, parent, nil
}

func (d *Driver) Stat(ctx context.Context, address core.NativeAddress, selections ...core.ReadSelection) (core.Metadata, error) {
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
	f, _, err := d.resolve(ctx, address)
	if err != nil {
		return core.Metadata{}, err
	}
	return f.m, selection.Check(core.Evidence{})
}

func (d *Driver) Enumerate(ctx context.Context, q core.NativeQuery) (core.Iterator, error) {
	if q.Delimiter() != "" || q.StartAfter() != "" {
		return nil, core.Fail(core.UnsupportedOperation, nil)
	}
	f, parent, err := d.resolve(ctx, q.Address())
	if err != nil {
		return nil, err
	}
	if f.m.Kind != core.Directory {
		return nil, core.Fail(core.NotDirectory, nil)
	}
	return &iterator{d: d, dir: directory{f.id, parent}}, nil
}

func (d *Driver) Open(ctx context.Context, address core.NativeAddress, o core.OpenOptions) (core.Opened, error) {
	if err := o.Validate(); err != nil {
		return core.Opened{}, err
	}
	if o.Mode != core.StreamRead || o.NoFollow || o.Selection.Version != "" {
		return core.Opened{}, core.Fail(core.UnsupportedOperation, nil)
	}
	if err := o.Selection.When.ForDomain(""); err != nil {
		return core.Opened{}, err
	}
	f, parent, err := d.resolve(ctx, address)
	if err != nil {
		return core.Opened{}, err
	}
	if f.m.Kind != core.File {
		return core.Opened{}, core.Fail(core.NotFile, nil)
	}
	if err := o.Selection.Check(core.Evidence{}); err != nil {
		return core.Opened{}, err
	}
	type fileRequest struct {
		ID     string `json:"file_id"`
		Parent string `json:"pdir_key"`
	}
	request := struct {
		Items  []fileRequest `json:"items"`
		Header clientHeader  `json:"req_header"`
	}{[]fileRequest{{f.id, parent}}, caller}
	var grant struct {
		nativeResult
		Items []struct {
			ID     string `json:"file_id"`
			URL    string `json:"https_download_url"`
			Size   *int64 `json:"file_size"`
			Cookie string `json:"cookie"`
		}
	}
	isError, err := d.call(ctx, "weiyun.download", request, &grant)
	if err != nil {
		return core.Opened{}, err
	}
	if err := grant.failure(isError); err != nil {
		return core.Opened{}, err
	}
	if len(grant.Items) != 1 || grant.Items[0].ID != f.id || grant.Items[0].Size == nil || *grant.Items[0].Size != *f.m.Size {
		return core.Opened{}, core.Fail(core.InvalidUpstreamResponse, errors.New("Weiyun did not confirm the selected download"))
	}
	item := grant.Items[0]
	cookies, err := http.ParseCookie(item.Cookie)
	if err != nil || len(cookies) != 1 || cookies[0].Name != "FTN5K" || cookies[0].Value == "" || cookies[0].Valid() != nil {
		return core.Opened{}, core.Fail(core.InvalidUpstreamResponse, errors.New("invalid Weiyun download cookie"))
	}
	cookieAllowed := true
	response, err := httpcontent.FollowDownload(item.URL, func(raw string, redirected bool) (*http.Response, error) {
		if err := d.check(ctx); err != nil {
			return nil, err
		}
		u, err := url.Parse(raw)
		if err != nil || u.Scheme != "https" || u.Hostname() == "" || u.User != nil || u.Opaque != "" || u.Fragment != "" {
			return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("invalid Weiyun download URL"))
		}
		host := strings.ToLower(u.Hostname())
		cookieAllowed = cookieAllowed && (host == "weiyun.com" || strings.HasSuffix(host, ".weiyun.com")) && (u.Port() == "" || u.Port() == "443")
		if !redirected && !cookieAllowed {
			return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("invalid Weiyun download cookie receiver"))
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, raw, nil)
		if err != nil {
			return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("invalid Weiyun download request"))
		}
		if cookieAllowed {
			req.AddCookie(cookies[0])
		}
		req.Header.Set("Accept-Encoding", "identity")
		if o.Range != nil {
			req.Header.Set("Range", httpcontent.Header(o.Range))
		}
		resp, err := d.content.Do(req)
		if err != nil {
			return nil, errors.Join(httpcontent.UpstreamIOError(err), ctx.Err())
		}
		return resp, nil
	}, d.cleanup.CloseBody)
	if err != nil {
		return core.Opened{}, err
	}
	switch response.StatusCode {
	case http.StatusOK, http.StatusPartialContent, http.StatusRequestedRangeNotSatisfiable:
	default:
		return core.Opened{}, errors.Join(responseError(response.StatusCode), d.cleanup.CloseBody(response.Body))
	}
	return d.cleanup.OpenSizedBody(response, o.Range, f.m.Name, *item.Size)
}
