package googlephotos

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/internal/childscan"
	"github.com/nostalume/mountweave/drivers/internal/httpcontent"
)

type mediaItem struct {
	ID, Filename, BaseURL, MimeType string
	MediaMetadata                   struct{ Video *struct{ Status string } }
}

type listing struct {
	d     *Driver
	token string
	seen  map[string]struct{}
	done  bool
}

func (s *listing) next(ctx context.Context) ([]mediaItem, error) {
	if s.done {
		return nil, io.EOF
	}
	const pageSize = 100
	payload, err := json.Marshal(struct {
		AlbumID   string `json:"albumId"`
		PageToken string `json:"pageToken,omitempty"`
		PageSize  int    `json:"pageSize"`
	}{AlbumID: s.d.album, PageToken: s.token, PageSize: pageSize})
	if err != nil {
		return nil, core.Fail(core.InvalidRequest, err)
	}
	var page struct {
		MediaItems    []mediaItem
		NextPageToken string
	}
	if err := s.d.getJSON(ctx, http.MethodPost, "/mediaItems:search", payload, &page); err != nil {
		return nil, err
	}
	if len(page.MediaItems) > pageSize {
		return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("Google Photos exceeded the media page size"))
	}
	for _, f := range page.MediaItems {
		if f.ID == "" {
			return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("Google Photos omitted the media item identity"))
		}
		if !core.ValidName(f.Filename) {
			return nil, core.Fail(core.UnrepresentableName, nil)
		}
	}
	if page.NextPageToken == "" {
		s.done, s.token, s.seen = true, "", nil
		return page.MediaItems, io.EOF
	}
	if _, duplicate := s.seen[page.NextPageToken]; duplicate {
		return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("Google Photos repeated the media page token"))
	}
	if s.seen == nil {
		s.seen = make(map[string]struct{})
	}
	s.seen[page.NextPageToken] = struct{}{}
	s.token = page.NextPageToken
	return page.MediaItems, nil
}

func (d *Driver) check(ctx context.Context, a core.NativeAddress) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if d.closed.Load() {
		return core.Fail(core.MountUnavailable, nil)
	}
	if a.Namespace() != core.TreeNamespace {
		return core.Fail(core.UnsupportedOperation, nil)
	}
	return core.ValidatePath(a.Value())
}

func (d *Driver) find(ctx context.Context, a core.NativeAddress) (mediaItem, error) {
	name := a.Value()[1:]
	if strings.Contains(name, "/") {
		return mediaItem{}, core.Fail(core.NotDirectory, nil)
	}
	scan := listing{d: d}
	f, exists, err := childscan.Unique(ctx, name, scan.next, func(f mediaItem) (string, string) { return f.ID, f.Filename })
	if err != nil {
		return mediaItem{}, err
	}
	if !exists {
		return mediaItem{}, core.Fail(core.NotFound, nil)
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
		return core.Metadata{Kind: core.Directory}, d.confirmAlbum(ctx)
	}
	f, err := d.find(ctx, a)
	if err != nil {
		return core.Metadata{}, err
	}
	return core.Metadata{Name: f.Filename, Kind: core.File}, nil
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
		if _, err := d.find(ctx, a); err != nil {
			return nil, err
		}
		return nil, core.Fail(core.NotDirectory, nil)
	}
	life, cancel := context.WithCancel(context.WithoutCancel(ctx))
	return &iterator{scan: listing{d: d}, life: life, cancel: cancel}, nil
}

type iterator struct {
	mu     sync.Mutex
	scan   listing
	life   context.Context
	cancel context.CancelFunc
}

func (i *iterator) Next(ctx context.Context, n int) ([]core.Metadata, error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.life.Err() != nil {
		return nil, io.ErrClosedPipe
	}
	if n < 1 {
		return nil, core.Fail(core.InvalidRequest, nil)
	}
	call, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(i.life, cancel)
	defer cancel()
	defer stop()
	items, err := i.scan.next(call)
	if cause := call.Err(); cause != nil {
		if err == io.EOF {
			err = nil
		}
		return nil, errors.Join(cause, err)
	}
	if err != nil && err != io.EOF {
		return nil, err
	}
	result := make([]core.Metadata, len(items))
	for k, item := range items {
		result[k] = core.Metadata{Name: item.Filename, Kind: core.File}
	}
	return result, err
}

func (i *iterator) Close() error {
	i.cancel()
	i.mu.Lock()
	i.scan.token, i.scan.seen = "", nil
	i.mu.Unlock()
	return nil
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
	f, err := d.find(ctx, a)
	if err != nil {
		return core.Opened{}, err
	}
	target, err := f.downloadURL()
	if err != nil {
		return core.Opened{}, err
	}
	byteRange := httpcontent.Header(o.Range)
	resp, err := httpcontent.FollowDownload(target, func(target string, _ bool) (*http.Response, error) {
		return d.request(ctx, &d.content, http.MethodGet, target, nil, byteRange)
	}, d.cleanup.CloseBody)
	if err != nil {
		return core.Opened{}, err
	}
	switch resp.StatusCode {
	case http.StatusOK, http.StatusPartialContent, http.StatusRequestedRangeNotSatisfiable:
		return d.cleanup.OpenBody(resp, o.Range, f.Filename)
	default:
		return core.Opened{}, errors.Join(responseError(resp.StatusCode), d.cleanup.CloseBody(resp.Body))
	}
}

func (f mediaItem) downloadURL() (string, error) {
	var suffix string
	switch {
	case strings.HasPrefix(f.MimeType, "image/"):
		suffix = "=d"
	case strings.HasPrefix(f.MimeType, "video/"):
		if f.MediaMetadata.Video == nil || f.MediaMetadata.Video.Status != "READY" {
			return "", core.Fail(core.UpstreamFailure, errors.New("Google Photos video is not ready for download"))
		}
		suffix = "=dv"
	default:
		return "", core.Fail(core.UnsupportedOperation, errors.New("Google Photos item has no supported download representation"))
	}
	u, err := url.Parse(f.BaseURL)
	if err != nil || u.Scheme != "https" || u.Hostname() == "" || u.User != nil ||
		u.Opaque != "" || u.Fragment != "" || u.Path == "" || u.Path == "/" {
		return "", core.Fail(core.InvalidUpstreamResponse, errors.New("invalid Google Photos media base URL"))
	}
	u.Path += suffix
	if u.RawPath != "" {
		u.RawPath += suffix
	}
	return u.String(), nil
}
