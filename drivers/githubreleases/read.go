package githubreleases

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/internal/childscan"
	"github.com/nostalume/mountweave/drivers/internal/httpcontent"
)

type asset struct {
	ID          int64
	Name, State string
	Size        *int64
	Updated     *time.Time `json:"updated_at"`
}

func (a asset) metadata() core.Metadata {
	return core.Metadata{Kind: core.File, Name: a.Name, Size: a.Size, ModifiedAt: a.Updated}
}

type listing struct {
	d    *Driver
	page int
	done bool
}

func (s *listing) next(ctx context.Context) ([]asset, error) {
	if s.done {
		return nil, io.EOF
	}
	var items []asset
	header, err := s.d.getJSON(ctx, s.d.release+"/assets?per_page=100&page="+strconv.Itoa(s.page+1), &items)
	if err != nil {
		return nil, err
	}
	if items == nil || len(items) > 100 {
		return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("invalid GitHub release assets page"))
	}
	for _, f := range items {
		if f.ID <= 0 || f.State == "" || f.State == "uploaded" && (f.Size == nil || *f.Size < 0) {
			return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("invalid GitHub release asset"))
		}
		if !core.ValidName(f.Name) {
			return nil, core.Fail(core.UnrepresentableName, nil)
		}
	}
	more, err := nextPage(header, s.page+2)
	if err != nil {
		return nil, err
	}
	s.page++
	if !more {
		s.done = true
		return items, io.EOF
	}
	return items, nil
}

func nextPage(header http.Header, expected int) (bool, error) {
	next := false
	for _, value := range header.Values("Link") {
		for _, link := range strings.Split(value, ",") {
			raw, params, _ := strings.Cut(strings.TrimSpace(link), ">")
			for _, param := range strings.Split(params, ";") {
				if strings.TrimSpace(param) != `rel="next"` {
					continue
				}
				target, err := url.Parse(strings.TrimPrefix(raw, "<"))
				if next || err != nil || target.Scheme != "https" || target.Host != "api.github.com" ||
					target.User != nil || target.Query().Get("page") != strconv.Itoa(expected) {
					return false, core.Fail(core.InvalidUpstreamResponse, errors.New("invalid GitHub release assets continuation"))
				}
				// GitHub may use /repositories/{id} in a Link. Advance only the
				// page number, keeping the original repository and release scope.
				next = true
			}
		}
	}
	return next, nil
}

func (d *Driver) selectAsset(ctx context.Context, address core.NativeAddress) (asset, error) {
	name := address.Value()[1:]
	if strings.Contains(name, "/") {
		return asset{}, core.Fail(core.NotDirectory, nil)
	}
	scan := listing{d: d}
	f, found, err := childscan.Unique(ctx, name, scan.next, func(f asset) (string, string) {
		return strconv.FormatInt(f.ID, 10), f.Name
	})
	if err != nil {
		return asset{}, err
	}
	if !found {
		return asset{}, core.Fail(core.NotFound, nil)
	}
	if f.State != "uploaded" {
		return asset{}, core.Fail(core.UnsupportedOperation, errors.New("GitHub release asset is not uploaded"))
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
		var release struct{ ID int64 }
		if _, err := d.getJSON(ctx, d.release, &release); err != nil {
			return core.Metadata{}, err
		}
		if "/"+strconv.FormatInt(release.ID, 10) != d.release {
			return core.Metadata{}, core.Fail(core.InvalidUpstreamResponse, errors.New("GitHub returned a different release"))
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
	result := make([]core.Metadata, 0, len(items))
	for _, f := range items {
		if f.State == "uploaded" {
			result = append(result, f.metadata())
		}
	}
	return result, err
}

func (i *iterator) Close() error {
	i.cancel()
	i.mu.Lock()
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
	f, err := d.selectAsset(ctx, a)
	if err != nil {
		return core.Opened{}, err
	}
	target := d.base + "/assets/" + strconv.FormatInt(f.ID, 10)
	byteRange := httpcontent.Header(o.Range)
	response, err := httpcontent.FollowDownload(target, func(target string, delegated bool) (*http.Response, error) {
		client := &d.api
		if delegated {
			client = &d.content
		}
		return d.get(ctx, client, target, "application/octet-stream", byteRange)
	}, d.cleanup.CloseBody)
	if err != nil {
		return core.Opened{}, err
	}
	switch response.StatusCode {
	case http.StatusOK, http.StatusPartialContent, http.StatusRequestedRangeNotSatisfiable:
		return d.cleanup.OpenBody(response, o.Range, f.Name)
	default:
		return core.Opened{}, errors.Join(responseError(response), d.cleanup.CloseBody(response.Body))
	}
}
