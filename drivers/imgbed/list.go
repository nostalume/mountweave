package imgbed

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

type page struct {
	Files           []struct{ Name string }
	Directories     []string
	DirectFileCount *int
	ReturnedCount   *int
	Indexed         bool `json:"isIndexedResponse"`
}

func (d *Driver) list(ctx context.Context, parent string, start, count int) (_ page, err error) {
	query := url.Values{"dir": {parent}, "start": {strconv.Itoa(start)}, "count": {strconv.Itoa(count)}, "recursive": {"false"}}
	response, err := d.get(ctx, &d.api, "/api/manage/list?"+query.Encode(), "")
	if err != nil {
		return page{}, err
	}
	defer func() { err = errors.Join(err, d.cleanup.CloseBody(response.Body), ctx.Err()) }()
	if response.StatusCode != http.StatusOK {
		return page{}, responseError(response.StatusCode)
	}
	var result page
	if err := httpcontent.DecodeJSON(response, d.limit, &result); err != nil {
		return page{}, err
	}
	if !result.Indexed {
		return page{}, core.Fail(core.InvalidUpstreamResponse, errors.New("ImgBed did not return an indexed listing"))
	}
	if result.Files == nil || result.Directories == nil || result.DirectFileCount == nil ||
		result.ReturnedCount == nil || *result.ReturnedCount != len(result.Files) || *result.DirectFileCount < start ||
		len(result.Files) != min(count, *result.DirectFileCount-start) {
		return page{}, core.Fail(core.InvalidUpstreamResponse, errors.New("invalid ImgBed listing counts"))
	}
	return result, nil
}

func directoryPrefix(key string) string {
	if key == "" {
		return ""
	}
	return key + "/"
}

type iterator struct {
	driver       *Driver
	parent       string
	start        int
	total        int
	done, closed bool
}

func (i *iterator) Next(ctx context.Context, n int) ([]core.Metadata, error) {
	if err := i.driver.check(ctx); err != nil {
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
	p, err := i.driver.list(ctx, i.parent, i.start, n)
	if err != nil {
		return nil, err
	}
	if i.start != 0 && *p.DirectFileCount != i.total {
		return nil, core.Fail(core.CursorUnavailable, errors.New("ImgBed file count changed during listing"))
	}
	size := len(p.Files)
	if i.start == 0 {
		size += len(p.Directories)
	}
	items := make([]core.Metadata, 0, size)
	names := make(map[string]core.Kind, len(p.Directories)+len(p.Files))
	add := func(key string, kind core.Kind) error {
		name, matched := strings.CutPrefix(key, i.parent)
		if !matched || strings.Contains(name, "/") {
			return core.Fail(core.InvalidUpstreamResponse, errors.New("ImgBed listing returned an item outside its direct parent"))
		}
		if !core.ValidName(name) || strings.Contains(name, ",") || kind == core.Directory && strings.Contains(name, "..") {
			return core.Fail(core.UnrepresentableName, nil)
		}
		if existing, found := names[name]; found {
			if existing != kind {
				return core.Fail(core.PathAmbiguous, nil)
			}
			return core.Fail(core.InvalidUpstreamResponse, errors.New("ImgBed repeated an item in one page"))
		}
		names[name] = kind
		if kind == core.File || i.start == 0 {
			items = append(items, core.Metadata{Name: name, Kind: kind})
		}
		return nil
	}
	for _, key := range p.Directories {
		if err := add(key, core.Directory); err != nil {
			return nil, err
		}
	}
	for _, file := range p.Files {
		if err := add(file.Name, core.File); err != nil {
			return nil, err
		}
	}
	i.total = *p.DirectFileCount
	i.start += len(p.Files)
	i.done = i.start == i.total
	if i.done {
		return items, io.EOF
	}
	return items, nil
}

func (i *iterator) Close() error {
	i.closed = true
	return nil
}
