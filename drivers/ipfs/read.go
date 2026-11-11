package ipfs

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/internal/childscan"
	"github.com/nostalume/mountweave/drivers/internal/httpcontent"
)

const (
	// Kubo ls reports raw and DAG-PB files as File and flattens HAMT
	// directories before reporting them as Directory.
	unixDirectory = 1
	unixFile      = 2
	unixSymlink   = 4
)

type link struct {
	Name, Hash string
	Type       *int
	Size       *int64
	meta       core.Metadata
}

func (d *Driver) links(ctx context.Context, cid string) ([]link, error) {
	var result struct {
		Objects []struct {
			Hash  string
			Links []link
		}
	}
	query := url.Values{"arg": {cid}, "resolve-type": {"true"}, "size": {"true"}, "stream": {"false"}}
	if err := d.json(ctx, "ls", query, &result); err != nil {
		return nil, err
	}
	if len(result.Objects) != 1 || result.Objects[0].Hash != cid {
		return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("Kubo did not return the requested UnixFS directory"))
	}
	items := result.Objects[0].Links
	for i := range items {
		f := &items[i]
		if !core.ValidName(f.Hash) || f.Type == nil {
			return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("Kubo omitted the child identity or type"))
		}
		if !core.ValidName(f.Name) {
			return nil, core.Fail(core.UnrepresentableName, nil)
		}
		f.meta = core.Metadata{Name: f.Name, Kind: core.File}
		switch *f.Type {
		case unixDirectory:
			f.meta.Kind = core.Directory
		case unixFile:
			if f.Size == nil || *f.Size < 0 {
				return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("Kubo returned an invalid file size"))
			}
			f.meta.Size = f.Size
		case unixSymlink:
		default:
			return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("unsupported UnixFS link type"))
		}
	}
	return items, nil
}

func (d *Driver) lookup(ctx context.Context, a core.NativeAddress) (link, error) {
	if err := ctx.Err(); err != nil {
		return link{}, err
	}
	if d.closed.Load() {
		return link{}, core.Fail(core.MountUnavailable, nil)
	}
	if a.Namespace() != core.TreeNamespace {
		return link{}, core.Fail(core.UnsupportedOperation, nil)
	}
	if err := core.ValidatePath(a.Value()); err != nil {
		return link{}, err
	}
	current := link{Hash: d.root, meta: core.Metadata{Kind: core.Directory}}
	if a.Value() == "/" {
		return current, nil
	}
	for _, name := range strings.Split(a.Value()[1:], "/") {
		if current.meta.Kind != core.Directory {
			return link{}, core.Fail(core.NotDirectory, nil)
		}
		items, err := d.links(ctx, current.Hash)
		if err != nil {
			return link{}, err
		}
		found, exists, err := childscan.Unique(ctx, name, func(context.Context) ([]link, error) {
			return items, io.EOF
		}, func(f link) (string, string) { return f.Hash, f.Name })
		if err != nil {
			return link{}, err
		}
		if !exists {
			return link{}, core.Fail(core.NotFound, nil)
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
	items, err := d.links(ctx, f.Hash)
	if err != nil {
		return nil, err
	}
	metadata := make([]core.Metadata, len(items))
	for k, item := range items {
		metadata[k] = item.meta
	}
	return childscan.Items(metadata), nil
}

func (d *Driver) Open(ctx context.Context, a core.NativeAddress, o core.OpenOptions) (opened core.Opened, err error) {
	if err := o.Validate(); err != nil {
		return opened, err
	}
	if o.Mode != core.StreamRead || o.Selection.Version != "" || !o.Selection.When.Empty() {
		return opened, core.Fail(core.UnsupportedOperation, nil)
	}
	f, err := d.lookup(ctx, a)
	if err != nil {
		return opened, err
	}
	if f.meta.Kind != core.File {
		return opened, core.Fail(core.NotFile, nil)
	}
	if f.meta.Size == nil {
		return opened, core.Fail(core.UnsupportedOperation, errors.New("UnixFS symlink content is not followed"))
	}
	length := *f.meta.Size
	query := url.Values{"arg": {"/ipfs/" + f.Hash}}
	var actual *core.ContentRange
	if o.Range != nil {
		resolved, err := o.Range.Resolve(length)
		if err != nil {
			return opened, err
		}
		actual = &resolved
		length = resolved.End - resolved.Start + 1
		query.Set("offset", strconv.FormatInt(resolved.Start, 10))
		query.Set("length", strconv.FormatInt(length, 10))
	}
	resp, err := d.request(ctx, "cat", query)
	if err != nil {
		return opened, err
	}
	defer func() {
		if err != nil {
			err = errors.Join(err, d.cleanup.CloseBody(resp.Body), ctx.Err())
		}
	}()
	if resp.StatusCode != http.StatusOK {
		return opened, responseError(resp.StatusCode)
	}
	coding := resp.Header.Get("Content-Encoding")
	nativeLength, parseErr := strconv.ParseInt(resp.Header.Get("X-Content-Length"), 10, 64)
	if coding != "" && coding != "identity" || parseErr != nil || nativeLength != length ||
		resp.ContentLength >= 0 && resp.ContentLength != length || resp.Header.Get("Content-Range") != "" {
		return opened, core.Fail(core.InvalidUpstreamResponse, errors.New("Kubo cat did not confirm the requested byte count"))
	}
	body := &catBody{resp: resp, left: length}
	if length == 0 {
		if err := body.end(); err != nil {
			return opened, err
		}
	}
	return core.Opened{Metadata: f.meta, Body: body, Length: &length, Range: actual}, nil
}

type catBody struct {
	resp *http.Response
	left int64
}

func (b *catBody) Read(p []byte) (int, error) {
	if b.left == 0 {
		return 0, io.EOF
	}
	if int64(len(p)) > b.left {
		p = p[:b.left]
	}
	n, err := b.resp.Body.Read(p)
	b.left -= int64(n)
	switch {
	case err == io.EOF:
		if b.left != 0 {
			return n, errors.Join(io.ErrUnexpectedEOF, streamFailure(b.resp))
		}
		if err := streamFailure(b.resp); err != nil {
			return n, err
		}
	case err != nil:
		return n, httpcontent.PrivateIOError(err)
	case b.left == 0:
		if err := b.end(); err != nil {
			return n, err
		}
		return n, io.EOF
	}
	return n, err
}

// Core may close immediately after Length bytes. Observe Kubo's terminal
// trailer before returning that last read, with at most a one-byte probe.
func (b *catBody) end() error {
	var extra [1]byte
	n, err := io.ReadFull(b.resp.Body, extra[:])
	if n != 0 {
		return core.Fail(core.InvalidUpstreamResponse, errors.New("Kubo cat exceeded its byte count"))
	}
	if err != io.EOF {
		return httpcontent.PrivateIOError(err)
	}
	return streamFailure(b.resp)
}

func (b *catBody) Close() error { return httpcontent.PrivateIOError(b.resp.Body.Close()) }
