// Package githubrepo exposes a GitHub repository tree as a read-only source.
package githubrepo

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"unicode/utf8"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/internal/httpcontent"
)

const (
	api      = "https://api.github.com/repos/"
	metadata = "application/vnd.github+json"
	rawBlob  = "application/vnd.github.raw+json"
)

// Options selects one repository and a ref to resolve when the Driver opens.
// A nil Client uses the default HTTP client for public repositories.
type Options struct {
	Owner            string
	Repository       string
	Ref              string
	RootPath         string
	Client           *http.Client
	MaxResponseBytes int64
}

type Driver struct {
	endpoint string
	root     string
	client   http.Client
	limit    int64
	closed   atomic.Bool
	mu       sync.Mutex
	failed   []io.Closer
}

// Open resolves a commit and optionally selects a subdirectory of its tree.
// Close a non-nil failed result to retry any response cleanup it still owns.
func Open(ctx context.Context, options Options) (*Driver, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !core.ValidName(options.Owner) || !core.ValidName(options.Repository) ||
		options.Ref == "" || options.Ref == "." || options.Ref == ".." || !utf8.ValidString(options.Ref) ||
		strings.ContainsRune(options.Ref, 0) || options.MaxResponseBytes < 0 || options.MaxResponseBytes == math.MaxInt64 {
		return nil, core.Fail(core.InvalidRequest, nil)
	}
	if options.RootPath == "" {
		options.RootPath = "/"
	}
	if err := core.ValidatePath(options.RootPath); err != nil {
		return nil, err
	}
	if options.Client == nil {
		options.Client = http.DefaultClient
	}
	if options.MaxResponseBytes == 0 {
		options.MaxResponseBytes = 4 << 20
	}
	d := &Driver{endpoint: api + url.PathEscape(options.Owner) + "/" + url.PathEscape(options.Repository),
		client: *options.Client, limit: options.MaxResponseBytes}
	d.client.Jar = nil
	d.client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	var commit struct {
		Commit struct {
			Tree struct {
				SHA string `json:"sha"`
			} `json:"tree"`
		} `json:"commit"`
	}
	err := d.getJSON(ctx, "/commits/"+url.PathEscape(options.Ref), &commit)
	if err == nil {
		d.root = commit.Commit.Tree.SHA
		if !validSHA(d.root) {
			err = core.Fail(core.InvalidUpstreamResponse, errors.New("GitHub commit did not identify a tree"))
		} else if options.RootPath != "/" {
			var selected entry
			selected, err = d.resolve(ctx, options.RootPath)
			if err == nil {
				if selected.Kind != core.Directory {
					err = core.Fail(core.NotDirectory, nil)
				} else {
					d.root = selected.SHA
				}
			}
		}
	}
	if err == nil {
		err = ctx.Err()
	}
	if err != nil {
		d.closed.Store(true)
	}
	return d, err
}

func validSHA(s string) bool {
	if len(s) != 40 {
		return false
	}
	var decoded [20]byte
	_, err := hex.Decode(decoded[:], []byte(s))
	return err == nil
}

func (d *Driver) request(ctx context.Context, route, accept, byteRange string) (*http.Response, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if d.closed.Load() {
		return nil, core.Fail(core.MountUnavailable, nil)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, d.endpoint+route, nil)
	if err != nil {
		return nil, core.Fail(core.InvalidRequest, err)
	}
	req.Header.Set("Accept", accept)
	req.Header.Set("Accept-Encoding", "identity")
	req.Header.Set("X-GitHub-Api-Version", "2022-11-28")
	if byteRange != "" {
		req.Header.Set("Range", byteRange)
	}
	resp, err := d.client.Do(req)
	if err != nil {
		var failure *url.Error
		if errors.As(err, &failure) {
			err = failure.Err
		}
		if resp != nil {
			err = errors.Join(err, d.closeBody(resp.Body))
		}
		return nil, errors.Join(core.Fail(core.UpstreamFailure, err), ctx.Err())
	}
	return resp, nil
}

func (d *Driver) getJSON(ctx context.Context, route string, out any) error {
	resp, err := d.request(ctx, route, metadata, "")
	if err != nil {
		return err
	}
	return d.readReply(resp, out)
}

// readReply owns control and error bodies. Successful blob bodies go to Core.
func (d *Driver) readReply(resp *http.Response, out any) (err error) {
	defer func() { err = errors.Join(err, d.closeBody(resp.Body)) }()
	if encoding := resp.Header.Get("Content-Encoding"); encoding != "" && encoding != "identity" {
		return core.Fail(core.InvalidUpstreamResponse, nil)
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, d.limit+1))
	if int64(len(data)) > d.limit {
		return core.Fail(core.ResponseBudgetExceeded, err)
	}
	if err != nil {
		return core.Fail(core.UpstreamFailure, err)
	}
	if resp.StatusCode == http.StatusOK {
		if err := json.Unmarshal(data, out); err != nil {
			return core.Fail(core.InvalidUpstreamResponse, err)
		}
		return nil
	}
	var failure struct {
		Message string `json:"message"`
	}
	_ = json.Unmarshal(data, &failure)
	code := core.UpstreamFailure
	switch resp.StatusCode {
	case http.StatusUnauthorized, http.StatusForbidden:
		code = core.UpstreamAccessDenied
		if resp.Header.Get("X-RateLimit-Remaining") == "0" || resp.Header.Get("Retry-After") != "" {
			code = core.UpstreamThrottled
		}
	case http.StatusNotFound:
		code = core.NotFound
	case http.StatusTooManyRequests:
		code = core.UpstreamThrottled
	}
	return core.Fail(code, fmt.Errorf("GitHub returned HTTP %d: %s", resp.StatusCode, failure.Message))
}

func (d *Driver) closeBody(body io.Closer) error {
	if err := body.Close(); err != nil {
		d.mu.Lock()
		d.failed = append(d.failed, body)
		d.mu.Unlock()
		return err
	}
	return nil
}

// Close retries failed response cleanup. It leaves the borrowed transport open.
func (d *Driver) Close() error {
	d.closed.Store(true)
	d.mu.Lock()
	bodies := d.failed
	d.failed = nil
	d.mu.Unlock()
	var err error
	for _, body := range bodies {
		err = errors.Join(err, d.closeBody(body))
	}
	return err
}

type entry struct {
	Name string    `json:"path"`
	Mode string    `json:"mode"`
	Type string    `json:"type"`
	SHA  string    `json:"sha"`
	Size *int64    `json:"size"`
	Kind core.Kind `json:"-"`
}

func (e entry) metadata() core.Metadata {
	return core.Metadata{Name: e.Name, Kind: e.Kind, Size: e.Size}
}

func (d *Driver) tree(ctx context.Context, sha string) ([]entry, error) {
	var result struct {
		SHA       string  `json:"sha"`
		Tree      []entry `json:"tree"`
		Truncated *bool   `json:"truncated"`
	}
	if err := d.getJSON(ctx, "/git/trees/"+sha, &result); err != nil {
		return nil, err
	}
	if !strings.EqualFold(result.SHA, sha) || result.Tree == nil || result.Truncated == nil || *result.Truncated {
		return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("missing, mismatched or truncated GitHub tree"))
	}
	for n := range result.Tree {
		e := &result.Tree[n]
		if !core.ValidName(e.Name) {
			return nil, core.Fail(core.UnrepresentableName, nil)
		}
		if !validSHA(e.SHA) {
			return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("invalid Git tree entry ID"))
		}
		switch {
		case e.Mode == "040000" && e.Type == "tree":
			e.Kind, e.Size = core.Directory, nil
		case (e.Mode == "100644" || e.Mode == "100755") && e.Type == "blob":
			if e.Size == nil || *e.Size < 0 {
				return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("missing or invalid Git blob size"))
			}
			e.Kind = core.File
		case e.Mode == "120000" && e.Type == "blob", e.Mode == "160000" && e.Type == "commit":
			// Retain exact names for lookup, but do not traverse special nodes.
		default:
			return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("inconsistent Git tree entry type and mode"))
		}
	}
	return result.Tree, ctx.Err()
}

func (d *Driver) resolve(ctx context.Context, value string) (entry, error) {
	at := entry{Kind: core.Directory, SHA: d.root}
	if value == "/" {
		return at, nil
	}
	for _, name := range strings.Split(value[1:], "/") {
		if at.Kind != core.Directory {
			return entry{}, core.Fail(core.NotDirectory, nil)
		}
		children, err := d.tree(ctx, at.SHA)
		if err != nil {
			return entry{}, err
		}
		at = entry{}
		for _, child := range children {
			if child.Name == name {
				if at.SHA != "" {
					return entry{}, core.Fail(core.PathAmbiguous, nil)
				}
				at = child
			}
		}
		if at.SHA == "" {
			return entry{}, core.Fail(core.NotFound, nil)
		}
		if at.Kind == "" {
			return entry{}, core.Fail(core.UnsupportedOperation, errors.New("Git symbolic links and submodules are not followed"))
		}
	}
	return at, nil
}

func (d *Driver) lookup(ctx context.Context, address core.NativeAddress) (entry, error) {
	if err := ctx.Err(); err != nil {
		return entry{}, err
	}
	if d.closed.Load() {
		return entry{}, core.Fail(core.MountUnavailable, nil)
	}
	if address.Namespace() != core.TreeNamespace {
		return entry{}, core.Fail(core.UnsupportedOperation, nil)
	}
	if err := core.ValidatePath(address.Value()); err != nil {
		return entry{}, err
	}
	return d.resolve(ctx, address.Value())
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
	at, err := d.lookup(ctx, address)
	if err != nil {
		return core.Metadata{}, err
	}
	return at.metadata(), selection.Check(core.Evidence{})
}

func (d *Driver) Open(ctx context.Context, address core.NativeAddress, options core.OpenOptions) (core.Opened, error) {
	if err := options.Validate(); err != nil {
		return core.Opened{}, err
	}
	if options.Mode != core.StreamRead || options.NoFollow || options.Selection.Version != "" {
		return core.Opened{}, core.Fail(core.UnsupportedOperation, nil)
	}
	if err := options.Selection.When.ForDomain(""); err != nil {
		return core.Opened{}, err
	}
	at, err := d.lookup(ctx, address)
	if err != nil {
		return core.Opened{}, err
	}
	if at.Kind != core.File {
		return core.Opened{}, core.Fail(core.NotFile, nil)
	}
	if err := options.Selection.Check(core.Evidence{}); err != nil {
		return core.Opened{}, err
	}
	resp, err := d.request(ctx, "/git/blobs/"+at.SHA, rawBlob, httpcontent.Header(options.Range))
	if err != nil {
		return core.Opened{}, err
	}
	switch resp.StatusCode {
	case http.StatusOK, http.StatusPartialContent, http.StatusRequestedRangeNotSatisfiable:
		opened, err := httpcontent.OpenSized(resp, options.Range, *at.Size)
		opened.Metadata.Name = at.Name
		if err == nil {
			opened.Metadata.Size = at.Size
		}
		return opened, err
	default:
		return core.Opened{}, d.readReply(resp, nil)
	}
}

func (d *Driver) Enumerate(ctx context.Context, query core.NativeQuery) (core.Iterator, error) {
	if query.Delimiter() != "" || query.StartAfter() != "" {
		return nil, core.Fail(core.UnsupportedOperation, nil)
	}
	at, err := d.lookup(ctx, query.Address())
	if err != nil {
		return nil, err
	}
	if at.Kind != core.Directory {
		return nil, core.Fail(core.NotDirectory, nil)
	}
	entries, err := d.tree(ctx, at.SHA)
	if err != nil {
		return nil, err
	}
	return &iterator{entries: entries}, nil
}

type iterator struct {
	mu      sync.Mutex
	entries []entry
	closed  bool
}

func (i *iterator) Next(ctx context.Context, n int) ([]core.Metadata, error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.closed {
		return nil, io.ErrClosedPipe
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if n < 1 {
		return nil, core.Fail(core.InvalidRequest, nil)
	}
	items := make([]core.Metadata, 0, min(n, len(i.entries)))
	for len(i.entries) > 0 && len(items) < n {
		at := i.entries[0]
		i.entries = i.entries[1:]
		if at.Kind != "" {
			items = append(items, at.metadata())
		}
	}
	if len(i.entries) == 0 {
		i.entries = nil
		return items, io.EOF
	}
	return items, nil
}

func (i *iterator) Close() error {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.entries, i.closed = nil, true
	return nil
}

var _ core.Driver = (*Driver)(nil)
