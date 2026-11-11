package dingtalkdrive

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/internal/childscan"
	"github.com/nostalume/mountweave/drivers/internal/httpcontent"
)

type item struct {
	ID        string     `json:"id"`
	Name      string     `json:"name"`
	Space     string     `json:"spaceId"`
	Parent    string     `json:"parentId"`
	Type      string     `json:"type"`
	Status    string     `json:"status"`
	Extension string     `json:"extension"`
	Size      *int64     `json:"size"`
	Modified  *time.Time `json:"modifiedTime"`
	meta      core.Metadata
}

func (f item) active() bool { return f.Status == "" || f.Status == "NORMAL" }

type listing struct {
	driver       *Driver
	parent       string
	cursor       string
	seen         map[string]struct{}
	done, closed bool
}

func (l *listing) next(ctx context.Context) ([]item, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if l.done {
		return nil, io.EOF
	}
	q := url.Values{"parentId": {l.parent}, "maxResults": {"30"}}
	if l.cursor != "" {
		q.Set("nextToken", l.cursor)
	}
	var page struct {
		Dentries  []item
		NextToken string
	}
	if err := l.driver.control(ctx, http.MethodGet, "", q, &page); err != nil {
		return nil, err
	}
	if page.Dentries == nil {
		return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("DingTalk omitted the directory entries"))
	}
	if _, repeated := l.seen[page.NextToken]; page.NextToken != "" && repeated {
		return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("DingTalk repeated a list token"))
	}
	for i := range page.Dentries {
		f := &page.Dentries[i]
		if !core.ValidName(f.ID) || f.ID == l.parent || f.ID == l.driver.root || f.Space != l.driver.space || f.Parent != l.parent || !f.active() {
			return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("DingTalk returned a child outside the selected directory"))
		}
		if !core.ValidName(f.Name) {
			return nil, core.Fail(core.UnrepresentableName, nil)
		}
		f.meta = core.Metadata{Name: f.Name, Kind: core.File, ModifiedAt: f.Modified}
		switch f.Type {
		case "FOLDER":
			f.meta.Kind = core.Directory
		case "FILE":
			if f.Size != nil && *f.Size < 0 {
				return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("negative DingTalk file size"))
			}
			if !strings.EqualFold(f.Extension, "dlink") {
				f.meta.Size = f.Size
			}
		}
	}
	l.cursor, l.done = page.NextToken, page.NextToken == ""
	if l.done {
		l.seen = nil
		return page.Dentries, io.EOF
	}
	if l.seen == nil {
		l.seen = make(map[string]struct{})
	}
	l.seen[l.cursor] = struct{}{}
	return page.Dentries, nil
}

func (l *listing) Next(ctx context.Context, n int) ([]core.Metadata, error) {
	if l.closed {
		return nil, io.ErrClosedPipe
	}
	if n < 1 {
		return nil, core.Fail(core.InvalidRequest, nil)
	}
	items, err := l.next(ctx)
	if err != nil && err != io.EOF {
		return nil, err
	}
	metadata := make([]core.Metadata, len(items))
	for i, child := range items {
		metadata[i] = child.meta
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
	p := address.Value()
	if err := core.ValidatePath(p); err != nil {
		return item{}, err
	}
	current := item{ID: d.root, meta: core.Metadata{Kind: core.Directory}}
	if p == "/" {
		return current, nil
	}
	for _, name := range strings.Split(p[1:], "/") {
		if current.meta.Kind != core.Directory {
			return item{}, core.Fail(core.NotDirectory, nil)
		}
		scan := listing{driver: d, parent: current.ID}
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
	file, err := d.resolve(ctx, address)
	if err != nil {
		return core.Metadata{}, err
	}
	return file.meta, selection.Check(core.Evidence{})
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
	return &listing{driver: d, parent: parent.ID}, nil
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
	file, err := d.resolve(ctx, address)
	if err != nil {
		return core.Opened{}, err
	}
	if file.meta.Kind != core.File {
		return core.Opened{}, core.Fail(core.NotFile, nil)
	}
	if err := o.Selection.Check(core.Evidence{}); err != nil {
		return core.Opened{}, err
	}
	if file.Type != "FILE" || strings.EqualFold(file.Extension, "dlink") {
		return core.Opened{}, core.Fail(core.UnsupportedOperation, errors.New("DingTalk item has no downloadable original"))
	}
	var grant struct {
		Protocol            string
		HeaderSignatureInfo struct {
			ResourceURLs []string `json:"resourceUrls"`
			Headers      map[string]string
		}
	}
	if err := d.control(ctx, http.MethodPost, "/"+url.PathEscape(file.ID)+"/downloadInfos/query", nil, &grant); err != nil {
		return core.Opened{}, err
	}
	if grant.Protocol != "HEADER_SIGNATURE" {
		return core.Opened{}, core.Fail(core.InvalidUpstreamResponse, errors.New("unsupported DingTalk download protocol"))
	}
	var target string
	for _, raw := range grant.HeaderSignatureInfo.ResourceURLs {
		u, err := url.Parse(raw)
		if err == nil && u.Scheme == "https" && u.Hostname() != "" && u.User == nil && u.Opaque == "" && u.Fragment == "" {
			target = raw
			break
		}
	}
	if target == "" {
		return core.Opened{}, core.Fail(core.InvalidUpstreamResponse, errors.New("DingTalk did not grant a public HTTPS download"))
	}
	headers, err := downloadHeaders(grant.HeaderSignatureInfo.Headers)
	if err != nil {
		return core.Opened{}, err
	}
	wanted := httpcontent.Header(o.Range)
	if signedRange := headers.Get("Range"); signedRange != "" && signedRange != wanted {
		return core.Opened{}, core.Fail(core.InvalidUpstreamResponse, errors.New("DingTalk download signed a different range"))
	}
	response, err := httpcontent.FollowDownload(target, func(target string, redirect bool) (*http.Response, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
		if err != nil {
			return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("invalid DingTalk download request"))
		}
		if !redirect {
			req.Header = headers
			if host := headers.Get("Host"); host != "" {
				if !strings.EqualFold(host, req.URL.Host) {
					return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("DingTalk download signed a different host"))
				}
				req.Host = host
				req.Header.Del("Host")
			}
		}
		if wanted != "" && req.Header.Get("Range") == "" {
			req.Header.Set("Range", wanted)
		}
		return d.request(&d.content, req)
	}, d.cleanup.CloseBody)
	if err != nil {
		return core.Opened{}, err
	}
	switch response.StatusCode {
	case http.StatusOK, http.StatusPartialContent, http.StatusRequestedRangeNotSatisfiable:
		return d.cleanup.OpenBody(response, o.Range, file.Name)
	default:
		return core.Opened{}, errors.Join(statusError(response.StatusCode, ""), d.cleanup.CloseBody(response.Body), ctx.Err())
	}
}

func downloadHeaders(values map[string]string) (http.Header, error) {
	headers := make(http.Header, len(values)+2)
	for name, value := range values {
		for _, c := range name {
			if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || strings.ContainsRune("!#$%&'*+-.^_`|~", c)) {
				return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("invalid DingTalk download header name"))
			}
		}
		key := http.CanonicalHeaderKey(name)
		switch key {
		case "", "X-Acs-Dingtalk-Access-Token", "Connection", "Proxy-Connection", "Keep-Alive", "Proxy-Authorization",
			"Proxy-Authenticate", "Content-Length", "Transfer-Encoding", "Te", "Trailer", "Upgrade":
			return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("unsafe DingTalk download header"))
		}
		if _, duplicate := headers[key]; duplicate || strings.ContainsFunc(value, func(c rune) bool { return c != '\t' && (c < 32 || c == 127) }) {
			return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("invalid DingTalk download header value"))
		}
		headers.Set(key, value)
	}
	return headers, nil
}
