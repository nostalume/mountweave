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
	"time"
	"unicode/utf8"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/internal/childscan"
	"github.com/nostalume/mountweave/drivers/internal/httpcontent"
)

// ShareOptions selects a recipient-accessible SMH share protected by an
// extraction code. Endpoint is the provider's HTTPS API origin. Both clients
// are borrowed and must use transports that do not add account credentials.
type ShareOptions struct {
	Endpoint              string
	ShareCode             string       `json:"-"`
	ExtractionCode        string       `json:"-"`
	Client, ContentClient *http.Client `json:"-"`
	MaxResponseBytes      int64
}

// Share exposes the share's read-only live directory view.
type Share struct {
	httpSource
	code, extractionCode string
	refresh              chan struct{}
	grant                shareGrant
}

type shareGrant struct {
	AccessToken string
	ExpireTime  time.Time
}

const shareRoot = "~"

// OpenShare verifies the extraction code and the share's root. Close a
// non-nil failed result to retry retained response cleanup.
func OpenShare(ctx context.Context, o ShareOptions) (*Share, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !core.ValidName(o.ShareCode) || !utf8.ValidString(o.ExtractionCode) ||
		o.ExtractionCode == "" || utf8.RuneCountInString(o.ExtractionCode) > 6 {
		return nil, core.Fail(core.InvalidRequest, nil)
	}
	s := &Share{code: url.PathEscape(o.ShareCode), extractionCode: o.ExtractionCode, refresh: make(chan struct{}, 1)}
	if err := s.configure(o.Endpoint, o.Client, o.ContentClient, o.MaxResponseBytes); err != nil {
		return nil, err
	}
	page := shareIterator{share: s}
	_, err := page.next(ctx, 1)
	if err == io.EOF {
		err = nil
	}
	if err != nil {
		s.closed.Store(true)
	}
	return s, err
}

func (s *Share) token(ctx context.Context) (string, error) {
	select {
	case s.refresh <- struct{}{}:
		defer func() { <-s.refresh }()
	case <-ctx.Done():
		return "", ctx.Err()
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if s.closed.Load() {
		return "", core.Fail(core.MountUnavailable, nil)
	}
	if time.Now().Before(s.grant.ExpireTime) {
		return s.grant.AccessToken, nil
	}
	payload, err := json.Marshal(struct {
		Code string `json:"extractionCode"`
	}{s.extractionCode})
	if err != nil {
		return "", err
	}
	var receipt struct{ AccessToken, ExpireTime string }
	if err := s.readJSON(ctx, s.base+"/share/verify/"+s.code, payload, &receipt); err != nil {
		return "", err
	}
	expiry, err := time.Parse(time.RFC3339Nano, receipt.ExpireTime)
	if err != nil || receipt.AccessToken == "" || !time.Now().Before(expiry) {
		return "", core.Fail(core.InvalidUpstreamResponse, errors.New("SMH did not issue a live share token"))
	}
	s.grant = shareGrant{receipt.AccessToken, expiry}
	return s.grant.AccessToken, nil
}

func (s *Share) target(chain string, query url.Values) string {
	if chain == "" {
		chain = shareRoot
	}
	return s.base + "/share/file/" + s.code + "/" + url.PathEscape(chain) + "?" + query.Encode()
}

type shareItem struct {
	inode       string
	meta        core.Metadata
	canDownload *bool
}

type shareIterator struct {
	share         *Share
	chain, marker string
	seen          map[string]struct{}
	closed, done  bool
}

func (i *shareIterator) next(ctx context.Context, n int) ([]shareItem, error) {
	if i.done {
		return nil, io.EOF
	}
	token, err := i.share.token(ctx)
	if err != nil {
		return nil, err
	}
	query := url.Values{"list": {"1"}, "limit": {strconv.Itoa(min(n, 100))}, "access_token": {token}}
	if i.marker != "" {
		query.Set("marker", i.marker)
	}
	var page struct {
		Contents []struct {
			Inode, Name, Type, Size, UpdateTime string
			CanDownload                         *bool
		}
		Marker  string
		HasMore *bool
	}
	if err := i.share.readJSON(ctx, i.share.target(i.chain, query), nil, &page); err != nil {
		return nil, err
	}
	if page.Contents == nil || page.HasMore == nil {
		return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("SMH did not confirm the share page"))
	}
	if *page.HasMore {
		if _, repeated := i.seen[page.Marker]; page.Marker == "" || repeated {
			return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("SMH did not advance the share marker"))
		}
	}
	items := make([]shareItem, len(page.Contents))
	for k, child := range page.Contents {
		if child.Inode == shareRoot || !core.ValidName(child.Inode) || child.Type != "dir" && child.Type != "file" {
			return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("invalid SMH share child identity or type"))
		}
		if !core.ValidName(child.Name) {
			return nil, core.Fail(core.UnrepresentableName, nil)
		}
		meta, err := (item{Name: child.Name, Type: child.Type, Size: child.Size, Modified: child.UpdateTime}).metadata()
		if err != nil {
			return nil, err
		}
		items[k] = shareItem{child.Inode, meta, child.CanDownload}
	}
	if !*page.HasMore {
		i.done, i.seen = true, nil
		return items, io.EOF
	}
	if i.seen == nil {
		i.seen = make(map[string]struct{})
	}
	i.marker = page.Marker
	i.seen[i.marker] = struct{}{}
	return items, nil
}

func (i *shareIterator) Next(ctx context.Context, n int) ([]core.Metadata, error) {
	if i.closed {
		return nil, io.ErrClosedPipe
	}
	if n < 1 {
		return nil, core.Fail(core.InvalidRequest, nil)
	}
	items, err := i.next(ctx, n)
	if err != nil && err != io.EOF {
		return nil, err
	}
	result := make([]core.Metadata, len(items))
	for k, child := range items {
		result[k] = child.meta
	}
	return result, err
}

func (i *shareIterator) Close() error {
	i.closed, i.seen = true, nil
	return nil
}

func (s *Share) resolve(ctx context.Context, address core.NativeAddress) (shareItem, string, error) {
	if err := ctx.Err(); err != nil {
		return shareItem{}, "", err
	}
	if s.closed.Load() {
		return shareItem{}, "", core.Fail(core.MountUnavailable, nil)
	}
	if address.Namespace() != core.TreeNamespace {
		return shareItem{}, "", core.Fail(core.UnsupportedOperation, nil)
	}
	path := address.Value()
	if err := core.ValidatePath(path); err != nil {
		return shareItem{}, "", err
	}
	current := shareItem{meta: core.Metadata{Kind: core.Directory}}
	chain := ""
	for _, name := range components(path) {
		if current.meta.Kind != core.Directory {
			return shareItem{}, "", core.Fail(core.NotDirectory, nil)
		}
		i := shareIterator{share: s, chain: chain}
		child, found, err := childscan.Unique(ctx, name, func(ctx context.Context) ([]shareItem, error) {
			return i.next(ctx, 100)
		}, func(child shareItem) (string, string) { return child.inode, child.meta.Name })
		if err != nil {
			return shareItem{}, "", err
		}
		if !found {
			return shareItem{}, "", core.Fail(core.NotFound, nil)
		}
		if chain == "" {
			chain = child.inode
		} else {
			chain += "/" + child.inode
		}
		current = child
	}
	return current, chain, nil
}

func (s *Share) Stat(ctx context.Context, address core.NativeAddress, selections ...core.ReadSelection) (core.Metadata, error) {
	if err := checkSelections(selections); err != nil {
		return core.Metadata{}, err
	}
	item, _, err := s.resolve(ctx, address)
	return item.meta, err
}

func (s *Share) Enumerate(ctx context.Context, q core.NativeQuery) (core.Iterator, error) {
	if q.Delimiter() != "" || q.StartAfter() != "" {
		return nil, core.Fail(core.UnsupportedOperation, nil)
	}
	item, chain, err := s.resolve(ctx, q.Address())
	if err != nil {
		return nil, err
	}
	if item.meta.Kind != core.Directory {
		return nil, core.Fail(core.NotDirectory, nil)
	}
	return &shareIterator{share: s, chain: chain}, nil
}

func (s *Share) Open(ctx context.Context, address core.NativeAddress, o core.OpenOptions) (core.Opened, error) {
	if err := checkOpen(o); err != nil {
		return core.Opened{}, err
	}
	file, chain, err := s.resolve(ctx, address)
	if err != nil {
		return core.Opened{}, err
	}
	if file.meta.Kind != core.File {
		return core.Opened{}, core.Fail(core.NotFile, nil)
	}
	if file.canDownload != nil && !*file.canDownload {
		return core.Opened{}, core.Fail(core.UpstreamAccessDenied, nil)
	}
	token, err := s.token(ctx)
	if err != nil {
		return core.Opened{}, err
	}
	query := url.Values{"download": {"1"}, "internal_domain": {"0"}, "access_token": {token}}
	byteRange := httpcontent.Header(o.Range)
	response, err := httpcontent.FollowDownload(s.target(chain, query), func(target string, delegated bool) (*http.Response, error) {
		if delegated {
			u, err := url.Parse(target)
			if err != nil || slices.Contains(u.Query()["access_token"], token) {
				return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("invalid SMH share download target"))
			}
			return s.request(ctx, &s.content, target, "", byteRange, nil)
		}
		reply, err := s.request(ctx, &s.api, target, "", "", nil)
		if err != nil {
			return nil, err
		}
		if reply.StatusCode != http.StatusFound {
			failure := statusError(reply.StatusCode)
			if reply.StatusCode < 400 {
				failure = core.Fail(core.InvalidUpstreamResponse, errors.New("SMH did not delegate the share download"))
			}
			return nil, errors.Join(failure, s.cleanup.CloseBody(reply.Body), ctx.Err())
		}
		return reply, nil
	}, s.cleanup.CloseBody)
	if err != nil {
		return core.Opened{}, err
	}
	// Listing size is a live observation, not part of the download receipt.
	return s.openContent(ctx, response, o.Range, file.meta.Name, nil)
}

var _ core.Driver = (*Share)(nil)
