package googledrive

import (
	"context"
	"errors"
	"io"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/internal/childscan"
)

const (
	folderType = "application/vnd.google-apps.folder"
	fileFields = "id,name,mimeType,parents,driveId,resourceKey,size,modifiedTime,trashed,capabilities(canDownload)"
)

type item struct {
	ID           string     `json:"id"`
	Name         string     `json:"name"`
	MIME         string     `json:"mimeType"`
	Parents      []string   `json:"parents"`
	DriveID      string     `json:"driveId"`
	ResourceKey  string     `json:"resourceKey"`
	Size         *int64     `json:"size,string"`
	Modified     *time.Time `json:"modifiedTime"`
	Trashed      bool       `json:"trashed"`
	Capabilities *struct {
		CanDownload *bool `json:"canDownload"`
	} `json:"capabilities"`
	meta core.Metadata
}

func nativeDocument(value string) bool {
	return strings.HasPrefix(value, "application/vnd.google-apps.") ||
		value == "application/vnd.google-gemini.conversation" || value == "application/vnd.google-gemini.gem"
}
func (d *Driver) accept(f item, parent string) (item, error) {
	if f.ID == "" || !utf8.ValidString(f.ID) || strings.ContainsAny(f.ID, "\r\n") ||
		f.MIME == "" || !utf8.ValidString(f.MIME) ||
		(f.ResourceKey != "" && (!headerPart(f.ID) || !headerPart(f.ResourceKey))) {
		return item{}, core.Fail(core.InvalidUpstreamResponse, nil)
	}
	if parent != "" {
		if len(f.Parents) != 1 || f.Parents[0] != parent || f.DriveID != d.root.DriveID || f.Trashed {
			return item{}, core.Fail(core.InvalidUpstreamResponse, errors.New("child left its requested parent or drive"))
		}
		if !core.ValidName(f.Name) {
			return item{}, core.Fail(core.UnrepresentableName, nil)
		}
	} else if f.Trashed {
		return item{}, core.Fail(core.NotFound, nil)
	}
	f.meta = core.Metadata{Name: f.Name, Kind: core.File, ModifiedAt: f.Modified}
	if f.MIME == folderType {
		f.meta.Kind = core.Directory
	} else if !nativeDocument(f.MIME) && d.exports[f.MIME] == "" {
		if f.Size != nil && *f.Size < 0 {
			return item{}, core.Fail(core.InvalidUpstreamResponse, nil)
		}
		f.meta.Size = f.Size
	}
	return f, nil
}

func (d *Driver) getItem(ctx context.Context, ref item) (item, error) {
	var f item
	query := url.Values{"fields": {fileFields}, "supportsAllDrives": {"true"}}
	if err := d.json(ctx, apiURL("/files/"+url.PathEscape(ref.ID), query), ref, &f); err != nil {
		return item{}, err
	}
	if ref.ID != "root" && f.ID != ref.ID {
		return item{}, core.Fail(core.InvalidUpstreamResponse, errors.New("root reply changed file ID"))
	}
	if f.ResourceKey == "" {
		f.ResourceKey = ref.ResourceKey
	}
	return d.accept(f, "")
}

func (d *Driver) admit(ctx context.Context, p string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if d.closed.Load() {
		return core.Fail(core.MountUnavailable, nil)
	}
	return core.ValidatePath(p)
}

func (d *Driver) resolve(ctx context.Context, p string) (item, error) {
	if err := d.admit(ctx, p); err != nil {
		return item{}, err
	}
	current := d.root
	if p == "/" {
		return current, nil
	}
	for _, name := range strings.Split(strings.TrimPrefix(p, "/"), "/") {
		if current.meta.Kind != core.Directory {
			return item{}, core.Fail(core.NotDirectory, nil)
		}
		token, pages := "", 0
		parent := current
		found, exists, err := childscan.Unique(ctx, name, func(ctx context.Context) ([]item, error) {
			if d.resolvePages > 0 && pages >= d.resolvePages {
				return nil, core.Fail(core.ResolutionBudgetExceeded, nil)
			}
			batch, next, err := d.page(ctx, parent, name, token, 100)
			if err != nil {
				return nil, err
			}
			pages++
			token = next
			if next == "" {
				return batch, io.EOF
			}
			return batch, nil
		}, func(f item) (string, string) { return f.ID, f.Name })
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

func quote(value string) string {
	return "'" + strings.NewReplacer("\\", "\\\\", "'", "\\'").Replace(value) + "'"
}

func (d *Driver) page(ctx context.Context, parent item, name, token string, n int) ([]item, string, error) {
	filter := quote(parent.ID) + " in parents and trashed = false"
	if name != "" {
		filter += " and name = " + quote(name)
	}
	query := url.Values{
		"q": {filter}, "pageSize": {strconv.Itoa(min(n, 1000))},
		"fields":            {"nextPageToken,incompleteSearch,files(" + fileFields + ")"},
		"supportsAllDrives": {"true"}, "spaces": {"drive"}, "corpora": {"user"},
	}
	if token != "" {
		query.Set("pageToken", token)
	}
	if d.root.DriveID != "" {
		query.Set("corpora", "drive")
		query.Set("driveId", d.root.DriveID)
		query.Set("includeItemsFromAllDrives", "true")
	}
	var result struct {
		Files      []item `json:"files"`
		Next       string `json:"nextPageToken"`
		Incomplete bool   `json:"incompleteSearch"`
	}
	if err := d.json(ctx, apiURL("/files", query), parent, &result); err != nil {
		return nil, "", err
	}
	if result.Incomplete {
		return nil, "", core.Fail(core.UpstreamFailure, errors.New("native search is incomplete"))
	}
	if result.Next != "" && result.Next == token {
		return nil, "", core.Fail(core.InvalidUpstreamResponse, errors.New("native page token did not advance"))
	}
	for n := range result.Files {
		accepted, err := d.accept(result.Files[n], parent.ID)
		if err != nil {
			return nil, "", err
		}
		result.Files[n] = accepted
	}
	return result.Files, result.Next, nil
}

func (d *Driver) Stat(ctx context.Context, p core.NativeAddress, selections ...core.ReadSelection) (core.Metadata, error) {
	if len(selections) > 1 {
		return core.Metadata{}, core.Fail(core.InvalidRequest, nil)
	}
	if p.Namespace() != core.TreeNamespace {
		return core.Metadata{}, core.Fail(core.UnsupportedOperation, nil)
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
	f, err := d.resolve(ctx, p.Value())
	if err != nil {
		return core.Metadata{}, err
	}
	meta := f.meta
	if p.Value() == "/" {
		meta.Name = ""
	}
	return meta, selection.Check(meta.Evidence)
}

func (d *Driver) Enumerate(ctx context.Context, query core.NativeQuery) (core.Iterator, error) {
	if query.Address().Namespace() != core.TreeNamespace || query.Delimiter() != "" || query.StartAfter() != "" {
		return nil, core.Fail(core.UnsupportedOperation, nil)
	}
	parent, err := d.resolve(ctx, query.Address().Value())
	if err != nil {
		return nil, err
	}
	if parent.meta.Kind != core.Directory {
		return nil, core.Fail(core.NotDirectory, nil)
	}
	life, cancel := context.WithCancel(context.WithoutCancel(ctx))
	return &iterator{d: d, parent: parent, life: life, cancel: cancel}, nil
}

type iterator struct {
	d            *Driver
	parent       item
	life         context.Context
	cancel       context.CancelFunc
	mu           sync.Mutex
	token        string
	done, closed bool
	failed       error
}

func (i *iterator) Close() error {
	i.cancel()
	i.mu.Lock()
	defer i.mu.Unlock()
	i.closed = true
	return nil
}

func (i *iterator) Next(ctx context.Context, n int) ([]core.Metadata, error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.closed {
		return nil, io.ErrClosedPipe
	}
	if n < 1 {
		return nil, core.Fail(core.InvalidRequest, nil)
	}
	if i.failed != nil {
		return nil, i.failed
	}
	if i.done {
		return nil, io.EOF
	}
	call, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(i.life, cancel)
	defer func() { stop(); cancel() }()
	batch, token, err := i.d.page(call, i.parent, "", i.token, n)
	if err == nil {
		err = call.Err()
	}
	if err != nil {
		i.failed = err
		return nil, err
	}
	items := make([]core.Metadata, len(batch))
	for n, f := range batch {
		items[n] = f.meta
	}
	i.token, i.done = token, token == ""
	if i.done {
		return items, io.EOF
	}
	return items, nil
}

var _ core.Driver = (*Driver)(nil)
