package lanzou

import (
	"context"
	"encoding/json"
	"errors"
	"html"
	"io"
	"net/http"
	"regexp"
	"strings"
	"sync"

	core "github.com/nostalume/mountweave"
)

const (
	folderTask = 47
	fileTask   = 5
)

var (
	// ErrWebsiteSignIn reports an expired website session.
	ErrWebsiteSignIn = errors.New("LanZou sign-in expired")
	// ErrWebsiteBindingMismatch reports a directory response from a different account.
	ErrWebsiteBindingMismatch = errors.New("LanZou sign-in does not match the mounted account")
	accountPattern            = regexp.MustCompile("^[1-9][0-9]{0,18}$")
)

// WebsiteCall evaluates a LanZou page function with a JSON argument.
type WebsiteCall = func(context.Context, string, json.RawMessage) (json.RawMessage, error)

type lanZouEntry struct {
	id   string
	meta core.Metadata
}

// Driver provides read-only access to a verified LanZou folder.
type Driver struct {
	call         WebsiteCall
	download     Download
	control      http.Client
	release      func() error
	root         lanZouEntry
	account      string
	mu           sync.Mutex
	firstFolders []lanZouEntry
}

type lanZouPageRequest struct {
	Task   int    `json:"task"`
	Folder string `json:"folder"`
	Page   int    `json:"page,omitempty"`
}

func (d *Driver) page(ctx context.Context, request lanZouPageRequest) ([]lanZouEntry, string, error) {
	args, _ := json.Marshal(request)
	raw, err := d.call(ctx, lanZouDirectoryRequest, args)
	if err != nil {
		return nil, "", err
	}
	if len(raw) > 1<<20 {
		return nil, "", core.Fail(core.ResponseBudgetExceeded, nil)
	}
	var reply struct {
		Code    json.Number
		HTTP    int
		Account string
		Entries []struct {
			ID   json.Number
			Name string
		}
		SignIn   bool `json:"sign_in"`
		Oversize bool
		End      bool
	}
	if json.Unmarshal(raw, &reply) != nil {
		return nil, "", core.Fail(core.InvalidUpstreamResponse, nil)
	}
	switch {
	case reply.SignIn || reply.Code == "9" || reply.HTTP == 401 || reply.HTTP == 403:
		return nil, "", core.Fail(core.UpstreamAccessDenied, ErrWebsiteSignIn)
	case reply.Oversize:
		return nil, "", core.Fail(core.ResponseBudgetExceeded, nil)
	case reply.Code == "4" || reply.HTTP == 429:
		return nil, "", core.Fail(core.UpstreamThrottled, nil)
	case reply.HTTP != 0:
		return nil, "", core.Fail(core.UpstreamFailure, nil)
	case reply.Code != "1" && reply.Code != "2":
		return nil, "", core.Fail(core.InvalidUpstreamResponse, nil)
	case reply.Account != "" && !accountPattern.MatchString(reply.Account):
		return nil, "", core.Fail(core.InvalidUpstreamResponse, nil)
	case d.account != "" && d.account != reply.Account:
		return nil, "", core.Fail(core.UpstreamAccessDenied, ErrWebsiteBindingMismatch)
	case reply.End && len(reply.Entries) == 0:
		return []lanZouEntry{}, reply.Account, nil
	case reply.Entries == nil:
		return nil, "", core.Fail(core.InvalidUpstreamResponse, nil)
	}
	kind := core.File
	if request.Task == folderTask {
		kind = core.Directory
	}
	items := make([]lanZouEntry, 0, len(reply.Entries))
	seen := make(map[string]struct{}, len(reply.Entries))
	for _, item := range reply.Entries {
		id, err := item.ID.Int64()
		if err != nil || id <= 0 || string(item.ID) == request.Folder && kind == core.Directory {
			return nil, "", core.Fail(core.InvalidUpstreamResponse, nil)
		}
		if _, ok := seen[string(item.ID)]; ok {
			return nil, "", core.Fail(core.InvalidUpstreamResponse, nil)
		}
		seen[string(item.ID)] = struct{}{}
		name := html.UnescapeString(item.Name)
		if !core.ValidName(name) {
			return nil, "", core.Fail(core.UnrepresentableName, nil)
		}
		items = append(items, lanZouEntry{string(item.ID), core.Metadata{Name: name, Kind: kind}})
	}
	return items, reply.Account, nil
}

func (d *Driver) folders(ctx context.Context, parent string) ([]lanZouEntry, error) {
	if parent == d.root.id {
		d.mu.Lock()
		first := d.firstFolders
		d.firstFolders = nil
		d.mu.Unlock()
		if first != nil {
			return first, ctx.Err()
		}
	}
	items, _, err := d.page(ctx, lanZouPageRequest{Task: folderTask, Folder: parent})
	return items, err
}

type lanZouListing struct {
	driver       *Driver
	parent       string
	page         int
	seenPages    map[string]struct{}
	done, closed bool
}

func (i *lanZouListing) next(ctx context.Context) ([]lanZouEntry, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if i.done {
		return nil, io.EOF
	}
	if i.page == 0 {
		items, err := i.driver.folders(ctx, i.parent)
		if err == nil {
			i.page = 1
		}
		return items, err
	}
	items, _, err := i.driver.page(ctx, lanZouPageRequest{Task: fileTask, Folder: i.parent, Page: i.page})
	if err != nil {
		return nil, err
	}
	if len(items) == 0 {
		i.done = true
		return nil, io.EOF
	}
	if i.seenPages == nil {
		i.seenPages = make(map[string]struct{})
	}
	if _, duplicate := i.seenPages[items[0].id]; duplicate {
		return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("LanZou repeated a file page"))
	}
	i.seenPages[items[0].id] = struct{}{}
	i.page++
	return items, nil
}

func (i *lanZouListing) Next(ctx context.Context, n int) ([]core.Metadata, error) {
	if i.closed {
		return nil, io.ErrClosedPipe
	}
	if n < 1 {
		return nil, core.Fail(core.InvalidRequest, nil)
	}
	items, err := i.next(ctx)
	result := make([]core.Metadata, 0, len(items))
	for _, item := range items {
		result = append(result, item.meta)
	}
	return result, err
}

func (i *lanZouListing) Close() error {
	i.closed = true
	i.seenPages = nil
	return nil
}

func (d *Driver) resolve(ctx context.Context, address core.NativeAddress) (lanZouEntry, error) {
	if err := ctx.Err(); err != nil {
		return lanZouEntry{}, err
	}
	if address.Namespace() != core.TreeNamespace {
		return lanZouEntry{}, core.Fail(core.UnsupportedOperation, nil)
	}
	p := address.Value()
	if err := core.ValidatePath(p); err != nil {
		return lanZouEntry{}, err
	}
	found := d.root
	if p == "/" {
		return found, nil
	}
	left := 200
	for _, name := range strings.Split(p[1:], "/") {
		if found.meta.Kind != core.Directory {
			return lanZouEntry{}, core.Fail(core.NotDirectory, nil)
		}
		list := lanZouListing{driver: d, parent: found.id}
		found = lanZouEntry{}
		for {
			if left == 0 {
				return lanZouEntry{}, core.Fail(core.ResolutionBudgetExceeded, nil)
			}
			left--
			items, err := list.next(ctx)
			for _, item := range items {
				if item.meta.Name != name {
					continue
				}
				if found.id != "" {
					return lanZouEntry{}, core.Fail(core.PathAmbiguous, nil)
				}
				found = item
			}
			if err == io.EOF {
				break
			}
			if err != nil {
				return lanZouEntry{}, err
			}
		}
		if found.id == "" {
			return lanZouEntry{}, core.Fail(core.NotFound, nil)
		}
	}
	return found, nil
}

func (d *Driver) Stat(ctx context.Context, address core.NativeAddress, selections ...core.ReadSelection) (core.Metadata, error) {
	var selection core.ReadSelection
	switch len(selections) {
	case 0:
	case 1:
		selection = selections[0]
	default:
		return core.Metadata{}, core.Fail(core.InvalidRequest, nil)
	}
	if selection.Version != "" {
		return core.Metadata{}, core.Fail(core.UnsupportedOperation, nil)
	}
	if err := selection.When.ForDomain(""); err != nil {
		return core.Metadata{}, err
	}
	entry, err := d.resolve(ctx, address)
	if err != nil {
		return core.Metadata{}, err
	}
	return entry.meta, selection.Check(core.Evidence{})
}

func (d *Driver) Enumerate(ctx context.Context, q core.NativeQuery) (core.Iterator, error) {
	if q.Delimiter() != "" || q.StartAfter() != "" {
		return nil, core.Fail(core.UnsupportedOperation, nil)
	}
	dir, err := d.resolve(ctx, q.Address())
	if err != nil {
		return nil, err
	}
	if dir.meta.Kind != core.Directory {
		return nil, core.Fail(core.NotDirectory, nil)
	}
	return &lanZouListing{driver: d, parent: dir.id}, nil
}

func (d *Driver) Close() error { return d.release() }

const lanZouDirectoryRequest = `async ({task, folder, page}) => {
	if (location.origin !== "` + websiteOrigin + `" || location.pathname !== "/mydisk.php")
		return '{"sign_in":true}';
	if ((task !== 5 && task !== 47) || !/^(?:-1|[1-9][0-9]{0,18})$/.test(folder)) return "{}";
	const target = new URL("/doupload.php", location.origin);
	// Keep routing values from the site's own request inside the same page and origin.
	const existingRoute = () => performance.getEntriesByType("resource").map(e => new URL(e.name))
		.filter(u => u.origin === target.origin && u.pathname === target.pathname).at(-1);
	let ownRequest = existingRoute();
	for (let i=0;!ownRequest && i<8;i++) {
		await new Promise(resolve => setTimeout(resolve,150));
		ownRequest = existingRoute();
	}
	for (const key of ["uid","vei"]) {
		const value = ownRequest?.searchParams.get(key);
		if (value && value.length <= 128) target.searchParams.set(key, value);
	}
	const form = new URLSearchParams({task, folder_id:folder});
	if (task === 5) form.set("pg", page);
	try {
		const res = await fetch(target, {method:"POST", body:form, credentials:"same-origin",
			redirect:"error", headers:{"Accept":"application/json","X-Requested-With":"XMLHttpRequest"},
			signal:AbortSignal.timeout(6500)});
		if (!res.ok) return JSON.stringify({http:res.status});
		const reader=res.body?.getReader();
		if (!reader) return "{}";
		let size=0, text="";
		const decoder = new TextDecoder();
		for (;;) {
			const {value, done} = await reader.read();
			if (done) break;
			size+=value.length;
			if (size>1048576) { await reader.cancel(); return '{"oversize":true}'; }
			text+=decoder.decode(value,{stream:true});
		}
		const data=JSON.parse(text+decoder.decode());
		const pending = Array.isArray(data?.text);
		const end = !pending && String(data?.info) === "0" &&
			(data?.text == null || data.text === "");
		let entries = null;
		if (pending) {
			entries = data.text.map(row => {
				const id = task === 47 ? row?.fol_id : row?.id;
				return {id:typeof id === "number" && !Number.isSafeInteger(id) ? null : id,
					name:task === 47 ? row?.name : row?.name_all};
			});
		}
		const account = target.searchParams.get("uid") || "";
		return JSON.stringify({code:data?.zt, entries, end, account:/^[1-9][0-9]{0,18}$/.test(account) ? account : ""});
	} catch {
		return "{}";
	}
}`
