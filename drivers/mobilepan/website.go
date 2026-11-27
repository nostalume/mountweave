package mobilepan

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"path"
	"sync"
	"time"

	core "github.com/nostalume/mountweave"
)

const WebsiteURL = "https://yun.139.com/w/"

// Mkdir creates a website directory and refuses an occupied name.
func (d *Driver) Mkdir(ctx context.Context, at core.NativeAddress) (core.Change, error) {
	unchanged := core.Change{Effect: core.Unchanged}
	if d.website.Call == nil || at.Namespace() != core.TreeNamespace {
		return unchanged, core.Fail(core.UnsupportedOperation, nil)
	}
	target := at.Value()
	if err := core.ValidatePath(target); err != nil {
		return unchanged, err
	}
	if target == "/" {
		if err := d.check(ctx); err != nil {
			return unchanged, err
		}
		unchanged.Before = core.TargetPresent
		return unchanged, core.Fail(core.AlreadyExists, nil)
	}
	parent, err := d.resolve(ctx, core.NativePath(path.Dir(target)))
	if err != nil {
		return unchanged, err
	}
	if parent.metadata.Kind != core.Directory {
		return unchanged, core.Fail(core.NotDirectory, nil)
	}
	name := path.Base(target)
	fields := map[string]string{
		"parentFileId": parent.id, "name": name, "type": "folder",
		"description": "", "fileRenameMode": "refuse",
	}
	uncertain := core.Change{Effect: core.Unknown}
	var receipt struct {
		ID     string `json:"fileId"`
		Parent string `json:"parentFileId"`
		Name   string
		Kind   string `json:"type"`
		Exists bool   `json:"exist"`
	}
	if err := d.call(ctx, "create", fields, &receipt); err != nil {
		return uncertain, err
	}
	if receipt.ID == "" || receipt.ID == rootID || receipt.ID == parent.id || len(receipt.ID) > 64 ||
		(receipt.Name != "" && receipt.Name != name) || (receipt.Parent != "" && receipt.Parent != parent.id) {
		return uncertain, core.Fail(core.InvalidUpstreamResponse, nil)
	}
	if receipt.Exists {
		if receipt.Name == "" || receipt.Parent == "" {
			pages := d.resolvePages
			child, err := d.child(ctx, parent.id, name, &pages)
			if err != nil && core.CodeOf(err) != core.NotFound {
				return uncertain, err
			}
			if child.id != receipt.ID {
				return uncertain, core.Fail(core.InvalidUpstreamResponse, err)
			}
		}
		unchanged.Before = core.TargetPresent
		return unchanged, core.Fail(core.AlreadyExists, nil)
	}
	if receipt.Name != name || receipt.Parent != parent.id || (receipt.Kind != "" && receipt.Kind != "folder") {
		return uncertain, core.Fail(core.InvalidUpstreamResponse, nil)
	}
	return core.Change{Effect: core.Applied, Before: core.TargetMissing}, nil
}

// Relocate exclusively renames a regular file within its current folder.
func (d *Driver) Relocate(ctx context.Context, source, target core.NativeAddress, options ...core.MoveOptions) (core.Change, error) {
	unchanged := core.Change{Effect: core.Unchanged}
	if len(options) > 1 {
		return unchanged, core.Fail(core.InvalidRequest, nil)
	}
	if d.website.Call == nil || source.Namespace() != core.TreeNamespace || target.Namespace() != core.TreeNamespace ||
		len(options) == 1 && (!options[0].Source.Empty() || !options[0].Target.Empty() ||
			options[0].Existing != "" && options[0].Existing != core.CreateNew) {
		return unchanged, core.Fail(core.UnsupportedOperation, nil)
	}
	from, to := source.Value(), target.Value()
	for _, value := range []string{from, to} {
		if err := core.ValidatePath(value); err != nil {
			return unchanged, err
		}
	}
	if from == "/" || to == "/" || from == to {
		return unchanged, core.Fail(core.InvalidRequest, nil)
	}
	dir := path.Dir(from)
	if dir != path.Dir(to) {
		return unchanged, core.Fail(core.UnsupportedOperation, nil)
	}
	parent, err := d.resolve(ctx, core.NativePath(dir))
	if err != nil {
		return unchanged, err
	}
	if parent.metadata.Kind != core.Directory {
		return unchanged, core.Fail(core.NotDirectory, nil)
	}
	pages := d.resolvePages
	file, err := d.child(ctx, parent.id, path.Base(from), &pages)
	if err != nil {
		return unchanged, err
	}
	if file.metadata.Kind != core.File {
		return unchanged, core.Fail(core.UnsupportedOperation, nil)
	}
	name := path.Base(to)
	args := map[string]string{"fileId": file.id, "name": name, "fileRenameMode": "refuse"}
	var receipt struct {
		ID       string `json:"fileId"`
		Name     string
		Occupied bool
	}
	unknown := core.Change{Effect: core.Unknown}
	if err := d.call(ctx, "update", args, &receipt); err != nil {
		return unknown, err
	}
	if !receipt.Occupied && (receipt.ID != file.id || receipt.Name != name) {
		return unknown, core.Fail(core.InvalidUpstreamResponse, nil)
	}
	pages = d.resolvePages
	confirmed, err := d.child(ctx, parent.id, name, &pages)
	if err != nil {
		return unknown, core.Fail(core.InvalidUpstreamResponse, err)
	}
	if receipt.Occupied && confirmed.id != file.id {
		return core.Change{Effect: core.Unchanged, Before: core.TargetPresent}, core.Fail(core.AlreadyExists, nil)
	}
	if receipt.Occupied || confirmed.id != file.id || confirmed.metadata.Kind != core.File {
		return unknown, core.Fail(core.InvalidUpstreamResponse, nil)
	}
	return core.Change{Effect: core.Applied, Before: core.TargetMissing}, nil
}

// RelocateInto moves one regular file into an existing website directory.
// The service may allocate a different name to preserve existing entries.
func (d *Driver) RelocateInto(ctx context.Context, source, directory core.NativeAddress) (string, core.Change, error) {
	unchanged := core.Change{Effect: core.Unchanged}
	if d.website.Call == nil {
		return "", unchanged, core.Fail(core.UnsupportedOperation, nil)
	}
	file, err := d.resolve(ctx, source)
	if err != nil {
		return "", unchanged, err
	}
	if file.metadata.Kind != core.File {
		return "", unchanged, core.Fail(core.UnsupportedOperation, nil)
	}
	parent, err := d.resolve(ctx, directory)
	if err != nil {
		return "", unchanged, err
	}
	if parent.metadata.Kind != core.Directory {
		return "", unchanged, core.Fail(core.NotDirectory, nil)
	}
	if path.Dir(source.Value()) == directory.Value() {
		return "", unchanged, core.Fail(core.InvalidRequest, nil)
	}
	if err := d.check(ctx); err != nil {
		return "", unchanged, err
	}
	request := struct {
		IDs    []string `json:"fileIds"`
		Parent string   `json:"toParentFileId"`
	}{[]string{file.id}, parent.id}
	unknown := core.Change{Effect: core.Unknown}
	if err := d.runBatch(ctx, "batchMove", request, file.id); err != nil {
		return "", unknown, err
	}
	name, err := d.movedName(ctx, parent.id, file.id)
	if err != nil {
		return "", unknown, err
	}
	confirmed, err := d.resolve(ctx, directory)
	if err != nil || confirmed.id != parent.id || confirmed.metadata.Kind != core.Directory {
		return "", unknown, core.Fail(core.InvalidUpstreamResponse, err)
	}
	return name, core.Change{Effect: core.Applied, Before: core.TargetMissing}, nil
}

// movedName proves that the returned name resolves to the moved file.
func (d *Driver) movedName(ctx context.Context, parent, id string) (string, error) {
	list := listing{d: d, parent: parent, pageSize: 100}
	seen := make(map[string]struct{})
	var name string
	for range d.resolvePages {
		items, err := list.next(ctx)
		if err != nil && err != io.EOF {
			return "", err
		}
		for _, item := range items {
			if item.id == id {
				if name != "" || item.metadata.Kind != core.File {
					return "", core.Fail(core.InvalidUpstreamResponse, nil)
				}
				name = item.metadata.Name
				if _, occupied := seen[name]; occupied {
					return "", core.Fail(core.PathAmbiguous, nil)
				}
				seen = nil
			} else if name == "" {
				seen[item.metadata.Name] = struct{}{}
			} else if item.metadata.Name == name {
				return "", core.Fail(core.PathAmbiguous, nil)
			}
		}
		if err == io.EOF {
			if name == "" {
				return "", core.Fail(core.InvalidUpstreamResponse, errors.New("moved Mobile Cloud file not present in target folder"))
			}
			return name, ctx.Err()
		}
	}
	return "", core.Fail(core.ResolutionBudgetExceeded, nil)
}

type batchReceipt struct {
	TaskID   string `json:"taskId"`
	TaskInfo struct {
		ID     string `json:"taskId"`
		Status string `json:"status"`
	} `json:"taskInfo"`
	Results []struct {
		Source struct {
			ID string `json:"fileId"`
		} `json:"srcFile"`
		Code string `json:"errCode"`
	} `json:"batchFileResults"`
}

// Remove uses the website's ordinary single-file delete and confirms its native ID.
func (d *Driver) Remove(ctx context.Context, at core.NativeAddress, options ...core.RemoveOptions) (core.Change, error) {
	unchanged := core.Change{Effect: core.Unchanged}
	if len(options) > 1 || at.Value() == "/" {
		return unchanged, core.Fail(core.InvalidRequest, nil)
	}
	if d.website.Call == nil || len(options) == 1 && (!options[0].When.Empty() ||
		options[0].Target != "" && options[0].Target != core.RemoveNonDirectory) {
		return unchanged, core.Fail(core.UnsupportedOperation, nil)
	}
	file, err := d.resolve(ctx, at)
	if err != nil {
		return unchanged, err
	}
	if file.metadata.Kind != core.File {
		return unchanged, core.Fail(core.UnsupportedOperation, nil)
	}
	if err := d.check(ctx); err != nil {
		return unchanged, err
	}
	if err := d.runBatch(ctx, "batchDelete", map[string][]string{"fileIds": {file.id}}, file.id); err != nil {
		return core.Change{Effect: core.Unknown}, err
	}
	return core.Change{Effect: core.Applied}, nil
}

func (d *Driver) runBatch(ctx context.Context, route string, request any, id string) error {
	var receipt batchReceipt
	if err := d.call(ctx, route, request, &receipt); err != nil {
		return err
	}
	if task := receipt.TaskID; task != "" {
		// The personal website offers background status after 15 seconds.
		poll, cancel := context.WithTimeoutCause(ctx, 15*time.Second, core.Fail(core.UpstreamTimeout, context.DeadlineExceeded))
		defer cancel()
	polling:
		for {
			receipt = batchReceipt{}
			if err := d.call(poll, "task/get", map[string]string{"taskId": task}, &receipt); err != nil {
				return errors.Join(err, context.Cause(poll))
			}
			switch {
			case receipt.TaskInfo.ID == "" && receipt.TaskInfo.Status == "" && receipt.TaskID == "" && len(receipt.Results) == 0:
				// The result is not visible yet.
			case receipt.TaskInfo.ID != task:
				return core.Fail(core.InvalidUpstreamResponse, nil)
			case receipt.TaskInfo.Status == "Succeed":
				break polling
			case receipt.TaskInfo.Status == "Running":
			case receipt.TaskInfo.Status == "Failed" || receipt.TaskInfo.Status == "PartialSucceed":
				return core.Fail(core.UpstreamFailure, nil)
			default:
				return core.Fail(core.InvalidUpstreamResponse, nil)
			}
			timer := time.NewTimer(1500 * time.Millisecond)
			select {
			case <-timer.C:
			case <-poll.Done():
				timer.Stop()
				return context.Cause(poll)
			}
		}
	}
	if len(receipt.Results) != 1 || receipt.Results[0].Source.ID != id || receipt.Results[0].Code == "" {
		return core.Fail(core.InvalidUpstreamResponse, nil)
	}
	if receipt.Results[0].Code != "0000" {
		return core.Fail(core.UpstreamFailure, nil)
	}
	return nil
}

// Website calls fixed file operations inside an authenticated Mobile Cloud page.
// The caller owns the session until Open returns a Driver.
type Website struct {
	Call    func(context.Context, string, json.RawMessage) (json.RawMessage, error)
	Release func() error
	// ExpectedRootID binds Open to a previously verified folder when set.
	ExpectedRootID string
	Upload         *WebsiteUploadOptions
}

// AccountID returns a comparison key for the signed-in account. Store it with
// the protected session reference. The page keeps the login credentials.
func (w Website) AccountID(ctx context.Context) (string, error) {
	var data struct{ Account string }
	err := w.request(ctx, "account", "", nil, 4096, &data)
	if err != nil {
		return "", err
	}
	if !validAccountID(data.Account) {
		return "", core.Fail(core.InvalidUpstreamResponse, nil)
	}
	return data.Account, nil
}

func validAccountID(id string) bool {
	if len(id) != 64 {
		return false
	}
	_, err := hex.DecodeString(id)
	return err == nil
}

// Open checks the account and resolves the root against the personal drive.
// The content client receives only delegated body requests and must not attach credentials.
func (w Website) Open(ctx context.Context, accountID, rootPath string, content *http.Client) (*Driver, error) {
	if w.Call == nil || w.Release == nil || !validAccountID(accountID) {
		return nil, core.Fail(core.InvalidRequest, nil)
	}
	staging, err := prepareWebsiteUpload(w.Upload)
	if err != nil {
		return nil, err
	}
	d, err := open(ctx, Options{RootPath: rootPath, ContentClient: content}, w, accountID)
	if err != nil {
		return nil, err
	}
	d.upload = staging
	d.releaseWebsite = sync.OnceValue(w.Release)
	return d, nil
}

func (w Website) request(ctx context.Context, route, accountID string, data json.RawMessage, limit int64, out any) error {
	if w.Call == nil {
		return core.Fail(core.InvalidRequest, nil)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	args, err := json.Marshal(struct {
		Route   string          `json:"route"`
		Account string          `json:"account"`
		Data    json.RawMessage `json:"data"`
	}{route, accountID, data})
	if err != nil {
		return core.Fail(core.InvalidRequest, nil)
	}
	raw, err := w.Call(ctx, websiteRequest, args)
	if err := ctx.Err(); err != nil {
		return err
	}
	if err != nil {
		return err
	}
	if int64(len(raw)) > limit {
		return core.Fail(core.ResponseBudgetExceeded, nil)
	}
	var reply struct {
		response
		Status          int
		SignIn          bool `json:"sign_in"`
		AccountMismatch bool `json:"account_mismatch"`
	}
	if json.Unmarshal(raw, &reply) != nil {
		return core.Fail(core.InvalidUpstreamResponse, nil)
	}
	if reply.SignIn {
		return core.Fail(core.UpstreamAccessDenied, ErrWebsiteSignIn)
	}
	if reply.AccountMismatch {
		return core.Fail(core.UpstreamAccessDenied, ErrWebsiteBindingMismatch)
	}
	switch reply.Status {
	case http.StatusUnauthorized:
		return core.Fail(core.UpstreamAccessDenied, ErrWebsiteSignIn)
	case http.StatusRequestEntityTooLarge:
		return core.Fail(core.ResponseBudgetExceeded, nil)
	case 0, http.StatusOK:
		return reply.decode(out)
	default:
		return requestFailure(reply.Status, "")
	}
}

const websiteRequest = `async ({route, account, data}) => {
	const result = (data) => JSON.stringify({success:true, data});
	const status = (status) => JSON.stringify({status});
	if (window !== window.top || location.origin !== "https://yun.139.com" ||
		!location.pathname.startsWith("/w/")) return status(401);
	const batch = route === "batchDelete" || route === "batchMove" || route === "task/get";
	const grant = route === "upload-grant", complete = route === "upload-complete";
	if (route !== "account" && route !== "list" && route !== "getDownloadUrl" && route !== "create" && route !== "update" && !batch && !grant && !complete) return "{}";
	for (let i=0;i<12 && !window.MCloudVM;i++) await new Promise(resolve=>setTimeout(resolve,250));
	const identity = () => {
		const auth = window.MCloudVM?.$store?.state?.auth;
		if (!auth?.authToken && !auth?.token) return "";
		try { return auth.accountPhone || atob(localStorage.getItem("encryptAccount") || ""); }
		catch { return ""; }
	};
	const user = identity();
	if (!user) return status(401);
	const accountMismatch = JSON.stringify({account_mismatch:true});
	const identityFailure = () => {
		const current = identity();
		if (!current) return status(401);
		if (current !== user) return accountMismatch;
	};
	const subject = new TextEncoder().encode("mweave:mobilepan:personal:" + user);
	const digest = await crypto.subtle.digest("SHA-256", subject);
	const id = Array.from(new Uint8Array(digest), b=>b.toString(16).padStart(2,"0")).join("");
	const failure = identityFailure();
	if (failure) return failure;
	if (route !== "account" && account !== id) return accountMismatch;
	if (route === "account") return result({account:id});

	const chunks = window.webpackJsonp;
	if (!Array.isArray(chunks)) return "{}";
	const key = "mweave-mobilepan";
	const req = chunks.push([[key],{[key]:(module,_,require)=>{module.exports=require;}},[[key]]]);
	if (typeof req !== "function" || typeof req.m?.["4279"] !== "function" ||
		typeof req.m?.["2bdd"] !== "function") return "{}";
	const send = req("4279").a, endpoint = req("2bdd").c;
	if (typeof send !== "function" || typeof endpoint !== "function") return "{}";
	try {
		const u = new URL(endpoint());
		if (u.protocol !== "https:" || u.username || u.password || u.port || u.hash || u.search ||
			u.pathname !== "/hcy" || !(u.hostname.endsWith(".yun.139.com") ||
			u.hostname === "api.hcy.aliyunccp.com")) return "{}";
		if (grant || complete) {
			if (typeof req.m?.["a3f2"] !== "function") return "{}";
			const service = req("a3f2"), options = {headers:{"x-yun-op-type":1}};
			const reply = complete ? await service.c(data, void 0, options) : await service.b(data, options);
			const failure = identityFailure();
			if (failure) return failure;
			const code = String(reply?.code ?? ""), accepted = reply?.success === true;
			const native = reply?.data ?? {};
			if (complete) return result({accepted, code:/^[0-9]{1,12}$/.test(code)?code:"", fileId:native.fileId});
			if (!accepted) return JSON.stringify({success:false, code:/^[0-9]{1,12}$/.test(code)?code:""});
			const {fileId, parentFileId, fileName, name, type, exist, uploadId, rapidUpload, partInfos} = native;
			if (exist != null && ![true,false,"true","false",1,0,"1","0"].includes(exist)) return "{}";
			if (rapidUpload != null && ![true,false,"true","false",1,0,"1","0"].includes(rapidUpload)) return "{}";
			const partCount = Array.isArray(partInfos) ? partInfos.length : 0;
			const {partNumber, partSize, uploadUrl} = partInfos?.[0] ?? {};
			return result({fileId, parentFileId, name:fileName ?? name, type, uploadId,
				exist:[true,"true",1,"1"].includes(exist), rapidUpload:[true,"true",1,"1"].includes(rapidUpload),
				partCount, part:{partNumber, partSize, uploadUrl:typeof uploadUrl === "string" && uploadUrl.length <= 16384 ? uploadUrl : ""}});
		}
		const operation = route === "task/get" ? "/task/get" : "/file/"+route;
		const reply = await send({url:u.href+operation, method:"POST", headers:{}, data, timeout:6500});
		const failure = identityFailure();
		if (failure) return failure;
		if (!reply?.success) {
			const code = String(reply?.code ?? "");
			if (route === "update" && code === "04010330") return result({occupied:true});
			return JSON.stringify({success:false, code:/^[0-9]{1,12}$/.test(code)?code:""});
		}
		if (batch) {
			// The page request mapper nests poll results under taskResult.
			const {taskId, taskInfo, batchFileResults} = (route === "task/get" ? reply.data?.taskResult ?? reply.data : reply.data) ?? {};
			if (batchFileResults !== undefined && (!Array.isArray(batchFileResults) || batchFileResults.length > 1)) return "{}";
			const receipt = {taskId, taskInfo:taskInfo && {taskId:taskInfo.taskId, status:taskInfo.status},
				batchFileResults:batchFileResults?.map(item=>({srcFile:{fileId:item?.srcFile?.fileId}, errCode:item?.errCode}))};
			return result(receipt);
		}
		if (route === "update") {
			const {fileId, name, fileName} = reply.data ?? {};
			return result({fileId, name:name ?? fileName});
		}
		if (route === "create") {
			const {fileId, parentFileId, fileName, name, type, exist} = reply.data ?? {};
			if (exist != null && ![true,false,"true","false",1,0,"1","0"].includes(exist)) return "{}";
			return result({fileId, parentFileId, name:fileName ?? name, type, exist:[true,"true",1,"1"].includes(exist)});
		}
		const {items, nextPageCursor, fileId, size, url} = reply.data ?? {};
		if (route === "getDownloadUrl") return typeof url === "string" && url.length <= 16384
			? result({fileId, size, url}) : "{}";
		if (!Array.isArray(items) || items.length > data.pageInfo.pageSize) return "{}";
		const encoded = result({items:items.map(({fileId,parentFileId,name,type,size,updatedAt}) =>
			({fileId,parentFileId,name,type,size,updatedAt})), nextPageCursor});
		return encoded.length <= 524288 ? encoded : status(413);
	} catch (error) {
		return identityFailure() || status(Number(error?.status) || 502);
	}
}`

// ErrWebsiteSignIn asks the host to renew the bound website session.
var ErrWebsiteSignIn = errors.New("sign in to the selected Mobile Cloud account")

// ErrWebsiteBindingMismatch reports a different website account or native folder.
var ErrWebsiteBindingMismatch = errors.New("the Mobile Cloud website account or folder differs from the selected binding")

var (
	_ core.Remover            = (*Driver)(nil)
	_ core.Relocator          = (*Driver)(nil)
	_ core.DirectoryRelocator = (*Driver)(nil)
)
var _ core.DirectoryMaker = (*Driver)(nil)
