package tianyipan

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"path"
	"strings"
	"time"

	core "github.com/nostalume/mountweave"
)

const (
	WebsiteURL      = "https://cloud.189.cn/web/main/file/folder/-11"
	WebsiteLoginURL = "https://cloud.189.cn/web/login.html"
	rootID          = "-11"
	pageSize        = 60
	maxFileSize     = 1 << 30
)

var (
	ErrWebsiteSignIn          = errors.New("sign in to Tianyi Personal Cloud")
	ErrWebsiteBindingMismatch = errors.New("the Tianyi account or directory differs from the selected binding")
)

// BrowserCall evaluates one fixed operation in the authenticated website.
type BrowserCall func(context.Context, string, json.RawMessage) (json.RawMessage, error)

type BrowserSession struct {
	call BrowserCall
}

func NewBrowserSession(call BrowserCall) *BrowserSession { return &BrowserSession{call: call} }

type websiteRequest struct {
	Operation string `json:"operation"`
	Account   string `json:"account,omitempty"`
	ID        string `json:"id,omitempty"`
	Name      string `json:"name,omitempty"`
	Page      int    `json:"page,omitempty"`
	UntilMS   int64  `json:"untilMS,omitempty"`
}

type item struct {
	ID     string
	Name   string
	Folder bool
	Size   *int64
}

type websiteResponse struct {
	Account, Folder, FileID, URL, Error string
	TaskID, TaskJSON                    string
	Name                                string
	SignIn                              bool `json:"sign_in"`
	Page                                int
	Count                               *int
	Items                               []item
}

// Relocate exclusively renames a regular file within its current folder.
func (w *Website) Relocate(ctx context.Context, source, target core.NativeAddress, policies ...core.MoveOptions) (core.Change, error) {
	unchanged := core.Change{Effect: core.Unchanged}
	if len(policies) > 1 {
		return unchanged, core.Fail(core.InvalidRequest, nil)
	}
	if source.Namespace() != core.TreeNamespace || target.Namespace() != core.TreeNamespace {
		return unchanged, core.Fail(core.UnsupportedOperation, nil)
	}
	if len(policies) == 1 && (!policies[0].Source.Empty() || !policies[0].Target.Empty() ||
		policies[0].Existing != "" && policies[0].Existing != core.CreateNew) {
		return unchanged, core.Fail(core.UnsupportedOperation, nil)
	}
	if err := core.ValidatePath(target.Value()); err != nil {
		return unchanged, err
	}
	if source.Value() == "/" || target.Value() == "/" || source == target {
		return unchanged, core.Fail(core.InvalidRequest, nil)
	}
	if path.Dir(source.Value()) != path.Dir(target.Value()) {
		return unchanged, core.Fail(core.UnsupportedOperation, nil)
	}
	file, err := w.resolve(ctx, source)
	if err != nil {
		return unchanged, err
	}
	if file.Folder {
		return unchanged, core.Fail(core.UnsupportedOperation, nil)
	}
	if err := w.active(ctx); err != nil {
		return unchanged, err
	}
	name := path.Base(target.Value())
	unknown := core.Change{Effect: core.Unknown}
	receipt, err := w.session.dispatch(ctx, websiteRequest{Operation: "rename", Account: w.account, ID: file.ID, Name: name})
	if core.CodeOf(err) == core.AlreadyExists {
		unchanged.Before = core.TargetPresent
		return unchanged, err
	}
	if err != nil {
		return unknown, err
	}
	if receipt.FileID != file.ID || receipt.Name != name {
		return unknown, core.Fail(core.InvalidUpstreamResponse, nil)
	}
	confirmed, err := w.resolve(ctx, target)
	if err != nil || confirmed.ID != file.ID || confirmed.Folder {
		return unknown, core.Fail(core.InvalidUpstreamResponse, err)
	}
	return core.Change{Effect: core.Applied, Before: core.TargetMissing}, nil
}

func (w *Website) RemoveWithDisposition(ctx context.Context, target core.NativeAddress, options core.RemoveOptions) (core.Change, error) {
	if options.Disposition != core.RemoveTrash {
		return core.Change{Effect: core.Unchanged}, core.Fail(core.UnsupportedOperation, nil)
	}
	options.Disposition = ""
	return w.Remove(ctx, target, options)
}

// Remove sends one ordinary file to the personal drive's recycle bin.
func (w *Website) Remove(ctx context.Context, target core.NativeAddress, policies ...core.RemoveOptions) (core.Change, error) {
	unchanged := core.Change{Effect: core.Unchanged}
	if len(policies) > 1 || target.Value() == "/" {
		return unchanged, core.Fail(core.InvalidRequest, nil)
	}
	if len(policies) == 1 && (policies[0].Disposition != "" || !policies[0].When.Empty() ||
		policies[0].Target != "" && policies[0].Target != core.RemoveNonDirectory) {
		return unchanged, core.Fail(core.UnsupportedOperation, nil)
	}
	file, err := w.resolve(ctx, target)
	if err != nil {
		return unchanged, err
	}
	if file.Folder {
		return unchanged, core.Fail(core.UnsupportedOperation, nil)
	}
	if err := w.active(ctx); err != nil {
		return unchanged, err
	}
	unknown := core.Change{Effect: core.Unknown}
	created, err := w.session.dispatch(ctx, websiteRequest{Operation: "recycle", Account: w.account, ID: file.ID, Name: file.Name})
	if err != nil {
		return unknown, err
	}
	if created.TaskID == "" || len(created.TaskID) > 128 {
		return unknown, core.Fail(core.InvalidUpstreamResponse, nil)
	}
	poll, cancel := context.WithTimeoutCause(ctx, 15*time.Second, core.Fail(core.UpstreamTimeout, context.DeadlineExceeded))
	defer cancel()
	for {
		receipt, err := w.session.request(poll, websiteRequest{Operation: "recycle-status", Account: w.account, ID: created.TaskID})
		if err != nil {
			return unknown, errors.Join(err, context.Cause(poll))
		}
		var task struct {
			ID        string        `json:"taskId"`
			Status    int           `json:"taskStatus"`
			Total     int           `json:"subTaskCount"`
			Succeeded int           `json:"successedCount"`
			Failed    *int          `json:"failedCount"`
			Skipped   *int          `json:"skipCount"`
			Files     []json.Number `json:"successedFileIdList"`
		}
		if json.Unmarshal([]byte(receipt.TaskJSON), &task) != nil || task.ID != created.TaskID {
			return unknown, core.Fail(core.InvalidUpstreamResponse, nil)
		}
		switch task.Status {
		case 4:
			if task.Total != 1 || task.Failed == nil || task.Skipped == nil || *task.Failed < 0 || *task.Skipped < 0 {
				return unknown, core.Fail(core.InvalidUpstreamResponse, nil)
			}
			if *task.Failed != 0 || *task.Skipped != 0 {
				return unknown, core.Fail(core.UpstreamFailure, nil)
			}
			if task.Succeeded != 1 || len(task.Files) != 1 || task.Files[0].String() != file.ID {
				return unknown, core.Fail(core.InvalidUpstreamResponse, nil)
			}
			return core.Change{Effect: core.Applied}, nil
		case 1, 3:
		case -1, 2, 5:
			return unknown, core.Fail(core.UpstreamFailure, nil)
		default:
			return unknown, core.Fail(core.InvalidUpstreamResponse, nil)
		}
		timer := time.NewTimer(time.Second)
		select {
		case <-timer.C:
		case <-poll.Done():
			timer.Stop()
			return unknown, context.Cause(poll)
		}
	}
}

func accountID(s string) bool {
	if len(s) != 64 {
		return false
	}
	_, err := hex.DecodeString(s)
	return err == nil
}

func objectID(s string) bool {
	if len(s) == 0 || len(s) > 32 {
		return false
	}
	if s[0] == '-' {
		switch s {
		case rootID, "-12", "-13", "-14", "-15", "-16":
			return true
		default:
			return false
		}
	}
	return strings.IndexFunc(s, func(r rune) bool { return r < '0' || r > '9' }) < 0
}

func (s *BrowserSession) Account(ctx context.Context) (string, error) {
	response, err := s.request(ctx, websiteRequest{Operation: "account"})
	return response.Account, err
}

// Let a submitted browser request settle before releasing the native call.
// The shorter page deadline also prevents a delayed account check from dispatching it later.
func (s *BrowserSession) dispatch(ctx context.Context, input websiteRequest) (websiteResponse, error) {
	if err := ctx.Err(); err != nil {
		return websiteResponse{}, err
	}
	until := time.Now().Add(8 * time.Second)
	call, cancel := context.WithDeadline(context.WithoutCancel(ctx), until)
	defer cancel()
	input.UntilMS = until.Add(-500 * time.Millisecond).UnixMilli()
	receipt, err := s.request(call, input)
	return receipt, errors.Join(err, ctx.Err())
}

func (s *BrowserSession) request(ctx context.Context, input websiteRequest) (websiteResponse, error) {
	if err := ctx.Err(); err != nil {
		return websiteResponse{}, err
	}
	if s == nil || s.call == nil {
		return websiteResponse{}, core.Fail(core.InvalidRequest, nil)
	}
	args, err := json.Marshal(input)
	if err != nil {
		return websiteResponse{}, core.Fail(core.InvalidRequest, nil)
	}
	raw, err := s.call(ctx, websiteOperation, args)
	if ctx.Err() != nil {
		return websiteResponse{}, ctx.Err()
	}
	if err != nil {
		if errors.Is(err, io.EOF) {
			err = core.Fail(core.InvalidUpstreamResponse, io.ErrUnexpectedEOF)
		}
		return websiteResponse{}, err
	}
	if len(raw) > 1<<20 {
		return websiteResponse{}, core.Fail(core.ResponseBudgetExceeded, nil)
	}
	var reply websiteResponse
	if json.Unmarshal(raw, &reply) != nil {
		return websiteResponse{}, core.Fail(core.InvalidUpstreamResponse, nil)
	}
	switch {
	case reply.SignIn || reply.Error == "InvalidSessionKey" || reply.Error == "InvalidAccessToken":
		return websiteResponse{}, core.Fail(core.UpstreamAccessDenied, ErrWebsiteSignIn)
	case reply.Error == "account_mismatch":
		return websiteResponse{}, core.Fail(core.UpstreamAccessDenied, ErrWebsiteBindingMismatch)
	case reply.Error == "adapter_unavailable":
		return websiteResponse{}, core.Fail(core.UnsupportedOperation, errors.New("the Tianyi website file view is unavailable"))
	case reply.Error == "mutation_deadline":
		return websiteResponse{}, core.Fail(core.UpstreamTimeout, context.DeadlineExceeded)
	case reply.Error == "cancel":
		return websiteResponse{}, core.Fail(core.UpstreamFailure, errors.New("the Tianyi website canceled the request"))
	case reply.Error == "FileNotFound" || reply.Error == "FolderNotFound":
		return websiteResponse{}, core.Fail(core.NotFound, nil)
	case input.Operation == "rename" && reply.Error == "FileAlreadyExists":
		return websiteResponse{}, core.Fail(core.AlreadyExists, nil)
	case reply.Error == "PermissionDenied" || reply.Error == "InfoSecurityErrorCode":
		return websiteResponse{}, core.Fail(core.UpstreamAccessDenied, nil)
	case reply.Error != "" || !accountID(reply.Account):
		return websiteResponse{}, core.Fail(core.InvalidUpstreamResponse, nil)
	case input.Account != "" && reply.Account != input.Account:
		return websiteResponse{}, core.Fail(core.UpstreamAccessDenied, ErrWebsiteBindingMismatch)
	default:
		return reply, nil
	}
}

// The native client applies the browser's current cookie and request policy.
// The bridge selects fixed personal file operations.
const websiteOperation = `(async input => {
	const reply = x => JSON.stringify(x);
	if (window !== window.top || location.origin !== "https://cloud.189.cn") return reply({sign_in:true});
	const findClient = () => {
		const frame = document.querySelector("#frame");
		if (!frame) return null;
		const walker = document.createTreeWalker(frame, NodeFilter.SHOW_ELEMENT);
		for (let node = frame, n = 0; node && n < 96; node = walker.nextNode(), n++) {
			const model = node.__vue__;
			if (model?.$store && typeof model?.$http?.get === "function") return model.$http;
		}
		return null;
	};
	let client;
	for (let i = 0; i < 30 && !(client = findClient()); i++) {
		await new Promise(resolve => setTimeout(resolve, 100));
	}
	if (!client) return reply({error:"adapter_unavailable"});
	const get = (path, params) => client.get(new URL(path, location.origin).href, {params, isHideError:true});
	const id = value => typeof value === "string" || Number.isSafeInteger(value) ? String(value) : "";
	const code = value => value?.errorCode ?? value?.res_code ?? value?.error;
	const fail = value => reply({error:String(code(value) ?? "invalid_receipt")});
	try {
		const user = await get("/api/open/user/getUserInfoForPortal.action");
		if (code(user) !== 0) return fail(user);
		const nativeAccount = user.userExtResp?.domainSpaceAccount;
		if (typeof nativeAccount !== "string" || !nativeAccount.trim()) return fail(null);
		const digest = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(nativeAccount.trim()));
		const account = Array.from(new Uint8Array(digest), b => b.toString(16).padStart(2, "0")).join("");
		if (input.account && input.account !== account) return reply({error:"account_mismatch"});
		if (input.operation === "account") return reply({account});
		if (input.operation === "list") {
			const page = await get("/api/open/file/listFiles.action", {
				folderId:input.id, pageNum:input.page, pageSize:60, mediaType:0, iconOption:5,
				orderBy:"lastOpTime", descending:true
			});
			if (code(page) !== 0) return fail(page);
			const native = page.fileListAO;
			if (!Number.isSafeInteger(native?.count) || !Array.isArray(native.fileList) ||
				!Array.isArray(native.folderList) || native.fileList.length + native.folderList.length > 60)
				return fail(null);
			const folders = native.folderList.map(x => ({id:id(x.id),name:x.name,folder:true}));
			const files = native.fileList.map(x => ({id:id(x.id),name:x.name,folder:false,
				size:Number.isSafeInteger(x.size) ? x.size : null}));
			return reply({account,folder:input.id,page:input.page,count:native.count,items:folders.concat(files)});
		}
		if (input.operation === "rename" || input.operation === "recycle" || input.operation === "recycle-status") {
			if (typeof client.post !== "function") return reply({error:"adapter_unavailable"});
			const post = (path, data, config) => client.post(new URL(path, location.origin).href, data, {isHideError:true, ...config});
			if (input.operation !== "recycle-status") {
				const remaining = input.untilMS - Date.now();
				if (!Number.isSafeInteger(remaining) || remaining <= 0 || remaining > 8000) return reply({error:"mutation_deadline"});
				const config = {timeout:remaining,signal:AbortSignal.timeout(remaining)};
				if (input.operation === "rename") {
					const renamed = await post("/api/open/file/renameFile.action", {fileId:input.id,destFileName:input.name}, config);
					return code(renamed) === 0 ? reply({account,fileID:id(renamed.id),name:renamed.name}) : fail(renamed);
				}
				const taskInfos = JSON.stringify([{fileId:input.id,fileName:input.name,isFolder:0}]);
				const task = await post("/api/open/batch/createBatchTask.action", {type:"DELETE",targetFolderId:"",taskInfos}, config);
				return code(task) === 0 ? reply({account,taskID:task.taskId}) : fail(task);
			}
			// Task receipts contain unquoted 64-bit file IDs. Preserve the response
			// text for the Go decoder rather than rounding those IDs in JavaScript.
			const task = await post("/api/open/batch/checkBatchTask.action", {type:"DELETE",taskId:input.id}, {
				transformResponse:[taskJSON => {
					if (typeof taskJSON !== "string" || taskJSON.length > 4096) return {};
					const native = JSON.parse(taskJSON);
					return {res_code:native?.res_code,errorCode:native?.errorCode,error:native?.error,taskJSON};
				}]
			});
			return code(task) === 0 ? reply({account,taskJSON:task.taskJSON}) : fail(task);
		}
		if (input.operation !== "grant") return fail(null);
		const grant = await get("/api/open/file/getFileDownloadUrl.action", {fileId:input.id});
		if (code(grant) !== 0) return fail(grant);
		if (typeof grant.fileDownloadUrl !== "string" || !grant.fileDownloadUrl) return fail(null);
		return reply({account,fileID:input.id,url:new URL(grant.fileDownloadUrl, location.href).href});
	} catch (error) {
		return fail(error);
	}
})`

type WebsiteOptions struct {
	Session          *BrowserSession
	Account          string
	RootPath, RootID string
	ContentClient    *http.Client
	// Release runs after a successful mount transfers ownership.
	Release func() error
}

var (
	_ core.Relocator = (*Website)(nil)
	_ core.Remover   = (*Website)(nil)
)
