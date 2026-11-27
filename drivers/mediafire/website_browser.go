package mediafire

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"mime"
	"net/http"
	"net/url"
	"regexp"
	"strings"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/internal/httpcontent"
)

// BrowserCall evaluates a bounded first-party operation in the browser's
// authenticated MediaFire file page. The browser retains its credentials.
type BrowserCall func(context.Context, string, json.RawMessage) (json.RawMessage, error)

// BrowserLanding resolves the ordinary link in a public download page.
// It must keep the authenticated account page and its credentials isolated.
// The final page must have the entry URL's origin and the supplied path prefix.
type BrowserLanding func(ctx context.Context, entryURL, pathPrefix, expression string) (string, error)

type BrowserSession struct {
	call    BrowserCall
	landing BrowserLanding
	content http.Client
}

func NewBrowserSession(call BrowserCall, content *http.Client, landing BrowserLanding) *BrowserSession {
	s := &BrowserSession{call: call, landing: landing}
	if content != nil {
		s.content = *content
	}
	s.content.Jar = nil
	s.content.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return s
}

type browserRequest struct {
	Operation string
	List      WebsiteListRequest
	Account   string         `json:",omitempty"`
	ObjectID  string         `json:",omitempty"`
	ParentID  string         `json:",omitempty"`
	Name      string         `json:",omitempty"`
	Upload    *uploadRequest `json:",omitempty"`
}

type browserReply struct {
	Account, Error, ObjectID, Name string
	Status                         int
	NativeError                    int64
	Body                           json.RawMessage
	SignIn                         bool `json:"sign_in"`
	Mismatch                       bool
}

func (s *BrowserSession) request(ctx context.Context, input browserRequest) (browserReply, error) {
	if s.call == nil {
		return browserReply{}, core.Fail(core.InvalidRequest, nil)
	}
	encoded, err := json.Marshal(input)
	if err != nil {
		return browserReply{}, core.Fail(core.InvalidRequest, nil)
	}
	data, err := s.call(ctx, browserSessionCall, encoded)
	if err != nil {
		return browserReply{}, err
	}
	var reply browserReply
	if json.Unmarshal(data, &reply) != nil {
		return browserReply{}, core.Fail(core.InvalidUpstreamResponse, nil)
	}
	switch {
	case reply.SignIn:
		return browserReply{}, core.Fail(core.UpstreamAccessDenied, ErrWebsiteSignIn)
	case reply.Mismatch:
		return browserReply{}, core.Fail(core.UpstreamAccessDenied, ErrWebsiteBindingMismatch)
	case reply.Error == "native":
		return browserReply{}, nativeFailure(reply.Status, reply.NativeError)
	case reply.Error == "pending":
		return browserReply{}, core.Fail(core.UpstreamFailure, errors.New("MediaFire accepted asynchronous folder recycling without confirming completion"))
	case reply.Error == "budget":
		return browserReply{}, core.Fail(core.ResponseBudgetExceeded, nil)
	case reply.Error != "":
		return browserReply{}, core.Fail(core.InvalidUpstreamResponse, nil)
	case reply.Status != http.StatusOK:
		return browserReply{}, httpFailure(reply.Status)
	case !websiteAccount(reply.Account):
		return browserReply{}, core.Fail(core.InvalidUpstreamResponse, nil)
	}
	return reply, nil
}

func (s *BrowserSession) Account(ctx context.Context) (string, error) {
	r, err := s.request(ctx, browserRequest{Operation: "account"})
	return r.Account, err
}

func (s *BrowserSession) List(ctx context.Context, q WebsiteListRequest) (WebsitePage, error) {
	r, err := s.request(ctx, browserRequest{Operation: "list", List: q})
	return WebsitePage{Account: r.Account, Status: r.Status, Body: r.Body}, err
}

var ordinaryContentHost = regexp.MustCompile(`^download[0-9]+\.mediafire\.com$`)

// The ordinary button may be added asynchronously. An empty result keeps the
// host's bounded landing observation open.
const ordinaryButton = `(() => {
	const buttons = document.querySelectorAll("a#downloadButton[href]");
	return buttons.length === 1 ? buttons[0].getAttribute("href") : "";
})`

func (s *BrowserSession) Download(ctx context.Context, file WebsiteFile) (out WebsiteDownload, err error) {
	if s.landing == nil || !websiteAccount(file.Account) || !ordinaryWebsiteURL(file.URL, file.ID) {
		return out, core.Fail(core.InvalidRequest, nil)
	}
	address, err := s.landing(ctx, file.URL, "/file/"+url.PathEscape(file.ID)+"/", ordinaryButton)
	if err != nil {
		return out, err
	}
	if address == "" {
		return out, core.Fail(core.UnsupportedOperation, errors.New("MediaFire did not offer ordinary download"))
	}
	u, err := url.Parse(address)
	if err != nil || u.Scheme != "https" || u.User != nil || u.Fragment != "" || u.Opaque != "" ||
		!ordinaryContentHost.MatchString(u.Host) {
		return out, core.Fail(core.InvalidUpstreamResponse, errors.New("MediaFire returned an invalid ordinary content address"))
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return out, core.Fail(core.InvalidUpstreamResponse, nil)
	}
	req.Header.Set("Accept-Encoding", "identity")
	req.Header.Set("Referer", file.URL)
	resp, err := s.content.Do(req)
	if err != nil {
		return out, httpcontent.UpstreamIOError(err)
	}
	out = WebsiteDownload{Account: file.Account, FileID: file.ID, Response: resp}
	if resp.StatusCode != http.StatusOK {
		return out, fmt.Errorf("MediaFire attachment: %w", httpFailure(resp.StatusCode))
	}
	kind, params, err := mime.ParseMediaType(resp.Header.Get("Content-Disposition"))
	if err != nil || !strings.EqualFold(kind, "attachment") || params["filename"] != file.Name {
		return out, core.Fail(core.InvalidUpstreamResponse, errors.New("MediaFire did not return the listed attachment"))
	}
	return out, nil
}

const browserSessionCall = `(async input => {
	const reply = value => JSON.stringify(value);
	const slot = Symbol.for("mweave.mediafire.website");
	const web = "https://www.mediafire.com";
	if (location.origin !== "https://app.mediafire.com" && location.origin !== web) return reply({error:"origin"});
	const budget = 4 << 20;
	const read = async response => {
		const body = await response.text();
		return body.length > budget ? null : JSON.parse(body);
	};
	const send = (path, body, credentials = "omit", signal) =>
		fetch(web + path, {credentials,redirect:"error",cache:"no-store",method:"POST",body,signal});
	try {
		let state = globalThis[slot];
		if (!state) {
			const issued = await send("/application/get_session_token.php",undefined,"include");
			if (issued.status === 401) return reply({sign_in:true});
			if (!issued.ok) return reply({status:issued.status});
			const grant = await read(issued);
			const token = grant?.response?.session_token;
			if (typeof token !== "string" || !token || token.length > 8192) return reply({error:"grant"});
			const accountResponse = await send("/api/1.5/user/get_info.php",new URLSearchParams({session_token:token,response_format:"json"}));
			if (accountResponse.status === 401) return reply({sign_in:true});
			if (!accountResponse.ok) return reply({status:accountResponse.status});
			const info = (await read(accountResponse))?.response;
			const email = info?.user_info?.email;
			if (info?.action !== "user/get_info" || info?.result !== "Success" || typeof email !== "string" || !email.trim())
				return reply({error:"account"});
			const hash = await crypto.subtle.digest("SHA-256",new TextEncoder().encode(email.trim().toLowerCase()));
			state = {token,account:Array.from(new Uint8Array(hash),b=>b.toString(16).padStart(2,"0")).join("")};
			globalThis[slot] = state;
		}
		const account = state.account;
		if (input.Operation === "account") return reply({account,status:200});
		if (input.Operation === "mkdir") {
			if (input.Account !== account) return reply({mismatch:true});
			const parent = input.ObjectID, name = input.Name;
			if (!/^(?:myfiles|[a-z0-9]{13})$/.test(parent) || typeof name !== "string" || !name)
				return reply({error:"request"});
			const response = await send("/api/1.5/folder/create.php", new URLSearchParams({
				session_token:state.token,response_format:"json",parent_key:parent,foldername:name,action_on_duplicate:"skip"
			}), "omit", AbortSignal.timeout(8000));
			if (response.status === 401) { delete globalThis[slot]; return reply({sign_in:true}); }
			if (!response.ok) return reply({status:response.status});
			const r = (await read(response))?.response;
			if (r?.action !== "folder/create") return reply({error:"receipt"});
			if (r.result === "Error") return reply({status:200,error:"native",nativeError:Number(r.error)||0});
			if (r.result !== "Success") return reply({error:"receipt"});
			if (r.folder_key === undefined && r.name === undefined && r.parent_folderkey === undefined)
				return reply({status:200,account});
			if (!/^[a-z0-9]{13}$/.test(r.folder_key) || r.folder_key === parent || r.name !== name ||
				parent !== "myfiles" && r.parent_folderkey !== parent) return reply({error:"receipt"});
			return reply({status:200,account,objectID:r.folder_key});
		}
		if (input.Operation === "move") {
			if (input.Account !== account) return reply({mismatch:true});
			const id = input.ObjectID, parent = input.ParentID;
			if (!/^(?:[a-z0-9]{11}|[a-z0-9]{15})$/.test(id) || !/^(?:myfiles|[a-z0-9]{13})$/.test(parent))
				return reply({error:"request"});
			const q = {session_token:state.token,response_format:"json",quick_key:id};
			const file = (action, query) => send("/api/1.5/file/" + action + ".php", new URLSearchParams(query), "omit", AbortSignal.timeout(8000));
			const moved = await file("move", {...q,folder_key:parent});
			if (moved.status === 401) { delete globalThis[slot]; return reply({sign_in:true}); }
			if (!moved.ok) return reply({status:moved.status});
			const result = (await read(moved))?.response;
			if (result?.action !== "file/move") return reply({error:"receipt"});
			if (result.result === "Error") return reply({status:200,error:"native",nativeError:Number(result.error)||0});
			if (result.result !== "Success") return reply({error:"receipt"});
			const checked = await file("get_info", q);
			if (checked.status === 401) { delete globalThis[slot]; return reply({sign_in:true}); }
			if (!checked.ok) return reply({status:checked.status});
			const proof = (await read(checked))?.response, item = proof?.file_info;
			if (proof?.action !== "file/get_info" || proof.result !== "Success" || item?.quickkey !== id ||
				typeof item.filename !== "string" || !item.filename) return reply({error:"receipt"});
			return reply({status:200,account,objectID:id,name:item.filename});
		}
		if (input.Operation === "recycle" || input.Operation === "recycle-folder") {
			if (input.Account !== account) return reply({mismatch:true});
			const folder = input.Operation === "recycle-folder";
			const kind = folder ? "folder" : "file";
			const id = input.ObjectID;
			const key = folder ? /^[a-z0-9]{13}$/ : /^(?:[a-z0-9]{11}|[a-z0-9]{15})$/;
			if (typeof id !== "string" || !key.test(id))
				return reply({error:"request"});
			const control = action => send("/api/1.5/" + kind + "/" + action + ".php", new URLSearchParams({
				session_token:state.token,response_format:"json",[folder ? "folder_key" : "quick_key"]:id
			}), "omit", AbortSignal.timeout(8000));
			const deleted = await control("delete");
			if (deleted.status === 401) { delete globalThis[slot]; return reply({sign_in:true}); }
			if (!deleted.ok) return reply({status:deleted.status});
			const result = (await read(deleted))?.response;
			if (result?.action !== kind + "/delete") return reply({error:"receipt"});
			if (result.result === "Error") return reply({status:200,error:"native",nativeError:Number(result.error)||0});
			if (result.result !== "Success") return reply({error:"receipt"});
			if (folder && result.asynchronous !== "no")
				return reply({error:result.asynchronous === "yes" ? "pending" : "receipt"});

			const checked = await control("get_info");
			if (checked.status === 401) { delete globalThis[slot]; return reply({sign_in:true}); }
			if (!checked.ok) return reply({status:checked.status});
			const proof = (await read(checked))?.response;
			const item = folder ? proof?.folder_info : proof?.file_info;
			const objectID = folder ? item?.folderkey : item?.quickkey;
			if (proof?.action !== kind + "/get_info" || proof.result !== "Success" || objectID !== id ||
				typeof item.delete_date !== "string" || !(Date.parse(item.delete_date) > 0)) return reply({error:"receipt"});
			return reply({status:200, account, objectID});
		}
		if (input.Operation.startsWith("upload-")) {
			if (input.Account !== account) return reply({mismatch:true});
			const u = input.Upload;
			const q = {session_token:state.token,response_format:"json"};
			const timeout = AbortSignal.timeout(8000);
			let action, response;
			if (input.Operation === "upload-poll") {
				action = "upload/poll_upload";
				if (!/^[A-Za-z0-9._-]{1,256}$/.test(u.Key)) return reply({error:"request"});
				response = await send("/api/1.5/" + action + ".php", new URLSearchParams({...q,key:u.Key}), "omit", timeout);
			} else {
				if (!/^(?:myfiles|[a-z0-9]{13})$/.test(u.Parent) || !/^[a-f0-9]{64}$/.test(u.Hash) ||
					!Number.isSafeInteger(u.Length) || u.Length<1 || typeof u.Name!=="string" || !u.Name) return reply({error:"request"});
				q.folder_key = u.Parent;
				if (input.Operation === "upload-check") {
					action = "upload/check";
					response = await send("/api/1.5/" + action + ".php", new URLSearchParams({...q,filename:u.Name,
						size:String(u.Length),hash:u.Hash,resumable:"yes"}), "omit", timeout);
				} else if (input.Operation === "upload-unit") {
					action = "upload/resumable";
					if (!Number.isSafeInteger(u.Unit??0) || (u.Unit??0)<0 || typeof u.Data!=="string" ||
						u.Data.length>5592408) return reply({error:"request"});
					const decoded = atob(u.Data);
					const bytes = Uint8Array.from(decoded, b=>b.charCodeAt(0));
					if (bytes.length<1 || bytes.length>(4<<20) || bytes.length>u.Length) return reply({error:"request"});
					const hash = await crypto.subtle.digest("SHA-256",bytes);
					const unitHash = Array.from(new Uint8Array(hash), b=>b.toString(16).padStart(2,"0")).join("");
					const query = new URLSearchParams({...q,action_on_duplicate:"skip"});
					response = await fetch(web+"/api/1.5/"+action+".php?"+query,{credentials:"omit",redirect:"error",
						cache:"no-store",method:"POST",body:bytes,signal:timeout,headers:{
							"Content-Type":"application/octet-stream","X-Filename":encodeURIComponent(u.Name),
							"X-Filesize":String(u.Length),"X-Filehash":u.Hash,"X-Unit-Id":String(u.Unit??0),
							"X-Unit-Size":String(bytes.length),"X-Unit-Hash":unitHash}});
				} else return reply({error:"request"});
			}
			if (response.status === 401) { delete globalThis[slot]; return reply({sign_in:true}); }
			if (!response.ok) return reply({status:response.status});
			const native = (await read(response))?.response;
			if (native?.action !== action) return reply({error:"receipt"});
			if (native.result === "Error") return reply({status:200,error:"native",nativeError:Number(native.error)||0});
			if (native.result !== "Success") return reply({error:"receipt"});
			const upload = native.doupload;
			if (action === "upload/poll_upload") {
				if (!upload || Number(upload.result)!==0) return reply({error:"receipt"});
				return reply({status:200,account,body:{terminal:Number(upload.status),failure:Number(upload.fileerror)||0,
					fileKey:upload.quickkey,name:upload.filename,size:Number(upload.size),hash:upload.hash}});
			}
			if (action === "upload/resumable" && Number(upload?.result)!==0)
				return reply({status:200,error:"native",nativeError:Number(upload?.result)||0});
			if (action === "upload/check" && native.file_exists === "yes")
				return reply({status:200,account,body:{exists:true}});
			const geometry = native.resumable_upload;
			if (!geometry || !Array.isArray(geometry.bitmap?.words) ||
				Number(geometry.bitmap.count)!==geometry.bitmap.words.length ||
				geometry.all_units_ready!=="yes" && geometry.all_units_ready!=="no") return reply({error:"receipt"});
			return reply({status:200,account,body:{key:action==="upload/resumable"?upload?.key:geometry.upload_key,
				unitBytes:Number(geometry.unit_size),units:Number(geometry.number_of_units),
				bitmap:geometry.bitmap.words.map(Number),ready:geometry.all_units_ready==="yes"}});
		}
		if (input.Operation === "list") {
			const q = input.List;
			const response = await send("/api/1.5/folder/get_content.php", new URLSearchParams({
				session_token:state.token,response_format:"json",folder_key:q.FolderKey,
				content_type:q.ContentType,chunk:String(q.Chunk),chunk_size:String(q.ChunkSize),details:q.Chunk === 1 ? "yes" : "no"
			}));
			if (response.status === 401) { delete globalThis[slot]; return reply({sign_in:true}); }
			if (!response.ok) return reply({status:response.status});
			const body = await read(response);
			if (body === null) return reply({error:"budget"});
			const native = body?.response;
			const content = native?.folder_content;
			const page = {action:native?.action,result:native?.result,error:native?.error};
			if (content) page.folder_content = {
				folderkey:content.folderkey,content_type:content.content_type,chunk_number:content.chunk_number,
				more_chunks:content.more_chunks,
				folders:content.folders?.map(f=>({folderkey:f.folderkey,name:f.name,parent_folderkey:f.parent_folderkey})),
				files:content.files?.map(f=>({quickkey:f.quickkey,filename:f.filename,size:f.size,
					parent_folderkey:f.parent_folderkey,links:{normal_download:f.links?.normal_download}}))
			};
			return reply({account,status:200,body:{response:page}});
		}
		return reply({error:"request"});
	} catch {
		return reply({error:"response"});
	}
})`
