package mobilepan

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"sync"

	core "github.com/nostalume/mountweave"
)

const WebsiteURL = "https://yun.139.com/w/"

// Website calls fixed file operations inside an authenticated Mobile Cloud page.
// The caller owns the session until Open returns a Driver.
type Website struct {
	Call    func(context.Context, string, json.RawMessage) (json.RawMessage, error)
	Release func() error
	// ExpectedRootID binds Open to a previously verified folder when set.
	ExpectedRootID string
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
// The content client receives only the short-lived original-file URL.
func (w Website) Open(ctx context.Context, accountID, rootPath string, content *http.Client) (*Driver, error) {
	if w.Call == nil || w.Release == nil || !validAccountID(accountID) {
		return nil, core.Fail(core.InvalidRequest, nil)
	}
	d, err := open(ctx, Options{RootPath: rootPath, ContentClient: content}, w, accountID)
	if err != nil {
		return nil, err
	}
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
	case http.StatusUnauthorized, http.StatusForbidden:
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
	if (route !== "account" && route !== "list" && route !== "getDownloadUrl") return "{}";
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
		const reply = await send({url:u.href+"/file/"+route, method:"POST", headers:{}, data, timeout:6500});
		const failure = identityFailure();
		if (failure) return failure;
		if (!reply?.success) {
			const code = String(reply?.code ?? "");
			return JSON.stringify({success:false, code:/^[0-9]{1,12}$/.test(code)?code:""});
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
