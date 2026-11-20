package uc

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/url"

	core "github.com/nostalume/mountweave"
)

const (
	websiteOrigin   = "https://drive.uc.cn"
	apiOrigin       = "https://pc-api.uc.cn"
	ucGrantEndpoint = apiOrigin + "/1/clouddrive/file/download"
)

// WebsiteURL is the page where a user signs in to UC Cloud Drive.
const WebsiteURL = websiteOrigin + "/"

var (
	ErrWebsiteSignIn          = errors.New("the UC website needs sign-in")
	ErrWebsiteBindingMismatch = errors.New("the UC website account or folder changed")
)

// WebsiteCall evaluates a UC page function with one JSON argument.
type WebsiteCall = func(context.Context, string, json.RawMessage) (json.RawMessage, error)

// WebsiteOptions binds an authenticated page to a selected UC folder.
// The caller retains the page on failure. The Driver owns Release on success.
type WebsiteOptions struct {
	Tab              context.Context
	Call             WebsiteCall
	Account          string
	RootPath, RootID string
	Release          func() error

	// Origin and GrantEndpoint override the download endpoints for loopback fixtures.
	Origin, GrantEndpoint string
}

func (o WebsiteOptions) downloadEndpoints() (string, string, error) {
	if o.Origin == "" && o.GrantEndpoint == "" {
		return websiteOrigin, ucGrantEndpoint, nil
	}
	site, siteErr := url.Parse(o.Origin)
	grant, grantErr := url.Parse(o.GrantEndpoint)
	if siteErr != nil || grantErr != nil || !loopbackHTTPS(site) || !loopbackHTTPS(grant) ||
		site.Path != "" || grant.Path == "" {
		return "", "", core.Fail(core.InvalidRequest, nil)
	}
	return o.Origin, o.GrantEndpoint, nil
}

func loopbackHTTPS(u *url.URL) bool {
	return u.Scheme == "https" && net.ParseIP(u.Hostname()).IsLoopback() &&
		u.User == nil && u.RawQuery == "" && !u.ForceQuery && u.Fragment == ""
}

// WebsiteAccount verifies the account in the authenticated page.
func WebsiteAccount(ctx context.Context, call WebsiteCall) (string, error) {
	if call == nil {
		return "", core.Fail(core.InvalidRequest, nil)
	}
	reply, err := readUCMetadata(ctx, call, ucMetadataRequest{Operation: "account"})
	return reply.Account, err
}

const ucBrowserMetadata = `async ({operation, account, parent, page}) => {
	const website = "` + websiteOrigin + `";
	if (window !== window.top || location.origin !== website)
		return JSON.stringify({status:401});
	const read = async url => {
		try {
			const response = await fetch(url, {credentials:"include", redirect:"error", signal:AbortSignal.timeout(3500)});
			return [response.status, response.ok ? await response.json().catch(() => null) : null];
		} catch (error) { return [error?.name === "TimeoutError" ? 408 : 503, null]; }
	};
	const identity = async () => {
		const [status, reply] = await read(website + "/account/info");
		if (status !== 200) return {status};
		if (reply === null) return {status:0};
		const uid = reply?.success === true ? reply?.data?.uid : null;
		if (!((typeof uid === "string" && /^[A-Za-z0-9_-]{1,128}$/.test(uid) && uid !== "0") ||
			(Number.isSafeInteger(uid) && uid > 0))) return {status:401};
		const bytes = new TextEncoder().encode("mweave:uc-drive:" + uid);
		const hash = new Uint8Array(await crypto.subtle.digest("SHA-256", bytes));
		return {status, account:Array.from(hash, b => b.toString(16).padStart(2,"0")).join("")};
	};
	const before = await identity();
	if (operation === "account" || before.status !== 200 || before.account !== account)
		return JSON.stringify(before);
	if (operation !== "page" || !/^[A-Za-z0-9_-]{1,128}$/.test(parent) || !Number.isSafeInteger(page) || page < 1)
		return "{}";
	const query = new URLSearchParams({
		pr:"UCBrowser", fr:"pc", pdir_fid:parent, _page:String(page), _size:"50",
		_fetch_total:"1", _sort:"file_type:asc,updated_at:desc"
	});
	const [http, reply] = await read("` + apiOrigin + `/1/clouddrive/file/sort?" + query);
	if (http !== 200) return JSON.stringify({status:http});
	if (Number.isSafeInteger(reply?.status) && reply.status !== 200)
		return JSON.stringify({status:reply.status});
	const rows = reply?.data?.list;
	if (reply?.status !== 200 || !Array.isArray(rows) || rows.length > 50 ||
		!Number.isSafeInteger(reply?.metadata?._total) || reply?.metadata?._page !== page)
		return "{}";
	const after = await identity();
	if (after.status !== 200 || after.account !== account) return JSON.stringify(after);
	const items = rows.map(row => ({
		fid:row?.fid, parent:row?.pdir_fid, name:row?.file_name,
		kind:row?.file_type, size:Number.isSafeInteger(row?.size) ? row.size : null
	}));
	return JSON.stringify({...after, page, total:reply.metadata._total, items});
}`
