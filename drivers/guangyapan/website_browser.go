package guangyapan

// The official web client stores its browser session through the Guangya login
// SDK. Only the current access ticket is used here. The SDK owns login and
// renewal, and neither the ticket nor the native account identifier leaves the page.
const browserCall = `(async q => {
	const encode = value => JSON.stringify(value);
	if (location.origin !== "https://www.guangyapan.com" || window !== window.top ||
		location.pathname.startsWith("/s/") || location.pathname.startsWith("/oauth")) return encode({sign_in:true});
	const client = "aMe-8VSlkrbQXpUR";
	try {
		let session;
		for (let attempt = 0; attempt < 10; attempt++) {
			session = JSON.parse(localStorage.getItem("credentials_" + client));
			if (typeof session?.access_token === "string" && session.access_token && typeof session.sub === "string" && session.sub &&
				(!session.expires_at || Date.parse(session.expires_at) > Date.now())) break;
			session = null;
			if (attempt < 9) await new Promise(resolve => setTimeout(resolve, 150));
		}
		if (!session) return encode({sign_in:true});
		const digest = async value => {
			const bytes = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(value));
			return Array.from(new Uint8Array(bytes), b => b.toString(16).padStart(2, "0")).join("");
		};
		const Account = await digest("guangyapan:website:" + session.sub);
		if (q.Account && q.Account !== Account) return encode({Mismatch:true});
		const appAPI = "https://api.guangyapan.com";
		const accountAPI = "https://account.guangyapan.com";
		const Authorization = "Bearer " + session.access_token;
		const headers = {Authorization,dt:"4","Content-Type":"application/json"};
		const device = localStorage.getItem("swangpan_web_device_id");
		if (device) headers.did = device;
		const appState = localStorage.getItem("shumei_id:v1");
		if (appState) headers.smid = appState;
		const request = async (url, options) => {
			const response = await fetch(url, {...options,redirect:"error",signal:AbortSignal.timeout(2500)});
			if (!response.ok) return {Status:response.status};
			const raw = await response.text();
			if (raw.length > (4 << 20)) return {Status:0};
			return {Status:response.status, body:JSON.parse(raw)};
		};
		const app = async (route, body) => {
			const result = await request(appAPI + route, {method:"POST",credentials:"omit",headers,body:body === undefined ? undefined : JSON.stringify(body)});
			const b = result.body;
			if (result.Status !== 200) return result;
			return {Status:200, Code:b?.code || 0, data:b?.data};
		};
		const status = r => ({Status:r.Status, Code:r.Code});
		const verifiedSession = Symbol.for("mweave.guangyapan.verified-session");
		const ticketBinding = await digest(session.sub + ":" + session.access_token);
		if (globalThis[verifiedSession] !== ticketBinding) {
			const current = await request(accountAPI+"/v1/user/me", {method:"GET",credentials:"include",
				headers:{Authorization,"x-client-id":client,"x-sdk-version":"9.1.3","x-protocol-version":"301"}});
			if (current.Status !== 200) return encode(status(current));
			if (typeof current.body?.sub !== "string" || current.body.sub !== session.sub) return encode({Mismatch:true});
			globalThis[verifiedSession] = ticketBinding;
		}
		if (q.Operation === "account") return encode({Status:200, Account});
		if (q.Operation === "list") {
			const r = await app("/userres/v1/file/get_file_list", {orderBy:0,page:q.Index,pageSize:q.PageSize,parentId:q.ParentID,sortType:0});
			if (r.Status !== 200 || r.Code) return encode(status(r));
			if (!r.data || Array.isArray(r.data) || typeof r.data !== "object") return encode({Status:0});
			const empty = Object.keys(r.data).length === 0;
			const List = empty ? [] : r.data.list;
			const Total = empty ? 0 : r.data.total;
			if (!Array.isArray(List) || !Number.isSafeInteger(Total)) return encode({Status:0});
			return encode({Status:200, Account, Total, Page:r.data.page, List:List.map(f=>({
				fileId:f.fileId,fileName:f.fileName,parentId:f.parentId,fileSize:f.fileSize,resType:f.resType,utime:f.utime
			}))});
		}
		if (q.Operation === "mkdir") {
			const r = await app("/userres/v1/file/create_dir", {parentId:q.ParentID, dirName:q.Name, failIfNameExist:true});
			return encode({...status(r), Account, Folder:{fileId:r.data?.fileId,fileName:r.data?.fileName,
				parentId:r.data?.parentId,resType:r.data?.resType}});
		}
		if (q.Operation === "rename" && typeof q.FileID === "string" && q.FileID && typeof q.Name === "string" && q.Name) {
			const r = await app("/userres/v1/file/rename", {fileId:q.FileID, newName:q.Name});
			return encode({...status(r), Account});
		}
		if (q.Operation === "move" && q.Account === Account &&
			typeof q.FileID === "string" && q.FileID && typeof q.ParentID === "string") {
			const r = await app("/userres/v1/file/move_file", {fileIds:[q.FileID], parentId:q.ParentID});
			return encode({...status(r), Account, TaskID:r.data?.taskId});
		}
		if (q.Operation === "move-status" && q.Account === Account && typeof q.TaskID === "string" && q.TaskID) {
			const r = await app("/userres/v1/get_task_status", {taskId:q.TaskID});
			return encode({...status(r), Account, TaskID:r.data?.taskId ?? q.TaskID,
				State:r.data?.status, DetailCode:r.data?.detail?.code});
		}
		if (q.Operation === "download" && typeof q.FileID === "string" && q.FileID) {
			const maxOrdinarySize = 10 * 1024 * 1024;
			if (q.Size > maxOrdinarySize) {
				const assets = await app("/assets/v1/get_assets");
				if (assets.Status !== 200 || assets.Code) return encode(status(assets));
				if (assets.data?.vipStatus !== 2 && assets.data?.svipStatus !== 2) return encode({Denied:true});
			} else {
				const size = await app("/nd.bizuserres.s/v1/get_files_size", {fileIds:[q.FileID]});
				if (size.Status !== 200 || size.Code) return encode(status(size));
				const n = size.data?.totalSize;
				if (typeof n !== "number" && typeof n !== "string" || BigInt(n) !== BigInt(q.Size)) return encode({Status:0});
			}
			const grant = await app("/userres/v1/get_res_download_url", {fileId:q.FileID});
			if (grant.Status !== 200 || grant.Code) return encode(status(grant));
			return encode({Status:200, Account, FileID:q.FileID, SignedURL:grant.data?.signedURL});
		}
		return encode({Status:0});
	} catch { return encode({Status:0}); }
})`
