package quark

const websiteBrowserAccount = `
	const subject = () => {
		const ids = document.cookie.split(";").map(value => value.trimStart())
			.filter(value => value.startsWith("__uid=")).map(value => value.slice(6));
		return ids.length === 1 && ids[0] !== "0" && ids[0].length <= 512 ? ids[0] : "";
	};
	const digest = async uid => {
		const hash = await crypto.subtle.digest("SHA-256", new TextEncoder().encode("mweave:quark:browser-uid:" + uid));
		return Array.from(new Uint8Array(hash), byte => byte.toString(16).padStart(2,"0")).join("");
	};
`

const websiteFileRequest = `async ({account, parent, page, size, fid, recycle_until}) => {
	if (window.top !== window || location.origin !== "` + WebsiteOrigin + `") return '{"sign_in":true}';
` + websiteBrowserAccount + `
	const uid = subject();
	if (!uid) return '{"sign_in":true}';
	const binding = await digest(uid);
	if (subject() !== uid || account && account !== binding) return '{"conflict":true}';
	const api = "https://drive-pc.quark.cn/1/clouddrive";
	const params = new URLSearchParams({pr: "ucpro", fr: "pc"});
	const read = async (address, options, timeout = 3500) => {
		try {
			const response = await fetch(address, {...options, credentials:"include", redirect:"error", cache:"no-store",
				signal:AbortSignal.timeout(timeout)});
			const payload = await response.json().catch(() => null);
			return {http:response.status, code:Number.isSafeInteger(payload?.code) ? payload.code : undefined, payload};
		} catch (error) {
			return {http:error.name === "TimeoutError" ? 408 : 503};
		}
	};
	if (recycle_until !== undefined) {
		if (!account || !Number.isSafeInteger(recycle_until) || typeof fid !== "string" || fid === "0" ||
			!/^[A-Za-z0-9_-]{1,128}$/.test(fid)) return "{}";
		const remaining = recycle_until - Date.now();
		if (remaining <= 0) return '{"http":408}';
		const issued = await read(api + "/file/delete?" + params, {
			method:"POST", headers:{"Content-Type":"application/json"},
			body:JSON.stringify({action_type:2,filelist:[fid],exclude_fids:[]})
		}, Math.min(remaining, 3500));
		if (issued.http !== 200 || issued.code !== 0) return JSON.stringify({http:issued.http,code:issued.code});
		const rawTask = issued.payload.data?.task_id;
		const task = typeof rawTask === "string" || Number.isSafeInteger(rawTask) ? String(rawTask) : "";
		if (!/^[A-Za-z0-9_-]{1,128}$/.test(task)) return "{}";
		params.set("task_id", task);
		const ms = issued.payload.metadata?.tq_gap;
		const gap = Number.isSafeInteger(ms) ? Math.max(1, ms) : 1000;
		const deadline = Math.min(recycle_until, Date.now() + 8000);
		for (let index = 0; Date.now() + gap < deadline; index++) {
			await new Promise(resolve => setTimeout(resolve, gap));
			const remain = deadline - Date.now();
			if (remain <= 0) break;
			if (subject() !== uid) return '{"conflict":true}';
			params.set("retry_index", String(index));
			const r = await read(api + "/task?" + params, {}, Math.min(remain, 3500));
			if (r.http !== 200 || r.code !== 0) return JSON.stringify({http:r.http,code:r.code});
			const result = r.payload.data;
			if (result?.task_id != null && String(result.task_id) !== task) return "{}";
			switch (result?.status) {
			case 0:
			case 1:
				continue;
			case 2:
				return subject() === uid ? JSON.stringify({http:200,code:0,account:binding,task}) : '{"conflict":true}';
			case 3:
				return '{"http":502}';
			default:
				return "{}";
			}
		}
		return '{"http":408}';
	}
	const address = new URL(api + "/file/" + (fid ? "download" : "sort"));
	address.search = params;
	const options = {};
	if (fid) {
		options.method = "POST";
		options.headers = {"Content-Type": "application/json"};
		options.body = JSON.stringify({fids: [fid]});
	} else {
		for (const [key, value] of Object.entries({pdir_fid: parent, _page: page, _size: size, _fetch_total: 1})) {
			address.searchParams.set(key, value);
		}
	}
	const {http, code, payload} = await read(address, options);
	const out = {http, code};
	if (out.http !== 200 || code !== 0) return JSON.stringify(out);
	if (subject() !== uid) return '{"conflict":true}';
	out.account = binding;

	// Preserve decimal strings and reject numbers rounded by the browser.
	const exact = (value) => value == null || typeof value === "string" || Number.isSafeInteger(value) ? value : false;
	if (fid) {
		if (Array.isArray(payload.data) && payload.data.length === 1) {
			const {fid, size, download_url} = payload.data[0] ?? {};
			let address;
			if (typeof download_url === "string" && /^https:\/\/[^@/?#\\]+(?:[/?]|$)/i.test(download_url) && !download_url.includes("#")) {
				// Fetch reports the browser's canonical URL. Keep the signed query untouched.
				try { address = new URL(download_url).href; } catch {}
			}
			out.grants = [{fid, size: exact(size), download_url: address}];
		}
	} else {
		out.page = Number.isSafeInteger(payload.metadata?._page) ? payload.metadata._page : undefined;
		out.total = Number.isSafeInteger(payload.metadata?._total) ? payload.metadata._total : undefined;
		if (Array.isArray(payload.data?.list) && payload.data.list.length <= size) {
			out.items = payload.data.list.map((row) => {
				const {fid, pdir_fid, file_name, dir, file, file_type, size} = row ?? {};
				return {fid, pdir_fid, file_name, dir, file, file_type: exact(file_type), size: exact(size)};
			});
		}
	}
	return JSON.stringify(out);
}`

const websiteStartDownload = `async ({url, range, account}) => {
	if (location.origin !== "` + WebsiteOrigin + `" || window !== window.top) return '{"sign_in":true}';
` + websiteBrowserAccount + `
	const uid = subject();
	if (!uid) return '{"sign_in":true}';
	if (await digest(uid) !== account || subject() !== uid) return '{"conflict":true}';
	const key = Symbol.for("mweave.quark.download");
	if (globalThis[key]) return "{}";
	const controller = globalThis[key] = new AbortController();
	const headers = range ? {"Range": range} : {};
	void fetch(url, {credentials:"include", redirect:"error", cache:"no-store", headers,
		signal:controller.signal}).then(reply => reply.body?.cancel()).catch(() => {});
	return JSON.stringify({origin:location.origin, cross:new URL(url).origin !== location.origin});
}`

const websiteStopDownload = `() => {
	const key = Symbol.for("mweave.quark.download");
	globalThis[key]?.abort();
	delete globalThis[key];
	return "{}";
}`
