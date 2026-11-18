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

const websiteReadScript = `async ({account, parent, page, size, fid}) => {
	if (window.top !== window || location.origin !== "` + WebsiteOrigin + `") return '{"sign_in":true}';
` + websiteBrowserAccount + `
	const uid = subject();
	if (!uid) return '{"sign_in":true}';
	const binding = await digest(uid);
	if (subject() !== uid || account && account !== binding) return '{"conflict":true}';
	const address = new URL("https://drive-pc.quark.cn/1/clouddrive/file/" + (fid ? "download" : "sort"));
	address.search = new URLSearchParams({pr: "ucpro", fr: "pc"});
	const options = {credentials: "include", redirect: "error", cache: "no-store", signal: AbortSignal.timeout(3500)};
	if (fid) {
		options.method = "POST";
		options.headers = {"Content-Type": "application/json"};
		options.body = JSON.stringify({fids: [fid]});
	} else {
		for (const [key, value] of Object.entries({pdir_fid: parent, _page: page, _size: size, _fetch_total: 1})) {
			address.searchParams.set(key, value);
		}
	}
	let response;
	try {
		response = await fetch(address, options);
	} catch (error) {
		return JSON.stringify({http: error.name === "TimeoutError" ? 408 : 503});
	}
	let payload;
	try {
		payload = await response.json();
	} catch {
		return JSON.stringify({http: response.status});
	}
	const code = Number.isSafeInteger(payload?.code) ? payload.code : undefined;
	const out = {http: response.status, code};
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
