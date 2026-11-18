package alipan

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"

	core "github.com/nostalume/mountweave"
)

const (
	websiteOrigin = "https://www.alipan.com/"
	WebsiteURL    = websiteOrigin + "drive"
)

// WebsiteWritePaths lists automatic website operations that must be blocked
// before loading a page for read access.
func WebsiteWritePaths() []string { return []string{"/v2/user/driveMigrate"} }

var ErrWebsiteSignIn = errors.New("sign in to the Aliyun Drive website")

// Website calls source-owned functions in an isolated authenticated page.
// Call receives the function and its JSON argument. Release relinquishes the
// session. The caller owns the session until Open returns a Driver.
type Website struct {
	Call    func(context.Context, string, json.RawMessage) (json.RawMessage, error)
	Release func() error
}

type WebsiteAccount struct {
	ID            string `json:"user_id"`
	Name          string `json:"nick_name"`
	ResourceDrive string `json:"resource_drive_id"`
	website       Website
}

const websiteRequest = `async ({operation, data}) => {
	if (window !== window.top || location.origin !== "https://www.alipan.com")
		return JSON.stringify({sign_in: true});
	const routes = {
		account: ["/user/get", {version: "v2", hostType: "user"}],
		file: ["/file/get", {}],
		page: ["/file/list", {version: "v3", prefix: "/adrive"}],
		create: ["/file/createWithFolders", {version: "v2", prefix: "/adrive"}],
		upload_url: ["/file/get_upload_url", {}],
		complete: ["/file/complete", {}],
		move: ["/file/move", {version: "v2"}],
		trash: ["/recyclebin/trash", {}],
		download: ["/file/get_download_url", {encryptedFields: ["encrypt_url", "encrypt_cdn_url"]}]
	};
	const projectFile = f => f && ({
		file_id: f.file_id, drive_id: f.drive_id, parent_file_id: f.parent_file_id,
		name: f.name, type: f.type, size: f.size, updated_at: f.updated_at, punish_flag: f.punish_flag
	});
	const [url, option] = routes[operation];
	const channel = new MessageChannel();
	try {
		return await new Promise(resolve => {
			const timeout = setTimeout(() => resolve(JSON.stringify({timeout: true})), 8000);
			channel.port1.onmessage = event => {
				clearTimeout(timeout);
				try {
					const reply = event.data;
					if (reply?.status !== "success") {
						resolve(JSON.stringify({status: Number(reply?.data?.status) || 0, code: reply?.data?.code || ""}));
						return;
					}
					const v = reply.value;
					let value;
					if (operation === "account") value = {user_id: v?.user_id, nick_name: v?.nick_name, resource_drive_id: v?.resource_drive_id};
					else if (operation === "file") value = projectFile(v);
					else if (operation === "page") value = {items: Array.isArray(v?.items) ? v.items.map(projectFile) : null, next_marker: v?.next_marker};
					else if (operation === "download") value = {url: v?.url || v?.encrypt_url, method: "GET"};
					else if (operation === "trash") value = projectFile(v) || {};
					else if (v && typeof v === "object") value = {...projectFile(v), name: v.name || v.file_name,
						file_name: v.file_name || v.name, exist: v.exist, upload_id: v.upload_id, available: v.available,
						rapid_upload: v.rapid_upload, async_task_id: v.async_task_id,
						part_info_list: v.part_info_list?.map(p => ({part_number: p.part_number, upload_url: p.upload_url}))};
					const encoded = JSON.stringify({value});
					resolve(encoded.length <= 1048576 ? encoded : JSON.stringify({oversize: true}));
				} catch {
					resolve("{}");
				}
			};
			window.postMessage({type: "page-channel-port:requestClient", requestData: {url, data, option, needRefreshToken: true}}, location.origin, [channel.port2]);
		});
	} finally {
		channel.port1.close();
	}
}`

func (w Website) request(ctx context.Context, operation string, data any, out any) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	args, err := json.Marshal(struct {
		Operation string `json:"operation"`
		Data      any    `json:"data"`
	}{operation, data})
	if err != nil {
		return err
	}
	raw, err := w.Call(ctx, websiteRequest, args)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(raw) > 1<<20 {
		return core.Fail(core.ResponseBudgetExceeded, nil)
	}
	var reply struct {
		Value    json.RawMessage `json:"value"`
		Status   int             `json:"status"`
		Code     string          `json:"code"`
		SignIn   bool            `json:"sign_in"`
		Timeout  bool            `json:"timeout"`
		Oversize bool            `json:"oversize"`
	}
	if err := json.Unmarshal(raw, &reply); err != nil {
		return core.Fail(core.InvalidUpstreamResponse, nil)
	}
	switch {
	case reply.SignIn || reply.Status == 401 || reply.Code == "AccessTokenExpired" || reply.Code == "AccessTokenInvalid":
		return core.Fail(core.UpstreamAccessDenied, ErrWebsiteSignIn)
	case reply.Timeout:
		return core.Fail(core.UpstreamTimeout, errors.New("Aliyun Drive's website did not answer"))
	case reply.Oversize:
		return core.Fail(core.ResponseBudgetExceeded, nil)
	case operation == "file" && reply.Status == http.StatusNotFound:
		return core.Fail(core.NotFound, nil)
	case reply.Status != 0 || reply.Code != "":
		return statusError(reply.Status, reply.Code, nil)
	case len(reply.Value) == 0 || string(reply.Value) == "null":
		return core.Fail(core.InvalidUpstreamResponse, nil)
	}
	if err := json.Unmarshal(reply.Value, out); err != nil {
		return core.Fail(core.InvalidUpstreamResponse, nil)
	}
	return nil
}

// Account checks the session without changing any drive state.
func (w Website) Account(ctx context.Context) (WebsiteAccount, error) {
	if w.Call == nil || w.Release == nil {
		return WebsiteAccount{}, core.Fail(core.InvalidRequest, nil)
	}
	var account WebsiteAccount
	if err := w.request(ctx, "account", struct{}{}, &account); err != nil {
		return WebsiteAccount{}, err
	}
	if account.ID == "" {
		return WebsiteAccount{}, core.Fail(core.InvalidUpstreamResponse, errors.New("Aliyun Drive did not identify the account"))
	}
	if account.ResourceDrive == "" {
		return WebsiteAccount{}, core.Fail(core.NotFound, errors.New("Aliyun Drive did not identify a resource drive"))
	}
	account.website = w
	return account, nil
}

// websiteDriver exposes the read capabilities of the Aliyun Drive tree reader.
type websiteDriver struct{ core.Driver }

// Open binds the selected resource-drive directory. On success the Driver owns
// the session lease. On failure the caller must release the session.
func (a WebsiteAccount) Open(ctx context.Context, rootID, rootPath string, client *http.Client, release func() error) (core.Driver, string, error) {
	d, root, err := a.open(ctx, rootID, rootPath, client, release)
	if err != nil {
		return nil, "", err
	}
	return &websiteDriver{d}, root, nil
}

func (a WebsiteAccount) open(ctx context.Context, rootID, rootPath string, client *http.Client, release func() error) (*Driver, string, error) {
	if a.website.Call == nil || a.ID == "" || a.ResourceDrive == "" || (rootID == "") == (rootPath == "") {
		return nil, "", core.Fail(core.InvalidRequest, nil)
	}
	if release != nil {
		a.website.Release = release
	}
	if client == nil {
		client = http.DefaultClient
	}
	d := &Driver{
		opts:    Options{DriveID: a.ResourceDrive, RootID: "root", MaxResolvePages: 100},
		client:  isolatedClient(client),
		website: &a.website,
	}
	switch {
	case rootPath != "":
		root, err := d.resolve(ctx, rootPath)
		if err != nil {
			return nil, "", err
		}
		if root.Type != "folder" {
			return nil, "", core.Fail(core.NotDirectory, nil)
		}
		rootID = root.ID
	case rootID != "root":
		if err := a.checkRootIdentity(ctx, rootID); err != nil {
			return nil, "", err
		}
	}
	d.opts.RootID = rootID
	if _, _, err := d.page(ctx, rootID, "", 1); err != nil {
		return nil, "", err
	}
	return d, rootID, nil
}

func (a WebsiteAccount) checkRootIdentity(ctx context.Context, rootID string) error {
	var root nativeFile
	if err := a.website.request(ctx, "file", map[string]string{"drive_id": a.ResourceDrive, "file_id": rootID}, &root); err != nil {
		return err
	}
	if root.Drive != a.ResourceDrive || root.ID != rootID {
		return core.Fail(core.InvalidUpstreamResponse, nil)
	}
	if root.Type != "folder" {
		return core.Fail(core.NotDirectory, nil)
	}
	return nil
}

// CheckRoot verifies a second folder using a retained session, without owning it.
func (a WebsiteAccount) CheckRoot(ctx context.Context, rootID string) error {
	if a.website.Call == nil || a.ResourceDrive == "" || rootID == "" {
		return core.Fail(core.InvalidRequest, nil)
	}
	if rootID != "root" {
		if err := a.checkRootIdentity(ctx, rootID); err != nil {
			return err
		}
	}
	d := &Driver{opts: Options{DriveID: a.ResourceDrive}, website: &a.website}
	_, _, err := d.page(ctx, rootID, "", 1)
	return err
}
