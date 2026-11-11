# Driver capabilities

See [Choosing a driver](driver-recommendations.md) for workloads, quotas and setup.
Each driver reference below lists its configuration and native limits.

## Behaviors by driver

Every listed driver supports `Stat`, `List` and ordinary streaming `Open` for the
scope shown. The stream can be a `Copy` source. Range reads depend on the native
response. FTP and CodeArts Repo return complete bodies on range requests.
Google Drive, Feishu and Tencent Docs do the same for document exports.

**Create** adds a new file through `Write`. **Publish** commits after receiving
the complete input and can replace a file where supported. **Subtree** selects
explicit recursive removal. A dash means the driver is read-only. Batches,
tree operations and cross-mount Copy use these capabilities. Cross-mount Move
also requires safe source deletion.

### Filesystems, protocols and object stores

| Driver | Read scope and extra capabilities | Changes |
| --- | --- | --- |
| [Local](../drivers/local/doc.go) | Rooted filesystem, byte ranges, LSTAT, bound reads and live editing on supported platforms | Create, staged Publish, Mkdir, typed removal and same-mount Move |
| [FTP](../drivers/ftp/README.md) | FTP or FTPS directory, complete-body range fallback | Mkdir |
| [SFTP](../drivers/sftp/README.md) | SSH filesystem tree and byte ranges | Create, Mkdir |
| [SMB](../drivers/smb/README.md) | Share or subdirectory, exact name lookup and byte ranges | Create, Mkdir |
| [WebDAV](../drivers/webdav/README.md) | HTTPS collection with standard or supported vendor pagination | Create, Mkdir and Subtree where the server supports the native operation |
| [S3](../drivers/s3/README.md) | Exact keys and tree view, byte ranges, native conditions and pinned versions where available | Create, buffered Publish and continuation, directory markers, file removal and Subtree. Check the service profile |
| [Azure Blob](../drivers/azureblob/README.md) | Exact keys, versions, native read conditions and start/end ranges | Create and Publish for block blobs with a write budget, plus conditional removal of the current key |
| [Bunny Storage](../drivers/bunny/README.md) | REST storage zone directory and actual ranges | Subtree |
| [UPYUN USS](../drivers/upyun/README.md) | Service directory and actual ranges | Mkdir and synchronous Remove |
| [Pool](../drivers/pool/README.md) | Logical filesystem across members, pinned reads and bound directory scopes | Create, Publish, Mkdir, Remove and Move, with pool generation conditions |

### Chinese cloud drives

| Driver | Read scope and extra capabilities | Changes |
| --- | --- | --- |
| [Aliyun Drive Open](../drivers/alipan/README.md) | Authorized root or folder. Frozen files remain visible, with content reads refused | Optional known-length Create, Mkdir, file removal, Subtree, same-parent rename and same-name file move. Coordinate concurrent same-name Mkdir in the host |
| [BabelBird](../drivers/babelbird/README.md) | Enterprise or personal space, with optional subdirectory | — |
| [Baidu Netdisk](../drivers/baidupan/README.md) | App-authorized directory and ordinary downloads | Mkdir and Create. Larger uploads require a stable replayable source |
| [DingTalk Drive](../drivers/dingtalkdrive/README.md) | Approved space and folder for the configured operator | — |
| [Fangcloud](../drivers/fangcloud/README.md) | Authorized personal space or folder | — |
| [Feishu Drive](../drivers/feishudrive/README.md) | Folder, uploaded originals and optional document exports | Optional recycling of one uploaded file |
| [Gokuai](../drivers/gokuai/README.md) | Enterprise library or selected path | — |
| [Guangya Pan](../drivers/guangyapan/README.md) | Authorized root or folder, signed originals and actual ranges | — |
| [Huawei Drive Kit](../drivers/huaweidrive/README.md) | Personal drive. `Stat` and `Open` accept a known version ID with history access | — |
| [KDocs](../drivers/kdocs/README.md) | Personal My Documents or a selected folder | — |
| [KooDrive](../drivers/koodrive/README.md) | Individual, department or group space | Optional Mkdir and known-length Create in the individual space |
| [115 Open](../drivers/pan115/README.md) | Authorized root or folder, ordinary unencrypted files and actual ranges | — |
| [123 Open](../drivers/pan123/README.md) | Account tree, ordinary downloads and optional direct links for authorized directories | Optional Mkdir, single-file recycling and Create through single-step or multipart upload |
| [Alibaba Cloud PDS](../drivers/pdsshare/README.md) | Authorized standard drive or received share | — |
| [Teambition](../drivers/teambition/README.md) | Approved enterprise project collection and its original files | — |
| [Tencent Docs](../drivers/tencentdocs/README.md) | Authorized folder and optional asynchronous document export | — |
| [Tencent Cloud SMH](../drivers/tencentsmh/README.md) | Tenant space or recipient share, with signed downloads | — |
| [WeCom Wedrive](../drivers/wecomdrive/README.md) | Approved space folder and uploaded originals. Online documents appear in listings | — |
| [Tencent Weiyun](../drivers/weiyun/README.md) | Official MCP personal root and ordinary downloads | — |
| [WPS 365](../drivers/wps365/README.md) | Authorized enterprise drive or folder and ordinary downloads | Optional Mkdir and single-file removal |
| [WUYING Drive](../drivers/wuyingdrive/README.md) | Assigned personal or team space and signed downloads | — |
| [360 AI Cloud Disk](../drivers/yunpan360/README.md) | Official MCP personal root and ordinary downloads | — |

### Other file, content and repository sources

| Driver | Read scope and extra capabilities | Changes |
| --- | --- | --- |
| [AtomGit](../drivers/atomgit/README.md) | Fixed commit and optional subtree. Raw blobs include Git LFS pointers | — |
| [Autoindex](../drivers/autoindex/README.md) | HTTPS Nginx JSON directory listing and files | — |
| [Box](../drivers/box/README.md) | Authorized folder and ordinary files. Web links appear in listings | — |
| [Cloudreve](../drivers/cloudreve/README.md) | Authorized Cloudreve V4 directory and ordinary files | Optional Create and durable continuation for the supported OneDrive upload workflow |
| [CNB Releases](../drivers/cnbreleases/README.md) | Uploaded assets of a selected release | — |
| [CodeArts Repo](../drivers/codeartsrepo/README.md) | Fixed commit and optional subtree, with raw Git blobs | — |
| [Dropbox](../drivers/dropbox/README.md) | Personal or team root, ordinary files and optional explicit exports | Optional Create through one request or an upload session |
| [Emby](../drivers/emby/README.md) | Selected media tree and downloadable originals | — |
| [GitHub Releases](../drivers/githubreleases/README.md) | Uploaded assets of a selected release | — |
| [GitHub Repo](../drivers/githubrepo/README.md) | Fixed Git tree, raw blobs and actual ranges | — |
| [Google Drive](../drivers/googledrive/README.md) | Authorized folder or shared drive, ordinary files and configured exports | — |
| [Google Photos](../drivers/googlephotos/README.md) | App-created album and its available download representation | — |
| [Graph Drive](../drivers/graphdrive/README.md) | Authorized drive, folder or share | Optional Create, exclusive Publish and durable upload continuation on ordinary mounts |
| [CloudFlare ImgBed](../drivers/imgbed/README.md) | Indexed deployment folder and ordinary files | — |
| [IPFS](../drivers/ipfs/README.md) | Fixed UnixFS directory CID through a trusted Kubo endpoint | — |
| [MediaFire](../drivers/mediafire/README.md) | Authorized folder and files with eligible native download links | — |
| [Misskey](../drivers/misskey/README.md) | Selected account folder and downloadable entries | — |
| [Seafile](../drivers/seafile/README.md) | Authorized library or directory | — |
| [Teldrive](../drivers/teldrive/README.md) | Active directory and ordinary files from the supported deployment | — |
| [URL tree](../drivers/urltree/README.md) | Explicit HTTPS directory and file URLs | — |
| [Yandex Disk](../drivers/yandex/README.md) | Authorized path and ordinary files | Mkdir, Subtree |
