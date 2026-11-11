# DingTalk Drive source

Call `dingtalkdrive.Open(ctx, dingtalkdrive.Options{SpaceID: spaceID,
RootID: folderID, OperatorUnionID: unionID, Client: client})` for a single
authorized DingTalk folder. Use the storage `spaceId` and `dentryId` for that
folder. The host's HTTP client supplies `x-acs-dingtalk-access-token` for an
approved internal organization app with `Storage.File.Read` and
`Storage.DownloadInfo.Read`. The operator must also have access to the folder
and its files. Close a non-nil failed result to retry retained cleanup.

Install the Driver in Core for `Stat`, `List`, `Open`, and `Copy` from DingTalk.
The Driver checks the folder's ID and space during construction. It pages
through each selected parent by `nextToken` and compares child names exactly.
Original reads use one `HEADER_SIGNATURE` grant per open. Each grant's headers
go only to the initial public HTTPS receiver. The optional `ContentClient`
must use a transport without account credentials. Actual HTTP framing determines
length and range, including unknown lengths read through EOF. JSON responses
have a 4 MiB default limit (`MaxResponseBytes`).

This source handles ordinary stored files and folders. Shortcuts are never
followed. Online document export, storage-space discovery, and writes are
outside the mounted folder workflow.

[DingTalk folder authorization](https://www.alibabacloud.com/help/zh/polardb/polardb-for-postgresql/sync-dingtalk-documents-to-the-knowledge-base) ·
[Official Storage API SDK](https://github.com/alibabacloud-go/dingtalk/blob/master/storage_1_0/client.go)
