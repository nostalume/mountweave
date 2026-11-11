# WPS 365

This driver mounts an authorized WPS 365 drive or folder. It reads by default.
Use the native drive ID and, optionally, a folder ID within that drive.
An empty `RootID` selects the drive root.

```go
drive, err := wps365.Open(ctx, wps365.Options{
    DriveID: "drive-id",
    Client:  wpsControlClient,
})
if err != nil {
    if drive != nil {
        err = errors.Join(err, drive.Close())
    }
    return err
}
if _, err = files.Install("/wps", drive); err != nil {
    return errors.Join(err, drive.Close())
}
```

The borrowed control client's transport supplies WPS authorization and signs the
actual request with KSO-1 if the application requires signing. Use a grant with
file-read access to the selected drive. Requests go to `https://openapi.wps.cn`.
The separate, optional `ContentClient` must carry no account credentials. It
fetches the HTTPS source address returned by WPS. Both clients disable cookies
from a cookie jar and automatic redirects. The driver redacts signed addresses
from rejected responses and retries failed response closes at retirement.

The driver supports `Stat`, directory listing, streaming reads with HTTP range
validation, and use as a `Copy` source. Each name is selected from its parent's
native pages. The mounted view includes ordinary files and folders. A lookup
of a WPS shortcut returns an unsupported-operation error and does not resolve
its destination. The driver opens the source format, has no document conversion,
and uses live observations rather than snapshot or version evidence.

Set `AllowMkdir: true` to enable native directory creation with a control grant
that includes file write access. WPS applies its fail-on-conflict policy at
creation. Mountweave confirms the drive, parent, name and directory ID in the
reply. If the outcome is uncertain it returns `Unknown`. [Create folder](https://open.wps.cn/documents/app-integration-dev/wps365/server/yundoc/file/create-file)

Set `AllowRemove: true` to enable ordinary `Core.Remove` for regular files.
The driver sends the selected file ID to the native delete endpoint and
confirms removal from its success envelope. Explicit type or condition
constraints, directory removal, and recycle-bin management are outside this
operation.
An unconfirmed response returns `Unknown`. [Delete file](https://open.wps.cn/documents/app-integration-dev/wps365/server/yundoc/file/delete-file)

The closed test fixture covers V7 request shapes, directory scope and cursor
handling, name ambiguity, a range read, credential isolation, directory creation,
file removal, and cancellation on mount retirement. Actual enterprise app
authorization and content endpoints require verification with a live grant.

[File metadata](https://open.wps.cn/documents/app-integration-dev/wps365/server/yundoc/file/get-file-info-by-id)
· [Children](https://open.wps.cn/documents/app-integration-dev/wps365/server/yundoc/file/get-file-list)
· [Download](https://open.wps.cn/documents/app-integration-dev/wps365/server/yundoc/file/get-file-download)
· [KSO-1](https://open.wps.cn/documents/app-integration-dev/wps365/server/api-description/signature-description)
