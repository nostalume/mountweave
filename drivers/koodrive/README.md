# Huawei Cloud KooDrive source

`koodrive.Open(ctx, Options{UserID: user, ContainerID: space, Client: signed})`
opens the ordinary file tree of one enterprise individual space in the China
region. Install the returned driver with Core's `Install`. The control client
must authenticate requests for the configured user and supply `Authorization`
and `X-Date` according to the [KooDrive application authentication
contract](https://support.huaweicloud.com/api-koodrive/koodrive_05_0010.html).
The driver sets `X-User-Id` and confirms the space, owner, and ordinary root.
It does not log in, store credentials, or expose the vault or smart folders.

Set `Options.TeamSpace: true` with a known department or group `ContainerID`
for an ordinary read-only mount. The signed `UserID` remains the acting user.
The returned container metadata determines the actual department or group ID
and list route. Directory pages confirm parent IDs against the selected
ordinary root, and signed downloads use the same container and file IDs.
`AllowMkdir` and `AllowCreate` are incompatible with `TeamSpace`.
[Space details](https://support.huaweicloud.com/api-koodrive/GetSpaceByContainerID.html) ·
[Team listing](https://support.huaweicloud.com/api-koodrive/getDepartmentFiles.html) ·
[Download grants](https://support.huaweicloud.com/api-koodrive/getDownLoadUrl.html)

`Stat` and `List` use exact child names and the native page cursor. `Open` and
the source side of `Copy` stream the body from the service's signed HTTPS
download address. Control requests do not follow redirects. The optional
`ContentClient` is separate and must never add account credentials. The
driver sends the requested byte range and reports the upstream's actual
response. A known listing size must match the reported HTTP total. If HTTP
omits its length, the stream checks the listing size through EOF.

`Options.AllowMkdir` enables Core `Mkdir` in the same individual space.
[KooDrive's create-directory API](https://support.huaweicloud.com/api-koodrive/createDirectory.html)
receives the resolved parent ID, using `root` for the space root, and rejects
same-name creation with `renameMode=3`. An explicit conflict returns `AlreadyExists`. An
unconfirmed request has unknown effect. Use a control transport that does not
retry mutation requests. The default mount remains read-only.

Set `Options.AllowCreate` to enable default `Core.Write` for files with a
known size up to 5 GiB. The driver requests one signed native part through
[createFile](https://support.huaweicloud.com/api-koodrive/createFile.html) with
native name-conflict rejection. It streams that part using the separate
`ContentClient`, validates the input length and EOF, then confirms the file ID
through [completeFile](https://support.huaweicloud.com/api-koodrive/completeFile.html).
An empty input uses the documented zero-length final part. Use transports
without mutation retries. The server may have created metadata before
publication, so failures after initialization may leave a possible effect.
The driver reports it as unknown and does not delete the file or replay the
operation. Content replacement and multipart continuation are outside this
capability.

`MaxResponseBytes` bounds each control response and defaults to 4 MiB.
Native directory pages are read as needed. Content is streamed without
buffering the file. Both HTTP clients remain host-owned.
