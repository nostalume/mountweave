# HUAWEI Drive Kit source

Use `huaweidrive.Open(ctx, huaweidrive.Options{Client: client})` with a host-owned
HTTP client that applies the selected user's OAuth access token. Prepare an
approved [Drive Kit web app](https://developer.huawei.com/consumer/en/doc/hmscore-guides/server-preparations-0000001050185577/)
and request `https://www.huawei.com/auth/drive.readonly`. The host renews that
user's credentials. Close a non-nil Driver returned with an error to retry any
retained response cleanup.

Install the Driver at a Core path for `Stat`, `List`, `Open`, and `Copy` from
the personal Drive root. Construction verifies access with one listing request.
The Driver follows Huawei's native cursors, including empty intermediate pages.
It uses parent and name filters where possible, then checks exact names and
checks the returned parent ID where it is known. Names containing quotes use a
parent listing because the query reference leaves literal escaping unspecified.

Content reads use `form=content` on the fixed Drive API origin. Redirects use an
optional separate `ContentClient` with a credential-free transport, or the Go
default transport, with no cookies or automatic redirects. Actual HTTP framing
determines length and range. `MaxResponseBytes` bounds JSON replies and defaults
to 4 MiB. Ordinary file content requires ownership.

For a file in the mounted tree, set `ReadSelection.Version` in Core Stat or
`OpenOptions.Selection` to a known native history version ID. The selected
version supplies its own size and modification time. Versioned Open uses the
same file and history IDs for content and observes Huawei's history permissions.
The user's Drive security policy must support the history API. Obtain history
IDs from Huawei's [HistoryVersions API](https://developer.huawei.com/consumer/en/doc/hmscore-guides/server-quaring-history-version-0000001064501116/).

Application data, shared-link entry points, and mutations are outside this
integration.

[Drive Kit queries](https://developer.huawei.com/consumer/en/doc/HMSCore-References/server-public-info-0000001050159641/) ·
[Files:list](https://developer.huawei.com/consumer/en/doc/HMSCore-References/server-api-fileslist-0000001050153649) ·
[Files:get.content](https://developer.huawei.com/consumer/en/doc/hmscore-references/server-api-filesgetcontent-0000001050153651/)
