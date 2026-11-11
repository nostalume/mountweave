# Teambition project files

This optional source mounts a folder in the Teambition V3 project file library.
The host provides the enterprise, operator, project, and collection IDs, and an
HTTP client that sends `Authorization: Bearer <app_access_token>` to
`https://open.teambition.com`. Use an enterprise-authorized app with access to
the selected project files. Signed download links require a paid enterprise.

```go
d, err := teambition.Open(ctx, teambition.Options{
    OrganizationID: organizationID, OperatorID: operatorID,
    ProjectID: projectID, RootID: collectionID,
    Client: authenticatedClient,
})
```

Select a collection from the chosen project. Construction confirms the
collection ID. Directory requests always send the configured project and
the selected parent together. A file is opened only after its ID, name, and
parent agree with the download receipt.

The driver supports Stat, List, streamed Open, and use as a Copy source.
It follows the service's directory tokens and rejects ambiguous path names.
It accepts download grants only when the service returns HTTPS and reports an
actual range when the server provides one. The API reference does not specify
the scheme of issued links. The optional `ContentClient` must have a
credential-free transport.
It can enforce host egress policy for the issued URL and its HTTPS redirects.
The driver borrows both clients. Close a non-nil result from a failed
construction to retry any retained response cleanup.

This scope covers project files. Personal storage, anonymous share links,
file writes, versions, and resumable transfers are outside this module.

- [Teambition directory listing](https://open.teambition.com/docs/apis/6411504e912d20d3b56ec5ff)
- [Teambition collection detail](https://open.teambition.com/docs/apis/64db2903912d20d3b5bb021f)
- [Teambition project file detail and downloads](https://open.teambition.com/docs/apis/64db2921912d20d3b5bb5250)
