# Graph Drive

An optional, standard-library-only Driver for the Microsoft Graph v1.0 Drive
contract. It provides a fixed tree root, metadata, native paging, streaming
downloads and known-length exclusive uploads through the existing Core API.

```go
d, err := graphdrive.Open(ctx, graphdrive.Options{
    APIClient: apiClient, ContentClient: contentClient,
    DriveID: driveID, RootPath: "/Documents",
    ChunkSize: chunkSize, MaxBufferedBytes: uploadBudget,
})
```

`Open` validates settings, obtains the selected directory and binds its actual ID.
`RootID` and drive-relative `RootPath` are mutually exclusive; `/` selects the
drive root. `Endpoint` defaults to `https://graph.microsoft.com`. `Validate`
performs the same static checks without a request. Configurations can use the
existing `control.TypedFactory[graphdrive.Options]`; runtime clients are not
serialized. Install the returned Driver with `Core.Install`. Installation
transfers ownership only on success. Close a non-nil failed construction result.

The host lends an authenticated `APIClient` and a `ContentClient` whose transport
does not inject account authentication. The Driver removes cookie jars and
automatic redirects from its client copies. It does not own the original clients,
their transports or credential providers. Their authentication middleware must
not blindly replay mutations. Graph continuation links remain on the configured
API authority and children resource; preauthenticated content URLs use only the
content client. Download request failures and rejected-response cleanup keep
signed URLs out of diagnostics. The host owns diagnostics produced by custom
response bodies after a successful handoff.

## Application credentials for organization drives

For OneDrive for Business or an organizational SharePoint document library,
supply an `APIClient` that obtains an application token for Microsoft Graph.
Microsoft documents the admin-consented application role `Files.Read.All`
for metadata, children and downloads. Set the known `DriveID` and either
`RootID` or `RootPath`. The driver requests DriveItems directly, so mounting
does not require a signed-in user or drive discovery. Omit `ChunkSize` and
`MaxBufferedBytes` to leave ordinary publication disabled. Personal OneDrive
uses delegated credentials.

[Microsoft Graph application authentication](https://learn.microsoft.com/en-us/graph/auth-v2-service)
· [OneDrive permission scopes](https://learn.microsoft.com/en-us/onedrive/developer/rest-api/concepts/permissions_reference?view=odsp-graph-online)

For Microsoft 365 operated by 21Vianet, set `Endpoint` to
`https://microsoftgraph.chinacloudapi.cn` and use an `APIClient` with a token
issued for China Graph by the tenant's China Microsoft Entra authority
(`https://login.chinacloudapi.cn`). Use a Drive ID from that tenant. The v1.0
metadata, children and original-file download APIs support this deployment.
The separate `ContentClient` follows preauthenticated HTTPS file URLs without
sending the Graph token. This configuration has been checked with the existing
sealed custom-endpoint fixture, without a live 21Vianet tenant.

[National cloud endpoints and authentication](https://learn.microsoft.com/en-us/graph/deployments)
· [List children](https://learn.microsoft.com/en-us/graph/api/driveitem-list-children?view=graph-rest-1.0)
· [Download content](https://learn.microsoft.com/en-us/graph/api/driveitem-get-content?view=graph-rest-1.0)

## Previously authorized shared folders

Set `ShareURL` in place of `DriveID`, `RootID` and `RootPath` to select a
folder from an HTTPS Graph share link. The host supplies the link from protected
configuration at construction. Ordinary Options JSON excludes this value.
The authenticated client must already
be able to read that folder. The Driver sends the encoded link only to the
configured Graph API origin, omits both redemption preferences, and verifies
the returned drive and item through an ordinary folder request. It uses only
that native folder identity after construction. The host's API transport must
not inject a redemption preference.

Microsoft lists delegated `Files.ReadWrite` and application
`Files.ReadWrite.All` as the minimum permissions for its Shares endpoint.
OneDrive for Business and SharePoint require authenticated user context for
shared content. This mode uses existing access and never requests a new grant.
Single-file links, anonymous links that require redemption, writes and upload
recovery are outside its contract. The share URL is used only during
construction. JSON control responses have a 4 MiB default limit in this mode.

[Microsoft Graph Shares API](https://learn.microsoft.com/en-us/graph/api/shares-get?view=graph-rest-1.0)

## Files and publication

Every path segment is checked against the stored name before its ID is used.
Native case-insensitive lookup therefore does not authorize a different spelling.
Remote-item references and packages are not followed or silently omitted. An
unrepresentable item ends that enumeration with an explicit error. Ordinary
downloads support one requested range or the native complete-body fallback;
bound editing, version selection and unproven content conditions are refused.

Configure both `ChunkSize` and `MaxBufferedBytes` to enable writes. Nonfinal chunks
must be multiples of 320 KiB and smaller than 60 MiB. Each upload holds one chunk;
the byte budget is shared by active uploads. Both values zero leaves reads usable.
`RangeAttempts` defaults to one; uncertain requests query session progress before
any permitted resend. A current chunk remains borrowed until the request ends.

`Core.Write` creates a new file by default. `Publish(IfAbsent())` uses the same
exclusive native policy. Existing names are not overwritten or automatically
renamed. Other publication policies are refused before input is read. Small and
empty inputs are validated before a single content PUT. Session uploads validate
normal EOF before the final range can publish. A final acknowledgement is required
for `Applied`; a lost final reply followed by a missing session remains `Unknown`.

Session writes share the preparation, delivery and retirement lifetime used by
Cloudreve. Graph interprets the received file response directly, without an extra
commit request. A verified file result survives response-close or later
cancellation errors; response bodies have one cleanup owner.

## Durable writes

The same `Core.Write` accepts `WriteOptions.Continuation` with an existing
`Replayable` source, known length and protected append-only log. `transfer.Service`
can own this log and retain the Input source. Direct exclusive creation and
`Publish(IfAbsent())` keep their original policies; no additional configuration
selector or resume API is required.

The log binds the API authority, Drive, actual root and target parent IDs, exact
root-relative path, source version, length and policy. Active recovery checks the
current parent identity once. Saved terminal receipts are replayed before any
live parent lookup, so later renaming or deletion cannot erase a known result.
Chunk size and resource budgets may change without changing this binding.

Small and empty durable writes keep the single content PUT. Larger writes use
the same OneDrive session sender as ordinary writes. Each new durable payload is
read through an exact verified source range before it is sent; retries reuse its
buffer. Recovery first queries the session and obeys the reported range bounds.
Progress is not a complete received-byte bitmap and is not saved per fragment.
The log records only identity, initialization, session, possible publication and
file or cleanup outcomes. The upload URL is a secret capability; do not expose
the checkpoint in public status or accept it from an untrusted client.

Cancellation pauses an attempt and releases its runtime buffers and readers.
Driver retirement and ordinary `Cleanup` do not cancel these durable sessions.
The checkpoint consumer serializes execution and explicit `Abort`, and owns
recovery after restart. Native requests and source readers finish before return;
the original source and host clients remain borrowed.

An acknowledged file result remains `Applied` even if saving it fails. Keep the
source until the consumer has durably recorded the terminal result. A definite
rejection is saved with its cause and is replayed without another upload; use
`Abort` to retire its private session. A possible publication without a file
receipt remains `Unknown`: GET progress, matching names or sizes, and DELETE
204/404 do not prove its historical file effect. Explicit Abort may retire the
session while leaving that file effect unresolved. It never deletes by path.

An initialization whose reply is lost has sent no body and cannot publish the
file. A later invocation may create at most one replacement session, recording
another initialization before the request. Earlier unknown private resources
remain represented in the log and expire natively; neither completion nor Abort
claims they were explicitly cleaned. Unknown append results stop execution and
require reloading the consumer's committed log before any retry.

## Cleanup

Failed ordinary uploads clean only their private session. `Cleanup(ctx)` retries
retained session cleanup after active writes end, never deleting a file by path.
Unconfirmed cleanup prevents creation of further sessions. Lost initialization
without a session identity requires native reconciliation and remains reported.
Concurrent unknown initiations retain separate local attempts and target paths;
they are not combined under a missing session URL. Confirmed session cleanup does
not turn an unknown file publication into an unchanged target.
`CleanupTimeout` optionally bounds automatic cleanup; zero adds no deadline beyond
the borrowed client. `Close` retries owned response closures and reports retained
resources; it does not close host clients or discard unresolved ownership.

The module currently uses the adjacent Core checkout through its `go.mod`
replacement. Its introduction does not publish a package version.
