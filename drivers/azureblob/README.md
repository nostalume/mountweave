# Azure Blob source

This optional module requires Go 1.25 or newer and Azure Blob SDK v1.8.2.
It uses exact keys in a prepared container and the existing Core APIs; it does
not create containers or synthesize filesystem directories.

```go
storage, err := azureblob.Open(ctx, azureblob.Options{
    Client:           containerClient,
    Prefix:           "archive/",
    PartSize:         partBytes,
    MaxBufferedBytes: sharedBufferBytes,
    PartConcurrency:  1,
})
// Handle err before installing. Successful Install transfers Driver ownership.
mount, err := files.Install("/objects", storage)

address := core.Key("/objects", "a//b%+name")
query := core.Keys("/objects", "", "/")
```

The host prepares `*container.Client` with the native endpoint, selected
container, credentials and transport. Open validates options without making a
permission probe or requiring a prefix object to exist. The client, credential
provider and transport remain borrowed. Prefix is empty or slash-terminated;
keys are appended literally, without path cleaning or a second URL decode.
Native service naming restrictions still apply.

| Core operation | Current behavior |
| --- | --- |
| Stat | Native properties and optional immutable Version selection |
| List | Flat or delimiter listing, opaque native continuation, explicit PrefixGroup items |
| Open | Direct Body, native start/end ranges, optional version and conditional selection |
| Write | Exclusive new block blob, with complete-input publication |
| Write with Publish | Complete new/replacement block blob, optionally native conditional |
| Remove | Current exact key, optionally native conditional; no snapshots or version expansion |
| Copy | Existing Core composition, verified in both directions with Local |

Names `a` and `a/b` remain independent. Grouped prefixes are not directories.
`Query.After` filters native pages locally; it may read preceding pages and does
not reinterpret a key as an opaque continuation marker. Ordinary enumeration is
not a snapshot. Azure XML name encoding is decoded once by the native SDK.

The ordinary Body remains the fast path. Suffix range requests use the existing
explicit full-body fallback (nil actual Range), without an extra HEAD. The SDK's
retry reader is not enabled. Actual 200/206 lengths and ranges use the shared
HTTP content interpreter; Core owns returned bodies on success and failure.

PartSize and MaxBufferedBytes enable writing together; leaving both zero allows
reading without allocating an upload budget. PartConcurrency defaults to one.
All writes on an instance share the byte and request budgets. PartSize must fit
the native block limit, and MaxBufferedBytes must cover at least one part.
Known-length inputs fitting one part are verified through normal EOF in a
bounded buffer and sent with one Put Blob. Larger or unknown-length inputs use
the shared bounded upload loop, StageBlock, then CommitBlockList after complete
input validation. Empty unknown-length input is a single empty staged block.
No complete-file disk staging or second SDK upload worker pool is introduced.

Reads accept native positive/negative tag conditions. Mutations accept one
presence test or one strong tag in one conditional header; unsupported compound,
multi-tag, weak-tag or foreign-domain mutations fail before input consumption.
Conditions are attached to the final native request, never simulated by a HEAD.
ObserveTarget alone may request one earlier presence observation; it is not CAS.

Each staged attempt has random, fixed-width block IDs; an ID never changes its
bytes. The final Latest list names only this attempt's blocks in input order.
Uncommitted blocks share the object name, not a private UploadId: another writer
can discard them and cause this attempt to fail. Fixed native block-ID width
conflicts also remain native failures. There is no per-attempt Azure Abort.
Cancel joins active SDK calls and returns buffers, without Delete Blob, an empty
commit or another name-based cleanup. Uncommitted remnants follow service GC;
no per-attempt expiration deadline or successful active cleanup is claimed.

Final PUT, block-list commit and DELETE disable SDK retries. A lost confirmation
returns Unknown. An acknowledged native success remains Applied even if response
header parsing or later cancellation also fails. Native refusal preserves its
cause separately from the file effect. Host-injected policies and transports
must also avoid replaying uncertain mutations. Close does not close the borrowed
client or pretend that upstream uncommitted blocks were deleted.

This profile supports flat blob access and Block Blob writes. It rejects observed
HNS directories, tree namespace, Mkdir, typed removal, Move, binding/NoFollow and
durable continuation; it does not silently convert Append/Page blobs. Ordinary
SDK error-response consumption remains SDK-owned. Closed fixtures verify the
native SDK and Core integration, not live Azure accounts or HNS/ADLS workflows.

Native contracts: [naming](https://learn.microsoft.com/en-us/rest/api/storageservices/naming-and-referencing-containers--blobs--and-metadata),
[list](https://learn.microsoft.com/en-us/rest/api/storageservices/list-blobs),
[conditions](https://learn.microsoft.com/en-us/rest/api/storageservices/specifying-conditional-headers-for-blob-service-operations),
[blocks and commit](https://learn.microsoft.com/en-us/rest/api/storageservices/put-block-list).
