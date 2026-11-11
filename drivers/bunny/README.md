# Bunny Storage source

This optional module uses Bunny Storage's native HTTP directory API with the
existing Core. It requires only Go 1.24, the standard library and Mountweave.

```go
storage, err := bunny.Open(ctx, bunny.Options{
    RootURL:          "https://storage.bunnycdn.com/my-zone/archive/",
    AccessKey:        zonePassword,
    Client:           httpClient,
    MaxResponseBytes: metadataBudget,
})
```

RootURL includes the regional endpoint, storage zone and optional subdirectory.
AccessKey is the zone password, not the account API key. The client and transport
remain borrowed; leave AccessKey empty only when that transport supplies auth.
Open validates options and probes the root directory. A non-nil Driver returned
with an error retains failed response cleanup and must be closed.

Install using Core.Install, then use Path, Children, Stat, List, Open and Copy.
This is a tree view, not Azure/S3-style arbitrary keys. Native names and paths are
preserved; no path cleaning, name trimming or second URL decoding is performed.
The chosen root is a path intent, not a pinned directory identity. Native listing
items must belong to the requested directory; unexpected paths fail explicitly.

Bunny returns one JSON array per directory. The Iterator streams that response
across public pages; public continuation and replay remain Core-owned. No native
page token, directory snapshot or permanent directory cache is invented. Full
traversal consumes O(directory entries) network data. Stat scans the exact parent
listing and also costs O(parent entries); the REST API does not document HEAD.
LastChanged is projected only with an explicit timezone, rather than guessing UTC.
GUID and checksums are not invented entity tags or immutable versions.

MaxResponseBytes defaults to 4 MiB per metadata response and is configurable;
it is not a file-size limit. Limit failure is an error, never enumeration EOF.
Five instance-local listing slots follow the documented native operation limit.
A streaming iterator keeps its slot until the response is closed. Multiple
instances using the same zone still share the service's actual quota; there is
no global registry. Native listing-limit HTTP400 and HTTP429 preserve throttling.

Open first obtains the ordinary file type, then directly GETs the body. This is
not a bound-object guarantee. It reuses the shared HTTP interpreter for actual
200/206/416 ranges and lengths; a 200 response to a range request is a complete
body fallback. No extra HEAD, full-content cache or implicit retry reader is used.
Conditional reads compare evidence from the actual GET, not the earlier listing.
Missing evidence cannot satisfy a requested tag condition. Core owns returned
bodies, including when Open returns an error.

Next cancellation stops and joins that response's activity. Successful calls do
not bind future Next calls to the earlier context. Close, cursor expiration and
retirement close the native response; failed cleanup keeps an owner and can be
retried. Borrowed HTTP clients and transports are never closed by the Driver.
Authorization requests do not follow redirects.

Explicit RemoveTree with Scope: core.Subtree uses native recursive DELETE.
Only native HTTP200 confirms Applied; a lost response remains Unknown. Native
root deletion is refused and allowRootDelete is never sent. An ordinary Remove
is not implemented because the same native request recursively removes directories.
The Driver does not automatically retry mutations; custom host transports must
also avoid replaying an uncertain DELETE. Close failures remain independent of
an already confirmed file effect.

The current capabilities are Driver and SubtreeRemover. FileCreator, Publisher,
Mkdir, Relocator, bound access, NoFollow and native continuation are not declared.
Ordinary PUT auto-creates missing parents, and its published contract does not
establish exclusive creation or full publication. Neither a HEAD precheck nor
client-side buffering is used to manufacture those guarantees. Bunny's separate
S3 endpoint should reuse the existing S3 module after checking its native feature
contract; this module does not create a second S3 engine.

Tests use synthetic credentials and confined HTTP services. They verify exact
routing, stream/replay lifetime, actual ranges, Copy to Local, cancellation,
metadata limits, cleanup ownership and explicit recursive effects. They do not
claim live-account, CDN or all-service compatibility.

Native references: [HTTP](https://bunny.net/docs/storage/http),
[list](https://bunny.net/docs/api-reference/storage/browse-files/list-files),
[delete](https://bunny.net/docs/api-reference/storage/manage-files/delete-file),
[limits](https://bunny.net/docs/storage/limits).
