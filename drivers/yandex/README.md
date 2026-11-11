# Yandex Disk source

This optional Go 1.24 module uses the native Yandex Disk REST API and the standard
library. It reuses Core paths, cursors, bodies and file composition.

```go
storage, err := yandex.Open(ctx, yandex.Options{
    RootPath:         "disk:/Archive",
    Token:            oauthToken,
    Client:           apiClient,
    ContentClient:    contentClient,
    MaxResponseBytes: metadataBudget,
})
```

The endpoint defaults to https://cloud-api.yandex.net/v1/disk. Endpoint can select
an actual deployment or a closed fixture. RootPath defaults to disk:/ and also
accepts the native app:/ scope. Open checks options and confirms the directory
root. The root remains a path intention, not a bound directory identity. An
error with a non-nil Driver retains failed response cleanup and requires Close.
Successful Core.Install transfers Driver ownership.

Token is added only to control requests. Client and ContentClient are borrowed;
both disable cookie jars, and control redirects are refused. The content client
uses the standard limit of 10 requests per redirect chain, only for GET and valid
HTTP(S) addresses without userinfo, opaque data or fragments. Each redirect
rebuilds headers from the original Range and Accept-Encoding: identity; account
authorization, cookies and Referer are not copied. HTTPS cannot downgrade to HTTP.
The default content client is independent of Client, so an authenticated API
transport is never silently reused for a native download URL. A supplied content
transport must not inject account auth; the Driver cannot strip authentication
added inside an arbitrary RoundTripper. HTTP content capabilities are accepted
only with an explicitly HTTP control endpoint. The host owns TLS, proxy,
credential refresh and any custom transport behavior. The Driver closes rejected
downloads and redacts capability details from failed exchanges and cleanup.

| Core operation | Native behavior |
| --- | --- |
| Stat | One resources query with path, type, size and modification observations |
| List | Native offset/limit/total page, followed by Core public pagination/replay |
| Open | File download capability, then direct content GET; no preliminary Stat/HEAD |
| Copy | Existing Core composition, verified as a source to Local |
| Mkdir | Native PUT; 201 confirms completion, 202 waits for its operation |
| RemoveTree with Subtree | Native recursive DELETE and confirmed operation completion |

Native /foo and disk:/foo spellings are normalized once, without path cleaning,
case changes or percent decoding of stored names. app:/ requests remain app:/
even when the service reports a canonical absolute disk:/ path: Open captures
that root mapping and later metadata must remain at the corresponding exact
parent and child names. It does not expand an app token's permission scope or
add a root-ID resolver. disk:/ responses must match the requested path directly.

Listings request a stable sort order, but are not snapshots. Offset advances by
the actual item count and total controls termination. A short nonterminal page
continues; an empty terminal page is valid. An empty nonterminal page has no
advancing native offset and fails instead of skipping a fabricated range or
looping. Invalid parent/path/bounds, truncated JSON and physical connection EOF
cannot become successful enumeration EOF. Memory is one native page plus Core's
public replay state. MaxResponseBytes defaults to 4 MiB per metadata/error
response, independently of file size.

Open validates the literal GET capability and uses the separate content client.
No public Link/Download API, URL cache, implicit sharing or full-file staging is
introduced. Actual 200/206/416 lengths and ranges use the shared HTTP content
interpreter. Core owns returned Body on success and failure, including EOF,
cancellation, closing and mount retirement. Optional valid ETag observations
remain separate from resource IDs, hashes or immutable Version selection.

A native 202 is acceptance, not Applied. The Driver observes only the returned
same-control-origin operations URL, using its literal valid operation ID. It
polls while the status is in-progress; success confirms Applied, failed remains
Unknown because partial effects are not excluded, and unknown states are errors.
Cancellation stops local polling and joins active I/O; remote work may continue.
No mutation is replayed or rolled back by name. DELETE retains the service's
normal recycle-bin behavior and never requests permanent purging. The selected
root is protected. Native 201/204 receipts and valid success states remain
Applied even if cleanup or later cancellation fails; a partially decoded JSON
status is not confirmation.

Each intermediate redirect response is closed before another request; a failed
close stops the chain and remains Driver-owned. Iterator Close cancels and joins
its current request. Failed metadata/error
response closes remain Driver-owned and are retried by Driver.Close; borrowed
clients and transports stay open. Custom host transports must also avoid
replaying uncertain mutations or changing synchronous/recursive intent.

Current capabilities are Driver, DirectoryMaker and SubtreeRemover. Ordinary
Remove, exact-target exclusive writing, complete Publisher, Move, binding,
NoFollow, sharing and native continuation are not declared. An upload overwrite
flag alone does not establish final exclusion or complete-publication guarantees.
Tests use synthetic credentials and confined HTTP origins, including a content
302 to a separate receiver with range preservation and credential isolation.
They do not certify live accounts, every redirected deployment or all native
app-folder layouts.

Native references: [access rules](https://yandex.com/dev/disk-api/doc/en/concepts/quickstart),
[official request definitions](https://github.com/yandex-disk/yandex-disk-restapi-java/blob/master/disk-restapi-sdk/src/main/java/com/yandex/disk/rest/retrofit/CloudApi.java),
[paging](https://github.com/yandex-disk/yandex-disk-restapi-java/blob/master/disk-restapi-sdk/src/main/java/com/yandex/disk/rest/json/ResourceList.java),
[operation states](https://github.com/yandex-disk/yandex-disk-restapi-java/blob/master/disk-restapi-sdk/src/main/java/com/yandex/disk/rest/json/Operation.java).
