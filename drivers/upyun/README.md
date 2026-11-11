# USS / UpYun source

This optional module uses the native USS directory REST API with Go 1.24 and the
standard library. It adds no SDK or Core API dependency beyond Mountweave itself.

```go
storage, err := upyun.Open(ctx, upyun.Options{
    RootURL:          "https://v0.api.upyun.com/bucket/archive/",
    Operator:         operator,
    Password:         operatorPassword,
    Client:           httpClient,
    MaxResponseBytes: metadataBudget,
})
```

Open validates the URL and paired credentials, then HEADs the selected directory.
The URL includes the bucket and actual subdirectory; it is a path intention,
not a pinned directory identity. A non-nil Driver returned with an error retains
failed response cleanup and must be closed. Successful Core.Install transfers
Driver ownership. The HTTP client and transport remain borrowed.

Operator/Password select native HMAC-SHA1 request signing using the lower-case
hex MD5 of the operator password as the signing key. URI escaping occurs once;
the exact transmitted RequestURI participates in the signature. Both credentials
may be empty when the client supplies authorization. Redirects are refused, and
mutations are not retried. Host-injected transport policies must not alter the
operation, add asynchronous deletion, or replay uncertain mutations.

Use Core Path/Children, Stat/List/Open, Mkdir, Remove and Copy. Listings retain one
native page, not a directory cache. Native iter tokens are private; empty or short
pages may continue. The native terminal marker, not page length, ends enumeration.
Malformed JSON, repeated tokens, conflicting header/body tokens and metadata
budget overflow fail explicitly. MaxResponseBytes defaults to 4 MiB per metadata
or error response; it is independent of public page size and file size.

Plain Open requests a file explicitly and returns its direct authenticated Body.
Actual 200/206 lengths and ranges use the existing HTTP content interpreter; a
full-body response remains an explicit range fallback. It makes no preliminary
HEAD. Close every returned Body. Core owns stream cancellation, EOF, close and
retirement. Each iterator Next uses its own call context; Close cancels and joins
active enumeration. Failed metadata/error response closes remain Driver-owned
and are retried by Driver.Close. Clients and transports are never closed.

HEAD exposes file/folder and actual native size. The creation-time header is not
reported as ModifiedAt, and Content-MD5 is not fabricated into a CAS validator.
Actual Last-Modified and ETag, when present, remain observations. Version, binding
and NoFollow modes are unsupported. Conditional reads compare the actual returned
observation; they do not make metadata a guard for a later operation.

Mkdir issues native POST with folder=true. Remove uses ordinary synchronous
DELETE, which only deletes an empty directory when the name denotes a directory.
It rejects type constraints and conditions before I/O because no native atomic
guard has been established. The selected root cannot be created or deleted.
Native 200 confirms Applied even when response cleanup or later cancellation
fails. Confirmation loss remains Unknown; no by-name cleanup or mutation retry
is attempted. Native error codes preserve directory-not-empty and other known
file errors, together with the original response cause.

This source does not yet implement exclusive Create, complete Publisher, Move,
NoFollow/bound handles or durable continuation. Ordinary PUT does not establish
those guarantees. Multipart initialization effects, publication visibility and
UUID-safe cleanup remain separate native questions. Copy is verified as a source
to Local; writing to USS through Copy requires future native writing capability.

Closed synthesis tests cover native signing vectors and encoded routing, opaque
pagination including empty pages, actual body ranges and full fallback, Copy,
synchronous changes, unsupported constraints, cancellation, budgets, cleanup and
unknown receipts. No live account or broad service compatibility is asserted.

Native references: [REST](https://docs.upyun.com/api/rest_api/),
[authorization](https://docs.upyun.com/api/authorization/),
[errors](https://docs.upyun.com/api/errno/),
[official SDK behavior](https://github.com/upyun/go-sdk/blob/master/upyun/rest.go).
