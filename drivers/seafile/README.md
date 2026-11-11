# Seafile source

This independent module connects one ordinary, unencrypted Seafile library to
Mountweave's existing tree API. It has no Seafile SDK or additional dependencies.

```go
driver, err := seafile.Open(ctx, seafile.Options{
    ServerURL: "https://seafile.example",
    LibraryID: "01234567-89ab-cdef-0123-456789abcdef",
    RootPath:  "/documents",
    Token:     token,
})
if err != nil {
    if driver != nil {
        _ = driver.Close()
    }
    return err
}
mount, err := storage.Install("/cloud", driver)
if err != nil {
    _ = driver.Close()
    return err
}
_ = mount
```

`LibraryID` selects a library, not a parent ID or content version. `RootPath` is
an exact library-relative directory and defaults to `/`. Construction checks the
selected library and, for a non-root directory, confirms that directory directly.
Encrypted libraries return `UnsupportedOperation`; unlocking is a separate
contract. Path spelling is preserved, including non-ASCII names.

## Existing Core operations

- `Stat` uses native file details, falling back to native directory details only
  after a file-detail 404. The library root uses library details. There is no
  account-wide library scan or parent-by-parent resolver.
- `List` streams the native complete directory JSON array through the existing
  iterator and public cursor machinery. It does not invent native pagination or
  use Seafile's directory checksum as a cursor. `MaxResponseBytes` bounds each
  metadata response, including a complete directory array; the default is 4 MiB.
- `Open` obtains a private download capability, closes that short response, then
  makes a fresh body request. It reuses Core's ordinary body ownership and the
  shared HTTP range parser. Actual returned ranges are reported; an upstream
  full response is not misreported as a range. There is no preliminary `Stat` or
  `HEAD` request and no cached download capability.
- `Copy` works through the existing Core API with Seafile as its source.

The fileserver's bare file-ID ETag is not an HTTP validator, and its download
Last-Modified can be response time. These headers do not become Core validators
or file modification times. Native metadata timestamps remain available from
`Stat` and directory entries. Conditional versions and bound access are not
claimed.

## Credentials and ownership

`Token` is sent only to the configured control API. `Client` and `ContentClient`
are borrowed and copied; cookie jars are disabled and redirects are refused.
The default content client is independent of `Client`, including its transport.
A supplied `ContentClient` must not inject account credentials. HTTPS capability
URLs are required for HTTPS servers; explicitly configured HTTP servers may also
return HTTP capabilities.

An iterator keeps one response across public pages. Each `Next` cancellation
interrupts and joins that request, and `Close` cancels before waiting. Core owns
iterators and opened bodies after mounting and retains its existing cancellation
and retirement boundaries. Failed short-response cleanup remains owned by the
Driver and can be retried with `Close`; construction can return a non-nil Driver
with an error for this reason. Borrowed clients are never closed. Native JSON
failure reasons remain attached to classified errors without exposing request
URLs through transport errors.

## Scope

Creation, replacement, directory creation, removal and movement are not exposed.
Native upload and mkdir can silently choose another name; replacement can also
remove conflicting directories. Prechecking a destination would not establish
Core's native publication guarantees. This module therefore reads and copies out
without claiming those mutation contracts.

Three closed synthesis families verify exact library/path scope, public paging,
separate credential recipients, actual ranges, Copy to Local, bounded malformed
responses, failure reasons, cancellation, retirement and retryable cleanup. They
use two local HTTP origins and synthetic credentials, not a real account.
