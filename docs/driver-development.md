# Driver development

Use this guide for a storage source. For a server that exposes mounted files,
start at the [protocol adapter boundary](architecture.md#where-to-change-behavior).
Pick a source from the [capability table](drivers.md) to find its typed options,
native behavior and tests. A source should work when embedded in any Go host.

## Follow one file through the Driver

1. **Construct and bind.** Accept the settings needed by the upstream service.
   Identify the authorized account or endpoint and one root. Explain whether
   a constructor resolves a path to an ID or accepts a native path or object
   prefix. Reject an invalid or mismatched root. Preserve the caller's ability
   to clean up a partially constructed Driver.
2. **Locate and list.** Implement the [Driver interface](../types.go) using
   rooted addresses. Keep native IDs and pagination inside the Driver. Support
   a lazy [iterator](../list.go) with the provider's termination rule and a
   cancellable `Next`. Path lookup must account for later pages if they can
   change the result, including duplicate names. Let Core own its public cursor.
3. **Open the body.** Bind the download to the selected file. Record the length
   and actual range from the native response where known. Keep the service's
   control credentials with authorized recipients. Treat a signed media URL as
   a separate receiver with its own rules. Keep sensitive URLs and credentials
   out of public errors.
4. **Add guarantees when proven.** A read-only Driver needs `Stat`, `Enumerate`,
   `Open` and `Close`. Use an existing optional interface for native creation,
   conditional publication or bound access. A preflight check alone cannot
   prove an exclusive change at commit. Return the observed effect and keep
   failed cleanup with its owner.

[URL tree](../drivers/urltree/README.md) shows a static path source.
[Guangya Pan](../drivers/guangyapan/README.md) shows a directory by ID with paged
lookup and a separate download grant. [S3](../drivers/s3/README.md) shows literal
object keys and native conditions. [Quark](../drivers/quark/README.md) shows how
a host supplies an authenticated website session and owns profile management.

## Change and verify a source

For a pagination change, begin in the source's iterator. Compare the native
cursor or offset, terminal response and malformed reply. Keep the public
`List` contract. Extend the existing fixture so a desired file or duplicate
name occurs on a later page and the old implementation would fail. Check
cancellation or cleanup when the change can affect it.

For example, to check folder and download behavior in Guangya Pan, run from the
repository root:

```sh
export GOWORK=off
go -C drivers/guangyapan test ./...
go -C drivers/guangyapan vet ./...
```

Use loopback or controlled transports and synthetic credentials. Reuse fixtures
and keep one test per distinct guarantee. When editing a shared Core interface,
check the affected consumers too. Update the Driver's README for native options
and limits. Update the capability table for user-visible behavior changes.
See the [contribution rules](../CONTRIBUTING.md) and [release guide](releases.md).
