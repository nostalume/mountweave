# File service specification

This guide describes the common contracts for an embedding Go host, a Driver
and a protocol adapter. The [Core package contract](../doc.go) specifies each
call and its edge cases. Deployment settings belong to the host; native
capabilities and service limits belong to each [Driver](drivers.md).

## Names, identity and authority

| Address | Meaning |
| --- | --- |
| `Path("/files/report.txt")` | A name in the directory tree. Core routes it to the installed mount. |
| `Key("/objects", "a//b")` | An exact object key in that binding. Key spelling is preserved. Keys `a` and `a/b` may coexist. |
| `Prefix("/objects", "a/")` | A selector for a group of keys. It cannot be used as a directory or file mutation target. |

The caller supplies a principal. Core asks the host's authorizer about the
operation and address. An alias also needs authorization on the corresponding
primary address. Tree and key views share the mount's lifetime and mutation
state. Admitted work uses the captured binding; a replacement mount cannot
silently acquire that work. An address alone does not identify fixed content.

## Read and enumerate

- `Stat` observes the selected entry. `List` selects children or keys and has
  an opaque cursor. The first request sets a lifetime. A continuation uses the
  same principal, query, mount binding and page limit. A cursor can expire or
  become invalid after a mutation or retirement. Listings are live views.
- `Open` returns a sequential body by default. A requested stream range can
  yield a complete response; use the returned range and length. A service
  granting a URL or advertising ranges has yet to prove the delivered bytes.
- Bound file access is optional. `BoundObject` retains the opened object's
  identity; `PinnedVersion` also fixes contents. `ReadAt` reaching EOF does not
  close the file. Explicit live editing can leave visible partial changes.

Callers select an immutable version separately from a validator used in a
condition. `Lstat` and no-follow access are optional; unsupported link safety
must be refused rather than replaced with ordinary link traversal.

## Change and retry

| Operation | Observable behavior |
| --- | --- |
| Default `Write` or `Create` | Requires create access at the new name. An input failure may leave incomplete new content. The caller owns and closes the input. |
| `Write` with `Publish(...)` | `Publish()` allows creation or replacement and needs write access. `Publish(IfAbsent())` needs create access and requires an absent name at native commit. Both validate full input, normal EOF and any declared length before publication. A failed predicate is distinct from an unsupported guarantee. |
| `Copy` and `Move` | Copy needs read access at the source. Move needs removal access. A supported cross-mount move publishes the target before deleting the matching source and keeps separate effects. |
| Directory destinations | `MoveInto`, `CopyInto`, `CopyTreeInto` and `WriteInto` take an existing mounted tree directory. They prefer the source or supplied name, preserve existing children and report the actual address with the effects. A Driver that allocates a name during relocation requires `CreateChildAction` at the directory, including the primary path of an alias. When Core selects an exact candidate, it checks metadata and authorizes that candidate before the exclusive operation. The candidate can still be occupied by a concurrent creator. |
| Batch and directory operations | Compose existing operations with bounded planning and execution. The default is serial. Conflict ordering, partial effects and removal scope remain observable. |

Check the effect together with the error: `Unchanged`, `Applied`, `Incomplete`
or `Unknown`. A confirmed mutation may still return a cancellation or cleanup
error. Investigate an unknown effect with native evidence before retrying.
Ordinary tree removal works on discovered names. Explicit subtree removal needs
its own grant and can affect descendants added during the operation. Native
subtree removal reports a root result rather than per-child effects.

## Close and retire

Successful `Install` transfers the Driver to Core; on rejection, the caller
closes it. Close returned bodies and files and release unused cursors. Streams
may release on completion; bound files retain their resource through individual
calls. Retirement stops admission, invalidates cursors, requests cancellation
and joins cleanup. A wait deadline can expire before native work or cleanup
finishes. Replacing a binding requires explicit installation or activation.

For wire requests and replies, see the [adapters](../protocols/) and the
[JSON HTTP schema](../api/openapi.json).
