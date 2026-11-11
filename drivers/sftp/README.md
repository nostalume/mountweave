# SFTP source

This optional module connects an SFTP v3 service to Mountweave. It requires Go
1.25 or newer and uses the same file-transfer codec already used by the SFTP
export. The root Core remains independent of SSH and SFTP implementations.

```go
storage, err := sftp.Open(ctx, sftp.Options{
    Address:  "storage.example:22",
    RootPath: "/project",
    SSHConfig: &ssh.ClientConfig{
        User:            "account",
        Auth:            []ssh.AuthMethod{ssh.PublicKeys(signer)},
        HostKeyCallback: knownHosts,
    },
})
if err != nil {
    if storage != nil {
        err = errors.Join(err, storage.Close())
    }
    return err
}
if _, err := files.Install("/remote", storage); err != nil {
    return errors.Join(err, storage.Close())
}
```

Use Core's existing Stat, List, Open, Write, Mkdir and Copy APIs. Open validates
settings and resolves/verifies the remote root. Validate is only optional Control
preflight and uses the same static rules. Existing TypedFactory can resolve native
authentication inputs; there is no additional registry or configuration format.
A failed constructor may return an owned Driver requiring Close. Successful Core
installation transfers ownership.

## Connection and transfer ownership

The Driver owns SSH connections, SFTP sessions and their buffers. One activity
leases a complete connection; normal completion returns it for reuse. Canceling
an activity closes only its connection and joins local I/O. A broken connection
is not reused, and mutations are never automatically replayed. This also covers
cancellation while an SSH channel is being opened. Authentication providers must
be safe for concurrent connection establishment; host-key verification is explicit.

Optional defaults are four connections, sixteen in-flight packets per activity,
4 MiB shared payload buffering and five seconds for native CLOSE cleanup. These
are configurable policies, not machine-derived requirements. Capacity exhaustion
fails before a new target effect; it does not wait while a composite operation
holds its source. In particular, same-instance Copy can need two connections.
SSH and codec buffers are separate from the payload budget.

READ/WRITE use bounded native packet windows, request-ID matching and actual
replies, including out-of-order and short DATA. File-operation scheduling remains
owned by Core. WRITE success alone is not file completion: CLOSE can still fail.
CLOSE consumes the handle even if its reply is lost, so cleanup never resends it.
Cleanup timeout retires that connection and preserves the uncertain file effect.
Failed local socket cleanup remains owned and can be retried.

## Paths and native effects

RootPath uses the server namespace, including Windows servers. It is resolved by
REALPATH and remains a path selection, not an immutable directory identity.
Logical paths never use the host OS's filepath rules.

STAT/FSTAT do not return a stored name. Existing path components are therefore
matched exactly against native READDIR names; metadata Stat consumes those
attributes instead of repeating a leaf STAT. Create/Mkdir resolve only the parent,
then use native exclusive creation. This adds directory lookup requests, including
directory CLOSE. Lookup may scan native pages until a matching name is found;
there is no persistent directory cache. Name observations do not establish atomic
NoFollow or protection from concurrent ancestor replacement.

Open uses the resolved name, then OPEN and FSTAT to describe the actual file
handle. Ordinary body reads support ranges and retain the opened object across a
path replacement. They do not expose the stronger bound-file API. Unknown native
sizes remain unknown; absent type flags are not fabricated as a regular file.
Open and List reject a resolved entry of the wrong kind before requesting a new
handle. Listings use OPENDIR/READDIR/CLOSE, retain one native batch, and reuse
Core cursors. A native NAME reply must supply at least one entry. The server
signals normal end of enumeration with SSH_FX_EOF.
Next contexts affect only their call; close/cancellation and retirement retain the
same resource owner. Frame lengths and collection counts are checked before
allocation; the current codec profile bounds native frames to its default limit.

Write uses CREATE|EXCL without truncation, implicit parent creation or deletion on
failure. Known, unknown and zero-length inputs use the existing EOF/length helpers
and shared byte budget. Applied requires complete input plus WRITE/CLOSE receipts.
Visible partial input retains Incomplete; lost receipts retain Unknown. Generic
v3 FAILURE does not prove an absence of effects, even when it might mean an existing
target; the original status/message remains available. MKDIR sends no permission
attributes, so the server supplies its defaults. There is no new POSIX/ACL system.

## Capability and validation scope

This source implements Driver, FileCreator and DirectoryMaker. It does not declare
Remover, Relocator, FilePublisher, DirectoryScope, LSTAT, NoFollow, BoundRead,
LiveEdit or durable continuation. Version/tag predicates are unsupported. A filename
lookup or REALPATH is not a substitute for those native guarantees.

The existing SFTP export requests BoundRead with NoFollow; consequently the source
baseline does not yet establish SFTP-export-to-SFTP-source composition. Core Copy
and ordinary reads/writes use their existing APIs and are covered independently.

Closed tests reuse the existing SFTP export and Local with an in-memory-key SSH
listener. They cover cancellation isolation, connection reuse, packet ordering,
partial input/effects and CLOSE receipts. An aliasing server fixture verifies
case variants cannot bypass exact-name selection. They do not certify a live
vendor account or every server/filesystem version. The export module is a test-only
dependency; production code imports no protocol export. Local module replacements
are for checkout development; no versioned source release is published here.
