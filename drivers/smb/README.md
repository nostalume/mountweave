# SMB source

This optional module connects a rooted SMB file share to Mountweave. It uses the
native client's authentication, transport, signing and request codecs; it does
not implement an SMB protocol stack or a second file-management API.

```go
storage, err := smb.Open(ctx, smb.Options{
    Server:   "nas.example",
    Share:    "data",
    RootPath: "/project",
    Dialer: &smb2.Dialer{
        Credentials: auth.NTLMCredential{User: username, Password: password},
        RequireMessageSigning: true,
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

Use Core's existing Stat, List, Open, Write, Mkdir and Copy. Open performs static
validation, establishes a session/share and verifies the root with a directory
open and a bounded query. Validate is optional Control preflight using the same
rules. RootPath defaults to `/` and uses Core-style share-relative components.
Server selects the native authentication target; custom ports and transport
settings belong to the native Dialer. A non-nil failed Driver must be closed;
successful Core installation transfers ownership.

The module requires Go 1.27. The client is pinned to
`github.com/hirochachacha/go-smb2/v2` commit `7f69f6587832`; its experimental request
types remain private to this module. Native Dialer configuration is snapshotted,
including its configuration slices. Referenced credentials and transport
factories remain borrowed, must be safe for concurrent use, and are not closed by
the Driver. A credential provider's own authentication/KDC exchanges retain its
timeout and cancellation behavior before a transport exists.
The native-request profile disables unrelated AAPL security-descriptor negotiation,
avoiding an extra open of the share root before selecting the configured subtree.

For Lenovo Personal Cloud T1 and T2, enable Samba under **Tools > SAMBA** in
the Data Guardian app and keep anonymous access disabled. Use the NAS LAN host
as `Server`, a share advertised by the NAS as `Share`, and the SMB credentials
shown by the app. `RootPath` can select a folder within that share. For a
read-only mount, deny mutations in the Core Authorizer. The device firmware's
SMB dialect and signing compatibility still need verification on a NAS.
[Lenovo T1 Samba setup](https://iknow.lenovo.com.cn/detail/188240) · [Lenovo T2 Samba setup](https://iknow.lenovo.com.cn/detail/196362)

For ORICO CyberData, enable Samba and select SMB3 in the app's file-sharing
settings. Create a Samba subaccount scoped to the selected folder. Use the NAS
LAN host as `Server`, the name from the SMB share path as `Share`, and the
subaccount credentials in `Dialer`. Keep message signing required as in the
example above. Deny mutations in the Core Authorizer for a read-only mount.
Signed connections have yet to be verified on a CyberData device.
[ORICO CyberData Samba setup](https://www.orico.com.cn/index/topic/nas/help_detail.html?cid=13&id=465&pid=37)

## Session and handle ownership

Each activity exclusively borrows a reusable Session and TreeConnect. The Driver
owns those sessions, their transports and opened FileIds. Constructor transport
ownership is captured before the SDK can fail without returning a Session.
Cancellation aborts only that activity's session and waits for the native call
and callback to exit before buffers or a reusable session are returned.

Normal completion closes the native handle. Driver shutdown retires owned
sessions by transport abort rather than waiting for LOGOFF. Unknown CREATE/WRITE
operations are not replayed, and disconnected FileIds are not reopened by path.
Close is serialized with in-flight reads/enumeration; repeated Close on a completed
owner cannot interrupt a successor. Failed cleanup remains owned and reported;
the SDK may cache a terminal transport-close failure, so retries cannot promise
recovery from every provider failure. An already closed socket alone is not a
cleanup failure; joined independent failures remain visible.

Defaults are four sessions, 64 KiB I/O blocks, 64 KiB native directory pages,
4 MiB shared write payload buffering, and five seconds for native handle cleanup.
The current one-credit request profile accepts block/page sizes up to 64 KiB.
These are protocol/profile and configurable resource policies, not machine-derived
budgets. Capacity exhaustion fails without waiting while a composite caller holds
another resource. Same-instance Copy can need two sessions. SDK packet, crypto
and credit storage and Core's public pages are separate from write payload storage.

## Names, reads and creation

Paths are admitted once at the native boundary. Each lookup uses the admitted
literal component as the native directory search pattern and checks the stored
name in the reply. A case-insensitive match or a short-name alias alone does not
select a path. Lookup reuses the matching entry's attributes and closes the
lookup handle. Ordinary listing still uses the wildcard query.

ADS syntax, native filename metacharacters, overlong components and trailing
dot/space aliases are refused instead of normalized. SDK response decoders validate
wire bounds and UTF-16; public directory items additionally meet Core/native name
rules. An exact lookup reuses the already admitted matching name. Unsupported
reparse entries are reported, not followed or silently hidden by enumeration.

Ordinary access describes the service's exported namespace. The configured
subdirectory is a path selection, not a pinned parent identity. Requests disable
the SDK's extra symbolic-link resolution; DFS or explicit link redirection does
not automatically connect elsewhere or escape the selected root. This does not
establish all-component NoFollow.

Open reads from one actual FileId and uses its CREATE response's size/type/time,
without a second path Stat. Single ranges read only their requested extent.
The Body stays on the same opened object across path replacement. It implements
ordinary streaming, not the stronger bound-file API. Enumeration retains one
native response batch and uses Core's cursor/replay ownership; Next contexts
apply only to that call.

Write uses native FILE_CREATE with a non-directory constraint, without an absence
precheck. Known, unknown and empty inputs reuse the shared EOF/length and budget
mechanisms. Complete input, acknowledged writes and confirmed CLOSE establish
Applied; a later cleanup error is still returned without erasing that fact.
Input failure can leave Incomplete, and missing native confirmation leaves Unknown.
No failure triggers deletion by name. Mkdir similarly uses FILE_CREATE with the
directory constraint; an acknowledged creation survives a later cleanup error.
Permissions inherit the server's policy; no POSIX mode or ACL layer is introduced.

## Validation scope

This source implements Driver, FileCreator and DirectoryMaker. No BoundRead,
LiveEdit, Lstat, NoFollow, FilePublisher, Remover, Relocator or durable continuation
is declared. FileId is an open identity, not a content version or deletion guard.
Core owns cross-driver Copy and its scheduling; SMB adds no copy implementation.

Closed tests use a test-only MIT SMB server, temporary storage, synthetic NTLMv2
accounts and signed SMB 3.0.2 connections. A small filesystem adapter supplies
actual OS-exclusive creation to that server's backend. Tests cover multi-block
reads/writes and Copy, range reads, independent-instance create competition,
partial input, lost WRITE/CLOSE replies, cancellation isolation and capacity.
The server itself does not correctly paginate a truncated native response; a
narrow signed response projection reuses its authenticator/signing and actual
connection to verify continuation and final NoMoreFiles. This is not live Windows,
Samba/NAS, Kerberos, DFS, QUIC or other-dialect certification. No throughput gain
is claimed. Local module replacements are for checkout development; no source
version is published here.
