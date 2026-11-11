# FTP source

This optional module connects an FTP service to Mountweave using the standard
library. It provides ordinary tree reads and directory creation through existing
Core APIs. There is no client registry, transfer scheduler or persistent journal.

```go
storage, err := ftp.Open(ctx, ftp.Options{
    Address:  "files.example:21",
    RootPath: "/data",
    Username: username,
    Password: password,
    Security: ftp.ExplicitTLS,
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

Open validates settings, authenticates and resolves the root using CWD/PWD.
Validate is optional Control preflight using the same static rules. Existing
TypedFactory can supply native parameters; no configuration format is introduced.
A non-nil failed Driver must be closed. Successful installation transfers ownership
to Core.

## Native configuration

Security selects Plain, ExplicitTLS or ImplicitTLS. TLS modes protect both control
and data connections and never fall back to plaintext. TLSConfig is cloned; when
omitted, normal certificate verification uses the configured address's hostname.
Control and data share a session cache. Passive EPSV is preferred; an explicitly
unsupported EPSV can fall back to PASV. Both use the control connection's peer
address, never an arbitrary host advertised in a reply.

RootPath uses the server's native syntax. PWD's quoted result is retained without
host filesystem normalization, and each borrowed session returns to that root.
Child components are selected by their exact listed spelling and CWD, avoiding
guesses about the server's absolute pathname syntax. This is a path selection,
not a pinned directory identity. Names use UTF-8; no lossy encoding conversion is
performed. Active data connections and legacy name encodings are not implemented.

Optional defaults are four connections, 1 MiB per complete control reply, 64 KiB
per control/listing line, Core's maximum page entries per native batch, and five
seconds for transfer finalization/cleanup. These are adjustable policies. Listing
and file totals are not limited to the control-response budget. TLS, socket,
decoded metadata and Core's public page storage have their own memory costs.

## Flow and ownership

One activity exclusively borrows a reusable control connection and owns its data
connection. Capacity exhaustion fails rather than waiting while a composite caller
holds another resource. No native command or mutation is automatically replayed.
Cancellation closes only that activity's connections. Cleanup stops and joins
its callback before returning a reusable connection, and failed local cleanup
remains owned for retry. Repeated Close on a completed owner cannot affect a later
borrower.

Normal data EOF is not transfer success: the final control reply must confirm
226 or 250. A missing, malformed or failed confirmation retires the session and
is returned to the caller. Early Close retires the session without draining a
file/directory or interpreting an ABOR acknowledgment as transfer success.

MLSD feeds a streaming Iterator. Known unsupported MLSD can fall back to LIST;
the baseline understands EPLF and common Unix records with an unambiguous name
separator. Other formats, ambiguous names and unsupported entry kinds fail
explicitly instead of silently omitting entries. Core owns public pagination,
cursor replay and expiration. Next's context applies only to that call.

Exact lookup consumes each parent listing, retaining only the matching metadata,
and checks final confirmation before changing directory. Time therefore grows
with the sum of scanned parent entries; there is no persistent directory cache.
Enumeration retains a bounded batch rather than an entire directory.

Open returns ordinary sequential Body. Earlier listing/SIZE observations do not
establish the actual transfer length, so Open leaves length and total size unknown.
Requested ranges receive the existing explicit full-body fallback (Range=nil).
No hidden tail drain or fabricated partial-range receipt is used. Core Copy can
use FTP as a source without a separate copy implementation.

## Capability boundary

Ordinary access uses the service's exported namespace, whose internal references
may not be visible through FTP facts. This does not establish LSTAT, NoFollow or
bound-object access; those modes are not declared. Mkdir uses native MKD and
preserves its actual effect and failure, including unknown confirmation.

This source does not implement FileCreator, FilePublisher, Remover or Relocator.
STOR can overwrite, STOU chooses a server name, and standard rename does not
establish conditional publication. Prechecks do not supply these guarantees.
No POSIX/ACL model or durable continuation is added. Protocol exports continue
to admit operations according to these actual capabilities.

Closed tests reuse the existing FTP export and Local over real loopback control
and data connections, with synthetic accounts and trusted test certificates.
They do not certify every FTP server or TLS session-reuse policy. Local module
replacements are for checkout development; no source version is published here.
