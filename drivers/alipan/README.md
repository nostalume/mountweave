# Aliyun Drive Open read adapter

This independent Go module resolves a host-prepared drive and folder root through
Aliyun Drive Open, then implements github.com/nostalume/mountweave.Driver. It uses the standard
library and the core module. It does not import the S3 SDK or OpenList.

The API client, drive ID, root folder ID, and one credential.Source are supplied
explicitly. The prepared root is assumed to be a directory. New transfers
credential-source ownership only on success; a rejected constructor leaves it
with the caller. A rejected core.Install leaves the driver with the caller.
After installation, retirement closes the driver and waits for its credential
writer. Clients and transports are borrowed and are never closed by the driver.

## Directory reads

OpenDirectory supplies core.DirectoryScope with one bound parent ID. Complete
enumeration verifies child-name uniqueness, retaining IDs only in that scope.
OpenFile uses the selected ID and the existing isolated download path without
rescanning ancestors. Close clears this lookup; it is not a global cache,
content snapshot, version condition or source-delete capability. Core.CopyTree
can use this adapter as a source; writes remain unsupported.

## Name and content semantics

Resolution lists every page at each path segment before reporting one matching
ID or absence. Two matching names are ambiguous, including a later-page match.
An incomplete scan returns its failure. There is no directory-wide ID cache,
get-by-path shortcut, strong validator, or directory snapshot.

Enumeration preserves native continuation and order, retaining one batch.
The native API page size is capped at its documented 100. MaxResolvePages and
MaxResponseBytes are optional host policies; zero leaves them unset. Contexts
and client deadlines also govern work. Immediate repeated markers are rejected;
longer faulty cycles require cancellation, cursor expiry, or a host resolution
budget. Benchmark numbers provide no defaults.

Temporary download URLs remain private. Downloads use fresh HTTPS requests with
no provider bearer header or cookie jar. API and download redirects are rejected.
Only GET download targets are admitted. Full responses remain full, unknown
length remains unknown, and partial responses need matching actual ranges.
Failures return any opened body for the core to close. A failed internal API
body close remains owned by the driver and prevents successful retirement.

## Rotating credentials

The credential subpackage owns one refresh writer and durable-save operation.
The host supplies the latest confirmed credential.Record and a durable
func(context.Context, credential.Record) error save callback. It owns exclusive
writer admission, complete record replacement, loading, and secret protection. OAuth.Refresh performs the official application token exchange;
it does not acquire consent, find secrets, or save tokens. Example assembly:

~~~go
oauth := alipan.OAuth{Client: client, ClientID: appID, ClientSecret: appSecret}
tokens, err := credential.New(initialCheckpoint, oauth.Refresh, saveDurably)
if err != nil { return err }
storage, err := alipan.New(alipan.Options{
    Client: client, DriveID: driveID, RootID: rootID, Credentials: tokens,
})
if err != nil { return errors.Join(err, tokens.Close()) }
if _, err := reader.Install("/cloud", storage); err != nil {
    return errors.Join(err, storage.Close())
}
~~~

Use a separate Source per ownership scope; do not transfer the same Source to
two drivers or create competing sources for the same rotating credential.
A canceled waiter does not cancel shared refresh. Native access-token
invalid/expired errors permit one replay after refresh. A stale rejection uses
the already-updated access token. Refresh and save deadlines belong to the
host's callbacks and prepared client.

Each rotation first saves Record{Token: current, Pending: true} and waits for
confirmation before invoking the provider. A failed pending write invokes no
refresh. The source stops; RetrySave does not retry an unacknowledged intent.
Reload/reconcile the store before preparing another source.

After a known refresh result, the new token is retained and saved in a confirmed
record (Pending: false) before use.
Save failure stops access; RetrySave retries only that retained token's
persistence. An uncertain refresh response stops automatic refresh and requires
external reconciliation or fresh authorization. RetrySave is explicit recovery
while the source remains open. Close stops admission, cancels the writer and
waits for acknowledgement; it is terminal and cannot roll back a rotation or
promise that an unsaved new token will survive process death.

New rejects pending checkpoints with ErrRefreshUncertain, without invoking the
provider. Following a crash, the host must load the latest complete record; it
must not ignore an unreadable or pending record and fall back to an older token.
A pending record requires explicit reconciliation or fresh authorization.
Confirmed records can resume without rotating an unexpired access token.

The source coordinates one writer; it is not a file/database implementation or a
cross-process lock. Save must acknowledge complete durable replacement, and the
initial record must be durably confirmed. Newly authorized or reconciled
credentials must be committed before construction; merely clearing Pending on
the old token is not recovery. A crash can still lose a newly issued token; the pending marker
prevents blind replay, not that loss or an atomic provider/store commit. Storage
corruption, power-loss durability and production secret protection remain host
obligations. Full configuration persistence stays outside the read core.

## Verification

~~~sh
cd drivers/alipan
go test ./...
go vet ./...
go test -race ./...  # requires a supported C compiler
~~~

Controlled transports test native JSON, ID resolution, range responses, bearer
separation, token exchange, shared refresh, persistence failure and cancellation.
Most save callbacks model outcomes. One disposable-file subprocess case kills
the writer after a synthetic provider effect and after confirmation; it checks
that pending recovery stops and confirmed recovery resumes. It does not test
power loss or certify a production store.
No live provider accounts are contacted. Account authorization, real service
compatibility, production storage durability and product performance remain
unverified.

The native contract was inspected in Alibaba's official
[SDK at revision 66cdb2679e9cbacd7f682e43e784cf481f200a5a](https://github.com/alibaba/aliyunpan-ios-sdk/tree/66cdb2679e9cbacd7f682e43e784cf481f200a5a/Sources/AliyunpanSDK).
The module identity is github.com/nostalume/mountweave/drivers/alipan.
Version v0.1.0 depends on Mountweave core v0.1.0. The relative replace is for checkout development only.
