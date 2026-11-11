# IPFS source

Mount a UnixFS directory from a trusted Kubo RPC endpoint. Provide the
endpoint and a directory CID, then use the existing Core Stat, List,
Open and Copy APIs.

```go
source, err := ipfs.Open(ctx, ipfs.Options{
    Endpoint: "http://127.0.0.1:5001",
    RootCID:  directoryCID,
})
if err != nil {
    if source != nil {
        err = errors.Join(err, source.Close())
    }
    return err
}
if _, err := storage.Install("/ipfs", source); err != nil {
    return errors.Join(err, source.Close())
}
```

Construction verifies the UnixFS directory at the configured CID. Each
path component is matched by exact name in its
parent directory. Reads use the selected file CID. Directory replies
have a per-response budget, `MaxResponseBytes`, which defaults to 4 MiB.
Core pages and replays the fetched list without repeating the RPC.

Files stream through Kubo's `cat` operation. A requested range uses the
native offset and length. The last read checks for an upstream stream
error before accepting the byte count. Symbolic links are never
followed. The module provides read-only CID mounts.

Supply a borrowed `Client` if the RPC endpoint requires authentication.
Kubo RPC is a privileged interface and should be on a trusted network
or authenticated proxy. The driver trusts that endpoint to resolve and
serve the DAG. The integration fixture runs against a closed local
HTTP service.

[Kubo RPC reference](https://docs.ipfs.tech/reference/kubo/rpc/)
