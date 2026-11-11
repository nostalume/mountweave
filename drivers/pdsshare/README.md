# Alibaba Cloud PDS sources

This optional Go module mounts a drive or a received share from Alibaba Cloud
Drive and Photo Service (PDS) in Standard Mode. Personal AliPan links and
account credentials belong to separate services.

## Received share

```go
source, err := pdsshare.Open(ctx, pdsshare.Options{
    DomainID: pdsDomainID,
    ShareID:  shareID,
    Password: extractionCode,
})
if err != nil {
    if source != nil {
        err = errors.Join(err, source.Close())
    }
    return err
}
if _, err := storage.Install("/shared", source); err != nil {
    return errors.Join(err, source.Close())
}
```

Omit `Password` for a share without an extraction code. The Driver exchanges
the supplied share credentials at the domain's HTTPS API endpoint, then verifies
the share root. It reuses the token until the native expiration time and renews
only for the same share. Revocation, tenant restrictions and interactive
verification are reported through the native API result.

## Authenticated drive

```go
source, err := pdsshare.OpenDrive(ctx, pdsshare.DriveOptions{
    DomainID: pdsDomainID,
    DriveID:  driveID,
    Client:   authenticatedClient,
})
```

`Client` supplies OAuth or AccessKey authentication for the selected tenant.
Omit `RootID` to use the drive root. An explicit folder ID is verified with
PDS before mounting. Handle a failed constructor and install the source the
same way as for a share.

## Reading

Core `Stat`, `List`, `Open` and `Copy` use the mounted tree.
Each path component reads the selected parent's pages and checks its exact name
and unique ID. PDS markers stay in the Driver, while Core owns public pagination
and replay. Pages are observations and may be empty before the final marker.
`MaxResolvePages` optionally limits each path component. `MaxResponseBytes`
limits one control response and defaults to 4 MiB.

Only the tenant API receives share or account authorization. The body request
uses a separate client and the authorized HTTPS URL. Both clients are borrowed.
A custom content transport must not add credentials.
The Driver disables cookie jars and redirects. Core owns successful bodies
and reports the range actually served. When the grant provides a size, the
Driver checks it against the response and checks unframed bodies through EOF.
The Driver owns control and failed download responses and retries failed
closes at retirement.

This module provides read-only access. The integration workflow uses synthetic
credentials and a closed HTTP fixture.

[Share workflow](https://www.alibabacloud.com/help/zh/pds/drive-and-photo-service-dev/user-guide/file-sharing-related-operations) ·
[Drive authentication](https://help.aliyun.com/zh/pds/drive-and-photo-service-dev/developer-reference/invoking-method) ·
[List files](https://help.aliyun.com/zh/pds/drive-and-photo-service-dev/developer-reference/api-pds-2022-03-01-listfile) ·
[Download URL](https://help.aliyun.com/zh/pds/drive-and-photo-service-dev/developer-reference/api-pds-2022-03-01-getdownloadurl)
