# WUYING enterprise drive

This optional module mounts a personal or team space in an Alibaba Cloud
WUYING enterprise drive. Pass the region, enterprise `DriveID` (native
`CdsId`), and a prepared HTTP client to
`wuyingdrive.Open(ctx, wuyingdrive.Options{...})`. Set exactly one of
`EndUserID` for the assigned personal space and `GroupID` for a team shared
drive (native `GroupId` from [DescribeCloudDriveGroups](https://help.aliyun.com/zh/wuying-workspace/developer-reference/api-ecd-2020-09-30-describeclouddrivegroups)).
Install the Driver in Core.
The control transport must sign the ECD OpenAPI requests with RAM or STS
credentials authorized for `ecd:ListCdsFiles` and `ecd:DownloadCdsFile`.

The control client uses the [regional public endpoint](https://help.aliyun.com/zh/wuying-workspace/developer-reference/api-ecd-2020-09-30-endpoint)
and the [ACS3 request format](https://help.aliyun.com/en/sdk/product-overview/v3-request-structure-and-signature).
`ListCdsFiles` walks exact names by native parent ID and continuation token.
`DownloadCdsFile` grants the original and confirms its file ID and total size.
The confirmed size is available as metadata even when the body has no HTTP
length and must be verified through EOF.
The separate, optional `ContentClient` must be credential-free, including for
HTTPS redirects back to the control host. Clients remain host-owned. The
control response limit defaults to 4 MiB.

The module supports Core Stat, List, streaming Open and Copy as a source.
Ranges are reported only when served upstream. The sealed test covers the
wire contract. A live tenant has not been tested.
