# WeCom Wedrive source

`wecomdrive.Open(ctx, Options{SpaceID: space, FolderID: folder, AccessToken: token})`
verifies a live folder in one WeCom Wedrive space. The host supplies a token
from the intended enterprise's approved self-built or third-party app with
Wedrive access. The app and its native permissions determine what it can read.
Core authorizes the host's callers separately. Install the driver with Core's
`Install`. The constructor requires a folder ID within the space.

`List` uses the native continuation and `Stat` resolves exact child names.
`Open` and the source side of `Copy` read ordinary files. Wedrive documents
remain visible as names without readable file bodies. The driver asks Wedrive
for a temporary HTTPS download receipt. Its dedicated cookie is sent to the
original origin and dropped permanently when a redirect changes origin.
The app token is sent only to `qyapi.weixin.qq.com`. An optional
`ContentClient` must never add account credentials.

The content response streams directly and an actual partial HTTP response
determines the range. `MaxResponseBytes` bounds each control response and
defaults to 4 MiB. Both clients remain host-owned. Verification uses a sealed
fixture; it does not use an enterprise account.

Official documentation for [folder verification](https://developer.work.weixin.qq.com/document/path/97886),
[listing](https://developer.work.weixin.qq.com/document/path/93657), and
[download receipts](https://developer.work.weixin.qq.com/document/path/97881).
