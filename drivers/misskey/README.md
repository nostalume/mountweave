# Misskey Drive source

Mount the selected account's Drive through the existing Core read API.

~~~go
source, err := misskey.Open(ctx, misskey.Options{
    Endpoint:       "https://social.example",
    RootPath:       "/Attachments",
    Client:         driveClient,
    ContentOrigins: []string{"https://media.example"},
})
if err != nil {
    if source != nil {
        err = errors.Join(err, source.Close())
    }
    return err
}
if _, err := storage.Install("/attachments", source); err != nil {
    return errors.Join(err, source.Close())
}
~~~

The host supplies a Client that authenticates with an account token carrying
`read:drive`. The Client and optional independent, credential-free ContentClient
are borrowed. Credential renewal must retain the same account. RootPath defaults
to the account's Drive root. File and folder lists
use independent native `untilId` pagination and validate parent membership.
Lookups match exact names across both lists and reject ambiguity. A scan retains
one boundary ID per full page of its active stream to detect continuation loops.
Listings are live. MaxResponseBytes defaults to 4 MiB per control response.

List and Stat work without content origins. To read or copy a file, explicitly
allow each trusted HTTPS origin that can serve the account's file URL, including
the instance origin if it serves file bodies. Origin admission applies to the
initial URL and every redirect. A redirect never carries the account token,
cookies or Referer from the control request. Keep private URLs out of late read
and close errors from a custom content transport.

The owner API exposes the file's own URL. Open reads that representation
without using the public proxy or thumbnail URL. An owner URL can itself
refer to a remote link. Stat and List leave byte size unknown, and Open
reports actual HTTP length and ranges. The Driver provides no writes or
bound/versioned access.

[Misskey permissions](https://misskey-hub.net/en/docs/for-developers/api/permission/)
[Drive file endpoint](https://github.com/misskey-dev/misskey/blob/develop/packages/backend/src/server/api/endpoints/drive/files.ts)
[Drive URL projection](https://github.com/misskey-dev/misskey/blob/develop/packages/backend/src/core/entities/DriveFileEntityService.ts#L186-L205)
