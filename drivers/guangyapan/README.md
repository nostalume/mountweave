# Guangya Pan Open API source

Mount an authorized user's Guangya Pan directory with the existing Core read
API. Use an approved Open Platform application and a host-provided user OAuth
credential. The platform assigns business requests to the authorized App/Web
client. Follow the approved application's deployment scope.

~~~go
source, err := guangyapan.Open(ctx, guangyapan.Options{
    ClientID: appID,
    RootID:   folderID,
    Client:   authenticatedClient,
})
if err != nil {
    if source != nil {
        err = errors.Join(err, source.Close())
    }
    return err
}
if _, err := storage.Install("/guangya", source); err != nil {
    return errors.Join(err, source.Close())
}
~~~

An empty `RootID` uses the account root. The Driver uses the production
Open API and sends `x-client-id` only there. The borrowed Client supplies
the matching user Bearer token. The host owns authorization, renewal for the
same account, and pacing across its clients. Platform limits include five
list requests and two download grant requests per second per IP. Native
`APIError.Code` distinguishes cases such as expired authorization (117) and
membership required (430) without exposing reply text.

Each native page requests 20 entries. Core owns its public cursor and replay.
Path selection scans the observed parent pages and matches exact names. The
listing is live, and each page uses its own reported total. `MaxResponseBytes`
defaults to 4 MiB per control response.

Open requests one file grant by its resolved ID and reads the signed HTTPS
URL with an independent, credential-free ContentClient. The Driver preserves
the signed query, follows bounded HTTPS redirects, and does not cache grants.
A host may restrict the content transport to its trusted origins. Keep
private URLs out of late I/O errors from a custom transport. Ordinary Open
and source Copy stream the body and expose actual HTTP ranges. When the
selected file reports a size, the Driver checks the HTTP total or verifies an
unframed stream through EOF. VOD playback, writes and bound access are outside
this source's scope.

[Official Guangya Pan Open API v1.3](https://ossdata.guangyapan.com/doc/guangyapan_openapi_cn.md)
[Official business API reference](https://github.com/guangyapan/agent-skills/blob/master/skills/guangyapan-open-api/references/business-api.md)
