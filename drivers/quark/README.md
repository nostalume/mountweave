# Quark Cloud Drive

## Open API

`quark.Open` mounts an authorized drive or subdirectory for metadata, paged listing, and streamed original-file reads. Supply `Options.Application` with your Quark-approved application ID and signing key, a callback supplying one account's current access token, and any application token issued by Quark. A mount also requires the application's approved HTTPS `ContentOrigin`.

- Control requests go to `quark.APIOrigin` with a request ID, an account token, and application signature headers. The signing key stays in the caller's process.
- Body requests use download cookies and the same account token used to acquire that download URL. The driver accepts grants only at the host and port in `ContentOrigin`, over HTTPS. Redirects are refused.

The clients default to the standard HTTP transport with `Options.Application`. If the host already applies credentials itself, omit `Application` and supply separate `Client` and `ContentClient` transports for the same account. Neither client uses its cookie jar for these requests.

The driver uses the native `query_cursor` for listing. Each open requests a fresh download URL for the selected FID and checks the returned FID and file size before streaming the response. The origin must come from the application's trusted configuration. Each new URL is checked against that origin. The caller manages account token issuance and renewal.

The wire contract comes from Quark's official v1.0.25 tool, distributed through its [installer](https://github.com/quark-clouddrive/quarkclouddrive_offical/blob/main/skills/quarkclouddrive/install.sh). The closed fixture covers traversal, native pagination, a ranged body, origin and redirect refusals, and cancellation during body acquisition.

Quark's [third-party consent page](https://pan.quark.cn/open/v1/oauth/authorize) expects an existing application and its registered callback. The reference client decodes a refresh token from the authorization-code exchange. The independent application's renewal endpoint is still unverified. The [official Skill workflow](https://www.quark.cn/documents/help/quark-drive-skill) uses a separate device authorization and token exchange. Obtain application access from Quark under its [service agreement](https://terms.alicdn.com/legal-agreement/terms/b_end_product_protocol/20230516173016165/20230516173016165.html). A public enrollment route and live downloads using an independent application remain unverified.

## Website session

Supply `WebsiteCall` for an authenticated page at `WebsiteOrigin`. Use
`WebsiteAccount` to obtain the account binding, then pass it and the selected
`RootPath` to `OpenWebsite` with the page call, transfer adapter and release
callback. Store the binding and the verified `RootID` with the protected
profile reference to check future opens. The caller owns the session until
`OpenWebsite` succeeds. Closing the Driver releases the session.

The website Driver verifies the account, folder and first listing page. It
uses the site's 50-item pages and checks the reported total. The caller's
`WebsiteTransfer` opens the matched browser response stream. The Driver
checks the download grant, original-file metadata and byte-range response
before returning the stream.

`Core.Remove` sends one file to Trash by FID, checks the terminal native task
and verifies the original directory no longer lists that file ID. An uncertain
result reports an unknown effect. Directory and conditional removal are
unavailable.

[Quark Trash](https://www.quark.cn/documents/help/recycle-bin)
