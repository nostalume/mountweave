# China Mobile Cloud personal drive

`mobilepan.Open(ctx, mobilepan.Options{Client: api, RootPath: "/"})` verifies a folder using the China Mobile Cloud personal-drive API. The returned Driver supports metadata, directory listing, and original-file reads, including verified byte ranges.

Use an HTTP client whose transport supplies the registered application's current business authorization only to `https://openapi.yun.139.com`. The caller owns account binding, ticket issuance and renewal. An optional `ContentClient` is for temporary signed URLs and must have a credential-free transport. A failed Open can return a Driver with pending response cleanup. Close that result before retrying.

The provider's [cm-cloud-manage v2.0.4 reference package](https://skillhub-1388575217.cos.accelerate.myqcloud.com/skills/163115/cm-cloud-manage/2.0.4.zip) documents the personal root and wire format in `scripts/workflow_s03_files_list.py`, `scripts/adapter_file_list.py` and `scripts/adapter_file_get_download_url.py`. The registered application workflow requires a business ticket.

For a signed-in [website session](https://yun.139.com/w/), supply a `mobilepan.Website` with a page call and a release callback. Use `AccountID` to bind the session, then `Open` with that key and a personal-drive path. Reopen with the stored key to detect an account change. A successful Open owns the session until Close. On failure the caller releases it.

The website provides listing and short-lived original-file URLs. File bytes stream through the isolated HTTP client. The CLI development build binds the selected account and native folder ID through a protected profile. The workflow is covered by closed fixtures. Live CLI downloads and range responses remain pending.
