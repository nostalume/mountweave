# China Mobile Cloud personal drive

`mobilepan.Open(ctx, mobilepan.Options{Client: api, RootPath: "/"})` verifies a folder using the China Mobile Cloud personal-drive API. The returned Driver supports metadata, directory listing, and original-file reads, including verified byte ranges.

Use an HTTP client whose transport supplies the registered application's current business authorization only to `https://openapi.yun.139.com`. The caller owns account binding, ticket issuance and renewal. An optional `ContentClient` is for temporary signed URLs and must have a credential-free transport. A failed Open can return a Driver with pending response cleanup. Close that result before retrying.

The provider's [cm-cloud-manage v2.0.4 reference package](https://skillhub-1388575217.cos.accelerate.myqcloud.com/skills/163115/cm-cloud-manage/2.0.4.zip) documents the personal root and wire format in `scripts/workflow_s03_files_list.py`, `scripts/adapter_file_list.py` and `scripts/adapter_file_get_download_url.py`. The registered application workflow requires a business ticket.

For a signed-in [website session](https://yun.139.com/w/), supply a `mobilepan.Website` with a page call and a release callback. Use `AccountID` to bind the session, then `Open` with that key and a personal-drive path. Reopen with the stored key to detect an account change. A successful Open owns the session until Close. On failure the caller releases it.

The website provides listing, short-lived original-file URLs and exclusive directory creation. Directory creation fails if a file or directory already occupies the name.

Set `Website.Upload` with a private local staging directory and byte budget
to enable `Core.Write` for one new nonempty file. The Driver consumes and hashes
the entire input before submitting an exclusive create. An ordinary transfer streams one signed
HTTPS part and confirms the exact file ID at the requested name after completion.
The byte budget is local. The cloud can refuse uploads outside its single-part
limits. Existing names are preserved, and an unconfirmed commit reports an
unknown effect. The Driver removes its staging file on return and reports cleanup failures.

Core.MoveInto moves one ordinary file to an existing website folder in the
same mount. The native move preserves existing entries and may choose a
different name. The driver checks the completed task, reads back the same
file ID and returns the actual name.

Single-file removal uses the website's ordinary deletion operation and verifies
completion for the original file ID. Recovery through the website Trash remains
unverified. File bytes
stream through the isolated HTTP client. The host keeps the account key,
verified folder ID and profile reference for reopening. Closed fixtures cover
listing, original-file reads and ranges. The CLI also verified session reopen,
an original-file prefix and a nonzero byte range against the personal drive.
