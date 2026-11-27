# Tianyi Personal Cloud

`NewBrowserSession` runs operations through the authenticated
personal website. The host manages the browser profile and closes the session
if opening the Driver fails. `OpenWebsite` resolves a root path and binds it
to the selected account and native folder ID.

The Driver supports directory browsing, native pagination and ordinary
single-file downloads up to 1 GiB. It asks the website for the selected
file's grant and reads the HTTPS attachment with an isolated HTTP client.
The browser's login credentials stay in the website session. The Driver
checks the attachment and the listed file size through EOF.

`Core.Remove` sends one regular file to the recycle bin. It submits the
native file ID and validates the exact completed task and successful file ID.
Unconfirmed effects are reported as unknown. Directory and conditional
removal remain unsupported.

`Core.Move` renames one regular file in its current directory. An occupied
name is reported as a conflict. The Driver confirms the same native file ID
at the requested destination. Cross-directory moves and conditional renames
remain unsupported.

[Personal website help](https://cloud.189.cn/web/static/template/help.html)
