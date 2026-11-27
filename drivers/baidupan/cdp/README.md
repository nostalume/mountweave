# Baidu website CDP transport

This optional Go module borrows an initialized tab in a dedicated browser
context. `Session.Account` reports the opaque website account binding.
Pass `Session.ReadPage` to `baidupan.OpenWebsite`. It opens a personal folder in that tab, checks the site's
directory request and completed response, and advances the native list by
scrolling after a full page. Directory reads share the tab with file capture.

`Session.CaptureOriginal` takes a verified `baidupan.WebsiteFile` and a callback
that starts an established ordinary-download action. It checks the grant and
redirect chain and retains the tab until the response body closes. The website
toolbar may choose a desktop action based on settings it loads after the click.
Use a verified ordinary browser action for the callback.

Pass `Session.ReadOrdinary` as the website's file reader with
`NewWithGuardedDownloads`. The command starts the exact file's download from
its verified directory and checks the signed grant and original response.
Open the site in a dedicated profile with verified native external-navigation
blocking and access to local services denied. An interactive verification
prompt or desktop-only choice fails explicitly.

The loopback Chromium test uses an isolated profile. Set `MWEAVE_TEST_BROWSER`
to the path of an installed Chromium browser to run it.
