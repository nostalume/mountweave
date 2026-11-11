# Tencent Weiyun source

`weiyun.Open(ctx, weiyun.Options{Token: token})` verifies the directory authorized
by a [personal Weiyun MCP token](https://www.weiyun.com/act/openclaw). Obtain the
token in the host and keep it out of serialized mount configuration. Close a
non-nil Driver returned with an error to retry any retained response cleanup.
After success, install the Driver at a Core path and use `Stat`, `List`, `Open`,
and `Copy` from Weiyun. An updated token requires a new Driver instance.

The fixed control endpoint receives `WyHeader`. The optional `Client` and
`ContentClient` are borrowed and must use transports that do not inject account
credentials. `MaxResponseBytes` bounds control replies and defaults to 4 MiB.
Download bodies stream outside that budget. The download grant's `FTN5K` cookie
is used only on HTTPS Weiyun receivers on port 443 and is dropped permanently
after leaving that domain. Signed URLs and cookies are kept out of errors.
The actual HTTP response determines the delivered range and length. When a
response has no HTTP length, a read through EOF verifies its byte count
against the grant and rejects short or excess content.

The Driver uses the vendor's live directory listing. One native page has up to
50 entries. A full unfinished page advances by 50 and `finish_flag` ends the
listing. A short unfinished page fails because the published contract leaves
the next offset ambiguous. Each path component scans its parent's pages for a
unique exact name. A read adds one download grant request and the HTTP body
request chain. Tencent Docs filtered by Weiyun, writes, and bound file handles
are outside this read contract. Cross-domain downloads that still require the
Weiyun cookie are outside the supported receiver policy.

[Weiyun MCP v1.0.6 reference package](https://skillhub-1388575217.cos.accelerate.myqcloud.com/skills/tencent-drive-mcp/1.0.6.zip)
