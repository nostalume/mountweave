// Package sftp exposes an explicit Core-backed protocol profile.
// The SFTP profile uses New(Core, Options) and ServeConn(ctx, principal, channel).
// Options.Root is a Core tree anchor; Home is export-relative, defaults to /,
// and is the base for relative client names. This is a lexical view, not a new
// physical chroot. Native link traversal remains subject to Driver contracts.
// The host supplies SSH transport, an authenticated principal and session context.
// ServeConn closes only the supplied channel, not SSH, listeners or Core.
//
// CursorTTL, Limits.Handles and PageNameBytes must be positive. PageEntries=0
// means core.DefaultPage; its maximum is core.MaxPage. PacketBytes=0 means 34000
// including the length header; ReadBytes=0 means 32768. Reads must fit a DATA
// response (ReadBytes+13 <= PacketBytes). Oversized frames terminate the channel
// before body allocation. Oversized READ requests fail, rather than return EOF.
// PageNameBytes covers retained name/address/evidence strings on one page; a
// directory response can split that page across frames without losing entries.
//
// OPEN uses BoundRead or LiveEdit with NoFollow on the same Core. READ, WRITE,
// FSTAT, size-only FSETSTAT and fsync@openssh.com act on that resource after
// path replacement. The fsync extension is advertised as revision 1 when it
// fits the packet budget. It requires a writable handle and renewed write
// authorization. Sync failure preserves the error and leaves prior write
// effects intact. Append uses native File.Append. OPENDIR/READDIR retain Core cursors and one page
// per handle; expiry/invalidation is an error. Directory FSTAT is unsupported.
// EOF does not close a file; CLOSE releases it. MKDIR accepts empty attributes;
// RENAME uses the existing no-replacement Move. REALPATH resolves lexical dot
// segments in this view, without claiming physical symlink canonicalization.
//
// LSTAT uses Core.Lstat and the same authorization as STAT. It refuses
// intermediate native links and reports the final link itself, even when
// dangling, without its target or permissions. The export root must observe as
// a directory. Drivers unable to provide the observation return an unsupported
// status. OPEN independently refuses links even if a prior LSTAT succeeded.
// REMOVE unlinks one non-directory entry, and RMDIR removes an empty directory
// where the Driver can enforce the predicate. The export root is protected
// from deletion and rename.
//
// ModeView defaults to OmitMode. ReadOnlyView presents files as 0400 and
// directories as 0500. ReadWriteView presents 0600 and 0700. Type bits come
// from Core Entry.Kind. These are export ceilings, and clients may copy them
// onto downloaded files. The host derives the view and Core Authorizer from
// the same export policy. A read-only view requires the Authorizer to deny
// mutations for the export's principals. Actual authorization and native
// capabilities can narrow the ceiling on any operation.
//
// With a mode view, MKDIR and OPEN with CREAT can carry the nine ordinary
// permission bits alone. The requested mask must contain the view's default.
// This is a logical ceiling check. Creation uses native defaults. A conflict
// returns create_mode_conflict before Core admission, even for CREAT+TRUNC
// on an existing name. Link resolution and creation, other extensions
// and attribute changes other than size-only FSETSTAT remain unsupported.
//
// OnFailure receives request ID, stage, original cause and known mutation
// receipts, including when a reply could not be delivered. The wire status
// carries bounded public text. The callback runs synchronously and is host
// owned. Distinct connections can call it concurrently.
//
// Requests execute in receive order. A bounded reader holds at most one next
// frame to observe channel termination during a blocked operation; the host must
// cancel ctx on SSH disconnect as well, including when unread pipelined input
// prevents observation of EOF. Cancellation closes transport and the current
// persistent File, then joins actual calls and cleans every handle/cursor.
// Unknown mutation effects or cleanup failure end the session without retry.
// Native failed cleanup stays with its Core owner; Handles is not a global quota.
//
// This optional module uses the public pkg/sftp/v2 codec at v2.0.0-alpha2,
// requiring Go 1.25+. It does not use that library's high-level RequestServer.
// The stable v1 client is a test-only independent wire consumer. Root Core and
// FTP/WebDAV/JSON HTTP retain their standard-library-only module.

package sftp
