// Package ftp exposes an explicit Core-backed protocol profile.
// The FTP profile uses New(Core, Options) and ServeConn(ctx, net.Conn). New only
// validates and snapshots configuration. The host owns Core, listening sockets,
// callbacks and policy. ServeConn owns the supplied control connection, pending
// passive listener and actual data connections until acknowledged cleanup.
// Root/Home have the same lexical tree-view meaning as SFTP; Home defaults to /.
// Authentication receives the real completed TLS state when available. The host
// decides whether to require TLS; configured certificates do not prove transport
// protection. TLSConfig is cloned; its shared contents must remain immutable.
//
// CursorTTL, DataConnectTimeout, TransferBufferBytes and PageNameBytes must be
// positive; CommandBytes includes CRLF and must be at least 2. PageEntries=0 uses
// core.DefaultPage, capped at core.MaxPage. PageNameBytes bounds retained string
// bytes per directory page, not the whole listing. Oversized control lines end
// the session rather than leave bytes interpreted as subsequent commands.
//
// At least one of ListenPassive and DialActive is required; nil disables that
// mode. The host supplies network admission (including accepted passive peers)
// and an advertised numeric passive address/port. A callback owns resources
// until return and must respond to cancellation; the session takes every non-nil
// returned resource even alongside an error. DataConnectTimeout covers setup,
// Accept and TLS handshake, not total large-file duration. TLSConfig enables
// AUTH TLS and protected data (PBSZ 0, PROT P); PROT C remains explicit.
//
// Implemented commands include USER/PASS, QUIT/NOOP/FEAT/SYST, TYPE I/A N,
// MODE S, STRU F, PWD/CWD/CDUP, SIZE (binary files), MDTM, LIST/MLST/MLSD/NLST,
// PASV/EPSV, PORT/EPRT, RETR/STOR/APPE, REST, ABOR, MKD, RNFR/RNTO and DELE/RMD.
// TYPE A converts LF/literal CR to NVT CRLF/CR-NUL; malformed input fails.
// REST requires binary representation. LIST uses EPLF for observed type and
// optional size. DELE unlinks one non-directory entry, and RMD removes an
// empty directory where the Driver can enforce the predicate. The export root
// is protected from deletion and rename. Link traversal, attribute changes,
// EPSV ALL, REIN and unlisted extensions are outside this profile.
// SYST reports UNKNOWN rather than claiming a Unix filesystem.
//
// CompletePublish (zero StorePolicy) makes ordinary STOR call Core.Write with
// Publish; input failure does not replace the old target. Local needs explicit
// staging configuration. LiveEdit selects visible OpenOrCreate+Truncate instead.
// Nonzero REST+STOR and APPE explicitly select File.WriteAt and native Append;
// they do not fall back to full upload. Failure can leave visible partial bytes.
// REST is not a durable upload record and cannot resume failed private staging.
// RETR preserves ordinary Body/WriterTo, adding Range only for a nonzero restart.
// RNFR stores name intent; RNTO invokes the same no-replacement Core.Move.
//
// Each connection has at most one transfer goroutine; control stays responsive
// to ABOR/QUIT. A transfer captures identity, address and representation. Abort,
// EOF and cancellation close actual data/content I/O and join Core/native work.
// A completed wire reply follows cleanup; unknown mutation or cleanup failure
// ends the session. Known partial effects remain. Neither a task queue nor a
// second staging implementation participates. Separate connections are independent.
// OnFailure receives the stage, original cause and known mutation receipt of
// failed file operations and transfers. It also receives confirmed effects
// when a reply fails. Complete publication and partial visible edits retain
// distinct effects on abort. Reply text is a bounded public projection.
// The host owns this synchronous callback, which can run concurrently on
// different sessions.

package ftp
