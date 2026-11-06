// Package local adapts a prepared filesystem root to file operations.
//
// # Filesystem and transfer behavior
//
// Local binds native parents beneath a prepared os.Root. Reads, listings and
// mutations use exact names and never traverse native links. Windows checks
// the stored name on the opened handle. Linux admits ext and tmpfs directories
// with readable native flags and case folding disabled. Unsupported filesystems
// or platforms refuse strict child access before effects.
//
// The host owns the native root and its external mutation policy. Bound
// directories continue to refer to the same objects after a rename. The root
// is a path boundary, not a host sandbox or an inode-provenance guarantee.
//
// Lstat on Windows and Linux walks bound parent directories without following
// links. It classifies a recognized final symbolic link without reading its
// target. Unrecognized reparse points are refused. The lookup borrows temporary
// handles and observes the current name without freezing it for a later open.
//
// Data operations accept regular files and directories. Linux uses a nonblocking
// open flag to avoid a FIFO-open race. Arbitrary filesystem calls still have no hard
// interruption guarantee. Other platform mechanisms remain local to the adapter.
//
// Content length and ranges come from the opened result. Copying retains compatible
// native I/O paths and detects known-length short reads. There is no whole-file
// staging or fabricated strong validator. HTTP delivery behavior belongs to package httpapi.
// LiveEdit reuses rooted parent scopes and the original native fd. Windows/Linux
// implement random writes, native append, truncate and file sync; no staging or
// replacement is performed. OpenOrCreate attempts exclusive creation then one
// existing open on conflict, reporting actual creation separately from opening.
// External removal between those attempts may still fail. Links/reparse points
// and nonregular entries are refused, and strong validator predicates remain
// unsupported. Open/byte effects survive later cancellation or cleanup failure.
//
// # Native name changes
//
// Local creates one directory with requested mode 0700, removes one entry without
// recursion, and creates new files with O_CREATE|O_EXCL and requested mode 0600.
// The operating system's permission rules still apply. Default creation never
// truncates existing contents. Typed removal unlinks one non-directory entry
// or removes one empty directory. Removing a final link removes the link itself.
// Windows coordinates this instance's name commits by native parent identity.
// The guard spans the name check and native commit, without covering streaming.
// The host must exclude unsynchronized replacement of names through external
// writers or other Driver instances when it relies on exact replacement.
//
// # Complete file publication
//
// Local enables publication with local.New(directory, local.Options{
// StagingDirectory: staging, MaxStagingBytes: budget}). Both settings are required
// together; the byte budget must be positive. The host supplies an existing staging
// directory outside the served root, keeps it private from other mounts, and does
// not move or manipulate its owned objects while the Driver is active. No user name
// is reserved or filtered. Each target's actual parent must share the staging
// filesystem; a mismatch refuses publication without a cross-device copy fallback.
// Missing configuration leaves default creation available and refuses replacement.
//
// Optional local.Options.MaxConcurrentCommits limits concurrent native complete-file
// publication commits within one Local instance. Zero adds no limit; negative values
// are invalid. It is independent of batch concurrency and staging configuration.
// Transfers remain parallel, and waiting calls retain their staging reservations,
// captured parents and runtime uses. Waiting honors cancellation and retirement;
// the permit is released after the native call, before temporary-file cleanup.
// The limit does not cover default creation, Move or Remove, and supplies no FIFO
// ordering, cross-instance limit or transaction isolation. Choose it for the host's
// filesystem workload; no platform-specific limit is imposed.
//
// The budget belongs to one Local instance across all its calls. Known lengths
// reserve their full logical byte count before reading input; unknown lengths
// reserve each write before growing the temporary file. Exhaustion fails with
// CapacityExceeded rather than queueing. Cleanup of unpublished data must finish
// before its reservation is released; failed cleanup remains owned and charged.
// Published data leaves this temporary budget, while cleanup errors still prevent
// successful retirement. This is not a physical-block, total-storage, file-count or
// cross-instance quota. It excludes old opened objects, foreign files and crash
// leftovers; restart cleanup and recovery belong to the host.
//
// Local performs one streaming write to private storage, then a parent-relative
// native rename: renameat on Linux and FileRenameInformationEx with replacement and
// POSIX semantics on Windows. It does not copy the temporary body back into target.
// See [Windows rename semantics](https://learn.microsoft.com/en-us/windows-hardware/drivers/ddi/ntifs/ns-ntifs-_file_rename_information).
// Observed directories, links and spelling mismatches are refused. Final links
// introduced by a race are never followed; compatible name replacement may race.
// There is no content snapshot, object-ID/ACL preservation or external-mutation
// isolation. Already opened readers retain their old object.
//
// # Native relocation
//
// Local admits regular files and directories with existing target parents, refusing
// observed final symbolic links and special objects. Name and type checks are
// observations, not object/version/type conditions against external replacement.
// Windows opens the source with DELETE access relative to its rooted parent and
// uses the target directory handle; Linux amd64 uses renameat2 with
// RENAME_NOREPLACE and rooted parent descriptors. Other native platform/filesystem
// combinations return UnsupportedOperation when this guarantee is unavailable.
// Failed owned-handle cleanup remains with the Driver. Cross-volume fallback and
// power-loss durability are not promised.
package local
