// Package local adapts a prepared filesystem root to file operations.
//
// # Filesystem and transfer behavior
//
// Local uses os.Root and exact directory-entry names, with incremental directory
// reads and root-anchored metadata. Name checks and subsequent opens are separate
// observations; concurrent external renames are not an atomic snapshot. Lookup may
// require directory-read permission. The root is a path boundary, not a host sandbox
// or an inode-provenance guarantee.
//
// Only regular files and directories are accepted. Linux uses a nonblocking open
// flag to avoid a FIFO-open race; arbitrary filesystem calls still have no hard
// interruption guarantee. Other platform mechanisms remain local to the adapter.
//
// Content length and ranges come from the opened result. Copying retains compatible
// native I/O paths and detects known-length short reads. There is no whole-file
// staging or fabricated strong validator. HTTP delivery behavior belongs to package httpapi.
package local
