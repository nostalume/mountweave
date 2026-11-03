// Durable sequential input is an optional consumer, not a Driver or a second
// file-operation API. Core does not import this package. It uses only the standard
// library and never opens a target mount or submits work on recovery.
//
// # Assembly and ownership
//
// Open(ctx, directory, Options) opens an existing private directory. The host
// supplies positive MaxBytes, MaxInputs, MaxBlocks and MaxAppendBytes, exclusively owns that
// directory and keeps it outside served namespaces. There is no cross-process
// lock, configuration loader or environment-derived capacity. The host controls
// retention with List and Release. Owner strings are opaque identities, not an
// authorization system; consumers must authorize input and file access separately.
//
// Create allocates an input ID. Append(ctx, id, offset, reader, length) accepts one
// positive, exact-length block at the confirmed offset, including normal EOF.
// Input is borrowed synchronously, never closed or replayed. An exact retry of a
// committed block verifies offset, length and SHA-256 without storing it twice;
// this also permits a late retry after sealing. Rechunked overlaps, mismatching
// bytes and gaps conflict. Same-input mutations refuse with ErrBusy before reading
// input; different inputs can proceed concurrently under the shared byte budget.
//
// Data and checksum receipts are synchronized before Offset is acknowledged.
// Failed/incomplete input truncates only the unconfirmed tail. Sync or rollback
// uncertainty blocks that input and preserves storage charges; do not resume from
// its reported lower-bound offset without reconciling the stored journal. Close
// the owner, then reopen/recover only after it has actually stopped using storage.
// A valid unacknowledged final record may be recovered: query Status and verify
// retries rather than blindly replaying. List finds unacknowledged creations too.
//
// # Sealed sources and release
//
// Seal(ctx, id, expectedOffset) fixes an immutable source version. Repeating it at
// the same offset returns the same version. Version identifies the sealed journal
// and its data digests; it is not a whole-file content hash. Acquire pins this
// sealed version without opening a file. Reference.Open uses the later execution
// context and returns a sequential body which skips journal framing and verifies
// each payload block. No complete-file copy is made at sealing or opening.
//
// Close every Reference and returned body. Each body owns an independent pin;
// closing a Reference does not invalidate an already opened body. Release commits
// retirement, refuses new references and preserves existing accepted references,
// which may still open/read the source. Bytes remain charged until all pins close
// and private deletion succeeds. Status then returns ErrNotFound. Failed cleanup
// remains owned and is reported, including by Store.Close.
//
// Hold/Unhold attach durable opaque owner keys to a sealed source. Repeated calls
// are idempotent; Release denies new keys but preserves existing keys across
// restart. AcquireHeld admits the named existing owner even after Release. The
// durable consumer persists preparation before Hold and terminal intent before
// Unhold, and reconciles failed handoffs by the same key. Hold reserves its later
// Unhold record within MaxBytes. Deletion requires zero runtime pins and holds;
// Store.Close waits for runtime users only, leaving durable holds in the journal.
//
// Opened bodies also expose SourceVersion and OpenRange(ctx, offset, length).
// Independent ranges locate indexed blocks without scanning earlier blocks. Both
// boundary blocks are fully verified before normal EOF, so a range may read their
// discarded prefixes/suffixes. Zero-length ranges are empty even inside a block.
// Range readers must be closed and retain their own runtime pins.
//
// Store.Close stops admission, requests active reads to stop and waits for actual
// operations, references and bodies before closing its root. Its context limits
// waiting, not arbitrary borrowed Reader/native I/O. Persisted receiving/sealed
// inputs survive shutdown. Host shutdown order must let consumers finish/release
// their references before expecting Store.Close to complete.
//
// # Storage, cost and recovery
//
// One append journal owns identity, data, digests, sealing and retirement. In-memory
// indexes are rebuilt from it. Independent header checksums distinguish corrupt
// complete headers from incomplete creation; complete corrupt metadata is reported,
// while unacknowledged incomplete tails are truncated. Recovery checks metadata
// chains and the final receiving data block, then synchronizes the recovered
// checkpoint before admission; payloads are verified again on use.
// It does not scan all sealed payload on every startup. Cost scales with block
// count and, for an unsealed input, its final block. There is no automatic repair
// of damaged acknowledged data or silently falling back to an older version.
//
// MaxBytes counts retained logical journal bytes and pre-reserved seal/release
// records, allowing finalization even at capacity. It is not filesystem block
// usage or a free-disk-space guarantee. MaxBlocks separately bounds retained and
// reserved block indexes, including recovery; retries do not reserve again.
// Index memory scales with block count;
// streaming buffers are at most 32 KiB per active append. File.Sync precedes
// acknowledgements. Native filesystem/storage guarantees determine power-loss and
// directory-entry durability; process-termination tests do not certify those.
//
// Runtime references do not persist task intent across process death. A released
// journal is reclaimed on recovery after those process-local references are lost.
// Receiving and sealed inputs can resume, while queued closures/file publication
// are never automatically replayed. Native remote-upload checkpoint recovery is
// a separate consumer of stable input and is not implemented here.
package input
