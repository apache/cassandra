# Category: I/O and Crash Safety

Bugs in durable persistence, flush/sync ordering, atomic file replacement, partial reads/writes, checksum handling, and journal/log replay where crashes, errors, or restarts can corrupt data, lose writes, or resurrect deleted state.

## Diff signals (when to load this category)

Load this category if the patch contains ANY of:
- New or changed file writes, especially overwrite-in-place, rename/move, or `FileChannel.write` / `OutputStream.write` paths
- Calls to `fsync`, `force()`, `flush()`, `sync()`, `close()` on file/channel/writer objects, or removal/addition of any of these
- Use of `FileChannel.read`, `read(ByteBuffer)`, `InputStream.read(byte[])` without explicit "read fully" / "write fully" loops
- Checksum / CRC / digest computation tied to a file or record (`CRC32`, `Adler32`, `MessageDigest`, custom digest update over a buffer)
- Commit-log, journal, write-ahead-log, replay, recovery, or checkpoint code paths
- Truncation, snapshot, or restore operations on files or persistent state
- Auto-closing serializer / try-with-resources around a writer that owns the file lifecycle
- Atomic-rename helpers (`Files.move`, `ATOMIC_MOVE`, `renameTo`) or marker-file presence checks for operation completion
- Buffer pool / off-heap memory release coordinated with disk I/O completion
- Multiple related files written together (data + index, segments + manifest)
- File-open flags such as `TRUNCATE_EXISTING`, `CREATE_NEW`, `APPEND`, `O_DSYNC`, or their absence

## Findings

### F-01: State file overwritten in-place with auto-closing serializer
A safety-critical state file is overwritten in-place using a serializer that auto-closes the underlying output stream on its own close, making a post-write `fsync` and atomic rename impossible. A crash mid-write corrupts the file and converts a recoverable error into an unrecoverable startup failure.
**Look for:** `serializer.serialize(out)` where `out` is a fresh `FileOutputStream`/`Writer` opened directly on the destination path, with no temp-file + rename pattern and no explicit `getFD().sync()` before close.

### F-02: `read()` not retried on short read
A stream `read()` fills the buffer with fewer bytes than requested without retrying. The remainder is uninitialized, and downstream processing of the partial data produces silently corrupt output.
**Look for:** `in.read(buf)`, `channel.read(buf)`, `is.read(arr, off, len)` without a wrapping loop that advances `off` until `len` bytes are consumed; call sites that ignore the returned length.

### F-03: `write()` not retried on partial write
A `FileChannel.write()` (or equivalent) may write fewer bytes than requested; the call site assumes the entire buffer was written. Result is silent partial writes to storage files.
**Look for:** `channel.write(buf)` or `out.write(arr, off, len)` without a loop checking `buf.hasRemaining()`; absence of a `writeFully` helper at the boundary to durable storage.

### F-04: Buffered writer closed without explicit flush/sync
A buffered writer is closed without first flushing the buffer or syncing the file descriptor, so a crash after close can silently lose buffered data even though `close()` returned cleanly.
**Look for:** `bufferedWriter.close()` paths that lack a preceding `flush()` + `getFD().sync()` / `force(true)` for files where durability is required.

### F-05: Checksum computed over wrong byte range or pre-write buffer
A checksum is computed over a buffer's pre-write region (covering zeros) or over only a subset of fields actually written. Stored checksums never validate, or validate against partial coverage.
**Look for:** `ByteBuffer.duplicate()` called after `serialize()` instead of before; CRC accumulator updated for some serialized fields but not others; digest computed before `flip()`.

### F-06: Checksum recorded before final close mutates the file
A file checksum is computed and written before a `close()` that itself performs truncation or buffer flushing. The on-disk content changes after the checksum is recorded, invalidating it.
**Look for:** `writeChecksum(...)` immediately followed by `out.close()` / `channel.close()` that may flush bytes; checksum on an open writer that still has pending data.

### F-07: Checksum suffix not consumed on error-skip path
An error-handling skip path returns early without consuming the trailing checksum bytes that follow each record. The stream is left positioned at checksum bytes that get misread as the next record's header.
**Look for:** `continue` / `return` inside a record-deserialization loop with no preceding `readFully(crcSize)` or position adjustment; layouts where each record is followed by a fixed-size CRC suffix.

### F-08: Integrity check fails when write batch or flush limit is exceeded
A configured batch-size or flush-period limit on a log/store is exceeded; downstream readers see torn writes that fail CRC or checksum validation because the limit is enforced asymmetrically between writer and reader.
**Look for:** Configurable flush thresholds paired with a CRC/checksum validator that assumes whole-record durability; writer paths that accumulate beyond the configured limit before issuing `force()` / `flush()`.

### F-09: Reader buffer not reset after checksum-mismatch exception
A shared mutable reader buffer is not reset to an empty view after a checksum-mismatch exception. The next read attempt observes stale data from the previously failed chunk.
**Look for:** Catch block for `CorruptDataException` / `IOException` in a chunk reader that does not call `buffer.clear()` / `position(0).limit(0)` before propagating or retrying.

### F-10: Omitting flush for one of several related files
When transitioning a mutable structure to a persisted read-only form, the flush for one of several related files (e.g., index but not data, or manifest but not children) is omitted. After recovery one file looks current while the others are stale.
**Look for:** A method that writes/closes multiple files but only calls `force()` / `fsync` on a subset; sibling files (`.db` + `.idx` + `.summary`, segments + manifest) where one path lacks a sync.

### F-11: File opened without `TRUNCATE_EXISTING`
A file-open utility omits the truncate-existing flag, so when a shorter payload is written to a pre-existing file the tail of the old content remains and the file contains a mix of new and stale bytes.
**Look for:** `Files.newByteChannel(path, CREATE, WRITE)` or `new FileOutputStream(file)` without `TRUNCATE_EXISTING`; rewrite-in-place paths that assume the new payload is at least as long as the old.

### F-12: Recovery start position advanced past truncation without comparing target
A recovery start position is advanced by a truncation record without checking whether that record post-dates the restore target. Records within the recovery window are silently skipped on restart.
**Look for:** Replay loops that consult a "last truncation" marker and unconditionally skip earlier offsets; absence of comparison between truncation timestamp and the requested recovery target.

### F-13: Replay path does not consult truncation records
A replay path does not consult truncation records at all, so data written before a truncation is resurrected when the log is replayed after restart.
**Look for:** Commit-log / journal replay loops with no filter step for truncation entries; replay code that bypasses the same truncation guard used in the live write path.

### F-18: New writer allocated immediately after closing previous
A new writer is allocated immediately after closing the previous one without checking whether there is more data to write. An empty writer is created on the final iteration and left in a dangling state on disk.
**Look for:** Compaction / split / segment-rotation loops that call `writer.close(); writer = new Writer(nextPath())` unconditionally inside the loop instead of guarding the next allocation on `iterator.hasNext()`.

### F-19: Empty file created on every idle flush cycle
A flush loop unconditionally opens and writes an output file before checking whether the source iterator has any data. Empty files are created on every idle flush cycle and accumulate.
**Look for:** `try (Writer w = openOutput(...)) { while (it.hasNext()) ... }` patterns where the writer is opened before the `hasNext()` check; periodic flush schedulers that materialize the destination eagerly.

### F-20: Partially-written file left on disk after exception
A file writer opened in an error-prone path is never aborted on exception; a partially-written output file remains on disk and is treated as a complete valid file by later readers.
**Look for:** `Writer w = open(...)` without try/finally that calls `w.abort()` / deletes the partial file on exception; commit semantics relying only on close success while the failure path leaves the destination intact.

### F-23: Directory not fsynced after delete/rename
A deletion or rename step does not sync the directory entry. A prior change may not yet be visible after a crash, causing the next pass to re-discover stale entries or fail to find committed names.
**Look for:** `Files.delete(...)` / `Files.move(..., ATOMIC_MOVE)` followed immediately by directory iteration without `directoryChannel.force(true)` or platform-equivalent.

### F-24: Stream `read()` returning 0 conflated with EOF (or vice versa)
A `read()` return value conflates zero bytes (try again) with end-of-stream (-1, closed). Genuine EOF is silently treated as a retry condition (loop never exits), or a transient zero-byte read closes a still-live stream.
**Look for:** `if (n == 0) return EOF;` on a non-blocking channel; `while (n != -1)` loops that treat a transient zero-byte read as "more to come"; missing `if (n < 0) closed=true;` guards.

### F-28: Dirty flag set after write loop, skipped on early exception
A dirty flag is set unconditionally after a write loop exits; if the loop terminates early due to an exception, the flag is never set and the buffer is not flushed, silently losing data.
**Look for:** `for (...) { write(...) } dirty = true;` with no try/finally; flush schedulers that gate work on `if (dirty)` while the write path can throw before reaching the flag assignment.

### F-29: Pooled buffer returned before in-flight write completes
A pooled buffer is returned to its pool before the in-flight network/disk write using it completes, allowing it to be overwritten and corrupting the original payload.
**Look for:** `pool.release(buf)` in the same scope as `channel.write(buf)` without awaiting completion; reference-counted buffers handed to async APIs without an extra `acquire()`.

### F-30: Use-after-free of off-heap buffer racing eviction
A cache get reads off-heap native memory without first incrementing the reference count; a concurrent eviction frees the memory between the map lookup and the read, causing a use-after-free crash or returning torn bytes.
**Look for:** `Buffer b = cache.get(key); read(b);` where the cache may evict and free `b`'s backing memory; missing `tryAcquire()` / `incrementRef()` before the read.

### F-31: Removing fsync as an "optimization" breaks crash safety
Removing `fsync` from checkpoint, truncation, or commit paths causes file corruption on ungraceful shutdown, leading to unrecoverable data loss on restart. The dropped barrier was the precondition for the higher-level atomicity claim (e.g., atomic-rename only guarantees ordering after the source is durable).
**Look for:** Diffs that delete `force(true)` / `fsync` / `getFD().sync()` calls in checkpoint, truncation, or commit paths; comments claiming the call was "redundant"; removal of a flush immediately preceding `Files.move(..., ATOMIC_MOVE)`.

### F-32: Snapshot scheduled at non-aligned offset
Scheduling a checkpoint/snapshot at the current position without verifying batch-boundary alignment produces an unreplayable snapshot. Recovery loads a snapshot pointing into the middle of a batch and cannot resume.
**Look for:** `snapshotAt(currentOffset)` without a preceding alignment check; snapshot writers that record an offset chosen by a clock or external signal rather than from a known-quiesced batch boundary.

### F-33: IOException swallowed on file close skips recovery
A swallowed `IOException` during file closure prevents error propagation; the runtime skips recovery on next startup and may leave index files in an inconsistent state with respect to the data files they describe. Filesystem errors that should trigger the disk-failure policy are silently logged instead.
**Look for:** `try { file.close(); } catch (IOException ignored) {}`; I/O catch blocks that only call `logger.warn(...)` without invoking the project's failure-policy dispatcher.

### F-34: Truncation resets file size but not companion offset
A truncation operation resets the file's physical size but does not update a companion offset-tracking field, leaving the field pointing past the new end of file and causing subsequent reads to access non-existent data.
**Look for:** `channel.truncate(newSize)` paired with no update to a sibling `lastValidPosition` / `endOffset` / `tailIndex` field; classes that maintain redundant size state.

### F-35: Channel fill assumes single read returns full buffer
A channel fill reads once and treats the result as the full buffer; short reads silently truncate data for all subsequent accesses from that buffer. Common in helpers misleadingly named `fill()` or `loadFully()`.
**Look for:** `channel.read(buf); buf.flip(); deserialize(buf);` with no loop ensuring `buf.remaining() == 0` after the fill; helpers that issue exactly one underlying read.
