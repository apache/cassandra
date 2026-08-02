/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.io.sstable;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.zip.CRC32;

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import com.clearspring.analytics.stream.cardinality.ICardinality;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.RateLimiter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.compaction.CompactionInfo;
import org.apache.cassandra.db.compaction.CompactionInterruptedException;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.io.sstable.format.FilterComponent;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.StatsComponent;
import org.apache.cassandra.io.sstable.format.TOCComponent;
import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.apache.cassandra.io.sstable.format.big.BigFormat.Components;
import org.apache.cassandra.io.sstable.format.big.RowIndexEntry;
import org.apache.cassandra.io.sstable.indexsummary.IndexSummary;
import org.apache.cassandra.io.sstable.indexsummary.IndexSummaryBuilder;
import org.apache.cassandra.io.sstable.metadata.CompactionMetadata;
import org.apache.cassandra.io.sstable.metadata.MetadataComponent;
import org.apache.cassandra.io.sstable.metadata.MetadataType;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileOutputStreamPlus;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.io.util.Reflink;
import org.apache.cassandra.io.util.SequentialWriter;
import org.apache.cassandra.io.util.SequentialWriterOption;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.EstimatedHistogram;
import org.apache.cassandra.utils.FilterFactory;
import org.apache.cassandra.utils.IFilter;
import org.apache.cassandra.utils.MurmurHash;
import org.apache.cassandra.utils.SyncUtil;
import org.apache.cassandra.utils.TimeUUID;

/**
 * Splits one BIG-format SSTable into K children by copying verbatim compression-chunk runs of Data.db and
 * rebuilding every other component from an Index.db-only pass. No decompression, no row deserialization.
 *
 * <h2>Why a chunk run, and not an exact byte cut</h2>
 * Uncompressed chunk boundaries are pinned to exact multiples of {@code chunkLength}:
 * {@link CompressionMetadata#chunkFor(long)} indexes the offsets array with
 * {@code 8 * (position / chunkLength)} and there is no per-chunk uncompressed length on disk. Therefore only
 * the <em>last</em> chunk of a file may be uncompressed-short, and a child can only ever be a verbatim run of
 * whole chunks {@code [i, j]}.
 *
 * <h2>Consequences of the general (suffix) form</h2>
 * A child whose first partition does not sit on a chunk boundary carries a <em>dead prefix</em> of
 * {@code lo mod chunkLength} bytes at the head of its Data.db. Index positions are rebased by
 * {@code shift = i * chunkLength} rather than by {@code lo}, so the child's first partition lands at
 * uncompressed offset {@code lo mod chunkLength}. That is tolerated by the read, compaction, cleanup and
 * repair-validation paths, all of which enter Data.db only at positions read from Index.db. It is
 * <em>not</em> tolerated by:
 * <ul>
 *   <li>entire-sstable zero-copy streaming, which requires
 *       {@code transferLength == sstable.uncompressedLength()}; such a child falls back to partial streaming;</li>
 *   <li>{@code Scrubber}/{@code Verifier}, which walk Data.db linearly from 0. Both have been given a
 *       three-line change to seek to the first index position instead of requiring it to be zero.</li>
 * </ul>
 *
 * <h2>The bytes are shared, not copied, where the filesystem can do it</h2>
 * A child's Data.db is a verbatim byte range of the parent's, which is exactly the shape the
 * {@code FICLONERANGE} ioctl exists for: on xfs formatted with {@code -m reflink=1} (or btrfs) the range is
 * made to point at the parent's physical extents and their reference count is bumped, so the split writes no
 * data blocks at all and consumes no additional disk space. When the parent is unlinked at commit its extents
 * are not freed, they simply belong to the children -- which turns "a split needs room for a second copy of the
 * sstable" into "a split needs room for the index and the metadata". See {@link Reflink}.
 * <p>
 * The one thing sharing costs is a <em>head pad</em>. The ioctl requires block-aligned offsets and lengths, and
 * a compression chunk boundary is aligned to nothing, so the copied range is extended backwards to the previous
 * 64 KiB boundary and the child's chunk offsets are rebased by that boundary instead of by {@code O(i)}. The
 * child's Data.db therefore begins with up to 64 KiB of the parent's previous chunk, and its
 * {@code offsets[0]} is that pad rather than 0. Those bytes belong to no chunk and are never read -- every
 * reader enters Data.db at an offset taken from the offsets array -- but they are a second, physical dead
 * prefix, independent of the uncompressed one above, and they are covered by Digest.crc32 because
 * {@code Verifier} checksums the whole file. {@link CopyPlan} is the arithmetic; the only consumer that had to
 * change for it is {@code MmappedRegions}, which used to seed its segment placement at physical 0 and so left
 * the tail of a front-padded file unmapped.
 * <p>
 * All of it is conditional and self-demoting: the pad is only planned for when {@code Reflink.isPossibleIn}
 * has not already learned that this directory's filesystem cannot share extents, a refusal costs one failing
 * ioctl and falls through to the ordinary transfer loop, and a padded range that ends up being copied produces
 * a child byte-for-byte identical to the one a clone would have produced. {@code Result.totalBytesCloned}
 * reports what actually happened. Set {@code zero_copy_split_reflink_enabled: false} to never try.
 * <p>
 * Two consequences worth knowing. Shared extents are cheap on disk but not in page cache, which is per inode,
 * so bytes read through both parent and child are cached twice -- transient here, since the parent is unlinked
 * as the children are published. And {@code du} counts shared blocks once per file while {@code df} counts them
 * once in total, so per-directory usage over-reports until the parent goes away.
 * <p>
 * Once the copy is gone, {@link #writeDigest} is the only thing left that touches the data at all, and it
 * therefore becomes the entire cost of a split. It can be turned off with
 * {@code zero_copy_split_digest_enabled: false}, which takes a split down to its Index.db pass; nothing requires
 * the component, but {@code Verifier} answers its absence by upgrading to a full extended verification, so
 * {@code nodetool verify} and {@code nodetool import --verify-sstables} get slower for those children. The
 * component audit behind that claim is on {@link org.apache.cassandra.config.Config#zero_copy_split_digest_enabled}.
 *
 * <h2>Trailing slack is forbidden</h2>
 * {@code CompressionMetadata.compressedFileLength} is taken from the physical file length, and the last
 * chunk's length is derived as {@code compressedFileLength - offsets[C-1] - 4}. A single trailing byte
 * inflates that length and can flip the reader's {@code length < maxCompressedLength} test, causing
 * compressed bytes to be handed back as raw data. The child's Data.db is therefore truncated to exactly
 * {@code headPad + O(j+1) - O(i)} and asserted -- the pad is at the head, so it does not interfere with this:
 * it shifts {@code offsets[C-1]} and {@code compressedFileLength} by the same amount.
 *
 * <h2>Uncompressed SSTables</h2>
 * Not supported; {@link #split(SSTableReader, int, LifecycleTransaction)} throws
 * {@link UnsupportedOperationException} whose message starts with {@link #UNCOMPRESSED_UNSUPPORTED_MESSAGE}.
 * An uncompressed split is a different algorithm, not a degenerate case of this one: the cut is exact (no
 * chunk grid, no dead prefix, {@code shift == lo}) and CRC.db must be regenerated wholesale because its
 * 64 KiB grid is addressed from origin 0 and a suffix cut is misaligned against it. Producing a child with a
 * stale or sliced CRC.db would corrupt outbound partial streaming silently, so this refuses instead.
 * Use {@link #isSupported(SSTableReader)} to test up front.
 *
 * <h2>Accepted imprecision in the children's Statistics.db</h2>
 * Four of the {@code StatsMetadata} fields are absolute per-sstable <em>totals</em> rather than min/max bounds,
 * and cannot be recomputed without deserialising rows -- which is the entire cost this class exists to avoid.
 * Each of the K children therefore inherits the PARENT-WIDE value of
 * {@code estimatedCellPerPartitionCount}, {@code totalRows}, {@code totalColumnsSet} and
 * {@code estimatedTombstoneDropTime}, while {@code estimatedPartitionSize} (and hence
 * {@code SSTableReader.estimatedKeys()}) is re-derived exactly per child. That mix is deliberate and its
 * consequences are ACCEPTED, not overlooked:
 * <ul>
 *   <li>Per-table aggregates that sum these across sstables -- {@code getMeanRowCount},
 *       {@code estimatedColumnCountHistogram}, the table-level droppable-tombstone ratio -- over-report by
 *       roughly K for as long as the children survive.</li>
 *   <li>{@code AbstractCompactionStrategy.worthDroppingTombstones} divides the child's exact key count by the
 *       parent-wide cell count, so its {@code remainingColumnsRatio} collapses to about 1/K and the effective
 *       {@code tombstone_threshold} for a child is about K times the configured one. Combined with this path not
 *       purging tombstones at all, a child both retains more droppable tombstones than a rewrite would have left
 *       and is less likely to be picked for the single-sstable tombstone compaction that would drop them. Set
 *       {@code unchecked_tombstone_compaction} or lower {@code tombstone_threshold} on tables where that
 *       matters.</li>
 *   <li>Inherited {@code maxLocalDeletionTime}/{@code maxTimestamp} similarly keep a fully-expired child from
 *       being dropped whole by {@code getFullyExpiredSSTables}, and put every child in the parent's TWCS
 *       window.</li>
 * </ul>
 * None of this can lose or resurrect data: every inherited value is at least as wide or as large as the truth,
 * so the errors are all in the conservative direction. They are a metrics and compaction-scheduling cost, paid
 * until the children are compacted normally, in exchange for not reading a single row.
 *
 * <h2>Durability: every component is fsynced before the child is published</h2>
 * The transaction's COMMIT record is itself fsynced, and committing is what unlinks the parent. So any child
 * component that is merely in page cache at that moment can be lost by a power failure while the parent's
 * removal survives -- and the key range it held is then gone from this replica, with the child failing to open.
 * Three of the eight components used to be in exactly that state, because the convenience helpers they went
 * through do not fsync: {@code MetadataSerializer.rewriteSSTableMetadata} (Statistics.db) only flushes and
 * renames, and {@code SSTableReader.saveBloomFilter} / {@code saveSummary} (Filter.db, Summary.db) only flush --
 * and both of the latter swallow the IOException and delete the half-written file. Statistics.db was the fatal
 * one: it is the only copy of the child's {@code SerializationHeader} and repair state, and unlike the filter and
 * the summary it cannot be rebuilt from anything.
 * <p>
 * They are now all written the way {@code BigTableWriter} writes them -- Statistics.db through a
 * {@code SequentialWriter} plus {@code finish()}, Filter.db and Summary.db through an explicitly synced
 * {@code FileOutputStreamPlus} -- and {@link SyncUtil#trySyncDir} makes the directory entries durable too, since
 * a file whose data is on disk but whose name is not in a synced directory is lost just the same. The full
 * inventory, all of it before {@code SSTableReader.open}:
 * <table>
 *   <tr><td>Data.db</td><td>{@code FileChannel.force(true)} in {@link #copyData} -- a clone is a metadata
 *       change and needs it just as much as a write does</td></tr>
 *   <tr><td>CompressionInfo.db</td><td>{@code CompressionMetadata.Writer.doPrepare}</td></tr>
 *   <tr><td>Index.db</td><td>{@code SequentialWriter.finish() -> syncInternal()}</td></tr>
 *   <tr><td>Statistics.db</td><td>{@code SequentialWriter.finish() -> syncInternal()}</td></tr>
 *   <tr><td>Filter.db</td><td>{@code FileOutputStreamPlus.sync()}</td></tr>
 *   <tr><td>Summary.db</td><td>{@code FileOutputStreamPlus.sync()}</td></tr>
 *   <tr><td>Digest.crc32</td><td>{@code FileOutputStreamPlus.sync()}, when written at all -- see
 *       {@code zero_copy_split_digest_enabled}</td></tr>
 *   <tr><td>TOC.txt</td><td>{@code SSTable.appendTOC}</td></tr>
 *   <tr><td>the directory</td><td>{@code SyncUtil.trySyncDir}, once per child</td></tr>
 * </table>
 *
 * <h2>This is a compaction, and behaves like one</h2>
 * When the caller supplies a {@link Progress} the copy is registered with the compaction framework: visible in
 * {@code nodetool compactionstats}, bounded by {@code compaction_throughput}, and stoppable by
 * {@code nodetool stop ANTICOMPACTION}, TRUNCATE, DROP and {@code runWithCompactionsDisabled}. Without one --
 * offline tools and tests -- it runs unthrottled, as those callers expect.
 */
public final class ZeroCopySSTableSplitter
{
    private static final Logger logger = LoggerFactory.getLogger(ZeroCopySSTableSplitter.class);

    /**
     * Prefix of the {@link UnsupportedOperationException} message raised for an uncompressed parent. Exposed so
     * tests can assert the refusal without string-matching the whole sentence.
     */
    public static final String UNCOMPRESSED_UNSUPPORTED_MESSAGE =
        "ZeroCopySSTableSplitter requires a compressed sstable";

    /**
     * One {@code transferTo} slice. FileChannel.transferTo caps near 0x7ffff000 and may return short counts, so
     * this only has to stay well under that -- but it is deliberately small, because it is also the granularity
     * at which {@link Progress} throttles against {@code compaction_throughput} and notices a stop request. A
     * multi-GiB slice would make the copy effectively unthrottled and uninterruptible.
     */
    private static final int TRANSFER_SLICE = 4 << 20;

    /** Same buffer size the digest/checksum writers use. */
    private static final int COPY_BUFFER_SIZE = 64 * 1024;

    /**
     * Alignment the head pad is computed against; see {@link Reflink#RANGE_ALIGNMENT} for why it is a constant
     * 64 KiB and not the filesystem's actual block size.
     */
    private static final long CLONE_ALIGNMENT = Reflink.RANGE_ALIGNMENT;

    /**
     * A child smaller than this is copied rather than shared. Sharing forces up to {@link #CLONE_ALIGNMENT}
     * bytes of head pad, which cost disk space and a longer digest pass, so it only pays for itself when the
     * range dwarfs the pad. 1 MiB is 16 times the pad: a 6% overhead ceiling at the very bottom of the range,
     * and immaterial for anything a split is actually run on.
     */
    private static final long MIN_CLONE_BYTES = 1L << 20;

    /**
     * Test hook. Lay every child out as if extent sharing were available -- head pad and all -- so that the
     * aligned layout is covered on filesystems that cannot share extents, which is every developer laptop and
     * CI box. Also lifts {@link #MIN_CLONE_BYTES}, since test sstables are far smaller than that. The copy
     * mechanism is unaffected: if the filesystem cannot clone, the padded range is transferred conventionally
     * and the child is byte-for-byte what a clone would have produced.
     */
    @VisibleForTesting
    static volatile boolean forceAlignedLayoutForTesting = false;

    /**
     * {@code MetadataCollector.defaultPartitionSizeHistogram()} is package-private; this is bit-identical.
     * A child's {@code estimatedPartitionSize} has to bucket the same way every writer-produced sstable's does,
     * or the two cannot be summed -- so this tracks that method and nothing else. {@code ZeroCopySplitStatsTest}
     * pins the two together against silent drift.
     */
    static final int PARTITION_SIZE_HISTOGRAM_BUCKETS = 155;

    /** {@code MetadataCollector.cardinality} is {@code new HyperLogLogPlus(13, 25)} (CASSANDRA-5906). */
    static final int HLL_P = 13;
    static final int HLL_SP = 25;

    /** Every component this class can write, i.e. everything {@link #cleanUp} has to remove. */
    private static final List<Component> WRITTEN_COMPONENTS = ImmutableList.of(Components.DATA,
                                                                               Components.PRIMARY_INDEX,
                                                                               Components.COMPRESSION_INFO,
                                                                               Components.STATS,
                                                                               Components.SUMMARY,
                                                                               Components.FILTER,
                                                                               Components.DIGEST,
                                                                               Components.TOC);

    private ZeroCopySSTableSplitter()
    {
    }

    // ------------------------------------------------------------------------------------------------
    // Arithmetic. Deliberately static and free of any sstable dependency so it can be unit tested alone.
    // ------------------------------------------------------------------------------------------------

    /**
     * Index of the compression chunk containing {@code uncompressedPosition}.
     * Mirrors {@code CompressionMetadata.chunkFor}, which does {@code 8 * (position / chunkLength)}.
     */
    public static long chunkIndexFor(long uncompressedPosition, int chunkLength)
    {
        checkChunkLength(chunkLength);
        if (uncompressedPosition < 0)
            throw new IllegalArgumentException("negative uncompressed position: " + uncompressedPosition);
        return uncompressedPosition / chunkLength;
    }

    /** First (inclusive) chunk of a child whose first live byte is at parent uncompressed offset {@code lo}. */
    public static long firstChunk(long lo, int chunkLength)
    {
        return chunkIndexFor(lo, chunkLength);
    }

    /**
     * Last (inclusive) chunk of a child whose live bytes end at exclusive parent uncompressed offset
     * {@code hi}. Note this is {@code (hi - 1) / L}, not {@code hi / L}: when {@code hi} lands exactly on a
     * chunk boundary the final chunk is the one <em>before</em> it, and using {@code hi / L} would read one
     * chunk too far (and throw {@code CorruptSSTableException(EOFException)} at the end of the file).
     */
    public static long lastChunk(long hi, int chunkLength)
    {
        checkChunkLength(chunkLength);
        if (hi <= 0)
            throw new IllegalArgumentException("child must contain at least one byte, hi=" + hi);
        return (hi - 1) / chunkLength;
    }

    /**
     * The child's {@code CompressionInfo.dataLength}: from the start of its first chunk up to the end of its
     * last live partition. There is no trailing slack -- {@code getPositionsForRanges} uses
     * {@code uncompressedLength()} as its right bound.
     */
    public static long childDataLength(long hi, long firstChunk, int chunkLength)
    {
        checkChunkLength(chunkLength);
        long dataLength = hi - firstChunk * chunkLength;
        if (dataLength <= 0)
            throw new IllegalArgumentException("non-positive child dataLength " + dataLength +
                                               " (hi=" + hi + ", firstChunk=" + firstChunk + ", L=" + chunkLength + ')');
        return dataLength;
    }

    /** Bytes at the head of the child Data.db that belong to no partition: {@code lo mod chunkLength}. */
    public static long deadPrefixBytes(long lo, int chunkLength)
    {
        checkChunkLength(chunkLength);
        if (lo < 0)
            throw new IllegalArgumentException("negative uncompressed position: " + lo);
        return lo % chunkLength;
    }

    /**
     * The whole chunk-range computation for one child, as an immutable value so a test can assert on it
     * directly.
     *
     * @param lo          first live byte, inclusive, in PARENT uncompressed space (a partition start)
     * @param hi          last live byte + 1, exclusive, in PARENT uncompressed space (a partition end)
     * @param chunkLength the parent's compression chunk length
     */
    public static ChunkRange chunkRange(long lo, long hi, int chunkLength)
    {
        checkChunkLength(chunkLength);
        if (lo < 0)
            throw new IllegalArgumentException("negative lo: " + lo);
        if (hi <= lo)
            throw new IllegalArgumentException("empty child range [" + lo + ", " + hi + ')');

        long i = firstChunk(lo, chunkLength);
        long j = lastChunk(hi, chunkLength);
        if (i > j)
            throw new IllegalStateException("firstChunk " + i + " > lastChunk " + j +
                                            " for [" + lo + ", " + hi + ") L=" + chunkLength);

        long chunkCount = j - i + 1;
        long dataLength = childDataLength(hi, i, chunkLength);

        // The reason a verbatim run works at all: the last chunk holds at least one live byte (so it is
        // mapped and decompressed) and at most a full chunk of them (so dataLength never overruns the run).
        if (!((chunkCount - 1) * (long) chunkLength < dataLength && dataLength <= chunkCount * (long) chunkLength))
            throw new IllegalStateException(String.format("invariant (C-1)*L < Dp <= C*L violated: " +
                                                          "C=%d L=%d Dp=%d lo=%d hi=%d",
                                                          chunkCount, chunkLength, dataLength, lo, hi));

        return new ChunkRange(lo, hi, chunkLength, i, j, chunkCount, dataLength,
                              i * (long) chunkLength, deadPrefixBytes(lo, chunkLength));
    }

    private static void checkChunkLength(int chunkLength)
    {
        if (chunkLength <= 0)
            throw new IllegalArgumentException("chunkLength must be positive: " + chunkLength);
    }

    /**
     * Where the child's Data.db comes from and how it gets there: the second, physical half of the arithmetic,
     * and the only part that knows about extent sharing.
     * <p>
     * {@code FICLONERANGE} needs its source offset, its destination offset and its length all aligned (see
     * {@link Reflink}), and a chunk boundary in the parent is aligned to nothing -- offsets advance by
     * {@code compressedLength + 4}, so they are effectively uniform modulo any block size. The destination
     * offset and the length we control; the source offset we do not. So the copy is extended BACKWARDS to the
     * previous alignment boundary and the child's chunk offsets are rebased by that boundary rather than by
     * {@code O(i)}, which puts {@code pad = O(i) mod A} bytes of the parent's previous chunk at the head of the
     * child's Data.db and makes the child's {@code offsets[0]} equal to {@code pad} instead of 0.
     * <p>
     * Those pad bytes belong to no chunk of the child and are never read: the reader only ever enters Data.db
     * at an offset taken from its own offsets array. They are a second, physical dead prefix, distinct from and
     * independent of {@link ChunkRange#deadPrefixBytes}, which lives in uncompressed space. Everything else
     * about the child is unchanged -- the last chunk's length is still derived as
     * {@code compressedFileLength - offsets[C-1] - 4} and the pad shifts both terms equally.
     * <p>
     * The tail is what alignment cannot buy: {@code cloneLength} is the aligned part of the child's length, and
     * the remaining {@code tailLength < A} bytes are copied conventionally. Rounding the clone UP instead and
     * truncating would work on xfs, but it would depend on truncate unsharing a partially shared final block,
     * and a sub-64-KiB copy is not worth that.
     *
     * @param copyFrom      {@code O(i)}, the parent offset of the child's first chunk
     * @param physicalBytes {@code O(j+1) - O(i)}, the child's live chunk bytes
     * @param align         whether to pad the head so that sharing is possible at all
     * @param share         whether to actually attempt the clone; {@code align} without {@code share} is what a
     *                      test uses to produce the padded layout on a filesystem that cannot share
     */
    public static CopyPlan copyPlan(long copyFrom, long physicalBytes, boolean align, boolean share)
    {
        if (copyFrom < 0)
            throw new IllegalArgumentException("negative copyFrom: " + copyFrom);
        if (physicalBytes <= 0)
            throw new IllegalArgumentException("non-positive physicalBytes: " + physicalBytes);

        long pad = align ? copyFrom & (CLONE_ALIGNMENT - 1) : 0;
        long childLength = pad + physicalBytes;
        // Aligned down, so the clone can never reach past the child's last live byte and into the parent's
        // trailing slack -- which chunkEnd() exists to keep out of the child.
        long cloneLength = share ? childLength - (childLength & (CLONE_ALIGNMENT - 1)) : 0;
        return new CopyPlan(copyFrom - pad, pad, childLength, cloneLength);
    }

    /** Immutable result of {@link #copyPlan(long, long, boolean, boolean)}. */
    public static final class CopyPlan
    {
        /** Parent offset the child's byte 0 is taken from: {@code O(i) - headPadBytes}, alignment aligned. */
        public final long srcStart;
        /** Bytes of the parent's previous chunk at the head of the child, and the child's {@code offsets[0]}. */
        public final long headPadBytes;
        /** Exact length of the child's Data.db: {@code headPadBytes + physicalBytes}. */
        public final long childLength;
        /** Leading part of the child that {@code FICLONERANGE} is asked for; 0 means "copy all of it". */
        public final long cloneLength;

        CopyPlan(long srcStart, long headPadBytes, long childLength, long cloneLength)
        {
            this.srcStart = srcStart;
            this.headPadBytes = headPadBytes;
            this.childLength = childLength;
            this.cloneLength = cloneLength;
        }

        /** Bytes that must be transferred even if the clone succeeds: {@code childLength mod A}. */
        public long tailLength()
        {
            return childLength - cloneLength;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o)
                return true;
            if (!(o instanceof CopyPlan))
                return false;
            CopyPlan that = (CopyPlan) o;
            return srcStart == that.srcStart && headPadBytes == that.headPadBytes
                   && childLength == that.childLength && cloneLength == that.cloneLength;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(srcStart, headPadBytes, childLength, cloneLength);
        }

        @Override
        public String toString()
        {
            return String.format("CopyPlan[src=%d pad=%d length=%d clone=%d tail=%d]",
                                 srcStart, headPadBytes, childLength, cloneLength, tailLength());
        }
    }

    /**
     * Immutable result of {@link #chunkRange(long, long, int)}. All chunk indices are into the PARENT's
     * offsets array; all byte counts are in the child's own space.
     */
    public static final class ChunkRange
    {
        /** First live byte of the child, inclusive, in parent uncompressed space. */
        public final long lo;
        /** Last live byte of the child + 1, exclusive, in parent uncompressed space. */
        public final long hi;
        /** The parent's compression chunk length. */
        public final int chunkLength;
        /** {@code i}: first parent chunk copied, inclusive. */
        public final long firstChunk;
        /** {@code j}: last parent chunk copied, inclusive. */
        public final long lastChunk;
        /** {@code C = j - i + 1}: the child's chunkCount. */
        public final long chunkCount;
        /** {@code Dp = hi - i*L}: the child's CompressionInfo dataLength. */
        public final long dataLength;
        /** {@code shift = i*L}: subtracted from every Index.db position. */
        public final long shift;
        /** {@code lo mod L}: bytes at the head of the child Data.db owned by no partition. */
        public final long deadPrefixBytes;

        ChunkRange(long lo, long hi, int chunkLength, long firstChunk, long lastChunk,
                   long chunkCount, long dataLength, long shift, long deadPrefixBytes)
        {
            this.lo = lo;
            this.hi = hi;
            this.chunkLength = chunkLength;
            this.firstChunk = firstChunk;
            this.lastChunk = lastChunk;
            this.chunkCount = chunkCount;
            this.dataLength = dataLength;
            this.shift = shift;
            this.deadPrefixBytes = deadPrefixBytes;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o)
                return true;
            if (!(o instanceof ChunkRange))
                return false;
            ChunkRange that = (ChunkRange) o;
            return lo == that.lo && hi == that.hi && chunkLength == that.chunkLength
                   && firstChunk == that.firstChunk && lastChunk == that.lastChunk
                   && chunkCount == that.chunkCount && dataLength == that.dataLength
                   && shift == that.shift && deadPrefixBytes == that.deadPrefixBytes;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(lo, hi, chunkLength, firstChunk, lastChunk, chunkCount, dataLength, shift, deadPrefixBytes);
        }

        @Override
        public String toString()
        {
            return String.format("ChunkRange[lo=%d hi=%d L=%d chunks=[%d,%d] C=%d Dp=%d shift=%d dead=%d]",
                                 lo, hi, chunkLength, firstChunk, lastChunk, chunkCount, dataLength, shift, deadPrefixBytes);
        }
    }

    // ------------------------------------------------------------------------------------------------
    // Results
    // ------------------------------------------------------------------------------------------------

    /**
     * Repair state to stamp into one child's Statistics.db, instead of inheriting the parent's. The triple is
     * written by {@link #writeStatistics} <em>before</em> the child reader is opened, so the reader is born with
     * the right state and no {@code mutateRepairedAndReload} is ever needed.
     * <p>
     * The two invariants enforced here are the ones {@code CompactionStrategyHolder.managesRepairedGroup} and
     * {@code PendingRepairHolder.managesRepairedGroup} assert when the Tracker routes a newly visible sstable to
     * a compaction strategy holder; violating them turns into an {@code IllegalArgumentException} thrown from
     * inside a Tracker notification, which is a far worse place to find out.
     */
    public static final class RepairState
    {
        /** {@code ActiveRepairService.UNREPAIRED_SSTABLE} (0) unless the data is already repaired. */
        public final long repairedAt;
        /** The incremental repair session id, or {@code ActiveRepairService.NO_PENDING_REPAIR} (null). */
        public final TimeUUID pendingRepair;
        /** Only ever true when {@code pendingRepair != null}. */
        public final boolean isTransient;

        public RepairState(long repairedAt, TimeUUID pendingRepair, boolean isTransient)
        {
            this(repairedAt, pendingRepair, isTransient, true);
        }

        private RepairState(long repairedAt, TimeUUID pendingRepair, boolean isTransient, boolean validate)
        {
            if (validate)
            {
                Preconditions.checkArgument(pendingRepair == ActiveRepairService.NO_PENDING_REPAIR
                                            || repairedAt == ActiveRepairService.UNREPAIRED_SSTABLE,
                                            "SSTables cannot be both repaired and pending repair");
                Preconditions.checkArgument(!isTransient || pendingRepair != ActiveRepairService.NO_PENDING_REPAIR,
                                            "isTransient can only be true for sstables pending repairs");
            }
            this.repairedAt = repairedAt;
            this.pendingRepair = pendingRepair;
            this.isTransient = isTransient;
        }

        /**
         * The state every child gets from the overloads that do not take an explicit one: the parent's, copied
         * verbatim and deliberately unvalidated, so those overloads behave exactly as they did before per-child
         * repair state existed.
         */
        public static RepairState inherit(StatsMetadata parentStats)
        {
            return new RepairState(parentStats.repairedAt, parentStats.pendingRepair, parentStats.isTransient, false);
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o)
                return true;
            if (!(o instanceof RepairState))
                return false;
            RepairState that = (RepairState) o;
            return repairedAt == that.repairedAt
                   && isTransient == that.isTransient
                   && Objects.equals(pendingRepair, that.pendingRepair);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(repairedAt, pendingRepair, isTransient);
        }

        @Override
        public String toString()
        {
            return String.format("RepairState[repairedAt=%d pendingRepair=%s transient=%s]",
                                 repairedAt, pendingRepair, isTransient);
        }
    }

    /** One produced child sstable. */
    public static final class Child
    {
        /** Descriptor of the child, in the parent's directory, version and format. */
        public final Descriptor descriptor;
        /** The child's first partition key (minimal copy). */
        public final DecoratedKey first;
        /** The child's last partition key (minimal copy). */
        public final DecoratedKey last;
        /** First parent chunk copied, inclusive. */
        public final long firstChunk;
        /** Last parent chunk copied, inclusive. */
        public final long lastChunk;
        /** Exact physical byte length of the child Data.db, {@code O(j+1) - O(i)}. */
        public final long physicalBytes;
        /** The child's CompressionInfo dataLength, {@code hi - i*L}. */
        public final long dataLength;
        /** Value subtracted from every Index.db position, {@code i*L}. */
        public final long shift;
        /** Bytes at the head of the child Data.db owned by no partition, {@code lo mod L}. */
        public final long deadPrefixBytes;
        /**
         * Bytes of the parent's PREVIOUS chunk physically present at the head of this child's Data.db so that
         * its first chunk lands on an alignment boundary, i.e. the child's {@code offsets[0]}. Zero unless the
         * child's extents were (or were meant to be) shared with the parent. See {@link CopyPlan}.
         */
        public final long headPadBytes;
        /** Bytes of {@link #physicalBytes} that were shared with the parent instead of copied. */
        public final long clonedBytes;
        /** Number of partitions in the child. */
        public final long partitionCount;
        /** Components written for the child; the exact set passed to {@code SSTableReader.open}. */
        public final Set<Component> components;
        /**
         * The repair state actually stamped into this child's Statistics.db. This is the state of the boundary
         * range the child came from, carried through rather than positionally re-derived, so an empty boundary
         * range that produced no child cannot shift the pairing.
         */
        public final RepairState repairState;
        /** The opened, validated child reader. The caller owns this reference and must release it. */
        public final SSTableReader reader;

        Child(Descriptor descriptor, DecoratedKey first, DecoratedKey last, ChunkRange range,
              long physicalBytes, long headPadBytes, long clonedBytes, long partitionCount,
              Set<Component> components, RepairState repairState, SSTableReader reader)
        {
            this.descriptor = descriptor;
            this.first = first;
            this.last = last;
            this.firstChunk = range.firstChunk;
            this.lastChunk = range.lastChunk;
            this.physicalBytes = physicalBytes;
            this.dataLength = range.dataLength;
            this.shift = range.shift;
            this.deadPrefixBytes = range.deadPrefixBytes;
            this.headPadBytes = headPadBytes;
            this.clonedBytes = clonedBytes;
            this.partitionCount = partitionCount;
            this.components = components;
            this.repairState = repairState;
            this.reader = reader;
        }

        /** Exact length of the child's Data.db, and its {@code compressedFileLength}: pad included. */
        public long onDiskLength()
        {
            return headPadBytes + physicalBytes;
        }

        @Override
        public String toString()
        {
            return String.format("Child[%s chunks=[%d,%d] physical=%d pad=%d cloned=%d dataLength=%d shift=%d" +
                                 " dead=%d partitions=%d %s]",
                                 descriptor, firstChunk, lastChunk, physicalBytes, headPadBytes, clonedBytes,
                                 dataLength, shift, deadPrefixBytes, partitionCount, repairState);
        }
    }

    /** Outcome of a whole split. */
    public static final class Result
    {
        /** The children, in token order. */
        public final List<Child> children;
        /**
         * Sum of every child's live chunk bytes, {@code O(j+1) - O(i)}. This is the size of the data the split
         * had to account for, NOT the number of bytes it moved: subtract {@link #totalBytesCloned} for that.
         */
        public final long totalPhysicalBytesCopied;
        /** Sum of every child's dead prefix. */
        public final long totalDeadPrefixBytes;
        /** Sum of every child's head pad, i.e. the disk space alignment cost. */
        public final long totalHeadPadBytes;
        /**
         * Bytes that were shared with the parent as copy-on-write extents rather than copied. Zero on a
         * filesystem that cannot share extents; otherwise within one alignment unit per child of
         * {@code totalPhysicalBytesCopied + totalHeadPadBytes}, and those bytes cost neither I/O nor disk space.
         */
        public final long totalBytesCloned;
        /**
         * Compressed bytes physically present in two children because a split boundary fell inside a chunk.
         * Bounded by one chunk per interior boundary -- and free, not merely bounded, when the children's
         * extents are shared: both point at the same physical chunk.
         */
        public final long duplicatedChunkBytes;
        /** Wall clock of the whole split. */
        public final long nanos;

        Result(List<Child> children, long totalPhysicalBytesCopied, long totalDeadPrefixBytes,
               long totalHeadPadBytes, long totalBytesCloned, long duplicatedChunkBytes, long nanos)
        {
            this.children = children;
            this.totalPhysicalBytesCopied = totalPhysicalBytesCopied;
            this.totalDeadPrefixBytes = totalDeadPrefixBytes;
            this.totalHeadPadBytes = totalHeadPadBytes;
            this.totalBytesCloned = totalBytesCloned;
            this.duplicatedChunkBytes = duplicatedChunkBytes;
            this.nanos = nanos;
        }

        /** Bytes actually written to produce every child's Data.db. */
        public long totalBytesWritten()
        {
            return totalPhysicalBytesCopied + totalHeadPadBytes - totalBytesCloned;
        }

        @Override
        public String toString()
        {
            return String.format("Result[children=%d physical=%d dead=%d pad=%d cloned=%d written=%d" +
                                 " duplicated=%d %.1fms]",
                                 children.size(), totalPhysicalBytesCopied, totalDeadPrefixBytes,
                                 totalHeadPadBytes, totalBytesCloned, totalBytesWritten(),
                                 duplicatedChunkBytes, nanos / 1_000_000.0);
        }
    }

    // ------------------------------------------------------------------------------------------------
    // Compaction-framework participation
    // ------------------------------------------------------------------------------------------------

    /**
     * Makes one split a first-class member of the compaction framework for its whole duration, so a verbatim
     * byte copy behaves like every other compaction-family operation instead of being an invisible, unbounded
     * burst of I/O:
     * <ul>
     *   <li>it appears in {@code nodetool compactionstats}, because the caller registers this holder with
     *       {@code CompactionManager.active};</li>
     *   <li>it is bounded by {@code compaction_throughput}, because every slice is acquired from the compaction
     *       {@link RateLimiter} before it moves;</li>
     *   <li>it stops when asked -- {@code nodetool stop ANTICOMPACTION}, {@code nodetool stop --id <id>},
     *       TRUNCATE, DROP, and anything else routed through {@code runWithCompactionsDisabled}, all of which
     *       work by walking {@code active.getCompactions()} and calling {@link CompactionInfo.Holder#stop()}.
     *       The {@link CompactionInfo} carries the parent sstable so {@code CompactionInfo.shouldStop} can match
     *       it.</li>
     * </ul>
     * A verbatim chunk copy has no partition boundary to stop cleanly at, so the stop check lives inside the
     * transfer loop and aborts the split outright: {@link CompactionInterruptedException} propagates out of
     * {@link #split}, the transaction is aborted and every child is deleted. A caller must NOT treat that as a
     * reason to fall back to the rewrite path -- the operator asked for the work to stop, not to be done a
     * different and more expensive way.
     * <p>
     * {@code total} is an estimate, and deliberately so: the copy pass accounts for the parent's physical bytes
     * once, and {@link #writeDigest} -- when it runs at all -- reads every child back for a second pass. A
     * boundary chunk lands in two children, and an aligned child carries a head pad, so a split with many
     * interior boundaries can report marginally over 100%. The digest pass is counted only when
     * {@code zero_copy_split_digest_enabled} says it will happen; otherwise a split would peg at 50% and finish.
     * <p>
     * Bytes that were SHARED rather than copied still count towards {@code total} -- otherwise a reflink split
     * would stall at 50% -- but they are not charged to the rate limiter, because throttling work that generates
     * no disk traffic would make sharing exactly as slow as copying. See {@link #cloned}.
     */
    public static final class Progress extends CompactionInfo.Holder
    {
        private final TableMetadata metadata;
        private final Set<SSTableReader> parent;
        private final long total;
        private final TimeUUID id;
        private final AtomicLong completed = new AtomicLong();
        private final RateLimiter limiter;

        private Progress(SSTableReader parent, RateLimiter limiter)
        {
            this.metadata = parent.metadata();
            this.parent = ImmutableSet.of(parent);
            int passes = DatabaseDescriptor.getZeroCopySplitDigestEnabled() ? 2 : 1;
            this.total = passes * parent.onDiskLength();
            this.id = TimeUUID.Generator.nextTimeUUID();
            this.limiter = limiter;
        }

        @Override
        public CompactionInfo getCompactionInfo()
        {
            // total and totalCompressed are the same number here: every byte this operation counts is already a
            // physical on-disk byte of the parent (a compressed chunk run, plus the digest pass over it), so
            // there is no uncompressed figure to scale against and the ratio is 1.0.
            return new CompactionInfo(metadata, OperationType.ANTICOMPACTION, completed.get(), total, total, id, parent);
        }

        /** One sstable of one table, so a paused global compaction must not silently stop it. */
        @Override
        public boolean isGlobal()
        {
            return false;
        }

        /**
         * Called immediately BEFORE {@code bytes} move: throws if a stop has been requested, then blocks until
         * the compaction rate limiter lets the slice through. Permits are acquired for the whole slice even
         * though {@code transferTo} may move fewer, which over-throttles by at most one slice per short count
         * -- the conservative direction.
         */
        void beforeSlice(int bytes)
        {
            checkStopped();
            if (bytes > 0)
                limiter.acquire(bytes);
        }

        void afterSlice(long bytes)
        {
            completed.addAndGet(bytes);
        }

        /** The stop half of {@link #beforeSlice}, for work that moves no bytes and so must not be throttled. */
        void checkStopped()
        {
            if (isStopRequested())
                throw new CompactionInterruptedException(getCompactionInfo());
        }

        /**
         * Bytes accounted for by sharing extents rather than by moving them. Deliberately NOT pushed through
         * the rate limiter: {@code compaction_throughput} exists to bound disk traffic, and a clone generates
         * none, so charging it would make a reflink split take exactly as long as the copy it replaced. They
         * still count towards {@code total} so that {@code nodetool compactionstats} reaches 100%.
         */
        void cloned(long bytes)
        {
            completed.addAndGet(bytes);
        }
    }

    /**
     * A {@link Progress} for splitting {@code parent}. The caller owns it: register it with
     * {@code CompactionManager.active.beginCompaction} before {@link #split} and finish it afterwards.
     */
    public static Progress progressFor(SSTableReader parent, RateLimiter limiter)
    {
        Preconditions.checkNotNull(parent, "parent");
        Preconditions.checkNotNull(limiter, "limiter");
        return new Progress(parent, limiter);
    }

    // ------------------------------------------------------------------------------------------------
    // Entry points
    // ------------------------------------------------------------------------------------------------

    /**
     * @return true iff {@link #split} can handle this parent, i.e. it is a compressed BIG-format sstable that
     *         does not use a compression dictionary. Anything else is refused with
     *         {@link UnsupportedOperationException}.
     */
    public static boolean isSupported(SSTableReader parent)
    {
        // getCompressionMetadata() throws when the sstable is not compressed, so the order of these matters.
        return BigFormat.is(parent.descriptor.getFormat())
               && parent.compression
               && parent.getCompressionMetadata().compressionDictionary() == null;
    }

    /**
     * Split at the partition boundaries nearest to {@code numChildren} approximately-equal byte shares of the
     * parent's uncompressed length.
     *
     * @param numChildren number of children to produce; must be >= 1 and <= the parent's partition count
     * @param txn         optional; if non-null every child is {@code trackNew}'d on it once fully written
     * @throws UnsupportedOperationException if the parent is not a compressed BIG-format sstable
     */
    public static Result split(SSTableReader parent, int numChildren, LifecycleTransaction txn)
    {
        return split(parent, numChildren, txn, null);
    }

    /**
     * As {@link #split(SSTableReader, int, LifecycleTransaction)}, but throttled by and interruptible through
     * {@code progress}.
     *
     * @param progress optional; when non-null the copy is rate limited and a stop request raises
     *                 {@link CompactionInterruptedException}. See {@link Progress}.
     */
    public static Result split(SSTableReader parent, int numChildren, LifecycleTransaction txn, Progress progress)
    {
        Preconditions.checkArgument(numChildren >= 1, "numChildren must be >= 1, got %s", numChildren);
        requireSupported(parent);

        long start = Clock.Global.nanoTime();
        // Three sequential passes over Index.db, none of which retains anything per partition: count, select,
        // build. The count has to come first because the split-point selection needs the exact partition count
        // up front for its tail-room clamp. Index.db is a couple of percent of Data.db, so the extra pass is
        // cheap next to copying the chunk runs -- and it is what keeps this O(numChildren) in heap instead of
        // O(partitions). See the note on RunSelector.
        int partitionCount = countPartitions(parent);
        if (numChildren > partitionCount)
            throw new IllegalArgumentException("cannot split " + partitionCount + " partitions into " +
                                               numChildren + " children");
        Runs runs = selectByByteShare(parent, numChildren, partitionCount);
        return build(parent, runs, null, txn, progress, start);
    }

    /**
     * Split at explicit boundaries. Child {@code b} covers keys {@code [boundaries[b-1], boundaries[b])}, with
     * the first child unbounded below and the last unbounded above -- so this produces up to
     * {@code boundaries.size() + 1} children. Boundaries must be strictly increasing.
     * <p>
     * A boundary range containing no partition produces no child (an empty sstable is not representable:
     * {@code IndexSummaryBuilder.build} asserts a non-zero key count and {@code getPositionsForRanges} asserts
     * {@code first < last}). So the returned list may be shorter than {@code boundaries.size() + 1}.
     *
     * @param txn optional; if non-null every child is {@code trackNew}'d on it once fully written
     * @throws UnsupportedOperationException if the parent is not a compressed BIG-format sstable
     */
    public static Result split(SSTableReader parent, List<DecoratedKey> boundaries, LifecycleTransaction txn)
    {
        return split(parent, boundaries, null, txn);
    }

    /**
     * Split at explicit boundaries, stamping a caller-supplied repair state into each child instead of
     * inheriting the parent's. Boundary semantics are exactly those of
     * {@link #split(SSTableReader, List, LifecycleTransaction)}: child {@code b} covers keys
     * {@code [boundaries[b-1], boundaries[b])}, and {@code perChild.get(b)} is the state for that key range.
     * <p>
     * <b>Pairing.</b> A boundary range containing no partition still produces no child, so
     * {@code result.children.size()} may be smaller than {@code perChild.size()}. The state is therefore
     * <em>carried</em> with the range rather than re-derived from a child's index afterwards, and the state
     * actually written is exposed on {@link Child#repairState}. Positional pairing of {@code children} against
     * {@code perChild} is only valid when every range is known to be non-empty; use {@link Child#repairState}
     * and do not assume it otherwise.
     *
     * @param perChild one state per boundary range, so exactly {@code boundaries.size() + 1} entries, in the
     *                 same order as the ranges; may be null to inherit the parent's state for every child
     * @param txn      optional; if non-null every child is {@code trackNew}'d on it once fully written
     * @throws IllegalArgumentException      if {@code perChild.size() != boundaries.size() + 1}, if any entry is
     *                                       null, or if the boundaries are not strictly increasing
     * @throws UnsupportedOperationException if the parent is not a compressed BIG-format sstable
     */
    public static Result split(SSTableReader parent,
                               List<DecoratedKey> boundaries,
                               List<RepairState> perChild,
                               LifecycleTransaction txn)
    {
        return split(parent, boundaries, perChild, txn, null);
    }

    /**
     * As {@link #split(SSTableReader, List, List, LifecycleTransaction)}, but throttled by and interruptible
     * through {@code progress}. This is the overload the anticompaction path uses.
     *
     * @param progress optional; when non-null the copy is rate limited against {@code compaction_throughput} and
     *                 a stop request raises {@link CompactionInterruptedException}. See {@link Progress}.
     */
    public static Result split(SSTableReader parent,
                               List<DecoratedKey> boundaries,
                               List<RepairState> perChild,
                               LifecycleTransaction txn,
                               Progress progress)
    {
        Preconditions.checkNotNull(boundaries, "boundaries");
        requireSupported(parent);
        for (int b = 1; b < boundaries.size(); b++)
        {
            if (boundaries.get(b - 1).compareTo(boundaries.get(b)) >= 0)
                throw new IllegalArgumentException("boundaries must be strictly increasing: " +
                                                   boundaries.get(b - 1) + " >= " + boundaries.get(b));
        }
        if (perChild != null)
        {
            if (perChild.size() != boundaries.size() + 1)
                throw new IllegalArgumentException("perChild must have one entry per boundary range, i.e. " +
                                                   (boundaries.size() + 1) + " entries for " + boundaries.size() +
                                                   " interior boundaries, got " + perChild.size());
            for (int b = 0; b < perChild.size(); b++)
            {
                if (perChild.get(b) == null)
                    throw new IllegalArgumentException("perChild[" + b + "] is null");
            }
        }

        long start = Clock.Global.nanoTime();
        // Two passes, as before: the run starts fall out of the same walk that resolves the boundaries, so this
        // form needs no counting pass.
        Runs runs = selectByBoundaries(parent, boundaries);
        return build(parent, runs, perChild, txn, progress, start);
    }

    private static void requireSupported(SSTableReader parent)
    {
        Preconditions.checkNotNull(parent, "parent");
        if (!BigFormat.is(parent.descriptor.getFormat()))
            throw new UnsupportedOperationException("ZeroCopySSTableSplitter only supports the BIG sstable " +
                                                    "format, got " + parent.descriptor.getFormat().name() +
                                                    ". The technique is to copy Data.db chunks verbatim and " +
                                                    "rewrite one position field per Index.db record; BTI has " +
                                                    "no Index.db and encodes positions inside trie payloads, " +
                                                    "so there is no single rebaseable field.");
        if (!parent.compression)
            throw new UnsupportedOperationException(UNCOMPRESSED_UNSUPPORTED_MESSAGE + ": " + parent.descriptor +
                                                    " has no CompressionInfo.db. An uncompressed split is a " +
                                                    "different algorithm -- the cut is exact rather than " +
                                                    "chunk-aligned, and CRC.db (whose 64KiB grid is addressed " +
                                                    "from origin 0) has to be regenerated wholesale rather " +
                                                    "than sliced. Refusing rather than emitting a child with " +
                                                    "a misaligned CRC.db.");
        if (!parent.descriptor.fileFor(Components.STATS).exists())
            throw new IllegalStateException("parent has no Statistics.db: " + parent.descriptor +
                                            "; MetadataSerializer would silently fabricate defaults");
    }

    // ------------------------------------------------------------------------------------------------
    // Walking the parent Index.db
    // ------------------------------------------------------------------------------------------------

    /** Receives every Index.db record in on-disk order. */
    private interface IndexRecordConsumer
    {
        void accept(int index, ByteBuffer key, long position);
    }

    /**
     * One sequential walk of the parent Index.db, retaining nothing.
     *
     * <p>This deliberately does not hand back the positions. An earlier version collected every partition's
     * uncompressed Data.db offset into a {@code long[]}, which is 8 bytes per partition steady state and 16-24
     * at the peak of the doubling and the final trim. That is invisible on a 512 MiB parent and a hard ceiling
     * on a real one: a terabyte of 1 KiB partitions is a billion records, i.e. tens of gigabytes of heap for an
     * array whose every access turned out to be sequential. Everything downstream now takes what it needs from
     * a stream -- {@link RunSelector} keeps O(numChildren), and {@link #buildChild} keeps one record of
     * lookback.
     *
     * @return the exact number of records
     */
    private static int walkIndex(SSTableReader parent, IndexRecordConsumer consumer)
    {
        long count = 0;
        // A buffered reader rather than an mmap, so no record can straddle a mapping boundary.
        try (RandomAccessReader in = RandomAccessReader.open(parent.descriptor.fileFor(Components.PRIMARY_INDEX)))
        {
            long indexSize = in.length();
            while (in.getFilePointer() != indexSize)
            {
                ByteBuffer key = ByteBufferUtil.readWithShortLength(in);
                long position = RowIndexEntry.Serializer.readPosition(in);
                int promotedSize = in.readUnsignedVInt32();
                if (promotedSize > 0)
                    in.skipBytesFully(promotedSize);

                if (count >= Integer.MAX_VALUE)
                    throw new IllegalStateException("parent has more than Integer.MAX_VALUE partitions, which " +
                                                    "run starts cannot address: " + parent.descriptor);
                consumer.accept((int) count++, key, position);
            }
        }
        catch (IOException e)
        {
            throw new CorruptSSTableException(e, parent.descriptor.fileFor(Components.PRIMARY_INDEX));
        }

        if (count == 0)
            throw new IllegalStateException("parent Index.db is empty: " + parent.descriptor);

        return (int) count;
    }

    /** Just the record count, for the byte-share form, whose selection needs it before it can start. */
    private static int countPartitions(SSTableReader parent)
    {
        return walkIndex(parent, (index, key, position) -> {});
    }

    // ------------------------------------------------------------------------------------------------
    // Split-point selection: the START index of each run; run b is
    // [runStarts[b], runStarts[b+1]) with an implicit terminator of partitionCount.
    // ------------------------------------------------------------------------------------------------

    /**
     * Where each child's run of index records begins, and the parent Data.db offset of that first record.
     * O(numChildren), which is the whole point of the shape: {@link #build} needs a run's {@code lo} before it
     * can copy that child's chunks, so these offsets cannot be recovered during the build pass, but there are
     * only ever {@code numChildren} of them.
     */
    @VisibleForTesting
    static final class Runs
    {
        final int[] runStarts;
        /**
         * {@code runPositions[b]} is the Data.db offset of record {@code runStarts[b]}. Meaningless for an
         * empty trailing run, whose {@code runStarts[b] == partitionCount}; {@link #build} skips those before
         * reading it.
         */
        final long[] runPositions;
        final int partitionCount;

        Runs(int[] runStarts, long[] runPositions, int partitionCount)
        {
            this.runStarts = runStarts;
            this.runPositions = runPositions;
            this.partitionCount = partitionCount;
        }
    }

    /** No record's offset can be this, so it doubles as "not filled in yet". */
    private static final long UNRESOLVED = -1;

    /**
     * The explicit-boundary form. The run starts fall out of the same walk that compares keys against the
     * boundaries, so this costs one pass and no extra reads -- and the keys still never have to be retained (a
     * wide sstable would otherwise cost ~150 bytes of heap per partition).
     */
    private static Runs selectByBoundaries(SSTableReader parent, List<DecoratedKey> boundaries)
    {
        IPartitioner partitioner = parent.getPartitioner();
        int[] runStarts = new int[boundaries.size() + 1];
        long[] runPositions = new long[boundaries.size() + 1];
        Arrays.fill(runPositions, UNRESOLVED);
        int[] nextBoundary = { 0 };

        int count = walkIndex(parent, (index, key, position) -> {
            if (index == 0)
                runPositions[0] = position;  // runStarts[0] is 0

            if (nextBoundary[0] < boundaries.size())
            {
                DecoratedKey dk = partitioner.decorateKey(key);
                // run b + 1 starts at the first record whose key is >= boundaries[b]; several boundaries can
                // land on the same record, and each of those runs then shares its offset
                while (nextBoundary[0] < boundaries.size() && dk.compareTo(boundaries.get(nextBoundary[0])) >= 0)
                {
                    runStarts[++nextBoundary[0]] = index;
                    runPositions[nextBoundary[0]] = position;
                }
            }
        });

        // Boundaries past the parent's last key produce trailing empty runs. Their offsets stay UNRESOLVED and
        // are never read: build() skips a run with from >= to, and the last non-empty run takes its hi from
        // dataLength precisely because the run after it starts at partitionCount.
        while (nextBoundary[0] < boundaries.size())
            runStarts[++nextBoundary[0]] = count;

        return new Runs(runStarts, runPositions, count);
    }

    /** The byte-share form: one pass, driving {@link RunSelector}. */
    private static Runs selectByByteShare(SSTableReader parent, int numChildren, int partitionCount)
    {
        RunSelector selector = new RunSelector(parent.uncompressedLength(), numChildren, partitionCount);
        int count = walkIndex(parent, (index, key, position) -> selector.offer(index, position));
        if (count != partitionCount)
            throw new IllegalStateException("parent Index.db grew or shrank between passes: counted " +
                                            partitionCount + ", then " + count + ": " + parent.descriptor);
        return selector.finish();
    }

    /**
     * Streaming form of {@link #chooseByByteShare}: fed every partition's Data.db offset in order, it produces
     * the same run starts, plus each run's first offset, in O(numChildren) heap rather than O(partitions).
     *
     * <p>The selection is a forward scan with one record of lookback, so the only reason the array version
     * needed random access was its two clamps, and both reach only a bounded distance:
     * <ul>
     *   <li><b>Tail room</b> ({@code min(candidate, partitionCount - (numChildren - m))}) can only name one of
     *       the last {@code numChildren} records, so those offsets are kept in {@link #tail}.</li>
     *   <li><b>Non-empty</b> ({@code max(candidate, runStarts[m - 1] + 1)}) only binds when the natural
     *       candidate has not advanced past the previous run's start, which means the record it names is at
     *       most one past the cursor. When it is exactly one past, the offset is not readable yet and is filled
     *       in by a later {@link #offer}; those deferrals are contiguous, so a single pointer tracks them.</li>
     * </ul>
     * {@link #chooseByByteShare} is kept as the reference implementation this is differentially tested against.
     */
    @VisibleForTesting
    static final class RunSelector
    {
        private final long uncompressedLength;
        private final int numChildren;
        private final int partitionCount;

        private final int[] runStarts;
        private final long[] runPositions;

        /** Offsets of the last {@code min(numChildren, partitionCount)} records: all the tail clamp can name. */
        private final long[] tail;
        private final int tailFrom;

        private long base = UNRESOLVED;
        private long total;
        /** The run being placed; runs {@code [1, nextRun)} have their start index decided. */
        private int nextRun = 1;
        /** Runs {@code [1, firstUnresolved)} have their offset filled in. */
        private int firstUnresolved = 1;
        private int previousIndex = -1;
        private long previousPosition = UNRESOLVED;

        RunSelector(long uncompressedLength, int numChildren, int partitionCount)
        {
            Preconditions.checkArgument(numChildren >= 1 && numChildren <= partitionCount,
                                        "numChildren %s out of range for %s partitions", numChildren, partitionCount);
            this.uncompressedLength = uncompressedLength;
            this.numChildren = numChildren;
            this.partitionCount = partitionCount;
            this.runStarts = new int[numChildren];
            this.runPositions = new long[numChildren];
            Arrays.fill(this.runPositions, UNRESOLVED);
            this.tailFrom = Math.max(0, partitionCount - numChildren);
            this.tail = new long[partitionCount - tailFrom];
        }

        void offer(int index, long position)
        {
            if (index >= tailFrom)
                tail[index - tailFrom] = position;

            if (index == 0)
            {
                base = position;
                total = uncompressedLength - base;
                runStarts[0] = 0;
                runPositions[0] = position;
            }

            // A placement forced onto the record after the cursor could not read its offset at the time.
            if (firstUnresolved < nextRun && runStarts[firstUnresolved] == index)
                runPositions[firstUnresolved++] = position;

            // Several targets can fall inside one partition, so keep placing until this record is short of the
            // next one.
            while (nextRun < numChildren)
            {
                long target = base + (total * nextRun) / numChildren;
                if (position < target)
                    break;
                place(index, position, target);
            }

            previousIndex = index;
            previousPosition = position;
        }

        Runs finish()
        {
            // Targets the scan never reached: the cursor is at partitionCount, which the tail clamp pulls back
            // to a real record. position and target are unread in that case -- the snap-back is guarded on
            // candidate < partitionCount.
            while (nextRun < numChildren)
                place(partitionCount, UNRESOLVED, UNRESOLVED);

            if (firstUnresolved != numChildren)
                throw new IllegalStateException("run " + firstUnresolved + " of " + numChildren +
                                                " never had its Data.db offset resolved");
            for (int m = 1; m < numChildren; m++)
            {
                if (runStarts[m] <= runStarts[m - 1])
                    throw new IllegalStateException("run starts are not strictly increasing: " +
                                                    Arrays.toString(runStarts));
            }
            return new Runs(runStarts, runPositions, partitionCount);
        }

        private void place(int index, long position, long target)
        {
            int m = nextRun;
            int candidate = index;
            long candidatePosition = position;

            // snap to whichever partition boundary is nearer the target
            if (candidate > 0 && candidate < partitionCount
                && (position - target) > (target - previousPosition))
            {
                candidate = previousIndex;
                candidatePosition = previousPosition;
            }

            // never emit an empty child ...
            if (candidate <= runStarts[m - 1])
            {
                candidate = runStarts[m - 1] + 1;
                // one past the cursor: its offset arrives with the next record
                candidatePosition = candidate == index ? position : UNRESOLVED;
            }
            // ... and always leave room for the runs still to be placed. This can only pull the candidate back
            // into the tail window, and never below the clamp above, because runStarts[m - 1] is itself bounded
            // by partitionCount - (numChildren - (m - 1)).
            int room = partitionCount - (numChildren - m);
            if (candidate > room)
            {
                candidate = room;
                candidatePosition = tail[candidate - tailFrom];
            }

            runStarts[m] = candidate;
            runPositions[m] = candidatePosition;
            if (candidatePosition != UNRESOLVED)
            {
                if (firstUnresolved != m)
                    throw new IllegalStateException("run " + m + " resolved out of order, expected " + firstUnresolved);
                firstUnresolved = m + 1;
            }
            nextRun++;
        }
    }

    /**
     * The reference implementation of split-point selection, kept because it is far easier to read than
     * {@link RunSelector} and because {@code RunSelector} is tested by asserting it agrees with this for
     * randomised inputs. Not used in production: it needs every partition's offset at once, which is exactly
     * the allocation this class no longer makes.
     */
    @VisibleForTesting
    static int[] chooseByByteShare(long[] positions, long uncompressedLength, int numChildren)
    {
        int n = positions.length;
        int[] runStarts = new int[numChildren];
        runStarts[0] = 0;

        long base = positions[0];
        long total = uncompressedLength - base;
        int cursor = 0;
        for (int m = 1; m < numChildren; m++)
        {
            long target = base + (total * m) / numChildren;
            while (cursor < n && positions[cursor] < target)
                cursor++;

            int candidate = cursor;
            // snap to whichever partition boundary is nearer the target
            if (candidate > 0 && candidate < n
                && (positions[candidate] - target) > (target - positions[candidate - 1]))
                candidate--;

            // never emit an empty child, and always leave room for the runs still to be placed
            candidate = Math.max(candidate, runStarts[m - 1] + 1);
            candidate = Math.min(candidate, n - (numChildren - m));
            runStarts[m] = candidate;
            cursor = Math.max(cursor, candidate);
        }
        return runStarts;
    }

    // ------------------------------------------------------------------------------------------------
    // Pass 2: build every child from a single sequential walk of the parent Index.db
    // ------------------------------------------------------------------------------------------------

    private static Result build(SSTableReader parent, Runs runs,
                                List<RepairState> perRun, LifecycleTransaction txn, Progress progress,
                                long startNanos)
    {
        int[] runStarts = runs.runStarts;
        int partitionCount = runs.partitionCount;

        CompressionMetadata meta = parent.getCompressionMetadata();  // owned by parent's dfile; never close it
        final int chunkLength = meta.chunkLength();
        final long parentDataLength = meta.dataLength;
        final long parentCompressedLength = meta.compressedFileLength;

        if (parent.uncompressedLength() != parentDataLength)
            throw new IllegalStateException("uncompressedLength " + parent.uncompressedLength() +
                                            " != CompressionMetadata.dataLength " + parentDataLength);

        // The offsets table must address every chunk the data needs. It is allowed to hold MORE: a
        // compaction-produced sstable carries one extra zero-uncompressed-length chunk, because
        // SSTableRewriter.doPrepare syncs the data file twice (switchWriter(null) -> openFinalEarly() ->
        // dataFile.sync(), then prepareToCommit() -> syncInternal()) and CompressedSequentialWriter.flushData
        // appends a chunk unconditionally, even on an empty buffer. Those bytes belong to no chunk this splitter
        // may copy; keeping them out is chunkEnd()'s job. Fewer entries than the data needs, on the other hand,
        // means the parent's CompressionInfo.db disagrees with its own dataLength and nothing here is safe.
        long addressableChunks = meta.offHeapSize() / 8;
        long neededChunks = (parentDataLength + chunkLength - 1) / chunkLength;
        if (neededChunks > addressableChunks)
            throw new IllegalStateException("parent CompressionInfo.db addresses only " + addressableChunks +
                                            " chunks but dataLength " + parentDataLength + " needs " +
                                            neededChunks + " at chunkLength " + chunkLength + ": " +
                                            parent.descriptor);

        // The four parent metadata components, read once. allOf() is mandatory: unselected types are skipped
        // on read and would be silently dropped from the child's Statistics.db.
        Map<MetadataType, MetadataComponent> parentMetadata = readParentMetadata(parent.descriptor);
        StatsMetadata parentStats = (StatsMetadata) parentMetadata.get(MetadataType.STATS);

        if (perRun != null && perRun.size() != runStarts.length)
            throw new IllegalStateException("perRun has " + perRun.size() + " entries for " + runStarts.length +
                                            " runs; the caller-visible check in split() should have caught this");
        RepairState inherited = perRun == null ? RepairState.inherit(parentStats) : null;

        Supplier<Descriptor> descriptors = descriptorAllocator(parent);

        List<Child> children = new ArrayList<>(runStarts.length);
        List<Descriptor> created = new ArrayList<>(runStarts.length);
        long physicalTotal = 0;
        long deadTotal = 0;
        long padTotal = 0;
        long clonedTotal = 0;
        long duplicated = 0;

        boolean success = false;
        try (RandomAccessReader index = RandomAccessReader.open(parent.descriptor.fileFor(Components.PRIMARY_INDEX)))
        {
            ChunkRange previous = null;
            for (int b = 0; b < runStarts.length; b++)
            {
                int from = runStarts[b];
                int to = (b + 1 < runStarts.length) ? runStarts[b + 1] : partitionCount;
                if (from >= to)
                    continue;  // empty boundary range -> no child

                long lo = runs.runPositions[b];
                // The run after this one starts where this one's data ends; for the last run that is the end of
                // the parent's data. An empty trailing run has runStarts == partitionCount, which is exactly
                // the case that takes dataLength, so its UNRESOLVED offset is never read.
                long hi = (to < partitionCount) ? runs.runPositions[b + 1] : parentDataLength;
                if (lo == UNRESOLVED || hi == UNRESOLVED)
                    throw new IllegalStateException("run " + b + " has an unresolved Data.db offset");
                ChunkRange range = chunkRange(lo, hi, chunkLength);

                long copyFrom = chunkStart(meta, range.firstChunk, chunkLength);
                long copyTo = chunkEnd(meta, range.lastChunk, chunkLength);
                long physicalBytes = copyTo - copyFrom;
                if (physicalBytes <= 0)
                    throw new IllegalStateException("non-positive physical length " + physicalBytes + " for " + range);
                if (copyTo > parentCompressedLength)
                    throw new IllegalStateException("child would read past the end of the parent's " +
                                                    parentCompressedLength + "-byte Data.db (copyTo=" + copyTo +
                                                    ") for " + range);

                // Carried with the range, never re-derived positionally: an empty range above produced no child
                // and must not shift the state of the ranges after it.
                RepairState repairState = perRun == null ? inherited : perRun.get(b);

                Descriptor child = descriptors.get();
                created.add(child);
                Child built = buildChild(parent, child, index, from, to, range, meta, copyFrom,
                                        physicalBytes, parentMetadata, parentStats, repairState, txn, progress);
                children.add(built);

                physicalTotal += physicalBytes;
                deadTotal += range.deadPrefixBytes;
                padTotal += built.headPadBytes;
                clonedTotal += built.clonedBytes;
                if (previous != null && previous.lastChunk == range.firstChunk)
                {
                    duplicated += chunkEnd(meta, range.firstChunk, chunkLength)
                                  - chunkStart(meta, range.firstChunk, chunkLength);
                }
                previous = range;
            }
            success = true;
        }
        catch (IOException e)
        {
            throw new UncheckedIOException("failed splitting " + parent.descriptor, e);
        }
        finally
        {
            if (!success)
                cleanUp(children, created);
        }

        Result result = new Result(ImmutableList.copyOf(children), physicalTotal, deadTotal, padTotal,
                                   clonedTotal, duplicated, Clock.Global.nanoTime() - startNanos);
        logger.info("Split {} into {} children: {}", parent.descriptor, children.size(), result);
        return result;
    }

    /** The absolute Data.db offset at which chunk {@code k} begins. */
    static long chunkStart(CompressionMetadata meta, long k, int chunkLength)
    {
        return chunkFor(meta, k, chunkLength).offset;
    }

    /**
     * The absolute Data.db offset one past the end of chunk {@code k}, INCLUDING its 4-byte inline CRC32.
     * <p>
     * Derived from the chunk itself, and deliberately never from the physical file length.
     * {@link CompressionMetadata.Chunk#length} excludes the checksum, so {@code offset + length + 4} is exactly
     * where the next chunk starts -- and {@code chunkFor} has already resolved that from the offsets table when
     * a further entry exists, and from {@code compressedFileLength} when it does not.
     * <p>
     * This is the fix for a silent-corruption bug worth spelling out, because the shape that triggers it is the
     * common one. An earlier version computed the end of chunk {@code k} as the start of chunk {@code k + 1},
     * with a chunk count taken to be {@code ceil(dataLength / chunkLength)} and {@code compressedFileLength}
     * substituted when {@code k + 1} reached that count. For a child that assumption is exact -- the
     * {@code (C-1)*L < Dp <= C*L} invariant in {@link #chunkRange} forces {@code ceil(Dp/L) == C}. For a
     * <em>compaction-produced</em> parent it is one short: such an sstable carries an extra
     * zero-uncompressed-length chunk (see the note in {@link #build}), so {@code compressedFileLength} is 9-ish
     * bytes past the end of the last real chunk. The last child then copied that trailing slack, its own last
     * chunk's length -- which the reader derives as {@code compressedFileLength - offsets[C-1] - 4} -- grew by
     * exactly that much, and every read of the child's final chunk failed its CRC32 with
     * {@code CorruptBlockException}, or, once the inflated length crossed {@code maxCompressedLength}, took the
     * reader's raw-chunk branch and returned compressed bytes as row data. Nothing caught it on the way out:
     * Digest.crc32 is computed over whatever bytes were actually written, so it stayed self-consistent, and the
     * parent had already been obsoleted by the time anything read the child.
     * <p>
     * The reason no test saw it: every test parent is <em>flushed</em>, and the flush path calls
     * {@code flushData} exactly once, at prepare, with the final partial buffer. Only the double-sync in
     * {@code SSTableRewriter} produces the extra chunk.
     */
    static long chunkEnd(CompressionMetadata meta, long k, int chunkLength)
    {
        CompressionMetadata.Chunk chunk = chunkFor(meta, k, chunkLength);
        return chunk.offset + chunk.length + 4;   // "4": the inline CRC32 the reader expects to follow the chunk
    }

    private static CompressionMetadata.Chunk chunkFor(CompressionMetadata meta, long k, int chunkLength)
    {
        if (k < 0)
            throw new IllegalArgumentException("negative chunk index " + k);
        return meta.chunkFor(k * (long) chunkLength);
    }

    @SuppressWarnings("resource")
    private static Child buildChild(SSTableReader parent,
                                    Descriptor child,
                                    RandomAccessReader index,
                                    int from,
                                    int to,
                                    ChunkRange range,
                                    CompressionMetadata meta,
                                    long copyFrom,
                                    long physicalBytes,
                                    Map<MetadataType, MetadataComponent> parentMetadata,
                                    StatsMetadata parentStats,
                                    RepairState repairState,
                                    LifecycleTransaction txn,
                                    Progress progress) throws IOException
    {
        TableMetadata metadata = parent.metadata();
        int chunkLength = range.chunkLength;
        int partitionCount = to - from;

        // DIGEST and FILTER are added below only if they are actually written; the set handed to
        // SSTableReader.open and to appendTOC has to name the files that exist and no others.
        Set<Component> components = Sets.newHashSet(Components.DATA,
                                                              Components.PRIMARY_INDEX,
                                                              Components.COMPRESSION_INFO,
                                                              Components.STATS,
                                                              Components.SUMMARY);

        // ---------- Data.db: verbatim compressed chunk run, shared with the parent where possible ----------
        // Sharing needs the head of the run aligned, which costs a pad, so it is only planned for when the
        // filesystem has not already said no. An unpadded run cannot be shared at all -- O(i) is aligned to
        // nothing -- so this decision has to be made before the copy, not after it fails.
        boolean canShare = DatabaseDescriptor.getZeroCopySplitReflinkEnabled()
                           && Reflink.isPossibleIn(child.directory);
        boolean align = forceAlignedLayoutForTesting || (canShare && physicalBytes >= MIN_CLONE_BYTES);
        CopyPlan plan = copyPlan(copyFrom, physicalBytes, align, align && canShare);
        long cloned = copyData(parent.descriptor.fileFor(Components.DATA), child.fileFor(Components.DATA),
                               child.directory, plan, progress);
        long actual = child.fileFor(Components.DATA).length();
        if (actual != plan.childLength)
            throw new IllegalStateException("child Data.db is " + actual + " bytes, expected exactly " +
                                            plan.childLength + " (trailing slack corrupts the last chunk's" +
                                            " length)");

        // ---------- CompressionInfo.db: same params, rebased offsets, offsets[0] == headPadBytes ----------
        writeCompressionInfo(child, meta, range, plan);

        // ---------- Index.db + FILTER + SUMMARY + HLL + partition-size histogram, one pass ----------
        EstimatedHistogram partitionSizes = new EstimatedHistogram(PARTITION_SIZE_HISTOGRAM_BUCKETS);
        ICardinality cardinality = new HyperLogLogPlus(HLL_P, HLL_SP);
        double fpChance = metadata.params.bloomFilterFpChance;
        // fpChance == 1.0 yields an AlwaysPresentFilter, whose serialize() is a no-op -- writing it would leave a
        // zero-length Filter.db, which requireNonEmpty rejects. The read path already treats a missing Filter.db
        // as always-present, so just omit the component.
        IFilter bf = fpChance < 1.0 ? FilterFactory.getFilter(partitionCount, fpChance) : null;
        DecoratedKey first = null;
        DecoratedKey last = null;

        try
        {
            try (SequentialWriter out = new SequentialWriter(child.fileFor(Components.PRIMARY_INDEX), writerOption());
                 IndexSummaryBuilder summary = new IndexSummaryBuilder(partitionCount,
                                                                       metadata.params.minIndexInterval,
                                                                       Downsampling.BASE_SAMPLING_LEVEL))
            {
                long previousPosition = UNRESOLVED;
                for (int r = from; r < to; r++)
                {
                    ByteBuffer key = ByteBufferUtil.readWithShortLength(index);
                    long position = RowIndexEntry.Serializer.readPosition(index);
                    int promotedSize = index.readUnsignedVInt32();
                    byte[] promoted = null;
                    if (promotedSize > 0)
                    {
                        promoted = new byte[promotedSize];
                        index.readFully(promoted);
                    }

                    // The selection pass and this one have to land on the same records. Checking the run's
                    // first offset against what selection recorded, and strict monotonicity from there on,
                    // catches a desynchronised walk without keeping an offset per partition -- and rules out a
                    // non-increasing parent index, which the old per-record equality check did not.
                    if (r == from)
                    {
                        if (position != range.lo)
                            throw new IllegalStateException("index walk desynchronised at record " + r +
                                                            ": run starts at " + position + ", selection said " +
                                                            range.lo);
                    }
                    else
                    {
                        if (position <= previousPosition)
                            throw new IllegalStateException("parent Index.db offsets are not strictly increasing" +
                                                            " at record " + r + ": " + previousPosition + " -> " +
                                                            position);
                        // exact estimatedPartitionSize: rowSize_i == position_{i+1} - position_i identically,
                        // so each partition is sized one record late, from the next record's offset
                        partitionSizes.add(position - previousPosition);
                    }
                    previousPosition = position;

                    DecoratedKey dk = parent.getPartitioner().decorateKey(key);
                    // MetadataCollector.addKey hashes the raw key bytes, position/remaining passed explicitly
                    long hashed = MurmurHash.hash2_64(key, key.position(), key.remaining(), 0);

                    long childIndexStart = out.position();
                    ByteBufferUtil.writeWithShortLength(key, out);
                    // The ONLY rewritten field. Canonical minimal vint, never padded -- so the child's records
                    // are shorter than the parent's and its index offsets are NOT the parent's minus a constant.
                    out.writeUnsignedVInt(position - range.shift);
                    out.writeUnsignedVInt32(promotedSize);
                    if (promoted != null)
                        out.write(promoted, 0, promotedSize);

                    if (first == null)
                        first = dk;
                    last = dk;
                    if (bf != null)
                        bf.add(dk);
                    summary.maybeAddEntry(dk, childIndexStart);
                    cardinality.offerHashed(hashed);
                }

                // The run's last partition ends where the next run's first record starts, which for the last
                // run is the end of the parent's data -- exactly what chunkRange() was handed as hi.
                if (range.hi <= previousPosition)
                    throw new IllegalStateException("run ends at " + range.hi + " but its last record is at " +
                                                    previousPosition);
                partitionSizes.add(range.hi - previousPosition);
                out.finish();

                first = first.retainable();
                last = last.retainable();
                try (IndexSummary built = summary.build(parent.getPartitioner()))
                {
                    writeSummary(child, first, last, built);
                }
            }
            requireNonEmpty(child, Components.SUMMARY);

            // ---------- Filter.db ----------
            if (bf != null)
            {
                writeFilter(child, bf);
                requireNonEmpty(child, Components.FILTER);
                components.add(Components.FILTER);
            }
        }
        finally
        {
            if (bf != null)
                bf.close();
        }

        // ---------- Statistics.db ----------
        // compressionRatio is compressed-over-uncompressed for the FILE, so the pad counts: it is on disk and
        // every consumer of the ratio is estimating disk footprint from a partition count.
        writeStatistics(child, metadata, parentMetadata, parentStats, partitionSizes, cardinality,
                        plan.childLength, range.dataLength, first, last, false, repairState);

        // ---------- Digest.crc32: CRC32 over EVERY physical byte of the child Data.db ----------
        // Optional, and the one component whose cost is proportional to the DATA rather than to the index: with
        // the extents shared this read is the whole remaining cost of the split. Skipping it is a supported
        // configuration, not a degraded one -- see writeDigest and Config.zero_copy_split_digest_enabled.
        if (DatabaseDescriptor.getZeroCopySplitDigestEnabled())
        {
            writeDigest(child, progress);
            requireNonEmpty(child, Components.DIGEST);
            components.add(Components.DIGEST);
        }

        // ---------- TOC.txt, last: it has to name every file that exists and no others ----------
        components.add(Components.TOC);
        TOCComponent.updateTOC(child, components);

        // Every component's CONTENTS are now fsynced individually; this makes their DIRECTORY ENTRIES durable
        // too. Without it a crash can leave a directory that does not list a file whose data is on disk, which is
        // the same loss as an unsynced file. Only the components written through SequentialWriter (Index.db,
        // Statistics.db) sync the directory themselves, on create (SequentialWriter.openChannel ->
        // SyncUtil.trySyncDir); Data.db, Filter.db, Summary.db, Digest.crc32 and TOC.txt do not. One fsync per
        // child, and it has to happen before the child is published, because zcTxn's COMMIT record -- which is
        // itself fsynced and which unlinks the parent -- must never become durable first.
        SyncUtil.trySyncDir(child.directory);

        SSTableReader reader = SSTableReader.open(parent.owner().orElse(null), child, components, parent.metadata);
        try
        {
            validateChild(reader, range, plan, physicalBytes, partitionCount, chunkLength);
        }
        catch (Throwable t)
        {
            reader.selfRef().release();
            throw t;
        }

        if (txn != null)
            txn.trackNew(reader);

        return new Child(child, first, last, range, physicalBytes, plan.headPadBytes, cloned, partitionCount,
                         ImmutableSet.copyOf(components), repairState, reader);
    }

    // ------------------------------------------------------------------------------------------------
    // Component writers
    //
    // Several of these are package-private rather than private because {@link ZeroCopySSTableSlice} synthesises
    // the same components for the same reason -- verbatim byte ranges need an index rebased onto them, and
    // everything else follows from that index -- and every remark below about what may and may not be inherited
    // applies there identically. Sharing them is what keeps the two paths from drifting; a second copy of
    // writeStatistics in particular would be a second place to get the SerializationHeader and the
    // commitlog-interval/host-id pair wrong. writeCompressionInfo is NOT shared: a split child is one chunk run
    // with an alignment pad, a slice is a concatenation of runs with none, and the two loops have nothing in
    // common but the writer they call.
    // ------------------------------------------------------------------------------------------------

    /**
     * Materialise the child's Data.db as the verbatim parent byte range
     * {@code [plan.srcStart, plan.srcStart + plan.childLength)}, sharing as much of it as the filesystem allows
     * and copying the rest.
     * <p>
     * The clone comes first and is all-or-nothing: {@code FICLONERANGE} either shares every byte it was asked
     * for or writes nothing at all, so a refusal costs one syscall and falls straight through to the transfer
     * loop, which then copies the whole range exactly as it did before extent sharing existed. That is also why
     * the head pad is harmless when the clone fails: the padded layout is a property of the plan, not of the
     * mechanism, and a padded range that had to be copied produces a child byte-for-byte identical to the one a
     * successful clone would have produced.
     * <p>
     * transferTo returns short counts and caps near 0x7ffff000, so it MUST be looped; {@code n <= 0} means EOF,
     * not "retry". The loop is also where this operation is throttled and cancelled: each
     * {@link #TRANSFER_SLICE} is cleared with {@code progress} before it moves, so {@code compaction_throughput}
     * bounds the copy and a stop request raises {@link CompactionInterruptedException} within one slice rather
     * than at the end of a multi-GiB file. A clone moves no bytes, so it is checked for cancellation but not
     * throttled.
     *
     * @return how many bytes were shared rather than copied; 0 means the whole range was transferred
     */
    private static long copyData(File src, File dst, File directory, CopyPlan plan, Progress progress)
    throws IOException
    {
        try (FileChannel in = src.newReadChannel();
             FileChannel outChannel = dst.newWriteChannel(File.WriteMode.OVERWRITE))
        {
            long cloned = 0;
            if (plan.cloneLength > 0)
            {
                if (progress != null)
                    progress.checkStopped();
                if (Reflink.tryCloneRange(in, plan.srcStart, outChannel, 0, plan.cloneLength, directory))
                {
                    cloned = plan.cloneLength;
                    if (progress != null)
                        progress.cloned(cloned);
                }
            }

            // The ioctl does not move the destination's file position, and transferTo writes at wherever that
            // is, so the tail has to be positioned explicitly. Without this the tail would overwrite the head
            // of the range that was just shared -- which, being copy-on-write, would silently succeed.
            outChannel.position(cloned);

            long position = plan.srcStart + cloned;
            long remaining = plan.childLength - cloned;
            while (remaining > 0)
            {
                int slice = (int) Math.min(remaining, TRANSFER_SLICE);
                if (progress != null)
                    progress.beforeSlice(slice);
                long n = in.transferTo(position, slice, outChannel);
                if (n <= 0)
                    throw new IOException(String.format("short transferTo of %s at %d with %d left",
                                                        src, position, remaining));
                position += n;
                remaining -= n;
                if (progress != null)
                    progress.afterSlice(n);
            }
            outChannel.truncate(plan.childLength);   // never leave a trailing byte
            outChannel.force(true);
            return cloned;
        }
    }

    /**
     * Child CompressionInfo.db via the same {@code Writer} every real sstable is written with, so the child
     * cannot drift from the format. Only dataLength, chunkCount and the offsets differ from the parent.
     * <p>
     * Offsets are rebased by {@link CopyPlan#srcStart}, not by {@code O(i)}, so the child's {@code offsets[0]}
     * is its head pad rather than 0 whenever the run was aligned for sharing. Nothing else changes: the offsets
     * remain absolute positions in the child's own Data.db, and the last chunk's derived length
     * ({@code compressedFileLength - offsets[C-1] - 4}) is unaffected because the pad shifts both terms.
     */
    private static void writeCompressionInfo(Descriptor child, CompressionMetadata meta, ChunkRange range,
                                             CopyPlan plan)
    {
        CompressionMetadata.Writer writer =
            CompressionMetadata.Writer.open(meta.parameters, child.fileFor(Components.COMPRESSION_INFO), meta.compressionDictionary());
        boolean prepared = false;
        try
        {
            for (long k = range.firstChunk; k <= range.lastChunk; k++)
            {
                long offset = meta.chunkFor(k * (long) range.chunkLength).offset - plan.srcStart;
                if (k == range.firstChunk && offset != plan.headPadBytes)
                    throw new IllegalStateException("child offsets[0] must be " + plan.headPadBytes +
                                                    ", got " + offset);
                writer.addOffset(offset);
            }
            writer.finalizeLength(range.dataLength, Math.toIntExact(range.chunkCount));
            writer.prepareToCommit();   // doPrepare() is what writes and fsyncs the file
            prepared = true;
            writer.commit();
        }
        catch (Throwable t)
        {
            // doAbort() only frees memory, it does not delete an already-written file
            if (!prepared)
                writer.abort();
            child.fileFor(Components.COMPRESSION_INFO).deleteIfExists();
            throw t;
        }
        finally
        {
            writer.close();
        }
    }

    /**
     * The child's Statistics.db: the parent's four components with exactly two derived replacements
     * (estimatedPartitionSize and the COMPACTION cardinality) plus a recomputed compressionRatio.
     * <p>
     * HEADER is passed through by reference and is MANDATORY to inherit byte-for-byte: rows in the copied
     * Data.db encode timestamps/localDeletionTime/TTL as unsigned vint deltas off
     * {@code stats.minTimestamp/minLocalDeletionTime/minTTL} and encode their columns as a bitmap subset of
     * {@code header.columns()}. Tightening any of those silently corrupts every relocated row with all CRCs
     * still passing.
     * <p>
     * {@code commitLogIntervals} and {@code originatingHostId} are inherited as an ATOMIC PAIR from the same
     * parent StatsMetadata (see docs/splits-research.md 4.5). Copying the parent's interval set into all K
     * children leaves the per-table union in CommitLogReplayer bit-identical because IntervalSet.Builder.add
     * is normalising and idempotent. The bug this avoids is stamping the child with the LOCAL host id (which
     * every MetadataCollector constructor does) while inheriting a foreign parent's intervals: the replayer
     * gates on {@code originatingHostId.equals(localhostId)} and would then interpret foreign segment ids
     * against the local commitlog, discarding acked-but-unflushed mutations.
     * <p>
     * {@code repairedAt}/{@code pendingRepair}/{@code isTransient} come from {@code repairState}, which
     * defaults to the parent's triple. Writing them here rather than mutating afterwards means the reader
     * opened a few lines later is already correct, so nothing ever publishes a child with the wrong repair
     * state -- the Tracker routes a newly visible sstable to a compaction strategy holder by exactly this
     * triple ({@code CompactionStrategyManager.handleListChangedNotification}).
     * <p>
     * {@code sstableLevel} is still inherited. That matches what {@code createWriterForAntiCompaction} does for
     * a single-input anticompaction (it preserves the level when all inputs agree), and it is safe here: the
     * children are disjoint contiguous key sub-ranges of the parent's range, so they cannot overlap each other,
     * and they occupy exactly the slot the obsoleted parent vacated.
     */
    static void writeStatistics(Descriptor child,
                                TableMetadata metadata,
                                Map<MetadataType, MetadataComponent> parentMetadata,
                                StatsMetadata parentStats,
                                EstimatedHistogram partitionSizes,
                                ICardinality cardinality,
                                long onDiskLength,
                                long dataLength,
                                DecoratedKey childFirst,
                                DecoratedKey childLast,
                                boolean hasUnindexedRegions,
                                RepairState repairState) throws IOException
    {
        // The four inherited absolute TOTALS below (estimatedCellPerPartitionCount, estimatedTombstoneDropTime,
        // totalColumnsSet, totalRows) are parent-wide in every child, so per-table aggregates over-report by
        // ~K and worthDroppingTombstones under-fires by ~K. Accepted, conservative in direction, and documented
        // on the class javadoc under "Accepted imprecision in the children's Statistics.db" -- recomputing them
        // would require deserialising every row, which is the whole cost this class exists to avoid.
        StatsMetadata childStats = new StatsMetadata(partitionSizes,                              // DERIVED, exact
                                                     parentStats.estimatedCellPerPartitionCount,  // ACCEPTED: parent-wide
                                                     parentStats.commitLogIntervals,              // atomic pair, see javadoc
                                                     parentStats.minTimestamp,
                                                     parentStats.maxTimestamp,
                                                     parentStats.minLocalDeletionTime,
                                                     parentStats.maxLocalDeletionTime,
                                                     parentStats.minTTL,
                                                     parentStats.maxTTL,
                                                     (double) onDiskLength / dataLength,          // DERIVED, exact
                                                     parentStats.estimatedTombstoneDropTime,      // ACCEPTED: parent-wide
                                                     parentStats.sstableLevel,
                                                     // What MetadataCollector.finalizeMetadata passes, and the
                                                     // only correct value: CQL cannot add a clustering column to
                                                     // an existing table, so the comparator cannot have drifted
                                                     // from the prefix coveredClustering was recorded against.
                                                     // Must be non-null -- StatsMetadata's serializer asserts it
                                                     // whenever version.hasImprovedMinMax().
                                                     metadata.comparator.subtypes(),
                                                     parentStats.coveredClustering,               // inherit: a superset of the child's
                                                     parentStats.hasLegacyCounterShards,
                                                     repairState.repairedAt,                      // CALLER SUPPLIED
                                                     parentStats.totalColumnsSet,                 // ACCEPTED: parent-wide
                                                     parentStats.totalRows,                       // ACCEPTED: parent-wide
                                                     // NOT inherited. The parent's coverage is the whole token
                                                     // range it spans; giving it to each of K children would
                                                     // multiply the table's apparent coverage by K and mislead
                                                     // the density calculations that drive compaction. It cannot
                                                     // be recomputed here (that needs the local ranges), and NaN
                                                     // is exactly MetadataCollector's default for "unknown".
                                                     Double.NaN,
                                                     parentStats.originatingHostId,               // atomic pair, see javadoc
                                                     repairState.pendingRepair,                   // CALLER SUPPLIED
                                                     repairState.isTransient,                     // CALLER SUPPLIED
                                                     parentStats.hasPartitionLevelDeletions,      // inherit: conservative direction
                                                     // The CHILD's own range, never the parent's. When
                                                     // version.hasKeyRange() these take priority over Summary.db
                                                     // in the reader's first/last, so inheriting would make every
                                                     // child claim the whole parent range and break every
                                                     // range-based sstable selection.
                                                     childFirst.getKey(),
                                                     childLast.getKey(),
                                                     // A split child is one contiguous chunk run: everything
                                                     // between its first indexed partition and dataLength is
                                                     // indexed, so a linear scan of it is sound. A slice is not,
                                                     // and passes true. See StatsMetadata#hasUnindexedRegions.
                                                     hasUnindexedRegions);

        Map<MetadataType, MetadataComponent> components = new EnumMap<>(parentMetadata);
        components.put(MetadataType.STATS, childStats);
        components.put(MetadataType.COMPACTION, new CompactionMetadata(cardinality));
        // VALIDATION (partitioner + fp chance) and HEADER pass through by reference: no schema lookup,
        // nothing that can throw, byte-identical to the parent.

        // StatsComponent.save is a SequentialWriter plus finish(), i.e. what BigTableWriter does, and NOT
        // MetadataSerializer.rewriteSSTableMetadata. That helper only flush()es and renames, with no fsync of
        // the file and no fsync of the directory. That is fine for its existing callers, which mutate the repair
        // status of an sstable whose Statistics.db is ALREADY durable, and it is not fine here: this is the only
        // copy of the child's SerializationHeader and repair state. finish() ends in syncInternal(), and
        // SequentialWriter fsyncs the directory when it creates the file, so both the contents and the directory
        // entry are durable before the transaction's COMMIT record unlinks the parent.
        new StatsComponent(components).save(child);
        requireNonEmpty(child, Components.STATS);
    }

    /**
     * Filter.db, fsynced. {@code FilterComponent.save} already does exactly what this path needs -- it writes
     * through a {@code FileOutputStreamPlus}, flushes, fsyncs, and propagates the IOException rather than
     * swallowing it -- so there is nothing to hand-roll here. {@code deleteOnFailure} is false because
     * {@link #cleanUp} removes every component of a failed child as one unit.
     */
    static void writeFilter(Descriptor child, IFilter filter) throws IOException
    {
        FilterComponent.save(filter, child, false);
    }

    /**
     * Summary.db, fsynced.
     * <p>
     * This deliberately does not call {@code IndexSummaryComponent.save}, which writes the same three things in
     * the same order -- treat it as the layout oracle if the format ever changes -- but does NOT fsync. That is
     * fine for its own caller, index summary redistribution, which can rebuild what it loses. It is not fine
     * here: the transaction's COMMIT record is itself fsynced and committing is what unlinks the parent, so a
     * component left in page cache at that moment can be lost while the parent's removal survives. A torn
     * Summary.db is the most survivable of the child's components -- the reader rebuilds it from Index.db -- but
     * "survivable" means a full Index.db pass per child at startup, so write it durably like the others.
     */
    static void writeSummary(Descriptor child, DecoratedKey first, DecoratedKey last, IndexSummary summary)
    throws IOException
    {
        try (FileOutputStreamPlus out = new FileOutputStreamPlus(child.fileFor(Components.SUMMARY)))
        {
            IndexSummary.serializer.serialize(summary, out);
            ByteBufferUtil.writeWithLength(first.getKey(), out);
            ByteBufferUtil.writeWithLength(last.getKey(), out);
            out.flush();
            out.sync();
        }
    }

    /**
     * Digest.crc32 is the plain decimal ASCII of a java.util.zip.CRC32 over EVERY physical byte of Data.db,
     * with no newline and no prefix. That is correct for a compressed sstable too: the writer folds the inline
     * per-chunk CRCs into the full checksum ({@code appendDirect(bb, checksumIncrementalResult=true)}).
     * <p>
     * "Every physical byte" includes the head pad, and it must: {@code Verifier} validates this digest by
     * CRC-ing the whole Data.db file with no reference to CompressionInfo.db, so a digest that skipped the pad
     * would fail there -- and a digest mismatch trips {@code markAndThrow}, which stamps the sstable
     * unrepaired and throws into the disk failure policy.
     * <p>
     * This pass is the dominant cost of a split whose extents were shared: the copy stops reading the parent,
     * but this still reads every byte of every child. Two ways out, one of them implemented:
     * <ul>
     *   <li>SKIP IT, with {@code zero_copy_split_digest_enabled: false}. Nothing needs the component and a
     *       compressed sstable is self-checking without it (every chunk carries an inline CRC32 that this path
     *       preserves and the read path verifies); the cost is that {@code Verifier} answers a missing digest by
     *       upgrading to a full extended verification. See
     *       {@link org.apache.cassandra.config.Config#zero_copy_split_digest_enabled} for the full consumer
     *       audit.</li>
     *   <li>DERIVE IT, not implemented. The child's digest is a CRC32 over a byte range that is verbatim parent,
     *       and each of the parent's per-chunk CRC32s is already stored inline after its chunk with no offset or
     *       chunk index mixed in, so the whole value could be assembled with {@code crc32_combine} from 4 bytes
     *       per chunk plus the pad. That keeps the component with no downstream change at all, for a quarter of
     *       the read at {@code chunk_length_in_kb: 16} and a sixteenth at 64 -- but it is a separate change with
     *       its own correctness burden, and a wrong digest is silent until somebody runs {@code nodetool
     *       verify}.</li>
     * </ul>
     */
    private static void writeDigest(Descriptor child, Progress progress) throws IOException
    {
        CRC32 crc = new CRC32();
        byte[] buffer = new byte[COPY_BUFFER_SIZE];
        try (InputStream in = child.fileFor(Components.DATA).newInputStream())
        {
            int n;
            while ((n = in.read(buffer)) > 0)
            {
                // A second full pass over every byte just written, so it is throttled and cancellable on the
                // same terms as the copy itself -- otherwise stopping the copy would still leave the node
                // grinding through an unbounded read of every child.
                if (progress != null)
                    progress.beforeSlice(n);
                crc.update(buffer, 0, n);
                if (progress != null)
                    progress.afterSlice(n);
            }
        }
        try (FileOutputStreamPlus out = new FileOutputStreamPlus(child.fileFor(Components.DIGEST)))
        {
            out.write(String.valueOf(crc.getValue()).getBytes(StandardCharsets.UTF_8));
            out.flush();
            out.sync();
        }
    }

    // ------------------------------------------------------------------------------------------------
    // Validation and plumbing
    // ------------------------------------------------------------------------------------------------

    /** Cheap post-write checks; every one of them catches a distinct off-by-one. */
    private static void validateChild(SSTableReader child, ChunkRange range, CopyPlan plan, long physicalBytes,
                                      int partitionCount, int chunkLength)
    {
        long onDisk = child.descriptor.fileFor(Components.DATA).length();
        if (onDisk != plan.childLength)
            throw new IllegalStateException("child Data.db length " + onDisk + " != " + plan.childLength);
        if (onDisk - plan.headPadBytes != physicalBytes)
            throw new IllegalStateException("child Data.db holds " + (onDisk - plan.headPadBytes) +
                                            " chunk bytes after a " + plan.headPadBytes + "-byte pad, != " +
                                            physicalBytes);
        if (child.uncompressedLength() != range.dataLength)
            throw new IllegalStateException("child uncompressedLength " + child.uncompressedLength() +
                                            " != " + range.dataLength);

        CompressionMetadata childMeta = child.getCompressionMetadata();
        // The head pad is the ONE place a child's physical layout differs from a writer's, so it is asserted
        // both ways: the file cannot be short of it and the offsets table cannot disagree about it.
        if (childMeta.chunkFor(0).offset != plan.headPadBytes)
            throw new IllegalStateException("child offsets[0] " + childMeta.chunkFor(0).offset + " != head pad "
                                            + plan.headPadBytes);
        if (childMeta.chunkLength() != chunkLength)
            throw new IllegalStateException("child chunkLength " + childMeta.chunkLength() + " != " + chunkLength);

        // getPosition hands back the data-file position itself now; a negative value is "no such key".
        long position = child.getPosition(child.getFirst(), SSTableReader.Operator.EQ);
        if (position < 0)
            throw new IllegalStateException("child cannot find its own first key " + child.getFirst());
        long expectedFirst = range.lo - range.shift;
        if (position != expectedFirst)
            throw new IllegalStateException("child first position " + position + " != " + expectedFirst);
        if (position != range.deadPrefixBytes)
            throw new IllegalStateException("child first position " + position +
                                            " != dead prefix " + range.deadPrefixBytes);
        if (position >= chunkLength)
            throw new IllegalStateException("child first position " + position +
                                            " must be inside the first chunk (L=" + chunkLength + ')');
        if (child.getFirst().compareTo(child.getLast()) > 0)
            throw new IllegalStateException("child first > last: " + child.getFirst() + " > " + child.getLast());

        long lastPosition = child.getPosition(child.getLast(), SSTableReader.Operator.EQ);
        if (lastPosition < 0)
            throw new IllegalStateException("child cannot find its own last key " + child.getLast());

        // Decompress the child's FINAL chunk. This is the one construct the whole design rests on and the one
        // every other check here is blind to: the last chunk is physically a whole chunk while the child's
        // dataLength says only part of it is live, so its length is derived rather than stored and a single byte
        // of trailing slack changes it. Digest.crc32 cannot catch that -- it is computed over whatever bytes were
        // actually written, so it stays self-consistent -- and the checks above only ever touch chunkFor(0) and
        // child.first. Reading the last live byte forces the reader down CompressedChunkReader's normal path,
        // where a wrong derived length fails the inline CRC32 (or LZ4's "Compressed lengths mismatch").
        try (RandomAccessReader in = child.openDataReader())
        {
            in.seek(child.uncompressedLength() - 1);
            in.readByte();
        }
        catch (IOException e)
        {
            throw new CorruptSSTableException(e, child.descriptor.fileFor(Components.DATA));
        }

        logger.trace("Child {} ok: {} partitions, {} physical bytes, dead prefix {}, last partition at {}",
                     child.descriptor, partitionCount, physicalBytes, range.deadPrefixBytes, lastPosition);
    }

    static Map<MetadataType, MetadataComponent> readParentMetadata(Descriptor parent)
    {
        Map<MetadataType, MetadataComponent> components;
        try
        {
            // load(Descriptor) reads MetadataType.values(), i.e. every type -- which is what the check below
            // then insists on. A child cannot be built from a partial parent Statistics.db.
            components = StatsComponent.load(parent).metadata;
        }
        catch (IOException e)
        {
            throw new CorruptSSTableException(e, parent.fileFor(Components.STATS));
        }
        for (MetadataType type : MetadataType.values())
        {
            if (components.get(type) == null)
                throw new IllegalStateException("parent Statistics.db is missing " + type + ": " + parent);
        }
        return components;
    }

    /**
     * Fresh descriptors in the parent's directory, version and format. Prefers the live
     * ColumnFamilyStore's id generator so we cannot collide with a concurrent flush or compaction; falls back
     * to a directory-derived generator plus an existence loop for offline use.
     */
    static Supplier<Descriptor> descriptorAllocator(SSTableReader parent)
    {
        Descriptor template = parent.descriptor;
        ColumnFamilyStore cfs = null;
        try
        {
            cfs = Schema.instance.getColumnFamilyStoreInstance(parent.metadata().id);
        }
        catch (Throwable t)
        {
            logger.debug("No live ColumnFamilyStore for {}, falling back to a directory-derived id generator",
                         template, t);
        }

        if (cfs != null)
        {
            ColumnFamilyStore store = cfs;
            return () -> store.newSSTableDescriptor(template.directory, template.version);
        }

        Supplier<SSTableId> ids = new Directories(parent.metadata()).getUIDGenerator(SSTableIdFactory.instance.defaultBuilder());
        return () -> {
            for (int attempt = 0; attempt < 1000; attempt++)
            {
                Descriptor candidate = new Descriptor(template.version, template.directory, template.ksname,
                                                      template.cfname, ids.get());
                if (!candidate.fileFor(Components.DATA).exists())
                    return candidate;
            }
            throw new IllegalStateException("could not allocate an unused sstable id in " + template.directory);
        };
    }

    static SequentialWriterOption writerOption()
    {
        return SequentialWriterOption.newBuilder()
                                     .trickleFsync(DatabaseDescriptor.getTrickleFsync())
                                     .trickleFsyncByteInterval(DatabaseDescriptor.getTrickleFsyncIntervalInKiB() * 1024)
                                     .build();
    }

    /**
     * Every component is now written by this class through a path that fsyncs and propagates IOException, so this
     * is a cheap post-condition rather than the only error signal it once was -- the {@code SSTableReader.save*}
     * helpers it used to guard against log at TRACE, delete the half-written file and return normally.
     */
    static void requireNonEmpty(Descriptor descriptor, Component component)
    {
        File file = descriptor.fileFor(component);
        if (!file.exists() || file.length() == 0)
            throw new IllegalStateException("failed to write " + component + " for " + descriptor);
    }

    /** Best-effort removal of every partially written child, so a failed split leaves no orphans behind. */
    private static void cleanUp(List<Child> children, List<Descriptor> created)
    {
        for (Child child : children)
        {
            try
            {
                child.reader.selfRef().release();
            }
            catch (Throwable t)
            {
                logger.warn("Failed releasing child {} during cleanup", child.descriptor, t);
            }
        }
        for (Descriptor descriptor : created)
        {
            for (Component component : WRITTEN_COMPONENTS)
            {
                deleteQuietly(descriptor.fileFor(component), descriptor);
            }
            // Statistics.db is written in place now, not via rewriteSSTableMetadata's tmp file + rename, so this
            // should never exist. Kept as belt and braces: a leftover tmp would be picked up as an orphan.
            deleteQuietly(descriptor.tmpFileFor(Components.STATS), descriptor);
        }
        children.clear();
        created.clear();
    }

    private static void deleteQuietly(File file, Descriptor descriptor)
    {
        try
        {
            file.deleteIfExists();
        }
        catch (Throwable t)
        {
            logger.warn("Failed deleting {} while cleaning up {}", file, descriptor, t);
        }
    }
}
