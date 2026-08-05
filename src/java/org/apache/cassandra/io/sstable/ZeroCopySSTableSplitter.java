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
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;
import java.util.zip.CRC32;

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
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableFormat.Components;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.StatsComponent;
import org.apache.cassandra.io.sstable.format.TOCComponent;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.apache.cassandra.io.sstable.format.big.RowIndexEntry;
import org.apache.cassandra.io.sstable.format.bti.BtiFormat;
import org.apache.cassandra.io.sstable.format.bti.BtiZeroCopySplit;
import org.apache.cassandra.io.sstable.indexsummary.IndexSummary;
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
import org.apache.cassandra.utils.IFilter;
import org.apache.cassandra.utils.SyncUtil;
import org.apache.cassandra.utils.TimeUUID;

/**
 * Splits one SSTable into K children by copying verbatim compression-chunk runs of Data.db and rebuilding every
 * other component from an index-only pass. No row is ever deserialised and nothing is recompressed.
 *
 * <h2>Both formats, and what BTI costs that BIG does not</h2>
 * A child's Data.db, CompressionInfo.db, Statistics.db, Digest.crc32 and TOC.txt are the same work whichever
 * format the parent is in -- a compressed chunk run and its offsets table do not know what indexes them. Only the
 * index side differs, and {@link ZeroCopySplitIndex} is where it lives:
 * <ul>
 *   <li><b>BIG</b> reads Index.db, whose records carry both the key and the position, and writes a child Index.db
 *       with exactly one field per record rewritten, plus a Summary.db.</li>
 *   <li><b>BTI</b> reads positions out of the Partitions.db trie, and writes a rebuilt Partitions.db plus a Rows.db
 *       whose entries are selected and re-placed one at a time -- each verbatim except for the one vint that moves,
 *       with zero-byte page padding inserted before an entry when placement needs it. A child's Rows.db is therefore
 *       NOT a byte range of the parent's and its length differs; only Data.db is a verbatim range. The partition
 *       trie has to be rebuilt because it is one structure over every key with no seam to cut; the row index tries
 *       do not, because everything inside them is already relative. See {@code BtiZeroCopySplit.RowIndexCopier}.</li>
 * </ul>
 * The one asymmetry worth knowing before turning this on for BTI: <b>BTI keys live in Data.db.</b> The partition
 * index stores only the shortest prefix that distinguishes a key from its neighbours, so for a partition with no
 * row index the only copy of the full key is at the start of the partition itself -- and a child needs full keys
 * for its bloom filter, its cardinality estimate and its own partition index. A BTI split of a table whose
 * partitions are all narrow therefore pays one sequential decompressing read of the parent's Data.db, which a BIG
 * split does not. It is a read and not a write, and it is zero for a table whose partitions have row indexes, but it
 * is NOT shared with anything else a split does: {@link #writeDigest} reads each CHILD's Data.db raw, with no
 * decompression, and only after the index pass, so {@code zero_copy_split_digest_enabled} neither pays for this read
 * nor avoids it. {@code BtiZeroCopySplit} has the full argument.
 *
 * <h2>The bytes are shared, not copied, where the filesystem can do it</h2>
 * A child's Data.db is a verbatim byte range of the parent's, exactly the shape the {@code FICLONERANGE} ioctl exists
 * for: on xfs formatted with {@code -m reflink=1} (or btrfs) the range is made to point at the parent's physical
 * extents and their reference count bumped, so the split writes no data blocks and unlinking the parent at commit
 * leaves those extents to the children. A split needs room for the index and metadata, not for a second copy of
 * the sstable. See {@link Reflink}.
 * <p>
 * What sharing costs is a <em>head pad</em>. The ioctl requires block-aligned offsets and lengths and a compression
 * chunk boundary is aligned to nothing, so the copied range is extended backwards to the previous 64 KiB boundary and
 * the child's chunk offsets are rebased by that boundary instead of by {@code O(i)}: the child's Data.db begins with
 * up to 64 KiB of the parent's previous chunk and its {@code offsets[0]} is that pad rather than 0. Those bytes belong
 * to no chunk and are never read -- every reader enters Data.db at an offset taken from the offsets array -- but they
 * are covered by Digest.crc32, because {@code Verifier} checksums the whole file. {@link CopyPlan} is the arithmetic.
 * <p>
 * A Data.db that does not begin with a partition -- and, for a sliced sstable, one that holds bytes its own index
 * never describes -- is not a shape the read path used to have to consider, and the consumers that had to learn about
 * it are not one but about nine. Two are the head pad's doing: {@code MmappedRegions} seeded its segment placement at
 * physical 0 and so left the tail of a front-padded file unmapped, and {@code SSTableReader.getPositionsForFullRange}
 * returned physical 0 as the start of the data rather than the first indexed partition. The rest follow
 * {@link StatsMetadata#hasUnindexedRegions}, which a split INHERITS but never sets (a slice sets it):
 * {@code SSTableSimpleScanner} now refuses such an sstable outright, three {@code SSTableReader.getScanner} overloads
 * divert it to {@code indexDrivenScanner} -- implemented by {@code BigTableReader}/{@code BtiTableReader} over
 * {@code BigTableScanner}/{@code BtiTableScanner}, which needed {@code SSTableScanner.makeBounds} to clip bounds that
 * did not come from a {@code DataRange} -- and {@code BigTableScrubber} and {@code SortedTableVerifier} had to stop
 * treating a linear walk of the data file as authoritative.
 * <p>
 * All of it is conditional and self-demoting: the pad is only planned for when {@code Reflink.isPossibleIn} has not
 * already learned that this directory's filesystem cannot share extents (remembered per filesystem, not per
 * directory), a refusal costs one failing ioctl and falls
 * through to the ordinary transfer loop, and a padded range that is copied instead produces a child byte-for-byte
 * identical to a cloned one. {@code Result.totalBytesCloned} reports what happened, and
 * {@code zero_copy_split_reflink_enabled: false} never tries.
 * <p>
 * Page cache is per inode, so bytes read through both parent and child are cached twice (transient: the parent is
 * unlinked when the children are published), and {@code du} counts shared blocks per file where {@code df} counts
 * them once, so per-directory usage over-reports until then.
 * <p>
 * With the copy gone, {@link #writeDigest} is all that still touches the data and so becomes the entire cost of a
 * split. {@code zero_copy_split_digest_enabled: false} takes a split down to its Index.db pass; nothing requires the
 * component, but {@code Verifier} answers its absence with a full extended verification, so {@code nodetool verify}
 * and {@code nodetool import --verify-sstables} get slower for those children (audit on
 * {@link org.apache.cassandra.config.Config#zero_copy_split_digest_enabled}).
 *
 * <h2>Trailing slack is forbidden</h2>
 * A child's LAST chunk has no successor in its own offsets table, so its length is derived as
 * {@code compressedFileLength - offsets[C-1] - 4} from the physical file length. One trailing byte therefore inflates
 * it: with stock settings ({@code crc_check_chance: 1.0}) the inline CRC32 over the inflated length fails first, and
 * where CRC checking is off or sampled the same inflation can instead flip the reader's
 * {@code length < maxCompressedLength} test and hand compressed bytes back as raw data.
 * <p>
 * The child's Data.db is therefore truncated to exactly {@code headPad + chunkEnd(j) - chunkStart(i)} and asserted.
 * {@link #chunkEnd} rather than {@code O(j+1)}: deriving the end as the START of chunk {@code j + 1} silently
 * corrupted the last child of a compaction-produced parent, whose extra zero-uncompressed-length chunk puts
 * {@code compressedFileLength} some bytes past the last real chunk -- {@link #chunkEnd}'s own javadoc has the whole
 * failure, and it must survive any rewrite of this paragraph. The pad shifts both terms equally and does not
 * interfere.
 *
 * <h2>Uncompressed SSTables</h2>
 * Refused, being a different algorithm rather than a degenerate case -- the cut is exact ({@code shift == lo}, no
 * chunk grid, no dead prefix) and CRC.db cannot be sliced. {@link #isSupported(SSTableReader)} tests up front, and
 * the {@link #UNCOMPRESSED_UNSUPPORTED_MESSAGE} refusal spells out what a misaligned CRC.db would cost.
 *
 * <h2>SSTable versions older than the {@code hasUnindexedRegions} marker</h2>
 * Refused as well, and for a reason that has nothing to do with the marker itself: a child inherits its parent's
 * version from {@link #descriptorAllocator}, and {@code CompressionMetadata.Writer.writeHeader} writes
 * {@code maxCompressedLength} UNCONDITIONALLY while the reader only reads it when
 * {@link Version#hasMaxCompressedLength()} (BIG {@code na}+). A child stamped with a pre-{@code na} version -- and
 * {@code earliest_supported_version} is {@code ma}, so a 3.x parent still opens -- would have its own
 * CompressionInfo.db misparsed, the 8-byte {@code dataLength} sliding into the 4-byte chunk count and producing
 * either a multi-GiB {@code Memory.allocate} (an OOM through {@code JVMStabilityInspector}) or a
 * {@code CorruptSSTableException}. {@link Version#hasUnindexedRegionsMarker()} (BIG {@code pb}+, BTI {@code eb}+) is
 * far above that, so testing it subsumes the problem, and it is the right gate anyway: a child's Statistics.db has to
 * be able to carry the marker it inherits. {@link #LEGACY_VERSION_UNSUPPORTED_MESSAGE} is the refusal. The
 * consequence for an operator is that this whole feature is INERT on a node whose sstables predate the marker --
 * including one running with {@code storage_compatibility_mode} set, which pins newly written BIG sstables to
 * {@code nb} or {@code oa} -- until {@code nodetool upgradesstables} has rewritten them.
 * <p>
 * A reader opened {@code MOVED_START} ({@code cloneWithNewStart}, i.e. an early-open reader of a running compaction)
 * is refused for an unrelated reason: its {@code getFirst()} has moved but its Data.db and index have not, so the
 * first child would be cut at a position covering partitions the parent no longer claims.
 *
 * <h2>Accepted imprecision in the children's Statistics.db</h2>
 * Absolute per-sstable <em>totals</em> and min/max bounds cannot be recomputed without deserialising rows -- the
 * entire cost this class exists to avoid -- so every child inherits the PARENT-WIDE value. The full inherited-verbatim
 * set, pinned field by field by {@code ZeroCopyAntiCompactionTest}'s both-paths differential test (which fails until
 * any newly added field is classified, so this list cannot silently fall behind):
 * {@code estimatedCellPerPartitionCount}, {@code estimatedTombstoneDropTime}, {@code totalRows},
 * {@code totalColumnsSet}, {@code minTimestamp}, {@code maxTimestamp}, {@code minLocalDeletionTime},
 * {@code maxLocalDeletionTime}, {@code minTTL}, {@code maxTTL}, {@code coveredClustering},
 * {@code hasPartitionLevelDeletions}, {@code hasLegacyCounterShards}, {@code commitLogIntervals},
 * {@code originatingHostId} and {@code sstableLevel}.
 * <p>
 * Re-derived exactly per child: {@code estimatedPartitionSize} (and hence {@code SSTableReader.estimatedKeys()}),
 * {@code compressionRatio}, and first/last key. NOT inherited: {@code tokenSpaceCoverage}, which is written as
 * {@code NaN} rather than the parent's value, because the parent's coverage is its whole token range and handing it to
 * K children would multiply the table's apparent coverage and mislead the density calculations that drive compaction;
 * recomputing a child's would need the local ranges. Note this is not a divergence from the rewrite path, which also
 * leaves it {@code NaN}: {@code SSTableWriter.setTokenSpaceCoverage}'s only caller is {@code ShardTracker}, on the UCS
 * sharded-writer path, which {@code createWriterForAntiCompaction} does not go through.
 * <p>
 * Every inherited value is at least as wide or large as the truth, so nothing can be lost or resurrected, and the
 * resulting error is ACCEPTED:
 * <ul>
 *   <li>Per-table aggregates that sum across sstables ({@code getMeanRowCount},
 *       {@code estimatedColumnCountHistogram}, the droppable-tombstone ratio) over-report by roughly K until the
 *       children are compacted normally.</li>
 *   <li>{@code AbstractCompactionStrategy.worthDroppingTombstones} divides the child's exact key count by the
 *       parent-wide cell count, so a child's effective {@code tombstone_threshold} is about K times the configured
 *       one. As this path also purges no tombstones, a child retains more droppable tombstones than a rewrite would
 *       have left AND is less likely to be picked for the compaction that would drop them: set
 *       {@code unchecked_tombstone_compaction} or lower {@code tombstone_threshold} where that matters.</li>
 *   <li>Inherited {@code maxTimestamp} puts every child in the parent's TWCS window, and inherited
 *       {@code minLocalDeletionTime} keeps a fully-expired child from being dropped whole by
 *       {@code getFullyExpiredSSTables}. Note it is {@code minLocalDeletionTime} that does this and not
 *       {@code maxLocalDeletionTime}: {@code MetadataCollector} feeds {@code Cell.NO_DELETION_TIME}
 *       ({@code Long.MAX_VALUE}) into the tracker for every live row, so {@code maxLocalDeletionTime} is that
 *       sentinel in any sstable holding live data and can only diverge for an all-tombstone child.</li>
 * </ul>
 *
 * <h2>Durability: every component is fsynced before the child is published</h2>
 * Committing is what unlinks the parent, and the COMMIT record is itself fsynced. So a child component still only in
 * page cache at that moment can be lost by a power failure that the parent's removal survives, and the key range it
 * held is then gone from this replica. Every component is therefore synced before {@code SSTableReader.open}, and
 * {@link SyncUtil#trySyncDir} syncs the directory entry too -- a file whose data is on disk but whose name is not in
 * a synced directory is lost just the same.
 * <p>
 * Statistics.db is the one that must not be got wrong: it is the only copy of the child's
 * {@code SerializationHeader} and repair state and cannot be rebuilt from anything, so it goes through a
 * {@code SequentialWriter} plus {@code finish()} the way {@code BigTableWriter} writes it, and specifically NOT
 * through {@code MetadataSerializer.rewriteSSTableMetadata}, which only flushes and renames. Data.db needs
 * {@code force()} even when its extents were cloned, a clone being a metadata change like any other.
 *
 * <h2>This is a compaction, and behaves like one</h2>
 * When the caller supplies a {@link Progress} the copy is registered with the compaction framework: visible in
 * {@code nodetool compactionstats}, bounded by {@code compaction_throughput}, and stoppable by
 * {@code nodetool stop ANTICOMPACTION}, TRUNCATE, DROP and {@code runWithCompactionsDisabled}. Without one --
 * offline tools and tests -- it runs unthrottled, as those callers expect.
 * <p>
 * That covers only the signals the framework knows about. {@code Progress.isGlobal()} is false and a repair session's
 * own cancellation -- a coordinator timeout, {@code nodetool repair_admin cancel}, a failed participant -- never
 * reaches {@code CompactionInfo.Holder.stop()}, so the overloads taking a {@link BooleanSupplier} exist to carry it:
 * the predicate is consulted between chunk transfers and periodically during the index walk, and raises the same
 * {@link CompactionInterruptedException} a stop does, so the same cleanup runs. Without it a 400 GiB split of a
 * session that failed in its first minute would run to completion and then publish children stamped with that dead
 * session's {@code pendingRepair}.
 */
public final class ZeroCopySSTableSplitter
{
    private static final Logger logger = LoggerFactory.getLogger(ZeroCopySSTableSplitter.class);

    /** Prefix of the refusal message for an uncompressed parent, so tests need not match the whole sentence. */
    public static final String UNCOMPRESSED_UNSUPPORTED_MESSAGE =
        "ZeroCopySSTableSplitter requires a compressed sstable";

    /**
     * Prefix of the refusal message for a parent whose version cannot carry {@code StatsMetadata.hasUnindexedRegions},
     * so tests need not match the whole sentence. See the class javadoc for why this also keeps pre-{@code na}
     * versions, whose CompressionInfo.db a child would misparse, out of the split entirely.
     */
    public static final String LEGACY_VERSION_UNSUPPORTED_MESSAGE =
        "ZeroCopySSTableSplitter requires an sstable version that records hasUnindexedRegions";

    /**
     * The cancellation predicate of a split nobody can cancel: what the overloads that do not take one pass, so that
     * the check site never has to test for null and an added overload cannot forget to thread it.
     */
    private static final BooleanSupplier NEVER_CANCELLED = () -> false;

    /** One {@code transferTo} slice, small because it is also the granularity of throttling and stop checks. */
    private static final int TRANSFER_SLICE = 4 << 20;

    /** Same buffer size the digest/checksum writers use. */
    private static final int COPY_BUFFER_SIZE = 64 * 1024;

    /** Head-pad alignment; {@link Reflink#RANGE_ALIGNMENT} says why it is a constant 64 KiB, not the block size. */
    private static final long CLONE_ALIGNMENT = Reflink.RANGE_ALIGNMENT;

    /**
     * A child smaller than this is copied rather than shared: the head pad costs up to {@link #CLONE_ALIGNMENT} bytes
     * of disk plus a longer digest pass, so sharing only pays when the range dwarfs it. 1 MiB is 16 times the pad.
     */
    private static final long MIN_CLONE_BYTES = 1L << 20;

    /**
     * Test hook: lay every child out as if extent sharing were available, so the aligned layout is covered on
     * filesystems that cannot share extents (every laptop and CI box). Also lifts {@link #MIN_CLONE_BYTES}, since test
     * sstables are smaller than that. The copy mechanism is unaffected.
     */
    @VisibleForTesting
    static volatile boolean forceAlignedLayoutForTesting = false;

    /**
     * Test hook, called with the number of children already built before each one is started, so a test can fail a
     * split PARTWAY THROUGH. No file-level way exists: everything the split reads before writing anything is also read
     * by the rewrite path it falls back to, so corrupting it fails both. That fallback, and {@link #cleanUp} with
     * children already open, are otherwise unreachable.
     */
    @VisibleForTesting
    public static volatile java.util.function.IntConsumer failBeforeChildForTesting = null;

    /**
     * Bit-identical copy of the package-private {@code MetadataCollector.defaultPartitionSizeHistogram()}: a child's
     * {@code estimatedPartitionSize} must bucket the way every writer-produced sstable's does or the two cannot be
     * summed. {@code ZeroCopySplitStatsTest} pins them together.
     */
    static final int PARTITION_SIZE_HISTOGRAM_BUCKETS = 155;

    /** {@code MetadataCollector.cardinality} is {@code new HyperLogLogPlus(13, 25)} (CASSANDRA-5906). */
    static final int HLL_P = 13;
    static final int HLL_SP = 25;

    /**
     * Every component this class can write, i.e. everything {@link #cleanUp} has to remove. Both formats' index
     * components are listed unconditionally: a child is only ever written in its parent's format, so at most one
     * pair of them can exist, and removing a file that was never created is what {@code deleteIfExists} is for.
     */
    private static final List<Component> WRITTEN_COMPONENTS = ImmutableList.of(Components.DATA,
                                                                               Components.COMPRESSION_INFO,
                                                                               Components.STATS,
                                                                               Components.FILTER,
                                                                               Components.DIGEST,
                                                                               Components.TOC,
                                                                               BigFormat.Components.PRIMARY_INDEX,
                                                                               BigFormat.Components.SUMMARY,
                                                                               BtiFormat.Components.PARTITION_INDEX,
                                                                               BtiFormat.Components.ROW_INDEX);

    /**
     * {@link #WRITTEN_COMPONENTS} as a set, for the {@link #unhandledComponents} difference. Same membership, and
     * deliberately the same list: a component this class cannot write is a component {@link #cleanUp} could not
     * remove either, so the two questions have one answer.
     */
    private static final ImmutableSet<Component> HANDLED_COMPONENTS = ImmutableSet.copyOf(WRITTEN_COMPONENTS);

    private ZeroCopySSTableSplitter()
    {
    }

    // ---- Arithmetic: static and free of any sstable dependency, so it can be unit tested alone ----

    /** Chunk containing {@code uncompressedPosition}; mirrors {@code CompressionMetadata.chunkFor}. */
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
     * Last (inclusive) chunk of a child whose live bytes end at exclusive parent uncompressed offset {@code hi}.
     * {@code (hi - 1) / L}, not {@code hi / L}: on an exact boundary the final chunk is the one <em>before</em> it,
     * and {@code hi / L} would read one chunk too far, throwing EOF at the end of the file.
     */
    public static long lastChunk(long hi, int chunkLength)
    {
        checkChunkLength(chunkLength);
        if (hi <= 0)
            throw new IllegalArgumentException("child must contain at least one byte, hi=" + hi);
        return (hi - 1) / chunkLength;
    }

    /**
     * The child's {@code CompressionInfo.dataLength}: first chunk start to last live partition end, with no trailing
     * slack, since {@code getPositionsForRanges} takes {@code uncompressedLength()} as its right bound.
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
     * The whole chunk-range computation for one child, as an immutable value a test can assert on directly.
     * {@code lo} and {@code hi} are a partition start and a partition end in PARENT uncompressed space, the latter
     * exclusive.
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

        // Why a verbatim run works: the last chunk holds at least one live byte (so it is mapped and decompressed)
        // and at most a full chunk of them (so dataLength never overruns the run).
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
     * The physical half of the arithmetic; the class javadoc has the head pad it computes, which is a second,
     * PHYSICAL dead prefix independent of {@link ChunkRange#deadPrefixBytes} in uncompressed space.
     * {@code cloneLength} is the aligned part of the child's length and the remaining {@code tailLength < A} bytes are
     * copied conventionally: rounding the clone UP and truncating would work on xfs, but it would depend on truncate
     * unsharing a partially shared final block, and a sub-64-KiB copy is not worth that.
     *
     * @param copyFrom      {@code O(i)}, the parent offset of the child's first chunk
     * @param physicalBytes {@code O(j+1) - O(i)}, the child's live chunk bytes
     * @param align         whether to pad the head so that sharing is possible at all
     * @param share         whether to attempt the clone; implies {@code align}, and rejected without it. {@code align}
     *                      without {@code share} is the legal asymmetry: it is how a test produces the padded layout
     *                      on a filesystem that cannot share
     */
    public static CopyPlan copyPlan(long copyFrom, long physicalBytes, boolean align, boolean share)
    {
        if (copyFrom < 0)
            throw new IllegalArgumentException("negative copyFrom: " + copyFrom);
        if (physicalBytes <= 0)
            throw new IllegalArgumentException("non-positive physicalBytes: " + physicalBytes);
        // Without the pad, srcStart is O(i), which is aligned to nothing; a plan with cloneLength > 0 over an
        // unaligned srcStart is one Reflink.tryCloneRange rejects as a caller bug with IllegalArgumentException --
        // before it consults isPossibleIn, so there is no fall-through to the copy -- and the split dies. Refuse to
        // build such a plan at all rather than let the combination reach the ioctl.
        if (share && !align)
            throw new IllegalArgumentException("share requires align: an unaligned srcStart cannot be cloned");

        long pad = align ? copyFrom & (CLONE_ALIGNMENT - 1) : 0;
        long childLength = pad + physicalBytes;
        // Aligned DOWN, so the clone can never reach past the child's last live byte into the parent's trailing
        // slack -- which chunkEnd() exists to keep out of the child.
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

        /** Bytes that have to be transferred conventionally: {@code childLength mod A}, or all of it if no clone. */
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

    /** {@link #chunkRange} result: chunk indices are into the PARENT's offsets array, byte counts the child's own. */
    public static final class ChunkRange
    {
        /** First live byte of the child, inclusive, in parent uncompressed space. */
        public final long lo;
        /** Last live byte of the child + 1, exclusive, in parent uncompressed space. */
        public final long hi;
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

    // ---- Results ----

    /**
     * Repair state to stamp into one child's Statistics.db, instead of inheriting the parent's. Written by
     * {@link #writeStatistics} <em>before</em> the child reader is opened, so no {@code mutateRepairedAndReload} is
     * needed. The invariants enforced here are the ones {@code CompactionStrategyHolder.managesRepairedGroup} and
     * {@code PendingRepairHolder.managesRepairedGroup} assert when the Tracker routes a newly visible sstable to a
     * strategy holder; violating them means an IllegalArgumentException from inside a Tracker notification.
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

        /** What the overloads without an explicit state use: the parent's, verbatim and deliberately UNVALIDATED. */
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
        /** {@code i} and {@code j}, both inclusive; see {@link ChunkRange} for these five derived numbers. */
        public final long firstChunk;
        public final long lastChunk;
        /** Live chunk bytes, {@code O(j+1) - O(i)}, EXCLUDING any head pad; the file is {@link #onDiskLength()}. */
        public final long physicalBytes;
        public final long dataLength;
        public final long shift;
        public final long deadPrefixBytes;
        /** Its {@code offsets[0]}: bytes of the parent's PREVIOUS chunk at the head, zero unless meant to be shared. */
        public final long headPadBytes;
        /** Bytes of {@link #physicalBytes} that were shared with the parent instead of copied. */
        public final long clonedBytes;
        public final long partitionCount;
        /** Components written for the child; the exact set passed to {@code SSTableReader.open}. */
        public final Set<Component> components;
        /** Stamped state, carried with the boundary range so an empty range cannot shift the pairing. */
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
         * Sum of every child's live chunk bytes: what the split accounted for, NOT what it moved.
         * {@link #totalBytesWritten()} is that, and adds the head pads before subtracting the clones -- a cloned range
         * spans its child's pad too, so subtracting clones alone omits the pads and can go negative.
         */
        public final long totalPhysicalBytesCopied;
        public final long totalDeadPrefixBytes;
        /** Sum of every child's head pad, i.e. the disk space alignment cost. */
        public final long totalHeadPadBytes;
        /**
         * Bytes shared as copy-on-write extents rather than copied, costing neither I/O nor disk. Anywhere between
         * zero and (physical + pad), and NOT a fixed fraction of it: a child under {@link #MIN_CLONE_BYTES} is
         * planned unaligned and never cloned at all, and a clone that fails part way through a split poisons
         * {@code Reflink}'s per-filesystem cache so the remaining children are copied too. Read it as "what actually
         * happened", not as a bound.
         */
        public final long totalBytesCloned;
        /** Bytes in two children from a boundary inside a chunk: one chunk per boundary, free if shared. */
        public final long duplicatedChunkBytes;
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

    // ---- Compaction-framework participation ----

    /**
     * Makes one split a first-class member of the compaction framework, so a verbatim byte copy is not an invisible,
     * unbounded burst of I/O. Stopping (TRUNCATE, DROP, {@code nodetool stop ANTICOMPACTION},
     * {@code runWithCompactionsDisabled}) walks {@code active.getCompactions()} and calls
     * {@link CompactionInfo.Holder#stop()}, matching on the parent sstable this {@link CompactionInfo} carries.
     * <p>
     * A verbatim chunk copy has no partition boundary to stop cleanly at, so the stop check lives inside the transfer
     * loop and aborts the split outright: {@link CompactionInterruptedException} propagates out of {@link #split}, the
     * transaction is aborted and every child is deleted. A caller must NOT treat that as a reason to fall back to the
     * rewrite path -- the operator asked for the work to stop, not to be done a more expensive way.
     * <p>
     * {@code total} is deliberately an estimate, and one that a split can OVERSHOOT: a boundary chunk lands in two
     * children, an aligned child carries a head pad, and both are counted as they move, while {@code total} is only
     * {@code passes * parent.onDiskLength()}. The reported figure is therefore clamped to {@code total} -- not for
     * tidiness in {@code nodetool compactionstats} but because
     * {@code CompactionInfo.estimatedRemainingWriteToDiskBytes} scales {@code total - completed}, and a negative
     * remainder would credit phantom free space to the disk-space checks {@code CompactionTask} and
     * {@code StreamSession} make against {@code ActiveCompactions}. The digest pass is counted only when
     * {@code zero_copy_split_digest_enabled} says it will happen, else a split would peg at 50% and finish; that flag
     * is read ONCE here and reused for every child, so a mid-split flip can neither desynchronise this figure nor
     * give siblings different component sets. SHARED bytes count towards it too -- otherwise a reflink split stalls
     * at 50% -- but are not rate limited; see {@link #cloned}.
     */
    public static final class Progress extends CompactionInfo.Holder
    {
        private final TableMetadata metadata;
        private final Set<SSTableReader> parent;
        private final long total;
        /** The parent's physical length: one copy of the data, i.e. the most a split can write. */
        private final long physicalBytes;
        private final TimeUUID id;
        private final AtomicLong completed = new AtomicLong();
        private final RateLimiter limiter;
        /**
         * {@code zero_copy_split_digest_enabled} as it was when this split started.
         * {@code ZeroCopySSTableSplitter.build} takes the decision from here rather than re-reading the config per
         * child, so {@code total} above and the children's component sets are answers to the same question.
         */
        final boolean digestEnabled;

        private Progress(SSTableReader parent, RateLimiter limiter)
        {
            this.metadata = parent.metadata();
            this.parent = ImmutableSet.of(parent);
            this.physicalBytes = parent.onDiskLength();
            this.digestEnabled = DatabaseDescriptor.getZeroCopySplitDigestEnabled();
            int passes = digestEnabled ? 2 : 1;
            this.total = passes * physicalBytes;
            this.id = TimeUUID.Generator.nextTimeUUID();
            this.limiter = limiter;
        }

        @Override
        public CompactionInfo getCompactionInfo()
        {
            // `totalCompressed` is not a second progress figure: CompactionInfo.estimatedRemainingWriteToDiskBytes
            // turns it into a WRITE RESERVATION of (totalCompressed / total) * remaining, which ActiveCompactions sums
            // per data directory and which StreamSession.checkAvailableDiskSpaceAndCompactions and
            // CompactionTask.buildCompactionCandidatesForAvailableDiskSpace subtract from free space. The parent's
            // physical length reserves one copy of the data however many passes `total` counts, the true worst case;
            // passing `total` would reserve twice that for an operation whose only guaranteed write is a ~10-byte
            // digest, so a 500 GiB split would reserve a terabyte and reject the very bootstraps this path exists for.
            // It deliberately over-reserves where extents CAN be shared, a clone's success not being knowable here:
            // over-reserving only delays other work, under-reserving invites ENOSPC.
            //
            // The clamp is what keeps that reservation non-negative; see the class javadoc for why `completed` can
            // pass `total` in the first place, and why a negative remainder is worse than a stuck 100%.
            return new CompactionInfo(metadata, OperationType.ANTICOMPACTION, Math.min(completed.get(), total), total,
                                      physicalBytes, id, parent);
        }

        /** One sstable of one table, so a paused global compaction must not silently stop it. */
        @Override
        public boolean isGlobal()
        {
            return false;
        }

        /**
         * Called BEFORE {@code bytes} move, and only for bytes that really do move: a short {@code transferTo}
         * over-throttles by at most one slice. Deliberately NOT a stop check as well -- the caller has just made one
         * through {@code ZeroCopySSTableSplitter.checkInterrupted}, which also answers the caller's own cancellation
         * predicate, and two paths to the same exception is how one of them ends up being the only one a new call
         * site uses.
         */
        void throttle(int bytes)
        {
            if (bytes > 0)
                limiter.acquire(bytes);
        }

        void afterSlice(long bytes)
        {
            completed.addAndGet(bytes);
        }

        /** Raises {@link CompactionInterruptedException} iff the compaction framework has asked this split to stop. */
        public void checkStopped()
        {
            if (isStopRequested())
                throw new CompactionInterruptedException(getCompactionInfo());
        }

        /**
         * Bytes shared rather than moved. Deliberately NOT rate limited -- {@code compaction_throughput} bounds disk
         * traffic and a clone makes none -- but still counted in {@code total}, so progress reaches 100%.
         */
        void cloned(long bytes)
        {
            completed.addAndGet(bytes);
        }
    }

    /** Caller-owned: register with {@code CompactionManager.active.beginCompaction}, then finish it after. */
    public static Progress progressFor(SSTableReader parent, RateLimiter limiter)
    {
        Preconditions.checkNotNull(parent, "parent");
        Preconditions.checkNotNull(limiter, "limiter");
        return new Progress(parent, limiter);
    }

    /**
     * The one interruption check of a split, composing the two independent signals that can stop it: the compaction
     * framework's stop flag, which only exists when the caller supplied a {@link Progress}, and the caller's own
     * cancellation predicate, which is how a repair session's failure gets in (see the class javadoc). Both raise the
     * same {@link CompactionInterruptedException}, so {@link #build}'s cleanup applies to either unchanged.
     * <p>
     * Every long-running loop in a split calls this and nothing else: the transfer loop between chunk transfers, the
     * digest read, the index walks and each format's index writer. Passed as a {@code Runnable} to the walks and to
     * {@link ZeroCopySplitIndex}, which is why the two signals are bound together here rather than at each call site.
     */
    private static void checkInterrupted(Progress progress, BooleanSupplier isCancelled)
    {
        if (progress != null)
            progress.checkStopped();
        if (isCancelled.getAsBoolean())
        {
            // Without a Progress there is no CompactionInfo to name; the message is all the framework would print.
            throw new CompactionInterruptedException(progress != null ? progress.getCompactionInfo()
                                                                     : "cancelled zero-copy sstable split");
        }
    }

    // ---- Entry points ----

    /**
     * @return true iff this is a normally-opened, compressed BIG- or BTI-format sstable with no compression dictionary
     *         and a version that can carry {@code StatsMetadata.hasUnindexedRegions}
     */
    public static boolean isSupported(SSTableReader parent)
    {
        // getCompressionMetadata() throws when the sstable is not compressed, so the order of these matters.
        return isSupportedFormat(parent.descriptor.getFormat())
               && parent.descriptor.version.hasUnindexedRegionsMarker()
               && parent.openReason != SSTableReader.OpenReason.MOVED_START
               && parent.compression
               && parent.getCompressionMetadata().compressionDictionary() == null;
    }

    /**
     * Whether this format's index can be rebased onto a copied chunk run at all. Split out so a caller that wants
     * to report an unsupported FORMAT separately from an unsupported compression setting -- which
     * {@code AntiCompactionRunPlanner} does -- does not have to keep its own copy of the list.
     */
    public static boolean isSupportedFormat(SSTableFormat<?, ?> format)
    {
        return BigFormat.is(format) || BtiFormat.is(format);
    }

    /**
     * The parent's components that a child would NOT get, because this class does not know how to produce them.
     * Empty for every sstable the split is meant for.
     * <p>
     * A child is assembled from a fixed list ({@link #WRITTEN_COMPONENTS}) rather than from the parent's own set, and
     * {@code TOCComponent.updateTOC} writes only that list, so a parent component outside it is silently absent from
     * every child. For storage-attached index components that absence is not benign: the child is live and readable
     * and its rows are invisible to every index predicate, because {@code SSTableContextManager.update} drops an
     * sstable with no per-sstable completion marker out of the index view without reporting it invalid. Refusing is
     * free -- the caller falls back to the rewrite, which builds indexes inline -- so this refuses on ANY unknown
     * component rather than on a list of known-dangerous ones, and stays correct for components added later.
     * <p>
     * This is a BACKSTOP, not the primary gate. It can only see what the reader's component set holds, and a parent
     * with no TOC.txt gets its set from {@code Descriptor.discoverComponents}, which enumerates only the format's
     * singleton components and is therefore structurally blind to the per-index-named SAI ones. Ask
     * {@code SecondaryIndexManager.hasSSTableAttachedIndexes()} wherever a {@code ColumnFamilyStore} is in hand;
     * this covers the callers that have only a reader, and any future component type nobody has thought about yet.
     */
    public static Set<Component> unhandledComponents(SSTableReader parent)
    {
        Preconditions.checkNotNull(parent, "parent");
        return Sets.difference(parent.getComponents(), HANDLED_COMPONENTS);
    }

    /**
     * Split at the partition boundaries nearest to {@code numChildren} approximately-equal byte shares of the parent's
     * uncompressed length.
     *
     * @param numChildren must be >= 1 and <= the parent's partition count
     * @param txn         optional; every child is {@code trackNew}'d on it once fully written
     */
    public static Result split(SSTableReader parent, int numChildren, LifecycleTransaction txn)
    {
        return split(parent, numChildren, txn, null);
    }

    /** As {@link #split(SSTableReader, int, LifecycleTransaction)}, but interruptible and throttled; see
     *  {@link Progress}. */
    public static Result split(SSTableReader parent, int numChildren, LifecycleTransaction txn, Progress progress)
    {
        return split(parent, numChildren, txn, progress, NEVER_CANCELLED);
    }

    /**
     * As {@link #split(SSTableReader, int, LifecycleTransaction, Progress)}, but also cancellable by a signal the
     * compaction framework knows nothing about; see {@link #checkInterrupted} and the class javadoc.
     *
     * @param isCancelled consulted between chunk transfers and periodically during the index walk; when it turns true
     *                    the split raises {@link CompactionInterruptedException} and deletes every child, exactly as
     *                    a {@code nodetool stop} does
     */
    public static Result split(SSTableReader parent, int numChildren, LifecycleTransaction txn, Progress progress,
                               BooleanSupplier isCancelled)
    {
        Preconditions.checkArgument(numChildren >= 1, "numChildren must be >= 1, got %s", numChildren);
        Preconditions.checkNotNull(isCancelled, "isCancelled");
        requireSupported(parent);

        long start = Clock.Global.nanoTime();
        Runnable interrupt = () -> checkInterrupted(progress, isCancelled);
        // Three passes over Index.db, none retaining anything per partition: count, select, build. Counting first is
        // what lets selection stay O(numChildren) in heap -- it needs the exact partition count up front for its
        // tail-room clamp. See RunSelector.
        int partitionCount = countPartitions(parent, interrupt);
        if (numChildren > partitionCount)
            throw new IllegalArgumentException("cannot split " + partitionCount + " partitions into " +
                                               numChildren + " children");
        Runs runs = selectByByteShare(parent, numChildren, partitionCount, interrupt);
        return build(parent, runs, null, txn, progress, interrupt, start);
    }

    /**
     * Split at explicit, strictly increasing boundaries. Child {@code b} covers keys
     * {@code [boundaries[b-1], boundaries[b])}, with the first child unbounded below and the last unbounded above. A
     * boundary range containing no partition produces no child -- an empty sstable is not representable
     * ({@code IndexSummaryBuilder.build} asserts a non-zero key count, and
     * {@code SSTableReader.getPositionsForBounds} silently returns null once {@code left >= right}, so every range
     * query would skip it) -- so the result may hold fewer than {@code boundaries.size() + 1} children.
     *
     * @param txn optional; every child is {@code trackNew}'d on it once fully written
     */
    public static Result split(SSTableReader parent, List<DecoratedKey> boundaries, LifecycleTransaction txn)
    {
        return split(parent, boundaries, null, txn);
    }

    /**
     * Split at explicit boundaries, stamping a caller-supplied repair state into each child instead of inheriting the
     * parent's. Boundary semantics are those of {@link #split(SSTableReader, List, LifecycleTransaction)}.
     * <p>
     * <b>Pairing.</b> An empty boundary range still produces no child, so {@code result.children.size()} may be
     * smaller than {@code perChild.size()}; the state is therefore <em>carried</em> with the range rather than
     * re-derived positionally afterwards, and what was written is exposed on {@link Child#repairState}. Positional
     * pairing of {@code children} against {@code perChild} is only valid when every range is known to be non-empty.
     *
     * @param perChild one state per boundary range, exactly {@code boundaries.size() + 1} entries in range order;
     *                 may be null to inherit the parent's state for every child
     * @param txn      optional; if non-null every child is {@code trackNew}'d on it once fully written

     */
    public static Result split(SSTableReader parent,
                               List<DecoratedKey> boundaries,
                               List<RepairState> perChild,
                               LifecycleTransaction txn)
    {
        return split(parent, boundaries, perChild, txn, null);
    }

    /** As {@link #split(SSTableReader, List, List, LifecycleTransaction)}, but interruptible and throttled against
     *  {@code compaction_throughput}. */
    public static Result split(SSTableReader parent,
                               List<DecoratedKey> boundaries,
                               List<RepairState> perChild,
                               LifecycleTransaction txn,
                               Progress progress)
    {
        return split(parent, boundaries, perChild, txn, progress, NEVER_CANCELLED);
    }

    /**
     * As {@link #split(SSTableReader, List, List, LifecycleTransaction, Progress)}, but also cancellable by a signal
     * the compaction framework knows nothing about. The overload the anticompaction path uses: a repair session that
     * fails or is cancelled reaches {@code CompactionInfo.Holder.stop()} through nothing, and without this a split
     * would run for hours and then publish children stamped with a FAILED session's {@code pendingRepair}. See
     * {@link #checkInterrupted} and the class javadoc.
     *
     * @param isCancelled consulted between chunk transfers and periodically during the index walk; when it turns true
     *                    the split raises {@link CompactionInterruptedException} and deletes every child, exactly as
     *                    a {@code nodetool stop} does
     */
    public static Result split(SSTableReader parent,
                               List<DecoratedKey> boundaries,
                               List<RepairState> perChild,
                               LifecycleTransaction txn,
                               Progress progress,
                               BooleanSupplier isCancelled)
    {
        Preconditions.checkNotNull(boundaries, "boundaries");
        Preconditions.checkNotNull(isCancelled, "isCancelled");
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
        Runnable interrupt = () -> checkInterrupted(progress, isCancelled);
        // Two passes: the run starts fall out of the same walk that resolves the boundaries, so no counting pass.
        Runs runs = selectByBoundaries(parent, boundaries, interrupt);
        return build(parent, runs, perChild, txn, progress, interrupt, start);
    }

    private static void requireSupported(SSTableReader parent)
    {
        Preconditions.checkNotNull(parent, "parent");
        if (!isSupportedFormat(parent.descriptor.getFormat()))
            throw new UnsupportedOperationException("ZeroCopySSTableSplitter supports the BIG and BTI sstable " +
                                                    "formats, got " + parent.descriptor.getFormat().name() +
                                                    ". The technique is to copy Data.db chunks verbatim and " +
                                                    "rebase every position that points into them, which needs an " +
                                                    "index whose partition positions can be found and rewritten " +
                                                    "without deserialising a row.");
        // Before the compression checks, because it is the cheapest and the one an operator is most likely to hit:
        // every sstable on a node that has not run upgradesstables since this feature landed fails it.
        if (!parent.descriptor.version.hasUnindexedRegionsMarker())
            throw new UnsupportedOperationException(LEGACY_VERSION_UNSUPPORTED_MESSAGE + ": " + parent.descriptor +
                                                    " is version '" + parent.descriptor.version.version +
                                                    "'. A child inherits its parent's version, and a version " +
                                                    "without the marker is also a version whose CompressionInfo.db " +
                                                    "reader may not match the writer -- maxCompressedLength is " +
                                                    "written unconditionally but only read from 'na' onwards, so a " +
                                                    "pre-'na' child would misparse its own dataLength into its " +
                                                    "chunk count and either OOM allocating the offsets table or " +
                                                    "throw CorruptSSTableException. Nothing is wrong with this " +
                                                    "sstable: zero-copy splitting is simply inert for it until " +
                                                    "nodetool upgradesstables has rewritten it (and, if " +
                                                    "storage_compatibility_mode pins the version being written, " +
                                                    "until that is cleared too).");
        if (parent.openReason == SSTableReader.OpenReason.MOVED_START)
            throw new UnsupportedOperationException("cannot split " + parent.descriptor +
                                                    ": it is open as MOVED_START. cloneWithNewStart moves the " +
                                                    "reader's first key past data that is still in Data.db and " +
                                                    "still in the index, so the first child would be cut at a " +
                                                    "position covering partitions the parent no longer claims.");
        if (!parent.compression)
            throw new UnsupportedOperationException(UNCOMPRESSED_UNSUPPORTED_MESSAGE + ": " + parent.descriptor +
                                                    " has no CompressionInfo.db. An uncompressed split is a " +
                                                    "different algorithm -- the cut is exact rather than " +
                                                    "chunk-aligned, and CRC.db (whose 64KiB grid is addressed " +
                                                    "from origin 0) has to be regenerated wholesale rather " +
                                                    "than sliced. Refusing rather than emitting a child with " +
                                                    "a misaligned CRC.db.");
        // isSupported() checks this too, and a direct split() call must not be the one path that skips it: a
        // dictionary-compressed child's chunks are copied verbatim while its CompressionInfo.db is written afresh,
        // and a wrong answer there is undecompressible data. Ordered after the compression test, since
        // getCompressionMetadata() throws on an uncompressed sstable.
        if (parent.getCompressionMetadata().compressionDictionary() != null)
            throw new UnsupportedOperationException("cannot split " + parent.descriptor +
                                                    ": it is compressed with a compression dictionary, which this " +
                                                    "path has not been shown to round trip. See the matching " +
                                                    "refusal in ZeroCopySSTableSlice.planCompressed.");
        if (!parent.descriptor.fileFor(Components.STATS).exists())
            throw new IllegalStateException("parent has no Statistics.db: " + parent.descriptor +
                                            "; MetadataSerializer would silently fabricate defaults");
        Set<Component> unhandled = unhandledComponents(parent);
        if (!unhandled.isEmpty())
            throw new UnsupportedOperationException("cannot split " + parent.descriptor + ": it carries " + unhandled +
                                                   ", which this class cannot produce for a child. A child would be" +
                                                   " live and readable with those components simply absent -- for" +
                                                   " storage-attached index components that means its rows answer no" +
                                                   " index predicate, silently. Callers that can fall back to a" +
                                                   " rewrite should ask AntiCompactionRunPlanner (or" +
                                                   " SecondaryIndexManager.hasSSTableAttachedIndexes) first.");
    }

    // ---- Walking the parent Index.db ----

    /** Receives every partition of the parent, in on-disk (token) order. */
    public interface IndexRecordConsumer
    {
        void accept(int index, ByteBuffer key, long position);
    }

    /**
     * One sequential walk of the parent Index.db, retaining nothing. Deliberately does not hand back the positions: a
     * {@code long[]} of every partition's Data.db offset costs 8 bytes per partition (16-24 at the peak of the doubling
     * and trim), i.e. tens of gigabytes of heap for a terabyte of 1 KiB partitions, and every access turned out to be
     * sequential anyway.
     *
     * @return the exact number of records
     */
    public static int walkIndex(SSTableReader parent, IndexRecordConsumer consumer)
    {
        return walkIndex(parent, consumer, null);
    }

    /**
     * @param stopCheck optional; run every 1024 records and expected to raise {@link CompactionInterruptedException}.
     *                  A {@code Runnable} rather than the {@link Progress} itself because a walk moves no Data.db
     *                  bytes and so has nothing to throttle, and because the caller's cancellation predicate has to
     *                  be answered here too -- {@link #checkInterrupted} composes both.
     */
    private static int walkIndex(SSTableReader parent, IndexRecordConsumer consumer, Runnable stopCheck)
    {
        return BtiFormat.is(parent.descriptor.getFormat()) ? walkBtiIndex(parent, consumer, stopCheck)
                                                          : walkBigIndex(parent, consumer, stopCheck);
    }

    private static int walkBigIndex(SSTableReader parent, IndexRecordConsumer consumer, Runnable stopCheck)
    {
        long count = 0;
        // A buffered reader rather than an mmap, so no record can straddle a mapping boundary.
        File index = parent.descriptor.fileFor(BigFormat.Components.PRIMARY_INDEX);
        try (RandomAccessReader in = RandomAccessReader.open(index))
        {
            long indexSize = in.length();
            while (in.getFilePointer() != indexSize)
            {
                // Unthrottled (this moves no data) but interruptible: a stop noticed only once the first child byte
                // moved is what let truncateBlocking exhaust its wait and report success with the data still there.
                if (stopCheck != null && (count & 0x3FF) == 0)
                    stopCheck.run();

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
            throw new CorruptSSTableException(e, index);
        }

        if (count == 0)
            throw new IllegalStateException("parent Index.db is empty: " + parent.descriptor);

        return (int) count;
    }

    /**
     * The BTI form. Same contract, different sources: the positions come out of the Partitions.db trie and the keys
     * out of Rows.db or Data.db. This is the pass that makes a BTI split cost a decompressing read of the parent's
     * data when its partitions have no row indexes -- see {@link BtiZeroCopySplit} for why, and for why there is
     * nowhere else to get the keys from.
     */
    private static int walkBtiIndex(SSTableReader parent, IndexRecordConsumer consumer, Runnable stopCheck)
    {
        long count = 0;
        try (BtiZeroCopySplit.Cursor cursor = BtiZeroCopySplit.cursor(parent))
        {
            while (cursor.advance())
            {
                // Same 1-in-1024 stop check the BIG walk does, and it matters more here: this walk can decompress
                // Data.db, so it is the slower of the two and the one most worth interrupting.
                if (stopCheck != null && (count & 0x3FF) == 0)
                    stopCheck.run();

                if (count >= Integer.MAX_VALUE)
                    throw new IllegalStateException("parent has more than Integer.MAX_VALUE partitions, which " +
                                                    "run starts cannot address: " + parent.descriptor);
                consumer.accept((int) count++, cursor.key(), cursor.dataPosition());
            }
        }
        catch (IOException e)
        {
            throw new CorruptSSTableException(e, parent.descriptor.fileFor(BtiFormat.Components.PARTITION_INDEX));
        }

        if (count == 0)
            throw new IllegalStateException("parent Partitions.db is empty: " + parent.descriptor);

        return (int) count;
    }

    /** Receives every partition's Data.db position, in on-disk order, and none of the keys. */
    public interface PositionConsumer
    {
        void accept(int index, long position);
    }

    /**
     * {@link #walkIndex} for the two passes that never look at a key: counting the parent's partitions, and
     * choosing split points by byte share.
     *
     * <p>For BIG this is the same read either way, since an Index.db record carries the key whether anybody wants
     * it or not. For BTI it is the difference between touching Data.db and not: {@code BtiZeroCopySplit.Cursor}
     * resolves a key only when asked, and the key of a partition with no row index is only in Data.db. So a
     * byte-share split of a narrow BTI table decompresses in the build pass and in no other.
     */
    private static int walkPositions(SSTableReader parent, PositionConsumer consumer, Runnable stopCheck)
    {
        if (!BtiFormat.is(parent.descriptor.getFormat()))
            return walkBigIndex(parent, (index, key, position) -> consumer.accept(index, position), stopCheck);

        long count = 0;
        try (BtiZeroCopySplit.Cursor cursor = BtiZeroCopySplit.cursor(parent))
        {
            while (cursor.advance())
            {
                if (stopCheck != null && (count & 0x3FF) == 0)
                    stopCheck.run();

                if (count >= Integer.MAX_VALUE)
                    throw new IllegalStateException("parent has more than Integer.MAX_VALUE partitions, which " +
                                                    "run starts cannot address: " + parent.descriptor);
                consumer.accept((int) count++, cursor.dataPosition());
            }
        }
        catch (IOException e)
        {
            throw new CorruptSSTableException(e, parent.descriptor.fileFor(BtiFormat.Components.PARTITION_INDEX));
        }

        if (count == 0)
            throw new IllegalStateException("parent Partitions.db is empty: " + parent.descriptor);

        return (int) count;
    }

    /** Just the record count, for the byte-share form, whose selection needs it before it can start. */
    private static int countPartitions(SSTableReader parent, Runnable stopCheck)
    {
        return walkPositions(parent, (index, position) -> {}, stopCheck);
    }

    // ---- Split-point selection: run b is [runStarts[b], runStarts[b+1]), terminator partitionCount ----

    /**
     * Where each child's run of index records begins, and the parent Data.db offset of that first record.
     * {@link #build} needs a run's {@code lo} before it can copy that child's chunks, so these cannot be recovered
     * during the build pass -- but there are only ever {@code numChildren} of them.
     */
    @VisibleForTesting
    static final class Runs
    {
        final int[] runStarts;
        /** Data.db offset of record {@code runStarts[b]}; meaningless (and unread) for an empty trailing run. */
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
     * The explicit-boundary form: the run starts fall out of the same walk that compares keys against the boundaries,
     * so one pass, and the keys are never retained (~150 bytes of heap per partition on a wide sstable).
     */
    private static Runs selectByBoundaries(SSTableReader parent, List<DecoratedKey> boundaries, Runnable stopCheck)
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
        }, stopCheck);

        // Boundaries past the parent's last key produce trailing empty runs whose offsets stay UNRESOLVED and unread:
        // build() skips a run with from >= to, and the last non-empty run takes its hi from dataLength precisely
        // because the run after it starts at partitionCount.
        while (nextBoundary[0] < boundaries.size())
            runStarts[++nextBoundary[0]] = count;

        return new Runs(runStarts, runPositions, count);
    }

    /** The byte-share form: one pass, driving {@link RunSelector}. */
    private static Runs selectByByteShare(SSTableReader parent, int numChildren, int partitionCount,
                                          Runnable stopCheck)
    {
        RunSelector selector = new RunSelector(parent.uncompressedLength(), numChildren, partitionCount);
        int count = walkPositions(parent, selector::offer, stopCheck);
        if (count != partitionCount)
            throw new IllegalStateException("parent Index.db grew or shrank between passes: counted " +
                                            partitionCount + ", then " + count + ": " + parent.descriptor);
        return selector.finish();
    }

    /**
     * Streaming form of {@link #chooseByByteShare}, which it is differentially tested against: same run starts, plus
     * each run's first offset, in O(numChildren) heap rather than O(partitions). A forward scan with one record of
     * lookback suffices because the two clamps that made the array version need random access each reach only a
     * bounded distance -- tail room only into the last {@code numChildren} offsets, kept in {@link #tail}, and
     * non-emptiness at most one record past the cursor, whose offset is then filled in by a later {@link #offer}
     * (those deferrals are contiguous, so one pointer tracks them).
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
            // Targets the scan never reached: the cursor is at partitionCount, which the tail clamp pulls back to a
            // real record. position and target go unread, the snap-back being guarded on candidate < partitionCount.
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
            // ... and always leave room for the runs still to be placed. This can only pull the candidate back into
            // the tail window, never below the clamp above, because runStarts[m - 1] is itself bounded by
            // partitionCount - (numChildren - (m - 1)).
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
     * Reference implementation of split-point selection, which {@link RunSelector} is asserted to agree with for
     * randomised inputs. Not used in production: it needs every partition's offset at once.
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

    // ---- Pass 2: build every child from a single sequential walk of the parent Index.db ----

    private static Result build(SSTableReader parent, Runs runs,
                                List<RepairState> perRun, LifecycleTransaction txn, Progress progress,
                                Runnable interrupt, long startNanos)
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

        // MORE entries than the data needs is allowed: a compaction-produced sstable carries one extra
        // zero-uncompressed-length chunk, because SSTableRewriter.doPrepare syncs the data file twice
        // (switchWriter(null) -> openFinalEarly() -> dataFile.sync(), then prepareToCommit() -> syncInternal()) and
        // CompressedSequentialWriter.flushData appends a chunk even on an empty buffer; keeping those bytes out of a
        // child is chunkEnd()'s job. FEWER means the parent's CompressionInfo.db disagrees with its own dataLength.
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

        // Read ONCE per split, not once per child: a flip of zero_copy_split_digest_enabled part way through would
        // otherwise leave siblings with different component sets, and desynchronise the `passes` Progress already
        // committed to when it computed its total. The Progress made that decision first, so it is the authority
        // wherever there is one -- the same way zero_copy_anticompaction_enabled is read once per group.
        boolean digestEnabled = progress != null ? progress.digestEnabled
                                                : DatabaseDescriptor.getZeroCopySplitDigestEnabled();

        List<Child> children = new ArrayList<>(runStarts.length);
        List<Descriptor> created = new ArrayList<>(runStarts.length);
        long physicalTotal = 0;
        long deadTotal = 0;
        long padTotal = 0;
        long clonedTotal = 0;
        long duplicated = 0;

        boolean success = false;
        // The try-with-resources is nested inside a plain try so that `success` can be set AFTER the index writer has
        // been closed. Set inside the resource block it would already be true when close() threw -- and BTI's
        // Cursor.close() ends in Throwables.maybeFail over six resources -- so cleanUp() would be skipped and the
        // caller would be handed, and would commit, K fully-formed children with valid Statistics.db plus leaked
        // reader references: a permanent duplication of the parent's data.
        try
        {
            try (ZeroCopySplitIndex indexWriter = ZeroCopySplitIndex.create(parent))
            {
                ChunkRange previous = null;
                for (int b = 0; b < runStarts.length; b++)
                {
                    int from = runStarts[b];
                    int to = (b + 1 < runStarts.length) ? runStarts[b + 1] : partitionCount;
                    if (from >= to)
                        continue;  // empty boundary range -> no child

                    long lo = runs.runPositions[b];
                    // The next run starts where this one's data ends; for the last run that is the end of the parent's
                    // data. An empty trailing run has runStarts == partitionCount, exactly the case that takes
                    // dataLength, so its UNRESOLVED offset is never read.
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

                    // Never re-derived positionally: an empty range above produced no child and must not shift the
                    // state of the ranges after it.
                    RepairState repairState = perRun == null ? inherited : perRun.get(b);

                    Descriptor child = descriptors.get();
                    created.add(child);
                    if (failBeforeChildForTesting != null)
                        failBeforeChildForTesting.accept(children.size());
                    Child built = buildChild(parent, child, indexWriter, from, to, range, meta, copyFrom,
                                             physicalBytes, parentMetadata, parentStats, repairState, digestEnabled,
                                             txn, progress, interrupt);
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
                cleanUp(txn, children, created);
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
     * Derived from the chunk itself, deliberately NEVER from the physical file length.
     * {@link CompressionMetadata.Chunk#length} excludes the checksum, so {@code offset + length + 4} is exactly where
     * the next chunk starts -- which {@code chunkFor} resolves from the offsets table, or from
     * {@code compressedFileLength} for the final entry.
     * <p>
     * Deriving it as the start of chunk {@code k + 1} instead, with a chunk count of
     * {@code ceil(dataLength / chunkLength)} and {@code compressedFileLength} substituted at that count, silently
     * corrupted the last child of a <em>compaction-produced</em> parent: that count is exact for a child (the
     * {@code (C-1)*L < Dp <= C*L} invariant in {@link #chunkRange} forces {@code ceil(Dp/L) == C}) but one short for
     * such a parent, whose extra zero-uncompressed-length chunk (see the note in {@link #build}) leaves
     * {@code compressedFileLength} 9-ish bytes past the last real chunk. The last child then copied that slack, its
     * own last chunk's derived length grew by the same amount, and every read of its final chunk failed the inline
     * CRC32 -- or, past {@code maxCompressedLength}, returned compressed bytes as row data. Digest.crc32 cannot catch
     * it, being computed over whatever bytes were written, and no test saw it: a <em>flushed</em> parent calls
     * {@code flushData} exactly once.
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
                                    ZeroCopySplitIndex indexWriter,
                                    int from,
                                    int to,
                                    ChunkRange range,
                                    CompressionMetadata meta,
                                    long copyFrom,
                                    long physicalBytes,
                                    Map<MetadataType, MetadataComponent> parentMetadata,
                                    StatsMetadata parentStats,
                                    RepairState repairState,
                                    boolean digestEnabled,
                                    LifecycleTransaction txn,
                                    Progress progress,
                                    Runnable interrupt) throws IOException
    {
        TableMetadata metadata = parent.metadata();
        int chunkLength = range.chunkLength;
        int partitionCount = to - from;

        // The format-independent components. The index components come back from the index pass below, and DIGEST
        // and FILTER are added only if they are actually written; the set handed to SSTableReader.open and to
        // TOCComponent.updateTOC has to name the files that exist and no others.
        Set<Component> components = Sets.newHashSet(Components.DATA,
                                                    Components.COMPRESSION_INFO,
                                                    Components.STATS);

        // ---------- Data.db: verbatim compressed chunk run, shared with the parent where possible ----------
        // An unpadded run cannot be shared at all -- O(i) is aligned to nothing -- so the padding decision has to
        // be made before the copy, not after it fails; hence the filesystem is asked up front.
        boolean canShare = DatabaseDescriptor.getZeroCopySplitReflinkEnabled()
                           && Reflink.isPossibleIn(child.directory);
        boolean align = forceAlignedLayoutForTesting || (canShare && physicalBytes >= MIN_CLONE_BYTES);
        CopyPlan plan = copyPlan(copyFrom, physicalBytes, align, align && canShare);
        long cloned = copyData(parent.descriptor.fileFor(Components.DATA), child.fileFor(Components.DATA),
                               child.directory, plan, progress, interrupt);
        long actual = child.fileFor(Components.DATA).length();
        if (actual != plan.childLength)
            throw new IllegalStateException("child Data.db is " + actual + " bytes, expected exactly " +
                                            plan.childLength + " (trailing slack corrupts the last chunk's" +
                                            " length)");

        // ---------- CompressionInfo.db: same params, rebased offsets, offsets[0] == headPadBytes ----------
        writeCompressionInfo(child, meta, range, plan);

        // ---------- The index, plus everything else derived from the keys, in one pass ----------
        // BIG writes Index.db and Summary.db; BTI writes Partitions.db and Rows.db. Either way this also produces
        // the child's Filter.db, its exact estimatedPartitionSize histogram, its key cardinality and its first/last
        // key, all of which are functions of the same (key, position) stream. Rebuilding an index moves no Data.db
        // bytes so it is not throttled, but it is the other place a split spends real time, so the interrupt check
        // goes with it. See ZeroCopySplitIndex, and BtiZeroCopySplit for what BTI does differently.
        ZeroCopySplitIndex.ChildIndex childIndex = indexWriter.writeChild(child, range, from, to, interrupt);
        components.addAll(childIndex.components);
        DecoratedKey first = childIndex.first;
        DecoratedKey last = childIndex.last;

        // ---------- Statistics.db ----------
        // plan.childLength, not physicalBytes: compressionRatio is compressed-over-uncompressed for the FILE, and
        // the pad is on disk. hasUnindexedRegions is inherited, never a literal false -- a split adds no unindexed
        // region of its own but cannot remove one the parent already had (a sliced sstable received by partial
        // zero-copy streaming), and clearing the marker hands the child to the linear scanner.
        writeStatistics(child, metadata, parentMetadata, parentStats, childIndex.partitionSizes,
                        childIndex.cardinality, plan.childLength, range.dataLength, first, last,
                        parentStats.hasUnindexedRegions, repairState);

        // ---------- Digest.crc32: CRC32 over EVERY physical byte of the child Data.db ----------
        // Optional, and the one component whose cost is proportional to the DATA rather than the index. Skipping it
        // is a supported configuration -- see writeDigest and Config.zero_copy_split_digest_enabled. The flag was
        // snapshotted for the whole split in build(), so every sibling gets the same component set.
        if (digestEnabled)
        {
            writeDigest(child, progress, interrupt);
            requireNonEmpty(child, Components.DIGEST);
            components.add(Components.DIGEST);
        }

        // ---------- TOC.txt, last: it has to name every file that exists and no others ----------
        components.add(Components.TOC);
        TOCComponent.updateTOC(child, components);

        // Component CONTENTS are each fsynced already; this makes their DIRECTORY ENTRIES durable too, without which
        // a crash can leave a directory not listing a file whose data is on disk -- the same loss. Only the components
        // written through a SequentialWriter sync the directory themselves, on create (SequentialWriter.openChannel ->
        // trySyncDir): Statistics.db, BIG's Index.db, and BTI's Partitions.db and Rows.db. Must happen before the
        // child is published: the fsynced COMMIT record that unlinks the parent must not be first.
        SyncUtil.trySyncDir(child.directory);

        SSTableReader reader = SSTableReader.open(parent.owner().orElse(null), child, components, parent.metadata);
        try
        {
            validateChild(reader, range, plan, physicalBytes, partitionCount, chunkLength);
            // Inside the same window as the validation, because trackNew writes to the transaction log and so can
            // throw an FSWriteError of its own on a failing disk. Outside it, a reader that had been opened AND
            // validated was released by nothing: cleanUp only releases the children build() has already collected,
            // and this one is not returned to it until below.
            if (txn != null)
                txn.trackNew(reader);
        }
        catch (Throwable t)
        {
            reader.selfRef().release();
            throw t;
        }

        return new Child(child, first, last, range, physicalBytes, plan.headPadBytes, cloned, partitionCount,
                         ImmutableSet.copyOf(components), repairState, reader);
    }

    // ---- Component writers ----
    // Several are package-private because ZeroCopySSTableSlice reuses them, and every remark below about what may and
    // may not be inherited applies there identically -- a second writeStatistics would be a second place to get the
    // SerializationHeader and the commitlog-interval/host-id pair wrong. writeCompressionInfo is NOT shared: a split
    // child is one chunk run with an alignment pad, a slice is a concatenation of runs with none.

    /**
     * Materialise the child's Data.db as the verbatim parent byte range
     * {@code [plan.srcStart, plan.srcStart + plan.childLength)}, sharing what the filesystem allows and copying the
     * rest -- a refusal is all-or-nothing as far as this caller is concerned ({@code FICLONERANGE} can leave part of
     * the range shared, and {@link Reflink#tryCloneRange} truncates that back before answering false), so it falls
     * through to a transfer loop that produces a byte-for-byte identical child. This loop rewrites the whole range
     * from {@code plan.srcStart} regardless, so it does not depend on that guarantee.
     * transferTo returns short counts and caps near 0x7ffff000, so it MUST be looped;
     * {@code n <= 0} means EOF, not "retry". A clone moves no bytes, so it is checked for cancellation but not
     * throttled.
     *
     * @return how many bytes were shared rather than copied; 0 means the whole range was transferred
     */
    private static long copyData(File src, File dst, File directory, CopyPlan plan, Progress progress,
                                 Runnable interrupt)
    throws IOException
    {
        try (FileChannel in = src.newReadChannel();
             FileChannel outChannel = dst.newWriteChannel(File.WriteMode.OVERWRITE))
        {
            long cloned = 0;
            if (plan.cloneLength > 0)
            {
                interrupt.run();
                if (Reflink.tryCloneRange(in, plan.srcStart, outChannel, 0, plan.cloneLength, directory))
                {
                    cloned = plan.cloneLength;
                    if (progress != null)
                        progress.cloned(cloned);
                }
            }

            // The ioctl does not move the destination's file position and transferTo writes at wherever it is, so
            // without this the tail would overwrite the shared head -- which, being copy-on-write, succeeds silently.
            outChannel.position(cloned);

            long position = plan.srcStart + cloned;
            long remaining = plan.childLength - cloned;
            while (remaining > 0)
            {
                int slice = (int) Math.min(remaining, TRANSFER_SLICE);
                // Between chunk transfers, which is the only place a verbatim copy can be interrupted: there is no
                // partition boundary inside it to stop cleanly at, and TRANSFER_SLICE is small precisely so that this
                // is asked often. Throttling follows, and only for bytes that are really about to move.
                interrupt.run();
                if (progress != null)
                    progress.throttle(slice);
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
     * Via the same {@code Writer} every real sstable uses, so it cannot drift from the format. Only dataLength,
     * chunkCount and the offsets differ, the latter rebased by {@link CopyPlan#srcStart} rather than {@code O(i)}.
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
     * The child's Statistics.db.
     * <p>
     * HEADER is MANDATORY to inherit byte-for-byte: rows in the copied Data.db encode timestamps/localDeletionTime/TTL
     * as unsigned vint deltas off {@code stats.minTimestamp/minLocalDeletionTime/minTTL} and their columns as a bitmap
     * subset of {@code header.columns()}, so tightening any of those silently corrupts every relocated row with all
     * CRCs still passing.
     * <p>
     * {@code commitLogIntervals} and {@code originatingHostId} are inherited as an ATOMIC PAIR from the same parent
     * StatsMetadata (docs/splits-research.md 4.5); the per-table union in CommitLogReplayer stays bit-identical because
     * IntervalSet.Builder.add is normalising and idempotent. The bug this avoids is stamping the child with the LOCAL
     * host id (which every MetadataCollector constructor does) while inheriting a foreign parent's intervals: the
     * replayer gates on {@code originatingHostId.equals(localhostId)}, so it would read foreign segment ids against
     * the local commitlog and discard acked-but-unflushed mutations.
     * <p>
     * {@code repairedAt}/{@code pendingRepair}/{@code isTransient} come from {@code repairState}, defaulting to the
     * parent's triple, and are written here rather than mutated afterwards so the reader opened a few lines later is
     * already correct: the Tracker routes a newly visible sstable to a compaction strategy holder by exactly this
     * triple ({@code CompactionStrategyManager.handleListChangedNotification}).
     * <p>
     * {@code sstableLevel} is inherited, matching {@code createWriterForAntiCompaction} for a single-input
     * anticompaction, and safe because the children are disjoint contiguous key sub-ranges of the parent's range.
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
        // The four ACCEPTED absolute TOTALS below are parent-wide in every child, so per-table aggregates
        // over-report by ~K and worthDroppingTombstones under-fires by ~K. Conservative in direction; see the class
        // javadoc under "Accepted imprecision in the children's Statistics.db".
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
                                                     // What MetadataCollector.finalizeMetadata passes; correct
                                                     // because CQL cannot add a clustering column, so the comparator
                                                     // cannot have drifted from the prefix coveredClustering was
                                                     // recorded against. Must be non-null when hasImprovedMinMax().
                                                     metadata.comparator.subtypes(),
                                                     parentStats.coveredClustering,               // inherit: a superset of the child's
                                                     parentStats.hasLegacyCounterShards,
                                                     repairState.repairedAt,                      // CALLER SUPPLIED
                                                     parentStats.totalColumnsSet,                 // ACCEPTED: parent-wide
                                                     parentStats.totalRows,                       // ACCEPTED: parent-wide
                                                     // NOT inherited: the parent's coverage is its whole token
                                                     // range, so giving it to K children would multiply the table's
                                                     // apparent coverage and mislead the density calculations that
                                                     // drive compaction. NaN is MetadataCollector's "unknown";
                                                     // recomputing would need the local ranges.
                                                     Double.NaN,
                                                     parentStats.originatingHostId,               // atomic pair, see javadoc
                                                     repairState.pendingRepair,                   // CALLER SUPPLIED
                                                     repairState.isTransient,                     // CALLER SUPPLIED
                                                     parentStats.hasPartitionLevelDeletions,      // inherit: conservative direction
                                                     // The CHILD's own range: when version.hasKeyRange() these
                                                     // outrank Summary.db in the reader's first/last, so inheriting
                                                     // would have every child claim the whole parent range and break
                                                     // range-based sstable selection.
                                                     childFirst.getKey(),
                                                     childLast.getKey(),
                                                     // Caller's decision: a split adds no unindexed region but
                                                     // cannot remove an inherited one, a slice creates one.
                                                     hasUnindexedRegions);

        Map<MetadataType, MetadataComponent> components = new EnumMap<>(parentMetadata);
        components.put(MetadataType.STATS, childStats);
        components.put(MetadataType.COMPACTION, new CompactionMetadata(cardinality));
        // VALIDATION and HEADER pass through by reference: no schema lookup, nothing that can throw.

        // StatsComponent.save is a SequentialWriter plus finish(), what BigTableWriter does, and NOT
        // MetadataSerializer.rewriteSSTableMetadata, which only flush()es and renames -- fine for its callers, which
        // mutate an ALREADY durable Statistics.db, but not here, this being the only copy of the child's
        // SerializationHeader and repair state. finish() ends in syncInternal(), and SequentialWriter fsyncs the
        // directory on create, so both are durable before COMMIT unlinks the parent.
        new StatsComponent(components).save(child);
        requireNonEmpty(child, Components.STATS);
    }

    /** {@code FilterComponent.save} fsyncs and propagates. {@code deleteOnFailure} false: {@link #cleanUp} does it. */
    static void writeFilter(Descriptor child, IFilter filter) throws IOException
    {
        FilterComponent.save(filter, child, false);
    }

    /**
     * Summary.db, fsynced. Deliberately not {@code IndexSummaryComponent.save}, which writes the same three things in
     * the same order -- treat it as the layout oracle if the format changes -- but does NOT fsync; fine for index
     * summary redistribution, which rebuilds what it loses, but here a torn Summary.db costs a full Index.db pass per
     * child at startup.
     */
    static void writeSummary(Descriptor child, DecoratedKey first, DecoratedKey last, IndexSummary summary)
    throws IOException
    {
        try (FileOutputStreamPlus out = new FileOutputStreamPlus(child.fileFor(BigFormat.Components.SUMMARY)))
        {
            IndexSummary.serializer.serialize(summary, out);
            ByteBufferUtil.writeWithLength(first.getKey(), out);
            ByteBufferUtil.writeWithLength(last.getKey(), out);
            out.flush();
            out.sync();
        }
    }

    /**
     * Digest.crc32 is the decimal ASCII of a CRC32 over EVERY physical byte of Data.db. That is correct for a
     * compressed sstable too: the writer folds the inline per-chunk CRCs into the full checksum
     * ({@code appendDirect(bb, checksumIncrementalResult=true)}). The head pad must be included, because
     * {@code Verifier} CRCs the whole file with no reference to CompressionInfo.db -- and a mismatch trips
     * {@code markAndThrow}, which stamps the sstable unrepaired and throws into the disk failure policy.
     * <p>
     * This pass is the dominant cost of a split whose extents were shared, hence
     * {@code zero_copy_split_digest_enabled: false}: nothing needs the component and a compressed sstable is
     * self-checking without it (every chunk carries an inline CRC32 that this path preserves and the read path
     * verifies), at the cost of {@code Verifier} answering a missing digest with a full extended verification.
     * Consumer audit on {@link org.apache.cassandra.config.Config#zero_copy_split_digest_enabled}.
     * <p>
     * The value could instead be DERIVED with {@code crc32_combine} from the parent's inline per-chunk CRC32s, which
     * carry no offset or chunk index -- 4 bytes read per chunk plus the pad. Not implemented: it carries its own
     * correctness burden, and a wrong digest is silent until {@code nodetool verify}.
     */
    private static void writeDigest(Descriptor child, Progress progress, Runnable interrupt) throws IOException
    {
        CRC32 crc = new CRC32();
        byte[] buffer = new byte[COPY_BUFFER_SIZE];
        try (InputStream in = child.fileFor(Components.DATA).newInputStream())
        {
            int n;
            while ((n = in.read(buffer)) > 0)
            {
                // Throttled and cancellable on the same terms as the copy, otherwise stopping the copy would still
                // leave the node grinding through an unbounded read of every child.
                interrupt.run();
                if (progress != null)
                    progress.throttle(n);
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

    // ---- Validation and plumbing ----

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

        // Decompress the child's FINAL chunk, which every check above is blind to: that chunk is physically whole
        // while the child's dataLength says only part of it is live, so its length is derived rather than stored and a
        // single byte of trailing slack changes it -- something Digest.crc32 cannot catch either, being computed over
        // whatever bytes were written. Reading the last live byte forces CompressedChunkReader's normal path, where a
        // wrong derived length fails the inline CRC32 (or LZ4's "Compressed lengths mismatch").
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
     * Fresh descriptors in the parent's directory, version and format. Prefers the live ColumnFamilyStore's id
     * generator so we cannot collide with a concurrent flush or compaction; the fallback is for offline use.
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

    /** Post-condition; unlike {@code SSTableReader.save*}, which logs at TRACE, deletes and returns normally. */
    static void requireNonEmpty(Descriptor descriptor, Component component)
    {
        File file = descriptor.fileFor(component);
        if (!file.exists() || file.length() == 0)
            throw new IllegalStateException("failed to write " + component + " for " + descriptor);
    }

    /**
     * Best-effort removal of every partially written child, so a failed split leaves no orphans behind.
     * <p>
     * Deliberately symmetric with {@code CompactionManager.discardUnpublishedChildren}: releasing the reader is not
     * enough for a child that reached {@code txn.trackNew}, because the transaction is the caller's and the caller
     * goes on to reuse and commit it. An ADD record naming files this method has just deleted would be committed with
     * it, so each tracked child is untracked as well as released.
     */
    private static void cleanUp(LifecycleTransaction txn, List<Child> children, List<Descriptor> created)
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
            if (txn == null)
                continue;
            try
            {
                txn.untrackNew(child.reader);   // drops the ADD record and deletes any surviving files
            }
            catch (Throwable t)
            {
                logger.warn("Failed untracking child {} during cleanup", child.descriptor, t);
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
