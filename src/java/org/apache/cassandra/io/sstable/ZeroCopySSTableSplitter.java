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
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.zip.CRC32;

import com.clearspring.analytics.stream.cardinality.ICardinality;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.RateLimiter;
import com.sun.management.UnixOperatingSystemMXBean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.compaction.CompactionInfo;
import org.apache.cassandra.db.compaction.CompactionInterruptedException;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Token;
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
import org.apache.cassandra.io.sstable.indexsummary.IndexSummary;
import org.apache.cassandra.io.sstable.metadata.CompactionMetadata;
import org.apache.cassandra.io.sstable.metadata.MetadataComponent;
import org.apache.cassandra.io.sstable.metadata.MetadataType;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.io.sstable.metadata.ValidationMetadata;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileOutputStreamPlus;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.io.util.Reflink;
import org.apache.cassandra.io.util.SequentialWriter;
import org.apache.cassandra.io.util.SequentialWriterOption;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.EstimatedHistogram;
import org.apache.cassandra.utils.IFilter;
import org.apache.cassandra.utils.StorageCompatibilityMode;
import org.apache.cassandra.utils.SyncUtil;
import org.apache.cassandra.utils.TimeUUID;
import org.apache.cassandra.utils.Throwables;

/**
 * Splits one compressed BIG SSTable into K children by copying verbatim compression-chunk runs of Data.db and
 * rebuilding every other component from sequential Index.db passes. Before creating a child it authenticates every
 * index record against checksummed Statistics.db and one forward Data.db pass that reads only the short partition key
 * at each claimed position. No row is ever deserialised and nothing is recompressed. Index.db records carry both the
 * partition key and Data.db position, so each child's index is rebuilt with only the position field rebased; promoted
 * row indexes remain relative to their partition and are copied verbatim.
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
 * A split child can also have a logical dead prefix: bytes from partitions preceding the child's first indexed
 * partition that share its first compression chunk. Its CompressionInfo data length ends exactly after the child's
 * last partition, so there are no unindexed gaps or suffix. Full scans, cursors, scrub and extended verification
 * therefore start at the independently checksummed {@link StatsMetadata#firstPartitionPosition} in Statistics.db.
 * Persisting the position, rather than a boolean that makes every consumer rediscover it from Index.db, also leaves
 * index-less scrub able to recover a damaged child. Separately, {@code MmappedRegions} must include any physical
 * reflink alignment pad in its first mapping while retaining the invariant that its first region begins at zero.
 * <p>
 * All of it is conditional and self-demoting: the pad is only planned for when {@code Reflink.isPossibleIn} has not
 * already learned that this directory's filesystem cannot share extents (remembered per filesystem, not per
 * directory), a refusal costs one failing ioctl and falls
 * through to the ordinary transfer loop, and a padded range that is copied instead produces a child byte-for-byte
 * identical to a cloned one. {@code Result.totalBytesCloned} reports what happened.
 * <p>
 * Page cache is per inode, so bytes read through both parent and child are cached twice (transient: the parent is
 * unlinked when the children are published), and {@code du} counts shared blocks per file where {@code df} counts
 * them once, so per-directory usage over-reports until then.
 * <p>
 * With the copy gone, the main remaining costs are {@link #authenticateParentIndex}, which performs a forward
 * decompressing Data.db pass to validate every index position, and {@link #writeDigest}, which performs another raw
 * read of all child data. For narrow partitions the authentication pass decompresses essentially the whole parent,
 * so operators should budget roughly two full data reads plus one full decompression pass.
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
 * <h2>Older SSTable versions</h2>
 * Parents before BIG {@code pa} are refused because relabelling their copied row and CompressionInfo bytes as a newer
 * format has not been shown safe. A child is stamped BIG {@code pb}, whose Statistics.db records where a full scan
 * must begin after a retained prefix. Ordinary writers remain on {@code pa}; both versions count as latest, so adding
 * the tool does not schedule every existing SSTable for automatic upgrade. Splitting is also disabled unless storage
 * compatibility mode is {@code NONE}, keeping {@code pb} children out of a rolling upgrade with older nodes.
 * <p>
 * A reader opened {@code MOVED_START} ({@code cloneWithNewStart}, i.e. an early-open reader of a running compaction)
 * is refused for an unrelated reason: its {@code getFirst()} has moved but its Data.db and index have not, so the
 * first child would be cut at a position covering partitions the parent no longer claims.
 *
 * <h2>Accepted imprecision in the children's Statistics.db</h2>
 * Absolute per-sstable <em>totals</em> and min/max bounds cannot be recomputed without deserialising rows -- the
 * entire cost this class exists to avoid -- so every child inherits the PARENT-WIDE value. The full inherited-verbatim
 * set is:
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
 * {@link #splitBySize} registers its {@link Progress} with the compaction framework and uses the shared compaction
 * rate limiter. Stop checks run between chunk transfers and periodically during index and digest passes; interruption
 * aborts the transaction and deletes every child.
 */
public final class ZeroCopySSTableSplitter
{
    private static final Logger logger = LoggerFactory.getLogger(ZeroCopySSTableSplitter.class);

    /** BIG minor version reserved for children whose Statistics.db can carry the first partition position. */
    private static final String SPLIT_VERSION = "pb";

    /** Prefix of the refusal message for an uncompressed parent, so tests need not match the whole sentence. */
    private static final String UNCOMPRESSED_UNSUPPORTED_MESSAGE =
        "ZeroCopySSTableSplitter requires a compressed sstable";

    /** Prefix of the refusal message when a safe split-version child cannot be produced. */
    private static final String SPLIT_PREFIX_VERSION_UNSUPPORTED_MESSAGE =
        "ZeroCopySSTableSplitter requires a BIG pa-or-later parent and storage compatibility mode NONE";

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

    /** Conservative allowance for each child's fixed-size metadata and per-component rounding. */
    private static final long CHILD_COMPONENT_OVERHEAD = 1L << 20;

    /**
     * Upper bound per partition for a Bloom filter produced by the current implementation. BloomCalculations tops
     * out at 20 bits per key; eight bytes leaves ample room for word rounding and its two integer headers.
     */
    private static final long BLOOM_FILTER_BYTES_PER_PARTITION = Long.BYTES;

    /** Beyond its key, a full-sampling Summary.db entry adds an index position and an offsets-table entry. */
    private static final long SUMMARY_BYTES_PER_PARTITION = Long.BYTES + Integer.BYTES;

    /** Each completed BIG child remains open through one Data.db and one Index.db {@code FileHandle}. */
    private static final long PERSISTENT_FILE_DESCRIPTORS_PER_CHILD = 2;

    /**
     * Descriptors needed in addition to completed child readers while the next child is built: the parent's index
     * walk, Data.db copy channels, component writers, and transient component inputs during reader loading.
     */
    private static final long TRANSIENT_FILE_DESCRIPTOR_HEADROOM = 8;

    /**
     * Test hook: lay every child out as if extent sharing were available, so the aligned layout is covered on
     * filesystems that cannot share extents (every laptop and CI box). Also lifts {@link #MIN_CLONE_BYTES}, since test
     * sstables are smaller than that. The copy mechanism is unaffected.
     */
    @VisibleForTesting
    static volatile boolean forceAlignedLayoutForTesting = false;

    /**
     * Test hook, called with the number of children already built after the next child is registered in the lifecycle
     * transaction but before its first component is created. This makes both the publication ordering and cleanup of
     * an incomplete current child directly testable.
     */
    @VisibleForTesting
    static volatile java.util.function.IntConsumer failBeforeChildForTesting = null;
    static volatile Consumer<SSTableReader> failAfterChildOpenForTesting = null;

    /** Deterministic override for the process's currently available file-descriptor budget. */
    @VisibleForTesting
    static volatile LongSupplier availableFileDescriptorsForTesting = null;

    /**
     * Bit-identical copy of the package-private {@code MetadataCollector.defaultPartitionSizeHistogram()}: a child's
     * {@code estimatedPartitionSize} must bucket the way every writer-produced sstable's does or the two cannot be
     * summed. {@code ZeroCopySplitStatsTest} pins them together.
     */
    static final int PARTITION_SIZE_HISTOGRAM_BUCKETS = 155;

    /**
     * Every component this class can write, i.e. everything {@link #cleanUp} has to remove.
     */
    private static final List<Component> WRITTEN_COMPONENTS = ImmutableList.of(Components.DATA,
                                                                               Components.COMPRESSION_INFO,
                                                                               Components.STATS,
                                                                               Components.FILTER,
                                                                               Components.DIGEST,
                                                                               Components.TOC,
                                                                               BigFormat.Components.PRIMARY_INDEX,
                                                                               BigFormat.Components.SUMMARY);

    /**
     * {@link #WRITTEN_COMPONENTS} as a set, for the {@link #unhandledComponents} difference. Same membership, and
     * deliberately the same list: a component this class cannot write is a component {@link #cleanUp} could not
     * remove either, so the two questions have one answer.
     */
    private static final ImmutableSet<Component> HANDLED_COMPONENTS = ImmutableSet.copyOf(WRITTEN_COMPONENTS);

    private ZeroCopySSTableSplitter()
    {
    }

    /**
     * Descriptor-backed lifecycle-log entry used before the first child component exists. The final reader replaces
     * it only in the caller's normal transaction update; the ADD record itself is keyed by descriptor and component
     * count, not by this object's identity.
     */
    private static final class PendingSSTable extends SSTable
    {
        private PendingSSTable(SSTableReader parent, Descriptor descriptor, Set<Component> components)
        {
            super(new SSTable.Builder<>(descriptor).setComponents(components)
                                                   .setTableMetadataRef(parent.metadata)
                                                   .setChunkCache(parent.chunkCache)
                                                   .setIOOptions(parent.ioOptions),
                  parent.owner().orElse(null));
        }

        @Override
        public DecoratedKey getFirst()
        {
            return null;
        }

        @Override
        public DecoratedKey getLast()
        {
            return null;
        }

        @Override
        public AbstractBounds<Token> getBounds()
        {
            return null;
        }
    }

    private static Set<Component> expectedChildComponents(SSTableReader parent)
    {
        ImmutableSet.Builder<Component> components = ImmutableSet.builder();
        for (Component component : WRITTEN_COMPONENTS)
        {
            if (component != Components.FILTER || parent.metadata().params.bloomFilterFpChance < 1.0)
                components.add(component);
        }
        return components.build();
    }

    private static void requireNoExistingComponents(Descriptor descriptor) throws IOException
    {
        String prefix = descriptor.baseFile().name() + Component.separator;
        String[] existing = descriptor.directory.listNames((dir, name) -> name.startsWith(prefix));
        if (existing.length > 0)
            throw new IllegalStateException("Cannot create " + descriptor + ": files already exist " +
                                            Arrays.toString(existing));
    }

    // ---- Arithmetic: static and free of any sstable dependency, so it can be unit tested alone ----

    /** Chunk containing {@code uncompressedPosition}; mirrors {@code CompressionMetadata.chunkFor}. */
    private static long chunkIndexFor(long uncompressedPosition, int chunkLength)
    {
        checkChunkLength(chunkLength);
        if (uncompressedPosition < 0)
            throw new IllegalArgumentException("negative uncompressed position: " + uncompressedPosition);
        return uncompressedPosition / chunkLength;
    }

    /** First (inclusive) chunk of a child whose first live byte is at parent uncompressed offset {@code lo}. */
    private static long firstChunk(long lo, int chunkLength)
    {
        return chunkIndexFor(lo, chunkLength);
    }

    /**
     * Last (inclusive) chunk of a child whose live bytes end at exclusive parent uncompressed offset {@code hi}.
     * {@code (hi - 1) / L}, not {@code hi / L}: on an exact boundary the final chunk is the one <em>before</em> it,
     * and {@code hi / L} would read one chunk too far, throwing EOF at the end of the file.
     */
    private static long lastChunk(long hi, int chunkLength)
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
    private static long childDataLength(long hi, long firstChunk, int chunkLength)
    {
        checkChunkLength(chunkLength);
        long dataLength = hi - firstChunk * chunkLength;
        if (dataLength <= 0)
            throw new IllegalArgumentException("non-positive child dataLength " + dataLength +
                                               " (hi=" + hi + ", firstChunk=" + firstChunk + ", L=" + chunkLength + ')');
        return dataLength;
    }

    /** Bytes at the head of the child Data.db that belong to no partition: {@code lo mod chunkLength}. */
    private static long deadPrefixBytes(long lo, int chunkLength)
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
    private static ChunkRange chunkRange(long lo, long hi, int chunkLength)
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
    private static CopyPlan copyPlan(long copyFrom, long physicalBytes, boolean align, boolean share)
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
    private static final class CopyPlan
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
    static final class ChunkRange
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

    /** Parent repair metadata inherited verbatim by every tool-produced child. */
    private static final class RepairState
    {
        private final long repairedAt;
        private final TimeUUID pendingRepair;
        private final boolean isTransient;

        private RepairState(StatsMetadata parentStats)
        {
            repairedAt = parentStats.repairedAt;
            pendingRepair = parentStats.pendingRepair;
            isTransient = parentStats.isTransient;
        }
    }

    /** One produced child sstable. */
    public static final class Child
    {
        /** Descriptor of the child, in the parent's directory and stamped as BIG {@code pb}. */
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
        /** The opened, validated child reader. The caller owns this reference and must release it. */
        public final SSTableReader reader;

        Child(Descriptor descriptor, DecoratedKey first, DecoratedKey last, ChunkRange range,
              long physicalBytes, long headPadBytes, long clonedBytes, long partitionCount,
              Set<Component> components, SSTableReader reader)
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
                                 " dead=%d partitions=%d]",
                                 descriptor, firstChunk, lastChunk, physicalBytes, headPadBytes, clonedBytes,
                                 dataLength, shift, deadPrefixBytes, partitionCount);
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
     * unbounded burst of I/O. Stopping (TRUNCATE, DROP, {@code nodetool stop COMPACTION},
     * {@code runWithCompactionsDisabled}) walks {@code active.getCompactions()} and calls
     * {@link CompactionInfo.Holder#stop()}, matching on the parent sstable this {@link CompactionInfo} carries.
     * <p>
     * A verbatim chunk copy has no partition boundary to stop cleanly at, so the stop check lives inside the transfer
     * loop and aborts the split outright: {@link CompactionInterruptedException} propagates out of
     * {@link #splitBySize}, the transaction is aborted and every child is deleted. A caller must NOT treat that as a
     * reason to fall back to the rewrite path -- the operator asked for the work to stop, not to be done a more
     * expensive way.
     * <p>
     * {@code total} is the preflight's conservative upper bound for every child component. {@code completed} advances
     * only when output bytes have actually been materialised: Data.db as each transfer or clone succeeds, and the
     * other components once a child has been validated. Digest input is throttled but is not a second output write.
     * This makes {@code CompactionInfo.estimatedRemainingWriteToDiskBytes} exactly {@code total - completed}, so a
     * huge first child cannot prematurely shed the reservation for many small children still to come. Shared bytes
     * count as materialised output but are not rate limited; see {@link #cloned}. Conservative estimation slack means
     * a split can finish below 100%; the holder is removed immediately afterwards.
     */
    private static final class Progress extends CompactionInfo.Holder
    {
        private final TableMetadata metadata;
        private final Set<SSTableReader> parent;
        private final long total;
        private final TimeUUID id;
        private final AtomicLong written = new AtomicLong();
        private final RateLimiter limiter;

        private Progress(SSTableReader parent, RateLimiter limiter, SplitEstimate estimate)
        {
            this.metadata = parent.metadata();
            this.parent = ImmutableSet.of(parent);
            this.total = estimate.totalWriteBytes;
            this.id = TimeUUID.Generator.nextTimeUUID();
            this.limiter = limiter;
        }

        @Override
        public CompactionInfo getCompactionInfo()
        {
            // totalCompressed == total makes CompactionInfo's scaled remaining-write estimate exactly the unreported
            // portion of the preflight reservation. Keep the clamp defensive: a future accounting call site must not
            // turn an underestimate into a negative reservation and phantom free space.
            return new CompactionInfo(metadata, OperationType.COMPACTION, Math.min(written.get(), total), total,
                                      total, id, parent);
        }

        /** One sstable of one table, so a paused global compaction must not silently stop it. */
        @Override
        public boolean isGlobal()
        {
            return false;
        }

        /**
         * Called before {@code bytes} move. The transfer loop carries unused permits across short
         * {@code transferTo} calls, so every byte is charged exactly once. Deliberately NOT a stop check as well --
         * the caller has just made one through {@code ZeroCopySSTableSplitter.checkInterrupted}, and two paths to the
         * same exception is how one of them ends up being the only one a new call site uses.
         */
        void throttle(int bytes)
        {
            if (bytes > 0)
                limiter.acquire(bytes);
        }

        void wrote(long bytes)
        {
            written.addAndGet(bytes);
        }

        /** Raises {@link CompactionInterruptedException} iff the compaction framework has asked this split to stop. */
        public void checkStopped()
        {
            if (isStopRequested())
                throw new CompactionInterruptedException(getCompactionInfo());
        }

        /**
         * Bytes shared rather than moved. Deliberately NOT rate limited -- {@code compaction_throughput} bounds disk
         * traffic and a clone makes none -- but still reported as materialised output.
         */
        void cloned(long bytes)
        {
            wrote(bytes);
        }
    }

    /**
     * The one interruption check used by every long-running loop. A caller without {@link Progress} is a focused test
     * helper and cannot be stopped through the compaction framework.
     */
    private static void checkInterrupted(Progress progress)
    {
        if (progress != null)
            progress.checkStopped();
    }

    // ---- Entry points ----

    /**
     * Split into children whose compressed Data.db is at most {@code targetSize} bytes. A partition is never divided,
     * so a child containing one partition whose compression-chunk span is already larger than the target is the sole
     * exception.
     */
    public static Result splitBySize(SSTableReader parent, long targetSize, LifecycleTransaction txn)
    {
        Preconditions.checkArgument(targetSize > 0, "targetSize must be > 0, got %s", targetSize);
        requireSupported(parent);
        Preconditions.checkNotNull(txn, "txn");

        long start = Clock.Global.nanoTime();
        Runs runs = selectByCompressedSize(parent, targetSize, null);
        authenticateParentIndex(parent, runs);
        requireFileDescriptorBudget(parent, runs);
        SplitEstimate estimate = requireDiskSpace(parent, runs, targetSize);
        Progress progress = new Progress(parent, CompactionManager.instance.getRateLimiter(), estimate);
        Runnable interrupt = () -> checkInterrupted(progress);
        return CompactionManager.instance.runAsActiveCompaction(progress,
                                                                () -> build(parent, runs, txn, progress, interrupt,
                                                                            start, targetSize));
    }

    /**
     * @return true iff this is a normally-opened, compressed BIG {@code pa}-or-later SSTable from which a marker-
     *         capable {@code pb} child may be produced, with no compression dictionary
     */
    public static boolean isSupported(SSTableReader parent)
    {
        try
        {
            requireSupported(parent);
            return true;
        }
        catch (UnsupportedOperationException | IllegalStateException | UncheckedIOException e)
        {
            return false;
        }
    }

    /**
     * Whether this format's index can be rebased onto a copied chunk run at all. Split out so callers can report an
     * unsupported format separately from an unsupported compression setting without keeping another format list.
     */
    private static boolean isSupportedFormat(SSTableFormat<?, ?> format)
    {
        return BigFormat.is(format);
    }

    private static boolean isSupportedParentVersion(Version version)
    {
        return version.supportsZeroCopySplitInput();
    }

    private static Version splitVersion()
    {
        return BigFormat.getInstance().getVersion(SPLIT_VERSION);
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
     * This is the on-disk component backstop: it unions the reader's component set with the authoritative TOC, so a
     * reader opened with an intentionally small set such as {@code batchComponents()} cannot hide attached files.
     * Callers with a {@code ColumnFamilyStore} must also ask
     * {@code SecondaryIndexManager.hasSSTableAttachedIndexes()}; that schema-level check refuses an attached index
     * even when its per-sstable files are already absent or otherwise cannot be established from this one TOC.
     */
    static Set<Component> unhandledComponents(SSTableReader parent)
    {
        Preconditions.checkNotNull(parent, "parent");
        try
        {
            // The reader may have been opened with batchComponents(), which is deliberately incomplete. TOC.txt is
            // the authoritative component set and includes per-index-named SAI components that format discovery
            // cannot enumerate.
            Set<Component> onDisk = TOCComponent.loadTOC(parent.descriptor, false);
            return Sets.difference(Sets.union(parent.getComponents(), onDisk), HANDLED_COMPONENTS);
        }
        catch (IOException e)
        {
            throw new UncheckedIOException("cannot establish the complete component set for " + parent.descriptor,
                                           e);
        }
    }

    /** Fixed-count split used only by focused tests; production exposes only {@link #splitBySize}. */
    @VisibleForTesting
    static Result splitForTesting(SSTableReader parent, int numChildren, LifecycleTransaction txn)
    {
        Preconditions.checkArgument(numChildren >= 1, "numChildren must be >= 1, got %s", numChildren);
        requireSupported(parent);

        long start = Clock.Global.nanoTime();
        Runnable interrupt = () -> checkInterrupted(null);
        // Four Index.db walks, none retaining anything per partition: count, select, authenticate, build.
        // Authentication also makes one forward Data.db pass that reads only the key at each claimed position.
        // Counting first is what lets selection stay O(numChildren) in heap -- it needs the exact partition count up
        // front for its tail-room clamp. See RunSelector.
        int partitionCount = countPartitions(parent, interrupt);
        if (numChildren > partitionCount)
            throw new IllegalArgumentException("cannot split " + partitionCount + " partitions into " +
                                               numChildren + " children");
        Runs runs = selectByByteShare(parent, numChildren, partitionCount, interrupt);
        authenticateParentIndex(parent, runs);
        requireFileDescriptorBudget(parent, runs);
        return build(parent, runs, txn, null, interrupt, start, 0);
    }

    /** Unsafe no-transaction variant for tests that inspect children without publishing them. */
    @VisibleForTesting
    static Result splitForTesting(SSTableReader parent, int numChildren)
    {
        return splitForTesting(parent, numChildren, null);
    }

    private static void requireSupported(SSTableReader parent)
    {
        Preconditions.checkNotNull(parent, "parent");
        if (!isSupportedFormat(parent.descriptor.getFormat()))
            throw new UnsupportedOperationException("ZeroCopySSTableSplitter supports only the BIG sstable format, " +
                                                    "got " + parent.descriptor.getFormat().name() +
                                                    ". The technique is to copy Data.db chunks verbatim and " +
                                                    "rebase every position that points into them, which needs an " +
                                                    "index whose partition positions can be found and rewritten " +
                                                    "without deserialising a row.");
        if (!isSupportedParentVersion(parent.descriptor.version)
            || !splitVersion().hasSplitPrefixMarker()
            || DatabaseDescriptor.getStorageCompatibilityMode() != StorageCompatibilityMode.NONE)
            throw new UnsupportedOperationException(SPLIT_PREFIX_VERSION_UNSUPPORTED_MESSAGE + ": " +
                                                    parent.descriptor + " is version '" +
                                                    parent.descriptor.version.version + "' and the configured mode is " +
                                                    DatabaseDescriptor.getStorageCompatibilityMode() + ". A split " +
                                                    "child can begin after a retained compression-chunk prefix and " +
                                                    "must be stamped '" + SPLIT_VERSION + "'; producing it during a " +
                                                    "rolling upgrade would expose it to readers that ignore the marker.");
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
        // isSupported() checks this too, and a direct split entry point must not be the one path that skips it: a
        // dictionary-compressed child's chunks are copied verbatim while its CompressionInfo.db is written afresh,
        // and a wrong answer there is undecompressible data. Ordered after the compression test, since
        // getCompressionMetadata() throws on an uncompressed sstable.
        if (parent.getCompressionMetadata().compressionDictionary() != null)
            throw new UnsupportedOperationException("cannot split " + parent.descriptor +
                                                    ": it is compressed with a compression dictionary, which this " +
                                                    "path has not been shown to round trip.");
        SSTable.Owner owner = parent.owner().orElse(null);
        if (owner instanceof ColumnFamilyStore
            && ((ColumnFamilyStore) owner).indexManager.hasSSTableAttachedIndexes())
        {
            throw new UnsupportedOperationException("cannot split " + parent.descriptor +
                                                    ": its table has SSTable-attached indexes, whose components " +
                                                    "cannot be produced for the children");
        }
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
                                                   " rewrite should do so; callers with a ColumnFamilyStore should" +
                                                   " also check SecondaryIndexManager.hasSSTableAttachedIndexes" +
                                                   " before attempting a split.");
    }

    // ---- Walking the parent Index.db ----

    /** Receives every partition of the parent, in on-disk (token) order. */
    private interface IndexRecordConsumer
    {
        void accept(int index, ByteBuffer key, long position) throws IOException;
    }

    /**
     * One sequential walk of the parent Index.db, retaining nothing. Deliberately does not hand back the positions: a
     * {@code long[]} of every partition's Data.db offset costs 8 bytes per partition (16-24 at the peak of the doubling
     * and trim), i.e. tens of gigabytes of heap for a terabyte of 1 KiB partitions, and every access is sequential.
     *
     * @param stopCheck optional; run every 1024 records and expected to raise {@link CompactionInterruptedException}.
     *                  A {@code Runnable} rather than the {@link Progress} itself because a walk moves no Data.db
     *                  bytes and so has nothing to throttle.
     * @return the exact number of records
     */
    private static int walkIndex(SSTableReader parent, IndexRecordConsumer consumer, Runnable stopCheck)
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

    /** Just the record count, for the byte-share form, whose selection needs it before it can start. */
    private static int countPartitions(SSTableReader parent, Runnable stopCheck)
    {
        return walkIndex(parent, (index, key, position) -> {}, stopCheck);
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
        /** Sum of the partition-key lengths in Index.db, used to bound a fully sampled Summary.db. */
        final long totalKeyBytes;
        /** First and last keys observed by the same selection pass that supplied {@link #partitionCount}. */
        final ByteBuffer firstKey;
        final ByteBuffer lastKey;

        Runs(int[] runStarts, long[] runPositions, int partitionCount, long totalKeyBytes)
        {
            this(runStarts, runPositions, partitionCount, totalKeyBytes, null, null);
        }

        Runs(int[] runStarts, long[] runPositions, int partitionCount, long totalKeyBytes,
             ByteBuffer firstKey, ByteBuffer lastKey)
        {
            this.runStarts = runStarts;
            this.runPositions = runPositions;
            this.partitionCount = partitionCount;
            this.totalKeyBytes = totalKeyBytes;
            this.firstKey = firstKey;
            this.lastKey = lastKey;
        }
    }

    /** No record's offset can be this, so it doubles as "not filled in yet". */
    private static final long UNRESOLVED = -1;

    /** The byte-share form: one pass, driving {@link RunSelector}. */
    private static Runs selectByByteShare(SSTableReader parent, int numChildren, int partitionCount,
                                          Runnable stopCheck)
    {
        RunSelector selector = new RunSelector(parent.uncompressedLength(), numChildren, partitionCount);
        long[] totalKeyBytes = { 0 };
        ByteBuffer[] firstKey = { null };
        ByteBuffer[] lastKey = { null };
        int count = walkIndex(parent, (index, key, position) -> {
            totalKeyBytes[0] += key.remaining();
            if (index == 0)
                firstKey[0] = key;
            lastKey[0] = key;
            selector.offer(index, position);
        }, stopCheck);
        if (count != partitionCount)
            throw new IllegalStateException("parent Index.db grew or shrank between passes: counted " +
                                            partitionCount + ", then " + count + ": " + parent.descriptor);
        return selector.finish(totalKeyBytes[0], firstKey[0], lastKey[0]);
    }

    /**
     * Greedily packs complete partitions while the exact compression-chunk span copied into Data.db fits the target.
     * The one-record look-behind lets an oversized candidate be cut before its last partition without retaining an
     * offset per partition. The only child allowed over the target is therefore a single indivisible partition.
     */
    private static Runs selectByCompressedSize(SSTableReader parent, long targetSize, Runnable stopCheck)
    {
        CompressionMetadata metadata = parent.getCompressionMetadata();
        int chunkLength = metadata.chunkLength();
        List<Integer> starts = new ArrayList<>();
        List<Long> positions = new ArrayList<>();

        int[] runStart = { 0 };
        long[] runPosition = { UNRESOLVED };
        long[] previousPosition = { UNRESOLVED };
        long[] totalKeyBytes = { 0 };
        ByteBuffer[] firstKey = { null };
        ByteBuffer[] lastKey = { null };

        int partitionCount = walkIndex(parent, (index, key, position) -> {
            totalKeyBytes[0] += key.remaining();
            if (index == 0)
                firstKey[0] = key;
            lastKey[0] = key;
            if (index == 0)
            {
                starts.add(0);
                positions.add(position);
                runPosition[0] = position;
            }
            else
            {
                if (position <= previousPosition[0])
                    throw new IllegalStateException("parent index positions are not strictly increasing at record " +
                                                    index + ": " + previousPosition[0] + " -> " + position);

                int candidatePartitions = index - runStart[0];
                if (candidatePartitions > 1
                    && compressedBytes(metadata, runPosition[0], position, chunkLength) > targetSize)
                {
                    // The candidate without its last partition was checked at the preceding record. Start the next
                    // child at that last partition; if it alone is oversized it will be isolated on the next offer.
                    runStart[0] = index - 1;
                    runPosition[0] = previousPosition[0];
                    starts.add(runStart[0]);
                    positions.add(runPosition[0]);
                }
            }
            previousPosition[0] = position;
        }, stopCheck);

        if (partitionCount - runStart[0] > 1
            && compressedBytes(metadata, runPosition[0], metadata.dataLength, chunkLength) > targetSize)
        {
            // EOF is the exclusive end of the final partition. The same one-record look-behind isolates it.
            runStart[0] = partitionCount - 1;
            runPosition[0] = previousPosition[0];
            starts.add(runStart[0]);
            positions.add(runPosition[0]);
        }

        int[] runStarts = new int[starts.size()];
        long[] runPositions = new long[positions.size()];
        for (int i = 0; i < starts.size(); i++)
        {
            runStarts[i] = starts.get(i);
            runPositions[i] = positions.get(i);
        }
        return new Runs(runStarts, runPositions, partitionCount, totalKeyBytes[0], firstKey[0], lastKey[0]);
    }

    /**
     * Authenticate the complete index against Statistics.db and Data.db before a child descriptor or component can
     * be created. Statistics.db is independently checksummed and every writer records one partition-size sample per
     * partition, so its count and key range are exact witnesses. One forward Data.db reader then reads only the short
     * partition key at every claimed position; it never deserialises a row. Checking every position matters even when
     * the count and endpoint keys agree: an interior index offset can otherwise silently assign the wrong Data bytes.
     * Together these checks prevent a parsable Index.db suffix from being mistaken for a complete parent and turning
     * a recoverable omitted Data.db prefix into permanently unindexed child bytes.
     */
    private static void authenticateParentIndex(SSTableReader parent, Runs runs)
    {
        StatsMetadata stats = (StatsMetadata) readParentMetadata(parent.descriptor).get(MetadataType.STATS);
        ByteBuffer[] firstKey = { null };
        ByteBuffer[] lastKey = { null };
        long[] firstPosition = { -1 };
        long[] previousPosition = { -1 };
        DecoratedKey[] previousKey = { null };

        int authenticatedCount;
        try (RandomAccessReader data = parent.openDataReaderForScan())
        {
            authenticatedCount = walkIndex(parent, (index, key, position) -> {
                if (index > 0 && position <= previousPosition[0])
                {
                    throw new IOException("Index.db positions are not strictly increasing at record " + index +
                                          ": " + previousPosition[0] + " -> " + position);
                }

                DecoratedKey decorated = parent.getPartitioner().decorateKey(key);
                if (previousKey[0] != null && previousKey[0].compareTo(decorated) >= 0)
                {
                    throw new IOException("Index.db keys are not strictly increasing at record " + index +
                                          ": " + previousKey[0] + " -> " + decorated);
                }

                data.seek(position);
                ByteBuffer dataKey = ByteBufferUtil.readWithShortLength(data);
                if (!key.equals(dataKey))
                    throw new IOException("Index.db key at record " + index +
                                          " does not match Data.db position " + position);

                if (index == 0)
                {
                    firstKey[0] = key;
                    firstPosition[0] = position;
                }
                lastKey[0] = key;
                previousPosition[0] = position;
                previousKey[0] = decorated;
            }, null);
        }

        if (authenticatedCount != runs.partitionCount)
        {
            throw corruptParentIndex(parent, "Index.db changed between selection and authentication: selected " +
                                             runs.partitionCount + " partitions but authenticated " +
                                             authenticatedCount);
        }

        long statsCount = stats.estimatedPartitionSize.count();
        if (authenticatedCount != statsCount)
        {
            throw corruptParentIndex(parent, "Index.db has " + authenticatedCount +
                                             " partitions but Statistics.db records " + statsCount);
        }
        if (stats.firstKey == null || !stats.firstKey.equals(firstKey[0]))
            throw corruptParentIndex(parent, "the first Index.db key does not match Statistics.db");
        if (stats.lastKey == null || !stats.lastKey.equals(lastKey[0]))
            throw corruptParentIndex(parent, "the last Index.db key does not match Statistics.db");

        if (!Objects.equals(firstKey[0], runs.firstKey) || !Objects.equals(lastKey[0], runs.lastKey))
            throw corruptParentIndex(parent, "Index.db changed between selection and authentication");

        SSTableReader.PartitionPositionBounds fullRange = parent.getPositionsForFullRange();
        if (fullRange == null || firstPosition[0] != fullRange.lowerPosition ||
            runs.runPositions.length == 0 || runs.runPositions[0] != firstPosition[0])
        {
            long expected = fullRange == null ? -1 : fullRange.lowerPosition;
            long actual = firstPosition[0];
            throw corruptParentIndex(parent, "the first Index.db position is " + actual +
                                             " but the authenticated parent start is " + expected);
        }
    }

    private static CorruptSSTableException corruptParentIndex(SSTableReader parent, String message)
    {
        File index = parent.descriptor.fileFor(BigFormat.Components.PRIMARY_INDEX);
        return new CorruptSSTableException(new IOException("Cannot safely split " + parent.descriptor + ": " +
                                                           message), index);
    }

    /** Refuse before allocating a descriptor when all child readers cannot remain open through the build. */
    private static void requireFileDescriptorBudget(SSTableReader parent, Runs runs)
    {
        int children = plannedChildren(runs);
        long required = children * PERSISTENT_FILE_DESCRIPTORS_PER_CHILD + TRANSIENT_FILE_DESCRIPTOR_HEADROOM;
        long available = availableFileDescriptors();
        if (required <= available)
            return;

        throw new IllegalStateException("Cannot split " + parent.descriptor + " into " + children +
                                        " children: their BIG Data.db and Index.db readers require " + required +
                                        " additional file descriptors (two per child plus " +
                                        TRANSIENT_FILE_DESCRIPTOR_HEADROOM +
                                        " transient build headroom), but only " + available +
                                        " are available. Use a larger --size to produce fewer children or raise " +
                                        "the process open-file limit.");
    }

    private static int plannedChildren(Runs runs)
    {
        int children = 0;
        for (int b = 0; b < runs.runStarts.length; b++)
        {
            int from = runs.runStarts[b];
            int to = b + 1 < runs.runStarts.length ? runs.runStarts[b + 1] : runs.partitionCount;
            if (from < to)
                children++;
        }
        return children;
    }

    private static long availableFileDescriptors()
    {
        LongSupplier override = availableFileDescriptorsForTesting;
        if (override != null)
            return Math.max(0, override.getAsLong());

        try
        {
            java.lang.management.OperatingSystemMXBean bean = ManagementFactory.getOperatingSystemMXBean();
            if (!(bean instanceof UnixOperatingSystemMXBean))
                return Long.MAX_VALUE;

            UnixOperatingSystemMXBean unix = (UnixOperatingSystemMXBean) bean;
            long maximum = unix.getMaxFileDescriptorCount();
            long open = unix.getOpenFileDescriptorCount();
            if (maximum <= 0 || open < 0)
                return Long.MAX_VALUE;
            return open >= maximum ? 0 : maximum - open;
        }
        catch (RuntimeException | LinkageError e)
        {
            // The preflight must not invent a low limit on non-Unix or restricted runtimes. The eventual open still
            // reports the real OS error; supported Unix MXBeans get the deterministic early refusal above.
            logger.debug("Cannot read the process file-descriptor budget; skipping zero-copy split FD preflight", e);
            return Long.MAX_VALUE;
        }
    }

    private static long compressedBytes(CompressionMetadata metadata, long lo, long hi, int chunkLength)
    {
        ChunkRange range = chunkRange(lo, hi, chunkLength);
        return chunkEnd(metadata, range.lastChunk, chunkLength)
               - chunkStart(metadata, range.firstChunk, chunkLength);
    }

    /**
     * Refuse before creating a child when the copy fallback cannot fit alongside ongoing compactions. The Data.db
     * total is exact for the selected runs, including duplicated boundary chunks and the largest possible alignment
     * pads. The non-data bound does not rely on the parent's current Filter.db or Summary.db sizes: both may have been
     * written under different schema parameters. Child Index.db records cannot exceed the parent's (only their
     * non-negative data position shrinks); Summary.db is bounded as if every index key were sampled; Filter.db is
     * bounded at eight bytes per key; Statistics.db is conservatively repeated in full per child; and
     * CompressionInfo.db is exact from the repeated parent header plus every selected chunk offset. A fixed per-child
     * allowance covers summary endpoint keys, component headers, rounding, and small metadata variation.
     */
    private static SplitEstimate requireDiskSpace(SSTableReader parent, Runs runs, long maxDataFileSize)
    {
        CompressionMetadata metadata = parent.getCompressionMetadata();
        int chunkLength = metadata.chunkLength();
        long dataBytes = 0;
        long chunkOffsets = 0;
        int children = 0;
        for (int b = 0; b < runs.runStarts.length; b++)
        {
            int from = runs.runStarts[b];
            int to = b + 1 < runs.runStarts.length ? runs.runStarts[b + 1] : runs.partitionCount;
            if (from >= to)
                continue;

            long lo = runs.runPositions[b];
            long hi = to < runs.partitionCount ? runs.runPositions[b + 1] : metadata.dataLength;
            ChunkRange range = chunkRange(lo, hi, chunkLength);
            long copyFrom = chunkStart(metadata, range.firstChunk, chunkLength);
            long physicalBytes = chunkEnd(metadata, range.lastChunk, chunkLength) - copyFrom;
            CopyPlan padded = copyPlan(copyFrom, physicalBytes, true, false);
            long childBytes = padded.headPadBytes > 0 && padded.childLength > maxDataFileSize
                              ? physicalBytes
                              : padded.childLength;
            dataBytes = saturatedAdd(dataBytes, childBytes);
            chunkOffsets = saturatedAdd(chunkOffsets, range.chunkCount);
            children++;
        }

        long primaryIndexBytes = parent.descriptor.fileFor(BigFormat.Components.PRIMARY_INDEX).length();
        long parentCompressionInfoBytes = parent.descriptor.fileFor(Components.COMPRESSION_INFO).length();
        long compressionHeaderBytes = Math.max(0, parentCompressionInfoBytes - metadata.offHeapSize());
        long compressionInfoBytes = saturatedAdd(saturatedMultiply(children, compressionHeaderBytes),
                                                  saturatedMultiply(chunkOffsets, Long.BYTES));
        long statsBytes = parent.descriptor.fileFor(Components.STATS).length();
        long summaryBytes = saturatedAdd(runs.totalKeyBytes,
                                         saturatedMultiply(runs.partitionCount,
                                                           SUMMARY_BYTES_PER_PARTITION));
        long bloomFilterBytes = saturatedMultiply(runs.partitionCount, BLOOM_FILTER_BYTES_PER_PARTITION);

        long estimatedWriteBytes = saturatedAdd(dataBytes, primaryIndexBytes);
        estimatedWriteBytes = saturatedAdd(estimatedWriteBytes, compressionInfoBytes);
        estimatedWriteBytes = saturatedAdd(estimatedWriteBytes, summaryBytes);
        estimatedWriteBytes = saturatedAdd(estimatedWriteBytes, bloomFilterBytes);
        estimatedWriteBytes = saturatedAdd(estimatedWriteBytes, saturatedMultiply(children, statsBytes));
        estimatedWriteBytes = saturatedAdd(estimatedWriteBytes,
                                           saturatedMultiply(children, CHILD_COMPONENT_OVERHEAD));

        Map<File, Long> expected = Collections.singletonMap(parent.descriptor.directory, estimatedWriteBytes);
        Map<File, Long> active = CompactionManager.instance.active.estimatedRemainingWriteToDiskBytes();
        if (!new Directories(parent.metadata()).hasDiskSpaceForCompactionsAndStreams(expected, active))
        {
            throw new IllegalStateException("Insufficient disk space to split " + parent.descriptor +
                                            ": the copy fallback may write approximately " + estimatedWriteBytes +
                                            " bytes in " + parent.descriptor.directory);
        }
        return new SplitEstimate(estimatedWriteBytes);
    }

    private static final class SplitEstimate
    {
        private final long totalWriteBytes;

        private SplitEstimate(long totalWriteBytes)
        {
            this.totalWriteBytes = totalWriteBytes;
        }
    }

    private static long saturatedAdd(long left, long right)
    {
        return Long.MAX_VALUE - left < right ? Long.MAX_VALUE : left + right;
    }

    private static long saturatedMultiply(long left, long right)
    {
        return left != 0 && Long.MAX_VALUE / left < right ? Long.MAX_VALUE : left * right;
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

        Runs finish(long totalKeyBytes)
        {
            return finish(totalKeyBytes, null, null);
        }

        Runs finish(long totalKeyBytes, ByteBuffer firstKey, ByteBuffer lastKey)
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
            return new Runs(runStarts, runPositions, partitionCount, totalKeyBytes, firstKey, lastKey);
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
                                LifecycleTransaction txn, Progress progress,
                                Runnable interrupt, long startNanos, long maxDataFileSize)
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
        RepairState repairState = new RepairState(parentStats);

        Supplier<Descriptor> descriptors = descriptorAllocator(parent);
        Set<Component> expectedComponents = expectedChildComponents(parent);

        List<Child> children = new ArrayList<>(runStarts.length);
        List<Descriptor> created = new ArrayList<>(runStarts.length);
        List<PendingSSTable> tracked = new ArrayList<>(runStarts.length);
        long physicalTotal = 0;
        long deadTotal = 0;
        long padTotal = 0;
        long clonedTotal = 0;
        long duplicated = 0;

        boolean success = false;
        Throwable failure = null;
        // The try-with-resources is nested inside a plain try so that `success` can be set AFTER the index writer has
        // been closed. Set inside the resource block it would already be true when close() threw, so cleanUp() would
        // be skipped and the caller could be handed K fully-formed children plus leaked reader references.
        try
        {
            try (ZeroCopySplitIndex indexWriter = new ZeroCopySplitIndex(parent))
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

                    Descriptor child = descriptors.get();
                    requireNoExistingComponents(child);
                    // From this point onward cleanup owns every file with this descriptor. Registering only after the
                    // collision check prevents a refusal from deleting somebody else's pre-existing SSTable.
                    created.add(child);
                    if (txn != null)
                    {
                        PendingSSTable pending = new PendingSSTable(parent, child, expectedComponents);
                        // CASSANDRA-18737 invariant: the ADD record must be durable before the first component name
                        // can appear, otherwise a crash can discover both the parent and an unlogged child as live.
                        txn.trackNew(pending);
                        tracked.add(pending);
                    }
                    if (failBeforeChildForTesting != null)
                        failBeforeChildForTesting.accept(children.size());
                    Child built = buildChild(parent, child, indexWriter, from, to, range, meta, copyFrom,
                                             physicalBytes, parentMetadata, parentStats, repairState,
                                             progress, interrupt, maxDataFileSize);
                    // Cleanup must own the reader before any post-build assertion can throw.
                    children.add(built);
                    if (maxDataFileSize > 0
                        && built.descriptor.fileFor(Components.DATA).length() > maxDataFileSize
                        && built.partitionCount > 1)
                    {
                        throw new IllegalStateException("size-based split produced a " +
                                                        built.descriptor.fileFor(Components.DATA).length() +
                                                        "-byte child for a " + maxDataFileSize +
                                                        "-byte maximum with " + built.partitionCount +
                                                        " divisible partitions");
                    }

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
            failure = new UncheckedIOException("failed splitting " + parent.descriptor, e);
        }
        catch (Throwable t)
        {
            failure = t;
        }
        finally
        {
            if (!success)
                failure = cleanUp(txn, children, created, tracked, failure);
        }
        Throwables.maybeFail(failure);

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
                                    Progress progress,
                                    Runnable interrupt,
                                    long maxDataFileSize) throws IOException
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
        boolean canShare = Reflink.isPossibleIn(child.directory);
        boolean align = forceAlignedLayoutForTesting || (canShare && physicalBytes >= MIN_CLONE_BYTES);
        CopyPlan plan = copyPlan(copyFrom, physicalBytes, align, align && canShare);
        if (maxDataFileSize > 0 && plan.childLength > maxDataFileSize && plan.headPadBytes > 0)
        {
            // The padding only enables extent sharing; it is not an indivisible part of any partition and therefore
            // cannot be allowed to turn the user-visible maximum into a suggestion.
            plan = copyPlan(copyFrom, physicalBytes, false, false);
        }
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
        // This writes Index.db and Summary.db and also produces the child's Filter.db, its exact
        // estimatedPartitionSize histogram, its key cardinality and its first/last
        // key, all of which are functions of the same (key, position) stream. Rebuilding an index moves no Data.db
        // bytes so it is not throttled, but it is the other place a split spends real time, so the interrupt check
        // goes with it.
        ZeroCopySplitIndex.ChildIndex childIndex = indexWriter.writeChild(child, range, from, to, interrupt);
        components.addAll(childIndex.components);
        DecoratedKey first = childIndex.first;
        DecoratedKey last = childIndex.last;

        // ---------- Statistics.db ----------
        // plan.childLength, not physicalBytes: compressionRatio is compressed-over-uncompressed for the file, and
        // the alignment pad is on disk.
        writeStatistics(child, metadata, parentMetadata, parentStats, childIndex.partitionSizes,
                        childIndex.cardinality, plan.childLength, range.dataLength, first, last,
                        range.deadPrefixBytes, repairState);

        // ---------- Digest.crc32: CRC32 over EVERY physical byte of the child Data.db ----------
        // This is the one component whose cost is proportional to the data rather than the index.
        writeDigest(child, meta, range, plan, progress, interrupt);
        requireNonEmpty(child, Components.DIGEST);
        components.add(Components.DIGEST);

        // ---------- TOC.txt, last: it has to name every file that exists and no others ----------
        components.add(Components.TOC);
        TOCComponent.updateTOC(child, components);

        // Component CONTENTS are each fsynced already; this makes their DIRECTORY ENTRIES durable too, without which
        // a crash can leave a directory not listing a file whose data is on disk -- the same loss. Only the components
        // written through a SequentialWriter sync the directory themselves, on create (SequentialWriter.openChannel ->
        // trySyncDir): Statistics.db and Index.db. Must happen before the
        // child is published: the fsynced COMMIT record that unlinks the parent must not be first.
        SyncUtil.trySyncDir(child.directory);

        // Split children are produced by an offline tool. Keep the load offline too: an online load initializes the
        // global key cache and may rewrite a rejected Summary.db or Filter.db after those components were fsynced.
        SSTableReader reader = SSTableReader.open(parent.owner().orElse(null), child, components, parent.metadata,
                                                  true, true);
        try
        {
            validateChild(reader, range, plan, physicalBytes, partitionCount, chunkLength);
            if (failAfterChildOpenForTesting != null)
                failAfterChildOpenForTesting.accept(reader);

            if (progress != null)
            {
                long nonDataBytes = 0;
                for (Component component : components)
                {
                    if (!component.equals(Components.DATA))
                        nonDataBytes = saturatedAdd(nonDataBytes, child.fileFor(component).length());
                }
                progress.wrote(nonDataBytes);
            }

            return new Child(child, first, last, range, physicalBytes, plan.headPadBytes, cloned, partitionCount,
                             ImmutableSet.copyOf(components), reader);
        }
        catch (Throwable t)
        {
            reader.selfRef().release();
            throw t;
        }
    }

    // ---- Component writers ----
    // Several are package-private for the index writer and focused tests.

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
            int permitted = 0;
            while (remaining > 0)
            {
                int slice = (int) Math.min(remaining, TRANSFER_SLICE);
                // Between chunk transfers, which is the only place a verbatim copy can be interrupted: there is no
                // partition boundary inside it to stop cleanly at, and TRANSFER_SLICE is small precisely so that this
                // is asked often. Throttling follows, and only for bytes that are really about to move.
                interrupt.run();
                if (progress != null && permitted < slice)
                {
                    progress.throttle(slice - permitted);
                    permitted = slice;
                }
                long n = in.transferTo(position, slice, outChannel);
                if (n <= 0)
                    throw new IOException(String.format("short transferTo of %s at %d with %d left",
                                                        src, position, remaining));
                position += n;
                remaining -= n;
                if (progress != null)
                {
                    permitted -= (int) n;
                    progress.wrote(n);
                }
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
     * StatsMetadata; the per-table union in CommitLogReplayer stays bit-identical because IntervalSet.Builder.add is
     * normalising and idempotent. The bug this avoids is stamping the child with the LOCAL host id (which every
     * MetadataCollector constructor does) while inheriting a foreign parent's intervals: the replayer gates on
     * {@code originatingHostId.equals(localhostId)}, so it would read foreign segment ids against the local commitlog
     * and discard acked-but-unflushed mutations.
     * <p>
     * {@code repairedAt}/{@code pendingRepair}/{@code isTransient} come from the parent's {@code repairState} and are
     * written here rather than mutated afterwards so the reader opened a few lines later is already correct: the
     * Tracker routes a newly visible sstable to a compaction strategy holder by exactly this triple
     * ({@code CompactionStrategyManager.handleListChangedNotification}).
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
                                long firstPartitionPosition,
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
                                                     repairState.repairedAt,                      // inherited verbatim
                                                     parentStats.totalColumnsSet,                 // ACCEPTED: parent-wide
                                                     parentStats.totalRows,                       // ACCEPTED: parent-wide
                                                     // NOT inherited: the parent's coverage is its whole token
                                                     // range, so giving it to K children would multiply the table's
                                                     // apparent coverage and mislead the density calculations that
                                                     // drive compaction. NaN is MetadataCollector's "unknown";
                                                     // recomputing would need the local ranges.
                                                     Double.NaN,
                                                     parentStats.originatingHostId,               // atomic pair, see javadoc
                                                     repairState.pendingRepair,                   // inherited verbatim
                                                     repairState.isTransient,                     // inherited verbatim
                                                     parentStats.hasPartitionLevelDeletions,      // inherit: conservative direction
                                                     // The CHILD's own range: for a marked split child these
                                                     // outrank Summary.db in the reader's first/last, so inheriting
                                                     // would have every child claim the whole parent range and break
                                                     // range-based sstable selection.
                                                     childFirst.getKey(),
                                                     childLast.getKey(),
                                                     firstPartitionPosition);

        ValidationMetadata parentValidation = (ValidationMetadata) parentMetadata.get(MetadataType.VALIDATION);
        Map<MetadataType, MetadataComponent> components = new EnumMap<>(parentMetadata);
        components.put(MetadataType.STATS, childStats);
        components.put(MetadataType.COMPACTION, new CompactionMetadata(cardinality));
        // The filter was built from the live schema, so record that same FP chance rather than inheriting the value
        // from when the parent was written. HEADER still passes through byte-for-byte.
        components.put(MetadataType.VALIDATION,
                       new ValidationMetadata(parentValidation.partitioner, metadata.params.bloomFilterFpChance));

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
     * This pass is the dominant cost of a split whose extents were shared. Each copied chunk's inline CRC32 is checked
     * as its bytes pass through the same buffer, so corrupted clone/copy output cannot bless its own bad Digest.crc32.
     * The first partition key is checked separately against the rebuilt index to authenticate the selected chunk run.
     * A compressed SSTable is self-checking without the digest component, but writing it preserves the existing
     * verifier's fast whole-file check.
     * <p>
     * The value could instead be DERIVED with {@code crc32_combine} from the parent's inline per-chunk CRC32s, which
     * carry no offset or chunk index -- 4 bytes read per chunk plus the pad. Not implemented: it carries its own
     * correctness burden, and a wrong digest is silent until {@code nodetool verify}.
     */
    private static void writeDigest(Descriptor child,
                                    CompressionMetadata metadata,
                                    ChunkRange range,
                                    CopyPlan plan,
                                    Progress progress,
                                    Runnable interrupt) throws IOException
    {
        CRC32 crc = new CRC32();
        InlineChunkChecksumValidator chunks = new InlineChunkChecksumValidator(metadata, range, plan.headPadBytes);
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
                chunks.update(buffer, 0, n);
            }
        }
        chunks.finish();
        try (FileOutputStreamPlus out = new FileOutputStreamPlus(child.fileFor(Components.DIGEST)))
        {
            out.write(String.valueOf(crc.getValue()).getBytes(StandardCharsets.UTF_8));
            out.flush();
            out.sync();
        }
    }

    /** Validates every copied compressed chunk's inline CRC32 while the digest pass already has its bytes in hand. */
    private static final class InlineChunkChecksumValidator
    {
        private final CompressionMetadata metadata;
        private final long lastChunk;
        private final int chunkLength;
        private final CRC32 checksum = new CRC32();

        private long padRemaining;
        private long chunkIndex;
        private int contentRemaining;
        private int checksumBytes;
        private int storedChecksum;
        private boolean complete;

        private InlineChunkChecksumValidator(CompressionMetadata metadata, ChunkRange range, long headPadBytes)
        {
            this.metadata = metadata;
            this.lastChunk = range.lastChunk;
            this.chunkLength = range.chunkLength;
            this.padRemaining = headPadBytes;
            this.chunkIndex = range.firstChunk;
            this.contentRemaining = chunkFor(metadata, chunkIndex, chunkLength).length;
        }

        private void update(byte[] bytes, int offset, int length) throws IOException
        {
            int end = offset + length;
            if (padRemaining > 0)
            {
                int skipped = (int) Math.min(padRemaining, length);
                offset += skipped;
                padRemaining -= skipped;
            }

            while (offset < end)
            {
                if (complete)
                    throw new IOException("Data.db contains bytes after the final compressed chunk");

                if (contentRemaining > 0)
                {
                    int count = Math.min(contentRemaining, end - offset);
                    checksum.update(bytes, offset, count);
                    offset += count;
                    contentRemaining -= count;
                    continue;
                }

                while (offset < end && checksumBytes < Integer.BYTES)
                {
                    storedChecksum = (storedChecksum << Byte.SIZE) | (bytes[offset++] & 0xFF);
                    checksumBytes++;
                }
                if (checksumBytes == Integer.BYTES)
                    finishChunk();
            }
        }

        private void finishChunk() throws IOException
        {
            int calculated = (int) checksum.getValue();
            if (storedChecksum != calculated)
            {
                throw new IOException("Inline CRC32 mismatch in compressed chunk " + chunkIndex + ": stored " +
                                      storedChecksum + ", calculated " + calculated);
            }

            if (chunkIndex == lastChunk)
            {
                complete = true;
                return;
            }

            chunkIndex++;
            contentRemaining = chunkFor(metadata, chunkIndex, chunkLength).length;
            checksumBytes = 0;
            storedChecksum = 0;
            checksum.reset();
        }

        private void finish() throws IOException
        {
            if (padRemaining != 0 || !complete)
            {
                throw new IOException("Data.db ended before its compressed chunk range was complete (pad=" +
                                      padRemaining + ", chunk=" + chunkIndex + ", content=" + contentRemaining +
                                      ", checksumBytes=" + checksumBytes + ')');
            }
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

        // Decompress both ends. The first key authenticates that the requested parent chunk run, rather than some
        // other internally valid run, backs the rebuilt index. The final chunk is physically whole while the child's
        // dataLength says only part of it is live, so its length is derived rather than stored and a single byte of
        // trailing slack changes it -- something Digest.crc32 cannot catch either, being computed over whatever bytes
        // were written. Reading the last live byte forces CompressedChunkReader's normal path, where a wrong derived
        // length fails the inline CRC32 (or LZ4's "Compressed lengths mismatch").
        try (RandomAccessReader in = child.openDataReader())
        {
            in.seek(position);
            ByteBuffer dataKey = ByteBufferUtil.readWithShortLength(in);
            if (!dataKey.equals(child.getFirst().getKey()))
                throw new IllegalStateException("child first data key does not match its rebuilt index: " +
                                                child.getFirst());

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
     * Fresh BIG {@code pb} descriptors in the parent's directory. Prefers the live ColumnFamilyStore's id generator
     * so we cannot collide with a concurrent flush or compaction; the fallback is for offline use. Normal writers
     * deliberately remain on {@code pa}; only the splitter needs the appended Stats marker.
     */
    static Supplier<Descriptor> descriptorAllocator(SSTableReader parent)
    {
        Descriptor template = parent.descriptor;
        Version childVersion = splitVersion();
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
            return () -> store.newSSTableDescriptor(template.directory, childVersion);
        }

        Supplier<SSTableId> ids = new Directories(parent.metadata()).getUIDGenerator(SSTableIdFactory.instance.defaultBuilder());
        return () -> {
            for (int attempt = 0; attempt < 1000; attempt++)
            {
                Descriptor candidate = new Descriptor(childVersion, template.directory, template.ksname,
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
                                     .trickleFsyncByteInterval(DatabaseDescriptor.getTrickleFsyncIntervalInBytes())
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
     * may go on to reuse and commit it. An ADD record naming files this method has just deleted would be committed
     * with it, so each successfully deleted child is untracked as well as released. If deletion or untracking fails,
     * the transaction is aborted: its ADD record remains the recovery authority, and the caller cannot accidentally
     * commit an incomplete child while performing fallback work.
     */
    private static Throwable cleanUp(LifecycleTransaction txn,
                                     List<Child> children,
                                     List<Descriptor> created,
                                     List<PendingSSTable> tracked,
                                     Throwable failure)
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
                failure = Throwables.merge(failure, t);
            }
        }

        Set<Descriptor> deletionFailures = Sets.newHashSet();
        for (Descriptor descriptor : created)
        {
            for (Component component : WRITTEN_COMPONENTS)
            {
                try
                {
                    descriptor.fileFor(component).deleteIfExists();
                }
                catch (Throwable t)
                {
                    logger.warn("Failed deleting {} while cleaning up {}", component, descriptor, t);
                    deletionFailures.add(descriptor);
                    failure = Throwables.merge(failure, t);
                }
            }
            // Statistics.db is written in place now, not via rewriteSSTableMetadata's tmp file + rename, so this
            // should never exist. Kept as belt and braces: a leftover tmp would be picked up as an orphan.
            try
            {
                descriptor.tmpFileFor(Components.STATS).deleteIfExists();
            }
            catch (Throwable t)
            {
                logger.warn("Failed deleting temporary Statistics.db while cleaning up {}", descriptor, t);
                deletionFailures.add(descriptor);
                failure = Throwables.merge(failure, t);
            }
        }

        boolean abortTransaction = !deletionFailures.isEmpty();
        if (txn != null && !abortTransaction)
        {
            for (PendingSSTable pending : tracked)
            {
                try
                {
                    txn.untrackNew(pending);
                }
                catch (Throwable t)
                {
                    logger.warn("Failed untracking child {} during cleanup", pending.descriptor, t);
                    failure = Throwables.merge(failure, t);
                    abortTransaction = true;
                }
            }
        }
        if (txn != null && abortTransaction)
        {
            try
            {
                failure = Throwables.merge(failure, txn.abort(null));
            }
            catch (Throwable t)
            {
                logger.warn("Failed aborting transaction after incomplete split cleanup", t);
                failure = Throwables.merge(failure, t);
            }
        }
        children.clear();
        created.clear();
        tracked.clear();
        return failure;
    }
}
