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
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.zip.CRC32;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.RateLimiter;

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import com.clearspring.analytics.stream.cardinality.ICardinality;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.RowIndexEntry;
import org.apache.cassandra.db.compaction.CompactionInfo;
import org.apache.cassandra.db.compaction.CompactionInterruptedException;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.Version;
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
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.utils.BloomFilter;
import org.apache.cassandra.utils.BloomFilterSerializer;
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
 * <p>Chunk boundaries are pinned to multiples of {@code chunkLength} -- {@link CompressionMetadata#chunkFor(long)}
 * indexes the offsets array by {@code position / chunkLength}, and no per-chunk uncompressed length is stored -- so
 * a child can only be a verbatim run of whole chunks {@code [i, j]}. A child whose first partition does not sit on
 * a chunk boundary therefore carries a <em>dead prefix</em> of {@code lo mod chunkLength} bytes, since index
 * positions are rebased by {@code shift = i * chunkLength} rather than by {@code lo}. Read paths enter Data.db only
 * at positions taken from Index.db and tolerate it; {@code Scrubber} and {@code Verifier}, which walked linearly
 * from 0, were changed to seek to the first index position.
 *
 * <p>Where the filesystem can share extents, the child's Data.db is reflinked from the parent's rather than copied
 * (see {@link Reflink}), so the split writes no data blocks. That costs a <em>head pad</em>: {@code FICLONERANGE}
 * needs block-aligned offsets, so the range is extended back to the previous 64 KiB boundary and the child's
 * offsets rebased by it, leaving up to 64 KiB of the parent's previous chunk at the head with
 * {@code offsets[0] == pad}. A second, physical dead prefix, independent of the uncompressed one, and covered by
 * Digest.crc32 since {@code Verifier} checksums the whole file. {@link CopyPlan} is the arithmetic. A padded range
 * that ends up copied instead produces a byte-for-byte identical child, so the two paths cannot diverge.
 *
 * <p>Trailing slack is forbidden. The last chunk's length is derived as
 * {@code compressedFileLength - offsets[C-1] - 4}, so one extra byte inflates it and can flip the reader's
 * {@code length < maxCompressedLength} test, handing compressed bytes back as raw data. The child's Data.db is
 * truncated to exactly {@code headPad + O(j+1) - O(i)} and asserted.
 *
 * <p>Uncompressed sstables and tables with a secondary index are refused; see {@link #isSupported(SSTableReader)}.
 * JBOD is unsupported but NOT refused, being a deployment constraint: {@link #descriptorAllocator} allocates every
 * child in the PARENT's directory and never asks {@code Directories} for a location with room, so children cannot
 * spill to a sibling disk and a parent larger than the free space on its own disk will fill it.
 *
 * <p>Statistics.db is deliberately imprecise. Four {@code StatsMetadata} fields are per-sstable totals that cannot
 * be recomputed without deserialising rows, so every child inherits the parent-wide
 * {@code estimatedCellPerPartitionCount}, {@code totalRows}, {@code totalColumnsSet} and
 * {@code estimatedTombstoneDropTime} while {@code estimatedPartitionSize} is re-derived exactly. Per-table
 * aggregates then over-report by roughly K and {@code worthDroppingTombstones} under-fires by roughly K. Every
 * inherited value is at least as wide as the truth, so nothing can lose or resurrect data.
 *
 * <p>Every component is fsynced before {@code SSTableReader.open}, plus one {@link SyncUtil#trySyncDir} per child:
 * committing unlinks the parent and the COMMIT record is itself fsynced, so a component still in page cache at that
 * moment could be lost while the parent's removal survives. That is why Statistics.db is not written through
 * {@code MetadataSerializer.rewriteSSTableMetadata} and the filter and summary not through
 * {@code SSTableReader.saveBloomFilter}/{@code saveSummary} -- none of those fsync, and the latter two swallow the
 * IOException and delete the half-written file.
 *
 * <p>With a {@link Progress} the copy joins the compaction framework: visible in {@code nodetool compactionstats},
 * bounded by {@code compaction_throughput}, and stoppable. Without one -- offline tools and tests -- it runs
 * unthrottled.
 */
public final class ZeroCopySSTableSplitter
{
    private static final Logger logger = LoggerFactory.getLogger(ZeroCopySSTableSplitter.class);

    /** Prefix of the refusal for an uncompressed parent, so tests need not match the whole sentence. */
    public static final String UNCOMPRESSED_UNSUPPORTED_MESSAGE =
        "ZeroCopySSTableSplitter requires a compressed sstable";

    /**
     * One {@code transferTo} slice. Deliberately small: it is the granularity at which {@link Progress}
     * throttles against {@code compaction_throughput} and notices a stop request, so a multi-GiB slice would
     * make the copy effectively unthrottled and uninterruptible.
     */
    private static final int TRANSFER_SLICE = 4 << 20;

    /** Same buffer size the digest/checksum writers use. */
    private static final int COPY_BUFFER_SIZE = 64 * 1024;

    /** Alignment the head pad is computed against; see {@link Reflink#RANGE_ALIGNMENT}. */
    private static final long CLONE_ALIGNMENT = Reflink.RANGE_ALIGNMENT;

    /**
     * A child smaller than this is copied rather than shared: the head pad costs disk space and a longer digest
     * pass, so sharing only pays when the range dwarfs it. 1 MiB is 16 times the pad, a 6% overhead ceiling at
     * the very bottom of the range.
     */
    private static final long MIN_CLONE_BYTES = 1L << 20;

    /**
     * Test hook: lay every child out as if extent sharing were available -- head pad and all -- so the aligned
     * layout is covered on filesystems that cannot share extents, i.e. every laptop and CI box. Also lifts
     * {@link #MIN_CLONE_BYTES}, since test sstables are smaller than that. The copy mechanism is unaffected.
     */
    @VisibleForTesting
    static volatile boolean forceAlignedLayoutForTesting = false;

    /** {@code MetadataCollector.defaultPartitionSizeHistogram()} is package-private; this is bit-identical. */
    static final int PARTITION_SIZE_HISTOGRAM_BUCKETS = 150;

    /** {@code MetadataCollector.cardinality} is {@code new HyperLogLogPlus(13, 25)} (CASSANDRA-5906). */
    static final int HLL_P = 13;
    static final int HLL_SP = 25;

    /** Every component this class can write, i.e. everything {@link #cleanUp} has to remove. */
    private static final List<Component> WRITTEN_COMPONENTS = ImmutableList.of(Component.DATA,
                                                                               Component.PRIMARY_INDEX,
                                                                               Component.COMPRESSION_INFO,
                                                                               Component.STATS,
                                                                               Component.SUMMARY,
                                                                               Component.FILTER,
                                                                               Component.DIGEST,
                                                                               Component.TOC);

    private ZeroCopySSTableSplitter()
    {
    }

    // ------------------------------------------------------------------------------------------------
    // Arithmetic. Deliberately static and free of any sstable dependency so it can be unit tested alone.
    // ------------------------------------------------------------------------------------------------

    /** Index of the compression chunk containing {@code uncompressedPosition}, as {@code chunkFor} computes it. */
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
     * {@code (hi - 1) / L}, not {@code hi / L}: when {@code hi} lands on a chunk boundary the final chunk is the
     * one <em>before</em> it, and {@code hi / L} would read a chunk too far.
     */
    public static long lastChunk(long hi, int chunkLength)
    {
        checkChunkLength(chunkLength);
        if (hi <= 0)
            throw new IllegalArgumentException("child must contain at least one byte, hi=" + hi);
        return (hi - 1) / chunkLength;
    }

    /**
     * The child's {@code CompressionInfo.dataLength}: from the start of its first chunk to the end of its last
     * live partition. No trailing slack -- {@code getPositionsForRanges} bounds on {@code uncompressedLength()}.
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
     * The whole chunk-range computation for one child, as an immutable value.
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
     * Where the child's Data.db comes from and how it gets there: the physical half of the arithmetic, and the only
     * part that knows about extent sharing.
     * <p>
     * {@code FICLONERANGE} needs source offset, destination offset and length all aligned, and a chunk boundary is
     * aligned to nothing. We control the destination offset and the length but not the source, so the copy is
     * extended BACKWARDS to the previous alignment boundary and the child's offsets are rebased by it, putting
     * {@code pad = O(i) mod A} bytes of the parent's previous chunk at the head of the child. That is a physical
     * dead prefix, distinct from {@link ChunkRange#deadPrefixBytes}, which lives in uncompressed space.
     * <p>
     * {@code cloneLength} is the aligned part of the child's length; the remaining {@code tailLength < A} bytes are
     * copied conventionally. Rounding the clone up and truncating instead would work on xfs, but would depend on
     * truncate unsharing a partially shared final block for the sake of a sub-64-KiB copy.
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
        // Aligned down, so the clone can never reach past the child's last live byte into the parent's trailing
        // slack -- which chunkEnd() exists to keep out of the child.
        long cloneLength = share ? childLength - (childLength & (CLONE_ALIGNMENT - 1)) : 0;
        return new CopyPlan(copyFrom - pad, pad, childLength, cloneLength);
    }

    /** Immutable result of {@link #copyPlan(long, long, boolean, boolean)}. */
    public static final class CopyPlan
    {
        /** Parent offset the child's byte 0 is taken from: {@code O(i) - headPadBytes}, alignment-aligned. */
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
     * Repair state to stamp into one child's Statistics.db instead of inheriting the parent's. Written by
     * {@link #writeStatistics} <em>before</em> the child reader is opened, so the reader is born with the right
     * state and no {@code mutateRepairedAndReload} is needed.
     * <p>
     * The invariants checked here are the ones {@code CompactionStrategyHolder.managesRepairedGroup} and
     * {@code PendingRepairHolder.managesRepairedGroup} assert when the Tracker routes a newly visible sstable to
     * a strategy holder -- failing there means an {@code IllegalArgumentException} from inside a Tracker
     * notification, a far worse place to find out.
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
         * The state the overloads without an explicit one give every child: the parent's, verbatim and
         * deliberately unvalidated, so they behave as they did before per-child repair state existed.
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
        public final long firstChunk;
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
         * Bytes of the parent's PREVIOUS chunk at the head of this child's Data.db, there so its first chunk
         * lands on an alignment boundary; also the child's {@code offsets[0]}. Zero unless the child's extents
         * were (or were meant to be) shared with the parent. See {@link CopyPlan}.
         */
        public final long headPadBytes;
        /** Bytes of {@link #physicalBytes} that were shared with the parent instead of copied. */
        public final long clonedBytes;
        public final long partitionCount;
        /** Components written for the child; the exact set passed to {@code SSTableReader.open}. */
        public final Set<Component> components;
        /**
         * The repair state actually stamped into this child's Statistics.db: the state of the boundary range it
         * came from, carried through rather than positionally re-derived, so an empty range that produced no
         * child cannot shift the pairing.
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
         * Sum of every child's live chunk bytes, {@code O(j+1) - O(i)}: the data the split had to account for,
         * NOT what it moved -- subtract {@link #totalBytesCloned} for that.
         */
        public final long totalPhysicalBytesCopied;
        public final long totalDeadPrefixBytes;
        /** Sum of every child's head pad, i.e. the disk space alignment cost. */
        public final long totalHeadPadBytes;
        /**
         * Bytes shared with the parent as copy-on-write extents rather than copied, costing neither I/O nor disk
         * space. Zero where the filesystem cannot share extents; otherwise within one alignment unit per child
         * of {@code totalPhysicalBytesCopied + totalHeadPadBytes}.
         */
        public final long totalBytesCloned;
        /**
         * Compressed bytes physically present in two children because a split boundary fell inside a chunk.
         * Bounded by one chunk per interior boundary, and free rather than merely bounded when the extents are
         * shared: both children point at the same physical chunk.
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
     * Makes one split a first-class member of the compaction framework rather than an invisible, unbounded burst of
     * I/O: visible in {@code nodetool compactionstats}, bounded by {@code compaction_throughput} via the compaction
     * {@link RateLimiter}, and stoppable by everything that walks {@code active.getCompactions()} and calls
     * {@link CompactionInfo.Holder#stop()}. The {@link CompactionInfo} carries the parent sstable so
     * {@code shouldStop} can match it.
     * <p>
     * A verbatim chunk copy has no partition boundary to stop cleanly at, so the stop check lives inside the
     * transfer loop and aborts the split outright. A caller must NOT answer that by falling back to the rewrite --
     * the operator asked for the work to stop, not to be done a more expensive way.
     * <p>
     * {@code total} is deliberately an estimate: a boundary chunk lands in two children and an aligned child carries
     * a head pad, so a split with many interior boundaries can report marginally over 100%. The digest pass is only
     * counted when it will actually happen, otherwise a split would peg at 50% and finish. Shared bytes count
     * towards {@code total} for the same reason but are not rate limited -- see {@link #cloned}.
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
            return new CompactionInfo(metadata, OperationType.ANTICOMPACTION, completed.get(), total, id, parent);
        }

        /** One sstable of one table, so a paused global compaction must not silently stop it. */
        @Override
        public boolean isGlobal()
        {
            return false;
        }

        /**
         * Called immediately BEFORE {@code bytes} move: throws if a stop was requested, then blocks until the
         * rate limiter lets the slice through. Permits cover the whole slice even though {@code transferTo} may
         * move fewer, which over-throttles by at most one slice per short count.
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
         * Bytes accounted for by sharing extents rather than moving them. Deliberately NOT rate limited:
         * {@code compaction_throughput} bounds disk traffic and a clone generates none, so charging it would make
         * a reflink split as slow as the copy it replaced. Still counted towards {@code total} so
         * {@code compactionstats} reaches 100%.
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
     * @return true iff {@link #split} can handle this parent: a compressed BIG-format sstable, at a version whose
     *         components this class can write ({@link #writesReadableComponents}), on a table with no secondary
     *         index ({@link #hasNoPerSSTableIndex}). Anything else is refused by {@link #requireSupported} with
     *         {@link UnsupportedOperationException}.
     */
    public static boolean isSupported(SSTableReader parent)
    {
        return parent.descriptor.formatType == SSTableFormat.Type.BIG
               && parent.compression
               && writesReadableComponents(parent.descriptor.version)
               && hasNoPerSSTableIndex(parent);
    }

    /**
     * Whether the components this class writes can be read back at {@code version}.
     * <p>
     * A child keeps the PARENT's version, since its Data.db is the parent's bytes verbatim. Two of the component
     * writers are version-blind though: {@link CompressionMetadata.Writer} always emits the
     * {@code maxCompressedLength} field only {@code na}+ reads back, and {@link BloomFilterSerializer} always
     * writes the 4.0 bit order only {@code na}+ expects (CASSANDRA-9067). Stamped with a 3.x version those are
     * read back wrong rather than rejected -- CompressionInfo.db is parsed four bytes out of phase, making
     * {@code chunkCount} the low half of {@code dataLength} and turning {@code open} into a multi-gigabyte
     * {@code Memory.allocate} then {@code CorruptSSTableException}.
     * <p>
     * Rather than teach those writers to downgrade, refuse: a 3.x sstable is one {@code upgradesstables} away and
     * every caller's fallback is a rewrite, which produces a current-version sstable anyway. Statistics.db is not
     * at issue -- {@link #writeStatistics} passes {@code child.version} to the metadata serializer.
     */
    static boolean writesReadableComponents(Version version)
    {
        return version.hasMaxCompressedLength() && !version.hasOldBfFormat();
    }

    /**
     * Whether the parent's table is free of secondary indexes, which a split cannot carry across.
     * <p>
     * The rewrite this replaces hands {@code cfs.indexManager.listIndexes()} to {@code SSTableWriter.create}, so an
     * index with per-sstable state gets an {@code SSTableFlushObserver} and its component is written alongside each
     * output. SASI is the one such index in this tree, and rebuilding its {@code SI_*.db} means reading the rows --
     * the entire cost this class exists to avoid. Emitting children without it fails silently rather than loudly:
     * {@code ColumnIndex.update} drops the un-indexed set {@code DataTracker.update} returns and
     * {@code getBuiltIndexes} skips any sstable whose index file is absent, so queries just stop matching those
     * partitions until a restart or {@code rebuild_index}.
     * <p>
     * This refuses on ANY index, not only those with per-sstable components. A plain {@code CassandraIndex} keeps
     * its data in a separate table and would survive a split untouched, so that is stricter than necessary -- but
     * it is the cheap, obviously-correct test, it needs no {@code ColumnFamilyStore} so it holds offline too, and
     * being wrong this way only costs such a table the rewrite it did before this existed.
     */
    static boolean hasNoPerSSTableIndex(SSTableReader parent)
    {
        return parent.metadata().indexes.isEmpty();
    }

    /**
     * The least {@link SSTable} that can carry a child's identity into the transaction log before any of its files
     * exist, so a crash mid-split is cleaned up rather than half-adopted. See the call site in {@link #buildChild}.
     * <p>
     * {@code LogRecord.make(ADD, table)} reads only {@code baseFilename()} and {@code getAllFilePaths().size()},
     * and the record's file list is rebuilt by listing the directory at replay, so the component set here only has
     * to be non-empty -- it is not a claim about what the child will have.
     */
    private static final class PendingChild extends SSTable
    {
        PendingChild(Descriptor descriptor, TableMetadataRef metadata)
        {
            super(descriptor, ImmutableSet.of(Component.DATA), metadata,
                  DatabaseDescriptor.getDiskOptimizationStrategy());
        }
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
        // Three sequential Index.db passes, none retaining anything per partition: count, select, build. The count
        // comes first because split-point selection needs the exact partition count up front for its tail-room
        // clamp. Index.db is a couple of percent of Data.db, so the extra pass is cheap next to copying the chunk
        // runs -- and it is what keeps heap at O(numChildren) instead of O(partitions). See RunSelector.
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
     * Split at explicit boundaries, stamping a caller-supplied repair state into each child instead of inheriting
     * the parent's. Boundary semantics are those of {@link #split(SSTableReader, List, LifecycleTransaction)}:
     * child {@code b} covers keys {@code [boundaries[b-1], boundaries[b])} and {@code perChild.get(b)} is the
     * state for that range.
     * <p>
     * <b>Pairing.</b> An empty boundary range still produces no child, so {@code result.children.size()} may be
     * smaller than {@code perChild.size()}. The state is therefore carried with the range rather than re-derived
     * afterwards, and what was written is exposed on {@link Child#repairState}. Pairing {@code children} against
     * {@code perChild} positionally is only valid when every range is known to be non-empty.
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
        // Two passes: the run starts fall out of the same walk that resolves the boundaries, so this form needs no
        // counting pass.
        Runs runs = selectByBoundaries(parent, boundaries);
        return build(parent, runs, perChild, txn, progress, start);
    }

    private static void requireSupported(SSTableReader parent)
    {
        Preconditions.checkNotNull(parent, "parent");
        if (parent.descriptor.formatType != SSTableFormat.Type.BIG)
            throw new UnsupportedOperationException("ZeroCopySSTableSplitter only supports the BIG sstable " +
                                                    "format, got " + parent.descriptor.formatType);
        if (!parent.compression)
            throw new UnsupportedOperationException(UNCOMPRESSED_UNSUPPORTED_MESSAGE + ": " + parent.descriptor +
                                                    " has no CompressionInfo.db. An uncompressed split is a " +
                                                    "different algorithm -- the cut is exact rather than " +
                                                    "chunk-aligned, and CRC.db (whose 64KiB grid is addressed " +
                                                    "from origin 0) has to be regenerated wholesale rather " +
                                                    "than sliced. Refusing rather than emitting a child with " +
                                                    "a misaligned CRC.db.");
        if (!writesReadableComponents(parent.descriptor.version))
            throw new UnsupportedOperationException("ZeroCopySSTableSplitter cannot write components for sstable " +
                                                    "version " + parent.descriptor.version + " (" +
                                                    parent.descriptor + "): a child keeps its parent's version, " +
                                                    "but CompressionInfo.db and Filter.db are written in the 'na'+ " +
                                                    "formats only. Run nodetool upgradesstables first.");
        if (!hasNoPerSSTableIndex(parent))
            throw new UnsupportedOperationException("ZeroCopySSTableSplitter cannot split " + parent.descriptor +
                                                    ": table " + parent.metadata().keyspace + '.' +
                                                    parent.metadata().name + " has secondary indexes " +
                                                    parent.metadata().indexes.stream()
                                                          .map(i -> i.name).collect(Collectors.joining(", ")) +
                                                    ", whose per-sstable components a split cannot rebuild " +
                                                    "without reading the rows.");
        if (!parent.descriptor.fileFor(Component.STATS).exists())
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
     * <p>Deliberately does not hand back the positions. Collecting every partition's offset into a {@code long[]}
     * is 8 bytes per partition (16-24 at the peak of a doubling), which is invisible on a 512 MiB parent and tens
     * of gigabytes of heap on a terabyte of 1 KiB partitions -- for an array whose every access is sequential
     * anyway. Downstream takes what it needs from the stream: {@link RunSelector} keeps O(numChildren) and
     * {@link #buildChild} keeps one record of lookback.
     *
     * @return the exact number of records
     */
    private static int walkIndex(SSTableReader parent, IndexRecordConsumer consumer)
    {
        long count = 0;
        // A buffered reader rather than an mmap, so no record can straddle a mapping boundary.
        try (RandomAccessReader in = RandomAccessReader.open(parent.descriptor.fileFor(Component.PRIMARY_INDEX)))
        {
            long indexSize = in.length();
            while (in.getFilePointer() != indexSize)
            {
                ByteBuffer key = ByteBufferUtil.readWithShortLength(in);
                long position = RowIndexEntry.Serializer.readPosition(in);
                int promotedSize = (int) in.readUnsignedVInt();
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
            throw new CorruptSSTableException(e, parent.descriptor.filenameFor(Component.PRIMARY_INDEX));
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
     * {@link #build} needs a run's {@code lo} before it can copy that child's chunks, so these cannot be recovered
     * during the build pass -- but there are only ever {@code numChildren} of them.
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
     * The explicit-boundary form: the run starts fall out of the same walk that compares keys against the
     * boundaries, so this costs one pass, no extra reads, and no retained keys.
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
     * Streaming form of {@link #chooseByByteShare}: fed every partition's Data.db offset in order, it produces the
     * same run starts plus each run's first offset in O(numChildren) heap rather than O(partitions).
     *
     * <p>The selection is a forward scan with one record of lookback; only its two clamps needed random access,
     * and both reach a bounded distance:
     * <ul>
     *   <li><b>Tail room</b> ({@code min(candidate, partitionCount - (numChildren - m))}) can only name one of the
     *       last {@code numChildren} records, whose offsets are kept in {@link #tail}.</li>
     *   <li><b>Non-empty</b> ({@code max(candidate, runStarts[m - 1] + 1)}) only binds when the natural candidate
     *       has not advanced past the previous run's start, so the record it names is at most one past the cursor.
     *       When it is exactly one past, the offset is filled in by a later {@link #offer}; those deferrals are
     *       contiguous, so one pointer tracks them.</li>
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

            // Several targets can fall inside one partition, so keep placing until this record is short of the next.
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
            // real record. position and target go unread -- the snap-back is guarded on candidate < partitionCount.
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
            // the tail window, never below the clamp above, since runStarts[m - 1] is itself bounded by
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
     * Reference implementation of split-point selection, kept because it reads far more easily than
     * {@link RunSelector}, which is tested by asserting it agrees with this on randomised inputs. Not used in
     * production: it needs every partition's offset at once, the allocation this class exists to avoid.
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

        // The offsets table must address every chunk the data needs, and is allowed to hold MORE: a
        // compaction-produced sstable carries one extra zero-uncompressed-length chunk, because
        // SSTableRewriter.doPrepare syncs the data file twice and CompressedSequentialWriter.flushData appends a
        // chunk unconditionally, even on an empty buffer. Keeping those bytes out of the children is chunkEnd()'s
        // job. Fewer entries than the data needs means the parent's CompressionInfo.db disagrees with its own
        // dataLength and nothing here is safe.
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
        try (RandomAccessReader index = RandomAccessReader.open(parent.descriptor.fileFor(Component.PRIMARY_INDEX)))
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
                // data. An empty trailing run has runStarts == partitionCount, which is exactly the case that takes
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

                // Carried with the range, never re-derived positionally: an empty range above produced no child and
                // must not shift the state of the ranges after it.
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
     * Derived from the chunk itself and deliberately never from the physical file length, which for a
     * compaction-produced parent sits ~9 bytes past the last real chunk (see {@link #build}). Taking the file length
     * as the end of the final chunk made the last child copy that slack, inflating its own last chunk's derived
     * length and failing CRC32 on every read of it -- or, once the length crossed {@code maxCompressedLength},
     * returning compressed bytes as row data. Silent, since Digest.crc32 covers whatever was written.
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
        Set<Component> components = Sets.newHashSet(Component.DATA,
                                                              Component.PRIMARY_INDEX,
                                                              Component.COMPRESSION_INFO,
                                                              Component.STATS,
                                                              Component.SUMMARY);

        // ---------- The transaction's ADD record, BEFORE the first byte of the child exists ----------
        // Same as BigTableWriter registering in its constructor ("must track before any files are created"), and
        // for the same reason: the ADD record is the ONLY thing that makes the child's files visible to
        // LogTransaction.removeUnfinishedLeftovers after a crash. Registering once the components are written
        // leaves a multi-minute window in which a kill -9 strands files no boot path reclaims --
        // removeUnfinishedLeftovers skips them for want of a record and scrubDataDirectories' orphan sweep keeps
        // any descriptor with a non-empty Data.db. A complete stranded child is then opened as a live sstable
        // ALONGSIDE the parent it was meant to replace, so the same partitions exist twice in two repair states;
        // one interrupted inside writeStatistics leaves a durable zero-length Statistics.db, which open() turns
        // into a CorruptSSTableException the startup failure policy escalates on every boot. And with extents
        // shared, a stranded Data.db pins the parent's blocks.
        //
        // The record needs nothing but the descriptor: LogRecord.make reads the base filename and component count,
        // the files it deletes are found by listing the directory at replay, and LogFile's numFiles strictness is
        // REMOVE-only.
        if (txn != null)
            txn.trackNew(new PendingChild(child, parent.metadata));

        // ---------- Data.db: verbatim compressed chunk run, shared with the parent where possible ----------
        // Sharing needs the head of the run aligned, which costs a pad, so it is only planned for when the
        // filesystem has not already said no. An unpadded run cannot be shared at all (O(i) is aligned to nothing),
        // so the decision has to be made before the copy rather than after it fails.
        boolean canShare = DatabaseDescriptor.getZeroCopySplitReflinkEnabled()
                           && Reflink.isPossibleIn(child.directory);
        boolean align = forceAlignedLayoutForTesting || (canShare && physicalBytes >= MIN_CLONE_BYTES);
        CopyPlan plan = copyPlan(copyFrom, physicalBytes, align, align && canShare);
        long cloned = copyData(parent.descriptor.fileFor(Component.DATA), child.fileFor(Component.DATA),
                               child.directory, plan, progress);
        long actual = child.fileFor(Component.DATA).length();
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
        // fpChance == 1.0 yields an AlwaysPresentFilter, which writeFilter's BloomFilter cast would fail on.
        // The read path already treats a missing Filter.db as always-present, so just omit the component.
        IFilter bf = fpChance < 1.0 ? FilterFactory.getFilter(partitionCount, fpChance) : null;
        DecoratedKey first = null;
        DecoratedKey last = null;

        try
        {
            try (SequentialWriter out = new SequentialWriter(child.fileFor(Component.PRIMARY_INDEX), writerOption());
                 IndexSummaryBuilder summary = new IndexSummaryBuilder(partitionCount,
                                                                       metadata.params.minIndexInterval,
                                                                       Downsampling.BASE_SAMPLING_LEVEL))
            {
                long previousPosition = UNRESOLVED;
                for (int r = from; r < to; r++)
                {
                    ByteBuffer key = ByteBufferUtil.readWithShortLength(index);
                    long position = RowIndexEntry.Serializer.readPosition(index);
                    int promotedSize = (int) index.readUnsignedVInt();
                    byte[] promoted = null;
                    if (promotedSize > 0)
                    {
                        promoted = new byte[promotedSize];
                        index.readFully(promoted);
                    }

                    // Selection and this pass have to land on the same records. Checking the run's first offset
                    // against what selection recorded, plus strict monotonicity from there on, catches a
                    // desynchronised walk without keeping an offset per partition, and rules out a non-increasing
                    // parent index as well.
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
                        // exact estimatedPartitionSize: rowSize_i == position_{i+1} - position_i identically, so
                        // each partition is sized one record late, from the next record's offset
                        partitionSizes.add(position - previousPosition);
                    }
                    previousPosition = position;

                    DecoratedKey dk = parent.getPartitioner().decorateKey(key);
                    // MetadataCollector.addKey hashes the raw key bytes, position/remaining passed explicitly
                    long hashed = MurmurHash.hash2_64(key, key.position(), key.remaining(), 0);

                    long childIndexStart = out.position();
                    ByteBufferUtil.writeWithShortLength(key, out);
                    // The ONLY rewritten field. Canonical minimal vint, never padded, so the child's records are
                    // shorter than the parent's and its index offsets are NOT the parent's minus a constant.
                    out.writeUnsignedVInt(position - range.shift);
                    out.writeUnsignedVInt(promotedSize);
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

                // The run's last partition ends where the next run's first record starts, which for the last run is
                // the end of the parent's data -- exactly what chunkRange() was handed as hi.
                if (range.hi <= previousPosition)
                    throw new IllegalStateException("run ends at " + range.hi + " but its last record is at " +
                                                    previousPosition);
                partitionSizes.add(range.hi - previousPosition);
                out.finish();

                first = SSTable.getMinimalKey(first);
                last = SSTable.getMinimalKey(last);
                try (IndexSummary built = summary.build(parent.getPartitioner()))
                {
                    writeSummary(child, first, last, built);
                }
            }
            requireNonEmpty(child, Component.SUMMARY);

            // ---------- Filter.db ----------
            if (bf != null)
            {
                writeFilter(child, bf);
                requireNonEmpty(child, Component.FILTER);
                components.add(Component.FILTER);
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
        writeStatistics(child, parentMetadata, parentStats, partitionSizes, cardinality,
                        plan.childLength, range.dataLength, repairState);

        // ---------- Digest.crc32: CRC32 over EVERY physical byte of the child Data.db ----------
        // Optional, and the one component whose cost scales with the DATA rather than the index: with extents
        // shared this read is the whole remaining cost of the split. Skipping it is a supported configuration --
        // see writeDigest and Config.zero_copy_split_digest_enabled.
        if (DatabaseDescriptor.getZeroCopySplitDigestEnabled())
        {
            writeDigest(child, progress);
            requireNonEmpty(child, Component.DIGEST);
            components.add(Component.DIGEST);
        }

        // ---------- TOC.txt, last: appendTOC opens in APPEND mode so it must run exactly once ----------
        components.add(Component.TOC);
        SSTable.appendTOC(child, components);

        // Every component's CONTENTS are fsynced individually above; this makes their DIRECTORY ENTRIES durable
        // too, since a directory that does not list a file whose data is on disk loses it just the same. Only the
        // components written through SequentialWriter (Index.db, Statistics.db) sync the directory themselves, on
        // create; Data.db, Filter.db, Summary.db, Digest.crc32 and TOC.txt do not. This has to happen before the
        // child is published: the transaction's COMMIT record is itself fsynced and unlinks the parent.
        SyncUtil.trySyncDir(child.directory);

        SSTableReader reader = SSTableReader.open(child, components, parent.metadata);
        try
        {
            validateChild(reader, range, plan, physicalBytes, partitionCount, chunkLength);
        }
        catch (Throwable t)
        {
            reader.selfRef().release();
            throw t;
        }

        // Deliberately no trackNew(reader): the ADD record for this descriptor went in before the copy started, and
        // trackNew does nothing but write that record, keyed on the base filename.

        return new Child(child, first, last, range, physicalBytes, plan.headPadBytes, cloned, partitionCount,
                         ImmutableSet.copyOf(components), repairState, reader);
    }

    // ------------------------------------------------------------------------------------------------
    // Component writers
    // ------------------------------------------------------------------------------------------------

    /**
     * Materialise the child's Data.db as the verbatim parent byte range
     * {@code [plan.srcStart, plan.srcStart + plan.childLength)}, sharing as much as the filesystem allows and
     * copying the rest.
     * <p>
     * The clone is all-or-nothing: {@code FICLONERANGE} either shares every byte asked for or writes nothing, so a
     * refusal costs one syscall and falls through to the transfer loop.
     * <p>
     * transferTo returns short counts and caps near 0x7ffff000, so it MUST be looped; {@code n <= 0} means EOF, not
     * "retry". The loop is also where the operation is throttled and cancelled, one {@link #TRANSFER_SLICE} at a
     * time. A clone moves no bytes, so it is checked for cancellation but not throttled.
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

            // The ioctl does not move the destination's file position and transferTo writes at wherever that is, so
            // the tail has to be positioned explicitly. Without this it would overwrite the head of the range just
            // shared -- which, being copy-on-write, would silently succeed.
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
     * Child CompressionInfo.db via the same {@code Writer} every real sstable uses, so it cannot drift from the
     * format. Only dataLength, chunkCount and the offsets differ from the parent.
     * <p>
     * Offsets are rebased by {@link CopyPlan#srcStart} rather than {@code O(i)}, so the child's {@code offsets[0]}
     * is its head pad instead of 0 whenever the run was aligned for sharing. They remain absolute positions in the
     * child's own Data.db, and the last chunk's derived length is unaffected because the pad shifts both terms.
     */
    private static void writeCompressionInfo(Descriptor child, CompressionMetadata meta, ChunkRange range,
                                             CopyPlan plan)
    {
        CompressionMetadata.Writer writer =
            CompressionMetadata.Writer.open(meta.parameters, child.filenameFor(Component.COMPRESSION_INFO));
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
            child.fileFor(Component.COMPRESSION_INFO).deleteIfExists();
            throw t;
        }
        finally
        {
            writer.close();
        }
    }

    /**
     * The child's Statistics.db: the parent's four components with two derived replacements
     * (estimatedPartitionSize and the COMPACTION cardinality) plus a recomputed compressionRatio.
     * <p>
     * HEADER passes through by reference and MUST be inherited byte-for-byte: rows in the copied Data.db encode
     * timestamps/localDeletionTime/TTL as unsigned vint deltas off
     * {@code stats.minTimestamp/minLocalDeletionTime/minTTL}, and their columns as a bitmap subset of
     * {@code header.columns()}. Tightening any of those silently corrupts every relocated row, with all CRCs still
     * passing.
     * <p>
     * {@code commitLogIntervals} and {@code originatingHostId} are inherited as an ATOMIC PAIR. Stamping the child
     * with the LOCAL host id, as every MetadataCollector constructor does, while inheriting a foreign parent's
     * intervals would have CommitLogReplayer -- which gates on {@code originatingHostId.equals(localhostId)} --
     * interpret foreign segment ids against the local commitlog and discard acked-but-unflushed mutations.
     * <p>
     * The repair state is written here rather than mutated afterwards so the reader opened a few lines later is
     * already correct: the Tracker routes a newly visible sstable to a strategy holder by exactly that triple.
     * {@code sstableLevel} is inherited, as {@code createWriterForAntiCompaction} does for a single-input
     * anticompaction -- safe because the children are disjoint contiguous sub-ranges of the parent's range.
     */
    static void writeStatistics(Descriptor child,
                                        Map<MetadataType, MetadataComponent> parentMetadata,
                                        StatsMetadata parentStats,
                                        EstimatedHistogram partitionSizes,
                                        ICardinality cardinality,
                                        long onDiskLength,
                                        long dataLength,
                                        RepairState repairState) throws IOException
    {
        // The four absolute TOTALS below (estimatedCellPerPartitionCount, estimatedTombstoneDropTime,
        // totalColumnsSet, totalRows) are parent-wide in every child, so per-table aggregates over-report by ~K and
        // worthDroppingTombstones under-fires by ~K. Accepted and conservative; see "Accepted imprecision in the
        // children's Statistics.db" on the class javadoc.
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
                                                     parentStats.minClusteringValues,
                                                     parentStats.maxClusteringValues,
                                                     parentStats.hasLegacyCounterShards,
                                                     repairState.repairedAt,                      // CALLER SUPPLIED
                                                     parentStats.totalColumnsSet,                 // ACCEPTED: parent-wide
                                                     parentStats.totalRows,                       // ACCEPTED: parent-wide
                                                     parentStats.originatingHostId,               // atomic pair, see javadoc
                                                     repairState.pendingRepair,                   // CALLER SUPPLIED
                                                     repairState.isTransient);                    // CALLER SUPPLIED

        Map<MetadataType, MetadataComponent> components = new EnumMap<>(parentMetadata);
        components.put(MetadataType.STATS, childStats);
        components.put(MetadataType.COMPACTION, new CompactionMetadata(cardinality));
        // VALIDATION (partitioner + fp chance) and HEADER pass through by reference: no schema lookup,
        // nothing that can throw, byte-identical to the parent.

        // Written the way BigTableWriter.writeMetadata does -- SequentialWriter plus finish() -- and NOT through
        // MetadataSerializer.rewriteSSTableMetadata, which only flushes and renames, fsyncing neither the file nor
        // the directory. That is fine for its existing callers, which mutate the repair status of an sstable whose
        // Statistics.db is ALREADY durable, and not fine here: this is the only copy of the child's
        // SerializationHeader and repair state. finish() ends in syncInternal() and SequentialWriter fsyncs the
        // directory on create, so both are durable before the COMMIT record unlinks the parent.
        File file = child.fileFor(Component.STATS);
        try (SequentialWriter out = new SequentialWriter(file, writerOption()))
        {
            child.getMetadataSerializer().serialize(components, out, child.version);
            out.finish();
        }
        requireNonEmpty(child, Component.STATS);
    }

    /**
     * Filter.db, fsynced. {@code BigTableWriter.IndexWriter.flushBf} rather than
     * {@code SSTableReader.saveBloomFilter}, which neither fsyncs nor reports failure: it logs at TRACE, deletes
     * the half-written file and returns normally, so {@code open()} would quietly rebuild the filter and hide the
     * error, and a crash could leave a torn one behind.
     */
    static void writeFilter(Descriptor child, IFilter filter) throws IOException
    {
        try (FileOutputStreamPlus out = new FileOutputStreamPlus(child.fileFor(Component.FILTER)))
        {
            BloomFilterSerializer.serialize((BloomFilter) filter, out);
            out.flush();
            out.sync();
        }
    }

    /**
     * Summary.db, fsynced. {@code SSTableReader.saveSummary} writes the same three things but never fsyncs and
     * swallows the failure. A torn Summary.db is the most survivable of the three -- {@code SSTableReaderBuilder}
     * rebuilds it from Index.db -- but that means a full Index.db pass per child at startup.
     */
    static void writeSummary(Descriptor child, DecoratedKey first, DecoratedKey last, IndexSummary summary)
    throws IOException
    {
        try (FileOutputStreamPlus out = new FileOutputStreamPlus(child.fileFor(Component.SUMMARY)))
        {
            IndexSummary.serializer.serialize(summary, out);
            ByteBufferUtil.writeWithLength(first.getKey(), out);
            ByteBufferUtil.writeWithLength(last.getKey(), out);
            out.flush();
            out.sync();
        }
    }

    /**
     * Digest.crc32 is the plain decimal ASCII of a java.util.zip.CRC32 over EVERY physical byte of Data.db, with no
     * newline and no prefix. Correct for a compressed sstable too: the writer folds the inline per-chunk CRCs into
     * the full checksum ({@code appendDirect(bb, checksumIncrementalResult=true)}).
     * <p>
     * "Every physical byte" must include the head pad, since {@code Verifier} validates this digest by CRC-ing the
     * whole Data.db file with no reference to CompressionInfo.db -- and a mismatch trips {@code markAndThrow},
     * which stamps the sstable unrepaired and throws into the disk failure policy.
     * <p>
     * This pass dominates the cost of a split whose extents were shared: the copy stops reading the parent, but
     * this still reads every byte of every child. Two ways out, one implemented:
     * <ul>
     *   <li>SKIP IT, with {@code zero_copy_split_digest_enabled: false}. Nothing needs the component and a
     *       compressed sstable is self-checking without it (every chunk carries an inline CRC32 that this path
     *       preserves and the read path verifies); the cost is {@code Verifier} upgrading to a full extended
     *       verification. See {@link org.apache.cassandra.config.Config#zero_copy_split_digest_enabled} for the
     *       consumer audit.</li>
     *   <li>DERIVE IT, not implemented. The digest covers a byte range that is verbatim parent, and each of the
     *       parent's per-chunk CRC32s is stored inline after its chunk with no offset or chunk index mixed in, so
     *       the value could be assembled with {@code crc32_combine} from 4 bytes per chunk plus the pad -- keeping
     *       the component for a quarter of the read at {@code chunk_length_in_kb: 16}. Separate change with its own
     *       correctness burden, and a wrong digest is silent until somebody runs {@code nodetool verify}.</li>
     * </ul>
     */
    private static void writeDigest(Descriptor child, Progress progress) throws IOException
    {
        CRC32 crc = new CRC32();
        byte[] buffer = new byte[COPY_BUFFER_SIZE];
        try (InputStream in = child.fileFor(Component.DATA).newInputStream())
        {
            int n;
            while ((n = in.read(buffer)) > 0)
            {
                // A second full pass over every byte just written, so throttled and cancellable on the same terms as
                // the copy -- otherwise stopping would leave the node grinding through a read of every child.
                if (progress != null)
                    progress.beforeSlice(n);
                crc.update(buffer, 0, n);
                if (progress != null)
                    progress.afterSlice(n);
            }
        }
        try (FileOutputStreamPlus out = new FileOutputStreamPlus(child.fileFor(Component.DIGEST)))
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
        long onDisk = child.descriptor.fileFor(Component.DATA).length();
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
        // The head pad is the ONE place a child's physical layout differs from a writer's, so it is asserted both
        // ways: the file cannot be short of it and the offsets table cannot disagree about it.
        if (childMeta.chunkFor(0).offset != plan.headPadBytes)
            throw new IllegalStateException("child offsets[0] " + childMeta.chunkFor(0).offset + " != head pad "
                                            + plan.headPadBytes);
        if (childMeta.chunkLength() != chunkLength)
            throw new IllegalStateException("child chunkLength " + childMeta.chunkLength() + " != " + chunkLength);

        RowIndexEntry entry = child.getPosition(child.first, SSTableReader.Operator.EQ, false);
        if (entry == null)
            throw new IllegalStateException("child cannot find its own first key " + child.first);
        long expectedFirst = range.lo - range.shift;
        if (entry.position != expectedFirst)
            throw new IllegalStateException("child first position " + entry.position + " != " + expectedFirst);
        if (entry.position != range.deadPrefixBytes)
            throw new IllegalStateException("child first position " + entry.position +
                                            " != dead prefix " + range.deadPrefixBytes);
        if (entry.position >= chunkLength)
            throw new IllegalStateException("child first position " + entry.position +
                                            " must be inside the first chunk (L=" + chunkLength + ')');
        if (child.first.compareTo(child.last) > 0)
            throw new IllegalStateException("child first > last: " + child.first + " > " + child.last);

        RowIndexEntry lastEntry = child.getPosition(child.last, SSTableReader.Operator.EQ, false);
        if (lastEntry == null)
            throw new IllegalStateException("child cannot find its own last key " + child.last);

        // Decompress the child's FINAL chunk -- the one construct every other check here is blind to. The last
        // chunk is physically whole while the child's dataLength says only part of it is live, so its length is
        // derived rather than stored and a single byte of trailing slack changes it. Digest.crc32 cannot catch that
        // (it covers whatever was written, so it stays self-consistent) and the checks above only touch chunkFor(0)
        // and child.first. Reading the last live byte forces CompressedChunkReader's normal path, where a wrong
        // derived length fails the inline CRC32 (or LZ4's "Compressed lengths mismatch").
        try (RandomAccessReader in = child.openDataReader())
        {
            in.seek(child.uncompressedLength() - 1);
            in.readByte();
        }
        catch (IOException e)
        {
            throw new CorruptSSTableException(e, child.descriptor.filenameFor(Component.DATA));
        }

        logger.trace("Child {} ok: {} partitions, {} physical bytes, dead prefix {}, last partition at {}",
                     child.descriptor, partitionCount, physicalBytes, range.deadPrefixBytes, lastEntry.position);
    }

    static Map<MetadataType, MetadataComponent> readParentMetadata(Descriptor parent)
    {
        Map<MetadataType, MetadataComponent> components;
        try
        {
            components = parent.getMetadataSerializer().deserialize(parent, EnumSet.allOf(MetadataType.class));
        }
        catch (IOException e)
        {
            throw new CorruptSSTableException(e, parent.filenameFor(Component.STATS));
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
     * generator so we cannot collide with a concurrent flush or compaction; falls back to a directory-derived
     * generator plus an existence loop for offline use.
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
            return () -> store.newSSTableDescriptor(template.directory, template.version, template.formatType);
        }

        Supplier<SSTableId> ids = new Directories(parent.metadata()).getUIDGenerator(SSTableIdFactory.instance.defaultBuilder());
        return () -> {
            for (int attempt = 0; attempt < 1000; attempt++)
            {
                Descriptor candidate = new Descriptor(template.version, template.directory, template.ksname,
                                                      template.cfname, ids.get(), template.formatType);
                if (!candidate.fileFor(Component.DATA).exists())
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
     * A cheap post-condition. Every component here is written through a path that fsyncs and propagates
     * IOException, unlike the {@code SSTableReader.save*} helpers, which log at TRACE, delete the half-written file
     * and return normally.
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
            // Statistics.db is written in place, not via rewriteSSTableMetadata's tmp file + rename, so this should
            // never exist. Belt and braces: a leftover tmp would be picked up as an orphan.
            deleteQuietly(new File(descriptor.tmpFilenameFor(Component.STATS)), descriptor);
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
