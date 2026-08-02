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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import com.clearspring.analytics.stream.cardinality.ICardinality;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.io.sstable.ZeroCopySSTableSplitter.RepairState;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableReader.PartitionPositionBounds;
import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.apache.cassandra.io.sstable.format.big.BigFormat.Components;
import org.apache.cassandra.io.sstable.format.big.RowIndexEntry;
import org.apache.cassandra.io.sstable.indexsummary.IndexSummary;
import org.apache.cassandra.io.sstable.indexsummary.IndexSummaryBuilder;
import org.apache.cassandra.io.sstable.indexsummary.IndexSummarySupport;
import org.apache.cassandra.io.sstable.metadata.MetadataComponent;
import org.apache.cassandra.io.sstable.metadata.MetadataType;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.io.util.SequentialWriter;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.ChecksumType;
import org.apache.cassandra.utils.EstimatedHistogram;
import org.apache.cassandra.utils.FilterFactory;
import org.apache.cassandra.utils.IFilter;
import org.apache.cassandra.utils.MurmurHash;

import static org.apache.cassandra.io.sstable.ZeroCopySSTableSplitter.HLL_P;
import static org.apache.cassandra.io.sstable.ZeroCopySSTableSplitter.HLL_SP;
import static org.apache.cassandra.io.sstable.ZeroCopySSTableSplitter.PARTITION_SIZE_HISTOGRAM_BUCKETS;
import static org.apache.cassandra.io.sstable.ZeroCopySSTableSplitter.chunkEnd;
import static org.apache.cassandra.io.sstable.ZeroCopySSTableSplitter.chunkStart;
import static org.apache.cassandra.io.sstable.ZeroCopySSTableSplitter.firstChunk;
import static org.apache.cassandra.io.sstable.ZeroCopySSTableSplitter.lastChunk;
import static org.apache.cassandra.io.sstable.ZeroCopySSTableSplitter.readParentMetadata;
import static org.apache.cassandra.io.sstable.ZeroCopySSTableSplitter.requireNonEmpty;
import static org.apache.cassandra.io.sstable.ZeroCopySSTableSplitter.writeFilter;
import static org.apache.cassandra.io.sstable.ZeroCopySSTableSplitter.writeStatistics;
import static org.apache.cassandra.io.sstable.ZeroCopySSTableSplitter.writeSummary;
import static org.apache.cassandra.io.sstable.ZeroCopySSTableSplitter.writerOption;

/**
 * The components an sstable's Data.db byte ranges would need in order to BE an sstable, without the Data.db.
 *
 * <p>{@link ZeroCopySSTableSplitter} produces a child of an sstable by copying verbatim ranges of its Data.db and
 * rebuilding every other component from an Index.db pass. This does the second half of that and none of the first:
 * it synthesises Index.db, Statistics.db, Summary.db, Filter.db and whichever of CompressionInfo.db / CRC.db the
 * format calls for, describing ranges that stay where they are, so that those ranges can be sent to a peer as if
 * they were a whole sstable. The peer writes what it is given -- {@code CassandraEntireSSTableStreamReader} loops
 * the component manifest and copies bytes, and inspects none of them -- so what it ends up with is an ordinary
 * sstable holding exactly the requested partitions, at the cost of no row ever having been deserialised on either
 * side.
 *
 * <h2>What it is for</h2>
 * Entire-sstable (zero-copy) streaming needs the requested ranges to cover the whole sstable. When they do not,
 * streaming falls back to a path whose sender is already cheap -- {@code CassandraCompressedStreamWriter} sends
 * whole compression chunks verbatim -- and whose RECEIVER decompresses, deserialises, re-serialises and
 * recompresses every row, then rebuilds the index, filter and summary it could have been handed. This exists to
 * hand it those instead. See {@code CassandraOutgoingFile}.
 *
 * <h2>The grid, the runs, and what "dead space" is</h2>
 * Neither format lets an arbitrary byte offset be treated as an origin, so a slice is always a whole number of
 * fixed-size CELLS of a grid, and the grid is what everything here is arithmetic over:
 * <ul>
 *   <li>COMPRESSED: the cell is {@code chunk_length_in_kb}. Uncompressed positions are pinned to exact multiples
 *       of it, because {@link CompressionMetadata#chunkFor} indexes the offsets array with
 *       {@code 8 * (position / chunkLength)} and no per-chunk uncompressed length is stored. A cell's PHYSICAL
 *       extent is its compressed chunk plus the 4-byte inline CRC, and cannot be cut at all.</li>
 *   <li>UNCOMPRESSED: the cell is the chunk size in the header of CRC.db, whose per-chunk CRC32s are addressed as
 *       {@code 4 * (position / chunkSize) + 4} from origin 0. Physical and uncompressed positions are the same
 *       thing, and a cell CAN be cut, at the price of recomputing one CRC.</li>
 * </ul>
 * The sections a stream asks for become RUNS: maximal groups of sections whose cell intervals are contiguous. Two
 * sections a whole cell or more apart belong to different runs, and the cells between them are not sent. A run is
 * one contiguous byte range of the parent's Data.db; the slice is those ranges concatenated, in order.
 * <p>
 * Concatenation is sound because cell ordinals stay consecutive across the join: every run but the last
 * contributes whole cells, so the child's cell {@code k} still covers child bytes {@code [k*G, (k+1)*G)} and the
 * grid is intact. What changes per run is only the rebase: a record at parent position {@code p} in run {@code r}
 * is written at {@code p - shift(r)}, where {@code shift(r) = (firstCell(r) - childCellBase(r)) * G} collapses to
 * the single-run {@code firstCell * G} when there is one run.
 * <p>
 * What the slice carries that nobody asked for is DEAD SPACE:
 * <ul>
 *   <li>a DEAD PREFIX of {@code lo mod G} bytes -- the head of the first cell, holding the tail of a partition
 *       that starts before the range;</li>
 *   <li>INTERIOR gaps: where two sections are less than a cell apart they stay in one run, and the partitions
 *       between them come along inside it.</li>
 * </ul>
 * Neither is indexed, so no read can reach either: every read path enters Data.db at a position taken from
 * Index.db. They are transferred and stored for nothing until the sstable is compacted, which is what
 * {@code zero_copy_partial_stream_max_dead_space_ratio} bounds. There is deliberately no dead SUFFIX: the last
 * run stops at the last live byte, which for a compressed slice means a final cell whose declared uncompressed
 * length is short of what it decompresses to (exactly what a split child's last chunk does), and for an
 * uncompressed one means a final cell whose CRC is recomputed over the bytes actually kept.
 *
 * <h2>Correctness notes that are not in the splitter</h2>
 * <ul>
 *   <li>INTERIOR dead regions are new here; a split child only ever has the prefix. Nothing reads them, and
 *       {@code Verifier} walks Data.db by seeking to each next index position so it steps over them. The one
 *       consumer that walks LINEARLY is {@code Scrubber}, which is given the same seek.</li>
 *   <li>Statistics.db carries the splitter's accepted imprecision verbatim -- see
 *       {@link ZeroCopySSTableSplitter}'s class javadoc -- because it is written by the same code. The receiver
 *       mutates level and repair state afterwards, so only the inherited totals and bounds survive.</li>
 *   <li>Digest.crc32 is not synthesised here: it is a CRC over every byte of the child's Data.db, and those bytes
 *       reach the socket by {@code sendfile} without entering the process. The RECEIVER computes it instead, as it
 *       writes the component -- see {@link SSTableZeroCopyWriter} -- so the sstable that lands has one.</li>
 *   <li>The components are written with the same fsyncs the splitter uses. That is unnecessary here (they are read
 *       back immediately and deleted) and is kept rather than forked, because a handful of small fsyncs are
 *       cheaper than a second copy of {@code writeStatistics}.</li>
 * </ul>
 */
public final class ZeroCopySSTableSlice
{
    private static final Logger logger = LoggerFactory.getLogger(ZeroCopySSTableSlice.class);

    /** Synthesised for a compressed parent. Data.db is absent: it stays in the parent, sent as ranges of it. */
    public static final List<Component> COMPRESSED_COMPONENTS = ImmutableList.of(Components.PRIMARY_INDEX,
                                                                                 Components.COMPRESSION_INFO,
                                                                                 Components.STATS,
                                                                                 Components.SUMMARY,
                                                                                 Components.FILTER);

    /** Synthesised for an uncompressed parent: CRC.db in place of CompressionInfo.db. */
    public static final List<Component> UNCOMPRESSED_COMPONENTS = ImmutableList.of(Components.PRIMARY_INDEX,
                                                                                   Components.CRC,
                                                                                   Components.STATS,
                                                                                   Components.SUMMARY,
                                                                                   Components.FILTER);

    /** Everything {@link #delete} has to consider, whichever format produced it. */
    public static final Set<Component> ALL_SYNTHESISED =
        ImmutableSet.<Component>builder().addAll(COMPRESSED_COMPONENTS).addAll(UNCOMPRESSED_COMPONENTS).build();

    /**
     * Runaway guard, not a tuning knob. A run costs one more entry in the plan and one more {@code sendfile}
     * range, and dead space already bounds how many runs a real range set can produce, so this only exists so a
     * pathological section list cannot build an unbounded plan. Vnode-shaped requests produce hundreds.
     */
    @VisibleForTesting
    static final int MAX_RUNS = 16384;

    private ZeroCopySSTableSlice()
    {
    }

    /** Why a slice was refused, so the caller can log it and a test can assert on it. */
    public enum Reason
    {
        ELIGIBLE,
        /** Only the BIG format has an Index.db of the shape this rebases. */
        WRONG_FORMAT,
        /**
         * The parent carries a pre-4.0 Filter.db, which {@code CassandraOutgoingFile} already calls incompatible
         * with zero-copy streaming and refuses to send a whole sstable in. A slice goes out over that same
         * entire-sstable path, so it has to refuse on the same test.
         */
        LEGACY_BLOOM_FILTER,
        /**
         * The parent's chunks were compressed against a dictionary. Rebasing the offsets is dictionary-agnostic,
         * but nothing has yet proven the dictionary survives the round trip, so this refuses rather than risk
         * undecompressible output. Mirrors {@link ZeroCopySSTableSplitter#isSupported}.
         */
        COMPRESSION_DICTIONARY,
        /** Reconciling those needs the rows. */
        LEGACY_COUNTER_SHARDS,
        /** Nothing to send. */
        NO_SECTIONS,
        /** Sections are not sorted and disjoint, or reach past the parent's data. */
        MALFORMED_SECTIONS,
        /** More runs than {@link #MAX_RUNS}. */
        TOO_MANY_RUNS,
        /** Dead space exceeds the configured fraction of the slice. */
        DEAD_SPACE,
        /** The parent's own CompressionInfo.db / CRC.db / Statistics.db do not support the arithmetic. */
        PARENT_UNSUITABLE
    }

    /**
     * One contiguous byte range of the parent's Data.db, and where it lands in the slice.
     *
     * <p>All cell indices are into the PARENT's grid; {@link #shift} converts a parent uncompressed position to
     * the slice's, and {@link #childPhysicalBase} is where the range begins in the slice's Data.db.
     */
    public static final class Run
    {
        public final long firstCell;
        public final long lastCell;
        /** Parent Data.db byte range, {@code [srcStart, srcEnd)}. */
        public final long srcStart;
        public final long srcEnd;
        /** {@code childPosition = parentPosition - shift}, in uncompressed space. */
        public final long shift;
        /** Ordinal of {@link #firstCell} in the slice's own grid. */
        public final long childCellBase;
        /** Offset of {@link #srcStart} within the slice's Data.db. */
        public final long childPhysicalBase;

        /** Inclusive range of {@link Plan#sections} this run covers. */
        final int firstSection;
        final int lastSection;

        Run(long firstCell, long lastCell, long srcStart, long srcEnd, long shift,
            long childCellBase, long childPhysicalBase, int firstSection, int lastSection)
        {
            this.firstCell = firstCell;
            this.lastCell = lastCell;
            this.srcStart = srcStart;
            this.srcEnd = srcEnd;
            this.shift = shift;
            this.childCellBase = childCellBase;
            this.childPhysicalBase = childPhysicalBase;
            this.firstSection = firstSection;
            this.lastSection = lastSection;
        }

        public long physicalBytes()
        {
            return srcEnd - srcStart;
        }

        public long cellCount()
        {
            return lastCell - firstCell + 1;
        }

        @Override
        public String toString()
        {
            return String.format("Run[cells %d..%d, bytes %d..%d -> %d, shift %d]",
                                 firstCell, lastCell, srcStart, srcEnd, childPhysicalBase, shift);
        }
    }

    /**
     * Where the slice's Data.db bytes are in the parent and what its rebased components will say about them.
     * Arithmetic only: nothing here has read Index.db.
     */
    public static final class Plan
    {
        public final Reason reason;

        /** The byte ranges to send, in order. Empty unless {@link #isEligible()}. */
        public final List<Run> runs;
        /** The sections the slice was planned for, in order. */
        public final List<PartitionPositionBounds> sections;
        /** {@code chunk_length_in_kb} for a compressed parent, CRC.db's chunk size otherwise. */
        public final int cellLength;
        /** Whether the parent is compressed, i.e. which component set gets synthesised. */
        public final boolean compressed;
        /** The slice's uncompressed length: what its CompressionInfo.db or its data file will say. */
        public final long dataLength;
        /** Physical bytes of the parent's Data.db the slice sends, over all runs. */
        public final long physicalBytes;
        /** Uncompressed bytes actually asked for: the sum of the sections' lengths. */
        public final long usefulBytes;
        /** Uncompressed bytes the slice carries that no read can reach: dead prefix plus interior gaps. */
        public final long deadBytes;

        private Plan(Reason reason, List<Run> runs, List<PartitionPositionBounds> sections, int cellLength,
                     boolean compressed, long dataLength, long physicalBytes, long usefulBytes, long deadBytes)
        {
            this.reason = reason;
            this.runs = runs;
            this.sections = sections;
            this.cellLength = cellLength;
            this.compressed = compressed;
            this.dataLength = dataLength;
            this.physicalBytes = physicalBytes;
            this.usefulBytes = usefulBytes;
            this.deadBytes = deadBytes;
        }

        static Plan ineligible(Reason reason)
        {
            Preconditions.checkArgument(reason != Reason.ELIGIBLE);
            return new Plan(reason, ImmutableList.of(), ImmutableList.of(), 0, false, 0, 0, 0, 0);
        }

        public boolean isEligible()
        {
            return reason == Reason.ELIGIBLE;
        }

        /** Total cells in the slice's own grid. */
        public long cellCount()
        {
            long count = 0;
            for (Run run : runs)
                count += run.cellCount();
            return count;
        }

        /** First live byte, in the parent's uncompressed space. */
        public long lo()
        {
            return sections.get(0).lowerPosition;
        }

        /** One past the last live byte, in the parent's uncompressed space. */
        public long hi()
        {
            return sections.get(sections.size() - 1).upperPosition;
        }

        public double deadRatio()
        {
            return dataLength == 0 ? 0.0 : (double) deadBytes / dataLength;
        }

        public List<Component> components()
        {
            return compressed ? COMPRESSED_COMPONENTS : UNCOMPRESSED_COMPONENTS;
        }

        @Override
        public String toString()
        {
            if (!isEligible())
                return "Plan[" + reason + ']';
            return String.format("Plan[%s, %d run(s), %d cells of %d, %d physical bytes, %d useful + %d dead " +
                                 "(%.1f%%) of %d uncompressed, %d sections]",
                                 compressed ? "compressed" : "uncompressed", runs.size(), cellCount(), cellLength,
                                 physicalBytes, usefulBytes, deadBytes, deadRatio() * 100, dataLength,
                                 sections.size());
        }
    }

    /** What was written, and everything the caller needs to describe it to a peer. */
    public static final class Slice
    {
        public final Descriptor descriptor;
        /** Exactly the files that exist; FILTER is absent when {@code bloom_filter_fp_chance} is 1.0. */
        public final Set<Component> components;
        /** Size of each of {@link #components} on disk, for the stream's component manifest. */
        public final Map<Component, Long> sizes;
        public final DecoratedKey first;
        public final DecoratedKey last;
        public final int partitionCount;

        Slice(Descriptor descriptor, Set<Component> components, Map<Component, Long> sizes,
              DecoratedKey first, DecoratedKey last, int partitionCount)
        {
            this.descriptor = descriptor;
            this.components = components;
            this.sizes = sizes;
            this.first = first;
            this.last = last;
            this.partitionCount = partitionCount;
        }

        public long totalComponentBytes()
        {
            long total = 0;
            for (long size : sizes.values())
                total += size;
            return total;
        }

        @Override
        public String toString()
        {
            return String.format("Slice[%s, %d partitions, %s, %d component bytes]",
                                 descriptor, partitionCount, components, totalComponentBytes());
        }
    }

    // ------------------------------------------------------------------------------------------------
    // Planning
    // ------------------------------------------------------------------------------------------------

    /**
     * Section index bounds of each run: {@code {firstSection, lastSection}}, inclusive. Two consecutive sections
     * are in the same run when the second's first cell is the first's last cell or the one immediately after it --
     * a gap of a whole cell or more leaves that cell out of the middle, which is a new run.
     *
     * @param sections sorted, disjoint, non-empty
     */
    @VisibleForTesting
    static List<int[]> runBounds(List<PartitionPositionBounds> sections, int cellLength)
    {
        List<int[]> bounds = new ArrayList<>();
        int start = 0;
        for (int i = 1; i < sections.size(); i++)
        {
            if (firstChunk(sections.get(i).lowerPosition, cellLength)
                > lastChunk(sections.get(i - 1).upperPosition, cellLength) + 1)
            {
                bounds.add(new int[]{ start, i - 1 });
                start = i;
            }
        }
        bounds.add(new int[]{ start, sections.size() - 1 });
        return bounds;
    }

    /** How many byte ranges {@code sections} would be sent as. */
    public static int runCount(List<PartitionPositionBounds> sections, int cellLength)
    {
        return runBounds(sections, cellLength).size();
    }

    /**
     * The slice's uncompressed length: every run but the last contributes whole cells, and the last stops at the
     * last live byte.
     */
    public static long dataLength(List<PartitionPositionBounds> sections, int cellLength)
    {
        List<int[]> bounds = runBounds(sections, cellLength);
        long length = 0;
        for (int r = 0; r < bounds.size(); r++)
        {
            long firstCell = firstChunk(sections.get(bounds.get(r)[0]).lowerPosition, cellLength);
            long hi = sections.get(bounds.get(r)[1]).upperPosition;
            length += (r == bounds.size() - 1)
                      ? hi - firstCell * cellLength                                  // exact, no dead suffix
                      : (lastChunk(hi, cellLength) - firstCell + 1) * (long) cellLength;
        }
        return length;
    }

    /**
     * Uncompressed bytes the slice carries that no read can reach: what it is long enough to hold, less what was
     * asked for.
     *
     * @param sections sorted, disjoint, non-empty
     */
    public static long deadBytes(List<PartitionPositionBounds> sections, int cellLength)
    {
        long useful = 0;
        for (PartitionPositionBounds section : sections)
            useful += section.upperPosition - section.lowerPosition;
        return dataLength(sections, cellLength) - useful;
    }

    /**
     * Plan a slice of {@code parent} covering {@code sections}, or say why not. Reads the parent's compression
     * metadata (already in memory) or the four-byte header of its CRC.db, so it is cheap enough to call while a
     * stream plan is being assembled.
     *
     * @param sections          as produced by {@link SSTableReader#getPositionsForRanges}: sorted, disjoint,
     *                          {@code [first partition start, one past last partition end)}
     * @param maxDeadSpaceRatio refuse if dead space is more than this fraction of the slice; 1.0 accepts any
     */
    public static Plan plan(SSTableReader parent, List<PartitionPositionBounds> sections, double maxDeadSpaceRatio)
    {
        Preconditions.checkNotNull(parent, "parent");

        if (!BigFormat.is(parent.descriptor.getFormat()))
            return Plan.ineligible(Reason.WRONG_FORMAT);
        // A slice is sent by the entire-sstable path, so anything that path refuses outright has to be refused
        // here too -- see CassandraOutgoingFile.computeShouldStreamEntireSSTables(), whose other two tests are
        // stream_entire_sstables (the caller's) and hasLegacyCounterShards (below).
        if (parent.descriptor.version.hasOldBfFormat())
            return Plan.ineligible(Reason.LEGACY_BLOOM_FILTER);
        if (parent.getSSTableMetadata().hasLegacyCounterShards)
            return Plan.ineligible(Reason.LEGACY_COUNTER_SHARDS);
        if (sections == null || sections.isEmpty())
            return Plan.ineligible(Reason.NO_SECTIONS);
        // MetadataSerializer fabricates defaults for a missing Statistics.db rather than failing, and the
        // SerializationHeader in it is the one thing a slice cannot do without.
        if (!parent.descriptor.fileFor(Components.STATS).exists())
            return Plan.ineligible(Reason.PARENT_UNSUITABLE);

        // Every arithmetic helper below throws on input it considers impossible. Streaming is not the place to
        // turn that into a failed session: a refusal costs the row-by-row path, which handles anything.
        try
        {
            return parent.compression ? planCompressed(parent, sections, maxDeadSpaceRatio)
                                      : planUncompressed(parent, sections, maxDeadSpaceRatio);
        }
        catch (RuntimeException e)
        {
            logger.warn("Not slicing {} for streaming: {}", parent.descriptor, e.toString());
            return Plan.ineligible(Reason.PARENT_UNSUITABLE);
        }
    }

    private static Plan planCompressed(SSTableReader parent, List<PartitionPositionBounds> sections, double maxDead)
    {
        CompressionMetadata meta = parent.getCompressionMetadata();   // owned by the parent's dfile; never closed

        // TODO: liftable. The chunk bytes are copied verbatim and the dictionary is serialised into the slice's
        // own CompressionInfo.db by Writer.open below, so this is expected to round trip -- but nothing proves it
        // yet, and a wrong answer here is undecompressible data on the receiver. Same refusal, same reason, as
        // ZeroCopySSTableSplitter.isSupported(); remove both once there is round-trip coverage.
        if (meta.compressionDictionary() != null)
            return Plan.ineligible(Reason.COMPRESSION_DICTIONARY);

        int cellLength = meta.chunkLength();
        long parentDataLength = meta.dataLength;

        if (parent.uncompressedLength() != parentDataLength)
            return Plan.ineligible(Reason.PARENT_UNSUITABLE);

        // The offsets table has to address every chunk a run can reach. It may hold MORE -- a compaction-produced
        // sstable carries a trailing zero-length chunk -- which is chunkEnd()'s problem, not this one. Fewer means
        // CompressionInfo.db disagrees with its own dataLength.
        long neededCells = (parentDataLength + cellLength - 1) / cellLength;
        if (neededCells > meta.offHeapSize() / 8)
            return Plan.ineligible(Reason.PARENT_UNSUITABLE);

        return build(parent, sections, cellLength, parentDataLength, true, maxDead,
                     // A compressed cell's physical extent is the chunk plus its inline CRC, and cannot be cut:
                     // the last run takes its whole final chunk and lets dataLength stop short inside it.
                     cell -> chunkStart(meta, cell, cellLength),
                     (cell, exactEnd) -> chunkEnd(meta, cell, cellLength),
                     meta.compressedFileLength);
    }

    private static Plan planUncompressed(SSTableReader parent, List<PartitionPositionBounds> sections, double maxDead)
    {
        File crc = parent.descriptor.fileFor(Components.CRC);
        if (!crc.exists())
            return Plan.ineligible(Reason.PARENT_UNSUITABLE);

        int cellLength = readCrcChunkSize(crc);
        if (cellLength <= 0)
            return Plan.ineligible(Reason.PARENT_UNSUITABLE);

        long parentDataLength = parent.uncompressedLength();
        long physicalLength = parent.descriptor.fileFor(Components.DATA).length();
        // Physical and uncompressed are the same bytes here, so anything else means the file is not what the
        // reader thinks it is.
        if (physicalLength != parentDataLength)
            return Plan.ineligible(Reason.PARENT_UNSUITABLE);
        // One CRC per cell, after the 4-byte header.
        if (crc.length() < 4 + 4 * ((parentDataLength + cellLength - 1) / cellLength))
            return Plan.ineligible(Reason.PARENT_UNSUITABLE);

        return build(parent, sections, cellLength, parentDataLength, false, maxDead,
                     cell -> cell * (long) cellLength,
                     // A cell CAN be cut here, so the last run ends at the last live byte and its CRC is
                     // recomputed over what was kept. Every other cell ends where the grid says.
                     (cell, exactEnd) -> exactEnd > 0 ? exactEnd : Math.min((cell + 1) * (long) cellLength, parentDataLength),
                     physicalLength);
    }

    /** Physical offset of the start of a cell. */
    private interface CellStart
    {
        long apply(long cell);
    }

    /**
     * Physical offset one past the end of a cell. {@code exactEnd} is positive only for the final cell of the
     * final run, and only a format that can cut a cell may honour it.
     */
    private interface CellEnd
    {
        long apply(long cell, long exactEnd);
    }

    private static Plan build(SSTableReader parent, List<PartitionPositionBounds> sections, int cellLength,
                              long parentDataLength, boolean compressed, double maxDeadSpaceRatio,
                              CellStart cellStart, CellEnd cellEnd, long parentPhysicalLength)
    {
        long useful = 0;
        long previousUpper = -1;
        for (PartitionPositionBounds section : sections)
        {
            if (section.lowerPosition < 0
                || section.upperPosition <= section.lowerPosition
                || section.upperPosition > parentDataLength
                || section.lowerPosition < previousUpper)
                return Plan.ineligible(Reason.MALFORMED_SECTIONS);
            useful += section.upperPosition - section.lowerPosition;
            previousUpper = section.upperPosition;
        }

        List<int[]> bounds = runBounds(sections, cellLength);
        if (bounds.size() > MAX_RUNS)
            return Plan.ineligible(Reason.TOO_MANY_RUNS);

        List<Run> runs = new ArrayList<>(bounds.size());
        long childCellBase = 0;
        long childPhysicalBase = 0;
        long dataLength = 0;

        for (int r = 0; r < bounds.size(); r++)
        {
            int firstSection = bounds.get(r)[0];
            int lastSection = bounds.get(r)[1];
            long lo = sections.get(firstSection).lowerPosition;
            long hi = sections.get(lastSection).upperPosition;
            boolean last = r == bounds.size() - 1;

            long firstCell = firstChunk(lo, cellLength);
            long lastCellIndex = lastChunk(hi, cellLength);
            if (firstCell > lastCellIndex)
                return Plan.ineligible(Reason.PARENT_UNSUITABLE);

            long srcStart = cellStart.apply(firstCell);
            long srcEnd = cellEnd.apply(lastCellIndex, last ? hi : 0);
            if (srcEnd <= srcStart || srcEnd > parentPhysicalLength)
                return Plan.ineligible(Reason.PARENT_UNSUITABLE);

            long cells = lastCellIndex - firstCell + 1;
            // Only the last run may stop short of its final cell's end; the others must contribute whole cells
            // or the grid would not survive the join.
            long uncompressedExtent = last ? hi - firstCell * (long) cellLength : cells * (long) cellLength;
            if (uncompressedExtent <= (cells - 1) * (long) cellLength || uncompressedExtent > cells * (long) cellLength)
                return Plan.ineligible(Reason.PARENT_UNSUITABLE);

            runs.add(new Run(firstCell, lastCellIndex, srcStart, srcEnd,
                             (firstCell - childCellBase) * (long) cellLength,
                             childCellBase, childPhysicalBase, firstSection, lastSection));

            childCellBase += cells;
            childPhysicalBase += srcEnd - srcStart;
            dataLength += uncompressedExtent;
        }

        // The invariant the whole grid rests on, restated over the slice: its last cell holds at least one live
        // byte and at most a full cell of them.
        long cellCount = childCellBase;
        if (cellCount > Integer.MAX_VALUE)   // CompressionMetadata.Writer.finalizeLength takes an int
            return Plan.ineligible(Reason.PARENT_UNSUITABLE);
        if (!((cellCount - 1) * (long) cellLength < dataLength && dataLength <= cellCount * (long) cellLength))
            return Plan.ineligible(Reason.PARENT_UNSUITABLE);

        long dead = dataLength - useful;
        if (dead < 0)
            return Plan.ineligible(Reason.PARENT_UNSUITABLE);
        if ((double) dead / dataLength > maxDeadSpaceRatio)
            return Plan.ineligible(Reason.DEAD_SPACE);

        return new Plan(Reason.ELIGIBLE, ImmutableList.copyOf(runs), ImmutableList.copyOf(sections), cellLength,
                        compressed, dataLength, childPhysicalBase, useful, dead);
    }

    private static int readCrcChunkSize(File crc)
    {
        try (RandomAccessReader in = RandomAccessReader.open(crc))
        {
            return in.readInt();
        }
        catch (IOException e)
        {
            throw new CorruptSSTableException(e, crc);
        }
    }

    // ------------------------------------------------------------------------------------------------
    // Synthesis
    // ------------------------------------------------------------------------------------------------

    /**
     * Write every component of the planned slice except Data.db into {@code target}.
     *
     * <p>Two scoped passes over the parent's Index.db: one to count the partitions, so the filter and the summary
     * are sized exactly rather than guessed, and one to write. Both start from the index summary's scan position
     * for the first key of the first run and stop at the end of the last, so the cost is proportional to the slice
     * and not to the parent -- a narrow range out of a large sstable does not read the large sstable's index.
     *
     * @param target a fresh descriptor whose component files do not exist; the caller owns deleting them, see
     *               {@link #delete}
     */
    public static Slice write(SSTableReader parent, Plan plan, Descriptor target) throws IOException
    {
        Preconditions.checkArgument(plan.isEligible(), "not an eligible plan: %s", plan);
        Preconditions.checkArgument(target.version.toString().equals(parent.descriptor.version.toString()),
                                    "slice must keep the parent's sstable version");

        TableMetadata metadata = parent.metadata();
        long scanFrom = indexScanStart(parent, plan.lo());

        int partitionCount = countPartitions(parent, plan, scanFrom);
        if (partitionCount <= 0)
            throw new IllegalStateException("no Index.db records in " + plan + " of " + parent.descriptor);

        Map<MetadataType, MetadataComponent> parentMetadata = readParentMetadata(parent.descriptor);
        StatsMetadata parentStats = (StatsMetadata) parentMetadata.get(MetadataType.STATS);

        Set<Component> components = Sets.newHashSet(Components.PRIMARY_INDEX, Components.STATS, Components.SUMMARY);
        boolean success = false;
        try
        {
            // ---------- CompressionInfo.db or CRC.db: the slice's own view of the grid ----------
            if (plan.compressed)
            {
                writeCompressionInfo(target, parent.getCompressionMetadata(), plan);
                components.add(Components.COMPRESSION_INFO);
            }
            else
            {
                writeCrc(target, parent, plan);
                components.add(Components.CRC);
            }

            // ---------- Index.db + Filter.db + Summary.db + histogram + HLL, one pass ----------
            EstimatedHistogram partitionSizes = new EstimatedHistogram(PARTITION_SIZE_HISTOGRAM_BUCKETS);
            ICardinality cardinality = new HyperLogLogPlus(HLL_P, HLL_SP);
            double fpChance = metadata.params.bloomFilterFpChance;
            // 1.0 yields an AlwaysPresentFilter, whose serialize() is a no-op -- so writing it would leave a
            // zero-length Filter.db that requireNonEmpty rejects. The read path already treats both a missing
            // and an empty Filter.db as always-present (FilterComponent.load), so omit the component instead.
            IFilter bf = fpChance < 1.0 ? FilterFactory.getFilter(partitionCount, fpChance) : null;
            KeyRange keys = new KeyRange();

            try
            {
                try (SequentialWriter out = new SequentialWriter(target.fileFor(Components.PRIMARY_INDEX), writerOption());
                     IndexSummaryBuilder summary = new IndexSummaryBuilder(partitionCount,
                                                                           metadata.params.minIndexInterval,
                                                                           Downsampling.BASE_SAMPLING_LEVEL))
                {
                    walk(parent, plan, scanFrom, true, new RecordVisitor()
                    {
                        long pending = UNRESOLVED;

                        public void record(ByteBuffer key, long position, byte[] promoted, int promotedSize,
                                           boolean included, Run run) throws IOException
                        {
                            // Sizes come from the NEXT record's offset, which is exact and is why they are
                            // recorded one record late: rowSize_i == position_{i+1} - position_i identically.
                            // Every record advances it, included or not, because an excluded partition still
                            // ends where the following one starts.
                            if (pending != UNRESOLVED)
                            {
                                partitionSizes.add(position - pending);
                                pending = UNRESOLVED;
                            }
                            if (!included)
                                return;
                            pending = position;

                            long indexStart = out.position();
                            ByteBufferUtil.writeWithShortLength(key, out);
                            // The ONLY rewritten field, as a canonical minimal vint: the slice's records are
                            // shorter than the parent's, so its index offsets are not the parent's less a
                            // constant, which is why Summary.db has to be rebuilt rather than sliced.
                            out.writeUnsignedVInt(position - run.shift);
                            out.writeUnsignedVInt32(promotedSize);
                            if (promoted != null)
                                out.write(promoted, 0, promotedSize);

                            DecoratedKey dk = parent.decorateKey(key);
                            keys.add(dk);
                            if (bf != null)
                                bf.add(dk);
                            summary.maybeAddEntry(dk, indexStart);
                            // MetadataCollector.addKey hashes the raw key bytes
                            cardinality.offerHashed(MurmurHash.hash2_64(key, key.position(), key.remaining(), 0));
                        }

                        public void end(long endPosition)
                        {
                            if (pending != UNRESOLVED)
                                partitionSizes.add(endPosition - pending);
                        }
                    });

                    out.finish();

                    keys.minimise();
                    try (IndexSummary built = summary.build(parent.getPartitioner()))
                    {
                        writeSummary(target, keys.first, keys.last, built);
                    }
                }
                requireNonEmpty(target, Components.PRIMARY_INDEX);
                requireNonEmpty(target, Components.SUMMARY);

                if (bf != null)
                {
                    writeFilter(target, bf);
                    requireNonEmpty(target, Components.FILTER);
                    components.add(Components.FILTER);
                }
            }
            finally
            {
                if (bf != null)
                    bf.close();
            }

            // ---------- Statistics.db ----------
            // onDiskLength is the runs' physical length, which is what the receiving node will have on disk, so
            // the derived compressionRatio stays a true compressed-over-uncompressed for the file. The first and
            // last key are the SLICE's own, gathered by the index pass above: when version.hasKeyRange() the
            // reader takes its bounds from Statistics.db in preference to Summary.db, so inheriting the parent's
            // would make the received sstable claim the parent's whole range.
            writeStatistics(target, metadata, parentMetadata, parentStats, partitionSizes, cardinality,
                            plan.physicalBytes, plan.dataLength, keys.first, keys.last,
                            // A slice carries whichever partitions shared a copied cell with a requested one and
                            // leaves them out of the index on purpose; deadBytes is exactly how many such bytes
                            // there are. Marking the sstable forces every scan of it through the index, without
                            // which a linear scan would hand back partitions this slice does not claim.
                            plan.deadBytes > 0,
                            RepairState.inherit(parentStats));

            Map<Component, Long> sizes = new LinkedHashMap<>();
            for (Component component : plan.components())
            {
                if (components.contains(component))
                    sizes.put(component, target.fileFor(component).length());
            }

            Slice slice = new Slice(target, ImmutableSet.copyOf(components), ImmutableMap.copyOf(sizes),
                                    keys.first, keys.last, partitionCount);
            success = true;
            logger.debug("Sliced {} for streaming: {} of {}", parent.descriptor, slice, plan);
            return slice;
        }
        finally
        {
            // Not `components`: a writer that threw part way through leaves a file the set does not name yet.
            if (!success)
                delete(target, ALL_SYNTHESISED);
        }
    }

    /**
     * The slice's CompressionInfo.db: the parent's parameters, its chunks' offsets rebased onto the concatenation
     * of the runs, and the slice's own dataLength.
     * <p>
     * {@code offsets[0]} is 0 -- unlike a split child there is no alignment pad, because nothing is being
     * reflinked -- and the offsets are contiguous across a run boundary, which they have to be: a reader derives a
     * chunk's compressed length as the difference between successive offsets, so a physical gap between runs would
     * inflate the preceding chunk's length and hand compressed bytes back as row data.
     */
    private static void writeCompressionInfo(Descriptor target, CompressionMetadata meta, Plan plan)
    {
        CompressionMetadata.Writer writer =
            CompressionMetadata.Writer.open(meta.parameters,
                                            target.fileFor(Components.COMPRESSION_INFO),
                                            meta.compressionDictionary());
        boolean prepared = false;
        try
        {
            long expected = 0;
            for (Run run : plan.runs)
            {
                for (long cell = run.firstCell; cell <= run.lastCell; cell++)
                {
                    long offset = run.childPhysicalBase + (chunkStart(meta, cell, plan.cellLength) - run.srcStart);
                    if (offset != expected)
                        throw new IllegalStateException("slice CompressionInfo.db offset " + offset +
                                                        " is not contiguous with the previous chunk's end " +
                                                        expected + " in " + run);
                    writer.addOffset(offset);
                    expected = offset + (chunkEnd(meta, cell, plan.cellLength) - chunkStart(meta, cell, plan.cellLength));
                }
            }
            if (expected != plan.physicalBytes)
                throw new IllegalStateException("slice chunks cover " + expected + " bytes but the plan sends " +
                                                plan.physicalBytes);

            writer.finalizeLength(plan.dataLength, Math.toIntExact(plan.cellCount()));
            writer.prepareToCommit();   // doPrepare() is what writes and fsyncs the file
            prepared = true;
            writer.commit();
        }
        catch (Throwable t)
        {
            // doAbort() only frees memory, it does not delete an already-written file
            if (!prepared)
                writer.abort();
            target.fileFor(Components.COMPRESSION_INFO).deleteIfExists();
            throw t;
        }
        finally
        {
            writer.close();
        }
    }

    /**
     * The slice's CRC.db: the parent's chunk size, then the parent's per-cell CRC32 for every cell of every run,
     * in the slice's order.
     * <p>
     * {@code ChecksumValidator} addresses these as {@code 4 * (position / chunkSize) + 4}, so they only line up if
     * the slice's cell {@code k} really does cover its bytes {@code [k*G, (k+1)*G)} -- which is what makes every
     * run but the last contribute whole cells. The last run ends at the last live byte, so if that cut falls
     * inside its final cell, that one CRC is recomputed over the bytes actually kept: one read of at most a cell,
     * and the alternative would be a dead suffix that {@code Scrubber}'s linear walk would try to read as a
     * partition.
     */
    private static void writeCrc(Descriptor target, SSTableReader parent, Plan plan) throws IOException
    {
        File parentCrc = parent.descriptor.fileFor(Components.CRC);
        Run lastRun = plan.runs.get(plan.runs.size() - 1);
        // The exclusive end of the last cell of the last run, as the parent would have it.
        long parentLastCellEnd = Math.min((lastRun.lastCell + 1) * (long) plan.cellLength, parent.uncompressedLength());
        boolean recomputeLast = lastRun.srcEnd < parentLastCellEnd;

        try (SequentialWriter out = new SequentialWriter(target.fileFor(Components.CRC), writerOption());
             RandomAccessReader in = RandomAccessReader.open(parentCrc))
        {
            int chunkSize = in.readInt();
            if (chunkSize != plan.cellLength)
                throw new IllegalStateException("parent CRC.db chunk size changed from " + plan.cellLength +
                                                " to " + chunkSize + ": " + parentCrc);
            out.writeInt(chunkSize);

            for (Run run : plan.runs)
            {
                for (long cell = run.firstCell; cell <= run.lastCell; cell++)
                {
                    if (recomputeLast && run == lastRun && cell == run.lastCell)
                    {
                        out.writeInt(crcOfDataRange(parent, cell * (long) plan.cellLength, run.srcEnd));
                        continue;
                    }
                    in.seek(4 + cell * 4L);
                    out.writeInt(in.readInt());
                }
            }
            out.finish();
        }
        requireNonEmpty(target, Components.CRC);

        long expected = 4 + 4L * plan.cellCount();
        long actual = target.fileFor(Components.CRC).length();
        if (actual != expected)
            throw new IllegalStateException("slice CRC.db is " + actual + " bytes, expected " + expected);
    }

    /** CRC32 of {@code [from, to)} of an uncompressed Data.db, computed the way {@code ChecksumWriter} does. */
    private static int crcOfDataRange(SSTableReader parent, long from, long to) throws IOException
    {
        int length = Math.toIntExact(to - from);
        byte[] bytes = new byte[length];
        try (RandomAccessReader in = RandomAccessReader.open(parent.descriptor.fileFor(Components.DATA)))
        {
            in.seek(from);
            in.readFully(bytes);
        }
        return (int) ChecksumType.CRC32.of(bytes, 0, length);
    }

    /**
     * A fresh descriptor in the parent's directory, version and format, for a slice's components to be written
     * under.
     * <p>
     * These files are named like an sstable's but there is no Data.db beside them, which is exactly the shape
     * {@code ColumnFamilyStore.scrubDataDirectories} removes at startup ("missing the DATA file! all components
     * are orphaned"), so a crash between {@link #write} and {@link #delete} cannot leave anything behind for
     * long. Nothing tracks them in the meantime; the caller deletes them when the stream ends.
     */
    public static Descriptor newDescriptor(SSTableReader parent)
    {
        return ZeroCopySSTableSplitter.descriptorAllocator(parent).get();
    }

    /** Best-effort removal of a slice's files. Nothing tracks them, so nothing else will. */
    public static void delete(Descriptor descriptor, Set<Component> components)
    {
        for (Component component : components)
        {
            try
            {
                descriptor.fileFor(component).deleteIfExists();
            }
            catch (Throwable t)
            {
                logger.warn("Failed deleting {} of streaming slice {}", component, descriptor, t);
            }
        }
    }

    // ------------------------------------------------------------------------------------------------
    // Walking the parent's Index.db
    // ------------------------------------------------------------------------------------------------

    /** No record's offset can be this, so it doubles as "not filled in yet". */
    private static final long UNRESOLVED = -1;

    private interface RecordVisitor
    {
        /**
         * @param included whether the record's partition is in one of the requested sections. Excluded records are
         *                 the dead space: physically present in a run, absent from the slice's index -- or between
         *                 runs, in which case they are not sent at all.
         * @param run      the run the record's position falls in, whose {@code shift} rebases it. Meaningful only
         *                 when {@code included}.
         */
        void record(ByteBuffer key, long position, byte[] promoted, int promotedSize, boolean included, Run run)
        throws IOException;

        /** @param endPosition where the slice's last record ends: the first offset past it, or the parent's end. */
        void end(long endPosition);
    }

    /** Accumulates the slice's first and last key without retaining any other. */
    private static final class KeyRange
    {
        DecoratedKey first;
        DecoratedKey last;

        void add(DecoratedKey key)
        {
            if (first == null)
                first = key;
            last = key;
        }

        void minimise()
        {
            first = first.retainable();
            last = last.retainable();
        }
    }

    /**
     * The first key of the slice, read out of Data.db, resolved to an Index.db offset at or before its record.
     * <p>
     * This is what keeps the passes proportional to the slice: {@code lo} is a partition start, so the bytes there
     * are that partition's key, and the index summary maps it to the sampled index position at or before its
     * record -- within one {@code min_index_interval} of it. One chunk is decompressed to learn that.
     * <p>
     * This goes at the summary directly rather than through {@code BigTableReader.getIndexScanPosition}, which is
     * package-private. The only thing that wrapper adds is clamping a key below {@code getFirst()} up to it for a
     * {@code MOVED_START} reader, and that cannot apply here: {@code lo} is inside the reader's live range, so the
     * key read at it is never below the reader's first. Erring low would be harmless anyway -- the walk skips
     * every record before {@code lo}.
     */
    private static long indexScanStart(SSTableReader parent, long lo)
    {
        // Safe: plan() refused anything but BIG, whose reader is the IndexSummarySupport implementation.
        IndexSummary summary = ((IndexSummarySupport<?>) parent).getIndexSummary();
        try (FileDataInput in = parent.getFileDataInput(lo))
        {
            return summary.getScanPosition(parent.decorateKey(ByteBufferUtil.readWithShortLength(in)));
        }
        catch (IOException e)
        {
            throw new CorruptSSTableException(e, parent.descriptor.fileFor(Components.DATA));
        }
    }

    private static int countPartitions(SSTableReader parent, Plan plan, long scanFrom)
    {
        int[] count = { 0 };
        walk(parent, plan, scanFrom, false, new RecordVisitor()
        {
            public void record(ByteBuffer key, long position, byte[] promoted, int promotedSize, boolean included,
                               Run run)
            {
                if (included)
                    count[0]++;
            }

            public void end(long endPosition)
            {
            }
        });
        return count[0];
    }

    /**
     * One scoped, sequential pass of the parent's Index.db: from {@code scanFrom} to the first record past the last
     * run, visiting every record in between and saying whether it is in a requested section and which run holds
     * it.
     */
    private static void walk(SSTableReader parent, Plan plan, long scanFrom, boolean readPromoted,
                             RecordVisitor visitor)
    {
        List<PartitionPositionBounds> sections = plan.sections;
        long lo = plan.lo();
        long hi = plan.hi();
        File file = parent.descriptor.fileFor(Components.PRIMARY_INDEX);

        try (RandomAccessReader in = RandomAccessReader.open(file))
        {
            long length = in.length();
            if (scanFrom > 0)
                in.seek(scanFrom);

            int section = 0;
            int run = 0;
            long previous = UNRESOLVED;
            boolean started = false;

            while (in.getFilePointer() != length)
            {
                ByteBuffer key = ByteBufferUtil.readWithShortLength(in);
                long position = RowIndexEntry.Serializer.readPosition(in);
                int promotedSize = in.readUnsignedVInt32();

                if (previous != UNRESOLVED && position <= previous)
                    throw new IllegalStateException("parent Index.db offsets are not strictly increasing: " +
                                                    previous + " -> " + position + " in " + file);
                previous = position;

                if (position < lo)
                {
                    // Summary slack: the scan position is at or before the slice's first record, never after it.
                    if (promotedSize > 0)
                        in.skipBytesFully(promotedSize);
                    continue;
                }

                if (!started)
                {
                    // lo came from getPositionsForRanges, which takes it from a record of this very file, so the
                    // first record at or past it must be exactly it. Anything else means the two disagree.
                    if (position != lo)
                        throw new IllegalStateException("the slice starts at " + lo + " but the first Index.db " +
                                                        "record at or past it is at " + position + " in " + file);
                    started = true;
                }

                if (position >= hi)
                {
                    visitor.end(position);
                    return;
                }

                // Sections and runs are both ordered and the walk is monotonic, so both pointers only advance.
                while (section < sections.size() && position >= sections.get(section).upperPosition)
                    section++;
                boolean included = section < sections.size() && position >= sections.get(section).lowerPosition;
                if (included)
                {
                    while (section > plan.runs.get(run).lastSection)
                        run++;
                }

                byte[] promoted = null;
                if (promotedSize > 0)
                {
                    if (readPromoted && included)
                    {
                        promoted = new byte[promotedSize];
                        in.readFully(promoted);
                    }
                    else
                    {
                        in.skipBytesFully(promotedSize);
                    }
                }

                visitor.record(key, position, promoted, promotedSize, included, plan.runs.get(run));
            }

            if (!started)
                throw new IllegalStateException("no Index.db record at or past " + lo + " in " + file);
            // The slice reaches the end of the parent's data, so its last partition ends there, which is the same
            // value the last section was given as its upper bound.
            visitor.end(hi);
        }
        catch (IOException e)
        {
            throw new CorruptSSTableException(e, file);
        }
    }
}
