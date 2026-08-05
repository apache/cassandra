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

import java.io.Closeable;
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

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.io.sstable.ZeroCopySSTableSplitter.RepairState;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableFormat.Components;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableReader.PartitionPositionBounds;
import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.apache.cassandra.io.sstable.format.big.RowIndexEntry;
import org.apache.cassandra.io.sstable.format.bti.BtiFormat;
import org.apache.cassandra.io.sstable.format.bti.BtiZeroCopySplit;
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
 * <p>{@link ZeroCopySSTableSplitter} produces a child sstable by copying verbatim ranges of a parent's Data.db and
 * rebuilding every other component from an index pass; this does the second half and none of the first, synthesising
 * the components below for ranges that stay where they are. The peer inspects none of what it is sent --
 * {@code CassandraEntireSSTableStreamReader} loops the component manifest and copies bytes -- so it ends up with an
 * ordinary sstable holding exactly the requested partitions, with no row deserialised on either side.
 *
 * <p>Why: entire-sstable (zero-copy) streaming needs the requested ranges to cover the whole sstable, and the path it
 * otherwise falls back to has a cheap sender ({@code CassandraCompressedStreamWriter} sends whole compression chunks
 * verbatim) but a RECEIVER that decompresses, deserialises, re-serialises and recompresses every row, then rebuilds
 * the index, filter and summary it could have been handed. See {@code CassandraOutgoingFile}.
 *
 * <h2>The grid, the runs, and what "dead space" is</h2>
 * Neither format lets an arbitrary byte offset be an origin, so a slice is always a whole number of fixed-size CELLS
 * of a grid, which is what everything here is arithmetic over:
 * <ul>
 *   <li>COMPRESSED: the cell is {@code chunk_length_in_kb}. Uncompressed positions are pinned to exact multiples
 *       of it, because {@link CompressionMetadata#chunkFor} indexes the offsets array with
 *       {@code 8 * (position / chunkLength)} and no per-chunk uncompressed length is stored. A cell's PHYSICAL
 *       extent is its compressed chunk plus the 4-byte inline CRC, and cannot be cut at all.</li>
 *   <li>UNCOMPRESSED: the cell is the chunk size in the header of CRC.db, whose per-chunk CRC32s are addressed as
 *       {@code 4 * (position / chunkSize) + 4} from origin 0. Physical and uncompressed positions are the same
 *       thing, and a cell CAN be cut, at the price of recomputing one CRC.</li>
 * </ul>
 * The sections a stream asks for become RUNS: maximal groups whose cell intervals are contiguous. Two sections a whole
 * cell or more apart belong to different runs, and the cells between them are not sent. A run is one contiguous byte
 * range of the parent's Data.db; the slice is those ranges concatenated in order, which is sound because cell ordinals
 * stay consecutive across the join: every run but the last contributes whole cells, so the child's cell {@code k} still
 * covers child bytes {@code [k*G, (k+1)*G)}. What changes per run is only the rebase, by {@link Run#shift}.
 * <p>
 * What the slice carries that nobody asked for is DEAD SPACE, in three places:
 * <ul>
 *   <li>a DEAD PREFIX of {@code lo mod G} bytes -- the head of the first cell, holding the tail of a partition
 *       that starts before the range;</li>
 *   <li>INTERIOR gaps: where two sections are less than a cell apart they stay in one run, and the partitions
 *       between them come along inside it;</li>
 *   <li>a dead SUFFIX, for a COMPRESSED slice only. {@code dataLength} stops at the last live byte, so the rest
 *       of the final chunk is outside it and unaddressable -- but the chunk cannot be cut, so all of it is still
 *       transferred and stored. An uncompressed slice has no suffix: it recomputes that one CRC and really does
 *       cut the cell.</li>
 * </ul>
 * None of the three is indexed, so no read can reach any of them: every read path enters Data.db at a position
 * taken from Index.db. They are transferred and stored for nothing until the sstable is compacted, which is what
 * {@code zero_copy_partial_stream_max_dead_space_ratio} bounds -- see {@code Plan.deadRatio()}, which counts all
 * three. Only the first two are inside {@code dataLength}, and only the INTERIOR ones need
 * {@code StatsMetadata#hasUnindexedRegions}; see {@code Plan.interiorDeadBytes()}.
 *
 * <h2>Both formats</h2>
 * Which index components are synthesised is the only place the parent's format shows through, and
 * {@link #componentsFor} is the whole of it:
 * <ul>
 *   <li><b>BIG</b> gets an Index.db with one rewritten position per record and the Summary.db that addresses it;</li>
 *   <li><b>BTI</b> gets a Partitions.db rebuilt over the slice's keys and a Rows.db holding the copied row index
 *       entries of exactly the partitions the slice claims.</li>
 * </ul>
 * The second half of that is the one thing a slice needs that a split does not. A split child holds a contiguous
 * run of the parent's partitions, so its Rows.db could be one verbatim range; a slice's runs can be spread across
 * the whole parent, and carrying the row indexes of everything in between would put bytes on the wire that no read
 * of the slice can reach -- with vnode-shaped ranges, potentially more than the slice was asked for. So entries are
 * copied one at a time, which makes where each one lands the caller's problem;
 * {@code BtiZeroCopySplit.RowIndexCopier} is where that is solved and why.
 * <p>
 * BTI also costs one thing BIG does not: the full keys of partitions with no row index exist only inside Data.db,
 * so a slice of a narrow-partition BTI table decompresses the chunks it is sending in order to read them. The
 * counting pass does not -- keys are resolved lazily -- so it is one pass, over the slice and not over the parent.
 * {@code BtiZeroCopySplit} has the full argument.
 *
 * <h2>Correctness notes that are not in the splitter</h2>
 * <ul>
 *   <li>INTERIOR dead regions are new here; a split child only ever has the prefix. A prefix can be stepped over and an
 *       interior gap cannot, so two mechanisms cover them. {@code Verifier} and {@code Scrubber} walk Data.db partition
 *       by partition, seeking to the next index position, so they step over both. {@code SSTableSimpleScanner} -- the
 *       linear full-scan reader compaction, cleanup and repair validation use -- cannot be steered that way, since the
 *       sections it is given resolve only their endpoints through the index; it is refused outright via
 *       {@code StatsMetadata#hasUnindexedRegions}, honoured by every {@code SSTableReader.getScanner} overload.</li>
 *   <li>That marker is INHERITED as well as set: it is the parent's flag OR'd with this slice's own interior gaps,
 *       never a literal answer about this slice alone. A slice of a slice -- node C bootstrapping a sub-range of what
 *       node B was sent -- has interior gaps of its own only by luck, and every single-section slice has none by
 *       construction, so computing the marker fresh would CLEAR one the parent already carried and hand the child to
 *       the linear scanner, which would then return partitions the child does not claim and write them out as its own
 *       at its next compaction. {@link ZeroCopySSTableSplitter} inherits it for the same reason.</li>
 *   <li>Because the marker has to survive, {@link #plan} refuses any parent whose sstable version cannot hold it
 *       ({@code Version#hasUnindexedRegionsMarker}, BIG {@code pb} / BTI {@code eb} and later). Older versions are
 *       still supported and still streamable, just not sliceable: a slice keeps the parent's version, and one that
 *       drops the marker on serialisation is the same silent corruption as clearing it.</li>
 *   <li>Statistics.db carries the splitter's accepted imprecision verbatim (see {@link ZeroCopySSTableSplitter}'s
 *       class javadoc) because the same code writes it; the receiver then mutates level and repair state, so only the
 *       inherited totals and bounds survive.</li>
 *   <li>Digest.crc32 is not synthesised here, and the receiver does not synthesise one either: it covers every byte
 *       of the child's Data.db, and those bytes reach the socket by {@code sendfile} without entering the process,
 *       so the sender cannot compute one -- and a digest the receiver computed from the bytes that arrived could not
 *       distinguish them from the bytes that were sent, which is the only thing a digest is for. A received slice
 *       therefore has no Digest.crc32, and {@code Verifier} answers its absence with extended verification, which
 *       actually reads the sstable -- see {@link SSTableZeroCopyWriter}.</li>
 *   <li>Components are written with the splitter's fsyncs -- unnecessary here (they are read back at once and
 *       deleted), but a few small fsyncs are cheaper than a second copy of {@code writeStatistics}.</li>
 * </ul>
 */
public final class ZeroCopySSTableSlice
{
    private static final Logger logger = LoggerFactory.getLogger(ZeroCopySSTableSlice.class);

    /**
     * Synthesised for a compressed BIG parent. Data.db is absent: it stays in the parent, sent as ranges of it.
     * <p>
     * The index components are the format's, and they are the only difference between the four lists below: BIG
     * needs an Index.db and the Summary.db that addresses it, BTI a Partitions.db and the Rows.db its payloads point
     * into. CompressionInfo.db or CRC.db is the other axis. Everything else -- Statistics.db, Filter.db -- is
     * common, and Digest.crc32 is on neither list because a slice has none: the sender cannot compute one over bytes
     * that never enter the process, and the receiver must not invent one (see the class javadoc).
     */
    public static final List<Component> COMPRESSED_COMPONENTS = ImmutableList.of(BigFormat.Components.PRIMARY_INDEX,
                                                                                Components.COMPRESSION_INFO,
                                                                                Components.STATS,
                                                                                BigFormat.Components.SUMMARY,
                                                                                Components.FILTER);

    /** Synthesised for an uncompressed BIG parent: CRC.db in place of CompressionInfo.db. */
    public static final List<Component> UNCOMPRESSED_COMPONENTS = ImmutableList.of(BigFormat.Components.PRIMARY_INDEX,
                                                                                  Components.CRC,
                                                                                  Components.STATS,
                                                                                  BigFormat.Components.SUMMARY,
                                                                                  Components.FILTER);

    /** Synthesised for a compressed BTI parent. */
    public static final List<Component> COMPRESSED_BTI_COMPONENTS =
        ImmutableList.of(BtiFormat.Components.PARTITION_INDEX,
                         BtiFormat.Components.ROW_INDEX,
                         Components.COMPRESSION_INFO,
                         Components.STATS,
                         Components.FILTER);

    /** Synthesised for an uncompressed BTI parent. */
    public static final List<Component> UNCOMPRESSED_BTI_COMPONENTS =
        ImmutableList.of(BtiFormat.Components.PARTITION_INDEX,
                         BtiFormat.Components.ROW_INDEX,
                         Components.CRC,
                         Components.STATS,
                         Components.FILTER);

    /** Everything {@link #delete} has to consider, whichever format produced it. */
    public static final Set<Component> ALL_SYNTHESISED =
        ImmutableSet.<Component>builder().addAll(COMPRESSED_COMPONENTS)
                                         .addAll(UNCOMPRESSED_COMPONENTS)
                                         .addAll(COMPRESSED_BTI_COMPONENTS)
                                         .addAll(UNCOMPRESSED_BTI_COMPONENTS)
                                         .build();

    /** The components a slice of a {@code format} parent synthesises. */
    public static List<Component> componentsFor(SSTableFormat<?, ?> format, boolean compressed)
    {
        if (BtiFormat.is(format))
            return compressed ? COMPRESSED_BTI_COMPONENTS : UNCOMPRESSED_BTI_COMPONENTS;
        return compressed ? COMPRESSED_COMPONENTS : UNCOMPRESSED_COMPONENTS;
    }

    /**
     * Runaway guard, not a tuning knob: a run only costs one more plan entry and one more {@code sendfile} range,
     * and dead space already bounds how many runs a real range set produces. Vnode-shaped requests produce hundreds.
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
        /** Neither BIG nor BTI, so there is no index whose positions this could rebase. */
        WRONG_FORMAT,
        /**
         * A pre-4.0 Filter.db, which {@code CassandraOutgoingFile} refuses to send a whole sstable in; a slice goes
         * out over that same entire-sstable path, so it refuses on the same test.
         */
        LEGACY_BLOOM_FILTER,
        /**
         * An sstable version whose Statistics.db cannot carry {@code StatsMetadata#hasUnindexedRegions}. A slice keeps
         * the parent's version, and the marker is the only thing standing between an interior dead region and the
         * linear scanner, so a version that would drop it on serialisation cannot be sliced at all -- not even when
         * this particular slice has no interior gap, because the parent may already have been carrying the flag.
         */
        NO_UNINDEXED_REGIONS_MARKER,
        /**
         * Chunks compressed against a dictionary. Rebasing offsets is dictionary-agnostic, but nothing has proven the
         * dictionary survives the round trip, so this refuses rather than risk undecompressible output. Mirrors
         * {@link ZeroCopySSTableSplitter#isSupported}.
         */
        COMPRESSION_DICTIONARY,
        /** Reconciling those needs the rows. */
        LEGACY_COUNTER_SHARDS,
        NO_SECTIONS,
        /** Sections are not sorted and disjoint, or reach past the parent's data. */
        MALFORMED_SECTIONS,
        TOO_MANY_RUNS,
        DEAD_SPACE,
        /** The parent's own CompressionInfo.db / CRC.db / Statistics.db do not support the arithmetic. */
        PARENT_UNSUITABLE,
        /**
         * The table has storage-attached indexes. THE authoritative gate, asked of the table and not of this
         * sstable's component set, because the case that matters is the one a component test cannot see: a
         * {@code CREATE INDEX} on a populated table, where the index exists and the sstables do not carry its
         * components yet. A slice synthesises a fixed component list and sends only that, so depending on which
         * file of the session happens to arrive last the receiver either fails
         * {@code validateSSTableAttachedIndexes(readers, true, true)} -- taking down every repair and bootstrap for
         * the duration of the build -- or publishes an sstable that is permanently missing its per-sstable
         * completion marker, whose rows are readable and answer no index predicate, silently and for ever. See
         * {@code SecondaryIndexManager#hasSSTableAttachedIndexes} and the same gate on the anticompaction path in
         * {@code CompactionManager}. Refusing sends the sstable partition-by-partition instead, which builds the
         * index components on the receiver through the ordinary flush observers.
         */
        SSTABLE_ATTACHED_INDEXES,
        /**
         * The parent would stream components this cannot synthesise. {@link #SSTABLE_ATTACHED_INDEXES} is the gate
         * that is meant to catch this in practice; this is the BACKSTOP for a table whose {@code ColumnFamilyStore}
         * is not reachable (offline tooling) and for component types nobody has thought about yet. A slice
         * synthesises a fixed list and sends only that, so those components would simply not arrive.
         */
        EXTRA_STREAMING_COMPONENTS
    }

    /** One contiguous range of the parent's Data.db and where it lands in the slice; cells are the PARENT's grid. */
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

    /** Where the slice's Data.db bytes are and what its components will say. Arithmetic only: no Index.db read yet. */
    public static final class Plan
    {
        public final Reason reason;

        /** The parent's format, which decides which index components {@link #components()} names. */
        public final SSTableFormat<?, ?> format;

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
        /**
         * Uncompressed bytes INSIDE the slice that no read can reach: the dead prefix plus any interior gaps.
         * Bounded by {@code dataLength}, which is what makes it the right input to
         * {@link #interiorDeadBytes()} and hence to {@code StatsMetadata#hasUnindexedRegions}.
         */
        public final long deadBytes;
        /**
         * Uncompressed bytes of the final cell PAST the last live byte. Outside {@code dataLength} -- no read can
         * address them at all -- but a compressed slice still transfers and stores them, because a compressed chunk
         * cannot be cut. Zero for an uncompressed slice, which recomputes its final CRC and so really does cut.
         * Counted by {@link #deadRatio()} and excluded from {@link #deadBytes} on purpose.
         */
        public final long suffixBytes;
        /**
         * The table's {@code bloom_filter_fp_chance} as it was when the plan was made. Frozen here rather than read
         * again in {@link #write}, because {@link #writesFilter()} follows from it and that decides the component
         * COUNT -- which {@code CassandraOutgoingFile.getNumFiles()} promises to the peer before the write and cannot
         * take back. An {@code ALTER TABLE} crossing 1.0 in between used to make the promise and the stream disagree.
         */
        public final double bloomFilterFpChance;

        private Plan(Reason reason, SSTableFormat<?, ?> format, List<Run> runs,
                     List<PartitionPositionBounds> sections, int cellLength,
                     boolean compressed, long dataLength, long physicalBytes, long usefulBytes, long deadBytes,
                     long suffixBytes, double bloomFilterFpChance)
        {
            this.reason = reason;
            this.format = format;
            this.runs = runs;
            this.sections = sections;
            this.cellLength = cellLength;
            this.compressed = compressed;
            this.dataLength = dataLength;
            this.physicalBytes = physicalBytes;
            this.usefulBytes = usefulBytes;
            this.deadBytes = deadBytes;
            this.suffixBytes = suffixBytes;
            this.bloomFilterFpChance = bloomFilterFpChance;
        }

        static Plan ineligible(Reason reason)
        {
            Preconditions.checkArgument(reason != Reason.ELIGIBLE);
            // fp chance 1.0 so writesFilter() is false for a plan that writes nothing at all.
            return new Plan(reason, null, ImmutableList.of(), ImmutableList.of(), 0, false, 0, 0, 0, 0, 0, 1.0);
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

        /**
         * What {@code zero_copy_partial_stream_max_dead_space_ratio} bounds: every uncompressed byte the transfer
         * carries for nothing, over everything it carries, {@link #suffixBytes} included.
         */
        public double deadRatio()
        {
            long carried = dataLength + suffixBytes;
            return carried == 0 ? 0.0 : (double) (deadBytes + suffixBytes) / carried;
        }

        /**
         * Carried partitions that sit BETWEEN indexed ones, i.e. the dead bytes other than the prefix. Only these need
         * {@code StatsMetadata#hasUnindexedRegions}: a scan's sections begin at positions taken from the index
         * ({@code getPositionsForRanges}), so the prefix is stepped over but an interior gap is walked into.
         */
        public long interiorDeadBytes()
        {
            return Math.max(0, deadBytes - lo() % cellLength);
        }

        public List<Component> components()
        {
            Preconditions.checkState(isEligible(), "an ineligible plan synthesises nothing: %s", this);
            return componentsFor(format, compressed);
        }

        /**
         * Whether the slice will have a Filter.db, so that the count promised to the peer and the files written agree
         * whatever the table's parameters do in between. An fp chance of 1.0 yields an {@code AlwaysPresentFilter}
         * whose {@code serialize()} is a no-op, leaving a zero-length component; the read path treats missing and
         * empty alike ({@code FilterComponent.load}), so none is written at all.
         */
        public boolean writesFilter()
        {
            return bloomFilterFpChance < 1.0;
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

    /**
     * Section index bounds of each run: {@code {firstSection, lastSection}}, inclusive. Consecutive sections stay in
     * one run while their cells touch; a gap of a whole cell or more leaves that cell out, so it starts a new run.
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

    /** The slice's uncompressed length: whole cells for every run but the last, which stops at the last live byte. */
    public static long dataLength(List<PartitionPositionBounds> sections, int cellLength)
    {
        List<int[]> bounds = runBounds(sections, cellLength);
        long length = 0;
        for (int r = 0; r < bounds.size(); r++)
        {
            long firstCell = firstChunk(sections.get(bounds.get(r)[0]).lowerPosition, cellLength);
            long hi = sections.get(bounds.get(r)[1]).upperPosition;
            length += (r == bounds.size() - 1)
                      // A COMPRESSED slice still transfers the rest of that final chunk; Plan.suffixBytes accounts
                      // for it, and is zero for an uncompressed one, which cuts the cell.
                      ? hi - firstCell * cellLength
                      : (lastChunk(hi, cellLength) - firstCell + 1) * (long) cellLength;
        }
        return length;
    }

    /**
     * Uncompressed bytes the slice carries that no read can reach: what it is long enough to hold, less what was asked
     * for. Sections must be sorted, disjoint and non-empty.
     */
    public static long deadBytes(List<PartitionPositionBounds> sections, int cellLength)
    {
        long useful = 0;
        for (PartitionPositionBounds section : sections)
            useful += section.upperPosition - section.lowerPosition;
        return dataLength(sections, cellLength) - useful;
    }

    /**
     * Plan a slice of {@code parent} covering {@code sections}, or say why not. Reads no index and no data: the
     * in-memory compression metadata or the four-byte header of CRC.db, two schema lookups for the table's indexes,
     * and arithmetic -- cheap enough to call per sstable while a stream plan is assembled.
     * @param sections          as produced by {@link SSTableReader#getPositionsForRanges}: sorted, disjoint,
     *                          {@code [first partition start, one past last partition end)}
     * @param maxDeadSpaceRatio refuse if dead space is more than this fraction of the slice; 1.0 accepts any
     */
    public static Plan plan(SSTableReader parent, List<PartitionPositionBounds> sections, double maxDeadSpaceRatio)
    {
        Preconditions.checkNotNull(parent, "parent");

        if (!ZeroCopySSTableSplitter.isSupportedFormat(parent.descriptor.getFormat()))
            return Plan.ineligible(Reason.WRONG_FORMAT);
        // A slice keeps the parent's version, and write() has to be able to record hasUnindexedRegions in it -- both
        // the flag this slice's own interior gaps earn and any the parent was already carrying. A version that cannot
        // hold it is still supported and still streamable, just not sliceable.
        if (!parent.descriptor.version.hasUnindexedRegionsMarker())
            return Plan.ineligible(Reason.NO_UNINDEXED_REGIONS_MARKER);
        // A slice goes out over the entire-sstable path, so anything that path refuses outright is refused here too:
        // see computeShouldStreamEntireSSTables(), whose other tests are stream_entire_sstables and the one below.
        if (parent.descriptor.version.hasOldBfFormat())
            return Plan.ineligible(Reason.LEGACY_BLOOM_FILTER);
        if (parent.getSSTableMetadata().hasLegacyCounterShards)
            return Plan.ineligible(Reason.LEGACY_COUNTER_SHARDS);
        if (sections == null || sections.isEmpty())
            return Plan.ineligible(Reason.NO_SECTIONS);
        // The authoritative storage-attached-index gate, and the one the component test below cannot stand in for:
        // during a CREATE INDEX on a populated table the index exists and no sstable carries its components yet, so
        // the difference is empty and the sstable looks eligible in exactly the window where slicing it is worst.
        // Asked of the table, the same way CompactionManager gates anticompaction. Reachable without plumbing a
        // parameter through the stream plan because the reader knows its table id; null only offline, where the
        // component backstop below is all there is.
        ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(parent.metadata().id);
        if (cfs != null && cfs.indexManager.hasSSTableAttachedIndexes())
        {
            logger.debug("Not slicing {} for streaming: its table has storage-attached indexes, whose components a" +
                         " slice cannot synthesise", parent.descriptor);
            return Plan.ineligible(Reason.SSTABLE_ATTACHED_INDEXES);
        }
        // MetadataSerializer fabricates defaults for a missing Statistics.db instead of failing, and the
        // SerializationHeader in it is the one thing a slice cannot do without.
        if (!parent.descriptor.fileFor(Components.STATS).exists())
            return Plan.ineligible(Reason.PARENT_UNSUITABLE);
        // Backstop to the gate above, for an offline reader with no ColumnFamilyStore and for component types nobody
        // has thought about yet. Exactly the set ComponentManifest.ordered() would reject: it takes its order from
        // allComponents(), so a streamable component that is not one of the format's own -- every storage-attached
        // index component, which registers its own non-singleton Component.Type -- cannot go in a slice's manifest at
        // all. Better to refuse here, in the constructor of CassandraOutgoingFile and before getNumFiles() promises
        // the peer a count, than to have writeSlice throw once the promise is out and leave the receive task short.
        // Note this cannot replace the gate: a table mid-CREATE-INDEX has the indexes and not yet the components.
        Set<Component> extra = Sets.difference(parent.getStreamingComponents(),
                                               parent.descriptor.getFormat().allComponents());
        if (!extra.isEmpty())
        {
            logger.debug("Not slicing {} for streaming: it would stream {}, which a slice cannot synthesise",
                         parent.descriptor, extra);
            return Plan.ineligible(Reason.EXTRA_STREAMING_COMPONENTS);
        }

        // The helpers below throw on input they consider impossible; a failed session is the wrong response, since
        // refusing only costs the row-by-row path, which handles anything.
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

        // TODO: liftable -- chunk bytes are copied verbatim and Writer.open below serialises the dictionary into the
        // slice's own CompressionInfo.db, so it should round trip, but a wrong answer is undecompressible data on the
        // receiver. Remove together with the same refusal in ZeroCopySSTableSplitter.isSupported().
        if (meta.compressionDictionary() != null)
            return Plan.ineligible(Reason.COMPRESSION_DICTIONARY);

        int cellLength = meta.chunkLength();
        long parentDataLength = meta.dataLength;

        if (parent.uncompressedLength() != parentDataLength)
            return Plan.ineligible(Reason.PARENT_UNSUITABLE);

        // The offsets table has to address every chunk a run can reach. MORE is fine (a compaction-produced sstable
        // carries a trailing zero-length chunk, chunkEnd()'s problem); fewer means it disagrees with its dataLength.
        long neededCells = (parentDataLength + cellLength - 1) / cellLength;
        if (neededCells > meta.offHeapSize() / 8)
            return Plan.ineligible(Reason.PARENT_UNSUITABLE);

        return build(parent, sections, cellLength, parentDataLength, true, maxDead,
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
        // Physical and uncompressed are the same bytes here; anything else and the file is not what the reader thinks.
        if (physicalLength != parentDataLength)
            return Plan.ineligible(Reason.PARENT_UNSUITABLE);
        // One CRC per cell, after the 4-byte header.
        if (crc.length() < 4 + 4 * ((parentDataLength + cellLength - 1) / cellLength))
            return Plan.ineligible(Reason.PARENT_UNSUITABLE);

        return build(parent, sections, cellLength, parentDataLength, false, maxDead,
                     cell -> cell * (long) cellLength,
                     // A cell CAN be cut here, so the last run ends at the last live byte and that CRC is recomputed.
                     (cell, exactEnd) -> exactEnd > 0 ? exactEnd : Math.min((cell + 1) * (long) cellLength, parentDataLength),
                     physicalLength);
    }

    /** Physical offset of the start of a cell. */
    private interface CellStart
    {
        long apply(long cell);
    }

    /**
     * Physical offset one past the end of a cell. {@code exactEnd} is positive only for the final cell of the final
     * run, and only a format that can cut a cell may honour it.
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
            // Only the last run may stop short of a cell end; whole cells elsewhere, or the join breaks the grid.
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

        // The grid invariant over the slice: its last cell holds at least one live byte and at most a full cell.
        long cellCount = childCellBase;
        if (cellCount > Integer.MAX_VALUE)   // CompressionMetadata.Writer.finalizeLength takes an int
            return Plan.ineligible(Reason.PARENT_UNSUITABLE);
        if (!((cellCount - 1) * (long) cellLength < dataLength && dataLength <= cellCount * (long) cellLength))
            return Plan.ineligible(Reason.PARENT_UNSUITABLE);

        // The final cell's suffix is outside dataLength but a COMPRESSED slice still transfers it -- a chunk cannot be
        // cut -- so dataLength - useful alone would leave up to a cell of waste out of the ratio: a narrow range
        // starting exactly on a cell boundary reported 0% dead and was accepted even at maxDeadSpaceRatio = 0.
        // For an UNCOMPRESSED slice there is no suffix to count: planUncompressed's last run ends exactly at hi and
        // writeCrc recomputes that one CRC, so nothing past hi is sent. Charging it anyway inflated deadRatio() and
        // refused eligible slices -- including ones with literally no dead space -- at the default 0.25.
        long suffix = 0;
        if (compressed)
        {
            Run lastRun = runs.get(runs.size() - 1);
            long lastCellUncompressedEnd = Math.min((lastRun.lastCell + 1) * (long) cellLength, parentDataLength);
            suffix = Math.max(0, lastCellUncompressedEnd - sections.get(sections.size() - 1).upperPosition);
        }
        long dead = dataLength - useful;
        if (dead < 0)
            return Plan.ineligible(Reason.PARENT_UNSUITABLE);

        Plan plan = new Plan(Reason.ELIGIBLE, parent.descriptor.getFormat(), ImmutableList.copyOf(runs),
                             ImmutableList.copyOf(sections), cellLength,
                             compressed, dataLength, childPhysicalBase, useful, dead, suffix,
                             // Frozen with the plan; see Plan.bloomFilterFpChance and Plan.writesFilter().
                             parent.metadata().params.bloomFilterFpChance);
        if (plan.deadRatio() > maxDeadSpaceRatio)
            return Plan.ineligible(Reason.DEAD_SPACE);
        return plan;
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

    /**
     * Write every component of the planned slice except Data.db into {@code target}. Two passes over the parent's
     * primary index, one to count the partitions (so the filter -- and for BIG the summary -- is sized exactly
     * rather than guessed) and one to write, both bounded by the runs, so the cost is proportional to the slice and
     * not to the parent. Both start at the first key of the first run: through the index summary for BIG, as a trie
     * lower bound for BTI. The counting pass reads no keys at all, which for BTI is what keeps it off Data.db.
     *
     * <p>Only ONE step of this is done under {@code parent.runWithLock}, and it is the only one that needs to be: the
     * read of the parent's Statistics.db, which {@code mutateLevelAndReload} and {@code mutateRepairedAndReload}
     * rewrite in place, and whose contents this inherits. The lock is {@code SSTableReader.tidy.global}, the same
     * monitor {@code setReplaced} and {@code markObsolete} take, so the Tracker blocks a finishing compaction behind
     * it -- the bound has to be one open/read/close of a file of a few KiB, not the two index passes, the bloom filter
     * build and the index summary build, which on a multi-GiB parent is tens of seconds. Nothing else here needs it:
     * Index.db, Partitions.db, Rows.db, Data.db and CRC.db are never mutated in place, the compression metadata
     * belongs to the parent's data file, and the in-memory index summary belongs to THIS reader instance -- a
     * redistribution publishes a different reader and leaves ours, which the caller's {@code Ref} keeps alive,
     * untouched.
     *
     * @param target a fresh descriptor whose component files do not exist; nothing tracks the files this leaves there,
     *               so the caller owns them -- {@link #toStreamingTemporaries} to rename them for a stream,
     *               {@link #delete} to remove them
     */
    public static Slice write(SSTableReader parent, Plan plan, Descriptor target) throws IOException
    {
        Preconditions.checkArgument(plan.isEligible(), "not an eligible plan: %s", plan);
        Preconditions.checkArgument(target.version.toString().equals(parent.descriptor.version.toString()),
                                    "slice must keep the parent's sstable version");

        TableMetadata metadata = parent.metadata();

        // Taken first and under the lock, before anything long-running; see the javadoc for the bound.
        Map<MetadataType, MetadataComponent> parentMetadata =
            parent.runWithLock(ignored -> readParentMetadata(parent.descriptor));
        StatsMetadata parentStats = (StatsMetadata) parentMetadata.get(MetadataType.STATS);

        DecoratedKey firstKey = firstKey(parent, plan.lo());

        int partitionCount = countPartitions(parent, plan, firstKey);
        if (partitionCount <= 0)
            throw new IllegalStateException("no index records in " + plan + " of " + parent.descriptor);

        Set<Component> components = Sets.newHashSet(Components.STATS);
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

            // ---------- The index, plus everything else derived from the keys, in one pass ----------
            EstimatedHistogram partitionSizes = new EstimatedHistogram(PARTITION_SIZE_HISTOGRAM_BUCKETS);
            ICardinality cardinality = new HyperLogLogPlus(HLL_P, HLL_SP);
            // From the PLAN, not from metadata.params: whether Filter.db exists decides the component count the peer
            // was already promised, and an ALTER TABLE crossing 1.0 in between must not change it. See
            // Plan.writesFilter() for why 1.0 means no component at all.
            IFilter bf = plan.writesFilter() ? FilterFactory.getFilter(partitionCount, plan.bloomFilterFpChance)
                                             : null;
            KeyRange keys = new KeyRange();

            try
            {
                if (BtiFormat.is(parent.descriptor.getFormat()))
                    components.addAll(writeBtiIndex(parent, plan, target, firstKey, partitionSizes, cardinality,
                                                    bf, keys));
                else
                    components.addAll(writeBigIndex(parent, plan, target, firstKey, partitionCount, partitionSizes,
                                                    cardinality, bf, keys));

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

            // onDiskLength is the runs' physical length, what the receiving node will actually have, so the derived
            // compressionRatio stays true. The keys are the SLICE's own: with version.hasKeyRange() the reader prefers
            // Statistics.db to every other component for its bounds, so inheriting the parent's would claim its
            // whole range.
            writeStatistics(target, metadata, parentMetadata, parentStats, partitionSizes, cardinality,
                            plan.physicalBytes, plan.dataLength, keys.first, keys.last,
                            // Forces every scan through the index; a linear one would hand back the partitions a
                            // copied cell dragged along. See Plan.interiorDeadBytes() for why only interior ones count.
                            // OR'd with the parent's, never a literal answer about this slice: a slice adds unindexed
                            // regions of its own but cannot remove one the parent already had, and every single-section
                            // slice has interiorDeadBytes() == 0 by construction -- so slicing a slice (node C taking
                            // a sub-range of what node B was sent) would clear the marker and hand the child to the
                            // linear scanner. Matches ZeroCopySSTableSplitter, which inherits it for the same reason.
                            plan.interiorDeadBytes() > 0 || parentStats.hasUnindexedRegions,
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
     * BIG: Index.db with one rewritten position per record, and the Summary.db that addresses it.
     *
     * @return the components written
     */
    private static Set<Component> writeBigIndex(SSTableReader parent, Plan plan, Descriptor target,
                                                DecoratedKey firstKey, int partitionCount,
                                                EstimatedHistogram partitionSizes, ICardinality cardinality,
                                                IFilter bf, KeyRange keys) throws IOException
    {
        TableMetadata metadata = parent.metadata();
        try (SequentialWriter out = new SequentialWriter(target.fileFor(BigFormat.Components.PRIMARY_INDEX),
                                                        writerOption());
             IndexSummaryBuilder summary = new IndexSummaryBuilder(partitionCount,
                                                                   metadata.params.minIndexInterval,
                                                                   Downsampling.BASE_SAMPLING_LEVEL))
        {
            walk(parent, plan, firstKey, true, new RecordVisitor()
            {
                long pending = UNRESOLVED;

                public void record(Record record, boolean included, Run run) throws IOException
                {
                    long position = record.position();
                    // One record late because the next offset gives the size exactly. Excluded records advance
                    // `pending` too, since an excluded partition still ends where the next one starts.
                    if (pending != UNRESOLVED)
                    {
                        partitionSizes.add(position - pending);
                        pending = UNRESOLVED;
                    }
                    if (!included)
                        return;
                    pending = position;

                    ByteBuffer key = record.key();
                    int promotedSize = record.promotedSize();
                    byte[] promoted = record.promoted();

                    long indexStart = out.position();
                    ByteBufferUtil.writeWithShortLength(key, out);
                    // The only rewritten field, and a shorter vint than the parent's, so the slice's index
                    // offsets are not the parent's less a constant -- hence Summary.db is rebuilt, not sliced.
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
        requireNonEmpty(target, BigFormat.Components.PRIMARY_INDEX);
        requireNonEmpty(target, BigFormat.Components.SUMMARY);
        return ImmutableSet.of(BigFormat.Components.PRIMARY_INDEX, BigFormat.Components.SUMMARY);
    }

    /**
     * BTI: Partitions.db rebuilt over the slice's keys, and Rows.db copied entry by entry.
     *
     * <p>Only the INCLUDED partitions' row index entries are copied. That is not an optimisation, it is the point:
     * a slice's runs can be spread across the whole parent, so the entries in between would be dead bytes on disk
     * and on the wire, and with vnode-shaped ranges they could outweigh everything the slice was asked for. See
     * {@code BtiZeroCopySplit.RowIndexCopier} for what constrains where each entry may land once they are no longer
     * contiguous.
     *
     * <p>Rows.db is written even when nothing has a row index, and is then zero length -- the same state a flush of
     * a narrow table leaves it in. It stays in the component list either way, because {@code getNumFiles()} has to
     * be predictable from the plan alone and the manifest is what the receiver sizes its writes from.
     *
     * @return the components written
     */
    private static Set<Component> writeBtiIndex(SSTableReader parent, Plan plan, Descriptor target,
                                               DecoratedKey firstKey, EstimatedHistogram partitionSizes,
                                               ICardinality cardinality, IFilter bf, KeyRange keys)
    throws IOException
    {
        try (BtiZeroCopySplit.RowIndexCopier rows = new BtiZeroCopySplit.RowIndexCopier(parent, target,
                                                                                       writerOption());
             BtiZeroCopySplit.PartitionIndexWriter partitions =
                 new BtiZeroCopySplit.PartitionIndexWriter(target, writerOption()))
        {
            walk(parent, plan, firstKey, false, new RecordVisitor()
            {
                long pending = UNRESOLVED;

                public void record(Record record, boolean included, Run run) throws IOException
                {
                    long position = record.position();
                    if (pending != UNRESOLVED)
                    {
                        partitionSizes.add(position - pending);
                        pending = UNRESOLVED;
                    }
                    if (!included)
                        return;
                    pending = position;

                    ByteBuffer key = record.key();
                    long slicePosition = position - run.shift;
                    long payload = record.hasRowIndex() ? rows.copy(record.cursor(), slicePosition)
                                                        : ~slicePosition;

                    DecoratedKey dk = parent.decorateKey(key);
                    keys.add(dk);
                    if (bf != null)
                        bf.add(dk);
                    cardinality.offerHashed(MurmurHash.hash2_64(key, key.position(), key.remaining(), 0));
                    partitions.addEntry(dk, payload);
                }

                public void end(long endPosition)
                {
                    if (pending != UNRESOLVED)
                        partitionSizes.add(endPosition - pending);
                }
            });

            keys.minimise();
            rows.finish();
            partitions.finish();
        }
        requireNonEmpty(target, BtiFormat.Components.PARTITION_INDEX);
        return ImmutableSet.of(BtiFormat.Components.PARTITION_INDEX, BtiFormat.Components.ROW_INDEX);
    }

    /**
     * The slice's CompressionInfo.db: the parent's parameters, its chunk offsets rebased onto the concatenation of the
     * runs, and the slice's own dataLength. {@code offsets[0]} is 0 (no alignment pad, unlike a split child: nothing is
     * reflinked) and offsets must stay contiguous across a run boundary, since a reader takes a chunk's compressed
     * length from successive offsets -- a physical gap would hand compressed bytes back as row data.
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
     * The slice's CRC.db: the parent's chunk size, then its per-cell CRC32 for every cell of every run, in the slice's
     * order -- these only line up with {@code ChecksumValidator}'s addressing because every run but the last gives
     * whole cells. A cut inside the last cell means recomputing that one CRC: one read of at most a cell, against a
     * dead suffix {@code Scrubber}'s linear walk would try to read as a partition.
     */
    private static void writeCrc(Descriptor target, SSTableReader parent, Plan plan) throws IOException
    {
        File parentCrc = parent.descriptor.fileFor(Components.CRC);
        Run lastRun = plan.runs.get(plan.runs.size() - 1);
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
     * A fresh descriptor in the parent's directory, version and format, for {@link #write} to build the slice's
     * components under. It allocates a real, unused sstable id, because the writers this shares with
     * {@link ZeroCopySSTableSplitter} all address their output as {@code descriptor.fileFor(component)} and a
     * {@link Descriptor} cannot name a temporary. So while a slice is being synthesised its components ARE named like
     * a live sstable's -- with no Data.db and no TOC.txt beside them, which is the shape
     * {@code ColumnFamilyStore.scrubDataDirectories} removes at startup, but only at startup and only because they
     * happen to look orphaned rather than because anything recorded them.
     * <p>
     * That window is bounded by {@link #write}, which deletes on any failure. What must NOT be left in that shape is
     * the far longer window while the slice is on the wire, so the caller hands the finished files to
     * {@link #toStreamingTemporaries} first; see there.
     */
    public static Descriptor newDescriptor(SSTableReader parent)
    {
        return ZeroCopySSTableSplitter.descriptorAllocator(parent).get();
    }

    /**
     * Rename a finished slice's component files to streaming temporaries of the PARENT's descriptor
     * ({@link Descriptor#tmpFileForStreaming}), and return where each one landed.
     * <p>
     * A slice is on the wire for as long as its Data.db ranges take to send, which for a multi-GiB run is minutes. Left
     * under {@link #newDescriptor}'s name for that whole time, a crash leaves {@code <version>-<id>-<format>-{Index,
     * Statistics,Filter,Summary,CompressionInfo}.db} with no Data.db: covered by no transaction log, indistinguishable
     * from a live sstable to anything that does not check for Data.db, and reclaimed only by the next restart. Under a
     * {@code .<uuid>.tmp} name it is instead exactly what the whole-sstable path's hardlinks are -- removable by
     * {@code scrubDataDirectories} and listed by {@link Descriptor#getTemporaryFiles()} -- while
     * {@code ComponentContext.close} still removes it on the ordinary path.
     * <p>
     * Renames within one directory, so it costs a directory entry per component and cannot half-write a file. On
     * failure the caller deletes both sets; the returned map is complete or the method threw.
     */
    public static Map<Component, File> toStreamingTemporaries(Descriptor parent, Slice slice)
    {
        Map<Component, File> moved = new LinkedHashMap<>(slice.components.size());
        try
        {
            for (Component component : slice.components)
            {
                File temporary = parent.tmpFileForStreaming(component);
                slice.descriptor.fileFor(component).move(temporary);
                moved.put(component, temporary);
            }
        }
        catch (Throwable t)
        {
            for (File file : moved.values())
            {
                try
                {
                    file.deleteIfExists();
                }
                catch (Throwable suppressed)
                {
                    t.addSuppressed(suppressed);
                }
            }
            throw t;
        }
        return moved;
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

    /** No record's offset can be this, so it doubles as "not filled in yet". */
    private static final long UNRESOLVED = -1;

    private interface RecordVisitor
    {
        /**
         * @param included whether the record's partition is in a requested section; excluded ones are the dead space,
         *                 present in a run but absent from the slice's index, or between runs and not sent at all
         * @param run      the run the record falls in, whose {@code shift} rebases it; only meaningful if included
         */
        void record(Record record, boolean included, Run run) throws IOException;

        /** @param endPosition where the slice's last record ends: the first offset past it, or the parent's end. */
        void end(long endPosition);
    }

    /**
     * One partition of the parent as the walk sees it. {@link #position} is all either format needs from an excluded
     * record; the rest is only ever asked for when the record is included, and is deliberately lazy -- for BTI a
     * {@link #key()} can mean decompressing a Data.db chunk, which the counting pass must not pay for.
     *
     * <p>The format-specific accessors are grouped rather than split into two interfaces because the walk's own
     * bookkeeping -- section and run cursors, monotonicity, where the slice ends -- is the part worth having exactly
     * once, and a visitor only ever calls the accessors of the format it was written for.
     */
    private interface Record
    {
        /** The partition's position in the PARENT's Data.db. */
        long position();

        /** The partition key. */
        ByteBuffer key() throws IOException;

        /** BIG: the record's promoted row index, or null. Already relative to the partition, so copied verbatim. */
        byte[] promoted();

        /** BIG: the promoted index's length, which is written even when it is zero. */
        int promotedSize();

        /** BTI: whether this partition has a Rows.db entry to copy. */
        boolean hasRowIndex();

        /** BTI: the cursor positioned on this record, for {@code BtiZeroCopySplit.RowIndexCopier}. */
        BtiZeroCopySplit.Cursor cursor();
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
     * The first key of the slice, read out of Data.db -- which is what keeps both formats' passes proportional to the
     * slice. {@code lo} is a partition start, so the bytes there are that partition's key (one chunk decompressed).
     * BIG resolves it through the index summary to an Index.db offset to seek to, within one
     * {@code min_index_interval} of its record; BTI uses it as the lower bound of a trie iteration.
     * <p>
     * BIG asks the summary directly because {@code BigTableReader.getIndexScanPosition} is package-private; all that
     * wrapper adds is clamping a key below {@code getFirst()} for a {@code MOVED_START} reader, which cannot apply
     * while {@code lo} is inside the reader's live range. Erring low is harmless either way: the walk skips records
     * before {@code lo}, and BTI's bound is on stored prefixes so it can undershoot regardless.
     */
    private static DecoratedKey firstKey(SSTableReader parent, long lo)
    {
        try (FileDataInput in = parent.getFileDataInput(lo))
        {
            return parent.decorateKey(ByteBufferUtil.readWithShortLength(in));
        }
        catch (IOException e)
        {
            throw new CorruptSSTableException(e, parent.descriptor.fileFor(Components.DATA));
        }
    }

    private static int countPartitions(SSTableReader parent, Plan plan, DecoratedKey first)
    {
        int[] count = { 0 };
        walk(parent, plan, first, false, new RecordVisitor()
        {
            public void record(Record record, boolean included, Run run)
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
     * One scoped, sequential pass of the parent's primary index: from the record holding {@code first} to the first
     * record past the last run, saying of every record in between whether it is in a requested section and which run
     * holds it.
     *
     * <p>The bookkeeping -- ordered section and run cursors, the strictly-increasing check, the two ways the walk can
     * end -- is here and is the same for both formats. Only how a record is READ differs, which is what
     * {@link Cursor} abstracts.
     *
     * @param readPromoted BIG only: whether an included record's promoted row index is needed. False for the
     *                     counting pass, which would otherwise read the whole index twice over.
     */
    private static void walk(SSTableReader parent, Plan plan, DecoratedKey first, boolean readPromoted,
                             RecordVisitor visitor)
    {
        List<PartitionPositionBounds> sections = plan.sections;
        long lo = plan.lo();
        long hi = plan.hi();

        try (Cursor cursor = BtiFormat.is(parent.descriptor.getFormat())
                             ? new BtiCursor(parent, first)
                             : new BigCursor(parent, first, readPromoted))
        {
            int section = 0;
            int run = 0;
            long previous = UNRESOLVED;
            boolean started = false;

            while (cursor.advance())
            {
                long position = cursor.position();

                if (previous != UNRESOLVED && position <= previous)
                    throw new IllegalStateException("parent index positions are not strictly increasing: " +
                                                    previous + " -> " + position + " in " + parent.descriptor);
                previous = position;

                if (position < lo)
                {
                    // Scan slack: BIG's summary position and BTI's prefix-bounded iterator are both at or before the
                    // slice's first record, never after it. prepare() still has to be called -- it is what steps
                    // over the rest of the record, and skipping it would desynchronise the next one.
                    cursor.prepare(false);
                    continue;
                }

                if (!started)
                {
                    // lo came from getPositionsForRanges, which takes it from a record of this very index, so the
                    // first record at or past it must be exactly it. Anything else means the two disagree.
                    if (position != lo)
                        throw new IllegalStateException("the slice starts at " + lo + " but the first index " +
                                                        "record at or past it is at " + position + " in " +
                                                        parent.descriptor);
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

                cursor.prepare(included);
                visitor.record(cursor, included, plan.runs.get(run));
            }

            if (!started)
                throw new IllegalStateException("no index record at or past " + lo + " in " + parent.descriptor);
            // No record past hi: the slice runs to the end of the parent's data, which is the same value the last
            // section was given as its upper bound.
            visitor.end(hi);
        }
        catch (IOException e)
        {
            throw new CorruptSSTableException(e, parent.descriptor.fileFor(Components.DATA));
        }
    }

    /** A forward-only reader of the parent's primary index that also serves as the {@link Record} handed out. */
    private interface Cursor extends Record, Closeable
    {
        /**
         * Read the next record's fixed part -- enough for {@link Record#position()} -- or return false at the end.
         * Must be followed by exactly one {@link #prepare} before the next call, unless the walk is stopping.
         */
        boolean advance() throws IOException;

        /**
         * Step over the rest of the current record, retaining whatever an included one needs. Separate from
         * {@link #advance} so that a format which can skip work for an excluded record does -- for BIG that is not
         * reading a promoted row index that is about to be thrown away -- which is also why it is MANDATORY: it is
         * the call that leaves the reader positioned on the next record.
         */
        void prepare(boolean included) throws IOException;
    }

    /**
     * BIG: one Index.db record at a time, seeked to the index summary's scan position for the slice's first key.
     *
     * <p>Buffered rather than mmap'd, so no record can straddle a mapping boundary -- and opened straight off the
     * descriptor, so it starts at offset 0 with no reader-level adjustment.
     */
    private static final class BigCursor implements Cursor
    {
        private final File file;
        private final RandomAccessReader in;
        private final long length;
        private final boolean readPromoted;

        private ByteBuffer key;
        private long position = UNRESOLVED;
        private int promotedSize;
        private byte[] promoted;
        /** True between an advance() that left promoted bytes unread and the prepare() that consumes them. */
        private boolean prepared;

        BigCursor(SSTableReader parent, DecoratedKey first, boolean readPromoted)
        {
            this.file = parent.descriptor.fileFor(BigFormat.Components.PRIMARY_INDEX);
            this.in = RandomAccessReader.open(file);
            this.length = in.length();
            this.readPromoted = readPromoted;

            // Safe: plan() refused anything but BIG and BTI, and BIG's reader is the IndexSummarySupport one. The
            // summary maps the key to the sampled index position at or before its record -- within one
            // min_index_interval of it -- which is why the walk skips records below lo.
            long scanFrom = ((IndexSummarySupport<?>) parent).getIndexSummary().getScanPosition(first);
            if (scanFrom > 0)
                in.seek(scanFrom);
        }

        @Override
        public boolean advance() throws IOException
        {
            if (prepared)
                throw new IllegalStateException("prepare() was not called for the record at " + position);
            if (in.getFilePointer() == length)
                return false;
            key = ByteBufferUtil.readWithShortLength(in);
            position = RowIndexEntry.Serializer.readPosition(in);
            promotedSize = in.readUnsignedVInt32();
            promoted = null;
            prepared = promotedSize > 0;
            return true;
        }

        @Override
        public void prepare(boolean included) throws IOException
        {
            if (promotedSize <= 0)
                return;
            prepared = false;
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

        public long position()
        {
            return position;
        }

        public ByteBuffer key()
        {
            return key;
        }

        public byte[] promoted()
        {
            return promoted;
        }

        public int promotedSize()
        {
            return promotedSize;
        }

        public boolean hasRowIndex()
        {
            throw new UnsupportedOperationException("BIG has no Rows.db");
        }

        public BtiZeroCopySplit.Cursor cursor()
        {
            throw new UnsupportedOperationException("BIG has no Rows.db");
        }

        @Override
        public void close()
        {
            in.close();
        }
    }

    /**
     * BTI: the Partitions.db trie, bounded below by the slice's first key so the walk descends straight to it.
     *
     * <p>Reading a record costs nothing beyond the trie node and, for a partition with a row index, its Rows.db
     * trailer. The key is resolved only when a visitor asks -- see {@code BtiZeroCopySplit.Cursor#key} -- which is
     * what keeps the counting pass off Data.db entirely.
     */
    private static final class BtiCursor implements Cursor
    {
        private final BtiZeroCopySplit.Cursor delegate;

        BtiCursor(SSTableReader parent, DecoratedKey first) throws IOException
        {
            this.delegate = BtiZeroCopySplit.cursor(parent, first);
        }

        @Override
        public boolean advance() throws IOException
        {
            return delegate.advance();
        }

        @Override
        public void prepare(boolean included)
        {
        }

        public long position()
        {
            return delegate.dataPosition();
        }

        public ByteBuffer key() throws IOException
        {
            return delegate.key();
        }

        public byte[] promoted()
        {
            throw new UnsupportedOperationException("BTI has no promoted index");
        }

        public int promotedSize()
        {
            throw new UnsupportedOperationException("BTI has no promoted index");
        }

        public boolean hasRowIndex()
        {
            return delegate.hasRowIndex();
        }

        public BtiZeroCopySplit.Cursor cursor()
        {
            return delegate;
        }

        @Override
        public void close()
        {
            delegate.close();
        }
    }
}
