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
import java.util.Set;

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import com.clearspring.analytics.stream.cardinality.ICardinality;
import com.google.common.collect.Sets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.apache.cassandra.io.sstable.format.big.RowIndexEntry;
import org.apache.cassandra.io.sstable.format.bti.BtiFormat;
import org.apache.cassandra.io.sstable.format.bti.BtiZeroCopySplit;
import org.apache.cassandra.io.sstable.indexsummary.IndexSummary;
import org.apache.cassandra.io.sstable.indexsummary.IndexSummaryBuilder;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.io.util.SequentialWriter;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.EstimatedHistogram;
import org.apache.cassandra.utils.FilterFactory;
import org.apache.cassandra.utils.IFilter;
import org.apache.cassandra.utils.MurmurHash;
import org.apache.cassandra.utils.Throwables;

import static org.apache.cassandra.io.sstable.ZeroCopySSTableSplitter.HLL_P;
import static org.apache.cassandra.io.sstable.ZeroCopySSTableSplitter.HLL_SP;
import static org.apache.cassandra.io.sstable.ZeroCopySSTableSplitter.PARTITION_SIZE_HISTOGRAM_BUCKETS;
import static org.apache.cassandra.io.sstable.ZeroCopySSTableSplitter.requireNonEmpty;
import static org.apache.cassandra.io.sstable.ZeroCopySSTableSplitter.writeFilter;
import static org.apache.cassandra.io.sstable.ZeroCopySSTableSplitter.writeSummary;
import static org.apache.cassandra.io.sstable.ZeroCopySSTableSplitter.writerOption;

/**
 * The per-format half of a {@link ZeroCopySSTableSplitter} split: one forward pass over the parent's primary index
 * that writes each child's index components and, along the way, everything else the child derives from its keys.
 *
 * <p>Data.db, CompressionInfo.db, Statistics.db, Digest.crc32 and TOC.txt are format-independent -- a compressed
 * chunk run and its offsets table look the same whichever index addresses them -- so the splitter keeps them. What
 * differs is only how a partition's key and Data.db position are read, and what has to be written so that a child
 * can find them again:
 *
 * <table>
 *   <tr><th></th><th>BIG</th><th>BTI</th></tr>
 *   <tr><td>reads</td><td>Index.db, one record per partition</td>
 *       <td>Partitions.db for the position; Rows.db or Data.db for the key</td></tr>
 *   <tr><td>writes</td><td>Index.db (rebuilt), Summary.db</td>
 *       <td>Partitions.db (rebuilt), Rows.db (entries selected and re-placed, one vint patched per entry, page
 *           padding inserted where placement needs it -- not a byte range of the parent's)</td></tr>
 * </table>
 *
 * <p>The parts that are neither -- the child's bloom filter, its HyperLogLog cardinality, its exact
 * {@code estimatedPartitionSize} histogram and its first/last key -- are all functions of the same
 * {@code (key, position)} stream, and live in {@link Accumulator} so that the two formats cannot drift on them.
 *
 * <p>One instance per split; {@link #writeChild} once per child, in order. Both implementations hold a forward-only
 * cursor over the parent's index and rely on the children partitioning the record sequence, which is what
 * {@code ZeroCopySSTableSplitter.Runs} guarantees.
 */
abstract class ZeroCopySplitIndex implements Closeable
{
    private static final Logger logger = LoggerFactory.getLogger(ZeroCopySplitIndex.class);

    /** What the format-independent part of a child needs back from its index pass. */
    static final class ChildIndex
    {
        /** The index components actually written, to add to the child's component set and its TOC. */
        final Set<Component> components;
        final DecoratedKey first;
        final DecoratedKey last;
        /** Exact per-child {@code estimatedPartitionSize}. */
        final EstimatedHistogram partitionSizes;
        /** Exact per-child key cardinality, for CompactionMetadata. */
        final ICardinality cardinality;

        ChildIndex(Set<Component> components, DecoratedKey first, DecoratedKey last,
                   EstimatedHistogram partitionSizes, ICardinality cardinality)
        {
            this.components = components;
            this.first = first;
            this.last = last;
            this.partitionSizes = partitionSizes;
            this.cardinality = cardinality;
        }
    }

    static ZeroCopySplitIndex create(SSTableReader parent) throws IOException
    {
        if (BigFormat.is(parent.descriptor.getFormat()))
            return new Big(parent);
        if (BtiFormat.is(parent.descriptor.getFormat()))
            return new Bti(parent);
        throw new UnsupportedOperationException("no zero-copy split index writer for format " +
                                                parent.descriptor.getFormat().name());
    }

    /**
     * Consume the parent's index records {@code [from, to)} and write the child's index components.
     *
     * @param range     the child's chunk range; {@code range.shift} is subtracted from every Data.db position
     *                  written, and {@code range.hi} closes the last partition's size
     * @param stopCheck optional; run every 1024 records and expected to raise
     *                  {@code CompactionInterruptedException} for either a framework stop or the caller's own
     *                  cancellation ({@code ZeroCopySSTableSplitter.checkInterrupted} composes the two). Rebuilding
     *                  an index moves no Data.db bytes so it is not throttled, but it is one of the two places a
     *                  split spends real time without noticing an interruption on its own.
     */
    abstract ChildIndex writeChild(Descriptor child, ZeroCopySSTableSplitter.ChunkRange range, int from, int to,
                                   Runnable stopCheck)
    throws IOException;

    /**
     * Everything a child derives from its {@code (key, position)} stream that is not the index itself.
     *
     * <p>{@code estimatedPartitionSize} is exact and derived one record late: a partition's size is identically the
     * next partition's position minus its own, and the last one's is closed by {@code range.hi}. The bloom filter is
     * omitted entirely at {@code bloomFilterFpChance == 1.0}, where {@code FilterFactory} hands back an
     * {@code AlwaysPresentFilter} whose {@code serialize()} is a no-op -- writing it would leave a zero-length
     * Filter.db, which {@code requireNonEmpty} rejects, and a missing one already means "always present" to the
     * read path.
     */
    static final class Accumulator implements Closeable
    {
        private final SSTableReader parent;
        private final long shift;
        private final long hi;
        private final long lo;

        final EstimatedHistogram partitionSizes = new EstimatedHistogram(PARTITION_SIZE_HISTOGRAM_BUCKETS);
        final ICardinality cardinality = new HyperLogLogPlus(HLL_P, HLL_SP);
        final IFilter filter;

        private DecoratedKey first;
        private DecoratedKey last;
        private long previousPosition = -1;
        private int expectedIndex;

        Accumulator(SSTableReader parent, ZeroCopySSTableSplitter.ChunkRange range, int from, int partitionCount)
        {
            this.parent = parent;
            this.shift = range.shift;
            this.hi = range.hi;
            this.lo = range.lo;
            this.expectedIndex = from;
            double fpChance = parent.metadata().params.bloomFilterFpChance;
            this.filter = fpChance < 1.0 ? FilterFactory.getFilter(partitionCount, fpChance) : null;
        }

        /**
         * @return the child-space Data.db position of this partition, i.e. what the child's index has to store
         */
        long add(int index, ByteBuffer key, long position)
        {
            // The selection pass and this one have to land on the same records. Checking the run's first offset
            // against what selection recorded, and strict monotonicity from there on, catches a desynchronised walk
            // without keeping an offset per partition -- and rules out a non-increasing parent index, which a
            // per-record equality check would not.
            if (index != expectedIndex)
                throw new IllegalStateException("index walk desynchronised: expected record " + expectedIndex +
                                                ", got " + index);
            ++expectedIndex;

            if (previousPosition < 0)
            {
                if (position != lo)
                    throw new IllegalStateException("index walk desynchronised at record " + index +
                                                    ": run starts at " + position + ", selection said " + lo);
            }
            else
            {
                if (position <= previousPosition)
                    throw new IllegalStateException("parent index positions are not strictly increasing at record " +
                                                    index + ": " + previousPosition + " -> " + position);
                partitionSizes.add(position - previousPosition);
            }
            previousPosition = position;

            DecoratedKey dk = parent.getPartitioner().decorateKey(key);
            if (first == null)
                first = dk;
            last = dk;
            if (filter != null)
                filter.add(dk);
            // MetadataCollector.addKey hashes the raw key bytes, position/remaining passed explicitly
            cardinality.offerHashed(MurmurHash.hash2_64(key, key.position(), key.remaining(), 0));

            return position - shift;
        }

        /** Closes the last partition's size against the run's exclusive end, and makes first/last retainable. */
        void finish()
        {
            if (hi <= previousPosition)
                throw new IllegalStateException("run ends at " + hi + " but its last record is at " +
                                                previousPosition);
            partitionSizes.add(hi - previousPosition);
            first = first.retainable();
            last = last.retainable();
        }

        DecoratedKey first()
        {
            return first;
        }

        /** The key of the record most recently passed to {@link #add}, until {@link #finish} freezes it. */
        DecoratedKey last()
        {
            return last;
        }

        /** Writes Filter.db if there is a filter to write, and reports whether the component now exists. */
        boolean writeFilterIfAny(Descriptor child) throws IOException
        {
            if (filter == null)
                return false;
            writeFilter(child, filter);
            requireNonEmpty(child, SSTableFormat.Components.FILTER);
            return true;
        }

        @Override
        public void close()
        {
            if (filter != null)
                filter.close();
        }
    }

    // ------------------------------------------------------------------------------------------------
    // BIG: one Index.db record per partition, with exactly one rewritten position field
    // ------------------------------------------------------------------------------------------------

    /**
     * The BIG format's index side, unchanged in behaviour from when it was inline in
     * {@code ZeroCopySSTableSplitter.buildChild}: the child's Index.db is the parent's records with the position
     * rebased and the promoted row index copied verbatim.
     *
     * <p>The promoted index can be copied because its {@code IndexInfo} offsets are already relative to the
     * partition's own start, so a partition that moves within Data.db does not move anything inside its own row
     * index. That is the same property that lets the BTI path copy a Rows.db trie verbatim.
     */
    private static final class Big extends ZeroCopySplitIndex
    {
        private final SSTableReader parent;
        private final RandomAccessReader index;
        private int nextRecord;

        Big(SSTableReader parent)
        {
            this.parent = parent;
            // A buffered reader rather than an mmap, so no record can straddle a mapping boundary.
            this.index = RandomAccessReader.open(parent.descriptor.fileFor(BigFormat.Components.PRIMARY_INDEX));
        }

        @Override
        ChildIndex writeChild(Descriptor child, ZeroCopySSTableSplitter.ChunkRange range, int from, int to,
                              Runnable stopCheck)
        throws IOException
        {
            int partitionCount = to - from;
            Set<Component> components = Sets.newHashSet(BigFormat.Components.PRIMARY_INDEX,
                                                        BigFormat.Components.SUMMARY);
            TableMetadata metadata = parent.metadata();

            try (Accumulator acc = new Accumulator(parent, range, from, partitionCount))
            {
                try (SequentialWriter out = new SequentialWriter(child.fileFor(BigFormat.Components.PRIMARY_INDEX),
                                                                 writerOption());
                     IndexSummaryBuilder summary = new IndexSummaryBuilder(partitionCount,
                                                                           metadata.params.minIndexInterval,
                                                                           Downsampling.BASE_SAMPLING_LEVEL))
                {
                    for (int r = from; r < to; r++)
                    {
                        if (stopCheck != null && ((r - from) & 0x3FF) == 0)
                            stopCheck.run();
                        if (r != nextRecord)
                            throw new IllegalStateException("Index.db cursor at " + nextRecord + ", child wants " + r);
                        ByteBuffer key = ByteBufferUtil.readWithShortLength(index);
                        long position = RowIndexEntry.Serializer.readPosition(index);
                        int promotedSize = index.readUnsignedVInt32();
                        byte[] promoted = null;
                        if (promotedSize > 0)
                        {
                            promoted = new byte[promotedSize];
                            index.readFully(promoted);
                        }
                        ++nextRecord;

                        long childPosition = acc.add(r, key, position);

                        long childIndexStart = out.position();
                        ByteBufferUtil.writeWithShortLength(key, out);
                        // The ONLY rewritten field. Canonical minimal vint, never padded -- so the child's records
                        // are shorter than the parent's and its index offsets are NOT the parent's minus a constant.
                        out.writeUnsignedVInt(childPosition);
                        out.writeUnsignedVInt32(promotedSize);
                        if (promoted != null)
                            out.write(promoted, 0, promotedSize);

                        summary.maybeAddEntry(acc.last(), childIndexStart);
                    }

                    acc.finish();
                    out.finish();

                    try (IndexSummary built = summary.build(parent.getPartitioner()))
                    {
                        writeSummary(child, acc.first(), acc.last(), built);
                    }
                }
                requireNonEmpty(child, BigFormat.Components.SUMMARY);

                if (acc.writeFilterIfAny(child))
                    components.add(SSTableFormat.Components.FILTER);

                return new ChildIndex(components, acc.first(), acc.last(), acc.partitionSizes, acc.cardinality);
            }
        }

        @Override
        public void close()
        {
            index.close();
        }
    }

    // ------------------------------------------------------------------------------------------------
    // BTI: Partitions.db rebuilt, Rows.db a verbatim range with one patched vint per entry
    // ------------------------------------------------------------------------------------------------

    /**
     * The BTI format's index side: Partitions.db rebuilt from the child's keys, Rows.db copied entry by entry. See
     * {@link BtiZeroCopySplit} for why the partition index has to be rebuilt, why the row index tries do not, and
     * what constrains where a copied entry may land.
     *
     * <p>Streamed, never collected: nothing per-partition is retained, for the same reason the splitter refuses to
     * keep a {@code long[]} of positions -- a big enough parent has hundreds of millions of them.
     */
    private static final class Bti extends ZeroCopySplitIndex
    {
        private final SSTableReader parent;
        private final BtiZeroCopySplit.Cursor cursor;

        Bti(SSTableReader parent) throws IOException
        {
            this.parent = parent;
            this.cursor = BtiZeroCopySplit.cursor(parent);
        }

        @Override
        @SuppressWarnings({ "resource", "RedundantSuppression" }) // both writers are closed in the finally below
        ChildIndex writeChild(Descriptor child, ZeroCopySSTableSplitter.ChunkRange range, int from, int to,
                              Runnable stopCheck)
        throws IOException
        {
            int partitionCount = to - from;
            Set<Component> components = Sets.newHashSet(BtiFormat.Components.PARTITION_INDEX,
                                                        BtiFormat.Components.ROW_INDEX);

            BtiZeroCopySplit.RowIndexCopier rows = null;
            BtiZeroCopySplit.PartitionIndexWriter partitions = null;
            boolean rowsFinished = false;
            try (Accumulator acc = new Accumulator(parent, range, from, partitionCount))
            {
                rows = new BtiZeroCopySplit.RowIndexCopier(parent, child, writerOption());
                partitions = new BtiZeroCopySplit.PartitionIndexWriter(child, writerOption());

                for (int r = from; r < to; r++)
                {
                    if (stopCheck != null && ((r - from) & 0x3FF) == 0)
                        stopCheck.run();
                    if (!cursor.advance())
                        throw new IllegalStateException("parent Partitions.db ended at record " + r +
                                                        ", expected at least " + to);
                    if (cursor.index() != r)
                        throw new IllegalStateException("Partitions.db cursor at " + cursor.index() +
                                                        ", child wants " + r);

                    long childPosition = acc.add(r, cursor.key(), cursor.dataPosition());

                    // A partition with a row index gets its entry copied into the child's Rows.db and the payload
                    // points there; one without gets ~pos straight into Data.db -- ~ rather than - so that position
                    // 0 with an index and position 0 without stay distinguishable.
                    long payload = cursor.hasRowIndex() ? rows.copy(cursor, childPosition) : ~childPosition;
                    partitions.addEntry(acc.last(), payload);
                }

                acc.finish();
                rows.finish();
                rowsFinished = true;
                partitions.finish();
                requireNonEmpty(child, BtiFormat.Components.PARTITION_INDEX);

                if (acc.writeFilterIfAny(child))
                    components.add(SSTableFormat.Components.FILTER);

                return new ChildIndex(components, acc.first(), acc.last(), acc.partitionSizes, acc.cardinality);
            }
            finally
            {
                // finish() closes the underlying writers; both wrappers hold something finish() does not release
                // (the trie builder, the parent's Rows.db reader), so both are closed on every path.
                Throwable accumulate = null;
                if (partitions != null)
                    accumulate = Throwables.close(accumulate, partitions);
                if (rows != null)
                    accumulate = Throwables.close(accumulate, rows);
                if (!rowsFinished && rows != null)
                    logger.trace("child {} Rows.db was not finished", child);
                Throwables.maybeFail(accumulate);
            }
        }

        @Override
        public void close()
        {
            cursor.close();
        }
    }
}
