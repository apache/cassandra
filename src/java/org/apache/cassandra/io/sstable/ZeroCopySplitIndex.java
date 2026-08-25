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

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.apache.cassandra.io.sstable.format.big.RowIndexEntry;
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

import static org.apache.cassandra.io.sstable.ZeroCopySSTableSplitter.PARTITION_SIZE_HISTOGRAM_BUCKETS;
import static org.apache.cassandra.io.sstable.ZeroCopySSTableSplitter.requireNonEmpty;
import static org.apache.cassandra.io.sstable.ZeroCopySSTableSplitter.writeFilter;
import static org.apache.cassandra.io.sstable.ZeroCopySSTableSplitter.writeSummary;
import static org.apache.cassandra.io.sstable.ZeroCopySSTableSplitter.writerOption;
import static org.apache.cassandra.io.sstable.metadata.MetadataCollector.CARDINALITY_HLL_P;
import static org.apache.cassandra.io.sstable.metadata.MetadataCollector.CARDINALITY_HLL_SP;

/**
 * The index half of a {@link ZeroCopySSTableSplitter} split: one forward pass over the parent's BIG primary index
 * that writes each child's Index.db and Summary.db and, along the way, everything else derived from its keys.
 *
 * <p>Data.db, CompressionInfo.db, Statistics.db, Digest.crc32 and TOC.txt are format-independent -- a compressed
 * chunk run and its offsets table are independent of Index.db -- so the splitter keeps them. The child's bloom
 * filter, its HyperLogLog cardinality, its exact
 * {@code estimatedPartitionSize} histogram and its first/last key -- are all functions of the same
 * {@code (key, position)} stream and live in {@link Accumulator}.
 *
 * <p>One instance per split; {@link #writeChild} once per child, in order. The writer holds a forward-only cursor
 * over the parent's index and relies on the children partitioning the record sequence, which is what
 * {@code ZeroCopySSTableSplitter.Runs} guarantees.
 */
final class ZeroCopySplitIndex implements Closeable
{
    private static final int PROMOTED_INDEX_COPY_BUFFER_SIZE = 64 * 1024;

    private final SSTableReader parent;
    private final RandomAccessReader index;
    private final byte[] promotedIndexCopyBuffer = new byte[PROMOTED_INDEX_COPY_BUFFER_SIZE];
    private int nextRecord;

    ZeroCopySplitIndex(SSTableReader parent)
    {
        if (!BigFormat.is(parent.descriptor.getFormat()))
            throw new UnsupportedOperationException("no zero-copy split index writer for format " +
                                                    parent.descriptor.getFormat().name());
        this.parent = parent;
        // A buffered reader rather than an mmap, so no record can straddle a mapping boundary.
        this.index = RandomAccessReader.open(parent.descriptor.fileFor(BigFormat.Components.PRIMARY_INDEX));
    }

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
        final ICardinality cardinality = new HyperLogLogPlus(CARDINALITY_HLL_P, CARDINALITY_HLL_SP);
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

    /**
     * Consume the parent's index records {@code [from, to)} and write the child's Index.db and Summary.db. The
     * child's records are the parent's records with the position rebased and the promoted row index copied verbatim.
     *
     * <p>The promoted index can be copied because its {@code IndexInfo} offsets are already relative to the
     * partition's own start, so a partition that moves within Data.db does not move anything inside its own row
     * index.
     *
     * @param range     the child's chunk range; {@code range.shift} is subtracted from every Data.db position
     *                  written, and {@code range.hi} closes the last partition's size
     * @param stopCheck optional; run every 1024 records and expected to raise
     *                  {@code CompactionInterruptedException} for either a framework stop or the caller's own
     *                  cancellation
     */
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
                    if (promotedSize > index.bytesRemaining())
                        throw new IOException("Index.db record " + r + " in " + parent.descriptor + " claims " +
                                              promotedSize + " promoted-index bytes with only " +
                                              index.bytesRemaining() + " bytes remaining");
                    ++nextRecord;

                    long childPosition = acc.add(r, key, position);

                    long childIndexStart = out.position();
                    ByteBufferUtil.writeWithShortLength(key, out);
                    // The ONLY rewritten field. Canonical minimal vint, never padded -- so the child's records
                    // are shorter than the parent's and its index offsets are NOT the parent's minus a constant.
                    out.writeUnsignedVInt(childPosition);
                    out.writeUnsignedVInt32(promotedSize);
                    copyPromotedIndex(promotedSize, out, stopCheck);

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

    /** Copy opaque promoted-index bytes without allocating an array controlled by an on-disk length field. */
    private void copyPromotedIndex(int size, SequentialWriter out, Runnable stopCheck) throws IOException
    {
        int remaining = size;
        while (remaining > 0)
        {
            if (stopCheck != null)
                stopCheck.run();
            int count = Math.min(remaining, promotedIndexCopyBuffer.length);
            index.readFully(promotedIndexCopyBuffer, 0, count);
            out.write(promotedIndexCopyBuffer, 0, count);
            remaining -= count;
        }
    }

    @Override
    public void close()
    {
        index.close();
    }
}
