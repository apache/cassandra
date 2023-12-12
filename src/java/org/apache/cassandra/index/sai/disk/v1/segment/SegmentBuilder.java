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
package org.apache.cassandra.index.sai.disk.v1.segment;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.v1.bbtree.NumericIndexWriter;
import org.apache.cassandra.index.sai.disk.v1.trie.LiteralIndexWriter;
import org.apache.cassandra.index.sai.disk.v1.vector.OnHeapGraph;
import org.apache.cassandra.index.sai.utils.NamedMemoryLimiter;
import org.apache.cassandra.index.sai.utils.PrimaryKey;

/**
 * Creates an on-heap index data structure to be flushed to an SSTable index.
 */
@NotThreadSafe
public abstract class SegmentBuilder
{
    private static final Logger logger = LoggerFactory.getLogger(SegmentBuilder.class);

    // Served as safe net in case memory limit is not triggered or when merger merges small segments..
    public static final long LAST_VALID_SEGMENT_ROW_ID = (Integer.MAX_VALUE / 2) - 1L;
    private static long testLastValidSegmentRowId = -1;

    /** The number of column indexes being built globally. (Starts at one to avoid divide by zero.) */
    private static final AtomicInteger ACTIVE_BUILDER_COUNT = new AtomicInteger(0);

    /** Minimum flush size, dynamically updated as segment builds are started and completed/aborted. */
    private static volatile long minimumFlushBytes;
    private final NamedMemoryLimiter limiter;
    private final long lastValidSegmentRowID;
    private boolean flushed = false;
    private boolean active = true;
    // segment metadata
    private long minSSTableRowId = -1;
    private long maxSSTableRowId = -1;
    private long segmentRowIdOffset = 0;

    // in token order
    private PrimaryKey minKey;
    private PrimaryKey maxKey;
    // in termComparator order
    private ByteBuffer minTerm;
    private ByteBuffer maxTerm;

    final StorageAttachedIndex index;
    long totalBytesAllocated;
    int rowCount = 0;
    int maxSegmentRowId = -1;

    public static class TrieSegmentBuilder extends SegmentBuilder
    {
        protected final SegmentTrieBuffer segmentTrieBuffer;

        public TrieSegmentBuilder(StorageAttachedIndex index, NamedMemoryLimiter limiter)
        {
            super(index, limiter);

            segmentTrieBuffer = new SegmentTrieBuffer();
            totalBytesAllocated = segmentTrieBuffer.memoryUsed();
        }

        @Override
        protected long addInternal(ByteBuffer term, int segmentRowId)
        {
            return segmentTrieBuffer.add(v -> index.termType().asComparableBytes(term, v), term.limit(), segmentRowId);
        }

        @Override
        protected SegmentMetadata.ComponentMetadataMap flushInternal(IndexDescriptor indexDescriptor) throws IOException
        {
            SegmentWriter writer = index.termType().isLiteral() ? new LiteralIndexWriter(indexDescriptor, index.identifier())
                                                                : new NumericIndexWriter(indexDescriptor, index.identifier(), index.termType().fixedSizeOf());

            return writer.writeCompleteSegment(segmentTrieBuffer.iterator());
        }

        @Override
        public boolean isEmpty()
        {
            return segmentTrieBuffer.numRows() == 0;
        }
    }

    public static class VectorSegmentBuilder extends SegmentBuilder
    {
        private final OnHeapGraph<Integer> graphIndex;

        public VectorSegmentBuilder(StorageAttachedIndex index, NamedMemoryLimiter limiter)
        {
            super(index, limiter);
            graphIndex = new OnHeapGraph<>(index.termType().indexType(), index.indexWriterConfig(), false);
        }

        @Override
        public boolean isEmpty()
        {
            return graphIndex.isEmpty();
        }

        @Override
        protected long addInternal(ByteBuffer term, int segmentRowId)
        {
            return graphIndex.add(term, segmentRowId, OnHeapGraph.InvalidVectorBehavior.IGNORE);
        }

        @Override
        protected SegmentMetadata.ComponentMetadataMap flushInternal(IndexDescriptor indexDescriptor) throws IOException
        {
            return graphIndex.writeData(indexDescriptor, index.identifier(), p -> p);
        }
    }

    public static int getActiveBuilderCount()
    {
        return ACTIVE_BUILDER_COUNT.get();
    }

    private SegmentBuilder(StorageAttachedIndex index, NamedMemoryLimiter limiter)
    {
        this.index = index;
        this.limiter = limiter;
        lastValidSegmentRowID = testLastValidSegmentRowId >= 0 ? testLastValidSegmentRowId : LAST_VALID_SEGMENT_ROW_ID;

        minimumFlushBytes = limiter.limitBytes() / ACTIVE_BUILDER_COUNT.incrementAndGet();
    }

    public SegmentMetadata flush(IndexDescriptor indexDescriptor) throws IOException
    {
        assert !flushed : "Cannot flush an already flushed segment";
        flushed = true;

        if (getRowCount() == 0)
        {
            logger.warn(index.identifier().logMessage("No rows to index during flush of SSTable {}."), indexDescriptor.sstableDescriptor);
            return null;
        }

        SegmentMetadata.ComponentMetadataMap indexMetas = flushInternal(indexDescriptor);

        return new SegmentMetadata(segmentRowIdOffset, rowCount, minSSTableRowId, maxSSTableRowId, minKey, maxKey, minTerm, maxTerm, indexMetas);
    }

    public long add(ByteBuffer term, PrimaryKey key, long sstableRowId)
    {
        assert !flushed : "Cannot add to a flushed segment.";
        assert sstableRowId >= maxSSTableRowId;
        minSSTableRowId = minSSTableRowId < 0 ? sstableRowId : minSSTableRowId;
        maxSSTableRowId = sstableRowId;

        assert maxKey == null || maxKey.compareTo(key) <= 0;
        if (minKey == null)
            minKey = key;
        maxKey = key;

        minTerm = index.termType().min(term, minTerm);
        maxTerm = index.termType().max(term, maxTerm);

        if (rowCount == 0)
        {
            // use first global rowId in the segment as segment rowId offset
            segmentRowIdOffset = sstableRowId;
        }

        rowCount++;

        // segmentRowIdOffset should encode sstableRowId into Integer
        int segmentRowId = castToSegmentRowId(sstableRowId, segmentRowIdOffset);
        maxSegmentRowId = Math.max(maxSegmentRowId, segmentRowId);

        long bytesAllocated = addInternal(term, segmentRowId);
        totalBytesAllocated += bytesAllocated;

        return bytesAllocated;
    }

    public static int castToSegmentRowId(long sstableRowId, long segmentRowIdOffset)
    {
        return Math.toIntExact(sstableRowId - segmentRowIdOffset);
    }

    public long totalBytesAllocated()
    {
        return totalBytesAllocated;
    }

    public boolean hasReachedMinimumFlushSize()
    {
        return totalBytesAllocated >= minimumFlushBytes;
    }

    public long getMinimumFlushBytes()
    {
        return minimumFlushBytes;
    }

    /**
     * This method does three things:
     * <p>
     * 1. It decrements active builder count and updates the global minimum flush size to reflect that.
     * 2. It releases the builder's memory against its limiter.
     * 3. It defensively marks the builder inactive to make sure nothing bad happens if we try to close it twice.
     *
     * @return the number of bytes used by the memory limiter after releasing this builder
     */
    public long release()
    {
        if (active)
        {
            minimumFlushBytes = limiter.limitBytes() / ACTIVE_BUILDER_COUNT.getAndDecrement();
            long used = limiter.decrement(totalBytesAllocated);
            active = false;
            return used;
        }

        logger.warn(index.identifier().logMessage("Attempted to release storage-attached index segment builder memory after builder marked inactive."));
        return limiter.currentBytesUsed();
    }

    public abstract boolean isEmpty();

    protected abstract long addInternal(ByteBuffer term, int segmentRowId);

    protected abstract SegmentMetadata.ComponentMetadataMap flushInternal(IndexDescriptor indexDescriptor) throws IOException;

    public int getRowCount()
    {
        return rowCount;
    }

    /**
     * @return true if next SSTable row ID exceeds max segment row ID
     */
    public boolean exceedsSegmentLimit(long ssTableRowId)
    {
        if (getRowCount() == 0)
            return false;

        // To handle the case where there are many non-indexable rows. eg. rowId-1 and rowId-3B are indexable,
        // the rest are non-indexable. We should flush them as 2 separate segments, because rowId-3B is going
        // to cause error in on-disk index structure with 2B limitation.
        return ssTableRowId - segmentRowIdOffset > lastValidSegmentRowID;
    }

    @VisibleForTesting
    public static void updateLastValidSegmentRowId(long lastValidSegmentRowID)
    {
        testLastValidSegmentRowId = lastValidSegmentRowID;
    }
}
