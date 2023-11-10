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

import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.v1.IndexWriterConfig;
import org.apache.cassandra.index.sai.disk.v1.bbtree.BlockBalancedTreeRamBuffer;
import org.apache.cassandra.index.sai.disk.v1.bbtree.NumericIndexWriter;
import org.apache.cassandra.index.sai.disk.v1.trie.LiteralIndexWriter;
import org.apache.cassandra.index.sai.disk.v1.vector.OnHeapGraph;
import org.apache.cassandra.index.sai.memory.RAMStringIndexer;
import org.apache.cassandra.index.sai.utils.IndexIdentifier;
import org.apache.cassandra.index.sai.utils.IndexTermType;
import org.apache.cassandra.index.sai.utils.NamedMemoryLimiter;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.utils.FastByteOperations;
import org.apache.lucene.util.BytesRefBuilder;

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

    final IndexTermType indexTermType;
    long totalBytesAllocated;
    int rowCount = 0;
    int maxSegmentRowId = -1;

    public static class BlockBalancedTreeSegmentBuilder extends SegmentBuilder
    {
        private final byte[] scratch;
        private final BlockBalancedTreeRamBuffer trieBuffer;

        public BlockBalancedTreeSegmentBuilder(IndexTermType indexTermType, NamedMemoryLimiter limiter)
        {
            super(indexTermType, limiter);

            scratch = new byte[indexTermType.fixedSizeOf()];
            trieBuffer = new BlockBalancedTreeRamBuffer(indexTermType.fixedSizeOf());
            totalBytesAllocated = this.trieBuffer.memoryUsed();
        }

        @Override
        public boolean isEmpty()
        {
            return trieBuffer.numRows() == 0;
        }

        @Override
        protected long addInternal(ByteBuffer term, int segmentRowId)
        {
            indexTermType.toComparableBytes(term, scratch);
            return trieBuffer.add(segmentRowId, scratch);
        }

        @Override
        protected SegmentMetadata.ComponentMetadataMap flushInternal(IndexDescriptor indexDescriptor, IndexIdentifier indexIdentifier) throws IOException
        {
            NumericIndexWriter writer = new NumericIndexWriter(indexDescriptor,
                                                               indexIdentifier,
                                                               indexTermType.fixedSizeOf(),
                                                               maxSegmentRowId);
            return writer.writeCompleteSegment(trieBuffer.iterator());
        }
    }

    public static class RAMStringSegmentBuilder extends SegmentBuilder
    {
        final RAMStringIndexer ramIndexer;

        final BytesRefBuilder stringBuffer = new BytesRefBuilder();

        public RAMStringSegmentBuilder(IndexTermType indexTermType, NamedMemoryLimiter limiter)
        {
            super(indexTermType, limiter);

            ramIndexer = new RAMStringIndexer();
            totalBytesAllocated = ramIndexer.estimatedBytesUsed();
        }

        @Override
        public boolean isEmpty()
        {
            return ramIndexer.isEmpty();
        }

        @Override
        protected long addInternal(ByteBuffer term, int segmentRowId)
        {
            copyBufferToBytesRef(term, stringBuffer);
            return ramIndexer.add(stringBuffer.get(), segmentRowId);
        }

        @Override
        protected SegmentMetadata.ComponentMetadataMap flushInternal(IndexDescriptor indexDescriptor, IndexIdentifier indexIdentifier) throws IOException
        {
            try (LiteralIndexWriter writer = new LiteralIndexWriter(indexDescriptor, indexIdentifier))
            {
                return writer.writeCompleteSegment(ramIndexer.getTermsWithPostings());
            }
        }

        private void copyBufferToBytesRef(ByteBuffer buffer, BytesRefBuilder stringBuffer)
        {
            int length = buffer.remaining();
            stringBuffer.clear();
            stringBuffer.grow(length);
            FastByteOperations.copy(buffer, buffer.position(), stringBuffer.bytes(), 0, length);
            stringBuffer.setLength(length);
        }
    }

    public static class VectorSegmentBuilder extends SegmentBuilder
    {
        private final OnHeapGraph<Integer> graphIndex;

        public VectorSegmentBuilder(IndexTermType indexTermType, NamedMemoryLimiter limiter, IndexWriterConfig indexWriterConfig)
        {
            super(indexTermType, limiter);
            graphIndex = new OnHeapGraph<>(indexTermType.indexType(), indexWriterConfig, false);
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
        protected SegmentMetadata.ComponentMetadataMap flushInternal(IndexDescriptor indexDescriptor, IndexIdentifier indexIdentifier) throws IOException
        {
            return graphIndex.writeData(indexDescriptor, indexIdentifier, p -> p);
        }
    }

    public static int getActiveBuilderCount()
    {
        return ACTIVE_BUILDER_COUNT.get();
    }

    private SegmentBuilder(IndexTermType indexTermType, NamedMemoryLimiter limiter)
    {
        this.indexTermType = indexTermType;
        this.limiter = limiter;
        lastValidSegmentRowID = testLastValidSegmentRowId >= 0 ? testLastValidSegmentRowId : LAST_VALID_SEGMENT_ROW_ID;

        minimumFlushBytes = limiter.limitBytes() / ACTIVE_BUILDER_COUNT.incrementAndGet();
    }

    public SegmentMetadata flush(IndexDescriptor indexDescriptor, IndexIdentifier indexIdentifier) throws IOException
    {
        assert !flushed : "Cannot flush an already flushed segment";
        flushed = true;

        if (getRowCount() == 0)
        {
            logger.warn(indexIdentifier.logMessage("No rows to index during flush of SSTable {}."), indexDescriptor.sstableDescriptor);
            return null;
        }

        SegmentMetadata.ComponentMetadataMap indexMetas = flushInternal(indexDescriptor, indexIdentifier);

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

        minTerm = indexTermType.min(term, minTerm);
        maxTerm = indexTermType.max(term, maxTerm);

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
     * @param indexIdentifier an {@link IndexIdentifier} used for creating logging messages
     *
     * @return the number of bytes used by the memory limiter after releasing this builder
     */
    public long release(IndexIdentifier indexIdentifier)
    {
        if (active)
        {
            minimumFlushBytes = limiter.limitBytes() / ACTIVE_BUILDER_COUNT.getAndDecrement();
            long used = limiter.decrement(totalBytesAllocated);
            active = false;
            return used;
        }

        logger.warn(indexIdentifier.logMessage("Attempted to release storage-attached index segment builder memory after builder marked inactive."));
        return limiter.currentBytesUsed();
    }

    public abstract boolean isEmpty();

    protected abstract long addInternal(ByteBuffer term, int segmentRowId);

    protected abstract SegmentMetadata.ComponentMetadataMap flushInternal(IndexDescriptor indexDescriptor, IndexIdentifier indexIdentifier) throws IOException;

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
