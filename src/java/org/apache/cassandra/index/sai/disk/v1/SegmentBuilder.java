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
package org.apache.cassandra.index.sai.disk.v1;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.disk.PostingList;
import org.apache.cassandra.index.sai.disk.RAMStringIndexer;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.hnsw.CassandraOnHeapHnsw;
import org.apache.cassandra.index.sai.disk.io.BytesRefUtil;
import org.apache.cassandra.index.sai.disk.v1.kdtree.BKDTreeRamBuffer;
import org.apache.cassandra.index.sai.disk.v1.kdtree.NumericIndexWriter;
import org.apache.cassandra.index.sai.disk.v1.trie.InvertedIndexWriter;
import org.apache.cassandra.index.sai.utils.NamedMemoryLimiter;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.TypeUtil;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.StringHelper;

import static org.apache.cassandra.index.sai.disk.hnsw.CassandraOnHeapHnsw.InvalidVectorBehavior.IGNORE;

/**
 * Creates an on-heap index data structure to be flushed to an SSTable index.
 */
@NotThreadSafe
public abstract class SegmentBuilder
{
    private static final Logger logger = LoggerFactory.getLogger(SegmentBuilder.class);

    // Served as safe net in case memory limit is not triggered or when merger merges small segments..
    public static final long LAST_VALID_SEGMENT_ROW_ID = ((long)Integer.MAX_VALUE / 2) - 1L;
    private static long testLastValidSegmentRowId = -1;

    /** The number of column indexes being built globally. (Starts at one to avoid divide by zero.) */
    public static final AtomicLong ACTIVE_BUILDER_COUNT = new AtomicLong(1);

    /** Minimum flush size, dynamically updated as segment builds are started and completed/aborted. */
    private static volatile long minimumFlushBytes;

    final AbstractType<?> termComparator;

    private final NamedMemoryLimiter limiter;
    long totalBytesAllocated;

    private final long lastValidSegmentRowID;

    private boolean flushed = false;
    private boolean active = true;

    // segment metadata
    private long minSSTableRowId = -1;
    private long maxSSTableRowId = -1;
    private long segmentRowIdOffset = 0;
    int rowCount = 0;
    int maxSegmentRowId = -1;
    // in token order
    private PrimaryKey minKey;
    private PrimaryKey maxKey;
    // in termComparator order
    private ByteBuffer minTerm;
    private ByteBuffer maxTerm;

    public static class KDTreeSegmentBuilder extends SegmentBuilder
    {
        protected final byte[] buffer;
        private final BKDTreeRamBuffer kdTreeRamBuffer;
        private final IndexWriterConfig indexWriterConfig;

        KDTreeSegmentBuilder(AbstractType<?> termComparator, NamedMemoryLimiter limiter, IndexWriterConfig indexWriterConfig)
        {
            super(termComparator, limiter);

            int typeSize = TypeUtil.fixedSizeOf(termComparator);
            this.kdTreeRamBuffer = new BKDTreeRamBuffer(1, typeSize);
            this.buffer = new byte[typeSize];
            this.totalBytesAllocated = this.kdTreeRamBuffer.ramBytesUsed();
            this.indexWriterConfig = indexWriterConfig;
        }

        public boolean isEmpty()
        {
            return kdTreeRamBuffer.numRows() == 0;
        }

        protected long addInternal(ByteBuffer term, int segmentRowId)
        {
            TypeUtil.toComparableBytes(term, termComparator, buffer);
            return kdTreeRamBuffer.addPackedValue(segmentRowId, new BytesRef(buffer));
        }

        @Override
        protected SegmentMetadata.ComponentMetadataMap flushInternal(IndexDescriptor indexDescriptor, IndexContext indexContext) throws IOException
        {
            try (NumericIndexWriter writer = new NumericIndexWriter(indexDescriptor,
                                                                    indexContext,
                                                                    TypeUtil.fixedSizeOf(termComparator),
                                                                    maxSegmentRowId,
                                                                    rowCount,
                                                                    indexWriterConfig,
                                                                    indexContext.isSegmentCompactionEnabled()))
            {
                return writer.writeAll(kdTreeRamBuffer.asPointValues());
            }
        }
    }

    public static class RAMStringSegmentBuilder extends SegmentBuilder
    {
        final RAMStringIndexer ramIndexer;

        final BytesRefBuilder stringBuffer = new BytesRefBuilder();

        RAMStringSegmentBuilder(AbstractType<?> termComparator, NamedMemoryLimiter limiter)
        {
            super(termComparator, limiter);

            ramIndexer = new RAMStringIndexer(termComparator);
            totalBytesAllocated = ramIndexer.estimatedBytesUsed();
        }

        public boolean isEmpty()
        {
            return ramIndexer.isEmpty();
        }

        protected long addInternal(ByteBuffer term, int segmentRowId)
        {
            BytesRefUtil.copyBufferToBytesRef(term, stringBuffer);
            return ramIndexer.add(stringBuffer.get(), segmentRowId);
        }

        @Override
        protected SegmentMetadata.ComponentMetadataMap flushInternal(IndexDescriptor indexDescriptor, IndexContext indexContext) throws IOException
        {
            try (InvertedIndexWriter writer = new InvertedIndexWriter(indexDescriptor,
                                                                      indexContext,
                                                                      indexContext.isSegmentCompactionEnabled()))
            {
                return writer.writeAll(ramIndexer.getTermsWithPostings());
            }
        }
    }

    public static class VectorSegmentBuilder extends SegmentBuilder
    {
        private final CassandraOnHeapHnsw<Integer> graphIndex;

        public VectorSegmentBuilder(AbstractType<?> termComparator, NamedMemoryLimiter limiter, IndexWriterConfig indexWriterConfig)
        {
            super(termComparator, limiter);
            graphIndex = new CassandraOnHeapHnsw<>(termComparator, indexWriterConfig, false);
        }

        @Override
        public boolean isEmpty()
        {
            return graphIndex.isEmpty();
        }

        @Override
        protected long addInternal(ByteBuffer term, int segmentRowId)
        {
            return graphIndex.add(term, segmentRowId, IGNORE);
        }

        @Override
        protected SegmentMetadata.ComponentMetadataMap flushInternal(IndexDescriptor indexDescriptor, IndexContext indexContext) throws IOException
        {
            return graphIndex.writeData(indexDescriptor, indexContext, p -> p);
        }
    }

    private SegmentBuilder(AbstractType<?> termComparator, NamedMemoryLimiter limiter)
    {
        this.termComparator = termComparator;
        this.limiter = limiter;
        this.lastValidSegmentRowID = testLastValidSegmentRowId >= 0 ? testLastValidSegmentRowId : LAST_VALID_SEGMENT_ROW_ID;

        minimumFlushBytes = limiter.limitBytes() / ACTIVE_BUILDER_COUNT.getAndIncrement();
    }

    public SegmentMetadata flush(IndexDescriptor indexDescriptor, IndexContext indexContext) throws IOException
    {
        assert !flushed;
        flushed = true;

        if (getRowCount() == 0)
        {
            logger.warn(indexContext.logMessage("No rows to index during flush of SSTable {}."), indexDescriptor.descriptor);
            return null;
        }

        SegmentMetadata.ComponentMetadataMap indexMetas = flushInternal(indexDescriptor, indexContext);

        return new SegmentMetadata(segmentRowIdOffset, rowCount, minSSTableRowId, maxSSTableRowId, minKey, maxKey, minTerm, maxTerm, indexMetas);
    }

    public long add(ByteBuffer term, PrimaryKey key, long sstableRowId)
    {
        assert !flushed : "Cannot add to flushed segment.";
        assert sstableRowId >= maxSSTableRowId;
        minSSTableRowId = minSSTableRowId < 0 ? sstableRowId : minSSTableRowId;
        maxSSTableRowId = sstableRowId;

        assert maxKey == null || maxKey.compareTo(key) <= 0;
        minKey = minKey == null ? key : minKey;
        maxKey = key;

        minTerm = TypeUtil.min(term, minTerm, termComparator);
        maxTerm = TypeUtil.max(term, maxTerm, termComparator);

        if (rowCount == 0)
        {
            // use first global rowId in the segment as segment rowId offset
            segmentRowIdOffset = sstableRowId;
        }

        rowCount++;

        // segmentRowIdOffset should encode sstableRowId into Integer
        int segmentRowId = Math.toIntExact(sstableRowId - segmentRowIdOffset);

        if (segmentRowId == PostingList.END_OF_STREAM)
            throw new IllegalArgumentException("Illegal segment row id: END_OF_STREAM found");

        maxSegmentRowId = Math.max(maxSegmentRowId, segmentRowId);

        long bytesAllocated = addInternal(term, segmentRowId);
        totalBytesAllocated += bytesAllocated;

        return bytesAllocated;
    }

    long totalBytesAllocated()
    {
        return totalBytesAllocated;
    }

    boolean hasReachedMinimumFlushSize()
    {
        return totalBytesAllocated >= minimumFlushBytes;
    }

    long getMinimumFlushBytes()
    {
        return minimumFlushBytes;
    }

    /**
     * This method does three things:
     *
     * 1.) It decrements active builder count and updates the global minimum flush size to reflect that.
     * 2.) It releases the builder's memory against its limiter.
     * 3.) It defensively marks the builder inactive to make sure nothing bad happens if we try to close it twice.
     *
     * @param indexContext
     *
     * @return the number of bytes currently used by the memory limiter
     */
    long release(IndexContext indexContext)
    {
        if (active)
        {
            minimumFlushBytes = limiter.limitBytes() / ACTIVE_BUILDER_COUNT.decrementAndGet();
            long used = limiter.decrement(totalBytesAllocated);
            active = false;
            return used;
        }

        logger.warn(indexContext.logMessage("Attempted to release storage attached index segment builder memory after builder marked inactive."));
        return limiter.currentBytesUsed();
    }

    public abstract boolean isEmpty();

    protected abstract long addInternal(ByteBuffer term, int segmentRowId);

    protected abstract SegmentMetadata.ComponentMetadataMap flushInternal(IndexDescriptor indexDescriptor, IndexContext indexContext) throws IOException;

    int getRowCount()
    {
        return rowCount;
    }

    /**
     * @return true if next SSTable row ID exceeds max segment row ID
     */
    boolean exceedsSegmentLimit(long ssTableRowId)
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
