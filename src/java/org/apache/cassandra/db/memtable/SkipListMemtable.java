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
package org.apache.cassandra.db.memtable;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.BufferDecoratedKey;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.partitions.AbstractUnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.AtomicBTreePartition;
import org.apache.cassandra.db.partitions.BTreePartitionData;
import org.apache.cassandra.db.partitions.BTreePartitionUpdater;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.IncludingExcludingBounds;
import org.apache.cassandra.dht.Murmur3Partitioner.LongToken;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.index.transactions.UpdateTransaction;
import org.apache.cassandra.io.sstable.SSTableReadsListener;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.memory.Cloner;
import org.apache.cassandra.utils.memory.MemtableAllocator;
import org.apache.cassandra.utils.memory.NativeAllocator;

import static org.apache.cassandra.config.CassandraRelevantProperties.MEMTABLE_OVERHEAD_COMPUTE_STEPS;
import static org.apache.cassandra.config.CassandraRelevantProperties.MEMTABLE_OVERHEAD_SIZE;

public class SkipListMemtable extends AbstractAllocatorMemtable
{
    private static final Logger logger = LoggerFactory.getLogger(SkipListMemtable.class);

    public static final Factory FACTORY = SkipListMemtableFactory.INSTANCE;

    protected static final int ROW_OVERHEAD_HEAP_SIZE;
    static
    {
        int userDefinedOverhead = MEMTABLE_OVERHEAD_SIZE.getInt(-1);
        if (userDefinedOverhead > 0)
            ROW_OVERHEAD_HEAP_SIZE = userDefinedOverhead;
        else
            ROW_OVERHEAD_HEAP_SIZE = estimateRowOverhead(MEMTABLE_OVERHEAD_COMPUTE_STEPS.getInt());
    }

    // We index the memtable by PartitionPosition only for the purpose of being able
    // to select key range using Token.KeyBound. However put() ensures that we
    // actually only store DecoratedKey.
    private final ConcurrentNavigableMap<PartitionPosition, AtomicBTreePartition> partitions = new ConcurrentSkipListMap<>();

    private final AtomicLong liveDataSize = new AtomicLong(0);

    protected SkipListMemtable(AtomicReference<CommitLogPosition> commitLogLowerBound, TableMetadataRef metadataRef, Owner owner)
    {
        super(commitLogLowerBound, metadataRef, owner);
    }

    @Override
    public boolean isClean()
    {
        return partitions.isEmpty();
    }

    /**
     * Should only be called by ColumnFamilyStore.apply via Keyspace.apply, which supplies the appropriate
     * OpOrdering.
     *
     * commitLogSegmentPosition should only be null if this is a secondary index, in which case it is *expected* to be null
     */
    @Override
    public long put(PartitionUpdate update, UpdateTransaction indexer, OpOrder.Group opGroup)
    {
        Cloner cloner = allocator.cloner(opGroup);
        AtomicBTreePartition previous = partitions.get(update.partitionKey());

        long initialSize = 0;
        if (previous == null)
        {
            final DecoratedKey cloneKey = cloner.clone(update.partitionKey());
            AtomicBTreePartition empty = new AtomicBTreePartition(metadata, cloneKey, allocator);
            // We'll add the columns later. This avoids wasting works if we get beaten in the putIfAbsent
            previous = partitions.putIfAbsent(cloneKey, empty);
            if (previous == null)
            {
                previous = empty;
                // allocate the row overhead after the fact; this saves over allocating and having to free after, but
                // means we can overshoot our declared limit.
                int overhead = (int) (cloneKey.getToken().getHeapSize() + ROW_OVERHEAD_HEAP_SIZE);
                allocator.onHeap().allocate(overhead, opGroup);
                initialSize = 8;
            }
        }

        BTreePartitionUpdater updater = previous.addAll(update, cloner, opGroup, indexer);
        updateMin(minTimestamp, update.stats().minTimestamp);
        updateMin(minLocalDeletionTime, update.stats().minLocalDeletionTime);
        liveDataSize.addAndGet(initialSize + updater.dataSize);
        columnsCollector.update(update.columns());
        statsCollector.update(update.stats());
        currentOperations.addAndGet(update.operationCount());
        return updater.colUpdateTimeDelta;
    }

    @Override
    public long partitionCount()
    {
        return partitions.size();
    }

    @Override
    public MemtableUnfilteredPartitionIterator partitionIterator(final ColumnFilter columnFilter,
                                                                 final DataRange dataRange,
                                                                 SSTableReadsListener readsListener)
    {
        AbstractBounds<PartitionPosition> keyRange = dataRange.keyRange();

        PartitionPosition left = keyRange.left;
        PartitionPosition right = keyRange.right;

        boolean isBound = keyRange instanceof Bounds;
        boolean includeLeft = isBound || keyRange instanceof IncludingExcludingBounds;
        boolean includeRight = isBound || keyRange instanceof Range;
        Map<PartitionPosition, AtomicBTreePartition> subMap = getPartitionsSubMap(left,
                                                                                  includeLeft,
                                                                                  right,
                                                                                  includeRight);

        return new MemtableUnfilteredPartitionIterator(metadata.get(), subMap, columnFilter, dataRange);
        // readsListener is ignored as it only accepts sstable signals
    }

    private Map<PartitionPosition, AtomicBTreePartition> getPartitionsSubMap(PartitionPosition left,
                                                                             boolean includeLeft,
                                                                             PartitionPosition right,
                                                                             boolean includeRight)
    {
        if (left != null && left.isMinimum())
            left = null;
        if (right != null && right.isMinimum())
            right = null;

        try
        {
            if (left == null)
                return right == null ? partitions : partitions.headMap(right, includeRight);
            else
                return right == null
                       ? partitions.tailMap(left, includeLeft)
                       : partitions.subMap(left, includeLeft, right, includeRight);
        }
        catch (IllegalArgumentException e)
        {
            logger.error("Invalid range requested {} - {}", left, right);
            throw e;
        }
    }

    Partition getPartition(DecoratedKey key)
    {
        return partitions.get(key);
    }

    @Override
    public UnfilteredRowIterator rowIterator(DecoratedKey key, Slices slices, ColumnFilter selectedColumns, boolean reversed, SSTableReadsListener listener)
    {
        Partition p = getPartition(key);
        if (p == null)
            return null;
        else
            return p.unfilteredIterator(selectedColumns, slices, reversed);
    }

    @Override
    public UnfilteredRowIterator rowIterator(DecoratedKey key)
    {
        Partition p = getPartition(key);
        return p != null ? p.unfilteredIterator() : null;
    }

    private static int estimateRowOverhead(final int count)
    {
        // calculate row overhead
        try (final OpOrder.Group group = new OpOrder().start())
        {
            int rowOverhead;
            MemtableAllocator allocator = MEMORY_POOL.newAllocator("");
            Cloner cloner = allocator.cloner(group);
            ConcurrentNavigableMap<PartitionPosition, Object> partitions = new ConcurrentSkipListMap<>();
            final Object val = new Object();
            final int testBufferSize = 8;
            for (int i = 0 ; i < count ; i++)
                partitions.put(cloner.clone(new BufferDecoratedKey(new LongToken(i), ByteBuffer.allocate(testBufferSize))), val);
            double avgSize = ObjectSizes.measureDeepOmitShared(partitions) / (double) count;
            rowOverhead = (int) ((avgSize - Math.floor(avgSize)) < 0.05 ? Math.floor(avgSize) : Math.ceil(avgSize));
            rowOverhead -= new LongToken(0).getHeapSize();
            rowOverhead += AtomicBTreePartition.EMPTY_SIZE;
            rowOverhead += BTreePartitionData.UNSHARED_HEAP_SIZE;
            if (!(allocator instanceof NativeAllocator))
                rowOverhead -= testBufferSize;  // measureDeepOmitShared includes the given number of bytes even for
                                                // off-heap buffers, but not for direct memory.
            // Decorated key overhead with byte buffer (if needed) is included
            allocator.setDiscarding();
            allocator.setDiscarded();
            return rowOverhead;
        }
    }

    @Override
    public FlushablePartitionSet<?> getFlushSet(PartitionPosition from, PartitionPosition to)
    {
        Map<PartitionPosition, AtomicBTreePartition> toFlush = getPartitionsSubMap(from, true, to, false);
        long keysSize = 0;
        long keyCount = 0;

        boolean trackContention = logger.isTraceEnabled();
        if (trackContention)
        {
            int heavilyContendedRowCount = 0;

            for (AtomicBTreePartition partition : toFlush.values())
            {
                keysSize += partition.partitionKey().getKey().remaining();
                ++keyCount;
                if (partition.useLock())
                    heavilyContendedRowCount++;
            }

            if (heavilyContendedRowCount > 0)
                logger.trace("High update contention in {}/{} partitions of {} ", heavilyContendedRowCount, toFlush.size(), SkipListMemtable.this);
        }
        else
        {
            for (PartitionPosition key : toFlush.keySet())
            {
                //  make sure we don't write non-sensical keys
                assert key instanceof DecoratedKey;
                keysSize += ((DecoratedKey) key).getKey().remaining();
                ++keyCount;
            }
        }
        final long partitionKeysSize = keysSize;
        final long partitionCount = keyCount;

        return new AbstractFlushablePartitionSet<AtomicBTreePartition>()
        {
            @Override
            public Memtable memtable()
            {
                return SkipListMemtable.this;
            }

            @Override
            public PartitionPosition from()
            {
                return from;
            }

            @Override
            public PartitionPosition to()
            {
                return to;
            }

            @Override
            public long partitionCount()
            {
                return partitionCount;
            }

            @Override
            public Iterator<AtomicBTreePartition> iterator()
            {
                return toFlush.values().iterator();
            }

            @Override
            public long partitionKeysSize()
            {
                return partitionKeysSize;
            }
        };
    }


    private static class MemtableUnfilteredPartitionIterator extends AbstractUnfilteredPartitionIterator implements UnfilteredPartitionIterator
    {
        private final TableMetadata metadata;
        private final Iterator<Map.Entry<PartitionPosition, AtomicBTreePartition>> iter;
        private final ColumnFilter columnFilter;
        private final DataRange dataRange;

        MemtableUnfilteredPartitionIterator(TableMetadata metadata, Map<PartitionPosition, AtomicBTreePartition> map, ColumnFilter columnFilter, DataRange dataRange)
        {
            this.metadata = metadata;
            this.iter = map.entrySet().iterator();
            this.columnFilter = columnFilter;
            this.dataRange = dataRange;
        }

        @Override
        public TableMetadata metadata()
        {
            return metadata;
        }

        @Override
        public boolean hasNext()
        {
            return iter.hasNext();
        }

        @Override
        public UnfilteredRowIterator next()
        {
            Map.Entry<PartitionPosition, AtomicBTreePartition> entry = iter.next();
            // Actual stored key should be true DecoratedKey
            assert entry.getKey() instanceof DecoratedKey;
            DecoratedKey key = (DecoratedKey)entry.getKey();
            ClusteringIndexFilter filter = dataRange.clusteringIndexFilter(key);

            return filter.getUnfilteredRowIterator(columnFilter, entry.getValue());
        }
    }

    @Override
    public long getLiveDataSize()
    {
        return liveDataSize.get();
    }

    /**
     * For testing only. Give this memtable too big a size to make it always fail flushing.
     */
    @VisibleForTesting
    public void makeUnflushable()
    {
        liveDataSize.addAndGet(1024L * 1024 * 1024 * 1024 * 1024);
    }
}
