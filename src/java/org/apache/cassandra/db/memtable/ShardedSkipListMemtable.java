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

import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.partitions.AbstractUnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.AtomicBTreePartition;
import org.apache.cassandra.db.partitions.BTreePartitionUpdater;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.IncludingExcludingBounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.index.transactions.UpdateTransaction;
import org.apache.cassandra.io.sstable.SSTableReadsListener;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.memory.Cloner;
import org.apache.cassandra.utils.memory.MemtableAllocator;
import org.github.jamm.Unmetered;

/**
 * A proof-of-concept sharded memtable implementation. This implementation splits the partition skip-list into several
 * independent skip-lists each covering a roughly equal part of the token space served by this node. This reduces
 * congestion of the skip-list from concurrent writes and can lead to improved write throughput.
 *
 * The implementation takes two parameters:
 * - shards: the number of shards to split into.
 * - serialize_writes: if false, each shard may serve multiple writes in parallel; if true, writes to each shard are
 *   synchronized.
 *
 * Also see Memtable_API.md.
 */
public class ShardedSkipListMemtable extends AbstractShardedMemtable
{
    private static final Logger logger = LoggerFactory.getLogger(ShardedSkipListMemtable.class);

    public static final String LOCKING_OPTION = "serialize_writes";

    /**
     * Sharded memtable sections. Each is responsible for a contiguous range of the token space (between boundaries[i]
     * and boundaries[i+1]) and is written to by one thread at a time, while reads are carried out concurrently
     * (including with any write).
     */
    final MemtableShard[] shards;

    ShardedSkipListMemtable(AtomicReference<CommitLogPosition> commitLogLowerBound,
                            TableMetadataRef metadataRef,
                            Owner owner,
                            Integer shardCountOption)
    {
        super(commitLogLowerBound, metadataRef, owner, shardCountOption);
        this.shards = generatePartitionShards(boundaries.shardCount(), allocator, metadataRef);
    }

    private static MemtableShard[] generatePartitionShards(int splits,
                                                           MemtableAllocator allocator,
                                                           TableMetadataRef metadata)
    {
        MemtableShard[] partitionMapContainer = new MemtableShard[splits];
        for (int i = 0; i < splits; i++)
            partitionMapContainer[i] = new MemtableShard(metadata, allocator);

        return partitionMapContainer;
    }

    public boolean isClean()
    {
        for (MemtableShard shard : shards)
            if (!shard.isClean())
                return false;
        return true;
    }

    /**
     * Should only be called by ColumnFamilyStore.apply via Keyspace.apply, which supplies the appropriate
     * OpOrdering.
     *
     * commitLogSegmentPosition should only be null if this is a secondary index, in which case it is *expected* to be null
     */
    public long put(PartitionUpdate update, UpdateTransaction indexer, OpOrder.Group opGroup)
    {
        DecoratedKey key = update.partitionKey();
        MemtableShard shard = shards[boundaries.getShardForKey(key)];
        return shard.put(key, update, indexer, opGroup);
    }

    /**
     * Technically we should scatter gather on all the core threads because the size in following calls are not
     * using volatile variables, but for metrics purpose this should be good enough.
     */
    @Override
    public long getLiveDataSize()
    {
        long total = 0L;
        for (MemtableShard shard : shards)
            total += shard.liveDataSize();
        return total;
    }

    @Override
    public long operationCount()
    {
        long total = 0L;
        for (MemtableShard shard : shards)
            total += shard.currentOperations();
        return total;
    }

    @Override
    public long partitionCount()
    {
        int total = 0;
        for (MemtableShard shard : shards)
            total += shard.size();
        return total;
    }

    /**
     * Returns the minTS if one available, otherwise NO_MIN_TIMESTAMP.
     *
     * EncodingStats uses a synthetic epoch TS at 2015. We don't want to leak that (CASSANDRA-18118) so we return NO_MIN_TIMESTAMP instead.
     *
     * @return The minTS or NO_MIN_TIMESTAMP if none available
     */
    @Override
    public long getMinTimestamp()
    {
        long min = Long.MAX_VALUE;
        for (MemtableShard shard : shards)
            min =  Long.min(min, shard.minTimestamp());
        return min != EncodingStats.NO_STATS.minTimestamp ? min : NO_MIN_TIMESTAMP;
    }

    @Override
    public long getMinLocalDeletionTime()
    {
        long min = Long.MAX_VALUE;
        for (MemtableShard shard : shards)
            min = Long.min(min, shard.minLocalDeletionTime());
        return min;
    }

    @Override
    RegularAndStaticColumns columns()
    {
        for (MemtableShard shard : shards)
            columnsCollector.update(shard.columnsCollector);
        return columnsCollector.get();
    }

    @Override
    EncodingStats encodingStats()
    {
        for (MemtableShard shard : shards)
            statsCollector.update(shard.statsCollector.get());
        return statsCollector.get();
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
        boolean includeStart = isBound || keyRange instanceof IncludingExcludingBounds;
        boolean includeStop = isBound || keyRange instanceof Range;

        Iterator<AtomicBTreePartition> iterator = getPartitionIterator(left, includeStart, right, includeStop);

        return new MemtableUnfilteredPartitionIterator(metadata(), iterator, columnFilter, dataRange);
        // readsListener is ignored as it only accepts sstable signals
    }

    private Iterator<AtomicBTreePartition> getPartitionIterator(PartitionPosition left, boolean includeStart, PartitionPosition right, boolean includeStop)
    {
        int leftShard = left != null && !left.isMinimum() ? boundaries.getShardForKey(left) : 0;
        int rightShard = right != null && !right.isMinimum() ? boundaries.getShardForKey(right) : boundaries.shardCount() - 1;
        Iterator<AtomicBTreePartition> iterator;
        if (leftShard == rightShard)
            iterator = shards[leftShard].getPartitionsSubMap(left, includeStart, right, includeStop).values().iterator();
        else
        {
            Iterator<AtomicBTreePartition>[] iters = new Iterator[rightShard - leftShard + 1];
            int i = leftShard;
            iters[0] = shards[leftShard].getPartitionsSubMap(left, includeStart, null, true).values().iterator();
            for (++i; i < rightShard; ++i)
                iters[i - leftShard] = shards[i].partitions.values().iterator();
            iters[i - leftShard] = shards[i].getPartitionsSubMap(null, true, right, includeStop).values().iterator();
            iterator = Iterators.concat(iters);
        }
        return iterator;
    }

    private Partition getPartition(DecoratedKey key)
    {
        int shardIndex = boundaries.getShardForKey(key);
        return shards[shardIndex].partitions.get(key);
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

    public FlushablePartitionSet<AtomicBTreePartition> getFlushSet(PartitionPosition from, PartitionPosition to)
    {
        long keySize = 0;
        int keyCount = 0;

        for (Iterator<AtomicBTreePartition> it = getPartitionIterator(from, true, to,false); it.hasNext();)
        {
            AtomicBTreePartition en = it.next();
            keySize += en.partitionKey().getKey().remaining();
            keyCount++;
        }
        long partitionKeySize = keySize;
        int partitionCount = keyCount;
        Iterator<AtomicBTreePartition> toFlush = getPartitionIterator(from, true, to,false);

        return new AbstractFlushablePartitionSet<AtomicBTreePartition>()
        {
            public Memtable memtable()
            {
                return ShardedSkipListMemtable.this;
            }

            public PartitionPosition from()
            {
                return from;
            }

            public PartitionPosition to()
            {
                return to;
            }

            public long partitionCount()
            {
                return partitionCount;
            }

            public Iterator<AtomicBTreePartition> iterator()
            {
                return toFlush;
            }

            public long partitionKeysSize()
            {
                return partitionKeySize;
            }
        };
    }

    static class MemtableShard
    {
        // The following fields are volatile as we have to make sure that when we
        // collect results from all sub-ranges, the thread accessing the value
        // is guaranteed to see the changes to the values.

        // The smallest timestamp for all partitions stored in this shard
        private final AtomicLong minTimestamp = new AtomicLong(Long.MAX_VALUE);
        private final AtomicLong minLocalDeletionTime = new AtomicLong(Long.MAX_VALUE);

        private final AtomicLong liveDataSize = new AtomicLong(0);

        private final AtomicLong currentOperations = new AtomicLong(0);

        // We index the memtable by PartitionPosition only for the purpose of being able
        // to select key range using Token.KeyBound. However put() ensures that we
        // actually only store DecoratedKey.
        private final ConcurrentNavigableMap<PartitionPosition, AtomicBTreePartition> partitions = new ConcurrentSkipListMap<>();

        private final ColumnsCollector columnsCollector;

        private final StatsCollector statsCollector;

        @Unmetered  // total pool size should not be included in memtable's deep size
        private final MemtableAllocator allocator;

        private final TableMetadataRef metadata;

        @VisibleForTesting
        MemtableShard(TableMetadataRef metadata, MemtableAllocator allocator)
        {
            this.columnsCollector = new ColumnsCollector(metadata.get().regularAndStaticColumns());
            this.statsCollector = new StatsCollector();
            this.allocator = allocator;
            this.metadata = metadata;
        }

        public long put(DecoratedKey key, PartitionUpdate update, UpdateTransaction indexer, OpOrder.Group opGroup)
        {
            Cloner cloner = allocator.cloner(opGroup);
            AtomicBTreePartition previous = partitions.get(key);

            long initialSize = 0;
            if (previous == null)
            {
                final DecoratedKey cloneKey = cloner.clone(key);
                AtomicBTreePartition empty = new AtomicBTreePartition(metadata, cloneKey, allocator);
                // We'll add the columns later. This avoids wasting works if we get beaten in the putIfAbsent
                previous = partitions.putIfAbsent(cloneKey, empty);
                if (previous == null)
                {
                    previous = empty;
                    // allocate the row overhead after the fact; this saves over allocating and having to free after, but
                    // means we can overshoot our declared limit.
                    int overhead = (int) (cloneKey.getToken().getHeapSize() + SkipListMemtable.ROW_OVERHEAD_HEAP_SIZE);
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

        public boolean isClean()
        {
            return partitions.isEmpty();
        }

        public int size()
        {
            return partitions.size();
        }

        long minTimestamp()
        {
            return minTimestamp.get();
        }

        long liveDataSize()
        {
            return liveDataSize.get();
        }

        long currentOperations()
        {
            return currentOperations.get();
        }

        public long minLocalDeletionTime()
        {
            return minLocalDeletionTime.get();
        }
    }

    public static class MemtableUnfilteredPartitionIterator extends AbstractUnfilteredPartitionIterator implements UnfilteredPartitionIterator
    {
        private final TableMetadata metadata;
        private final Iterator<AtomicBTreePartition> iter;
        private final ColumnFilter columnFilter;
        private final DataRange dataRange;

        public MemtableUnfilteredPartitionIterator(TableMetadata metadata, Iterator<AtomicBTreePartition> iterator, ColumnFilter columnFilter, DataRange dataRange)
        {
            this.metadata = metadata;
            this.iter = iterator;
            this.columnFilter = columnFilter;
            this.dataRange = dataRange;
        }

        public TableMetadata metadata()
        {
            return metadata;
        }

        public boolean hasNext()
        {
            return iter.hasNext();
        }

        public UnfilteredRowIterator next()
        {
            AtomicBTreePartition entry = iter.next();
            DecoratedKey key = entry.partitionKey();
            ClusteringIndexFilter filter = dataRange.clusteringIndexFilter(key);

            return filter.getUnfilteredRowIterator(columnFilter, entry);
        }
    }

    static class Locking extends ShardedSkipListMemtable
    {
        Locking(AtomicReference<CommitLogPosition> commitLogLowerBound, TableMetadataRef metadataRef, Owner owner, Integer shardCountOption)
        {
            super(commitLogLowerBound, metadataRef, owner, shardCountOption);
        }

        /**
         * Should only be called by ColumnFamilyStore.apply via Keyspace.apply, which supplies the appropriate
         * OpOrdering.
         *
         * commitLogSegmentPosition should only be null if this is a secondary index, in which case it is *expected* to be null
         */
        public long put(PartitionUpdate update, UpdateTransaction indexer, OpOrder.Group opGroup)
        {
            DecoratedKey key = update.partitionKey();
            MemtableShard shard = shards[boundaries.getShardForKey(key)];
            synchronized (shard)
            {
                return shard.put(key, update, indexer, opGroup);
            }
        }

    }

    public static Factory factory(Map<String, String> optionsCopy)
    {
        String shardsString = optionsCopy.remove(SHARDS_OPTION);
        Integer shardCount = shardsString != null ? Integer.parseInt(shardsString) : null;
        boolean isLocking = Boolean.parseBoolean(optionsCopy.remove(LOCKING_OPTION));
        return new Factory(shardCount, isLocking);
    }

    static class Factory implements Memtable.Factory
    {
        final Integer shardCount;
        final boolean isLocking;

        Factory(Integer shardCount, boolean isLocking)
        {
            this.shardCount = shardCount;
            this.isLocking = isLocking;
        }

        public Memtable create(AtomicReference<CommitLogPosition> commitLogLowerBound,
                               TableMetadataRef metadataRef,
                               Owner owner)
        {
            return isLocking
                   ? new Locking(commitLogLowerBound, metadataRef, owner, shardCount)
                   : new ShardedSkipListMemtable(commitLogLowerBound, metadataRef, owner, shardCount);
        }

        public boolean equals(Object o)
        {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            Factory factory = (Factory) o;
            return Objects.equals(shardCount, factory.shardCount);
        }

        public int hashCode()
        {
            return Objects.hash(shardCount);
        }
    }
}
