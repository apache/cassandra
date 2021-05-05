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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.BufferDecoratedKey;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionInfo;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.partitions.AbstractUnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.BTreePartitionData;
import org.apache.cassandra.db.partitions.BTreePartitionUpdater;
import org.apache.cassandra.db.partitions.ImmutableBTreePartition;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.tries.MemtableTrie;
import org.apache.cassandra.db.tries.Trie;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.IncludingExcludingBounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.index.transactions.UpdateTransaction;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.metrics.TableMetrics;
import org.apache.cassandra.metrics.TrieMemtableMetricsView;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.MBeanWrapper;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.memory.Cloner;
import org.apache.cassandra.utils.memory.EnsureOnHeap;
import org.apache.cassandra.utils.memory.MemtableAllocator;

public class TrieMemtable extends AbstractAllocatorMemtable
{
    private static final Logger logger = LoggerFactory.getLogger(TrieMemtable.class);
    public static final String TRIE_MEMTABLE_CONFIG_OBJECT_NAME = "org.apache.cassandra.db:type=TrieMemtableConfig";

    public static final Factory FACTORY = new TrieMemtable.Factory();

    /** Buffer type to use for memtable tries (on- vs off-heap) */
    public static final BufferType BUFFER_TYPE;

    static
    {
        switch (DatabaseDescriptor.getMemtableAllocationType())
        {
        case unslabbed_heap_buffers:
        case heap_buffers:
            BUFFER_TYPE = BufferType.ON_HEAP;
            break;
        case offheap_buffers:
        case offheap_objects:
            BUFFER_TYPE = BufferType.OFF_HEAP;
            break;
        default:
            throw new AssertionError();
        }

        MBeanWrapper.instance.registerMBean(new TrieMemtableConfig(), TRIE_MEMTABLE_CONFIG_OBJECT_NAME, MBeanWrapper.OnException.LOG);
    }

    /** If keys is below this length, we will use a recursive procedure for inserting data in the memtable trie. */
    @VisibleForTesting
    public static final int MAX_RECURSIVE_KEY_LENGTH = 128;

    /** The byte-ordering conversion version to use for memtables. */
    public static final ByteComparable.Version BYTE_COMPARABLE_VERSION = ByteComparable.Version.OSS41;

    // Set to true when the memtable requests a switch (e.g. for trie size limit being reached) to ensure only one
    // thread calls cfs.switchMemtableIfCurrent.
    private AtomicBoolean switchRequested = new AtomicBoolean(false);


    // The boundaries for the keyspace as they were calculated when the memtable is created.
    // The boundaries will be NONE for system keyspaces or if StorageService is not yet initialized.
    // The fact this is fixed for the duration of the memtable lifetime, guarantees we'll always pick the same core
    // for the a given key, even if we race with the StorageService initialization or with topology changes.
    private final ShardBoundaries boundaries;

    /**
     * Core-specific memtable regions. All writes must go through the specific core. The data structures used
     * are concurrent-read safe, thus reads can be carried out from any thread.
     */
    private final MemtableShard[] shards;

    /**
     * A merged view of the memtable map. Used for partition range queries and flush.
     * For efficiency we serve single partition requests off the shard which offers more direct MemtableTrie methods.
     */
    private final Trie<BTreePartitionData> mergedTrie;

    private final TrieMemtableMetricsView metrics;

    @VisibleForTesting
    public static final String SHARD_COUNT_PROPERTY = "cassandra.trie.memtable.shard.count";

    private static volatile int SHARD_COUNT = Integer.getInteger(SHARD_COUNT_PROPERTY, FBUtilities.getAvailableProcessors());

    // only to be used by init(), to setup the very first memtable for the cfs
    TrieMemtable(AtomicReference<CommitLogPosition> commitLogLowerBound, TableMetadataRef metadataRef, Owner owner)
    {
        super(commitLogLowerBound, metadataRef, owner);
        this.boundaries = owner.localRangeSplits(getShardCount());
        this.metrics = new TrieMemtableMetricsView(metadataRef.keyspace, metadataRef.name);
        this.shards = generatePartitionShards(boundaries.shardCount(), metadataRef, metrics);
        this.mergedTrie = makeMergedTrie(shards);
    }

    private static MemtableShard[] generatePartitionShards(int splits,
                                                           TableMetadataRef metadata,
                                                           TrieMemtableMetricsView metrics)
    {
        if (splits == 1)
            return new MemtableShard[] { new MemtableShard(0, metadata, metrics) };

        MemtableShard[] partitionMapContainer = new MemtableShard[splits];
        for (int i = 0; i < splits; i++)
            partitionMapContainer[i] = new MemtableShard(i, metadata, metrics);

        return partitionMapContainer;
    }

    private static Trie<BTreePartitionData> makeMergedTrie(MemtableShard[] shards)
    {
        List<Trie<BTreePartitionData>> tries = new ArrayList<>(shards.length);
        for (MemtableShard shard : shards)
            tries.add(shard.data);
        return Trie.mergeDistinct(tries);
    }

    protected Factory factory()
    {
        return FACTORY;
    }

    public boolean isClean()
    {
        for (MemtableShard shard : shards)
            if (!shard.isEmpty())
                return false;
        return true;
    }

    @VisibleForTesting
    @Override
    public void switchOut(OpOrder.Barrier writeBarrier, AtomicReference<CommitLogPosition> commitLogUpperBound)
    {
        super.switchOut(writeBarrier, commitLogUpperBound);

        for (MemtableShard shard : shards)
            shard.allocator.setDiscarding();
    }

    @Override
    public void discard()
    {
        super.discard();
        // metrics here are not thread safe, but I think we can live with that
        metrics.lastFlushShardDataSizes.reset();
        for (MemtableShard shard : shards)
        {
            metrics.lastFlushShardDataSizes.update(shard.liveDataSize());
        }
        for (MemtableShard shard : shards)
        {
            shard.allocator.setDiscarded();
            shard.data.discardBuffers();
        }
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
        long colUpdateTimeDelta = shard.put(key, update, indexer, opGroup);

        if (shard.data.reachedAllocatedSizeThreshold() && !switchRequested.getAndSet(true))
        {
            logger.info("Scheduling flush due to trie size limit reached.");
            owner.signalFlushRequired(this, ColumnFamilyStore.FlushReason.MEMTABLE_LIMIT);
        }

        return colUpdateTimeDelta;
    }

    @Override
    public void addMemoryUsageTo(MemoryUsage stats)
    {
        super.addMemoryUsageTo(stats);
        for (MemtableShard shard : shards)
        {
            stats.ownsOnHeap += shard.allocator.onHeap().owns();
            stats.ownsOffHeap += shard.allocator.offHeap().owns();
            stats.ownershipRatioOnHeap += shard.allocator.onHeap().ownershipRatio();
            stats.ownershipRatioOffHeap += shard.allocator.offHeap().ownershipRatio();
        }
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
    public long getOperations()
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

    @Override
    public long getMinTimestamp()
    {
        long min = Long.MAX_VALUE;
        for (MemtableShard shard : shards)
            min =  Long.min(min, shard.minTimestamp());
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

    public MemtableUnfilteredPartitionIterator makePartitionIterator(final ColumnFilter columnFilter, final DataRange dataRange)
    {
        AbstractBounds<PartitionPosition> keyRange = dataRange.keyRange();

        PartitionPosition left = keyRange.left;
        PartitionPosition right = keyRange.right;
        if (left.isMinimum())
            left = null;
        if (right.isMinimum())
            right = null;

        boolean isBound = keyRange instanceof Bounds;
        boolean includeStart = isBound || keyRange instanceof IncludingExcludingBounds;
        boolean includeStop = isBound || keyRange instanceof Range;

        Trie<BTreePartitionData> subMap = mergedTrie.subtrie(left, includeStart, right, includeStop);

        return new MemtableUnfilteredPartitionIterator(metadata(),
                                                       allocator.ensureOnHeap(),
                                                       subMap,
                                                       columnFilter,
                                                       dataRange);
    }

    public Partition getPartition(DecoratedKey key)
    {
        int shardIndex = boundaries.getShardForKey(key);
        BTreePartitionData data = shards[shardIndex].data.get(key);
        if (data != null)
            return createPartition(metadata(), allocator.ensureOnHeap(), key, data);
        else
            return null;
    }

    private static MemtablePartition createPartition(TableMetadata metadata, EnsureOnHeap ensureOnHeap, DecoratedKey key, BTreePartitionData data)
    {
        return new MemtablePartition(metadata, ensureOnHeap, key, data);
    }

    private static MemtablePartition getPartitionFromTrieEntry(TableMetadata metadata, EnsureOnHeap ensureOnHeap, Map.Entry<ByteComparable, BTreePartitionData> en)
    {
        DecoratedKey key = BufferDecoratedKey.fromByteComparable(en.getKey(),
                                                                 BYTE_COMPARABLE_VERSION,
                                                                 metadata.partitioner);
        return createPartition(metadata, ensureOnHeap, key, en.getValue());
    }


    public FlushCollection<MemtablePartition> getFlushSet(PartitionPosition from, PartitionPosition to)
    {
        Trie<BTreePartitionData> toFlush = mergedTrie.subtrie(from, true, to, false);
        long keySize = 0;
        int keyCount = 0;

        for (Iterator<Map.Entry<ByteComparable, BTreePartitionData>> it = toFlush.entryIterator(); it.hasNext(); )
        {
            Map.Entry<ByteComparable, BTreePartitionData> en = it.next();
            ByteComparable byteComparable = v -> en.getKey().asPeekableBytes(BYTE_COMPARABLE_VERSION);
            byte[] keyBytes = DecoratedKey.keyFromByteComparable(byteComparable, BYTE_COMPARABLE_VERSION, metadata().partitioner);
            keySize += keyBytes.length;
            keyCount++;
        }
        long partitionKeySize = keySize;
        int partitionCount = keyCount;

        return new AbstractFlushCollection<MemtablePartition>()
        {
            public Memtable memtable()
            {
                return TrieMemtable.this;
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

            public Iterator<MemtablePartition> iterator()
            {
                return Iterators.transform(toFlush.entryIterator(),
                                           // TODO: During flushing we shouldn't need to copy partition data on heap because the memtable can't
                                           // disappear until we are done with the flush. Figure out why EnsureOnHeap.NOOP doesn't work.
                                           entry -> getPartitionFromTrieEntry(metadata(), allocator.ensureOnHeap(), entry));
            }

            public long partitionKeySize()
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
        private volatile long minTimestamp = Long.MAX_VALUE;

        private volatile long liveDataSize = 0;

        private volatile long currentOperations = 0;

        private ReentrantLock writeLock = new ReentrantLock();

        // Content map for the given shard. This is implemented as a memtable trie which uses the prefix-free
        // byte-comparable ByteSource representations of the keys to address the partitions.
        //
        // This map is used in a single-producer, multi-consumer fashion: only one thread will insert items but
        // several threads may read from it and iterate over it. Iterators are created when a the first item of
        // a flow is requested for example, and then used asynchronously when sub-sequent items are requested.
        //
        // Therefore, iterators should not throw ConcurrentModificationExceptions if the underlying map is modified
        // during iteration, they should provide a weakly consistent view of the map instead.
        //
        // Also, this data is backed by memtable memory, when accessing it callers must specify if it can be accessed
        // unsafely, meaning that the memtable will not be discarded as long as the data is used, or whether the data
        // should be copied on heap for off-heap allocators.
        @VisibleForTesting
        final MemtableTrie<BTreePartitionData> data;

        private final ColumnsCollector columnsCollector;

        private final StatsCollector statsCollector;

        private final MemtableAllocator allocator;

        private final TrieMemtableMetricsView metrics;

        MemtableShard(int shardId, TableMetadataRef metadata, TrieMemtableMetricsView metrics)
        {
            this(metadata, AbstractAllocatorMemtable.MEMORY_POOL.newAllocator(), metrics);
        }

        @VisibleForTesting
        MemtableShard(TableMetadataRef metadata, MemtableAllocator allocator, TrieMemtableMetricsView metrics)
        {
            this.data = new MemtableTrie<>(BUFFER_TYPE);
            this.columnsCollector = new AbstractMemtable.ColumnsCollector(metadata.get().regularAndStaticColumns());
            this.statsCollector = new AbstractMemtable.StatsCollector();
            this.allocator = allocator;
            this.metrics = metrics;
        }

        public long put(DecoratedKey key, PartitionUpdate update, UpdateTransaction indexer, OpOrder.Group opGroup)
        {
            BTreePartitionUpdater updater = new BTreePartitionUpdater(allocator, opGroup, indexer);
            boolean locked = writeLock.tryLock();
            if (locked)
            {
                metrics.uncontendedPuts.inc();
            }
            else
            {
                metrics.contendedPuts.inc();
                long lockStartTime = System.nanoTime();
                writeLock.lock();
                metrics.contentionTime.addNano(System.nanoTime() - lockStartTime);
            }
            try
            {
                try
                {
                    long onHeap = data.sizeOnHeap();
                    long offHeap = data.sizeOffHeap();
                    // Use the fast recursive put if we know the key is small enough to not cause a stack overflow.
                    try
                    {
                        data.putSingleton(key,
                                          update,
                                          updater::mergePartitions,
                                          key.getKeyLength() < MAX_RECURSIVE_KEY_LENGTH);
                    }
                    catch (MemtableTrie.SpaceExhaustedException e)
                    {
                        // This should never really happen as a flush would be triggered long before this limit is reached.
                        throw Throwables.propagate(e);
                    }
                    allocator.offHeap().adjust(data.sizeOffHeap() - offHeap, opGroup);
                    allocator.onHeap().adjust(data.sizeOnHeap() - onHeap, opGroup);
                }
                finally
                {
                    updateMinTimestamp(update.stats().minTimestamp);
                    updateLiveDataSize(updater.dataSize);
                    updateCurrentOperations(update.operationCount());

                    // TODO: lambov 2021-03-30: check if stats are further optimisable
                    columnsCollector.update(update.columns());
                    statsCollector.update(update.stats());
                }
            }
            finally
            {
                writeLock.unlock();
            }
            return updater.colUpdateTimeDelta;
        }

        public boolean isEmpty()
        {
            return data.isEmpty();
        }

        private void updateMinTimestamp(long timestamp)
        {
            if (timestamp < minTimestamp)
                minTimestamp = timestamp;
        }

        void updateLiveDataSize(long size)
        {
            liveDataSize = liveDataSize + size;
        }

        private void updateCurrentOperations(long op)
        {
            currentOperations = currentOperations + op;
        }

        public int size()
        {
            return data.valuesCount();
        }

        long minTimestamp()
        {
            return minTimestamp;
        }

        long liveDataSize()
        {
            return liveDataSize;
        }

        long currentOperations()
        {
            return currentOperations;
        }
    }

    static class MemtableUnfilteredPartitionIterator extends AbstractUnfilteredPartitionIterator implements Memtable.MemtableUnfilteredPartitionIterator
    {
        private final TableMetadata metadata;
        private final EnsureOnHeap ensureOnHeap;
        private final Trie<BTreePartitionData> source;
        private final Iterator<Map.Entry<ByteComparable, BTreePartitionData>> iter;
        private final ColumnFilter columnFilter;
        private final DataRange dataRange;

        public MemtableUnfilteredPartitionIterator(TableMetadata metadata,
                                                   EnsureOnHeap ensureOnHeap,
                                                   Trie<BTreePartitionData> source,
                                                   ColumnFilter columnFilter,
                                                   DataRange dataRange)
        {
            this.metadata = metadata;
            this.ensureOnHeap = ensureOnHeap;
            this.iter = source.entryIterator();
            this.source = source;
            this.columnFilter = columnFilter;
            this.dataRange = dataRange;
        }

        public int getMinLocalDeletionTime()
        {
            int minLocalDeletionTime = Integer.MAX_VALUE;
            for (BTreePartitionData partition : source.values())
                minLocalDeletionTime = Math.min(minLocalDeletionTime, partition.stats.minLocalDeletionTime);

            return minLocalDeletionTime;
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
            Partition partition = getPartitionFromTrieEntry(metadata(), ensureOnHeap, iter.next());
            DecoratedKey key = partition.partitionKey();
            ClusteringIndexFilter filter = dataRange.clusteringIndexFilter(key);

            return filter.getUnfilteredRowIterator(columnFilter, partition);
        }
    }

    static class MemtablePartition extends ImmutableBTreePartition
    {

        private final EnsureOnHeap ensureOnHeap;

        private MemtablePartition(TableMetadata table, EnsureOnHeap ensureOnHeap, DecoratedKey key, BTreePartitionData data)
        {
            super(table, key, data);
            this.ensureOnHeap = ensureOnHeap;
        }

        @Override
        protected boolean canHaveShadowedData()
        {
            // The BtreePartitionData we store in the memtable are build iteratively by BTreePartitionData.add(), which
            // doesn't make sure there isn't shadowed data, so we'll need to eliminate any.
            return true;
        }


        @Override
        public DeletionInfo deletionInfo()
        {
            return ensureOnHeap.applyToDeletionInfo(super.deletionInfo());
        }

        @Override
        public Row staticRow()
        {
            return ensureOnHeap.applyToStatic(super.staticRow());
        }

        @Override
        public DecoratedKey partitionKey()
        {
            return ensureOnHeap.applyToPartitionKey(super.partitionKey());
        }

        @Override
        public Row getRow(Clustering<?> clustering)
        {
            return ensureOnHeap.applyToRow(super.getRow(clustering));
        }

        @Override
        public Row lastRow()
        {
            return ensureOnHeap.applyToRow(super.lastRow());
        }

        @Override
        public UnfilteredRowIterator unfilteredIterator(ColumnFilter selection, Slices slices, boolean reversed)
        {
            return ensureOnHeap.applyToPartition(super.unfilteredIterator(selection, slices, reversed));
        }

        @Override
        public UnfilteredRowIterator unfilteredIterator(ColumnFilter selection, NavigableSet<Clustering<?>> clusteringsInQueryOrder, boolean reversed)
        {
            return ensureOnHeap
                            .applyToPartition(super.unfilteredIterator(selection, clusteringsInQueryOrder, reversed));
        }

        @Override
        public UnfilteredRowIterator unfilteredIterator()
        {
            return ensureOnHeap.applyToPartition(super.unfilteredIterator());
        }

        @Override
        public UnfilteredRowIterator unfilteredIterator(BTreePartitionData current, ColumnFilter selection, Slices slices, boolean reversed)
        {
            return ensureOnHeap
                            .applyToPartition(super.unfilteredIterator(current, selection, slices, reversed));
        }

        @Override
        public Iterator<Row> iterator()
        {
            return ensureOnHeap.applyToPartition(super.iterator());
        }
    }

    static class Factory implements Memtable.Factory
    {
        public Memtable create(AtomicReference<CommitLogPosition> commitLogLowerBound,
                               TableMetadataRef metadaRef,
                               Owner owner)
        {
            return new TrieMemtable(commitLogLowerBound, metadaRef, owner);
        }

        @Override
        public TableMetrics.ReleasableMetric createMemtableMetrics(TableMetadataRef metadataRef)
        {
            TrieMemtableMetricsView metrics = new TrieMemtableMetricsView(metadataRef.keyspace, metadataRef.name);
            return metrics::release;
        }
    }

    private static class TrieMemtableConfig implements TrieMemtableConfigMXBean
    {
        @Override
        public void setShardCount(String shardCount)
        {
            if ("auto".equalsIgnoreCase(shardCount))
            {
                SHARD_COUNT = FBUtilities.getAvailableProcessors();
            }
            else
            {
                try
                {
                    SHARD_COUNT = Integer.valueOf(shardCount);
                }
                catch (NumberFormatException ex)
                {
                    logger.warn("Unable to parse {} as valid value for shard count", shardCount);
                    return;
                }
            }
            logger.info("Requested setting shard count to {}; set to: {}", shardCount, SHARD_COUNT);
        }
    }

    @VisibleForTesting
    public static int getShardCount()
    {
        return SHARD_COUNT;
    }
}
