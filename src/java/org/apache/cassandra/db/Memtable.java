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
package org.apache.cassandra.db;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.SchemaConstants;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.commitlog.IntervalSet;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.*;
import org.apache.cassandra.dht.Murmur3Partitioner.LongToken;
import org.apache.cassandra.index.transactions.UpdateTransaction;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableMultiWriter;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.memory.HeapPool;
import org.apache.cassandra.utils.memory.MemtableAllocator;
import org.apache.cassandra.utils.memory.MemtablePool;
import org.apache.cassandra.utils.memory.NativePool;
import org.apache.cassandra.utils.memory.SlabPool;

public class Memtable implements Comparable<Memtable>
{
    private static final Logger logger = LoggerFactory.getLogger(Memtable.class);

    public static final MemtablePool MEMORY_POOL = createMemtableAllocatorPool();

    private static MemtablePool createMemtableAllocatorPool()
    {
        long heapLimit = DatabaseDescriptor.getMemtableHeapSpaceInMb() << 20;
        long offHeapLimit = DatabaseDescriptor.getMemtableOffheapSpaceInMb() << 20;
        switch (DatabaseDescriptor.getMemtableAllocationType())
        {
            case unslabbed_heap_buffers:
                return new HeapPool(heapLimit, DatabaseDescriptor.getMemtableCleanupThreshold(), new ColumnFamilyStore.FlushLargestColumnFamily());
            case heap_buffers:
                return new SlabPool(heapLimit, 0, DatabaseDescriptor.getMemtableCleanupThreshold(), new ColumnFamilyStore.FlushLargestColumnFamily());
            case offheap_buffers:
                if (!FileUtils.isCleanerAvailable)
                {
                    throw new IllegalStateException("Could not free direct byte buffer: offheap_buffers is not a safe memtable_allocation_type without this ability, please adjust your config. This feature is only guaranteed to work on an Oracle JVM. Refusing to start.");
                }
                return new SlabPool(heapLimit, offHeapLimit, DatabaseDescriptor.getMemtableCleanupThreshold(), new ColumnFamilyStore.FlushLargestColumnFamily());
            case offheap_objects:
                return new NativePool(heapLimit, offHeapLimit, DatabaseDescriptor.getMemtableCleanupThreshold(), new ColumnFamilyStore.FlushLargestColumnFamily());
            default:
                throw new AssertionError();
        }
    }

    private static final int ROW_OVERHEAD_HEAP_SIZE = estimateRowOverhead(Integer.parseInt(System.getProperty("cassandra.memtable_row_overhead_computation_step", "100000")));

    private final MemtableAllocator allocator;
    private final AtomicLong liveDataSize = new AtomicLong(0);
    private final AtomicLong currentOperations = new AtomicLong(0);

    // the write barrier for directing writes to this memtable during a switch
    private volatile OpOrder.Barrier writeBarrier;
    // the precise upper bound of CommitLogPosition owned by this memtable
    private volatile AtomicReference<CommitLogPosition> commitLogUpperBound;
    // the precise lower bound of CommitLogPosition owned by this memtable; equal to its predecessor's commitLogUpperBound
    private AtomicReference<CommitLogPosition> commitLogLowerBound;

    // The approximate lower bound by this memtable; must be <= commitLogLowerBound once our predecessor
    // has been finalised, and this is enforced in the ColumnFamilyStore.setCommitLogUpperBound
    private final CommitLogPosition approximateCommitLogLowerBound = CommitLog.instance.getCurrentPosition();

    public int compareTo(Memtable that)
    {
        return this.approximateCommitLogLowerBound.compareTo(that.approximateCommitLogLowerBound);
    }

    public static final class LastCommitLogPosition extends CommitLogPosition
    {
        public LastCommitLogPosition(CommitLogPosition copy)
        {
            super(copy.segmentId, copy.position);
        }
    }

    // We index the memtable by PartitionPosition only for the purpose of being able
    // to select key range using Token.KeyBound. However put() ensures that we
    // actually only store DecoratedKey.
    private final ConcurrentNavigableMap<PartitionPosition, AtomicBTreePartition> partitions = new ConcurrentSkipListMap<>();
    public final ColumnFamilyStore cfs;
    private final long creationNano = System.nanoTime();

    // The smallest timestamp for all partitions stored in this memtable
    private long minTimestamp = Long.MAX_VALUE;

    // Record the comparator of the CFS at the creation of the memtable. This
    // is only used when a user update the CF comparator, to know if the
    // memtable was created with the new or old comparator.
    public final ClusteringComparator initialComparator;

    private final ColumnsCollector columnsCollector;
    private final StatsCollector statsCollector = new StatsCollector();

    // only to be used by init(), to setup the very first memtable for the cfs
    public Memtable(AtomicReference<CommitLogPosition> commitLogLowerBound, ColumnFamilyStore cfs)
    {
        this.cfs = cfs;
        this.commitLogLowerBound = commitLogLowerBound;
        this.allocator = MEMORY_POOL.newAllocator();
        this.initialComparator = cfs.metadata.comparator;
        this.cfs.scheduleFlush();
        this.columnsCollector = new ColumnsCollector(cfs.metadata.partitionColumns());
    }

    // ONLY to be used for testing, to create a mock Memtable
    @VisibleForTesting
    public Memtable(CFMetaData metadata)
    {
        this.initialComparator = metadata.comparator;
        this.cfs = null;
        this.allocator = null;
        this.columnsCollector = new ColumnsCollector(metadata.partitionColumns());
    }

    public MemtableAllocator getAllocator()
    {
        return allocator;
    }

    public long getLiveDataSize()
    {
        return liveDataSize.get();
    }

    public long getOperations()
    {
        return currentOperations.get();
    }

    @VisibleForTesting
    public void setDiscarding(OpOrder.Barrier writeBarrier, AtomicReference<CommitLogPosition> commitLogUpperBound)
    {
        assert this.writeBarrier == null;
        this.commitLogUpperBound = commitLogUpperBound;
        this.writeBarrier = writeBarrier;
        allocator.setDiscarding();
    }

    void setDiscarded()
    {
        allocator.setDiscarded();
    }

    // decide if this memtable should take the write, or if it should go to the next memtable
    public boolean accepts(OpOrder.Group opGroup, CommitLogPosition commitLogPosition)
    {
        // if the barrier hasn't been set yet, then this memtable is still taking ALL writes
        OpOrder.Barrier barrier = this.writeBarrier;
        if (barrier == null)
            return true;
        // if the barrier has been set, but is in the past, we are definitely destined for a future memtable
        if (!barrier.isAfter(opGroup))
            return false;
        // if we aren't durable we are directed only by the barrier
        if (commitLogPosition == null)
            return true;
        while (true)
        {
            // otherwise we check if we are in the past/future wrt the CL boundary;
            // if the boundary hasn't been finalised yet, we simply update it to the max of
            // its current value and ours; if it HAS been finalised, we simply accept its judgement
            // this permits us to coordinate a safe boundary, as the boundary choice is made
            // atomically wrt our max() maintenance, so an operation cannot sneak into the past
            CommitLogPosition currentLast = commitLogUpperBound.get();
            if (currentLast instanceof LastCommitLogPosition)
                return currentLast.compareTo(commitLogPosition) >= 0;
            if (currentLast != null && currentLast.compareTo(commitLogPosition) >= 0)
                return true;
            if (commitLogUpperBound.compareAndSet(currentLast, commitLogPosition))
                return true;
        }
    }

    public CommitLogPosition getCommitLogLowerBound()
    {
        return commitLogLowerBound.get();
    }

    public CommitLogPosition getCommitLogUpperBound()
    {
        return commitLogUpperBound.get();
    }

    public boolean isLive()
    {
        return allocator.isLive();
    }

    public boolean isClean()
    {
        return partitions.isEmpty();
    }

    public boolean mayContainDataBefore(CommitLogPosition position)
    {
        return approximateCommitLogLowerBound.compareTo(position) < 0;
    }

    /**
     * @return true if this memtable is expired. Expiration time is determined by CF's memtable_flush_period_in_ms.
     */
    public boolean isExpired()
    {
        int period = cfs.metadata.params.memtableFlushPeriodInMs;
        return period > 0 && (System.nanoTime() - creationNano >= TimeUnit.MILLISECONDS.toNanos(period));
    }

    /**
     * Should only be called by ColumnFamilyStore.apply via Keyspace.apply, which supplies the appropriate
     * OpOrdering.
     *
     * commitLogSegmentPosition should only be null if this is a secondary index, in which case it is *expected* to be null
     */
    long put(PartitionUpdate update, UpdateTransaction indexer, OpOrder.Group opGroup)
    {
        AtomicBTreePartition previous = partitions.get(update.partitionKey());

        long initialSize = 0;
        if (previous == null)
        {
            final DecoratedKey cloneKey = allocator.clone(update.partitionKey(), opGroup);
            AtomicBTreePartition empty = new AtomicBTreePartition(cfs.metadata, cloneKey, allocator);
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

        long[] pair = previous.addAllWithSizeDelta(update, opGroup, indexer);
        minTimestamp = Math.min(minTimestamp, previous.stats().minTimestamp);
        liveDataSize.addAndGet(initialSize + pair[0]);
        columnsCollector.update(update.columns());
        statsCollector.update(update.stats());
        currentOperations.addAndGet(update.operationCount());
        return pair[1];
    }

    public int partitionCount()
    {
        return partitions.size();
    }

    public List<FlushRunnable> flushRunnables(LifecycleTransaction txn)
    {
        return createFlushRunnables(txn);
    }

    private List<FlushRunnable> createFlushRunnables(LifecycleTransaction txn)
    {
        DiskBoundaries diskBoundaries = cfs.getDiskBoundaries();
        List<PartitionPosition> boundaries = diskBoundaries.positions;
        List<Directories.DataDirectory> locations = diskBoundaries.directories;
        if (boundaries == null)
            return Collections.singletonList(new FlushRunnable(txn));

        List<FlushRunnable> runnables = new ArrayList<>(boundaries.size());
        PartitionPosition rangeStart = cfs.getPartitioner().getMinimumToken().minKeyBound();
        try
        {
            for (int i = 0; i < boundaries.size(); i++)
            {
                PartitionPosition t = boundaries.get(i);
                runnables.add(new FlushRunnable(rangeStart, t, locations.get(i), txn));
                rangeStart = t;
            }
            return runnables;
        }
        catch (Throwable e)
        {
            throw Throwables.propagate(abortRunnables(runnables, e));
        }
    }

    public Throwable abortRunnables(List<FlushRunnable> runnables, Throwable t)
    {
        if (runnables != null)
            for (FlushRunnable runnable : runnables)
                t = runnable.writer.abort(t);
        return t;
    }

    public String toString()
    {
        return String.format("Memtable-%s@%s(%s serialized bytes, %s ops, %.0f%%/%.0f%% of on/off-heap limit)",
                             cfs.name, hashCode(), FBUtilities.prettyPrintMemory(liveDataSize.get()), currentOperations,
                             100 * allocator.onHeap().ownershipRatio(), 100 * allocator.offHeap().ownershipRatio());
    }

    public MemtableUnfilteredPartitionIterator makePartitionIterator(final ColumnFilter columnFilter, final DataRange dataRange, final boolean isForThrift)
    {
        AbstractBounds<PartitionPosition> keyRange = dataRange.keyRange();

        boolean startIsMin = keyRange.left.isMinimum();
        boolean stopIsMin = keyRange.right.isMinimum();

        boolean isBound = keyRange instanceof Bounds;
        boolean includeStart = isBound || keyRange instanceof IncludingExcludingBounds;
        boolean includeStop = isBound || keyRange instanceof Range;
        Map<PartitionPosition, AtomicBTreePartition> subMap;
        if (startIsMin)
            subMap = stopIsMin ? partitions : partitions.headMap(keyRange.right, includeStop);
        else
            subMap = stopIsMin
                   ? partitions.tailMap(keyRange.left, includeStart)
                   : partitions.subMap(keyRange.left, includeStart, keyRange.right, includeStop);

        int minLocalDeletionTime = Integer.MAX_VALUE;

        // avoid iterating over the memtable if we purge all tombstones
        if (cfs.getCompactionStrategyManager().onlyPurgeRepairedTombstones())
            minLocalDeletionTime = findMinLocalDeletionTime(subMap.entrySet().iterator());

        final Iterator<Map.Entry<PartitionPosition, AtomicBTreePartition>> iter = subMap.entrySet().iterator();

        return new MemtableUnfilteredPartitionIterator(cfs, iter, isForThrift, minLocalDeletionTime, columnFilter, dataRange);
    }

    private int findMinLocalDeletionTime(Iterator<Map.Entry<PartitionPosition, AtomicBTreePartition>> iterator)
    {
        int minLocalDeletionTime = Integer.MAX_VALUE;
        while (iterator.hasNext())
        {
            Map.Entry<PartitionPosition, AtomicBTreePartition> entry = iterator.next();
            minLocalDeletionTime = Math.min(minLocalDeletionTime, entry.getValue().stats().minLocalDeletionTime);
        }
        return minLocalDeletionTime;
    }

    public Partition getPartition(DecoratedKey key)
    {
        return partitions.get(key);
    }

    public long getMinTimestamp()
    {
        return minTimestamp;
    }

    /**
     * For testing only. Give this memtable too big a size to make it always fail flushing.
     */
    @VisibleForTesting
    public void makeUnflushable()
    {
        liveDataSize.addAndGet(1L * 1024 * 1024 * 1024 * 1024 * 1024);
    }

    class FlushRunnable implements Callable<SSTableMultiWriter>
    {
        private final long estimatedSize;
        private final ConcurrentNavigableMap<PartitionPosition, AtomicBTreePartition> toFlush;

        private final boolean isBatchLogTable;
        private final SSTableMultiWriter writer;

        // keeping these to be able to log what we are actually flushing
        private final PartitionPosition from;
        private final PartitionPosition to;

        FlushRunnable(PartitionPosition from, PartitionPosition to, Directories.DataDirectory flushLocation, LifecycleTransaction txn)
        {
            this(partitions.subMap(from, to), flushLocation, from, to, txn);
        }

        FlushRunnable(LifecycleTransaction txn)
        {
            this(partitions, null, null, null, txn);
        }

        FlushRunnable(ConcurrentNavigableMap<PartitionPosition, AtomicBTreePartition> toFlush, Directories.DataDirectory flushLocation, PartitionPosition from, PartitionPosition to, LifecycleTransaction txn)
        {
            this.toFlush = toFlush;
            this.from = from;
            this.to = to;
            long keySize = 0;
            for (PartitionPosition key : toFlush.keySet())
            {
                //  make sure we don't write non-sensical keys
                assert key instanceof DecoratedKey;
                keySize += ((DecoratedKey) key).getKey().remaining();
            }
            estimatedSize = (long) ((keySize // index entries
                                    + keySize // keys in data file
                                    + liveDataSize.get()) // data
                                    * 1.2); // bloom filter and row index overhead

            this.isBatchLogTable = cfs.name.equals(SystemKeyspace.BATCHES) && cfs.keyspace.getName().equals(SchemaConstants.SYSTEM_KEYSPACE_NAME);

            if (flushLocation == null)
                writer = createFlushWriter(txn, cfs.getSSTablePath(getDirectories().getWriteableLocationAsFile(estimatedSize)), columnsCollector.get(), statsCollector.get());
            else
                writer = createFlushWriter(txn, cfs.getSSTablePath(getDirectories().getLocationForDisk(flushLocation)), columnsCollector.get(), statsCollector.get());

        }

        protected Directories getDirectories()
        {
            return cfs.getDirectories();
        }

        private void writeSortedContents()
        {
            logger.debug("Writing {}, flushed range = ({}, {}]", Memtable.this.toString(), from, to);

            boolean trackContention = logger.isTraceEnabled();
            int heavilyContendedRowCount = 0;
            // (we can't clear out the map as-we-go to free up memory,
            //  since the memtable is being used for queries in the "pending flush" category)
            for (AtomicBTreePartition partition : toFlush.values())
            {
                // Each batchlog partition is a separate entry in the log. And for an entry, we only do 2
                // operations: 1) we insert the entry and 2) we delete it. Further, BL data is strictly local,
                // we don't need to preserve tombstones for repair. So if both operation are in this
                // memtable (which will almost always be the case if there is no ongoing failure), we can
                // just skip the entry (CASSANDRA-4667).
                if (isBatchLogTable && !partition.partitionLevelDeletion().isLive() && partition.hasRows())
                    continue;

                if (trackContention && partition.usePessimisticLocking())
                    heavilyContendedRowCount++;

                if (!partition.isEmpty())
                {
                    try (UnfilteredRowIterator iter = partition.unfilteredIterator())
                    {
                        writer.append(iter);
                    }
                }
            }

            long bytesFlushed = writer.getFilePointer();
            logger.debug("Completed flushing {} ({}) for commitlog position {}",
                                                                              writer.getFilename(),
                                                                              FBUtilities.prettyPrintMemory(bytesFlushed),
                                                                              commitLogUpperBound);
            // Update the metrics
            cfs.metric.bytesFlushed.inc(bytesFlushed);

            if (heavilyContendedRowCount > 0)
                logger.trace("High update contention in {}/{} partitions of {} ", heavilyContendedRowCount, toFlush.size(), Memtable.this);
        }

        public SSTableMultiWriter createFlushWriter(LifecycleTransaction txn,
                                                  String filename,
                                                  PartitionColumns columns,
                                                  EncodingStats stats)
        {
            MetadataCollector sstableMetadataCollector = new MetadataCollector(cfs.metadata.comparator)
                    .commitLogIntervals(new IntervalSet<>(commitLogLowerBound.get(), commitLogUpperBound.get()));

            return cfs.createSSTableMultiWriter(Descriptor.fromFilename(filename),
                                                toFlush.size(),
                                                ActiveRepairService.UNREPAIRED_SSTABLE,
                                                sstableMetadataCollector,
                                                new SerializationHeader(true, cfs.metadata, columns, stats), txn);
        }

        @Override
        public SSTableMultiWriter call()
        {
            writeSortedContents();
            return writer;
        }
    }

    private static int estimateRowOverhead(final int count)
    {
        // calculate row overhead
        try (final OpOrder.Group group = new OpOrder().start())
        {
            int rowOverhead;
            MemtableAllocator allocator = MEMORY_POOL.newAllocator();
            ConcurrentNavigableMap<PartitionPosition, Object> partitions = new ConcurrentSkipListMap<>();
            final Object val = new Object();
            for (int i = 0 ; i < count ; i++)
                partitions.put(allocator.clone(new BufferDecoratedKey(new LongToken(i), ByteBufferUtil.EMPTY_BYTE_BUFFER), group), val);
            double avgSize = ObjectSizes.measureDeep(partitions) / (double) count;
            rowOverhead = (int) ((avgSize - Math.floor(avgSize)) < 0.05 ? Math.floor(avgSize) : Math.ceil(avgSize));
            rowOverhead -= ObjectSizes.measureDeep(new LongToken(0));
            rowOverhead += AtomicBTreePartition.EMPTY_SIZE;
            allocator.setDiscarding();
            allocator.setDiscarded();
            return rowOverhead;
        }
    }

    public static class MemtableUnfilteredPartitionIterator extends AbstractUnfilteredPartitionIterator
    {
        private final ColumnFamilyStore cfs;
        private final Iterator<Map.Entry<PartitionPosition, AtomicBTreePartition>> iter;
        private final boolean isForThrift;
        private final int minLocalDeletionTime;
        private final ColumnFilter columnFilter;
        private final DataRange dataRange;

        public MemtableUnfilteredPartitionIterator(ColumnFamilyStore cfs, Iterator<Map.Entry<PartitionPosition, AtomicBTreePartition>> iter, boolean isForThrift, int minLocalDeletionTime, ColumnFilter columnFilter, DataRange dataRange)
        {
            this.cfs = cfs;
            this.iter = iter;
            this.isForThrift = isForThrift;
            this.minLocalDeletionTime = minLocalDeletionTime;
            this.columnFilter = columnFilter;
            this.dataRange = dataRange;
        }

        public boolean isForThrift()
        {
            return isForThrift;
        }

        public int getMinLocalDeletionTime()
        {
            return minLocalDeletionTime;
        }

        public CFMetaData metadata()
        {
            return cfs.metadata;
        }

        public boolean hasNext()
        {
            return iter.hasNext();
        }

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

    private static class ColumnsCollector
    {
        private final HashMap<ColumnDefinition, AtomicBoolean> predefined = new HashMap<>();
        private final ConcurrentSkipListSet<ColumnDefinition> extra = new ConcurrentSkipListSet<>();
        ColumnsCollector(PartitionColumns columns)
        {
            for (ColumnDefinition def : columns.statics)
                predefined.put(def, new AtomicBoolean());
            for (ColumnDefinition def : columns.regulars)
                predefined.put(def, new AtomicBoolean());
        }

        public void update(PartitionColumns columns)
        {
            for (ColumnDefinition s : columns.statics)
                update(s);
            for (ColumnDefinition r : columns.regulars)
                update(r);
        }

        private void update(ColumnDefinition definition)
        {
            AtomicBoolean present = predefined.get(definition);
            if (present != null)
            {
                if (!present.get())
                    present.set(true);
            }
            else
            {
                extra.add(definition);
            }
        }

        public PartitionColumns get()
        {
            PartitionColumns.Builder builder = PartitionColumns.builder();
            for (Map.Entry<ColumnDefinition, AtomicBoolean> e : predefined.entrySet())
                if (e.getValue().get())
                    builder.add(e.getKey());
            return builder.addAll(extra).build();
        }
    }

    private static class StatsCollector
    {
        private final AtomicReference<EncodingStats> stats = new AtomicReference<>(EncodingStats.NO_STATS);

        public void update(EncodingStats newStats)
        {
            while (true)
            {
                EncodingStats current = stats.get();
                EncodingStats updated = current.mergeWith(newStats);
                if (stats.compareAndSet(current, updated))
                    return;
            }
        }

        public EncodingStats get()
        {
            return stats.get();
        }
    }
}
