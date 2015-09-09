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

import java.io.File;
import java.util.AbstractMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.annotations.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.commitlog.ReplayPosition;
import org.apache.cassandra.db.composites.CellNameType;
import org.apache.cassandra.db.index.SecondaryIndexManager;
import org.apache.cassandra.dht.Murmur3Partitioner.LongToken;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.utils.*;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.memory.*;

public class Memtable implements Comparable<Memtable>
{
    private static final Logger logger = LoggerFactory.getLogger(Memtable.class);

    static final MemtablePool MEMORY_POOL = DatabaseDescriptor.getMemtableAllocatorPool();
    private static final int ROW_OVERHEAD_HEAP_SIZE = estimateRowOverhead(Integer.parseInt(System.getProperty("cassandra.memtable_row_overhead_computation_step", "100000")));

    private final MemtableAllocator allocator;
    private final AtomicLong liveDataSize = new AtomicLong(0);
    private final AtomicLong currentOperations = new AtomicLong(0);

    // the write barrier for directing writes to this memtable during a switch
    private volatile OpOrder.Barrier writeBarrier;
    // the precise upper bound of ReplayPosition owned by this memtable
    private volatile AtomicReference<ReplayPosition> commitLogUpperBound;
    // the precise lower bound of ReplayPosition owned by this memtable; equal to its predecessor's commitLogUpperBound
    private AtomicReference<ReplayPosition> commitLogLowerBound;
    // the approximate lower bound by this memtable; must be <= commitLogLowerBound once our predecessor
    // has been finalised, and this is enforced in the ColumnFamilyStore.setCommitLogUpperBound
    private final ReplayPosition approximateCommitLogLowerBound = CommitLog.instance.getContext();

    public int compareTo(Memtable that)
    {
        return this.approximateCommitLogLowerBound.compareTo(that.approximateCommitLogLowerBound);
    }

    public static final class LastReplayPosition extends ReplayPosition
    {
        public LastReplayPosition(ReplayPosition copy) {
            super(copy.segment, copy.position);
        }
    }

    // We index the memtable by RowPosition only for the purpose of being able
    // to select key range using Token.KeyBound. However put() ensures that we
    // actually only store DecoratedKey.
    private final ConcurrentNavigableMap<RowPosition, AtomicBTreeColumns> rows = new ConcurrentSkipListMap<>();
    public final ColumnFamilyStore cfs;
    private final long creationNano = System.nanoTime();

    // The smallest timestamp for all partitions stored in this memtable
    private long minTimestamp = Long.MAX_VALUE;

    // Record the comparator of the CFS at the creation of the memtable. This
    // is only used when a user update the CF comparator, to know if the
    // memtable was created with the new or old comparator.
    public final CellNameType initialComparator;

    public Memtable(AtomicReference<ReplayPosition> commitLogLowerBound, ColumnFamilyStore cfs)
    {
        this.cfs = cfs;
        this.commitLogLowerBound = commitLogLowerBound;
        this.allocator = MEMORY_POOL.newAllocator();
        this.initialComparator = cfs.metadata.comparator;
        this.cfs.scheduleFlush();
    }

    // ONLY to be used for testing, to create a mock Memtable
    @VisibleForTesting
    public Memtable(CFMetaData metadata)
    {
        this.initialComparator = metadata.comparator;
        this.cfs = null;
        this.allocator = null;
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
    public void setDiscarding(OpOrder.Barrier writeBarrier, AtomicReference<ReplayPosition> lastReplayPosition)
    {
        assert this.writeBarrier == null;
        this.commitLogUpperBound = lastReplayPosition;
        this.writeBarrier = writeBarrier;
        allocator.setDiscarding();
    }

    void setDiscarded()
    {
        allocator.setDiscarded();
    }

    // decide if this memtable should take the write, or if it should go to the next memtable
    public boolean accepts(OpOrder.Group opGroup, ReplayPosition replayPosition)
    {
        // if the barrier hasn't been set yet, then this memtable is still taking ALL writes
        OpOrder.Barrier barrier = this.writeBarrier;
        if (barrier == null)
            return true;
        // if the barrier has been set, but is in the past, we are definitely destined for a future memtable
        if (!barrier.isAfter(opGroup))
            return false;
        // if we aren't durable we are directed only by the barrier
        if (replayPosition == null)
            return true;
        while (true)
        {
            // otherwise we check if we are in the past/future wrt the CL boundary;
            // if the boundary hasn't been finalised yet, we simply update it to the max of
            // its current value and ours; if it HAS been finalised, we simply accept its judgement
            // this permits us to coordinate a safe boundary, as the boundary choice is made
            // atomically wrt our max() maintenance, so an operation cannot sneak into the past
            ReplayPosition currentLast = commitLogUpperBound.get();
            if (currentLast instanceof LastReplayPosition)
                return currentLast.compareTo(replayPosition) >= 0;
            if (currentLast != null && currentLast.compareTo(replayPosition) >= 0)
                return true;
            if (commitLogUpperBound.compareAndSet(currentLast, replayPosition))
                return true;
        }
    }

    public ReplayPosition getCommitLogLowerBound()
    {
        return commitLogLowerBound.get();
    }

    public boolean isLive()
    {
        return allocator.isLive();
    }

    public boolean isClean()
    {
        return rows.isEmpty();
    }

    public boolean mayContainDataBefore(ReplayPosition position)
    {
        return approximateCommitLogLowerBound.compareTo(position) < 0;
    }

    /**
     * @return true if this memtable is expired. Expiration time is determined by CF's memtable_flush_period_in_ms.
     */
    public boolean isExpired()
    {
        int period = cfs.metadata.getMemtableFlushPeriod();
        return period > 0 && (System.nanoTime() - creationNano >= TimeUnit.MILLISECONDS.toNanos(period));
    }

    /**
     * Should only be called by ColumnFamilyStore.apply via Keyspace.apply, which supplies the appropriate
     * OpOrdering.
     *
     * replayPosition should only be null if this is a secondary index, in which case it is *expected* to be null
     */
    long put(DecoratedKey key, ColumnFamily cf, SecondaryIndexManager.Updater indexer, OpOrder.Group opGroup)
    {
        AtomicBTreeColumns previous = rows.get(key);

        long initialSize = 0;
        if (previous == null)
        {
            AtomicBTreeColumns empty = cf.cloneMeShallow(AtomicBTreeColumns.factory, false);
            final DecoratedKey cloneKey = allocator.clone(key, opGroup);
            // We'll add the columns later. This avoids wasting works if we get beaten in the putIfAbsent
            previous = rows.putIfAbsent(cloneKey, empty);
            if (previous == null)
            {
                previous = empty;
                // allocate the row overhead after the fact; this saves over allocating and having to free after, but
                // means we can overshoot our declared limit.
                int overhead = (int) (key.getToken().getHeapSize() + ROW_OVERHEAD_HEAP_SIZE);
                allocator.onHeap().allocate(overhead, opGroup);
                initialSize = 8;
            }
            else
            {
                allocator.reclaimer().reclaimImmediately(cloneKey);
            }
        }

        final AtomicBTreeColumns.ColumnUpdater updater = previous.addAllWithSizeDelta(cf, allocator, opGroup, indexer);
        minTimestamp = Math.min(minTimestamp, updater.minTimestamp);
        liveDataSize.addAndGet(initialSize + updater.dataSize);
        currentOperations.addAndGet(cf.getColumnCount() + (cf.isMarkedForDelete() ? 1 : 0) + cf.deletionInfo().rangeCount());
        return updater.colUpdateTimeDelta;
    }

    // for debugging
    public String contents()
    {
        StringBuilder builder = new StringBuilder();
        builder.append("{");
        for (Map.Entry<RowPosition, AtomicBTreeColumns> entry : rows.entrySet())
        {
            builder.append(entry.getKey()).append(": ").append(entry.getValue()).append(", ");
        }
        builder.append("}");
        return builder.toString();
    }

    public int partitionCount()
    {
        return rows.size();
    }

    public String toString()
    {
        return String.format("Memtable-%s@%s(%s serialized bytes, %s ops, %.0f%%/%.0f%% of on/off-heap limit)",
                             cfs.name, hashCode(), FBUtilities.prettyPrintMemory(liveDataSize.get()), currentOperations,
                             100 * allocator.onHeap().ownershipRatio(), 100 * allocator.offHeap().ownershipRatio());
    }

    /**
     * @param startWith Include data in the result from and including this key and to the end of the memtable
     * @return An iterator of entries with the data from the start key
     */
    public Iterator<Map.Entry<DecoratedKey, ColumnFamily>> getEntryIterator(final RowPosition startWith, final RowPosition stopAt)
    {
        return new Iterator<Map.Entry<DecoratedKey, ColumnFamily>>()
        {
            private Iterator<? extends Map.Entry<? extends RowPosition, AtomicBTreeColumns>> iter = stopAt.isMinimum()
                    ? rows.tailMap(startWith).entrySet().iterator()
                    : rows.subMap(startWith, true, stopAt, true).entrySet().iterator();

            private Map.Entry<? extends RowPosition, ? extends ColumnFamily> currentEntry;

            public boolean hasNext()
            {
                return iter.hasNext();
            }

            public Map.Entry<DecoratedKey, ColumnFamily> next()
            {
                Map.Entry<? extends RowPosition, ? extends ColumnFamily> entryRowPosition = iter.next();
                // Actual stored key should be true DecoratedKey
                assert entryRowPosition.getKey() instanceof DecoratedKey;
                @SuppressWarnings("unchecked") // Object cast is required since otherwise we can't turn RowPosition into DecoratedKey
                Map.Entry<DecoratedKey, ColumnFamily> entry = (Map.Entry<DecoratedKey, ColumnFamily>) entryRowPosition;
                if (MEMORY_POOL.needToCopyOnHeap())
                {
                    DecoratedKey key = entry.getKey();
                    key = new BufferDecoratedKey(key.getToken(), HeapAllocator.instance.clone(key.getKey()));
                    ColumnFamily cells = ArrayBackedSortedColumns.localCopy(entry.getValue(), HeapAllocator.instance);
                    entry = new AbstractMap.SimpleImmutableEntry<>(key, cells);
                }
                // Store the reference to the current entry so that remove() can update the current size.
                currentEntry = entry;
                return entry;
            }

            public void remove()
            {
                iter.remove();
                liveDataSize.addAndGet(-currentEntry.getValue().dataSize());
                currentEntry = null;
            }
        };
    }

    public ColumnFamily getColumnFamily(DecoratedKey key)
    {
        return rows.get(key);
    }

    public SSTableReader flush()
    {
        long estimatedSize = estimatedSize();
        Directories.DataDirectory dataDirectory = cfs.directories.getWriteableLocation(estimatedSize);
        File sstableDirectory = cfs.directories.getLocationForDisk(dataDirectory);
        assert sstableDirectory != null : "Flush task is not bound to any disk";
        return writeSortedContents(sstableDirectory);
    }

    public long getMinTimestamp()
    {
        return minTimestamp;
    }

    private long estimatedSize()
    {
        long keySize = 0;
        for (RowPosition key : rows.keySet())
        {
            //  make sure we don't write non-sensical keys
            assert key instanceof DecoratedKey;
            keySize += ((DecoratedKey)key).getKey().remaining();
        }
        return (long) ((keySize // index entries
                        + keySize // keys in data file
                        + liveDataSize.get()) // data
                       * 1.2); // bloom filter and row index overhead
    }

    private SSTableReader writeSortedContents(File sstableDirectory)
    {
        logger.info("Writing {}", Memtable.this.toString());

        SSTableReader ssTable;
        // errors when creating the writer that may leave empty temp files.
        try (SSTableWriter writer = createFlushWriter(cfs.getTempSSTablePath(sstableDirectory)))
        {
            boolean trackContention = logger.isTraceEnabled();
            int heavilyContendedRowCount = 0;
            // (we can't clear out the map as-we-go to free up memory,
            //  since the memtable is being used for queries in the "pending flush" category)
            for (Map.Entry<RowPosition, AtomicBTreeColumns> entry : rows.entrySet())
            {
                AtomicBTreeColumns cf = entry.getValue();

                if (cf.isMarkedForDelete() && cf.hasColumns())
                {
                    // When every node is up, there's no reason to write batchlog data out to sstables
                    // (which in turn incurs cost like compaction) since the BL write + delete cancel each other out,
                    // and BL data is strictly local, so we don't need to preserve tombstones for repair.
                    // If we have a data row + row level tombstone, then writing it is effectively an expensive no-op so we skip it.
                    // See CASSANDRA-4667.
                    if (cfs.name.equals(SystemKeyspace.BATCHLOG) && cfs.keyspace.getName().equals(SystemKeyspace.NAME))
                        continue;
                }

                if (trackContention && cf.usePessimisticLocking())
                    heavilyContendedRowCount++;

                if (!cf.isEmpty())
                    writer.append((DecoratedKey)entry.getKey(), cf);
            }

            if (writer.getFilePointer() > 0)
            {
                logger.debug(String.format("Completed flushing %s (%s) for commitlog position %s",
                                           writer.getFilename(),
                                           FBUtilities.prettyPrintMemory(writer.getOnDiskFilePointer()),
                                           commitLogUpperBound));

                // temp sstables should contain non-repaired data.
                ssTable = writer.finish(true);
            }
            else
            {
                logger.debug("Completed flushing {}; nothing needed to be retained.  Commitlog position was {}",
                             writer.getFilename(), commitLogUpperBound);
                writer.abort();
                ssTable = null;
            }

            if (heavilyContendedRowCount > 0)
                logger.trace(String.format("High update contention in %d/%d partitions of %s ", heavilyContendedRowCount, rows.size(), Memtable.this.toString()));

            return ssTable;
        }
    }

    private SSTableWriter createFlushWriter(String filename)
    {
        MetadataCollector sstableMetadataCollector = new MetadataCollector(cfs.metadata.comparator)
                                                     .commitLogLowerBound(commitLogLowerBound.get())
                                                     .commitLogUpperBound(commitLogUpperBound.get());
        return SSTableWriter.create(Descriptor.fromFilename(filename), (long) rows.size(), ActiveRepairService.UNREPAIRED_SSTABLE, cfs.metadata, cfs.partitioner, sstableMetadataCollector);
    }

    private static int estimateRowOverhead(final int count)
    {
        // calculate row overhead
        try (final OpOrder.Group group = new OpOrder().start())
        {
            int rowOverhead;
            MemtableAllocator allocator = MEMORY_POOL.newAllocator();
            ConcurrentNavigableMap<RowPosition, Object> rows = new ConcurrentSkipListMap<>();
            final Object val = new Object();
            for (int i = 0; i < count; i++)
                rows.put(allocator.clone(new BufferDecoratedKey(new LongToken(i), ByteBufferUtil.EMPTY_BYTE_BUFFER), group), val);
            double avgSize = ObjectSizes.measureDeep(rows) / (double) count;
            rowOverhead = (int) ((avgSize - Math.floor(avgSize)) < 0.05 ? Math.floor(avgSize) : Math.ceil(avgSize));
            rowOverhead -= ObjectSizes.measureDeep(new LongToken(0));
            rowOverhead += AtomicBTreeColumns.EMPTY_SIZE;
            allocator.setDiscarding();
            allocator.setDiscarded();
            return rowOverhead;
        }
    }
}
