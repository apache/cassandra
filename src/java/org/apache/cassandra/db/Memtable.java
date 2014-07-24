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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.commitlog.ReplayPosition;
import org.apache.cassandra.db.composites.CellNameType;
import org.apache.cassandra.db.index.SecondaryIndexManager;
import org.apache.cassandra.dht.LongToken;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.sstable.SSTableWriter;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.io.util.DiskAwareRunnable;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.memory.*;

public class Memtable
{
    private static final Logger logger = LoggerFactory.getLogger(Memtable.class);

    static final MemtablePool MEMORY_POOL = DatabaseDescriptor.getMemtableAllocatorPool();
    private static final int ROW_OVERHEAD_HEAP_SIZE = estimateRowOverhead(Integer.valueOf(System.getProperty("cassandra.memtable_row_overhead_computation_step", "100000")));

    private final MemtableAllocator allocator;
    private final AtomicLong liveDataSize = new AtomicLong(0);
    private final AtomicLong currentOperations = new AtomicLong(0);

    // the write barrier for directing writes to this memtable during a switch
    private volatile OpOrder.Barrier writeBarrier;
    // the last ReplayPosition owned by this Memtable; all ReplayPositions lower are owned by this or an earlier Memtable
    private final AtomicReference<ReplayPosition> lastReplayPosition = new AtomicReference<>();
    // the "first" ReplayPosition owned by this Memtable; this is inaccurate, and only used as a convenience to prevent CLSM flushing wantonly
    private final ReplayPosition minReplayPosition = CommitLog.instance.getContext();

    // We index the memtable by RowPosition only for the purpose of being able
    // to select key range using Token.KeyBound. However put() ensures that we
    // actually only store DecoratedKey.
    private final ConcurrentNavigableMap<RowPosition, AtomicBTreeColumns> rows = new ConcurrentSkipListMap<>();
    public final ColumnFamilyStore cfs;
    private final long creationTime = System.currentTimeMillis();
    private final long creationNano = System.nanoTime();

    // Record the comparator of the CFS at the creation of the memtable. This
    // is only used when a user update the CF comparator, to know if the
    // memtable was created with the new or old comparator.
    public final CellNameType initialComparator;

    public Memtable(ColumnFamilyStore cfs)
    {
        this.cfs = cfs;
        this.allocator = MEMORY_POOL.newAllocator();
        this.initialComparator = cfs.metadata.comparator;
        this.cfs.scheduleFlush();
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

    void setDiscarding(OpOrder.Barrier writeBarrier, ReplayPosition minLastReplayPosition)
    {
        assert this.writeBarrier == null;
        this.lastReplayPosition.set(minLastReplayPosition);
        this.writeBarrier = writeBarrier;
        allocator.setDiscarding();
    }

    void setDiscarded()
    {
        allocator.setDiscarded();
    }

    public boolean accepts(OpOrder.Group opGroup)
    {
        OpOrder.Barrier barrier = this.writeBarrier;
        return barrier == null || barrier.isAfter(opGroup);
    }

    public boolean isLive()
    {
        return allocator.isLive();
    }

    public boolean isClean()
    {
        return rows.isEmpty();
    }

    public boolean isCleanAfter(ReplayPosition position)
    {
        return isClean() || (position != null && minReplayPosition.compareTo(position) >= 0);
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
    void put(DecoratedKey key, ColumnFamily cf, SecondaryIndexManager.Updater indexer, OpOrder.Group opGroup, ReplayPosition replayPosition)
    {
        if (replayPosition != null && writeBarrier != null)
        {
            // if the writeBarrier is set, we want to maintain lastReplayPosition; this is an optimisation to avoid
            // casing it for every write, but still ensure it is correct when writeBarrier.await() completes.
            while (true)
            {
                ReplayPosition last = lastReplayPosition.get();
                if (last.compareTo(replayPosition) >= 0)
                    break;
                if (lastReplayPosition.compareAndSet(last, replayPosition))
                    break;
            }
        }

        AtomicBTreeColumns previous = rows.get(key);

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
                int overhead = (int) (cfs.partitioner.getHeapSizeOf(key.getToken()) + ROW_OVERHEAD_HEAP_SIZE);
                allocator.onHeap().allocate(overhead, opGroup);
            }
            else
            {
                allocator.reclaimer().reclaimImmediately(cloneKey);
            }
        }

        liveDataSize.addAndGet(previous.addAllWithSizeDelta(cf, allocator, opGroup, indexer));
        currentOperations.addAndGet(cf.getColumnCount() + (cf.isMarkedForDelete() ? 1 : 0) + cf.deletionInfo().rangeCount());
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

    public FlushRunnable flushRunnable()
    {
        return new FlushRunnable(lastReplayPosition.get());
    }

    public String toString()
    {
        return String.format("Memtable-%s@%s(%s serialized bytes, %s ops, %.0f%%/%.0f%% of on/off-heap limit)",
                             cfs.name, hashCode(), liveDataSize, currentOperations, 100 * allocator.onHeap().ownershipRatio(), 100 * allocator.offHeap().ownershipRatio());
    }

    /**
     * @param startWith Include data in the result from and including this key and to the end of the memtable
     * @return An iterator of entries with the data from the start key
     */
    public Iterator<Map.Entry<DecoratedKey, ColumnFamily>> getEntryIterator(final RowPosition startWith, final RowPosition stopAt)
    {
        return new Iterator<Map.Entry<DecoratedKey, ColumnFamily>>()
        {
            private Iterator<? extends Map.Entry<? extends RowPosition, AtomicBTreeColumns>> iter = stopAt.isMinimum(cfs.partitioner)
                    ? rows.tailMap(startWith).entrySet().iterator()
                    : rows.subMap(startWith, true, stopAt, true).entrySet().iterator();

            private Map.Entry<? extends RowPosition, ? extends ColumnFamily> currentEntry;

            public boolean hasNext()
            {
                return iter.hasNext();
            }

            public Map.Entry<DecoratedKey, ColumnFamily> next()
            {
                Map.Entry<? extends RowPosition, ? extends ColumnFamily> entry = iter.next();
                // Actual stored key should be true DecoratedKey
                assert entry.getKey() instanceof DecoratedKey;
                if (MEMORY_POOL.needToCopyOnHeap())
                {
                    DecoratedKey key = (DecoratedKey) entry.getKey();
                    key = new BufferDecoratedKey(key.getToken(), HeapAllocator.instance.clone(key.getKey()));
                    ColumnFamily cells = ArrayBackedSortedColumns.localCopy(entry.getValue(), HeapAllocator.instance);
                    entry = new AbstractMap.SimpleImmutableEntry<>(key, cells);
                }
                // Store the reference to the current entry so that remove() can update the current size.
                currentEntry = entry;
                // Object cast is required since otherwise we can't turn RowPosition into DecoratedKey
                return (Map.Entry<DecoratedKey, ColumnFamily>) entry;
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

    public long creationTime()
    {
        return creationTime;
    }

    public ReplayPosition getLastReplayPosition()
    {
        return lastReplayPosition.get();
    }

    class FlushRunnable extends DiskAwareRunnable
    {
        private final ReplayPosition context;
        private final long estimatedSize;

        FlushRunnable(ReplayPosition context)
        {
            this.context = context;

            long keySize = 0;
            for (RowPosition key : rows.keySet())
            {
                //  make sure we don't write non-sensical keys
                assert key instanceof DecoratedKey;
                keySize += ((DecoratedKey)key).getKey().remaining();
            }
            estimatedSize = (long) ((keySize // index entries
                                    + keySize // keys in data file
                                    + liveDataSize.get()) // data
                                    * 1.2); // bloom filter and row index overhead
        }

        public long getExpectedWriteSize()
        {
            return estimatedSize;
        }

        protected void runWith(File sstableDirectory) throws Exception
        {
            assert sstableDirectory != null : "Flush task is not bound to any disk";

            SSTableReader sstable = writeSortedContents(context, sstableDirectory);
            cfs.replaceFlushed(Memtable.this, sstable);
        }

        protected Directories getDirectories()
        {
            return cfs.directories;
        }

        private SSTableReader writeSortedContents(ReplayPosition context, File sstableDirectory)
        throws ExecutionException, InterruptedException
        {
            logger.info("Writing {}", Memtable.this.toString());

            SSTableReader ssTable;
            // errors when creating the writer that may leave empty temp files.
            SSTableWriter writer = createFlushWriter(cfs.getTempSSTablePath(sstableDirectory));
            try
            {
                // (we can't clear out the map as-we-go to free up memory,
                //  since the memtable is being used for queries in the "pending flush" category)
                for (Map.Entry<RowPosition, AtomicBTreeColumns> entry : rows.entrySet())
                {
                    ColumnFamily cf = entry.getValue();

                    if (cf.isMarkedForDelete() && cf.hasColumns())
                    {
                        // When every node is up, there's no reason to write batchlog data out to sstables
                        // (which in turn incurs cost like compaction) since the BL write + delete cancel each other out,
                        // and BL data is strictly local, so we don't need to preserve tombstones for repair.
                        // If we have a data row + row level tombstone, then writing it is effectively an expensive no-op so we skip it.
                        // See CASSANDRA-4667.
                        if (cfs.name.equals(SystemKeyspace.BATCHLOG_CF) && cfs.keyspace.getName().equals(Keyspace.SYSTEM_KS))
                            continue;
                    }

                    if (!cf.isEmpty())
                        writer.append((DecoratedKey)entry.getKey(), cf);
                }

                if (writer.getFilePointer() > 0)
                {
                    writer.isolateReferences();

                    // temp sstables should contain non-repaired data.
                    ssTable = writer.closeAndOpenReader();
                    logger.info(String.format("Completed flushing %s (%d bytes) for commitlog position %s",
                                              ssTable.getFilename(), new File(ssTable.getFilename()).length(), context));
                }
                else
                {
                    writer.abort();
                    ssTable = null;
                    logger.info("Completed flushing; nothing needed to be retained.  Commitlog position was {}",
                                context);
                }

                return ssTable;
            }
            catch (Throwable e)
            {
                writer.abort();
                throw Throwables.propagate(e);
            }
        }

        public SSTableWriter createFlushWriter(String filename) throws ExecutionException, InterruptedException
        {
            MetadataCollector sstableMetadataCollector = new MetadataCollector(cfs.metadata.comparator).replayPosition(context);
            return new SSTableWriter(filename,
                                     rows.size(),
                                     ActiveRepairService.UNREPAIRED_SSTABLE,
                                     cfs.metadata,
                                     cfs.partitioner,
                                     sstableMetadataCollector);
        }
    }

    private static int estimateRowOverhead(final int count)
    {
        // calculate row overhead
        final OpOrder.Group group = new OpOrder().start();
        int rowOverhead;
        MemtableAllocator allocator = MEMORY_POOL.newAllocator();
        ConcurrentNavigableMap<RowPosition, Object> rows = new ConcurrentSkipListMap<>();
        final Object val = new Object();
        for (int i = 0 ; i < count ; i++)
            rows.put(allocator.clone(new BufferDecoratedKey(new LongToken((long) i), ByteBufferUtil.EMPTY_BYTE_BUFFER), group), val);
        double avgSize = ObjectSizes.measureDeep(rows) / (double) count;
        rowOverhead = (int) ((avgSize - Math.floor(avgSize)) < 0.05 ? Math.floor(avgSize) : Math.ceil(avgSize));
        rowOverhead -= ObjectSizes.measureDeep(new LongToken((long) 0));
        rowOverhead += AtomicBTreeColumns.EMPTY_SIZE;
        allocator.setDiscarding();
        allocator.setDiscarded();
        return rowOverhead;
    }
}
