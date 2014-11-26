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
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.base.Function;
import com.google.common.base.Throwables;
import org.apache.cassandra.utils.*;
import org.cliffc.high_scale_lib.NonBlockingHashSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.JMXEnabledThreadPoolExecutor;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.commitlog.ReplayPosition;
import org.apache.cassandra.db.index.SecondaryIndexManager;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.sstable.SSTableMetadata;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.sstable.SSTableWriter;
import org.apache.cassandra.io.util.DiskAwareRunnable;
import org.github.jamm.MemoryMeter;

public class Memtable
{
    private static final Logger logger = LoggerFactory.getLogger(Memtable.class);

    // size in memory can never be less than serialized size.
    private static final double MIN_SANE_LIVE_RATIO = 1.0;
    // max liveratio seen w/ 1-byte columns on a 64-bit jvm was 19. If it gets higher than 64 something is probably broken.
    private static final double MAX_SANE_LIVE_RATIO = 64.0;
    // reasonable initial live ratio used until we compute one.
    private static final double INITIAL_LIVE_RATIO = 10.0;

    // ratio of in-memory memtable size, to serialized size
    private volatile double liveRatio = INITIAL_LIVE_RATIO;
    // ops count last time we computed liveRatio
    private final AtomicLong liveRatioComputedAt = new AtomicLong(32);

    /*
     * switchMemtable puts Memtable.getSortedContents on the writer executor.  When the write is complete,
     * we turn the writer into an SSTableReader and add it to ssTables where it is available for reads.
     *
     * There are two other things that switchMemtable does.
     * First, it puts the Memtable into memtablesPendingFlush, where it stays until the flush is complete
     * and it's been added as an SSTableReader to ssTables_.  Second, it adds an entry to commitLogUpdater
     * that waits for the flush to complete, then calls onMemtableFlush.  This allows multiple flushes
     * to happen simultaneously on multicore systems, while still calling onMF in the correct order,
     * which is necessary for replay in case of a restart since CommitLog assumes that when onMF is
     * called, all data up to the given context has been persisted to SSTables.
     */
    private static final ExecutorService flushWriter
            = new JMXEnabledThreadPoolExecutor(DatabaseDescriptor.getFlushWriters(),
                                               StageManager.KEEPALIVE,
                                               TimeUnit.SECONDS,
                                               new LinkedBlockingQueue<Runnable>(DatabaseDescriptor.getFlushQueueSize()),
                                               new NamedThreadFactory("FlushWriter"),
                                               "internal");

    // We need to take steps to avoid retaining inactive membtables in memory, because counting is slow (can be
    // minutes, for a large memtable and a busy server).  A strictly FIFO Memtable queue could keep memtables
    // alive waiting for metering after they're flushed and would otherwise be GC'd.  Instead, the approach we take
    // is to enqueue the CFS instead of the memtable, and to meter whatever the active memtable is when the executor
    // starts to work on it.  We use a Set to make sure we don't enqueue redundant tasks for the same CFS.
    private static final Set<ColumnFamilyStore> meteringInProgress = new NonBlockingHashSet<ColumnFamilyStore>();
    private static final ExecutorService meterExecutor = new JMXEnabledThreadPoolExecutor(1,
                                                                                          Integer.MAX_VALUE,
                                                                                          TimeUnit.MILLISECONDS,
                                                                                          new LinkedBlockingQueue<Runnable>(),
                                                                                          new NamedThreadFactory("MemoryMeter"),
                                                                                          "internal");
    private final MemoryMeter meter;

    volatile static ColumnFamilyStore activelyMeasuring;

    private final AtomicLong currentSize = new AtomicLong(0);
    private final AtomicLong currentOperations = new AtomicLong(0);

    // We index the memtable by RowPosition only for the purpose of being able
    // to select key range using Token.KeyBound. However put() ensures that we
    // actually only store DecoratedKey.
    private final ConcurrentNavigableMap<RowPosition, AtomicSortedColumns> rows = new ConcurrentSkipListMap<RowPosition, AtomicSortedColumns>();
    public final ColumnFamilyStore cfs;
    private final long creationTime = System.currentTimeMillis();
    private final long creationNano = System.nanoTime();

    private final Allocator allocator = DatabaseDescriptor.getMemtableAllocator();
    // We really only need one column by allocator but one by memtable is not a big waste and avoids needing allocators to know about CFS
    private final Function<Column, Column> localCopyFunction = new Function<Column, Column>()
    {
        public Column apply(Column c)
        {
            return c.localCopy(cfs, allocator);
        }
    };

    // Record the comparator of the CFS at the creation of the memtable. This
    // is only used when a user update the CF comparator, to know if the
    // memtable was created with the new or old comparator.
    public final AbstractType initialComparator;

    public Memtable(ColumnFamilyStore cfs)
    {
        this.cfs = cfs;
        this.initialComparator = cfs.metadata.comparator;
        this.cfs.scheduleFlush();

        Callable<Set<Object>> provider = new Callable<Set<Object>>()
        {
            public Set<Object> call() throws Exception
            {
                // avoid counting this once for each row
                Set<Object> set = Collections.newSetFromMap(new IdentityHashMap<Object, Boolean>());
                set.add(Memtable.this.cfs.metadata);
                return set;
            }
        };
        meter = new MemoryMeter().omitSharedBufferOverhead().withTrackerProvider(provider);
    }

    public long getLiveSize()
    {
        long estimatedSize = (long) (currentSize.get() * liveRatio);

        // liveRatio is just an estimate; we can get a lower bound directly from the allocator
        if (estimatedSize < allocator.getMinimumSize())
            return allocator.getMinimumSize();

        return estimatedSize;
    }

    public long getOperations()
    {
        return currentOperations.get();
    }

    /**
     * Should only be called by ColumnFamilyStore.apply.  NOT a public API.
     * (CFS handles locking to avoid submitting an op
     *  to a flushing memtable.  Any other way is unsafe.)
    */
    long put(DecoratedKey key, ColumnFamily columnFamily, SecondaryIndexManager.Updater indexer)
    {
        return resolve(key, columnFamily, indexer);
    }

    public void maybeUpdateLiveRatio()
    {
        // recompute liveRatio, if we have increased the number of ops 10x since last calculated
        while (true)
        {
            long last = liveRatioComputedAt.get();
            long operations = currentOperations.get();
            if (operations <= 10L * last)
                break;
            if (liveRatioComputedAt.compareAndSet(last, operations))
            {
                logger.debug("computing liveRatio of {} at {} ops", this, operations);
                updateLiveRatio();
            }
        }
    }

    public void updateLiveRatio() throws RuntimeException
    {
        if (!MemoryMeter.isInitialized())
        {
            // hack for openjdk.  we log a warning about this in the startup script too.
            logger.error("MemoryMeter uninitialized (jamm not specified as java agent); assuming liveRatio of {}.  "
                         + " Usually this means cassandra-env.sh disabled jamm because you are using a buggy JRE; "
                         + " upgrade to the Sun JRE instead", liveRatio);
            return;
        }

        if (!meteringInProgress.add(cfs))
        {
            logger.debug("Metering already pending or active for {}; skipping liveRatio update", cfs);
            return;
        }

        meterExecutor.submit(new MeteringRunnable(cfs));
    }

    private long resolve(DecoratedKey key, ColumnFamily cf, SecondaryIndexManager.Updater indexer)
    {
        AtomicSortedColumns previous = rows.get(key);

        if (previous == null)
        {
            AtomicSortedColumns empty = cf.cloneMeShallow(AtomicSortedColumns.factory, false);
            // We'll add the columns later. This avoids wasting works if we get beaten in the putIfAbsent
            previous = rows.putIfAbsent(new DecoratedKey(key.token, allocator.clone(key.key)), empty);
            if (previous == null)
                previous = empty;
        }

        final Pair<Long, Long> pair = previous.addAllWithSizeDelta(cf, allocator, localCopyFunction, indexer);
        currentSize.addAndGet(pair.left);
        currentOperations.addAndGet(cf.getColumnCount() + (cf.isMarkedForDelete() ? 1 : 0) + cf.deletionInfo().rangeCount());
        return pair.right;
    }

    // for debugging
    public String contents()
    {
        StringBuilder builder = new StringBuilder();
        builder.append("{");
        for (Map.Entry<RowPosition, AtomicSortedColumns> entry : rows.entrySet())
        {
            builder.append(entry.getKey()).append(": ").append(entry.getValue()).append(", ");
        }
        builder.append("}");
        return builder.toString();
    }

    public void flushAndSignal(final CountDownLatch latch, final Future<ReplayPosition> context)
    {
        flushWriter.execute(new FlushRunnable(latch, context));
    }

    public String toString()
    {
        return String.format("Memtable-%s@%s(%s/%s serialized/live bytes, %s ops)",
                             cfs.name, hashCode(), currentSize, getLiveSize(), currentOperations);
    }

    /**
     * @param startWith Include data in the result from and including this key and to the end of the memtable
     * @return An iterator of entries with the data from the start key
     */
    public Iterator<Map.Entry<DecoratedKey, AtomicSortedColumns>> getEntryIterator(final RowPosition startWith, final RowPosition stopAt)
    {
        return new Iterator<Map.Entry<DecoratedKey, AtomicSortedColumns>>()
        {
            private Iterator<Map.Entry<RowPosition, AtomicSortedColumns>> iter = stopAt.isMinimum(cfs.partitioner)
                                                                               ? rows.tailMap(startWith).entrySet().iterator()
                                                                               : rows.subMap(startWith, true, stopAt, true).entrySet().iterator();
            private Map.Entry<RowPosition, AtomicSortedColumns> currentEntry;

            public boolean hasNext()
            {
                return iter.hasNext();
            }

            public Map.Entry<DecoratedKey, AtomicSortedColumns> next()
            {
                Map.Entry<RowPosition, AtomicSortedColumns> entry = iter.next();
                // Store the reference to the current entry so that remove() can update the current size.
                currentEntry = entry;
                // Actual stored key should be true DecoratedKey
                assert entry.getKey() instanceof DecoratedKey;
                // Object cast is required since otherwise we can't turn RowPosition into DecoratedKey
                return (Map.Entry<DecoratedKey, AtomicSortedColumns>) (Object)entry;
            }

            public void remove()
            {
                iter.remove();
                currentSize.addAndGet(-currentEntry.getValue().dataSize());
                currentEntry = null;
            }
        };
    }

    public boolean isClean()
    {
        return rows.isEmpty();
    }

    /**
     * @return true if this memtable is expired. Expiration time is determined by CF's memtable_flush_period_in_ms.
     */
    public boolean isExpired()
    {
        int period = cfs.metadata.getMemtableFlushPeriod();
        return period > 0 && (System.nanoTime() - creationNano >= TimeUnit.MILLISECONDS.toNanos(period));
    }

    public ColumnFamily getColumnFamily(DecoratedKey key)
    {
        return rows.get(key);
    }

    public long creationTime()
    {
        return creationTime;
    }

    class FlushRunnable extends DiskAwareRunnable
    {
        private final CountDownLatch latch;
        private final Future<ReplayPosition> context;
        private final long estimatedSize;

        FlushRunnable(CountDownLatch latch, Future<ReplayPosition> context)
        {
            this.latch = latch;
            this.context = context;

            long keySize = 0;
            for (RowPosition key : rows.keySet())
            {
                //  make sure we don't write non-sensical keys
                assert key instanceof DecoratedKey;
                keySize += ((DecoratedKey)key).key.remaining();
            }
            estimatedSize = (long) ((keySize // index entries
                                    + keySize // keys in data file
                                    + currentSize.get()) // data
                                    * 1.2); // bloom filter and row index overhead
        }

        public long getExpectedWriteSize()
        {
            return estimatedSize;
        }

        protected void runMayThrow() throws Exception
        {
            long writeSize = getExpectedWriteSize();
            Directories.DataDirectory dataDirectory = getWriteDirectory(writeSize);
            File sstableDirectory = cfs.directories.getLocationForDisk(dataDirectory);
            assert sstableDirectory != null : "Flush task is not bound to any disk";
            SSTableReader sstable = writeSortedContents(context, sstableDirectory);
            cfs.replaceFlushed(Memtable.this, sstable);
            latch.countDown();
        }

        protected Directories getDirectories()
        {
            return cfs.directories;
        }

        private SSTableReader writeSortedContents(Future<ReplayPosition> context, File sstableDirectory)
        throws ExecutionException, InterruptedException
        {
            logger.info("Writing " + Memtable.this.toString());

            SSTableReader ssTable;
            // errors when creating the writer that may leave empty temp files.
            SSTableWriter writer = createFlushWriter(cfs.getTempSSTablePath(sstableDirectory));
            try
            {
                // (we can't clear out the map as-we-go to free up memory,
                //  since the memtable is being used for queries in the "pending flush" category)
                for (Map.Entry<RowPosition, AtomicSortedColumns> entry : rows.entrySet())
                {
                    ColumnFamily cf = entry.getValue();
                    if (cf.isMarkedForDelete())
                    {
                        // When every node is up, there's no reason to write batchlog data out to sstables
                        // (which in turn incurs cost like compaction) since the BL write + delete cancel each other out,
                        // and BL data is strictly local, so we don't need to preserve tombstones for repair.
                        // If we have a data row + row level tombstone, then writing it is effectively an expensive no-op so we skip it.
                        // See CASSANDRA-4667.
                        if (cfs.name.equals(SystemKeyspace.BATCHLOG_CF) && cfs.keyspace.getName().equals(Keyspace.SYSTEM_KS) && !(cf.getColumnCount() == 0))
                            continue;

                        // Pedantically, you could purge column level tombstones that are past GcGRace when writing to the SSTable.
                        // But it can result in unexpected behaviour where deletes never make it to disk,
                        // as they are lost and so cannot override existing column values. So we only remove deleted columns if there
                        // is a CF level tombstone to ensure the delete makes it into an SSTable.
                        // We also shouldn't be dropping any columns obsoleted by partition and/or range tombstones in case
                        // the table has secondary indexes, or else the stale entries wouldn't be cleaned up during compaction,
                        // and will only be dropped during 2i query read-repair, if at all.
                        if (!cfs.indexManager.hasIndexes())
                            currentSize.addAndGet(-ColumnFamilyStore.removeDeletedColumnsOnly(cf, Integer.MIN_VALUE));
                    }

                    if (cf.getColumnCount() > 0 || cf.isMarkedForDelete())
                        writer.append((DecoratedKey)entry.getKey(), cf);
                }

                if (writer.getFilePointer() > 0)
                {
                    ssTable = writer.closeAndOpenReader();
                    logger.info(String.format("Completed flushing %s (%d bytes) for commitlog position %s",
                                              ssTable.getFilename(), new File(ssTable.getFilename()).length(), context.get()));
                }
                else
                {
                    writer.abort();
                    ssTable = null;
                    logger.info("Completed flushing; nothing needed to be retained.  Commitlog position was {}",
                                context.get());
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
            SSTableMetadata.Collector sstableMetadataCollector = SSTableMetadata.createCollector(cfs.metadata.comparator).replayPosition(context.get());
            return new SSTableWriter(filename,
                                     rows.size(),
                                     cfs.metadata,
                                     cfs.partitioner,
                                     sstableMetadataCollector);
        }
    }

    private static class MeteringRunnable implements Runnable
    {
        // we might need to wait in the meter queue for a while.  measure whichever memtable is active at that point,
        // rather than keeping the original memtable referenced (and thus un-freeable) until this runs.
        private final ColumnFamilyStore cfs;

        public MeteringRunnable(ColumnFamilyStore cfs)
        {
            this.cfs = cfs;
        }

        public void run()
        {
            try
            {
                activelyMeasuring = cfs;
                Memtable memtable = cfs.getMemtableThreadSafe();

                long start = System.nanoTime();
                // ConcurrentSkipListMap has cycles, so measureDeep will have to track a reference to EACH object it visits.
                // So to reduce the memory overhead of doing a measurement, we break it up to row-at-a-time.
                long deepSize = memtable.meter.measure(memtable.rows);
                int objects = 0;
                for (Map.Entry<RowPosition, AtomicSortedColumns> entry : memtable.rows.entrySet())
                {
                    deepSize += memtable.meter.measureDeep(entry.getKey()) + memtable.meter.measureDeep(entry.getValue());
                    objects += entry.getValue().getColumnCount();

                    if (memtable != cfs.getMemtableThreadSafe())
                    {
                        // this memtable is flushing or already flushed
                        return;
                    }
                }
                double newRatio = (double) deepSize / memtable.currentSize.get();

                if (newRatio < MIN_SANE_LIVE_RATIO)
                {
                    logger.debug("setting live ratio to minimum of {} instead of {}", MIN_SANE_LIVE_RATIO, newRatio);
                    newRatio = MIN_SANE_LIVE_RATIO;
                }
                if (newRatio > MAX_SANE_LIVE_RATIO)
                {
                    logger.debug("setting live ratio to maximum of {} instead of {}", MAX_SANE_LIVE_RATIO, newRatio);
                    newRatio = MAX_SANE_LIVE_RATIO;
                }

                // we want to be very conservative about our estimate, since the penalty for guessing low is OOM
                // death. thus, higher estimates are believed immediately; lower ones are averaged w/ the old
                if (newRatio > memtable.liveRatio)
                    memtable.liveRatio = newRatio;
                else
                    memtable.liveRatio = (memtable.liveRatio + newRatio) / 2.0;

                logger.debug("{} liveRatio is {} (just-counted was {}).  calculation took {}ms for {} cells",
                             cfs, memtable.liveRatio, newRatio, TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start), objects);
            }
            finally
            {
                activelyMeasuring = null;
                meteringInProgress.remove(cfs);
            }
        }
    }
}
