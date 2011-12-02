/**
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
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.DebuggableThreadPoolExecutor;
import org.apache.cassandra.db.columniterator.IColumnIterator;
import org.apache.cassandra.db.columniterator.SimpleAbstractColumnIterator;
import org.apache.cassandra.db.commitlog.ReplayPosition;
import org.apache.cassandra.db.filter.AbstractColumnIterator;
import org.apache.cassandra.db.filter.NamesQueryFilter;
import org.apache.cassandra.db.filter.SliceQueryFilter;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.sstable.SSTableWriter;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.SlabAllocator;
import org.apache.cassandra.utils.WrappedRunnable;
import org.github.jamm.MemoryMeter;

public class Memtable
{
    private static final Logger logger = LoggerFactory.getLogger(Memtable.class);

    // size in memory can never be less than serialized size
    private static final double MIN_SANE_LIVE_RATIO = 1.0;
    // max liveratio seen w/ 1-byte columns on a 64-bit jvm was 19. If it gets higher than 64 something is probably broken.
    private static final double MAX_SANE_LIVE_RATIO = 64.0;

    // we're careful to only allow one count to run at a time because counting is slow
    // (can be minutes, for a large memtable and a busy server), so we could keep memtables
    // alive after they're flushed and would otherwise be GC'd.
    private static final ExecutorService meterExecutor = new ThreadPoolExecutor(1, 1, Integer.MAX_VALUE, TimeUnit.MILLISECONDS, new SynchronousQueue<Runnable>())
    {
        @Override
        protected void afterExecute(Runnable r, Throwable t)
        {
            super.afterExecute(r, t);
            DebuggableThreadPoolExecutor.logExceptionsAfterExecute(r, t);
        }
    };

    private final MemoryMeter meter;

    volatile static Memtable activelyMeasuring;

    private volatile boolean isFrozen;
    private final AtomicLong currentThroughput = new AtomicLong(0);
    private final AtomicLong currentOperations = new AtomicLong(0);

    private final ConcurrentNavigableMap<DecoratedKey, ColumnFamily> columnFamilies = new ConcurrentSkipListMap<DecoratedKey, ColumnFamily>();
    public final ColumnFamilyStore cfs;
    private final long creationTime;

    private SlabAllocator allocator = new SlabAllocator();

    public Memtable(ColumnFamilyStore cfs)
    {
        this.cfs = cfs;
        this.creationTime = System.currentTimeMillis();

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
        // 25% fudge factor on the base throughput * liveRatio calculation.  (Based on observed
        // pre-slabbing behavior -- not sure what accounts for this. May have changed with introduction of slabbing.)
        return (long) (currentThroughput.get() * cfs.liveRatio * 1.25);
    }

    public long getSerializedSize()
    {
        return currentThroughput.get();
    }

    public long getOperations()
    {
        return currentOperations.get();
    }

    boolean isFrozen()
    {
        return isFrozen;
    }

    void freeze()
    {
        isFrozen = true;
    }

    /**
     * Should only be called by ColumnFamilyStore.apply.  NOT a public API.
     * (CFS handles locking to avoid submitting an op
     *  to a flushing memtable.  Any other way is unsafe.)
    */
    void put(DecoratedKey key, ColumnFamily columnFamily)
    {
        assert !isFrozen; // not 100% foolproof but hell, it's an assert
        resolve(key, columnFamily);
    }

    public void updateLiveRatio()
    {
        if (!MemoryMeter.isInitialized())
        {
            // hack for openjdk.  we log a warning about this in the startup script too.
            logger.warn("MemoryMeter uninitialized (jamm not specified as java agent); assuming liveRatio of 10.0.  Usually this means cassandra-env.sh disabled jamm because you are using a buggy JRE; upgrade to the Sun JRE instead");
            cfs.liveRatio = 10.0;
            return;
        }

        Runnable runnable = new Runnable()
        {
            public void run()
            {
                activelyMeasuring = Memtable.this;

                long start = System.currentTimeMillis();
                // ConcurrentSkipListMap has cycles, so measureDeep will have to track a reference to EACH object it visits.
                // So to reduce the memory overhead of doing a measurement, we break it up to row-at-a-time.
                long deepSize = meter.measure(columnFamilies);
                int objects = 0;
                for (Map.Entry<DecoratedKey, ColumnFamily> entry : columnFamilies.entrySet())
                {
                    deepSize += meter.measureDeep(entry.getKey()) + meter.measureDeep(entry.getValue());
                    objects += entry.getValue().getColumnCount();
                }
                double newRatio = (double) deepSize / currentThroughput.get();

                if (newRatio < MIN_SANE_LIVE_RATIO)
                {
                    logger.warn("setting live ratio to minimum of 1.0 instead of {}", newRatio);
                    newRatio = MIN_SANE_LIVE_RATIO;
                }
                if (newRatio > MAX_SANE_LIVE_RATIO)
                {
                    logger.warn("setting live ratio to maximum of 64 instead of {}", newRatio);
                    newRatio = MAX_SANE_LIVE_RATIO;
                }
                cfs.liveRatio = Math.max(cfs.liveRatio, newRatio);

                logger.info("{} liveRatio is {} (just-counted was {}).  calculation took {}ms for {} columns",
                            new Object[]{ cfs, cfs.liveRatio, newRatio, System.currentTimeMillis() - start, objects });
                activelyMeasuring = null;
            }
        };

        try
        {
            meterExecutor.submit(runnable);
        }
        catch (RejectedExecutionException e)
        {
            logger.debug("Meter thread is busy; skipping liveRatio update for {}", cfs);
        }
    }

    private void resolve(DecoratedKey key, ColumnFamily cf)
    {
        currentThroughput.addAndGet(cf.size());
        currentOperations.addAndGet((cf.getColumnCount() == 0)
                                    ? cf.isMarkedForDelete() ? 1 : 0
                                    : cf.getColumnCount());

        ColumnFamily clonedCf = columnFamilies.get(key);
        // if the row doesn't exist yet in the memtable, clone cf to our allocator.
        if (clonedCf == null)
        {
            clonedCf = cf.cloneMeShallow();
            for (IColumn column : cf.getSortedColumns())
                clonedCf.addColumn(column.localCopy(cfs, allocator));
            clonedCf = columnFamilies.putIfAbsent(new DecoratedKey(key.token, allocator.clone(key.key)), clonedCf);
            if (clonedCf == null)
                return;
            // else there was a race and the other thread won.  fall through to updating his CF object
        }

        // we duplicate the funcationality of CF.resolve here to avoid having to either pass the Memtable in for
        // the cloning operation, or cloning the CF container as well as the Columns.  fortunately, resolve
        // is really quite simple:
        clonedCf.delete(cf);
        for (IColumn column : cf.getSortedColumns())
            clonedCf.addColumn(column.localCopy(cfs, allocator), allocator);
    }

    // for debugging
    public String contents()
    {
        StringBuilder builder = new StringBuilder();
        builder.append("{");
        for (Map.Entry<DecoratedKey, ColumnFamily> entry : columnFamilies.entrySet())
        {
            builder.append(entry.getKey()).append(": ").append(entry.getValue()).append(", ");
        }
        builder.append("}");
        return builder.toString();
    }


    private SSTableReader writeSortedContents(ReplayPosition context) throws IOException
    {
        logger.info("Writing " + this);

        long keySize = 0;
        for (DecoratedKey key : columnFamilies.keySet())
            keySize += key.key.remaining();
        long estimatedSize = (long) ((keySize // index entries
                                      + keySize // keys in data file
                                      + currentThroughput.get()) // data
                                     * 1.2); // bloom filter and row index overhead
        SSTableReader ssTable;
        // errors when creating the writer that may leave empty temp files.
        SSTableWriter writer = cfs.createFlushWriter(columnFamilies.size(), estimatedSize, context);
        try
        {
            // (we can't clear out the map as-we-go to free up memory,
            //  since the memtable is being used for queries in the "pending flush" category)
            for (Map.Entry<DecoratedKey, ColumnFamily> entry : columnFamilies.entrySet())
            {
                ColumnFamily cf = entry.getValue();
                if (cf.isMarkedForDelete())
                {
                    // Pedantically, you could purge column level tombstones that are past GcGRace when writing to the SSTable.
                    // But it can result in unexpected behaviour where deletes never make it to disk,
                    // as they are lost and so cannot override existing column values. So we only remove deleted columns if there
                    // is a CF level tombstone to ensure the delete makes it into an SSTable.
                    ColumnFamilyStore.removeDeletedColumnsOnly(cf, Integer.MIN_VALUE);
                }
                writer.append(entry.getKey(), cf);
            }

            ssTable = writer.closeAndOpenReader();
        }
        catch (Exception e)
        {
            writer.abort();
            throw FBUtilities.unchecked(e);
        }
        logger.info(String.format("Completed flushing %s (%d bytes)",
                                  ssTable.getFilename(), new File(ssTable.getFilename()).length()));
        return ssTable;
    }

    public void flushAndSignal(final CountDownLatch latch, ExecutorService writer, final ReplayPosition context)
    {
        writer.execute(new WrappedRunnable()
        {
            public void runMayThrow() throws IOException
            {
                cfs.flushLock.lock();
                try
                {
                    if (!cfs.isDropped())
                    {
                        SSTableReader sstable = writeSortedContents(context);
                        cfs.replaceFlushed(Memtable.this, sstable);
                    }
                }
                finally
                {
                    cfs.flushLock.unlock();
                }
                latch.countDown();
            }
        });
    }

    public String toString()
    {
        return String.format("Memtable-%s@%s(%s/%s serialized/live bytes, %s ops)",
                             cfs.getColumnFamilyName(), hashCode(), currentThroughput, getLiveSize(), currentOperations);
    }

    /**
     * @param startWith Include data in the result from and including this key and to the end of the memtable
     * @return An iterator of entries with the data from the start key 
     */
    public Iterator<Map.Entry<DecoratedKey, ColumnFamily>> getEntryIterator(DecoratedKey startWith)
    {
        return columnFamilies.tailMap(startWith).entrySet().iterator();
    }

    public boolean isClean()
    {
        return columnFamilies.isEmpty();
    }

    /**
     * obtain an iterator of columns in this memtable in the specified order starting from a given column.
     */
    public static IColumnIterator getSliceIterator(final DecoratedKey key, final ColumnFamily cf, SliceQueryFilter filter, AbstractType typeComparator)
    {
        assert cf != null;
        final boolean isSuper = cf.isSuper();
        final Collection<IColumn> filteredColumns = filter.reversed ? cf.getReverseSortedColumns() : cf.getSortedColumns();

        // ok to not have subcolumnComparator since we won't be adding columns to this object
        IColumn startColumn = isSuper ? new SuperColumn(filter.start, (AbstractType)null) :  new Column(filter.start);
        Comparator<IColumn> comparator = filter.getColumnComparator(typeComparator);

        final PeekingIterator<IColumn> filteredIter = Iterators.peekingIterator(filteredColumns.iterator());
        if (!filter.reversed || filter.start.remaining() != 0)
        {
            while (filteredIter.hasNext() && comparator.compare(filteredIter.peek(), startColumn) < 0)
            {
                filteredIter.next();
            }
        }

        return new AbstractColumnIterator()
        {
            public ColumnFamily getColumnFamily()
            {
                return cf;
            }

            public DecoratedKey getKey()
            {
                return key;
            }

            public boolean hasNext()
            {
                return filteredIter.hasNext();
            }

            public IColumn next()
            {
                return filteredIter.next();                
            }
        };
    }

    public static IColumnIterator getNamesIterator(final DecoratedKey key, final ColumnFamily cf, final NamesQueryFilter filter)
    {
        assert cf != null;
        final boolean isStandard = !cf.isSuper();

        return new SimpleAbstractColumnIterator()
        {
            private Iterator<ByteBuffer> iter = filter.columns.iterator();

            public ColumnFamily getColumnFamily()
            {
                return cf;
            }

            public DecoratedKey getKey()
            {
                return key;
            }

            protected IColumn computeNext()
            {
                while (iter.hasNext())
                {
                    ByteBuffer current = iter.next();
                    IColumn column = cf.getColumn(current);
                    if (column != null)
                        // clone supercolumns so caller can freely removeDeleted or otherwise mutate it
                        return isStandard ? column : ((SuperColumn)column).cloneMe();
                }
                return endOfData();
            }
        };
    }

    public ColumnFamily getColumnFamily(DecoratedKey key)
    {
        return columnFamilies.get(key);
    }

    void clearUnsafe()
    {
        columnFamilies.clear();
    }

    public long creationTime()
    {
        return creationTime;
    }
}
