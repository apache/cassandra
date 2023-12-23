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

package org.apache.cassandra.harry.tracker;

import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import org.apache.cassandra.harry.core.Configuration;
import org.apache.cassandra.harry.ddl.SchemaSpec;
import org.apache.cassandra.harry.model.OpSelectors;
import org.apache.cassandra.utils.concurrent.WaitQueue;

/**
 * Locking data tracker, that can be used with a quiescent model checker while providing
 * a high degree of concurrency. It works by isolating readers from writers. In other words,
 * readers can intersect with other readers, and writers can coincide with other writers.
 *
 * We achieve quiescence on a partition level, not on LTS level, and we know for sure
 * which operations have finished for a partition, even if their LTS are non-contiguous.
 *
 * We use a simple wait queue for queuing up waiters, and a compact long counter for
 * tracking the number of concurrent readers and writers. Lower 32 bits hold a number of
 * readers, and higher 32 bits - a number of writers.
 */
public class LockingDataTracker extends DefaultDataTracker
{
    private final Map<Long, ReadersWritersLock> locked = new ConcurrentHashMap<>();

    private final WaitQueue readersQueue = WaitQueue.newWaitQueue();
    private final WaitQueue writersQueue = WaitQueue.newWaitQueue();
    private final OpSelectors.PdSelector pdSelector;
    private final SchemaSpec schemaSpec;

    private final Set<Long> readingFrom = new ConcurrentSkipListSet<>();
    private final Set<Long> writingTo = new ConcurrentSkipListSet<>();

    public LockingDataTracker(OpSelectors.PdSelector pdSelector, SchemaSpec schemaSpec)
    {
        this.pdSelector = pdSelector;
        this.schemaSpec = schemaSpec;
    }

    @Override
    public void beginModification(long lts)
    {
        ReadersWritersLock partitionLock = getLockForLts(lts);
        partitionLock.lockForWrite();
        assert !readingFrom.contains(partitionLock.descriptor) : String.format("Reading from should not have contained %d", partitionLock.descriptor);
        writingTo.add(partitionLock.descriptor);
        super.beginModification(lts);
    }

    @Override
    public void endModification(long lts)
    {
        super.endModification(lts);
        ReadersWritersLock partitionLock = getLockForLts(lts);
        assert !readingFrom.contains(partitionLock.descriptor) : String.format("Reading from should not have contained %d", partitionLock.descriptor);
        writingTo.remove(partitionLock.descriptor);
        partitionLock.unlockAfterWrite();
    }

    @Override
    public void beginValidation(long pd)
    {
        ReadersWritersLock partitionLock = getLock(pd);
        partitionLock.lockForRead();
        assert !writingTo.contains(pd) : String.format("Writing to should not have contained %d", pd);
        readingFrom.add(pd);
        super.beginValidation(pd);
    }

    @Override
    public void endValidation(long pd)
    {
        super.endValidation(pd);
        ReadersWritersLock partitionLock = getLock(pd);
        assert !writingTo.contains(pd) : String.format("Writing to should not have contained %d", pd);
        readingFrom.remove(partitionLock.descriptor);
        partitionLock.unlockAfterRead();
    }

    public void validate(long pd, Runnable runnable)
    {
        ReadersWritersLock partitionLock = getLockForLts(pd);
        partitionLock.lockForRead();
        runnable.run();
        partitionLock.unlockAfterRead();
    }

    private ReadersWritersLock getLockForLts(long lts)
    {
        long pd = pdSelector.pd(lts, schemaSpec);
        return getLock(pd);
    }

    private ReadersWritersLock getLock(long pd)
    {
        return locked.computeIfAbsent(pd, (pd_) -> new ReadersWritersLock(readersQueue, writersQueue, pd));
    }

    /**
     * Readers/writers lock. It was decided not to use signals here, and instead go for a
     * busyspin instead, since we expect locks to be released briefly and contention to be minimal.
     */
    public static class ReadersWritersLock
    {
        private static final AtomicLongFieldUpdater<ReadersWritersLock> fieldUpdater = AtomicLongFieldUpdater.newUpdater(ReadersWritersLock.class, "lock");
        private volatile long lock;

        final long descriptor;
        final WaitQueue readersQueue;
        final WaitQueue writersQueue;

        public ReadersWritersLock(WaitQueue readersQueue, WaitQueue writersQueue, long descriptor)
        {
            this.readersQueue = readersQueue;
            this.writersQueue = writersQueue;
            this.lock = 0L;
            this.descriptor = descriptor;
        }

        @Override
        public String toString()
        {
            long lock = this.lock;
            return "PartitionLock{" +
                   "pd = " + descriptor +
                   ", readers = " + getReaders(lock) +
                   ", writers = " + getWriters(lock) +
                   '}';
        }

        public void lockForWrite()
        {
            while (true)
            {
                WaitQueue.Signal signal = writersQueue.register();
                long v = lock;
                if (getReaders(v) == 0)
                {
                    if (fieldUpdater.compareAndSet(this, v, incWriters(v)))
                    {
                        signal.cancel();
                        return;
                    }
                }
                signal.awaitUninterruptibly();
            }
        }

        public void unlockAfterWrite()
        {
            while (true)
            {
                long v = lock;
                if (fieldUpdater.compareAndSet(this, v, decWriters(v)))
                {
                    readersQueue.signalAll();
                    writersQueue.signalAll();
                    return;
                }
            }
        }

        public void lockForRead()
        {
            while (true)
            {
                WaitQueue.Signal signal = readersQueue.register();
                long v = lock;
                if (getWriters(v) == 0)
                {
                    if (fieldUpdater.compareAndSet(this, v, incReaders(v)))
                    {
                        signal.cancel();
                        return;
                    }
                }
                signal.awaitUninterruptibly();
            }
        }

        public boolean tryLockForRead()
        {
            long v = lock;
            if (getWriters(v) == 0 && fieldUpdater.compareAndSet(this, v, incReaders(v)))
                return true;

            return false;
        }

        public void unlockAfterRead()
        {
            while (true)
            {
                long v = lock;
                if (fieldUpdater.compareAndSet(this, v, decReaders(v)))
                {
                    writersQueue.signalAll();
                    readersQueue.signalAll();
                    return;
                }
            }
        }

        private long incReaders(long v)
        {
            long readers = getReaders(v);
            assert getWriters(v) == 0;
            v &= ~0x00000000ffffffffL; // erase all readers
            return v | (readers + 1L);
        }

        private long decReaders(long v)
        {
            long readers = getReaders(v);
            assert getWriters(v) == 0;
            assert readers >= 1;
            v &= ~0x00000000ffffffffL; // erase all readers
            return v | (readers - 1L);
        }

        private long incWriters(long v)
        {
            long writers = getWriters(v);
            assert getReaders(v) == 0;
            v &= ~0xffffffff00000000L; // erase all writers
            return v | ((writers + 1L) << 32);
        }

        private long decWriters(long v)
        {
            long writers = getWriters(v);
            assert getReaders(v) == 0;
            assert writers >= 1 : "Writers left " + writers;
            v &= ~0xffffffff00000000L; // erase all writers
            return v | ((writers - 1L) << 32);
        }

        public int getReaders(long v)
        {
            v &= 0xffffffffL;
            return (int) v;
        }

        public int getWriters(long v)
        {
            v >>= 32;
            v &= 0xffffffffL;
            return (int) v;
        }
    }

    @Override
    public Configuration.DataTrackerConfiguration toConfig()
    {
        return new Configuration.LockingDataTrackerConfiguration(maxSeenLts.get(), maxCompleteLts.get(), new ArrayList<>(reorderBuffer));
    }

    @Override
    public String toString()
    {
        return "Locking" + super.toString();
    }
}
