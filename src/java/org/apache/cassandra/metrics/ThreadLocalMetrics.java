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

package org.apache.cassandra.metrics;

import java.lang.ref.PhantomReference;
import java.lang.ref.ReferenceQueue;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.concurrent.CassandraThread;
import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.concurrent.Shutdownable;

import io.netty.util.concurrent.FastThreadLocal;

import static com.google.common.collect.ImmutableList.of;
import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;
import static org.apache.cassandra.concurrent.InfiniteLoopExecutor.SimulatorSafe.UNSAFE;
import static org.apache.cassandra.utils.ExecutorUtils.shutdownAndWait;

/**
 * A thread-local counter implementation designed to use in metrics as an alternative to LongAdder used by Dropwizard metrics.
 * This implementation has reduced write (increment) CPU usage costs in exchange for a higher read cost.
 * We keep and increment parts of a counter locally for each thread.
 * To reduce memory footprint per counter they are grouped together to a long[] array for each thread.
 * A position of a counter value is the same for every thread for the same counter id.
 * Piggyback volatile visibility is expected for readers who execute getCount method to see recent writes to thread local arrays.
 * If a metric is not used anymore the position in the array is reused. Phantom references are used to track aliveness of metric users.
 * When a thread died the counter values accumulated by it are transfered to a shared summaryValues collection.
 * Threads death is tracked using 2 approaches: FastThreadLocal.onRemoval callback and phantom references to Thread objects.
 */
public class ThreadLocalMetrics
{
    private static final int INITIAL_COUNTERS_CAPACITY = 16;

    static final AtomicInteger idGenerator = new AtomicInteger();

    private static final Object freeMetricIdSetGuard = new Object();

    @VisibleForTesting
    static final BitSet freeMetricIdSet = new BitSet();

    private static final List<ThreadLocalMetrics> allThreadLocalMetrics = new CopyOnWriteArrayList<>();

    /* the lock is used to coordinate the threads which:
     * 1) transfer values from a dead thread to summaryValues
     * 2) calculate a getCount value.
     * Using this lock we want to avoid
     *  a value lost while moving it in getCount
     *  as well as a double-counting
     */
    private static final ReadWriteLock summaryLock = new ReentrantReadWriteLock();

    private static final FastThreadLocal<ThreadLocalMetrics> threadLocalMetricsCurrent = new FastThreadLocal<>()
    {
        @Override
        protected ThreadLocalMetrics initialValue()
        {
            return create();
        }

        // this method is invoked when a thread is going to finish, but it works only for FastThreadLocalThread
        // so, we use phantom references for other cases
        @Override
        protected void onRemoval(ThreadLocalMetrics value)
        {
            value.release();
        }
    };

    public static ThreadLocalMetrics create()
    {
        ThreadLocalMetrics result = new ThreadLocalMetrics();
        allThreadLocalMetrics.add(result);
        destroyWhenUnreachable(Thread.currentThread(), result::release);
        return result;
    }

    private static volatile AtomicLongArray summaryValues = new AtomicLongArray(INITIAL_COUNTERS_CAPACITY);

    private static final Shutdownable cleaner;
    private static final Set<PhantomReference<Object>> phantomReferences = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private static final ReferenceQueue<Object> referenceQueue = new ReferenceQueue<>();

    static
    {
        cleaner = executorFactory().infiniteLoop("ThreadLocalMetrics-Cleaner", ThreadLocalMetrics::cleanupOneReference, UNSAFE);
    }

    // we assume that counterValues can be only extended
    // piggyback volatile visibility is expected for readers who execute getCount method to see recent writes
    private long[] counterValues = new long[INITIAL_COUNTERS_CAPACITY];

    private static void cleanupOneReference() throws InterruptedException
    {
        Object obj = referenceQueue.remove(100);
        if (obj instanceof MetricIdReference)
        {
            ((MetricIdReference) obj).release();
            phantomReferences.remove(obj);
        }
        else if (obj instanceof MetricCleanerReference)
        {
            ((MetricCleanerReference) obj).release();
            phantomReferences.remove(obj);
        }
    }

    private static class MetricIdReference extends PhantomReference<Object>
    {
        private final int metricId;

        public MetricIdReference(Object referent, ReferenceQueue<? super Object> q, int metricId)
        {
            super(referent, q);
            this.metricId = metricId;
        }

        public void release()
        {
            recycleMetricId(metricId);
        }
    }

    private static class MetricCleanerReference extends PhantomReference<Object>
    {
        private final MetricCleaner metricCleaner;

        public MetricCleanerReference(Object referent, ReferenceQueue<? super Object> q, MetricCleaner metricCleaner)
        {
            super(referent, q);
            this.metricCleaner = metricCleaner;
        }

        public void release()
        {
            metricCleaner.clean();
        }
    }

    interface MetricCleaner
    {
        void clean();
    }

    static void destroyWhenUnreachable(Object referent, int metricId)
    {
        phantomReferences.add(new MetricIdReference(referent, referenceQueue, metricId));
    }

    static void destroyWhenUnreachable(Object referent, MetricCleaner metricCleaner)
    {
        phantomReferences.add(new MetricCleanerReference(referent, referenceQueue, metricCleaner));
    }

    @VisibleForTesting
    public static void shutdownCleaner(long timeout, TimeUnit unit) throws InterruptedException, TimeoutException
    {
        shutdownAndWait(timeout, unit, of(cleaner));
    }

    public void release()
    {
        // Using this lock while moving we want to avoid races with readers in getCount
        // such races can cause a transfered value lost or its double-counting by a reader
        Lock lock = summaryLock.writeLock();
        lock.lock();
        try
        {
            // we may try to release ThreadLocalMetrics 2 times: onRemoval and by PhantomReference
            // so this if check is needed to avoid a potential double release
            if (allThreadLocalMetrics.remove(this))
            {
                for (int metricId = 0; metricId < counterValues.length; metricId++)
                {
                    long value = counterValues[metricId];
                    if (value != 0)
                        updateSummary(metricId, value);
                }
            }
        }
        finally
        {
            lock.unlock();
        }
    }

    /**
     * If we already have ThreadLocalMetrics instance looked up for the current thread
     * we can use this method to avoid thread local lookup costs.
     * It can be used if you need to update several counters at the same time.
     * @param metricId metric to add a value
     * @param n valuen to add, can be negative number as well
     */
    public void addNonStatic(int metricId, long n)
    {
        getNonStatic(metricId)[metricId] += n;
    }

    public static void add(int metricId, long n)
    {
        get(metricId)[metricId] += n;
    }

    private static long getCount(int metricId, boolean resetToZero)
    {
        long result;
        Lock readLock = summaryLock.readLock();
        readLock.lock();
        try
        {
            result = getSummaryValue(metricId);
            for (ThreadLocalMetrics threadLocalMetrics : allThreadLocalMetrics)
            {
                long count = 0;
                long[] currentCounterValues = threadLocalMetrics.counterValues;
                // currentCounterValues is extended for a thread when a value for metricId is reported in the thread
                if (metricId < currentCounterValues.length)
                    count = currentCounterValues[metricId];
                result += count;
            }
            if (resetToZero)
                updateSummary(metricId, -result); // compensative reset without writing to thread local values
        }
        finally
        {
            readLock.unlock();
        }
        return result;
    }

    // must be executed under summaryLock
    private static long getSummaryValue(int metricId)
    {
        return summaryValues.get(metricId);
    }

    // must be executed under summaryLock
    private static void updateSummary(int metricId, long value)
    {
        summaryValues.getAndAdd(metricId, value);
    }

    public static long getCount(int metricId)
    {
        return getCount(metricId, false);
    }

    public static long getCountAndReset(int metricId)
    {
        return getCount(metricId, true);
    }

    public static ThreadLocalMetrics get()
    {
        Thread currentThread = Thread.currentThread();
        if (currentThread instanceof CassandraThread)
            return ((CassandraThread)currentThread).getThreadLocalMetrics();
        return threadLocalMetricsCurrent.get();
    }

    private static long[] get(int metricId)
    {
        ThreadLocalMetrics threadLocalMetrics = ThreadLocalMetrics.get();
        return threadLocalMetrics.getNonStatic(metricId);
    }

    private long[] getNonStatic(int metricId)
    {
        long[] currentCounterValues = counterValues;
        if (metricId < currentCounterValues.length)
            return currentCounterValues;

        long[] newCounterValues = new long[calculateNewCapacity(metricId)];
        // to avoid a race condition with a metric value reset within recycleMetricId logic
        synchronized (this)
        {
            System.arraycopy(currentCounterValues, 0, newCounterValues, 0, currentCounterValues.length);
            counterValues = newCounterValues;
            return newCounterValues;
        }
    }

    private static int calculateNewCapacity(int metricId)
    {
        return Math.max(metricId + 1, (int)(metricId * 1.1) );
    }

    static int allocateMetricId()
    {
        int metricId;
        synchronized (freeMetricIdSetGuard)
        {
            metricId = freeMetricIdSet.nextSetBit(0);
            if (metricId >= 0)
                freeMetricIdSet.clear(metricId);
        }
        if (metricId < 0)
            metricId = idGenerator.getAndIncrement();

        if (metricId >= summaryValues.length()) // double-checked locking
        {
            Lock lock = summaryLock.writeLock();
            lock.lock();
            try
            {
                if (metricId >= summaryValues.length())
                {
                    AtomicLongArray newSummaryValues = new AtomicLongArray(calculateNewCapacity(metricId));
                    for (int i = 0; i < summaryValues.length(); i++)
                        newSummaryValues.set(i, summaryValues.get(i));
                    summaryValues = newSummaryValues;
                }
            }
            finally
            {
                lock.unlock();
            }
        }
        return metricId;
    }

    static void recycleMetricId(int metricId)
    {
        // we use lock here to avoid potential issues when a metric is releasing and a thread is detected as dead at the same time
        // in this case we may clean a summary value and later the thread removal logic may re-add a non-zero summary value
        Lock lock = summaryLock.writeLock();
        lock.lock();
        try
        {
            for (ThreadLocalMetrics threadLocalMetrics : allThreadLocalMetrics)
            {
                // to avoid a race condition with counterValues array extension by a metric updating thread within getNonStatic
                synchronized (threadLocalMetrics)
                {
                    long[] currentCounterValues = threadLocalMetrics.counterValues;
                    if (metricId < currentCounterValues.length)
                        currentCounterValues[metricId] = 0;
                }
            }
            summaryValues.set(metricId, 0);
        }
        finally
        {
            lock.unlock();
        }

        // there's no an obvious happens-before relation between currentCounterValues[metricId] = 0 write we just did
        // and an initial read of the entry by a thread which updates the reused metric
        // as a workaround we introduce a delay in recyling to provide the write visibility in practice
        //  even if it is not formally guaranteed by the JMM
        ScheduledExecutors.scheduledTasks.schedule(() -> {
            synchronized (freeMetricIdSetGuard)
            {
                freeMetricIdSet.set(metricId);
            }
        }, 5, TimeUnit.SECONDS);
    }

    @VisibleForTesting
    static int getAllocatedMetricsCount()
    {
        int freeCount;
        synchronized (freeMetricIdSetGuard)
        {
            freeCount = freeMetricIdSet.cardinality();
        }
        return idGenerator.get() - freeCount;
    }

    @VisibleForTesting
    static int getThreadLocalMetricsObjectsCount()
    {
        return allThreadLocalMetrics.size();
    }
}
