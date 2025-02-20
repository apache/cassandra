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
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.annotations.VisibleForTesting;

import io.netty.util.concurrent.FastThreadLocal;
import org.apache.cassandra.concurrent.Shutdownable;

import static com.google.common.collect.ImmutableList.of;
import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;
import static org.apache.cassandra.concurrent.InfiniteLoopExecutor.SimulatorSafe.UNSAFE;
import static org.apache.cassandra.utils.ExecutorUtils.shutdownAndWait;

public class ThreadLocalMetrics
{
    static final AtomicInteger idGenerator = new AtomicInteger();

    private static final Object freeIdsGuard = new Object();
    static final BitSet freeMetricIdSet = new BitSet();

    static final List<ThreadLocalMetrics> allThreadLocalMetrics = new CopyOnWriteArrayList<>();

    private static final FastThreadLocal<ThreadLocalMetrics> threadLocalMetricsCurrent = new FastThreadLocal<>()
    {
        @Override
        protected ThreadLocalMetrics initialValue()
        {
            ThreadLocalMetrics result = new ThreadLocalMetrics();
            allThreadLocalMetrics.add(result);
            destroyWhenUnreachable(Thread.currentThread(), () -> {
                result.release();
                allThreadLocalMetrics.remove(result);
            });
            return result;
        }

        // this method is invoked when a thread is going to finish, but it works only for FastThreadLocalThread
        @Override
        protected void onRemoval(ThreadLocalMetrics value)
        {
            value.release();
            allThreadLocalMetrics.remove(value);
        }
    };

    private static final Map<Integer, AtomicLong> summaryValues = new ConcurrentHashMap<>();

    private static final Shutdownable cleaner;
    private static final Set<PhantomReference<Object>> phantomReferences = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private static final ReferenceQueue<Object> referenceQueue = new ReferenceQueue<>();

    static
    {
        cleaner = executorFactory().infiniteLoop("ThreadLocalMetrics-Cleaner", ThreadLocalMetrics::cleanupRound, UNSAFE);
    }

    private long[] counterValues = new long[16];
    private volatile int arrayMutations = 0;

    private static void cleanupRound() throws InterruptedException
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

    private void release()
    {
        for (int metricId = 0; metricId < counterValues.length; metricId++)
        {
            long value = counterValues[metricId];
            if (value != 0)
                getSummary(metricId).addAndGet(value);
        }
    }

    public void addNonStatic(int metricId, long n)
    {
        getNonStatic(metricId)[metricId] += n;
    }

    public static void add(int metricId, long n)
    {
        get(metricId)[metricId] += n;
    }

    private static long getCount(int metricId, boolean reset)
    {
        long summaryLocal;
        long result;
        AtomicLong summary = getSummary(metricId);
        do
        {
            summaryLocal = summary.get();
            result = 0;
            for (ThreadLocalMetrics threadLocalMetrics : allThreadLocalMetrics)
            {
                if (threadLocalMetrics != null)
                {
                    long count = 0;
                    long[] currentCounterValues;
                    long currentArrayMutations;
                    do
                    {
                        currentArrayMutations = threadLocalMetrics.arrayMutations;
                        currentCounterValues = threadLocalMetrics.counterValues;
                        if (metricId < currentCounterValues.length)
                        {
                            count = currentCounterValues[metricId];
                        }
                    }
                    while (currentArrayMutations != threadLocalMetrics.arrayMutations);
                    result += count;
                }
            }
            // we use a kind of optimistic locking here
            // to get a correct sum of thread-local and summary parts for the total count
            // in case of a concurrent cleanDeadAndUpdatedSummaries invocation
        } while (summaryLocal != summary.get());
        result += summaryLocal;
        if (reset)
            summary.addAndGet(-result); // compensative reset without writing to thread local values
        return result;
    }

    private static AtomicLong getSummary(int metricId)
    {
        AtomicLong result = summaryValues.get(metricId);
        if (result != null)
            return result;
        return summaryValues.computeIfAbsent(metricId, (metricIdToAdd) -> new AtomicLong());
    }

    public static long getCount(int metricId)
    {
        return getCount(metricId, false);
    }

    public static long getCountAndReset(int metricId)
    {
        return getCount(metricId, true);
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

        long[] newCounterValues = new long[(int)(metricId * 1.1)];
        System.arraycopy(currentCounterValues, 0, newCounterValues, 0, currentCounterValues.length);
        counterValues = newCounterValues;
        arrayMutations++; // to provide visibility of the new array to other reading threads
        return newCounterValues;
    }

    static int allocateMetricId()
    {
        int metricId;
        synchronized (freeIdsGuard)
        {
            metricId = freeMetricIdSet.nextSetBit(0);
            if (metricId >= 0)
                freeMetricIdSet.clear(metricId);
        }
        if (metricId < 0)
            metricId = idGenerator.getAndIncrement();

        return metricId;
    }

    static void recycleMetricId(int metricId)
    {
        for (ThreadLocalMetrics threadLocalMetrics : allThreadLocalMetrics)
            if (threadLocalMetrics != null)
            {
                long[] currentCounterValues;
                int currentArrayMutations;
                do
                {
                    currentArrayMutations = threadLocalMetrics.arrayMutations;
                    currentCounterValues = threadLocalMetrics.counterValues;
                    if (metricId < currentCounterValues.length)
                    {
                        currentCounterValues[metricId] = 0;
                    }
                }
                while (threadLocalMetrics.arrayMutations != currentArrayMutations);
            }
        summaryValues.remove(metricId);
        synchronized (freeIdsGuard)
        {
            freeMetricIdSet.set(metricId);
        }
    }

    static ThreadLocalMetrics get() {
        return threadLocalMetricsCurrent.get();
    }

    @VisibleForTesting
    static int getAllocatedMetricsCount()
    {
        int freeCount;
        synchronized (freeIdsGuard) {
            freeCount = freeMetricIdSet.cardinality();
        }
        return idGenerator.get() - freeCount;
    }

    @VisibleForTesting
    static int getThreadLocalMetricsCount()
    {
        return allThreadLocalMetrics.size();
    }

    @Override
    public String toString()
    {
        return "ThreadLocalMetrics{" +
               ", counterValues=" + Arrays.toString(counterValues) +
               '}';
    }
}
