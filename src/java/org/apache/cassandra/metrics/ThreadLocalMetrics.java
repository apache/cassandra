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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.annotations.VisibleForTesting;

import io.netty.util.concurrent.FastThreadLocal;
import org.apache.cassandra.concurrent.ScheduledExecutors;

public class ThreadLocalMetrics
{
    private static final int DEAD_THREADS_INFO_CLEANUP_INTERVAL_SEC = 120;
    static
    {
        ScheduledExecutors.scheduledTasks.scheduleWithFixedDelay(ThreadLocalMetrics::cleanDeadAndUpdateSummaries,
                                                                 DEAD_THREADS_INFO_CLEANUP_INTERVAL_SEC,
                                                                 DEAD_THREADS_INFO_CLEANUP_INTERVAL_SEC,
                                                                 TimeUnit.SECONDS);
    }

    static final AtomicInteger idGenerator = new AtomicInteger();

    static final NavigableSet<Integer> freeMetricIdSet = new ConcurrentSkipListSet<>();

    static final List<ThreadLocalMetrics> allThreadLocalMetrics = new CopyOnWriteArrayList<>();

    private static final FastThreadLocal<ThreadLocalMetrics> threadLocalMetricsCurrent = new FastThreadLocal<>()
    {
        @Override
        protected ThreadLocalMetrics initialValue()
        {

            ThreadLocalMetrics result = new ThreadLocalMetrics(Thread.currentThread());
            allThreadLocalMetrics.add(result);
            return result;
        }
    };

    private static final Map<Integer, AtomicLong> summaryValues = new ConcurrentHashMap<>();
    private static final AtomicBoolean transferInProgress = new AtomicBoolean();

    private final Thread thread;

    private long[] counterValues = new long[16];

    public ThreadLocalMetrics(Thread thread)
    {
        this.thread = thread;
    }

    private static void cleanDeadAndUpdateSummaries() {
        if (transferInProgress.compareAndSet(false, true))
            try
            {
                List<ThreadLocalMetrics> toRemove = new ArrayList<>();
                for (ThreadLocalMetrics threadLocalMetrics : allThreadLocalMetrics)
                {
                    if (!threadLocalMetrics.thread.isAlive())
                    {
                        for (int metricId = 0; metricId < threadLocalMetrics.counterValues.length; metricId++)
                        {
                            long value = threadLocalMetrics.counterValues[metricId];
                            if (value != 0)
                                getSummary(metricId).addAndGet(value);
                        }
                        toRemove.add(threadLocalMetrics);
                    }
                }
                if (!toRemove.isEmpty())
                    allThreadLocalMetrics.removeAll(toRemove);
            }
            finally
            {
                transferInProgress.set(false);
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
                    long[] currentCounterValues = threadLocalMetrics.counterValues;
                    if (metricId < currentCounterValues.length)
                    {
                        result += currentCounterValues[metricId];
                    }
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
        return newCounterValues;
    }


    static int allocateMetricId()
    {
        cleanDeadAndUpdateSummaries();
        Integer id = freeMetricIdSet.pollFirst();
        if (id != null)
            return id;
        return idGenerator.getAndIncrement();
    }

    public static void destroyMetric(int metricId)
    {
        cleanDeadAndUpdateSummaries();
        for (ThreadLocalMetrics threadLocalMetrics : allThreadLocalMetrics)
            if (threadLocalMetrics != null)
            {
                long[] currentCounterValues = threadLocalMetrics.counterValues;
                if (metricId < currentCounterValues.length)
                    currentCounterValues[metricId] = 0;
            }
        summaryValues.remove(metricId);
        freeMetricIdSet.add(metricId);
    }

    @VisibleForTesting
    public static ThreadLocalMetrics get() {
        return threadLocalMetricsCurrent.get();
    }

    @Override
    public String toString()
    {
        return "ThreadLocalMetrics{" +
               "thread=" + thread +
               ", counterValues=" + counterValues +
               '}';
    }
}
