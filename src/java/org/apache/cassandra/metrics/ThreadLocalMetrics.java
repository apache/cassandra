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
import java.util.NavigableSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.AtomicReference;

import io.netty.util.concurrent.FastThreadLocal;

public class ThreadLocalMetrics
{
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

    private static final ConcurrentHashMap<Integer, AtomicLong> deadThreadsSummaryValues = new ConcurrentHashMap<>();
    private static final AtomicBoolean transferInProgress = new AtomicBoolean();

    private final Thread thread;

    private final AtomicReference<AtomicLongArray> counterValues = new AtomicReference<>(new AtomicLongArray(16));

    public ThreadLocalMetrics(Thread thread)
    {
        this.thread = thread;
    }

    private static void cleanDeadAndUpdateSummaries() {
        // TODO: should we invoke it peridically as well (to avoid memory leak if nobody invokes getCount()?
        // TODO: add and check allThreadLocalMetrics size threshold to avoid the iteration every time
        if (transferInProgress.compareAndSet(false, true))
            try
            {
                List<ThreadLocalMetrics> toRemove = new ArrayList<>();
                for (ThreadLocalMetrics threadLocalMetrics : allThreadLocalMetrics)
                {
                    if (!threadLocalMetrics.thread.isAlive())
                    {
                        for (int i = 0; i < threadLocalMetrics.counterValues.get().length(); i++)
                        {
                            long value = threadLocalMetrics.counterValues.get().get(i);
                            if (value != 0)
                                deadThreadsSummaryValues.computeIfAbsent(i, (metricId) -> new AtomicLong()).addAndGet(value);
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

    public static ThreadLocalCounter createCounter()
    {
        return new ThreadLocalCounter(getMetricId());
    }

    private static int getMetricId()
    {
        Integer id = freeMetricIdSet.pollFirst();
        if (id != null)
            return id;
        return idGenerator.getAndIncrement();
    }

    public static void destroyCounter(ThreadLocalCounter counter)
    {
        int metricId = counter.metricId;
        for (ThreadLocalMetrics threadLocalMetrics : allThreadLocalMetrics)
            if (threadLocalMetrics != null)
            {
                AtomicLongArray currentCounterValues = threadLocalMetrics.counterValues.get();
                if (metricId < currentCounterValues.length())
                    currentCounterValues.set(metricId, 0);
            }
        deadThreadsSummaryValues.remove(metricId);
        freeMetricIdSet.add(metricId);
    }

    private static ThreadLocalMetrics get() {
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

    public interface CounterMetric {
        void inc();
        void inc(long n);
        void dec();
        void dec(long n);
        long getCount();
    }

    public static class ThreadLocalCounter implements CounterMetric
    {
        private final int metricId;

        ThreadLocalCounter(int metricId)
        {
            this.metricId = metricId;
        }

        private AtomicLongArray get()
        {
            ThreadLocalMetrics threadLocalMetrics = ThreadLocalMetrics.get();
            AtomicLongArray currentCounterValues = threadLocalMetrics.counterValues.getPlain();
            if (metricId < currentCounterValues.length())
                return currentCounterValues;

            AtomicLongArray newCounterValues = new AtomicLongArray((int)(metricId * 1.1));
            for (int i = 0; i < currentCounterValues.length(); i++)
                newCounterValues.setPlain(i, currentCounterValues.getPlain(i));
            threadLocalMetrics.counterValues.lazySet(newCounterValues);
            return newCounterValues;
        }

        @Override
        public void inc()
        {
           AtomicLongArray values = get();
           long current = values.getPlain(metricId);
           values.lazySet(metricId, ++current);
        }

        @Override
        public void inc(long n)
        {
            AtomicLongArray values = get();
            long current = values.getPlain(metricId);
            values.lazySet(metricId, current + n);
        }

        @Override
        public void dec()
        {
            AtomicLongArray values = get();
            long current = values.getPlain(metricId);
            values.lazySet(metricId, --current);
        }

        @Override
        public void dec(long n)
        {
            AtomicLongArray values = get();
            long current = values.getPlain(metricId);
            values.lazySet(metricId, current - n);
        }

        @Override
        public long getCount()
        {
            cleanDeadAndUpdateSummaries();
            long dead;
            long result;
            do
            {
                dead = getDeadSummary(metricId);
                result = 0;
                for (ThreadLocalMetrics threadLocalMetrics : allThreadLocalMetrics)
                {
                    if (threadLocalMetrics != null)
                    {
                        AtomicLongArray currentCounterValues = threadLocalMetrics.counterValues.get();
                        if (metricId < currentCounterValues.length())
                        {
                            result += currentCounterValues.get(metricId);
                        }
                    }
                }
                // we use a kind of optimistic locking here
                // to get a correct sum of live and dead parts for the total count
                // in case of a concurrent cleanDeadAndUpdatedSummaries invocation
            } while (dead != getDeadSummary(metricId));
            result += dead;
            return result;
        }
    }

    private static long getDeadSummary(int metricId)
    {
        AtomicLong dead = deadThreadsSummaryValues.get(metricId);
        return (dead != null) ? dead.get() : 0;
    }
}
