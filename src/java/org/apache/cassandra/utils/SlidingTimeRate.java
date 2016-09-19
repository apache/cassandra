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
package org.apache.cassandra.utils;

import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

/**
 * Concurrent rate computation over a sliding time window.
 */
public class SlidingTimeRate
{
    private final ConcurrentSkipListMap<Long, AtomicInteger> counters = new ConcurrentSkipListMap<>();
    private final AtomicLong lastCounterTimestamp = new AtomicLong(0);
    private final ReadWriteLock pruneLock = new ReentrantReadWriteLock();
    private final long sizeInMillis;
    private final long precisionInMillis;
    private final TimeSource timeSource;

    /**
     * Creates a sliding rate whose time window is of the given size, with the given precision and time unit.
     * <br/>
     * The precision defines how accurate the rate computation is, as it will be computed over window size +/-
     * precision.
     */
    public SlidingTimeRate(TimeSource timeSource, long size, long precision, TimeUnit unit)
    {
        Preconditions.checkArgument(size > precision, "Size should be greater than precision.");
        Preconditions.checkArgument(TimeUnit.MILLISECONDS.convert(precision, unit) >= 1, "Precision must be greater than or equal to 1 millisecond.");
        this.sizeInMillis = TimeUnit.MILLISECONDS.convert(size, unit);
        this.precisionInMillis = TimeUnit.MILLISECONDS.convert(precision, unit);
        this.timeSource = timeSource;
    }

    /**
     * Updates the rate.
     */
    public void update(int delta)
    {
        pruneLock.readLock().lock();
        try
        {
            while (true)
            {
                long now = timeSource.currentTimeMillis();
                long lastTimestamp = lastCounterTimestamp.get();
                boolean isWithinPrecisionRange = (now - lastTimestamp) < precisionInMillis;
                AtomicInteger lastCounter = counters.get(lastTimestamp);
                // If there's a valid counter for the current last timestamp, and we're in the precision range,
                // update such counter:
                if (lastCounter != null && isWithinPrecisionRange)
                {
                    lastCounter.addAndGet(delta);

                    break;
                }
                // Else if there's no counter or we're past the precision range, try to create a new counter,
                // but only the thread updating the last timestamp will create a new counter:
                else if (lastCounterTimestamp.compareAndSet(lastTimestamp, now))
                {
                    AtomicInteger existing = counters.putIfAbsent(now, new AtomicInteger(delta));
                    if (existing != null)
                    {
                        existing.addAndGet(delta);
                    }

                    break;
                }
            }
        }
        finally
        {
            pruneLock.readLock().unlock();
        }
    }

    /**
     * Gets the current rate in the given time unit from the beginning of the time window to the
     * provided point in time ago.
     */
    public double get(long toAgo, TimeUnit unit)
    {
        pruneLock.readLock().lock();
        try
        {
            long toAgoInMillis = TimeUnit.MILLISECONDS.convert(toAgo, unit);
            Preconditions.checkArgument(toAgoInMillis < sizeInMillis, "Cannot get rate in the past!");

            long now = timeSource.currentTimeMillis();
            long sum = 0;
            ConcurrentNavigableMap<Long, AtomicInteger> tailCounters = counters
                    .tailMap(now - sizeInMillis, true)
                    .headMap(now - toAgoInMillis, true);
            for (AtomicInteger i : tailCounters.values())
            {
                sum += i.get();
            }

            double rateInMillis = sum == 0
                                  ? sum
                                  : sum / (double) Math.max(1000, (now - toAgoInMillis) - tailCounters.firstKey());
            double multiplier = TimeUnit.MILLISECONDS.convert(1, unit);
            return rateInMillis * multiplier;
        }
        finally
        {
            pruneLock.readLock().unlock();
        }
    }

    /**
     * Gets the current rate in the given time unit.
     */
    public double get(TimeUnit unit)
    {
        return get(0, unit);
    }

    /**
     * Prunes the time window of old unused updates.
     */
    public void prune()
    {
        pruneLock.writeLock().lock();
        try
        {
            long now = timeSource.currentTimeMillis();
            counters.headMap(now - sizeInMillis, false).clear();
        }
        finally
        {
            pruneLock.writeLock().unlock();
        }
    }

    @VisibleForTesting
    public int size()
    {
        return counters.values().stream().reduce(new AtomicInteger(), (v1, v2) -> {
            v1.addAndGet(v2.get());
            return v1;
        }).get();
    }
}
