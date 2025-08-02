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

import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

import com.codahale.metrics.Snapshot;
import org.apache.cassandra.metrics.LogLinearHistogram.LogLinearSnapshot;
import org.apache.cassandra.utils.Clock;

import static org.apache.cassandra.metrics.CassandraReservoir.BucketStrategy.log_linear;

public class ShardedHistogram extends OverrideHistogram
{
    private static final long REFRESH_RATE = TimeUnit.SECONDS.toNanos(15);

    static class HistogramShard
    {
        final Lock lock;
        final LogLinearHistogram histogram;

        HistogramShard(Lock lock, LogLinearHistogram histogram)
        {
            this.lock = lock;
            this.histogram = histogram;
        }

        long total()
        {
            lock.lock();
            try
            {
                return histogram.totalCount;
            }
            finally
            {
                lock.unlock();
            }
        }

        public void updateSnapshot(LogLinearSnapshot snapshot)
        {
            lock.lock();
            try
            {
                histogram.updateSnapshot(snapshot);
            }
            finally
            {
                lock.unlock();
            }
        }
    }

    final CopyOnWriteArrayList<HistogramShard> shards = new CopyOnWriteArrayList<>();
    final long initialMaxValue;

    public ShardedHistogram()
    {
        this(1 << 16);
    }

    @Override
    public CassandraReservoir.BucketStrategy bucketStrategy()
    {
        return log_linear;
    }

    @Override
    public long[] bucketStarts(int length)
    {
        return LogLinearHistogram.bucketsWithLength(length);
    }

    public ShardedHistogram(long initialMaxValue)
    {
        this.initialMaxValue = initialMaxValue;
    }

    public LogLinearHistogram newShard(Lock guardedBy)
    {
        HistogramShard shard = new HistogramShard(guardedBy, new LogLinearHistogram(initialMaxValue));
        shards.add(shard);
        return shard.histogram;
    }

    private LogLinearSnapshot snapshot;
    private long snapshotAt;

    public synchronized void refresh()
    {
        refresh(Clock.Global.nanoTime());
    }

    private synchronized LogLinearSnapshot refresh(long now)
    {
        LogLinearSnapshot snapshot = LogLinearSnapshot.emptyForMax(initialMaxValue);
        for (HistogramShard shard : shards)
            shard.updateSnapshot(snapshot);
        this.snapshot = snapshot;
        this.snapshotAt = now;
        return snapshot;
    }

    private synchronized LogLinearSnapshot maybeRefresh()
    {
        if (shards.isEmpty())
            return snapshot = new LogLinearSnapshot(0);

        long now = Clock.Global.nanoTime();
        if (snapshot != null && snapshotAt + REFRESH_RATE >= now)
            return snapshot;

        return refresh(now);
    }

    @Override
    public synchronized long getCount()
    {
        return maybeRefresh().totalCount;
    }

    @Override
    public Snapshot getSnapshot()
    {
        return maybeRefresh();
    }
}
