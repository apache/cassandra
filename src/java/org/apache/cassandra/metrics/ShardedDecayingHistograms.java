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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

import com.codahale.metrics.Snapshot;

import org.apache.cassandra.metrics.LogLinearDecayingHistograms.LogLinearDecayingHistogram;
import org.apache.cassandra.metrics.LogLinearHistogram.LogLinearSnapshot;
import org.apache.cassandra.utils.Clock;

import static org.apache.cassandra.metrics.CassandraReservoir.BucketStrategy.log_linear;

public class ShardedDecayingHistograms
{
    private static final long REFRESH_RATE = TimeUnit.SECONDS.toNanos(15);

    public class ShardedDecayingHistogram extends OverrideHistogram
    {
        final int histogramIndex;
        final long initialMaxValue;

        private ShardedDecayingHistogram(int histogramIndex, long initialMaxValue)
        {
            this.histogramIndex = histogramIndex;
            this.initialMaxValue = initialMaxValue;
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

        @Override
        public synchronized long getCount()
        {
            return maybeRefresh().get(histogramIndex).totalCount;
        }

        @Override
        public Snapshot getSnapshot()
        {
            return maybeRefresh().get(histogramIndex);
        }

        public LogLinearDecayingHistogram forShard(DecayingHistogramsShard shard)
        {
            return shard.histograms.get(histogramIndex);
        }

        public LogLinearSnapshot refresh()
        {
            synchronized (ShardedDecayingHistograms.this)
            {
                long now = Clock.Global.currentTimeMillis();
                List<LogLinearSnapshot> snapshot = new ArrayList<>(ShardedDecayingHistograms.this.snapshot);
                if (snapshot.size() <= histogramIndex)
                    return ShardedDecayingHistograms.this.refresh(now).get(histogramIndex);

                LogLinearSnapshot result = LogLinearSnapshot.emptyForMax(initialMaxValue);
                snapshot.set(histogramIndex, result);
                for (DecayingHistogramsShard shard : shards)
                    shard.updateSnapshot(histogramIndex, result, now);
                ShardedDecayingHistograms.this.snapshot = snapshot;
                // don't update snapshotAt, since we have only refreshed one histogram
                return result;
            }
        }

        public void clear()
        {
            for (DecayingHistogramsShard shard : shards)
            {
                shard.lock.lock();
                try
                {
                    shard.histograms.get(histogramIndex).clear();
                }
                finally
                {
                    shard.lock.unlock();
                }
            }
        }

        @Override
        public boolean isCumulative()
        {
            return false;
        }
    }

    public static class DecayingHistogramsShard
    {
        final Lock lock;
        final LogLinearDecayingHistograms histograms;

        DecayingHistogramsShard(Lock lock, LogLinearDecayingHistograms histograms)
        {
            this.lock = lock;
            this.histograms = histograms;
        }

        void updateSnapshot(List<LogLinearSnapshot> update, long at)
        {
            lock.lock();
            try
            {
                for (int i = 0 ; i < update.size() ; ++i)
                    histograms.get(i).updateSnapshot(update.get(i), at);
            }
            finally
            {
                lock.unlock();
            }
        }

        private void updateSnapshot(int histogramIndex, LogLinearSnapshot update, long at)
        {
            lock.lock();
            try
            {
                histograms.get(histogramIndex).updateSnapshot(update, at);
            }
            finally
            {
                lock.unlock();
            }
        }

        public LogLinearDecayingHistograms unsafeGetInternal()
        {
            return histograms;
        }
    }

    final List<DecayingHistogramsShard> shards = new ArrayList<>();
    final List<ShardedDecayingHistogram> histograms = new ArrayList<>();

    public synchronized DecayingHistogramsShard newShard(Lock guardedBy)
    {
        DecayingHistogramsShard shard = new DecayingHistogramsShard(guardedBy, new LogLinearDecayingHistograms());
        for (int i = 0 ; i < histograms.size() ; ++i)
            shard.histograms.allocate(histograms.get(i).initialMaxValue);
        shards.add(shard);
        return shard;
    }

    public synchronized ShardedDecayingHistogram newHistogram(long initialMaxValue)
    {
        ShardedDecayingHistogram histogram = new ShardedDecayingHistogram(histograms.size(), initialMaxValue);
        for (int i = 0 ; i < shards.size() ; ++i)
            shards.get(i).histograms.allocate(initialMaxValue);
        histograms.add(histogram);
        return histogram;
    }

    private long snapshotAt = Long.MIN_VALUE;
    private List<LogLinearSnapshot> snapshot = Collections.emptyList();

    public synchronized void refresh()
    {
        refresh(Clock.Global.nanoTime());
    }

    private synchronized List<LogLinearSnapshot> refresh(long now)
    {
        List<LogLinearSnapshot> snapshot = new ArrayList<>(histograms.size());
        for (ShardedDecayingHistogram histogram : histograms)
            snapshot.add(LogLinearSnapshot.emptyForMax(histogram.initialMaxValue));
        for (DecayingHistogramsShard shard : shards)
            shard.updateSnapshot(snapshot, now);
        this.snapshot = snapshot;
        this.snapshotAt = now;
        return this.snapshot;
    }

    private synchronized List<LogLinearSnapshot> maybeRefresh()
    {
        long now = Clock.Global.nanoTime();
        if (snapshot.size() == histograms.size() && snapshotAt + REFRESH_RATE >= now)
            return snapshot;

        return refresh(now);
    }
}
