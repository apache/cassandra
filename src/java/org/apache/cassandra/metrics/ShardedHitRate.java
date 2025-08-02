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

import java.util.Arrays;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

import accord.utils.Invariants;
import org.apache.cassandra.utils.Clock;

public class ShardedHitRate
{
    private static final long REFRESH_RATE = TimeUnit.SECONDS.toNanos(15L);
    private static final int BUCKETS = 16;
    private static final int BUCKET_MASK = 0xF;
    private static final long CLOCK_SLICE_SHIFT = 10;
    private static final long CLOCK_SLICE = 1 << CLOCK_SLICE_SHIFT;
    private static final long CLOCK_DIVISOR = 60_000_000_000L >>> CLOCK_SLICE_SHIFT;

    private static long now()
    {
        return Clock.Global.nanoTime() / CLOCK_DIVISOR;
    }

    private static int atToMinutes(long at)
    {
        return (int) (at >>> CLOCK_SLICE_SHIFT);
    }

    public static class HitRateShard
    {
        final Lock lock;

        long lastAt;
        final long[] hits = new long[BUCKETS];
        final long[] misses = new long[BUCKETS];

        long totalHits, totalMisses;

        public HitRateShard(Lock lock)
        {
            this.lock = lock;
        }

        void tick(long at)
        {
            if (lastAt >= at)
                return;

            int count = Math.min(BUCKETS, atToMinutes(at - lastAt));
            for (int i = 1 ; i <= count ; ++i)
            {
                int index = (atToMinutes(lastAt) + i) & BUCKET_MASK;
                hits[index] = misses[index] = 0;
            }
            lastAt = at;
        }

        // assumes the lock is held
        public void markHitExclusive()
        {
            markHitExclusive(now());
        }

        public void markMissExclusive()
        {
            markMissExclusive(now());
        }

        void markHitExclusive(long at)
        {
            tick(at);
            hits[atToMinutes(at) & BUCKET_MASK]++;
            ++totalHits;
        }

        public void markMissExclusive(long at)
        {
            tick(at);
            misses[atToMinutes(at) & BUCKET_MASK]++;
            ++totalMisses;
        }

        void updateSnapshot(ShardedHitRate.Snapshot snapshot, long at)
        {
            lock.lock();
            try
            {
                if (at > lastAt)
                    tick(at);

                int srcIndex = atToMinutes(lastAt) & BUCKET_MASK;
                double split = (at % CLOCK_SLICE) / (double)CLOCK_SLICE;
                {
                    snapshot.hits[0] += hits[srcIndex];
                    snapshot.misses[0] += misses[srcIndex];
                }
                for (int i = 1 ; i < BUCKETS - 1 ; ++i)
                {
                    srcIndex = (srcIndex - 1) & BUCKET_MASK;
                    long h = hits[srcIndex], m = misses[srcIndex];
                    long hSplit = Math.round(h * split), mSplit = Math.round(m * split);
                    snapshot.hits[i] += hSplit;
                    snapshot.misses[i] += mSplit;
                    snapshot.hits[i - 1] += h - hSplit;
                    snapshot.misses[i - 1] += m - mSplit;
                }
                {
                    srcIndex = (srcIndex - 1) & BUCKET_MASK;
                    long h = hits[srcIndex], m = misses[srcIndex];
                    long hSplit = (long) (h * split), mSplit = (long) (m * split);
                    snapshot.hits[BUCKETS - 2] += h - hSplit;
                    snapshot.misses[BUCKETS - 2] += m - mSplit;
                }
                snapshot.totalHits += totalHits;
                snapshot.totalMisses += totalMisses;
            }
            finally
            {
                lock.unlock();
            }
        }
    }

    static class Snapshot
    {
        final long[] hits = new long[BUCKETS - 1];
        final long[] misses = new long[BUCKETS - 1];
        long totalHits, totalMisses;
        long lastUpdated = Clock.Global.nanoTime() - REFRESH_RATE;

        double hitRate(int minutes)
        {
            Invariants.requireArgument(minutes <= BUCKETS);
            long misses = 0, hits = 0;
            for (int i = 0 ; i < minutes; ++i)
            {
                hits += this.hits[i];
                misses += this.misses[i];
            }
            long total = hits + misses;
            if (total == 0)
                return 0.0D;
            return hits / (double)(total);
        }

        long requests(int minutes)
        {
            Invariants.requireArgument(minutes <= BUCKETS);
            long requests = 0;
            for (int i = 0 ; i < minutes; ++i)
                requests += hits[i] + misses[i];
            return requests;
        }

        void reset()
        {
            Arrays.fill(hits, 0);
            Arrays.fill(misses, 0);
            totalMisses = totalHits = 0;
        }
    }

    final CopyOnWriteArrayList<HitRateShard> shards = new CopyOnWriteArrayList<>();
    final Snapshot snapshot = new Snapshot();

    public ShardedHitRate()
    {
    }

    public HitRateShard newShard(Lock guardedBy)
    {
        HitRateShard shard = new HitRateShard(guardedBy);
        shards.add(shard);
        return shard;
    }

    private void maybeRefresh()
    {
        if (Clock.Global.nanoTime() - snapshot.lastUpdated >= REFRESH_RATE)
            refresh();
    }

    public synchronized void refresh()
    {
        long at = now();
        snapshot.reset();
        for (HitRateShard shard : shards)
            shard.updateSnapshot(snapshot, at);
    }

    public synchronized long totalHits()
    {
        maybeRefresh();
        return snapshot.totalHits;
    }

    public synchronized long totalMisses()
    {
        maybeRefresh();
        return snapshot.totalMisses;
    }

    public synchronized long totalRequests()
    {
        maybeRefresh();
        return snapshot.totalHits + snapshot.totalMisses;
    }

    public double hitRateAllTime()
    {
        maybeRefresh();
        long totalRequests = snapshot.totalHits + snapshot.totalMisses;
        if (totalRequests == 0)
            return 0.0D;
        return snapshot.totalHits / (double)(snapshot.totalHits + snapshot.totalMisses);
    }

    public double hitRate(int minutes)
    {
        maybeRefresh();
        return snapshot.hitRate(minutes);
    }

    public double requestsPerSecond(int minutes)
    {
        maybeRefresh();
        return snapshot.requests(minutes) / (minutes * 60d);
    }
}
