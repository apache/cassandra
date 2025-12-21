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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.function.LongBinaryOperator;
import java.util.function.ToLongFunction;

import com.codahale.metrics.Gauge;

import accord.utils.Invariants;

import org.apache.cassandra.utils.Clock;

public class ShardedLongGauges<S>
{
    private static final long REFRESH_RATE = TimeUnit.SECONDS.toNanos(15);

    public class ShardedLongGauge implements Gauge<Long>
    {
        final int gaugeIndex;
        final ToLongFunction<S> compute;
        final LongBinaryOperator reduce;

        private ShardedLongGauge(int gaugeIndex, ToLongFunction<S> compute, LongBinaryOperator reduce)
        {
            this.gaugeIndex = gaugeIndex;
            this.compute = compute;
            this.reduce = reduce;
        }

        @Override
        public Long getValue()
        {
            return maybeRefresh()[gaugeIndex];
        }
    }

    static class LongGaugeShard<T>
    {
        final Lock lock;
        final T shard;

        LongGaugeShard(Lock lock, T shard)
        {
            this.lock = lock;
            this.shard = shard;
        }

        public void init(long[] init, List<ShardedLongGauges<T>.ShardedLongGauge> gauges)
        {
            lock.lock();
            try
            {
                for (int i = 0 ; i < init.length ; ++i)
                    init[i] = gauges.get(i).compute.applyAsLong(shard);
            }
            finally
            {
                lock.unlock();
            }
        }

        public void update(long[] update, List<ShardedLongGauges<T>.ShardedLongGauge> gauges)
        {
            lock.lock();
            try
            {
                for (int i = 0 ; i < update.length ; ++i)
                {
                    ShardedLongGauges<T>.ShardedLongGauge gauge = gauges.get(i);
                    Invariants.require(gauge.gaugeIndex == i);
                    update[i] = gauge.reduce.applyAsLong(update[i], gauge.compute.applyAsLong(shard));
                }
            }
            finally
            {
                lock.unlock();
            }
        }
    }

    final List<LongGaugeShard<S>> shards = new ArrayList<>();
    final List<ShardedLongGauge> gauges = new ArrayList<>();

    public synchronized void newShard(Lock guardedBy, S shard)
    {
        shards.add(new LongGaugeShard<>(guardedBy, shard));
    }

    public synchronized ShardedLongGauge newGauge(ToLongFunction<S> compute, LongBinaryOperator reduce)
    {
        ShardedLongGauge gauge = new ShardedLongGauge(gauges.size(), compute, reduce);
        gauges.add(gauge);
        return gauge;
    }

    private long snapshotAt = Long.MIN_VALUE;
    private long[] snapshot = new long[0];

    public synchronized void refresh()
    {
        refresh(Clock.Global.nanoTime());
    }

    private synchronized long[] refresh(long now)
    {
        if (gauges.isEmpty())
            return new long[0];

        long[] snapshot = new long[gauges.size()];
        if (shards.isEmpty())
            return snapshot;

        shards.get(0).init(snapshot, gauges);
        for (int i = 1; i < shards.size() ; ++i)
            shards.get(i).update(snapshot, gauges);
        this.snapshot = snapshot;
        this.snapshotAt = now;
        return this.snapshot;
    }

    private synchronized long[] maybeRefresh()
    {
        long now = Clock.Global.nanoTime();
        if (snapshot.length == gauges.size() && snapshotAt + REFRESH_RATE >= now)
            return snapshot;

        return refresh(now);
    }
}
