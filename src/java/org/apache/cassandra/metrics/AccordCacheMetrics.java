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

import java.util.concurrent.locks.Lock;
import java.util.function.Function;
import java.util.function.ToLongFunction;

import com.codahale.metrics.Gauge;
import org.apache.cassandra.service.accord.AccordExecutor;
import org.apache.cassandra.service.accord.IAccordService;

import static org.apache.cassandra.metrics.AccordMetricUtils.fromAccordService;
import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

public class AccordCacheMetrics
{
    public static final String ACCORD_CACHE = "AccordCache";
    public static final AccordCacheMetrics CommandsCacheMetrics = new AccordCacheMetrics("Commands");
    public static final AccordCacheMetrics CommandsForKeyCacheMetrics = new AccordCacheMetrics("CommandsForKey");
    public static final AccordCacheGlobalMetrics Global = new AccordCacheGlobalMetrics();

    // not sure why we create these wrapper objects that can only be instantiated once, but it's a pattern in this package so...
    public static class AccordCacheGlobalMetrics
    {
        final Gauge<Long> usedBytes;
        final Gauge<Long> unreferencedBytes;

        public AccordCacheGlobalMetrics()
        {
            DefaultNameFactory factory = new DefaultNameFactory("AccordCache");
            this.usedBytes = Metrics.gauge(factory.createMetricName("UsedBytes"), fromAccordService(sumExecutors(executor -> executor.cacheUnsafe().weightedSize()), 0L));
            this.unreferencedBytes = Metrics.gauge(factory.createMetricName("UnreferencedBytes"), fromAccordService(sumExecutors(executor -> executor.cacheUnsafe().unreferencedBytes()), 0L));
        }

        private static Function<IAccordService, Long> sumExecutors(ToLongFunction<AccordExecutor> f)
        {
            return service -> {
                long sum = 0;
                for (AccordExecutor executor : service.executors())
                    sum += f.applyAsLong(executor);
                return sum;
            };
        }
    }

    public static class Shard
    {
        public final ShardedHitRate.HitRateShard hitRate;
        public final LogLinearHistogram objectSize;

        public Shard(ShardedHitRate.HitRateShard hitRate, LogLinearHistogram objectSize)
        {
            this.hitRate = hitRate;
            this.objectSize = objectSize;
        }
    }

    public final ShardedHitRate hitRate = new ShardedHitRate();
    public final ShardedHistogram objectSize;

    public final Gauge<Long> hits;
    public final Gauge<Long> misses;
    public final Gauge<Long> requests;
    public final Gauge<Double> requestRate1m;
    public final Gauge<Double> requestRate5m;
    public final Gauge<Double> requestRate15m;
    public final Gauge<Double> hitRateAllTime;
    public final Gauge<Double> hitRate1m;
    public final Gauge<Double> hitRate5m;
    public final Gauge<Double> hitRate15m;
    private final String subTypeName;

    public AccordCacheMetrics(String subTypeName)
    {
        DefaultNameFactory factory = new DefaultNameFactory("AccordCache", subTypeName);
        this.objectSize = Metrics.shardedHistogram(factory.createMetricName("EntrySize"));
        this.hits = Metrics.gauge(factory.createMetricName("Hits"), hitRate::totalHits);
        this.misses = Metrics.gauge(factory.createMetricName("Misses"), hitRate::totalMisses);
        this.requests = Metrics.gauge(factory.createMetricName("Requests"), hitRate::totalRequests);
        this.requestRate1m = Metrics.gauge(factory.createMetricName("Requests"), () -> hitRate.requestsPerSecond(1));
        this.requestRate5m = Metrics.gauge(factory.createMetricName("Requests"), () -> hitRate.requestsPerSecond(5));
        this.requestRate15m = Metrics.gauge(factory.createMetricName(RatioGaugeSet.FIFTEEN_MINUTE + "RequestRate"), () -> hitRate.requestsPerSecond(15));
        this.hitRate1m = Metrics.gauge(factory.createMetricName(RatioGaugeSet.ONE_MINUTE + "HitRate"), () -> hitRate.hitRate(1));
        this.hitRate5m = Metrics.gauge(factory.createMetricName(RatioGaugeSet.FIVE_MINUTE + "HitRate"), () -> hitRate.hitRate(5));
        this.hitRate15m = Metrics.gauge(factory.createMetricName(RatioGaugeSet.FIFTEEN_MINUTE + "HitRate"), () -> hitRate.hitRate(15));
        this.hitRateAllTime = Metrics.gauge(factory.createMetricName("Misses"), hitRate::hitRateAllTime);
        this.subTypeName = subTypeName;
    }

    public Shard newShard(Lock guardedBy)
    {
        return new Shard(hitRate.newShard(guardedBy), objectSize.newShard(guardedBy));
    }
}
