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

import java.net.InetAddress;
import java.util.Map.Entry;

import com.codahale.metrics.Counter;
import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;

import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.utils.UUIDGen;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

/**
 * Metrics for {@link org.apache.cassandra.hints.HintsService}.
 */
public class HintedHandoffMetrics
{
    private static final Logger logger = LoggerFactory.getLogger(HintedHandoffMetrics.class);

    private static final MetricNameFactory factory = new DefaultNameFactory("HintedHandOffManager");

    /** Total number of hints which are not stored, This is not a cache. */
    private final LoadingCache<InetAddress, DifferencingCounter> notStored = Caffeine.newBuilder().build(new CacheLoader<InetAddress, DifferencingCounter>()
    {
        public DifferencingCounter load(InetAddress address)
        {
            return new DifferencingCounter(address);
        }
    });

    /** Total number of hints that have been created, This is not a cache. */
    private final LoadingCache<InetAddress, Counter> createdHintCounts = Caffeine.newBuilder().build(new CacheLoader<InetAddress, Counter>()
    {
        public Counter load(InetAddress address)
        {
            return Metrics.counter(factory.createMetricName("Hints_created-" + address.getHostAddress().replace(':', '.')));
        }
    });

    public void incrCreatedHints(InetAddress address)
    {
        createdHintCounts.get(address).inc();
    }

    public void incrPastWindow(InetAddress address)
    {
        notStored.get(address).mark();
    }

    public void log()
    {
        for (Entry<InetAddress, DifferencingCounter> entry : notStored.asMap().entrySet())
        {
            long difference = entry.getValue().difference();
            if (difference == 0)
                continue;
            logger.warn("{} has {} dropped hints, because node is down past configured hint window.", entry.getKey(), difference);
            SystemKeyspace.updateHintsDropped(entry.getKey(), UUIDGen.getTimeUUID(), (int) difference);
        }
    }

    public static class DifferencingCounter
    {
        private final Counter meter;
        private long reported = 0;

        public DifferencingCounter(InetAddress address)
        {
            this.meter = Metrics.counter(factory.createMetricName("Hints_not_stored-" + address.getHostAddress().replace(':', '.')));
        }

        public long difference()
        {
            long current = meter.getCount();
            long difference = current - reported;
            this.reported = current;
            return difference;
        }

        public long count()
        {
            return meter.getCount();
        }

        public void mark()
        {
            meter.inc();
        }
    }
}
