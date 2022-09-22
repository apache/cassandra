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

import java.util.Map.Entry;

import com.codahale.metrics.Counter;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;

import org.apache.cassandra.concurrent.ImmediateExecutor;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.locator.InetAddressAndPort;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;
import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;

/**
 * Metrics for {@link org.apache.cassandra.hints.HintsService}.
 */
public class HintedHandoffMetrics
{
    private static final Logger logger = LoggerFactory.getLogger(HintedHandoffMetrics.class);

    private static final MetricNameFactory factory = new DefaultNameFactory("HintsService");

    /** Total number of hints which are not stored, This is not a cache. */
    private final LoadingCache<InetAddressAndPort, DifferencingCounter> notStored = Caffeine.newBuilder()
                                                                                            .executor(ImmediateExecutor.INSTANCE)
                                                                                            .build(DifferencingCounter::new);

    /** Total number of hints that have been created, This is not a cache. */
    private final LoadingCache<InetAddressAndPort, Counter> createdHintCounts = Caffeine.newBuilder()
                                                                                        .executor(ImmediateExecutor.INSTANCE)
                                                                                        .build(address -> Metrics.counter(factory.createMetricName("Hints_created-" + address.toString().replace(':', '.'))));

    public void incrCreatedHints(InetAddressAndPort address)
    {
        createdHintCounts.get(address).inc();
    }

    public void incrPastWindow(InetAddressAndPort address)
    {
        notStored.get(address).mark();
    }

    public void log()
    {
        for (Entry<InetAddressAndPort, DifferencingCounter> entry : notStored.asMap().entrySet())
        {
            long difference = entry.getValue().difference();
            if (difference == 0)
                continue;
            logger.warn("{} has {} dropped hints, because node is down past configured hint window.", entry.getKey(), difference);
            SystemKeyspace.updateHintsDropped(entry.getKey(), nextTimeUUID(), (int) difference);
        }
    }

    public static class DifferencingCounter
    {
        private final Counter meter;
        private long reported = 0;

        public DifferencingCounter(InetAddressAndPort address)
        {
            //This changes the name of the metric, people can update their monitoring when upgrading?
            this.meter = Metrics.counter(factory.createMetricName("Hints_not_stored-" + address.toString().replace(':', '.')));
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
