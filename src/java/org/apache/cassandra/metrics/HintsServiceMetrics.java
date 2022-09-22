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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import org.apache.cassandra.concurrent.ImmediateExecutor;
import org.apache.cassandra.locator.InetAddressAndPort;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

/**
 * Metrics for {@link org.apache.cassandra.hints.HintsService}.
 */
public final class HintsServiceMetrics
{
    private static final Logger logger = LoggerFactory.getLogger(HintsServiceMetrics.class);

    private static final MetricNameFactory factory = new DefaultNameFactory("HintsService");

    public static final Meter hintsSucceeded = Metrics.meter(factory.createMetricName("HintsSucceeded"));
    public static final Meter hintsFailed    = Metrics.meter(factory.createMetricName("HintsFailed"));
    public static final Meter hintsTimedOut  = Metrics.meter(factory.createMetricName("HintsTimedOut"));

    /** Histogram of all hint delivery delays */
    private static final Histogram globalDelayHistogram = Metrics.histogram(factory.createMetricName("Hint_delays"), false);

    /** Histograms per-endpoint of hint delivery delays, This is not a cache. */
    private static final LoadingCache<InetAddressAndPort, Histogram> delayByEndpoint = Caffeine.newBuilder()
                                                                                               .executor(ImmediateExecutor.INSTANCE)
                                                                                               .build(address -> Metrics.histogram(factory.createMetricName("Hint_delays-"+address.toString().replace(':', '.')), false));

    public static void updateDelayMetrics(InetAddressAndPort endpoint, long delay)
    {
        if (delay <= 0)
        {
            logger.warn("Invalid negative latency in hint delivery delay: {}", delay);
            return;
        }

        globalDelayHistogram.update(delay);
        delayByEndpoint.get(endpoint).update(delay);
    }
}
