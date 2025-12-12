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

import java.util.concurrent.TimeUnit;

import com.codahale.metrics.Gauge;
import org.apache.cassandra.metrics.ShardedDecayingHistograms.ShardedDecayingHistogram;
import org.apache.cassandra.service.accord.AccordExecutor;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;
import static org.apache.cassandra.service.accord.AccordExecutor.HISTOGRAMS;

public class AccordExecutorMetrics
{
    public static final String ACCORD_EXECUTOR = "AccordExecutor";
    public static final AccordExecutorMetrics INSTANCE = new AccordExecutorMetrics();

    public final ShardedLongGauges<AccordExecutor> gauges = new ShardedLongGauges<>();

    // latency
    public final ShardedDecayingHistogram elapsedPreparingToRun = HISTOGRAMS.newHistogram(TimeUnit.SECONDS.toNanos(1L));
    public final ShardedDecayingHistogram elapsedWaitingToRun = HISTOGRAMS.newHistogram(TimeUnit.SECONDS.toNanos(1L));
    public final ShardedDecayingHistogram elapsedRunning = HISTOGRAMS.newHistogram(TimeUnit.SECONDS.toNanos(1L));

    // number of keys involved
    public final ShardedDecayingHistogram keys = HISTOGRAMS.newHistogram(1 << 12);

    public final Gauge<Long> preparingToRun;
    public final Gauge<Long> waitingToRun;
    public final Gauge<Long> running;

    public AccordExecutorMetrics()
    {
        DefaultNameFactory factory = new DefaultNameFactory(ACCORD_EXECUTOR);
        Metrics.register(factory.createMetricName("ElapsedPreparingToRun"), elapsedPreparingToRun);
        Metrics.register(factory.createMetricName("ElapsedWaitingToRun"), elapsedWaitingToRun);
        Metrics.register(factory.createMetricName("ElapsedRunning"), elapsedRunning);

        Metrics.register(factory.createMetricName("Keys"), keys);
        preparingToRun = Metrics.register(factory.createMetricName("PreparingToRun"), gauges.newGauge(AccordExecutor::unsafePreparingToRunCount, Long::sum));
        waitingToRun = Metrics.register(factory.createMetricName("WaitingToRun"), gauges.newGauge(AccordExecutor::unsafeWaitingToRunCount, Long::sum));
        running = Metrics.register(factory.createMetricName("Running"), gauges.newGauge(AccordExecutor::unsafeRunningCount, Long::sum));
    }
}
