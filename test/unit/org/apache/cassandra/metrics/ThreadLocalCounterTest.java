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

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.LocalAwareExecutorPlus;

import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;
import static org.junit.Assert.assertEquals;

public class ThreadLocalCounterTest
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ThreadLocalCounterTest.class);

    @Test
    public void testLifecycleAndMultipleInstancesCreation() throws InterruptedException
    {
        final List<List<Counter>> metricsPerIteration = new ArrayList<>();
        int METRICS_COUNT = 50;
        int ITERATIONS_COUNT = 100;
        long TASKS_COUNT = 100_000;
        int THREADS = 20;
        boolean DESTROY_COUNTERS_AT_THE_END_OF_ITERATION = true;

        for (int iteration = 0; iteration < ITERATIONS_COUNT; iteration++)
        {
            {
                final List<Counter> metrics = new ArrayList<>();
                for (int i = 0; i < METRICS_COUNT; i++)
                    metrics.add(new ThreadLocalCounter());
                metricsPerIteration.add(metrics);
            }

            // note: these threads are FastThreadLocalThread instances
            // so, we recycle thread local metrics using io.netty.util.concurrent.FastThreadLocal.onRemoval
            LocalAwareExecutorPlus executor = executorFactory()
                                              .localAware()
                                              .pooled("executor-" + iteration, THREADS);

            for (int i = 0; i < TASKS_COUNT; i++)
            {
                executor.submit(() -> {
                    for (List<Counter> metricSet : metricsPerIteration)
                        for (Counter metric : metricSet)
                            metric.inc();
                });
            }
            boolean allIncremented = false;
            while (!allIncremented)
            {
                allIncremented = true;
                for (int metricSetId = 0; metricSetId < metricsPerIteration.size(); metricSetId++)
                    for (Counter metric : metricsPerIteration.get(metricSetId))
                        allIncremented &= TASKS_COUNT * (metricsPerIteration.size() - metricSetId) == metric.getCount();
            }
            assertEquals(THREADS, ThreadLocalMetrics.getThreadLocalMetricsObjectsCount());
            executor.shutdown();
            executor.awaitTermination(30, TimeUnit.SECONDS);
            for (int metricSetId = 0; metricSetId < metricsPerIteration.size(); metricSetId++)
                for (Counter metric : metricsPerIteration.get(metricSetId))
                    assertEquals(TASKS_COUNT * (metricsPerIteration.size() - metricSetId), metric.getCount());

            if (DESTROY_COUNTERS_AT_THE_END_OF_ITERATION)
            {
                metricsPerIteration.clear();
            }

            LOGGER.info("id generator state: {}, free IDs: {}",
                        ThreadLocalMetrics.idGenerator.get(),
                        ThreadLocalMetrics.freeMetricIdSet);
            LOGGER.info("iteration completed: {} / {}", iteration + 1, ITERATIONS_COUNT);
        }
    }

    @Test
    public void testBasicOperations()
    {
        Counter counter = new ThreadLocalCounter();
        counter.inc();
        assertEquals(1, counter.getCount());
        counter.inc(15);
        assertEquals(1 + 15, counter.getCount());
        counter.dec(10);
        assertEquals(1 + 15 - 10, counter.getCount());
        counter.dec();
        assertEquals(1 + 15 - 10 - 1, counter.getCount());
    }
}
