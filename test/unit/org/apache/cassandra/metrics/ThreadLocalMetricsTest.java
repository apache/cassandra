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
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.LocalAwareExecutorPlus;

import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;
import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class ThreadLocalMetricsTest
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ThreadLocalMetricsTest.class);

    @Parameterized.Parameter
    public CounterSource counterSource;

    @Parameterized.Parameters(name = "{0}")
    public static List<CounterSource> parameters()
    {
        List<CounterSource> parameters = new ArrayList<>();
        parameters.add(new CounterSource()
        {
            @Override
            public CounterMetric createCounter()
            {
                return LazySetArrayThreadLocalMetrics.createCounter();
            }

            @Override
            public void destroyCounter(CounterMetric counterMetric)
            {
                LazySetArrayThreadLocalMetrics.destroyCounter(counterMetric);
            }

            @Override
            public void printDiagnostic()
            {
                LOGGER.info("id generator state: {}, free IDs: {}",
                            LazySetArrayThreadLocalMetrics.idGenerator.get(),
                            LazySetArrayThreadLocalMetrics.freeMetricIdSet);
            }

            public String toString()
            {
                return LazySetArrayThreadLocalMetrics.class.getSimpleName();
            }
        });
        parameters.add(new CounterSource()
        {
            @Override
            public CounterMetric createCounter()
            {
                return PiggybackArrayThreadLocalMetrics.createCounter();
            }

            @Override
            public void destroyCounter(CounterMetric counterMetric)
            {
                PiggybackArrayThreadLocalMetrics.destroyCounter(counterMetric);
            }

            @Override
            public void printDiagnostic()
            {
                LOGGER.info("id generator state: {}, free IDs: {}",
                            PiggybackArrayThreadLocalMetrics.idGenerator.get(),
                            PiggybackArrayThreadLocalMetrics.freeMetricIdSet);
            }
            public String toString()
            {
                return PiggybackArrayThreadLocalMetrics.class.getSimpleName();
            }
        });
        return parameters;
    }

    public interface CounterSource
    {
        CounterMetric createCounter();
        void destroyCounter(CounterMetric counterMetric);

        void printDiagnostic();
    }

    @Test
    public void test() throws InterruptedException
    {
        final List<List<CounterMetric>> metricsPerIteration = new ArrayList<>();
        int METRICS_COUNT = 50;
        int ITERATIONS_COUNT = 50;
        long TASKS_COUNT = 100_000;
        int THREADS = 10;
        boolean DESTROY_COUNTERS_AT_THE_END_OF_ITERATION = false;

        for (int iteration = 0; iteration < ITERATIONS_COUNT; iteration++)
        {
            {
                final List<CounterMetric> metrics = new ArrayList<>();
                for (int i = 0; i < METRICS_COUNT; i++)
                    metrics.add(counterSource.createCounter());
                metricsPerIteration.add(metrics);
            }

            LocalAwareExecutorPlus executor = executorFactory()
                                              .localAware()
                                              .pooled("executor-" + iteration, THREADS);

            for (int i = 0; i < TASKS_COUNT; i++)
            {
                executor.submit(() -> {
                    for (List<CounterMetric> metricSet : metricsPerIteration)
                        for (CounterMetric metric : metricSet)
                            metric.inc();
                });
            }
            boolean allIncremented = false;
            while (!allIncremented)
            {
                allIncremented = true;
                for (int metricSetId = 0; metricSetId < metricsPerIteration.size(); metricSetId++)
                    for (CounterMetric metric : metricsPerIteration.get(metricSetId))
                        allIncremented &= TASKS_COUNT * (metricsPerIteration.size() - metricSetId) == metric.getCount();
            }
            executor.shutdown();
            executor.awaitTermination(30, TimeUnit.SECONDS);
            for (int metricSetId = 0; metricSetId < metricsPerIteration.size(); metricSetId++)
                for (CounterMetric metric : metricsPerIteration.get(metricSetId))
                    assertEquals(TASKS_COUNT * (metricsPerIteration.size() - metricSetId), metric.getCount());

            if (DESTROY_COUNTERS_AT_THE_END_OF_ITERATION)
            {
                for (int metricSetId = 0; metricSetId < metricsPerIteration.size(); metricSetId++)
                    for (CounterMetric metric : metricsPerIteration.get(metricSetId))
                        counterSource.destroyCounter(metric);
                metricsPerIteration.clear();
            }

            counterSource.printDiagnostic();
            LOGGER.info("iteration completed: {} / {}", iteration + 1, ITERATIONS_COUNT);
        }
    }
}
