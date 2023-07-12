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

package org.apache.cassandra.service.throttler.dynamic;

import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import org.apache.cassandra.concurrent.SEPExecutor;
import org.apache.cassandra.concurrent.ScheduledExecutorPlus;
import org.apache.cassandra.concurrent.SharedExecutorPool;
import org.apache.cassandra.metrics.CassandraMetricsRegistry;
import org.apache.cassandra.metrics.DefaultNameFactory;
import org.apache.cassandra.metrics.MetricNameFactory;

import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;
import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;


public class CassandraResourceUtilization
{
    private static final Logger logger = LoggerFactory.getLogger(CassandraResourceUtilization.class);
    private final ScheduledExecutorPlus reportThread = executorFactory().scheduled(false, "CassandraResourceUtilization", Thread.MAX_PRIORITY);
    private static final DecimalFormat df = new DecimalFormat("0");

    // TODO: make this configurable
    private static final IResourceUtilzation resourceUtilzation = new NativeResourceUtilization();
    private static final MetricNameFactory factory = new DefaultNameFactory("CassandraResourceUtilization");

    public static final String READ_THREAD_POOL = "ReadStage";
    public static final String MUTATION_THREAD_POOL = "MutationStage";

    // Maintain 1 minute, 5 minutes, and 15 minutes history
    public static volatile Map<String, CpuUtilMetrics> cpuMetrics = new HashMap<>();
    public static volatile Meter pendingReadsHistory = CassandraMetricsRegistry.Metrics.meter(factory.createMetricName("PendingReads"));
    public static volatile Meter pendingMutationsHistory = CassandraMetricsRegistry.Metrics.meter(factory.createMetricName("PendingMutations"));
    public static volatile Meter nrThrottledHistory = CassandraMetricsRegistry.Metrics.meter(factory.createMetricName("NRThrottled"));
    public static Gauge<Integer> pendingReadsCur;
    public static Gauge<Integer> pendingMutationsCur;
    public static volatile Gauge<Long> nrThrottledCur;

    private volatile int pendingReadTaskCount;
    private volatile int pendingMutationTaskCount;
    private volatile long nrThrottledDelta;
    private long nrThrottledPrev = Long.MAX_VALUE;

    public void setup()
    {
        pendingReadsCur = Metrics.register(factory.createMetricName("PendingReadsCur"), new Gauge<Integer>()
        {
            public Integer getValue()
            {
                return pendingReadTaskCount;
            }
        });
        pendingMutationsCur = Metrics.register(factory.createMetricName("PendingMutationsCur"), new Gauge<Integer>()
        {
            public Integer getValue()
            {
                return pendingMutationTaskCount;
            }
        });
        nrThrottledCur = Metrics.register(factory.createMetricName("NRThrottledCur"), new Gauge<Long>()
        {
            public Long getValue()
            {
                return nrThrottledDelta;
            }
        });


        resourceUtilzation.setup();
        // TODO: make this configurable
        reportThread.scheduleAtFixedRate(() -> getCurrentUtilization(), 10, 1, TimeUnit.SECONDS);

        for (String cpuUtilType : resourceUtilzation.getCurrentCpuUtil().keySet())
        {
            cpuMetrics.putIfAbsent(cpuUtilType, new CpuUtilMetrics(cpuUtilType, factory));
        }
    }

    public void getCurrentUtilization()
    {
        StringBuilder sb = new StringBuilder();
        Map<String, Double> cpuUtil = resourceUtilzation.getCurrentCpuUtil();
        for (Map.Entry<String, Double> cpuUtilEntry : cpuUtil.entrySet())
        {
            CpuUtilMetrics oneCpuMetric = cpuMetrics.get(cpuUtilEntry.getKey());
            long currentCpuUtilization = Double.valueOf(cpuUtilEntry.getValue()).longValue();
            oneCpuMetric.cpuUtilHistory.mark(currentCpuUtilization);
            oneCpuMetric.currentCpuUtilization = currentCpuUtilization;

            sb.append(cpuUtilEntry.getKey()).append(": ").append(df.format(cpuUtilEntry.getValue())).
              append("-").append(df.format(oneCpuMetric.cpuUtilHistory.getOneMinuteRate())).
              append("-").append(df.format(oneCpuMetric.cpuUtilHistory.getFiveMinuteRate())).
              append("-").append(df.format(oneCpuMetric.cpuUtilHistory.getFifteenMinuteRate())).append(", ");
        }

        SEPExecutor readSEPTP = SharedExecutorPool.SHARED.getExecutor(READ_THREAD_POOL);
        SEPExecutor mutationSEPTP = SharedExecutorPool.SHARED.getExecutor(MUTATION_THREAD_POOL);
        if (readSEPTP != null)
        {
            pendingReadTaskCount = readSEPTP.getPendingTaskCount();
        }
        if (mutationSEPTP != null)
        {
            pendingMutationTaskCount = mutationSEPTP.getPendingTaskCount();
        }
        pendingReadsHistory.mark(pendingReadTaskCount);
        pendingMutationsHistory.mark(pendingMutationTaskCount);


        long nrThrottledNow = resourceUtilzation.getCpuNRThrottled();
        if (nrThrottledPrev != Long.MAX_VALUE )
        {
            nrThrottledDelta = nrThrottledNow - nrThrottledPrev;
        }
        nrThrottledPrev = nrThrottledNow;
        nrThrottledHistory.mark(nrThrottledDelta);

        sb.append("PendingReads").append(": ").append(pendingReadTaskCount).
          append("-").append(df.format(pendingReadsHistory.getOneMinuteRate())).
          append("-").append(df.format(pendingReadsHistory.getFiveMinuteRate())).
          append("-").append(df.format(pendingReadsHistory.getFifteenMinuteRate())).append(", ");
        sb.append("PendingMutations").append(": ").append(pendingMutationTaskCount).
          append("-").append(df.format(pendingMutationsHistory.getOneMinuteRate())).
          append("-").append(df.format(pendingMutationsHistory.getFiveMinuteRate())).
          append("-").append(df.format(pendingMutationsHistory.getFifteenMinuteRate()));
        sb.append("NRThrottled").append(": ").append(nrThrottledDelta).
          append("-").append(df.format(nrThrottledHistory.getOneMinuteRate())).
          append("-").append(df.format(nrThrottledHistory.getFiveMinuteRate())).
          append("-").append(df.format(nrThrottledHistory.getFifteenMinuteRate()));

        // TODO: Eventually, change this to Debug to avoid log flooding
        logger.info("CassandraResourceUtilization {}", sb);
    }
}
