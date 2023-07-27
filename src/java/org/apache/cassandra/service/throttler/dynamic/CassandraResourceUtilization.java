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
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.SEPExecutor;
import org.apache.cassandra.concurrent.ScheduledExecutorPlus;
import org.apache.cassandra.concurrent.SharedExecutorPool;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.metrics.KeyspaceMetrics;
import org.apache.cassandra.service.throttler.dynamic.metrics.KeyspaceThrottlingMetrics;
import org.apache.cassandra.service.throttler.dynamic.metrics.ThrottlingMetrics;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.service.throttler.dynamic.metrics.KeyspaceThrottlingMetricsManager;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;


public class CassandraResourceUtilization
{
    private static final Logger logger = LoggerFactory.getLogger(CassandraResourceUtilization.class);
    private final ScheduledExecutorPlus reportThread = executorFactory().scheduled(false, "CassandraResourceUtilization", Thread.MAX_PRIORITY);
    private static final DecimalFormat df = new DecimalFormat("0");

    // TODO: make this configurable
    public final IResourceUtilzation resourceUtilzation = new NativeResourceUtilization();

    private static final String READ_THREAD_POOL = "ReadStage";
    private static final String MUTATION_THREAD_POOL = "MutationStage";
    private static double MAX_THROTTLING = 1.0;

    // Maintain 1 minute, 5 minutes, and 15 minutes history
    public long nrThrottled1Prev = -1;
    public long nrThrottled2Prev = -1;

    public ResourcesStats resourcesStats;
    public ThrottlingOptions throttlingOptions;
    public ThrottlingMetrics throttlingMetrics;
    public volatile long lastThrottlingCheckPointTimeInMS = 0;
    public volatile long lastThrottlingIndicatorTimeInMS = 0;
    public volatile double throttlingPercentageCur = 0.1;
    public volatile Set aggressiveThorttlingDatastores = new HashSet<>();
    public volatile boolean shouldThrottle = false;

    public void setup(boolean continuousHealthCheck)
    {
        resourcesStats = new ResourcesStats();
        throttlingOptions = new ThrottlingOptions();
        throttlingMetrics = new ThrottlingMetrics();
        resourceUtilzation.setup();
        if (continuousHealthCheck)
        {
            // TODO: make this configurable
            reportThread.scheduleAtFixedRate(() -> fetchCurrentHealth(), 10, 1, TimeUnit.SECONDS);
        }
    }

    public void fetchCurrentHealth()
    {
        resourcesStats.setCpuUtil1(resourceUtilzation.getCurrentCpuUtil1());
        resourcesStats.setCpuUtil2(resourceUtilzation.getCurrentCpuUtil2());

        SEPExecutor readSEPTP = SharedExecutorPool.SHARED.getExecutor(READ_THREAD_POOL);
        if (readSEPTP != null)
        {
            resourcesStats.setPendingReads(readSEPTP.getPendingTaskCount());
        }
        SEPExecutor mutationSEPTP = SharedExecutorPool.SHARED.getExecutor(MUTATION_THREAD_POOL);
        if (mutationSEPTP != null)
        {
            resourcesStats.setPendingMutations(mutationSEPTP.getPendingTaskCount());
        }
        long nrThrottled1Now = resourceUtilzation.getCpuNRThrottled1();
        if (nrThrottled1Prev != -1)
        {
            resourcesStats.setNrThrottled1(nrThrottled1Now - nrThrottled1Prev);
        }
        nrThrottled1Prev = nrThrottled1Now;

        long nrThrottled2Now = resourceUtilzation.getCpuNRThrottled2();
        if (nrThrottled2Prev != -1)
        {
            resourcesStats.setNrThrottled2(nrThrottled2Now - nrThrottled2Prev);
        }
        nrThrottled2Prev = nrThrottled2Now;

        shouldThrottle();
        adjustThrottling();

        // TODO: Eventually, change this to Debug to avoid log flooding
        logger.info("CassandraResourceUtilization {}", this);
    }

    public void shouldThrottle()
    {
        boolean cpuUtilSignal1 = false;
        if (resourcesStats.getCpuUtil1Cur() >= throttlingOptions.cpu_threshold_cur && resourcesStats.getCpuUtil1OneMinute() >= throttlingOptions.cpu_threshold_one_minute)
        {
            cpuUtilSignal1 = true;
        }
        boolean cpuUtilSignal2 = false;
        if (resourcesStats.getCpuUtil2Cur() == -1 || (resourcesStats.getCpuUtil2Cur() >= throttlingOptions.cpu_threshold_cur && resourcesStats.getCpuUtil2OneMinute() >= throttlingOptions.cpu_threshold_one_minute))
        {
            cpuUtilSignal2 = true;
        }
        boolean nrThrottlingSignal1 = false;
        if (resourcesStats.getNrThrottled1Cur() >= throttlingOptions.nr_throttling_threshold_cur && resourcesStats.getNrThrottled1OneMinute() >= throttlingOptions.nr_throttling_threshold_one_minute)
        {
            nrThrottlingSignal1 = true;
        }
        boolean nrThrottlingSignal2 = false;
        if (resourcesStats.getNrThrottled2Cur() == -1 || (resourcesStats.getNrThrottled2Cur() >= throttlingOptions.nr_throttling_threshold_cur && resourcesStats.getNrThrottled2OneMinute() >= throttlingOptions.nr_throttling_threshold_one_minute))
        {
            nrThrottlingSignal2 = true;
        }
        boolean pendingReadsSignal = false;
        if (resourcesStats.getPendingReadsCur() >= throttlingOptions.pending_reads_threshold_cur && resourcesStats.getPendingReadsOneMinute() >= throttlingOptions.pending_reads_threshold_one_minute)
        {
            pendingReadsSignal = true;
        }
        boolean pendingMutationsSignal = false;
        if (resourcesStats.getPendingMutationsCur() >= throttlingOptions.pending_mutations_threshold_cur && resourcesStats.getPendingMutationsOneMinute() >= throttlingOptions.pending_mutations_threshold_one_minute)
        {
            pendingMutationsSignal = true;
        }
        if (cpuUtilSignal1 && cpuUtilSignal2 && nrThrottlingSignal1 && nrThrottlingSignal2 && pendingReadsSignal && pendingMutationsSignal)
        {
            shouldThrottle = true;
            lastThrottlingIndicatorTimeInMS = System.currentTimeMillis();
            throttlingMetrics.needsThrottling.inc();
        }
        else
        {
            shouldThrottle = false;
            throttlingMetrics.doesNotNeedThrottling.inc();
        }
    }

    public void adjustThrottling()
    {
        if (lastThrottlingCheckPointTimeInMS != 0 && MILLISECONDS.toSeconds(System.currentTimeMillis() - lastThrottlingCheckPointTimeInMS) >= throttlingOptions.more_aggressive_throttling_after_in_sec)
        {
            if (MILLISECONDS.toSeconds(System.currentTimeMillis() - lastThrottlingCheckPointTimeInMS) >= throttlingOptions.reset_after_no_throttling_seen_in_sec)
            {
                throttlingMetrics.resetThrottling.inc();
                logger.info("Reset everything....");
                // reset everything as the system seems to have recovered
                lastThrottlingCheckPointTimeInMS = 0;
                lastThrottlingIndicatorTimeInMS = 0;
                throttlingPercentageCur = throttlingOptions.percentage_of_traffice_to_throttling;
                aggressiveThorttlingDatastores.clear();
                shouldThrottle = false;
            }
            else if (lastThrottlingIndicatorTimeInMS != 0)
            {
                lastThrottlingCheckPointTimeInMS = lastThrottlingIndicatorTimeInMS;
                if (throttlingPercentageCur < MAX_THROTTLING)
                {
                    throttlingMetrics.doubleThrottling.inc();
                    // more aggressive throttling
                    double previous = throttlingPercentageCur;
                    throttlingPercentageCur = Math.min(MAX_THROTTLING, throttlingPercentageCur * 2);
                    logger.info("Double min throttling previous: {}, now: {}", previous, throttlingPercentageCur);
                }
            }
        }
        if (lastThrottlingCheckPointTimeInMS == 0 && lastThrottlingIndicatorTimeInMS != 0)
        {
            lastThrottlingCheckPointTimeInMS = lastThrottlingIndicatorTimeInMS;
        }
    }

    public String toString()
    {
        StringBuilder sb = new StringBuilder();

        sb.append("CpuUtil1: ").append(df.format(resourcesStats.getCpuUtil1Cur())).
          append("-").append(df.format(resourcesStats.getCpuUtil1OneMinute())).
          append("-").append(df.format(resourcesStats.getCpuUtil1FiveMinute())).
          append("-").append(df.format(resourcesStats.getCpuUtil1FifteenMinute())).

          append(", CpuUtil2: ").append(df.format(resourcesStats.getCpuUtil2Cur())).
          append("-").append(df.format(resourcesStats.getCpuUtil2OneMinute())).
          append("-").append(df.format(resourcesStats.getCpuUtil2FiveMinute())).
          append("-").append(df.format(resourcesStats.getCpuUtil2FifteenMinute())).

          append(", NrThrottled1: ").append(df.format(resourcesStats.getNrThrottled1Cur())).
          append("-").append(df.format(resourcesStats.getNrThrottled1OneMinute())).
          append("-").append(df.format(resourcesStats.getNrThrottled1FiveMinute())).
          append("-").append(df.format(resourcesStats.getNrThrottled1FifteenMinute())).

          append(", NrThrottled2: ").append(df.format(resourcesStats.getNrThrottled2Cur())).
          append("-").append(df.format(resourcesStats.getNrThrottled2OneMinute())).
          append("-").append(df.format(resourcesStats.getNrThrottled2FiveMinute())).
          append("-").append(df.format(resourcesStats.getNrThrottled2FifteenMinute())).

          append(", PendingReads: ").append(df.format(resourcesStats.getPendingReadsCur())).
          append("-").append(df.format(resourcesStats.getPendingReadsOneMinute())).
          append("-").append(df.format(resourcesStats.getPendingReadsFiveMinute())).
          append("-").append(df.format(resourcesStats.getPendingReadsFifteenMinute())).

          append(", PendingMutations: ").append(df.format(resourcesStats.getPendingMutationsCur())).
          append("-").append(df.format(resourcesStats.getPendingMutationsOneMinute())).
          append("-").append(df.format(resourcesStats.getPendingMutationsFiveMinute())).
          append("-").append(df.format(resourcesStats.getPendingMutationsFifteenMinute()));

        return sb.toString();
    }

    public boolean throttleUserTraffic(String keyspaceName, boolean reads)
    {
        if (!throttlingOptions.enabled)
        {
            throttlingMetrics.disableThrottling.inc();
            logger.info("Throttling is disabled reads: {}....", reads);
            return false;
        }
        KeyspaceThrottlingMetrics ksThrottlingMetrics = KeyspaceThrottlingMetricsManager.getMetrics(keyspaceName);
        if (SchemaConstants.isSystemKeyspace(keyspaceName))
        {
            ksThrottlingMetrics.skipSystemKSThrottling.inc();
            return false;
        }

        KeyspaceMetrics metrics = Keyspace.open(keyspaceName).metric;
        if (shouldThrottle)
        {
            return decideThrottling(keyspaceName, metrics, reads, ksThrottlingMetrics);
        }
        return false;
    }

    public boolean decideThrottling(String ksName, KeyspaceMetrics metrics, boolean reads, KeyspaceThrottlingMetrics ksThrottlingMetrics)
    {
        if (throttlingPercentageCur < MAX_THROTTLING && !aggressiveThorttlingDatastores.contains(ksName.toLowerCase()))
        {
            if (spikeInRequestRate(ksName, metrics, reads, ksThrottlingMetrics) || spikeInLatency(ksName, metrics, reads, ksThrottlingMetrics))
            {
                // if we find that there is a keyspace, which is the root cause, then throttle it more aggressively
                aggressiveThorttlingDatastores.add(ksName.toLowerCase());
                ksThrottlingMetrics.addKSForThrottling.inc();
                return true;
            }
            if (ThreadLocalRandom.current().nextDouble() <= throttlingPercentageCur)
            {
                ksThrottlingMetrics.minThrottling.inc();
                logger.info("Do minimum throttling keyspace: {}", ksName);
                return true;
            }
            else
            {
                ksThrottlingMetrics.noThrottling.inc();
                logger.info("Do no throttling keyspace: {}", ksName);
                return false;
            }
        }
        ksThrottlingMetrics.maxThrottling.inc();
        return true;
    }

    public boolean spikeInRequestRate(String ksName, KeyspaceMetrics metrics, boolean reads, KeyspaceThrottlingMetrics ksThrottlingMetrics)
    {
        double oneMinuteRate = 0.0;
        double fiveMinuteRate = 0.0;
        double fifteenMinuteRate = 0.0;

        if (reads)
        {
            oneMinuteRate = metrics.readLatency.latency.getOneMinuteRate();
            fiveMinuteRate = metrics.readLatency.latency.getFiveMinuteRate();
            fifteenMinuteRate = metrics.readLatency.latency.getFifteenMinuteRate();
        }
        else
        {
            oneMinuteRate = metrics.writeLatency.latency.getOneMinuteRate();
            fiveMinuteRate = metrics.writeLatency.latency.getFiveMinuteRate();
            fifteenMinuteRate = metrics.writeLatency.latency.getFifteenMinuteRate();
        }
        boolean trendingUp = isTrendingUpward(oneMinuteRate, fiveMinuteRate, fifteenMinuteRate);
        if (trendingUp)
        {
            ksThrottlingMetrics.requestsTrendingUpward.inc();
            double ratio = oneMinuteRate / fifteenMinuteRate;
            logger.info("Trending requests upward keyspace: {}, ratio: {}, oneMinuteRate: {}, fiveMinuteRate: {}, fifteenMinuteRate: {}",
                        ksName, ratio, oneMinuteRate, fiveMinuteRate, fifteenMinuteRate);
            if (ratio >= throttlingOptions.aggressive_throttling_qps_ratio)
            {
                return true;
            }
        }
        return false;
    }

    public boolean spikeInLatency(String ksName, KeyspaceMetrics metrics, boolean reads, KeyspaceThrottlingMetrics ksThrottlingMetrics)
    {
        double oneMinuteRate = 0.0;
        double fiveMinuteRate = 0.0;
        double fifteenMinuteRate = 0.0;

        if (reads)
        {
            oneMinuteRate = metrics.readLatency.latencyMeter.getOneMinuteRate();
            fiveMinuteRate = metrics.readLatency.latencyMeter.getFiveMinuteRate();
            fifteenMinuteRate = metrics.readLatency.latencyMeter.getFifteenMinuteRate();
        }
        else
        {
            oneMinuteRate = metrics.writeLatency.latencyMeter.getOneMinuteRate();
            fiveMinuteRate = metrics.writeLatency.latencyMeter.getFiveMinuteRate();
            fifteenMinuteRate = metrics.writeLatency.latencyMeter.getFifteenMinuteRate();
        }
        boolean trendingUp = isTrendingUpward(oneMinuteRate, fiveMinuteRate, fifteenMinuteRate);
        if (trendingUp)
        {
            ksThrottlingMetrics.latencyTrendingUpward.inc();
            double ratio = oneMinuteRate / fifteenMinuteRate;
            logger.info("Trending latency upward keyspace: {}, ratio: {}, oneMinuteRate: {}, fiveMinuteRate: {}, fifteenMinuteRate: {}",
                        ksName, ratio, oneMinuteRate, fiveMinuteRate, fifteenMinuteRate);
            if (ratio >= throttlingOptions.aggressive_throttling_latency_ratio)
            {
                return true;
            }
        }
        return false;
    }

    public static boolean isTrendingUpward(double a, double b, double c)
    {
        if (a > b && b > c)
        {
            return true;
        }
        return false;
    }
}
