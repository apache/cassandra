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

import java.net.InetAddress;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.Date;
import java.util.Map;
import java.util.TimeZone;

import com.codahale.metrics.Gauge;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.OverloadedException;
import org.apache.cassandra.service.RateLimiterService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.SEPExecutor;
import org.apache.cassandra.concurrent.ScheduledExecutorPlus;
import org.apache.cassandra.concurrent.SharedExecutorPool;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.metrics.KeyspaceMetrics;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.throttler.dynamic.metrics.KeyspaceThrottlingMetrics;
import org.apache.cassandra.service.throttler.dynamic.metrics.ThrottlingMetrics;
import org.apache.cassandra.service.throttler.dynamic.metrics.KeyspaceThrottlingMetricsManager;
import org.apache.cassandra.utils.FBUtilities;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;
import static org.apache.cassandra.transport.Dispatcher.NATIVE_TRANSPORT_THREAD_POOL;

import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;


public class CassandraResourceUtilization
{
    private static final Logger logger = LoggerFactory.getLogger(CassandraResourceUtilization.class);
    private static final DecimalFormat df = new DecimalFormat("0");
    private static final String THROW_MESSAGE = "from dynamic throttler";
    // TODO: make this configurable
    private final IResourceUtilzation resourceUtilzation = new NativeResourceUtilization();
    private static final String READ_THREAD_POOL = "ReadStage";
    private static final String MUTATION_THREAD_POOL = "MutationStage";
    private static double MAX_THROTTLING = 1.0;

    public volatile double currentThrottlingPercentage;
    public ScheduledExecutorPlus reportThread = executorFactory().scheduled(false, "CassandraResourceUtilization", Thread.MAX_PRIORITY);
    public ResourcesStats resourcesStats;
    public ThrottlingMetrics throttlingMetrics;
    public ThrottlingOptions throttlingOptions;
    public long lastThrottlingCheckPointTimeInMS = 0;
    public long lastThrottlingIndicatorTimeInMS = 0;
    public Map<String, Boolean> readAggressiveThorttlingKeyspaces = new ConcurrentHashMap<>();
    public Map<String, Boolean> mutationAggressiveThorttlingKeyspaces = new ConcurrentHashMap<>();
    public volatile boolean shouldThrottle = false;

    public static CassandraResourceUtilization instance = new CassandraResourceUtilization();

    private CassandraResourceUtilization()
    {
        RateLimiterService.instance.setThrottlingOptions(DatabaseDescriptor.getThrottlingOptions());
        throttlingOptions = RateLimiterService.instance.getThrottlingOptions();
        throttlingMetrics = new ThrottlingMetrics();
        throttlingMetrics.currentThrottlingPercentage = Metrics.register(ThrottlingMetrics.factory.createMetricName("CurrentThrottlingPercentage"), new Gauge<Double>()
        {
            public Double getValue()
            {
                return currentThrottlingPercentage;
            }
        });
        currentThrottlingPercentage = throttlingOptions.getPercentageOfTrafficToThrottling();
        resourcesStats = new ResourcesStats();
    }

    public void setup(boolean continuousHealthCheck)
    {
        resourceUtilzation.setup();
        if (continuousHealthCheck)
        {
            reportThread.scheduleAtFixedRate(() -> fetchCurrentHealth(), throttlingOptions.getHealthCheckInitDelayInSec(), throttlingOptions.getHealthCheckPeriodInSec(), TimeUnit.SECONDS);
        }
    }

    public void fetchCurrentHealth()
    {
        // make throttling decisions only when Cassandra is normal
        if (StorageService.instance.isNormal())
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
            SEPExecutor nativeTransportSEPTP = SharedExecutorPool.SHARED.getExecutor(NATIVE_TRANSPORT_THREAD_POOL);
            if (nativeTransportSEPTP != null)
            {
                resourcesStats.setPendingNativeTransport(nativeTransportSEPTP.getPendingTaskCount());
            }
            checkSignals();
            adjustThrottling();

            // TODO: Eventually, change this to Debug to avoid log flooding
            logger.info("CassandraResourceUtilization {}", this);
        }
    }

    public void checkSignals()
    {
        boolean cpuUtilSignal1 = false;
        if (resourcesStats.getCpuUtil1OneMinute() >= throttlingOptions.getCpuThresholdOneMinute())
        {
            cpuUtilSignal1 = true;
        }
        boolean cpuUtilSignal2 = false;
        if (resourcesStats.getCpuUtil2Cur() == -1 || (resourcesStats.getCpuUtil2OneMinute() >= throttlingOptions.getCpuThresholdOneMinute()))
        {
            cpuUtilSignal2 = true;
        }
        boolean pendingReadsSignal = false;
        if (resourcesStats.getPendingReadsCur() >= throttlingOptions.getPendingReadsThresholdCur() && resourcesStats.getPendingReadsOneMinute() >= throttlingOptions.getPendingReadsThresholdOneMinute())
        {
            pendingReadsSignal = true;
        }
        boolean pendingMutationsSignal = false;
        if (resourcesStats.getPendingMutationsCur() >= throttlingOptions.getPendingMutationsThresholdCur() && resourcesStats.getPendingMutationsOneMinute() >= throttlingOptions.getPendingMutationsThresholdOneMinute())
        {
            pendingMutationsSignal = true;
        }
        boolean pendingNativeTransportSignal = false;
        if (resourcesStats.getPendingNativeTransportCur() >= throttlingOptions.getPendingNativeTransportThresholdCur() && resourcesStats.getPendingNativeTransportOneMinute() >= throttlingOptions.getPendingNativeTransportThresholdOneMinute())
        {
            pendingNativeTransportSignal = true;
        }
        if (cpuUtilSignal1 && cpuUtilSignal2 && (pendingReadsSignal || pendingMutationsSignal || pendingNativeTransportSignal))
        {
            shouldThrottle = true;
            lastThrottlingIndicatorTimeInMS = System.currentTimeMillis();
            throttlingMetrics.needsThrottling.inc();
            logger.info("Enforce throttling CpuUtil1: {}-{}, CpuUtil2: {}-{}, PendingReads: {}-{}, PendingMutations: {}-{}, PendingNativeTransportSignal: {}-{}",
                        resourcesStats.getCpuUtil1Cur(), resourcesStats.getCpuUtil1OneMinute(),
                        resourcesStats.getCpuUtil2Cur(), resourcesStats.getCpuUtil2OneMinute(),
                        resourcesStats.getPendingReadsCur(), resourcesStats.getPendingReadsOneMinute(),
                        resourcesStats.getPendingMutationsCur(), resourcesStats.getPendingMutationsOneMinute(),
                        resourcesStats.getPendingNativeTransportCur(), resourcesStats.getPendingNativeTransportOneMinute());
        }
        else
        {
            shouldThrottle = false;
            throttlingMetrics.doesNotNeedThrottling.inc();
            logger.info("DO NOT Enforce throttling CpuUtil1: {}-{}-{}, CpuUtil2: {}-{}-{}, PendingReads: {}-{}-{}, PendingMutations: {}-{}-{}, PendingNativeTransportSignal: {}-{}",
                        cpuUtilSignal1, resourcesStats.getCpuUtil1Cur(), resourcesStats.getCpuUtil1OneMinute(),
                        cpuUtilSignal2, resourcesStats.getCpuUtil2Cur(), resourcesStats.getCpuUtil2OneMinute(),
                        pendingReadsSignal, resourcesStats.getPendingReadsCur(), resourcesStats.getPendingReadsOneMinute(),
                        pendingMutationsSignal, resourcesStats.getPendingMutationsCur(), resourcesStats.getPendingMutationsOneMinute(),
                        resourcesStats.getPendingNativeTransportCur(), resourcesStats.getPendingNativeTransportOneMinute());
        }
    }

    public void resetThrottlingParams()
    {
        logger.info("Reset everything....");
        throttlingMetrics.resetThrottling.inc();
        // reset everything as the system seems to have recovered
        lastThrottlingCheckPointTimeInMS = 0;
        lastThrottlingIndicatorTimeInMS = 0;
        currentThrottlingPercentage = throttlingOptions.getPercentageOfTrafficToThrottling();
        readAggressiveThorttlingKeyspaces.clear();
        mutationAggressiveThorttlingKeyspaces.clear();
        shouldThrottle = false;
    }

    public void adjustThrottling()
    {
        if (lastThrottlingCheckPointTimeInMS != 0 && MILLISECONDS.toSeconds(System.currentTimeMillis() - lastThrottlingCheckPointTimeInMS) >= throttlingOptions.getMoreAggressiveThrottlingAfterInSec())
        {
            if (MILLISECONDS.toSeconds(System.currentTimeMillis() - lastThrottlingCheckPointTimeInMS) >= throttlingOptions.getResetAfterNoThrottlingSeenInSec())
            {
                resetThrottlingParams();
            }
            else if (lastThrottlingIndicatorTimeInMS != 0)
            {
                lastThrottlingCheckPointTimeInMS = lastThrottlingIndicatorTimeInMS;
                if (currentThrottlingPercentage < MAX_THROTTLING)
                {
                    // more aggressive throttling
                    throttlingMetrics.increaseThrottling.inc();
                    double previous = currentThrottlingPercentage;
                    currentThrottlingPercentage = Math.min(MAX_THROTTLING, previous + throttlingOptions.getPercentageOfTrafficToThrottling());
                    logger.info("Increase min throttling previous: {}, now: {}", previous, currentThrottlingPercentage);
                }
            }
        }
        if (lastThrottlingCheckPointTimeInMS == 0 && lastThrottlingIndicatorTimeInMS != 0)
        {
            lastThrottlingCheckPointTimeInMS = lastThrottlingIndicatorTimeInMS;
        }
        if (lastThrottlingCheckPointTimeInMS == 0 && lastThrottlingIndicatorTimeInMS == 0 && currentThrottlingPercentage != throttlingOptions.getPercentageOfTrafficToThrottling())
        {
            // if we are not having any throttling, and in between, an operator adjusts the throttling percentage
            // then we need to adjust the value accordingly
            currentThrottlingPercentage = throttlingOptions.getPercentageOfTrafficToThrottling();
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

          append(", PendingReads: ").append(df.format(resourcesStats.getPendingReadsCur())).
          append("-").append(df.format(resourcesStats.getPendingReadsOneMinute())).
          append("-").append(df.format(resourcesStats.getPendingReadsFiveMinute())).
          append("-").append(df.format(resourcesStats.getPendingReadsFifteenMinute())).

          append(", PendingMutations: ").append(df.format(resourcesStats.getPendingMutationsCur())).
          append("-").append(df.format(resourcesStats.getPendingMutationsOneMinute())).
          append("-").append(df.format(resourcesStats.getPendingMutationsFiveMinute())).
          append("-").append(df.format(resourcesStats.getPendingMutationsFifteenMinute())).
          append(", PendingNativeTransport: ").append(df.format(resourcesStats.getPendingNativeTransportCur())).
          append("-").append(df.format(resourcesStats.getPendingNativeTransportOneMinute())).
          append("-").append(df.format(resourcesStats.getPendingNativeTransportFiveMinute())).
          append("-").append(df.format(resourcesStats.getPendingNativeTransportFifteenMinute())).
          append(", LastThrottlingCheckPointTimeInMS: ").append(convertEpochTimeToUTC(lastThrottlingCheckPointTimeInMS)).
          append(", LastThrottlingIndicatorTimeInMS: ").append(convertEpochTimeToUTC(lastThrottlingIndicatorTimeInMS)).
          append(", CurrentThrottlingPercentage: ").append(currentThrottlingPercentage);

        return sb.toString();
    }

    public boolean throttleUserTraffic(String keyspaceName, boolean reads, boolean replicationTraffic)
    {
        if (!throttlingOptions.isEnabled())
        {
            throttlingMetrics.disableThrottling.inc();
            return false;
        }
        if (replicationTraffic && !throttlingOptions.getThrottleReplicaTraffic())
        {
            throttlingMetrics.disableReplicaTrafficThrottling.inc();
            return false;
        }
        KeyspaceThrottlingMetrics ksThrottlingMetrics = KeyspaceThrottlingMetricsManager.getMetrics(keyspaceName);
        if (throttlingOptions.getIgnoreKeyspacesPattern().matcher(keyspaceName.toLowerCase()).matches())
        {
            ksThrottlingMetrics.skipKSThrottling.inc();
            return false;
        }
        KeyspaceMetrics metrics = Keyspace.open(keyspaceName).metric;
        if (shouldThrottle)
        {
            return decideThrottling(keyspaceName, metrics, reads, ksThrottlingMetrics);
        }
        return false;
    }

    public void throttle(String keyspaceName, boolean reads, boolean replicationTraffic) throws OverloadedException
    {
        if (throttleUserTraffic(keyspaceName, reads, replicationTraffic)) {
            throw buildOverloadeExceptionDuetoRateLimiter();
        }
    }

    public boolean decideThrottling(String ksName, KeyspaceMetrics metrics, boolean reads, KeyspaceThrottlingMetrics ksThrottlingMetrics)
    {
        double throttlingPercentage = currentThrottlingPercentage;
        if (throttlingPercentage < MAX_THROTTLING)
        {
            if ((reads && readAggressiveThorttlingKeyspaces.containsKey(ksName.toLowerCase())) || (!reads && mutationAggressiveThorttlingKeyspaces.containsKey(ksName.toLowerCase())))
            {
                throttlingPercentage = Math.min(MAX_THROTTLING, throttlingPercentage + throttlingOptions.getPercentageOfTrafficToThrottling());
                ksThrottlingMetrics.aggressiveThrottling.inc();
            }
            else if(spikeInRequestRate(ksName, metrics, reads, ksThrottlingMetrics) || spikeInLatency(ksName, metrics, reads, ksThrottlingMetrics))
            {
                // if we find that there is a keyspace, which is the root cause, then throttle it more aggressively
                if (reads)
                {
                    ksThrottlingMetrics.addKSForReadThrottling.inc();
                    readAggressiveThorttlingKeyspaces.put(ksName.toLowerCase(), true);
                }
                else
                {
                    ksThrottlingMetrics.addKSForWriteThrottling.inc();
                    mutationAggressiveThorttlingKeyspaces.put(ksName.toLowerCase(), true);
                }
            }
        }
        if (throttlingPercentage < MAX_THROTTLING)
        {
            if (ThreadLocalRandom.current().nextDouble() <= throttlingPercentage)
            {
                if (reads)
                {
                    ksThrottlingMetrics.minReadThrottling.inc();
                }
                else
                {
                    ksThrottlingMetrics.minWriteThrottling.inc();
                }
                logger.info("Do minimum throttling read op: {}, percentage: {}, keyspace: {}", reads, throttlingPercentage, ksName);
                return true;
            }
            else
            {
                if (reads)
                {
                    ksThrottlingMetrics.noReadThrottling.inc();
                }
                else
                {
                    ksThrottlingMetrics.noWriteThrottling.inc();
                }
                logger.info("Do no throttling read op: {}, keyspace: {}", reads, ksName);
                return false;
            }
        }
        if (reads)
        {
            ksThrottlingMetrics.maxReadThrottling.inc();
        }
        else
        {
            ksThrottlingMetrics.maxWriteThrottling.inc();
        }
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
            if (reads)
            {
                ksThrottlingMetrics.readRequestsTrendingUpward.inc();
            }
            else
            {
                ksThrottlingMetrics.writeRequestsTrendingUpward.inc();
            }
            double ratio = oneMinuteRate;
            if (Double.compare(fifteenMinuteRate, 0.0) != 0)
            {
                ratio = oneMinuteRate / fifteenMinuteRate;
            }
            logger.info("Trending requests upward read op: {} keyspace: {}, ratio: {}, oneMinuteRate: {}, fiveMinuteRate: {}, fifteenMinuteRate: {}",
                        reads, ksName, ratio, oneMinuteRate, fiveMinuteRate, fifteenMinuteRate);
            if (ratio >= throttlingOptions.getAggressiveThrottlingQpsRatio())
            {
                logger.info("Trending qualified latency upward read op: {}, keyspace: {}, ratio: {}, oneMinuteRate: {}, fiveMinuteRate: {}, fifteenMinuteRate: {}",
                            reads, ksName, ratio, oneMinuteRate, fiveMinuteRate, fifteenMinuteRate);
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
            if (reads)
            {
                ksThrottlingMetrics.readLatencyTrendingUpward.inc();
            }
            else
            {
                ksThrottlingMetrics.writeLatencyTrendingUpward.inc();
            }
            double ratio = oneMinuteRate;
            if (Double.compare(fifteenMinuteRate, 0.0) != 0)
            {
                ratio = oneMinuteRate / fifteenMinuteRate;
            }
            logger.info("Trending latency upward read op: {}, keyspace: {}, ratio: {}, oneMinuteRate: {}, fiveMinuteRate: {}, fifteenMinuteRate: {}",
                        reads, ksName, ratio, oneMinuteRate, fiveMinuteRate, fifteenMinuteRate);
            if (ratio >= throttlingOptions.getAggressiveThrottlingLatencyRatio())
            {
                logger.info("Trending qualified latency upward read op: {}, keyspace: {}, ratio: {}, oneMinuteRate: {}, fiveMinuteRate: {}, fifteenMinuteRate: {}",
                            reads, ksName, ratio, oneMinuteRate, fiveMinuteRate, fifteenMinuteRate);
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

    public static OverloadedException buildOverloadeExceptionDuetoRateLimiter()
    {
        return buildOverloadeExceptionDuetoRateLimiter(FBUtilities.getJustLocalAddress().getHostAddress());
    }

    public static OverloadedException buildOverloadeExceptionDuetoRateLimiter(String ip)
    {
        return new OverloadedException(String.format(THROW_MESSAGE + ": %s", ip));
    }

    public static boolean isExceptionDuetoRateLimiter(OverloadedException e)
    {
        return e.getMessage().contains(THROW_MESSAGE.toLowerCase());
    }

    public static String convertEpochTimeToUTC(long currentTimeMillis) {
        // Convert to a Date object
        Date currentDate = new Date(currentTimeMillis);

        // Format the date for UTC
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
        String formattedDate = sdf.format(currentDate);
        return formattedDate + " UTC";
    }
}
