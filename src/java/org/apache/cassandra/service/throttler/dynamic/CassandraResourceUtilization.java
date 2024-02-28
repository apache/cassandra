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
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.Date;
import java.util.TimeZone;

import com.codahale.metrics.Counter;
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
import org.apache.cassandra.utils.NoSpamLogger;

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
    protected static double MAX_THROTTLING = 1.0;
    protected static int LOG_CPU_CORES_INTERVAL_MINUTES = 10;

    public boolean isSetupComplete = false;
    public volatile double currentThrottlingPercentage;
    public ScheduledExecutorPlus reportThread = executorFactory().scheduled(false, "CassandraResourceUtilization", Thread.MAX_PRIORITY);
    public ResourcesStats resourcesStats;
    public ThrottlingMetrics throttlingMetrics;
    public ThrottlingOptions throttlingOptions;
    public long lastThrottlingCheckPointTimeInMS = 0;
    public long lastThrottlingIndicatorTimeInMS = 0;
    public volatile boolean shouldThrottle = false;

    public TableFiltersRefresher tableFiltersRefresher = new TableFiltersRefresher();
    public TableFilter ignoreTablesFilter = new TableFilter("ignore_tables");

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
        setupFilters();

        if (continuousHealthCheck)
        {
            startHealthCheckThread();
        }

        isSetupComplete = true; // this line should be the last line in this method
    }

    public void startHealthCheckThread()
    {
        reportThread.scheduleAtFixedRate(() -> fetchCurrentHealth(), throttlingOptions.getHealthCheckInitDelayInSec(), throttlingOptions.getHealthCheckPeriodInSec(), TimeUnit.SECONDS);
    }

    public void setupFilters()
    {
        syncAllFilters();

        tableFiltersRefresher.addFilter(ignoreTablesFilter);
        for (TrafficType trafficType : TrafficType.values())
        {
            tableFiltersRefresher.addFilter(trafficType.getHardBlockTablesFilter());
        }
        tableFiltersRefresher.registerSchemaChangeListener();
    }

    public void syncIgnoreTablesFilter()
    {
        ignoreTablesFilter.setRegexPatternAndRefresh(throttlingOptions.getIgnoreTablesRegex());
    }

    public void syncHardBlockSinglePartitionCoordReadsTablesFilter()
    {
        TrafficType.SinglePartitionCoordRead.getHardBlockTablesFilter().setRegexPatternAndRefresh(
        throttlingOptions.getHardBlockSinglePartitionCoordReadsTablesRegex());
    }

    public void syncHardBlockRangeCoordReadsTablesFilter()
    {
        TrafficType.RangeCoordRead.getHardBlockTablesFilter().setRegexPatternAndRefresh(
        throttlingOptions.getHardBlockRangeCoordReadsTablesRegex());
    }

    public void syncHardBlockSinglePartitionReplicaReadsTablesFilter()
    {
        TrafficType.SinglePartitionReplicaRead.getHardBlockTablesFilter().setRegexPatternAndRefresh(
        throttlingOptions.getHardBlockSinglePartitionReplicaReadsTablesRegex());
    }

    public void syncHardBlockRangeReplicaReadsTablesFilter()
    {
        TrafficType.RangeReplicaRead.getHardBlockTablesFilter().setRegexPatternAndRefresh(
        throttlingOptions.getHardBlockRangeReplicaReadsTablesRegex());
    }

    public void syncHardBlockCoordWritesTablesFilter()
    {
        TrafficType.CoordWrite.getHardBlockTablesFilter().setRegexPatternAndRefresh(
        throttlingOptions.getHardBlockCoordWritesTablesRegex());
    }

    public void syncHardBlockReplicaWritesTablesFilter()
    {
        TrafficType.ReplicaWrite.getHardBlockTablesFilter().setRegexPatternAndRefresh(
        throttlingOptions.getHardBlockReplicaWritesTablesRegex());
    }


    public void syncAllFilters()
    {
        syncIgnoreTablesFilter();

        syncHardBlockSinglePartitionCoordReadsTablesFilter();
        syncHardBlockSinglePartitionReplicaReadsTablesFilter();
        syncHardBlockRangeCoordReadsTablesFilter();
        syncHardBlockRangeReplicaReadsTablesFilter();
        syncHardBlockCoordWritesTablesFilter();
        syncHardBlockReplicaWritesTablesFilter();
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

            NoSpamLogger.log(logger, NoSpamLogger.Level.INFO, LOG_CPU_CORES_INTERVAL_MINUTES, TimeUnit.MINUTES,
                             "availableProcessors = {}", Runtime.getRuntime().availableProcessors());
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
            // TODO: avoid the spamming of logs before deploying to higher tiers
            logger.info("Enforcing throttling CpuUtil1: {}-{}, CpuUtil2: {}-{}, PendingReads: {}-{}, PendingMutations: {}-{}, PendingNativeTransportSignal: {}-{}, " +
                        "LastThrottlingCheckPointTimeInMS: {}, LastThrottlingIndicatorTimeInMS: {}, CurrentThrottlingPercentage: {}",
                        resourcesStats.getCpuUtil1Cur(), resourcesStats.getCpuUtil1OneMinute(),
                        resourcesStats.getCpuUtil2Cur(), resourcesStats.getCpuUtil2OneMinute(),
                        resourcesStats.getPendingReadsCur(), resourcesStats.getPendingReadsOneMinute(),
                        resourcesStats.getPendingMutationsCur(), resourcesStats.getPendingMutationsOneMinute(),
                        resourcesStats.getPendingNativeTransportCur(), resourcesStats.getPendingNativeTransportOneMinute(),
                        convertEpochTimeToUTC(lastThrottlingCheckPointTimeInMS), convertEpochTimeToUTC(lastThrottlingIndicatorTimeInMS), currentThrottlingPercentage);
        }
        else
        {
            shouldThrottle = false;
            throttlingMetrics.doesNotNeedThrottling.inc();
            // TODO: avoid the spamming of logs before deploying to higher tiers
            logger.info("DO NOT Enforce throttling CpuUtil1: {}-{}-{}, CpuUtil2: {}-{}-{}, PendingReads: {}-{}-{}, PendingMutations: {}-{}-{}, PendingNativeTransportSignal: {}-{}, " +
                        "LastThrottlingCheckPointTimeInMS: {}, LastThrottlingIndicatorTimeInMS: {}, CurrentThrottlingPercentage: {}",
                        cpuUtilSignal1, resourcesStats.getCpuUtil1Cur(), resourcesStats.getCpuUtil1OneMinute(),
                        cpuUtilSignal2, resourcesStats.getCpuUtil2Cur(), resourcesStats.getCpuUtil2OneMinute(),
                        pendingReadsSignal, resourcesStats.getPendingReadsCur(), resourcesStats.getPendingReadsOneMinute(),
                        pendingMutationsSignal, resourcesStats.getPendingMutationsCur(), resourcesStats.getPendingMutationsOneMinute(),
                        resourcesStats.getPendingNativeTransportCur(), resourcesStats.getPendingNativeTransportOneMinute(),
                        convertEpochTimeToUTC(lastThrottlingCheckPointTimeInMS), convertEpochTimeToUTC(lastThrottlingIndicatorTimeInMS), currentThrottlingPercentage);
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
        CulpritTrafficChecker.resetCulpritCache();
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
            else if (lastThrottlingIndicatorTimeInMS != 0 && lastThrottlingIndicatorTimeInMS > lastThrottlingCheckPointTimeInMS)
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
          append('-').append(df.format(resourcesStats.getCpuUtil1OneMinute())).
          append('-').append(df.format(resourcesStats.getCpuUtil1FiveMinute())).
          append('-').append(df.format(resourcesStats.getCpuUtil1FifteenMinute())).

          append(", CpuUtil2: ").append(df.format(resourcesStats.getCpuUtil2Cur())).
          append('-').append(df.format(resourcesStats.getCpuUtil2OneMinute())).
          append('-').append(df.format(resourcesStats.getCpuUtil2FiveMinute())).
          append('-').append(df.format(resourcesStats.getCpuUtil2FifteenMinute())).

          append(", PendingReads: ").append(df.format(resourcesStats.getPendingReadsCur())).
          append('-').append(df.format(resourcesStats.getPendingReadsOneMinute())).
          append('-').append(df.format(resourcesStats.getPendingReadsFiveMinute())).
          append('-').append(df.format(resourcesStats.getPendingReadsFifteenMinute())).

          append(", PendingMutations: ").append(df.format(resourcesStats.getPendingMutationsCur())).
          append('-').append(df.format(resourcesStats.getPendingMutationsOneMinute())).
          append('-').append(df.format(resourcesStats.getPendingMutationsFiveMinute())).
          append('-').append(df.format(resourcesStats.getPendingMutationsFifteenMinute())).
          append(", PendingNativeTransport: ").append(df.format(resourcesStats.getPendingNativeTransportCur())).
          append('-').append(df.format(resourcesStats.getPendingNativeTransportOneMinute())).
          append('-').append(df.format(resourcesStats.getPendingNativeTransportFiveMinute())).
          append('-').append(df.format(resourcesStats.getPendingNativeTransportFifteenMinute())).
          append(", LastThrottlingCheckPointTimeInMS: ").append(convertEpochTimeToUTC(lastThrottlingCheckPointTimeInMS)).
          append(", LastThrottlingIndicatorTimeInMS: ").append(convertEpochTimeToUTC(lastThrottlingIndicatorTimeInMS)).
          append(", CurrentThrottlingPercentage: ").append(currentThrottlingPercentage);

        return sb.toString();
    }

    public boolean throttleUserTraffic(String keyspaceName, Collection<String> tables, TrafficType trafficType)
    {
        if (!isSetupComplete)
        {
            return false;
        }

        if (!throttlingOptions.isEnabled())
        {
            throttlingMetrics.disableThrottling.inc();
            return false;
        }

        KeyspaceThrottlingMetrics ksThrottlingMetrics = KeyspaceThrottlingMetricsManager.getMetrics(keyspaceName);
        if (decideHardBlock(keyspaceName, tables, trafficType, ksThrottlingMetrics)) {
            return true;
        }

        if (applyTableFilter(ignoreTablesFilter, keyspaceName, tables))
        {
            ksThrottlingMetrics.skipKSThrottling.inc();
            return false;
        }

        if (!trafficType.isCoordTraffic())
        {
            if (!trafficType.isWrite() && !throttlingOptions.getThrottleReadReplicaTraffic())
            {
                throttlingMetrics.disableReadReplicaTrafficThrottling.inc();
                return false;
            }
            if (trafficType.isWrite() && !throttlingOptions.getThrottleMutationReplicaTraffic())
            {
                throttlingMetrics.disableMutationReplicaTrafficThrottling.inc();
                return false;
            }
        }

        KeyspaceMetrics metrics = Keyspace.open(keyspaceName).metric;
        if (shouldThrottle)
        {
            return decideThrottling(keyspaceName, metrics, trafficType, ksThrottlingMetrics);
        }

        return false;
    }

    public void throttle(String keyspaceName, Collection<String> tables, TrafficType trafficType) throws OverloadedException
    {
        if (throttleUserTraffic(keyspaceName, tables, trafficType))
        {
            throw buildOverloadeExceptionDuetoRateLimiter();
        }
    }

    @FunctionalInterface
    public interface HardBlockCounterSupplier
    {
        public Counter getCounter(KeyspaceThrottlingMetrics keyspaceThrottlingMetrics);
    }

    static Counter hardBlockSinglePartitionCoordReadsCounter(KeyspaceThrottlingMetrics keyspaceThrottlingMetrics)
    {
        return keyspaceThrottlingMetrics.hardBlockSinglePartitionCoordReads;
    }

    static Counter hardBlockSinglePartitionReplicaReadsCounter(KeyspaceThrottlingMetrics keyspaceThrottlingMetrics)
    {
        return keyspaceThrottlingMetrics.hardBlockSinglePartitionReplicaReads;
    }

    static Counter hardBlockRangeCoordReadsCounter(KeyspaceThrottlingMetrics keyspaceThrottlingMetrics)
    {
        return keyspaceThrottlingMetrics.hardBlockRangeCoordReads;
    }

    static Counter hardBlockRangeReplicaReadsCounter(KeyspaceThrottlingMetrics keyspaceThrottlingMetrics)
    {
        return keyspaceThrottlingMetrics.hardBlockRangeReplicaReads;
    }

    static Counter hardBlockCoordWritesCounter(KeyspaceThrottlingMetrics keyspaceThrottlingMetrics)
    {
        return keyspaceThrottlingMetrics.hardBlockCoordWrites;
    }

    static Counter hardBlockReplicaWritesCounter(KeyspaceThrottlingMetrics keyspaceThrottlingMetrics)
    {
        return keyspaceThrottlingMetrics.hardBlockReplicaWrites;
    }

    public boolean decideHardBlock(String keyspace, Collection<String> tables, TrafficType trafficType, KeyspaceThrottlingMetrics ksThrottlingMetrics)
    {
        TableFilter filter = trafficType.getHardBlockTablesFilter();

        boolean shouldBlock = applyTableFilter(filter, keyspace, tables);
        if (shouldBlock)
        {
            trafficType.getHardBlockCounterSupplier().getCounter(ksThrottlingMetrics).inc();
        }
        return shouldBlock;
    }

    // returns true if the table filter matches any of the tables
    public boolean applyTableFilter(TableFilter filter, String keyspace, Collection<String> tables)
    {
        for (String table : tables)
        {
            if (filter.matches(keyspace, table))
            {
                return true;
            }
        }
        return false;
    }

    public boolean decideThrottling(String ksName, KeyspaceMetrics metrics, TrafficType trafficType, KeyspaceThrottlingMetrics ksThrottlingMetrics)
    {
        double throttlingPercentage = CulpritTrafficChecker.doCheck(currentThrottlingPercentage,
                ksName, trafficType, ksThrottlingMetrics, throttlingOptions);

        boolean reads = !trafficType.isWrite();
        if (throttlingPercentage < MAX_THROTTLING)
        {
            if (ThreadLocalRandom.current().nextDouble() < throttlingPercentage)
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
