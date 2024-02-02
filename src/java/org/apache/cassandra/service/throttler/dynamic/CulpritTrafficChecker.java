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

import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.metrics.KeyspaceMetrics;
import org.apache.cassandra.metrics.LatencyMetrics;
import org.apache.cassandra.service.throttler.dynamic.metrics.KeyspaceThrottlingMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.codahale.metrics.Counter;

import static org.apache.cassandra.service.throttler.dynamic.CassandraResourceUtilization.MAX_THROTTLING;

/**
 * CulpritTrafficChecker analyze the traffic, and decide if the traffic is the culprit to the performance degradation.
 * If the traffic is considered culprit, it will incur higher throttlingPercentage.
 */
public class CulpritTrafficChecker
{
    private static final Logger logger = LoggerFactory.getLogger(CulpritTrafficChecker.class);

    public static final Map<String, Boolean> readAggressiveThrottlingKeyspaces = new ConcurrentHashMap<>();
    public static final Map<String, Boolean> rangeReadAggressiveThrottlingKeyspaces = new ConcurrentHashMap<>();
    public static final Map<String, Boolean> mutationAggressiveThrottlingKeyspaces = new ConcurrentHashMap<>();

    // returns the updated throttling percentage
    public static double doCheck(double currentThrottlingPercentage, String keyspace,
                                 TrafficType trafficType, KeyspaceThrottlingMetrics ksThrottlingMetrics, ThrottlingOptions throttlingOptions){
        if (currentThrottlingPercentage >= MAX_THROTTLING)
        {
            return MAX_THROTTLING;
        }

        double res = currentThrottlingPercentage;
        boolean isCulprit = trafficType.getCulpritKeyspaceCache().containsKey(keyspace);
        if (!isCulprit)
        {
            KeyspaceMetrics keyspaceMetrics =  Keyspace.open(keyspace).metric;
            if (spikeInRequestRate(keyspace, trafficType, keyspaceMetrics, ksThrottlingMetrics, throttlingOptions) ||
                    spikeInLatency(keyspace, trafficType, keyspaceMetrics, ksThrottlingMetrics, throttlingOptions))
            {
                trafficType.getCulpritKeyspaceAddedCounterSupplier().getCounter(ksThrottlingMetrics).inc();
                trafficType.getCulpritKeyspaceCache().put(keyspace, true);
                isCulprit = true;
            }
        }

        if (isCulprit)
        {
            res = Math.min(MAX_THROTTLING, res + throttlingOptions.getPercentageOfTrafficToThrottling()); // TODO: make this behavior customizable via nodetool, similar to the auditLog class
            ksThrottlingMetrics.aggressiveThrottling.inc();
        }

        return res;
    }

    public static boolean spikeInRequestRate(String keyspace, TrafficType trafficType, KeyspaceMetrics keyspaceMetrics, KeyspaceThrottlingMetrics keyspaceThrottlingMetrics, ThrottlingOptions throttlingOptions)
    {
        double oneMinuteRate = 0.0;
        double fiveMinuteRate = 0.0;
        double fifteenMinuteRate = 0.0;

        LatencyMetrics latencyMetrics = trafficType.getCulpritLatencyMetricsSupplier().getLatencyMetrics(keyspaceMetrics);
        oneMinuteRate = latencyMetrics.latency.getOneMinuteRate();
        fiveMinuteRate =latencyMetrics.latency.getFiveMinuteRate();
        fifteenMinuteRate = latencyMetrics.latency.getFifteenMinuteRate();

        boolean trendingUp = isTrendingUpward(oneMinuteRate, fiveMinuteRate, fifteenMinuteRate);
        if (trendingUp)
        {
            trafficType.getRequestsTrendingUpwardCounterSupplier().getCounter(keyspaceThrottlingMetrics).inc();
            double ratio = 0.0;
            if (Double.compare(fifteenMinuteRate, 0.0) != 0)
            {
                ratio = oneMinuteRate / fifteenMinuteRate;
            }
            logger.info("Trending requests upward. traffic type: {} keyspace: {}, ratio: {}, oneMinuteRate: {}, fiveMinuteRate: {}, fifteenMinuteRate: {}",
                    trafficType, keyspace, ratio, oneMinuteRate, fiveMinuteRate, fifteenMinuteRate);
            if (ratio >= throttlingOptions.getAggressiveThrottlingQpsRatio())
            {
                logger.info("Trending qualified requests upward. traffic type: {}, keyspace: {}, ratio: {}, oneMinuteRate: {}, fiveMinuteRate: {}, fifteenMinuteRate: {}",
                        trafficType, keyspace, ratio, oneMinuteRate, fiveMinuteRate, fifteenMinuteRate);
                return true;
            }
        }
        return false;
    }

    public static boolean spikeInLatency(String keyspace, TrafficType trafficType, KeyspaceMetrics keyspaceMetrics, KeyspaceThrottlingMetrics keyspaceThrottlingMetrics, ThrottlingOptions throttlingOptions)
    {
        double oneMinuteRate = 0.0;
        double fiveMinuteRate = 0.0;
        double fifteenMinuteRate = 0.0;

        LatencyMetrics latencyMetrics = trafficType.getCulpritLatencyMetricsSupplier().getLatencyMetrics(keyspaceMetrics);
        oneMinuteRate = latencyMetrics.latencyMeter.getOneMinuteRate();
        fiveMinuteRate =latencyMetrics.latencyMeter.getFiveMinuteRate();
        fifteenMinuteRate = latencyMetrics.latencyMeter.getFifteenMinuteRate();

        boolean trendingUp = isTrendingUpward(oneMinuteRate, fiveMinuteRate, fifteenMinuteRate);
        if (trendingUp)
        {
            trafficType.getLatencyTrendingUpwardCounterSupplier().getCounter(keyspaceThrottlingMetrics).inc();
            double ratio = 0.0;
            if (Double.compare(fifteenMinuteRate, 0.0) != 0)
            {
                ratio = oneMinuteRate / fifteenMinuteRate;
            }
            logger.info("Trending latency upward. traffic type: {} keyspace: {}, ratio: {}, oneMinuteRate: {}, fiveMinuteRate: {}, fifteenMinuteRate: {}",
                    trafficType, keyspace, ratio, oneMinuteRate, fiveMinuteRate, fifteenMinuteRate);
            if (ratio >= throttlingOptions.getAggressiveThrottlingQpsRatio())
            {
                logger.info("Trending qualified latency upward. traffic type: {}, keyspace: {}, ratio: {}, oneMinuteRate: {}, fiveMinuteRate: {}, fifteenMinuteRate: {}",
                        trafficType, keyspace, ratio, oneMinuteRate, fiveMinuteRate, fifteenMinuteRate);
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

    public static void resetCulpritCache()
    {
        readAggressiveThrottlingKeyspaces.clear();
        rangeReadAggressiveThrottlingKeyspaces.clear();
        mutationAggressiveThrottlingKeyspaces.clear();
    }

    @FunctionalInterface
    public interface CulpritLatencyMetricsSupplier
    {
        public LatencyMetrics getLatencyMetrics(KeyspaceMetrics keyspaceMetrics);
    }

    static LatencyMetrics writeLatencyMetrics(KeyspaceMetrics keyspaceMetrics)
    {
        return keyspaceMetrics.writeLatency;
    }

    static LatencyMetrics singlePartitionReadLatencyMetrics(KeyspaceMetrics keyspaceMetrics)
    {
        return keyspaceMetrics.readLatency;
    }

    static LatencyMetrics rangeLatencyMetrics(KeyspaceMetrics keyspaceMetrics)
    {
        return keyspaceMetrics.rangeLatency;
    }

    @FunctionalInterface
    public interface CulpritKeyspaceAddedCounterSupplier
    {
        public Counter getCounter(KeyspaceThrottlingMetrics keyspaceThrottlingMetrics);
    }

    static Counter addKSForReadThrottlingCounter(KeyspaceThrottlingMetrics keyspaceThrottlingMetrics)
    {
        return keyspaceThrottlingMetrics.addKSForReadThrottling;
    }

    static Counter addKSForRangeThrottlingCounter(KeyspaceThrottlingMetrics keyspaceThrottlingMetrics)
    {
        return keyspaceThrottlingMetrics.addKSForRangeThrottling;
    }

    static Counter addKSForWriteThrottlingCounter(KeyspaceThrottlingMetrics keyspaceThrottlingMetrics)
    {
        return keyspaceThrottlingMetrics.addKSForWriteThrottling;
    }

    @FunctionalInterface
    public interface RequestsTrendingUpwardCounterSupplier
    {
        public Counter getCounter(KeyspaceThrottlingMetrics keyspaceThrottlingMetrics);
    }

    static Counter readRequestsTrendingUpwardCounter(KeyspaceThrottlingMetrics keyspaceThrottlingMetrics)
    {
        return keyspaceThrottlingMetrics.readRequestsTrendingUpward;
    }

    static Counter rangeRequestsTrendingUpwardCounter(KeyspaceThrottlingMetrics keyspaceThrottlingMetrics)
    {
        return keyspaceThrottlingMetrics.rangeRequestsTrendingUpward;
    }

    static Counter writeRequestsTrendingUpwarddCounter(KeyspaceThrottlingMetrics keyspaceThrottlingMetrics)
    {
        return keyspaceThrottlingMetrics.writeRequestsTrendingUpward;
    }

    @FunctionalInterface
    public interface LatencyTrendingUpwardCounterSupplier
    {
        public Counter getCounter(KeyspaceThrottlingMetrics keyspaceThrottlingMetrics);
    }

    static Counter readLatencyTrendingUpwardCounter(KeyspaceThrottlingMetrics keyspaceThrottlingMetrics)
    {
        return keyspaceThrottlingMetrics.readLatencyTrendingUpward;
    }

    static Counter rangeLatencyTrendingUpwardCounter(KeyspaceThrottlingMetrics keyspaceThrottlingMetrics)
    {
        return keyspaceThrottlingMetrics.rangeLatencyTrendingUpward;
    }

    static Counter writeLatencyTrendingUpwardCounter(KeyspaceThrottlingMetrics keyspaceThrottlingMetrics)
    {
        return keyspaceThrottlingMetrics.writeLatencyTrendingUpward;
    }
}
