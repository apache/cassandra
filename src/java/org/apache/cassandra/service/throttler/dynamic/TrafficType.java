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

import java.util.Map;

public enum TrafficType
{
    SinglePartitionCoordRead
    (false, false, true,
            CulpritTrafficChecker.readAggressiveThrottlingKeyspaces,
            CulpritTrafficChecker::singlePartitionReadLatencyMetrics,
            CulpritTrafficChecker::addKSForReadThrottlingCounter,
            CulpritTrafficChecker::readRequestsTrendingUpwardCounter,
            CulpritTrafficChecker::readLatencyTrendingUpwardCounter,
            CassandraResourceUtilization::hardBlockSinglePartitionCoordReadsCounter
    ),
    SinglePartitionReplicaRead
    (false, false, false,
            CulpritTrafficChecker.readAggressiveThrottlingKeyspaces,
            CulpritTrafficChecker::singlePartitionReadLatencyMetrics,
            CulpritTrafficChecker::addKSForReadThrottlingCounter,
            CulpritTrafficChecker::readRequestsTrendingUpwardCounter,
            CulpritTrafficChecker::readLatencyTrendingUpwardCounter,
            CassandraResourceUtilization::hardBlockSinglePartitionReplicaReadsCounter
    ),
    RangeCoordRead
    (false, true, true,
            CulpritTrafficChecker.rangeReadAggressiveThrottlingKeyspaces,
            CulpritTrafficChecker::rangeLatencyMetrics,
            CulpritTrafficChecker::addKSForRangeThrottlingCounter,
            CulpritTrafficChecker::rangeRequestsTrendingUpwardCounter,
            CulpritTrafficChecker::rangeLatencyTrendingUpwardCounter,
            CassandraResourceUtilization::hardBlockRangeCoordReadsCounter
    ),
    RangeReplicaRead
    (false, true, false,
            CulpritTrafficChecker.rangeReadAggressiveThrottlingKeyspaces,
            CulpritTrafficChecker::rangeLatencyMetrics,
            CulpritTrafficChecker::addKSForRangeThrottlingCounter,
            CulpritTrafficChecker::rangeRequestsTrendingUpwardCounter,
            CulpritTrafficChecker::rangeLatencyTrendingUpwardCounter,
            CassandraResourceUtilization::hardBlockRangeReplicaReadsCounter
    ),
    CoordWrite
    (true, false, true,
            CulpritTrafficChecker.mutationAggressiveThrottlingKeyspaces,
            CulpritTrafficChecker::writeLatencyMetrics,
            CulpritTrafficChecker::addKSForWriteThrottlingCounter,
            CulpritTrafficChecker::writeRequestsTrendingUpwarddCounter,
            CulpritTrafficChecker::writeLatencyTrendingUpwardCounter,
            CassandraResourceUtilization::hardBlockCoordWritesCounter
    ),
    ReplicaWrite
    (true, false, false,
            CulpritTrafficChecker.mutationAggressiveThrottlingKeyspaces,
            CulpritTrafficChecker::writeLatencyMetrics,
            CulpritTrafficChecker::addKSForWriteThrottlingCounter,
            CulpritTrafficChecker::writeRequestsTrendingUpwarddCounter,
            CulpritTrafficChecker::writeLatencyTrendingUpwardCounter,
            CassandraResourceUtilization::hardBlockReplicaWritesCounter
    );

    private final boolean isWrite;
    private final boolean isRangeRead;
    private final boolean isCoordTraffic;
    private final TableFilter hardBlockTablesFilter;

    private final Map<String, Boolean> culpritKeyspaceCache;
    private final CulpritTrafficChecker.CulpritLatencyMetricsSupplier culpritLatencyMetricsSupplier;
    private final CulpritTrafficChecker.CulpritKeyspaceAddedCounterSupplier culpritKeyspaceAddedCounterSupplier;
    private final CulpritTrafficChecker.RequestsTrendingUpwardCounterSupplier requestsTrendingUpwardCounterSupplier;
    private final CulpritTrafficChecker.LatencyTrendingUpwardCounterSupplier latencyTrendingUpwardCounterSupplier;

    private final CassandraResourceUtilization.HardBlockCounterSupplier hardBlockCounterSupplier;

    TrafficType(boolean isWrite, boolean isRangeRead, boolean isCoordTraffic,
                Map<String, Boolean> culpritKeyspaceCache,
                CulpritTrafficChecker.CulpritLatencyMetricsSupplier latencyMetricsSupplier,
                CulpritTrafficChecker.CulpritKeyspaceAddedCounterSupplier culpritKeyspaceAddedCounterSupplier,
                CulpritTrafficChecker.RequestsTrendingUpwardCounterSupplier requestsTrendingUpwardCounterSupplier,
                CulpritTrafficChecker.LatencyTrendingUpwardCounterSupplier latencyTrendingUpwardCounterSupplier,
                CassandraResourceUtilization.HardBlockCounterSupplier hardBlockCounterSupplier)
    {
        this.isWrite = isWrite;
        this.isRangeRead = isRangeRead;
        this.isCoordTraffic = isCoordTraffic;
        this.culpritLatencyMetricsSupplier = latencyMetricsSupplier;
        this.culpritKeyspaceCache = culpritKeyspaceCache;
        this.culpritKeyspaceAddedCounterSupplier = culpritKeyspaceAddedCounterSupplier;
        this.requestsTrendingUpwardCounterSupplier = requestsTrendingUpwardCounterSupplier;
        this.latencyTrendingUpwardCounterSupplier = latencyTrendingUpwardCounterSupplier;
        this.hardBlockCounterSupplier = hardBlockCounterSupplier;

        this.hardBlockTablesFilter = new TableFilter("HardBlock" + name());
    }

    public boolean isWrite()
    {
        return isWrite;
    }

    public boolean isRangeRead()
    {
        return isRangeRead;
    }

    public boolean isCoordTraffic()
    {
        return isCoordTraffic;
    }

    public CulpritTrafficChecker.CulpritLatencyMetricsSupplier getCulpritLatencyMetricsSupplier()
    {
        return culpritLatencyMetricsSupplier;
    }

    public Map<String, Boolean> getCulpritKeyspaceCache()
    {
        return culpritKeyspaceCache;
    }

    public CulpritTrafficChecker.CulpritKeyspaceAddedCounterSupplier getCulpritKeyspaceAddedCounterSupplier()
    {
        return culpritKeyspaceAddedCounterSupplier;
    }

    public CulpritTrafficChecker.RequestsTrendingUpwardCounterSupplier getRequestsTrendingUpwardCounterSupplier()
    {
        return requestsTrendingUpwardCounterSupplier;
    }

    public CulpritTrafficChecker.LatencyTrendingUpwardCounterSupplier getLatencyTrendingUpwardCounterSupplier()
    {
        return latencyTrendingUpwardCounterSupplier;
    }

    public TableFilter getHardBlockTablesFilter() {
        return hardBlockTablesFilter;
    }

    public CassandraResourceUtilization.HardBlockCounterSupplier getHardBlockCounterSupplier()
    {
        return hardBlockCounterSupplier;
    }
}
