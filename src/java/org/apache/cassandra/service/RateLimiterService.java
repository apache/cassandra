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
package org.apache.cassandra.service;

import org.apache.cassandra.service.throttler.dynamic.CassandraResourceUtilization;
import org.apache.cassandra.service.throttler.dynamic.ThrottlingOptions;
import org.apache.cassandra.utils.MBeanWrapper;

public class RateLimiterService implements RateLimiterServiceMBean {
    public static final String MBEAN_NAME = "org.apache.cassandra.db:type=RateLimiterService";

    private ThrottlingOptions throttlingOptions;

    public static final RateLimiterService instance = new RateLimiterService();

    private RateLimiterService()
    {
    }

    static
    {
        MBeanWrapper.instance.registerMBean(instance, MBEAN_NAME);
    }

    public void setThrottlingOptions(ThrottlingOptions throttlingOptions)
    {
        this.throttlingOptions = throttlingOptions;
    }

    public ThrottlingOptions getThrottlingOptions()
    {
        return throttlingOptions;
    }

    @Override
    public String getThrottlingOptionsToString()
    {
        CassandraResourceUtilization cassandraResourceUtilization = CassandraResourceUtilization.instance;
        StringBuilder sb = new StringBuilder();
        sb.append(this.throttlingOptions.toString());
        sb.append("\n\n");
        sb.append("All table filters: ");
        sb.append(cassandraResourceUtilization.tableFiltersRefresher.allFiltersToString());
        sb.append("\n");

        return sb.toString();
    }

    // setters for individual parameters starts from here
    @Override
    public void setEnabled(boolean enabled)
    {
        this.throttlingOptions.setEnabled(enabled);
    }

    @Override
    public void setCpuThresholdOneMinute(long cpuThresholdOneMinute)
    {
        this.throttlingOptions.setCpuThresholdOneMinute(cpuThresholdOneMinute);
    }

    @Override
    public void setPendingReadsThresholdOneMinute(int pendingReadsThresholdOneMinute)
    {
        this.throttlingOptions.setPendingReadsThresholdOneMinute(pendingReadsThresholdOneMinute);
    }

    @Override
    public void setPendingMutationsThresholdOneMinute(int pendingMutationsThresholdOneMinute)
    {
        this.throttlingOptions.setPendingMutationsThresholdOneMinute(pendingMutationsThresholdOneMinute);
    }

    @Override
    public void setPendingNativeTransportThresholdOneMinute(int pendingNativeTransportThresholdOneMinute)
    {
        this.throttlingOptions.setPendingNativeTransportThresholdOneMinute(pendingNativeTransportThresholdOneMinute);
    }

    @Override
    public void setThreadpoolThresholdReads(long threadpoolThresholdReads)
    {
        this.throttlingOptions.setThreadpoolThresholdReads(threadpoolThresholdReads);
    }

    @Override
    public void setThreadpoolThresholdWrites(long threadpoolThresholdWrites)
    {
        this.throttlingOptions.setThreadpoolThresholdWrites(threadpoolThresholdWrites);
    }

    @Override
    public void setThreadpoolThresholdNativeTransport(long threadpoolThresholdNativeTransport)
    {
        this.throttlingOptions.setThreadpoolThresholdNativeTransport(threadpoolThresholdNativeTransport);
    }

    @Override
    public void setPercentageOfTrafficToThrottling(double percentageOfTrafficToThrottling)
    {
        this.throttlingOptions.setPercentageOfTrafficToThrottling(percentageOfTrafficToThrottling);
    }

    @Override
    public void setMoreAggressiveThrottlingAfterInSec(int moreAggressiveThrottlingAfterInSec)
    {
        this.throttlingOptions.setMoreAggressiveThrottlingAfterInSec(moreAggressiveThrottlingAfterInSec);
    }

    @Override
    public void setResetAfterNoThrottlingSeenInSec(int resetAfterNoThrottlingSeenInSec)
    {
        this.throttlingOptions.setResetAfterNoThrottlingSeenInSec(resetAfterNoThrottlingSeenInSec);
    }

    @Override
    public void setAggressiveThrottlingQpsRatio(double aggressiveThrottlingQpsRatio)
    {
        this.throttlingOptions.setAggressiveThrottlingQpsRatio(aggressiveThrottlingQpsRatio);
    }

    @Override
    public void setAggressiveThrottlingLatencyRatio(double aggressiveThrottlingLatencyRatio)
    {
        this.throttlingOptions.setAggressiveThrottlingLatencyRatio(aggressiveThrottlingLatencyRatio);
    }

    public void setIgnoreTablesRegex(String ignoreTablesRegex)
    {
        this.throttlingOptions.setIgnoreTablesRegex(ignoreTablesRegex);
        CassandraResourceUtilization.instance.syncIgnoreTablesFilter();
    }

    @Override
    public void setHealthCheckInitDelayInSec(int healthCheckInitDelayInSec)
    {
        this.throttlingOptions.setHealthCheckInitDelayInSec(healthCheckInitDelayInSec);
    }

    @Override
    public void setHealthCheckFreqInSec(int healthCheckPeriodInSec)
    {
        this.throttlingOptions.setHealthCheckFreqInSec(healthCheckPeriodInSec);
    }

    @Override
    public void setThrottleReadReplicaTraffic(boolean throttleReadReplicaTraffic)
    {
        this.throttlingOptions.setThrottleReadReplicaTraffic(throttleReadReplicaTraffic);
    }

    @Override
    public void setThrottleMutationReplicaTraffic(boolean throttleMutationReplicaTraffic)
    {
        this.throttlingOptions.setThrottleMutationReplicaTraffic(throttleMutationReplicaTraffic);
    }

    @Override
    public void setHardBlockSinglePartitionCoordReadsTablesRegex(String hardBlockSinglePartitionCoordReadsTablesRegex)
    {
        this.throttlingOptions.setHardBlockSinglePartitionCoordReadsTablesRegex(hardBlockSinglePartitionCoordReadsTablesRegex);
        CassandraResourceUtilization.instance.syncHardBlockSinglePartitionCoordReadsTablesFilter();
    }

    @Override
    public void setHardBlockSinglePartitionReplicaReadsTablesRegex(String hardBlockSinglePartitionReplicaReadsTablesRegex)
    {
        this.throttlingOptions.setHardBlockSinglePartitionReplicaReadsTablesRegex(hardBlockSinglePartitionReplicaReadsTablesRegex);
        CassandraResourceUtilization.instance.syncHardBlockSinglePartitionReplicaReadsTablesFilter();
    }

    @Override
    public void setHardBlockRangeCoordReadsTablesRegex(String hardBlockRangeCoordReadsTablesRegex)
    {
        this.throttlingOptions.setHardBlockRangeCoordReadsTablesRegex(hardBlockRangeCoordReadsTablesRegex);
        CassandraResourceUtilization.instance.syncHardBlockRangeCoordReadsTablesFilter();
    }

    @Override
    public void setHardBlockRangeReplicaReadsTablesRegex(String hardBlockRangeReplicaReadsTablesRegex)
    {
        this.throttlingOptions.setHardBlockRangeReplicaReadsTablesRegex(hardBlockRangeReplicaReadsTablesRegex);
        CassandraResourceUtilization.instance.syncHardBlockRangeReplicaReadsTablesFilter();
    }

    @Override
    public void setHardBlockCoordWritesTablesRegex(String hardBlockCoordWritesTablesRegex)
    {
        this.throttlingOptions.setHardBlockCoordWritesTablesRegex(hardBlockCoordWritesTablesRegex);
        CassandraResourceUtilization.instance.syncHardBlockCoordWritesTablesFilter();
    }

    @Override
    public void setHardBlockReplicaWritesTablesRegex(String hardBlockReplicaWritesTablesRegex)
    {
        this.throttlingOptions.setHardBlockReplicaWritesTablesRegex(hardBlockReplicaWritesTablesRegex);
        CassandraResourceUtilization.instance.syncHardBlockReplicaWritesTablesFilter();
    }
}
