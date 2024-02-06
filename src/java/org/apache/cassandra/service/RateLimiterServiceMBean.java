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

public interface RateLimiterServiceMBean
{
    /**
     * Get throttling options
     */
    public String getThrottlingOptionsToString();

    /**
     * Set enabled
     */
    public void setEnabled(boolean enabled);

    /**
     * Set cpu_threshold_cur
     */
    public void setCpuThresholdCur(long cpuThresholdCur);

    /**
     * Set cpu_threshold_one_minute
     */
    public void setCpuThresholdOneMinute(long cpuThresholdOneMinute);

    /**
     * Set pending_reads_threshold_cur
     */
    public void setPendingReadsThresholdCur(int pendingReadsThresholdCur);

    /**
     * Set pending_reads_threshold_one_minute
     */
    public void setPendingReadsThresholdOneMinute(int pendingReadsThresholdOneMinute);

    /**
     * Set pending_mutations_threshold_cur
     */
    public void setPendingMutationsThresholdCur(int pendingMutationsThresholdCur);

    /**
     * Set pending_mutations_threshold_one_minute
     */
    public void setPendingMutationsThresholdOneMinute(int pendingMutationsThresholdOneMinute);

    /**
     * Set pending_native_transport_threshold_cur
     */
    public void setPendingNativeTransportThresholdCur(int pendingNativeTransportThresholdCur);

    /**
     * Set pending_native_transport_threshold_one_minute
     */
    public void setPendingNativeTransportThresholdOneMinute(int pendingNativeTransportThresholdOneMinute);

    /**
     * Set percentage_of_traffic_to_throttling
     */
    public void setPercentageOfTrafficToThrottling(double percentageOfTrafficToThrottling);

    /**
     * Set more_aggressive_throttling_after_in_sec
     */
    public void setMoreAggressiveThrottlingAfterInSec(int moreAggressiveThrottlingAfterInSec);

    /**
     * Set reset_after_no_throttling_seen_in_sec
     */
    public void setResetAfterNoThrottlingSeenInSec(int resetAfterNoThrottlingSeenInSec);

    /**
     * Set aggressive_throttling_qps_ratio
     */
    public void setAggressiveThrottlingQpsRatio(double aggressiveThrottlingQpsRatio);

    /**
     * Set aggressive_throttling_latency_ratio
     */
    public void setAggressiveThrottlingLatencyRatio(double aggressiveThrottlingLatencyRatio);

    /**
     * Set ignore_tables_regex
     */
    public void setIgnoreTablesRegex(String ignoreTablesRegex);

    /**
     * Set health_check_init_delay_in_sec
     */
    public void setHealthCheckInitDelayInSec(int healthCheckInitDelayInSec);

    /**
     * Set health_check_period_in_sec
     */
    public void setHealthCheckFreqInSec(int healthCheckPeriodInSec);

    /**
     * Set throttle_read_replica_traffic
     */
    public void setThrottleReadReplicaTraffic(boolean throttleReadReplicaTraffic);

    /**
     * Set throttle_mutation_replica_traffic
     */
    public void setThrottleMutationReplicaTraffic(boolean throttleMutationReplicaTraffic);

    /**
     * Set hard_block_single_partition_coord_reads_tables_regex
     */
    public void setHardBlockSinglePartitionCoordReadsTablesRegex(String hardBlockSinglePartitionCoordReadsTablesRegex);

    /**
     * Set hard_block_single_partition_replica_reads_tables_regex
     */
    public void setHardBlockSinglePartitionReplicaReadsTablesRegex(String hardBlockSinglePartitionReplicaReadsTablesRegex);

    /**
     * Set hard_block_range_coord_reads_tables_regex
     */
    public void setHardBlockRangeCoordReadsTablesRegex(String hardBlockRangeCoordReadsTablesRegex);

    /**
     * Set hard_block_range_replica_reads_tables_regex
     */
    public void setHardBlockRangeReplicaReadsTablesRegex(String hardBlockRangeReplicaReadsTablesRegex);

    /**
     * Set hard_block_coord_writes_tables_regex
     */
    public void setHardBlockCoordWritesTablesRegex(String hardBlockCoordWritesTablesRegex);

    /**
     * Set hard_block_replica_writes_tables_regex
     */
    public void setHardBlockReplicaWritesTablesRegex(String hardBlockReplicaWritesTables);
}
