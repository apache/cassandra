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

public class ThrottlingOptions
{
    // Also, adjust the default values based on the POC
    // TODO: think of the need to declare the following variables a vilotale, given they can be modified by nodetool at runtime
    public boolean enabled;

    public long cpu_threshold_cur;
    public long cpu_threshold_one_minute;
    public int pending_reads_threshold_cur;
    public int pending_reads_threshold_one_minute;
    public int pending_mutations_threshold_cur;
    public int pending_mutations_threshold_one_minute;
    public int pending_native_transport_threshold_cur;
    public int pending_native_transport_threshold_one_minute;
    public double percentage_of_traffic_to_throttling;
    public int more_aggressive_throttling_after_in_sec;
    public int reset_after_no_throttling_seen_in_sec;
    public double aggressive_throttling_qps_ratio;
    public double aggressive_throttling_latency_ratio;
    public String ignore_tables_regex;
    public int health_check_init_delay_in_sec;
    public int health_check_freq_in_sec;
    public boolean throttle_read_replica_traffic;
    public boolean throttle_mutation_replica_traffic;
    public String hard_block_single_partition_coord_reads_tables_regex;
    public String hard_block_single_partition_replica_reads_tables_regex;
    public String hard_block_range_coord_reads_tables_regex;
    public String hard_block_range_replica_reads_tables_regex;
    public String hard_block_coord_writes_tables_regex;
    public String hard_block_replica_writes_tables_regex;

    public ThrottlingOptions()
    {
        setToDefault();
    }

    public void setToDefault()
    {
        // By default, we'd like to have rate limiter disabled.
        // When the field 'throttling_options' dones't exist in cassandra.yaml, which is the default, DatabaseDescriptor
        // will refer to the default values here.
        enabled = false;

        cpu_threshold_cur = 35;
        cpu_threshold_one_minute = 35;
        pending_reads_threshold_cur = 0;
        pending_reads_threshold_one_minute = 0;
        pending_mutations_threshold_cur = 0;
        pending_mutations_threshold_one_minute = 0;
        pending_native_transport_threshold_cur = 0;
        pending_native_transport_threshold_one_minute = 0;
        percentage_of_traffic_to_throttling = 0.1;
        more_aggressive_throttling_after_in_sec = 1 * 60; // 1 minutes
        reset_after_no_throttling_seen_in_sec = 15 * 60; // 15 minutes
        aggressive_throttling_qps_ratio = 4;
        aggressive_throttling_latency_ratio = 4;
        ignore_tables_regex = "^system.*\\..+|^pingless\\..+";
        health_check_init_delay_in_sec = 60;
        health_check_freq_in_sec = 1;
        throttle_read_replica_traffic = true;
        throttle_mutation_replica_traffic = true;
        hard_block_single_partition_coord_reads_tables_regex = "";
        hard_block_single_partition_replica_reads_tables_regex = "";
        hard_block_range_coord_reads_tables_regex = "";
        hard_block_range_replica_reads_tables_regex = "";
        hard_block_coord_writes_tables_regex = "";
        hard_block_replica_writes_tables_regex = "";
    }

    // getters and setters
    public boolean isEnabled()
    {
        return enabled;
    }

    public void setEnabled(boolean enabled)
    {
        this.enabled = enabled;
    }

    public long getCpuThresholdCur()
    {
        return cpu_threshold_cur;
    }

    public void setCpuThresholdCur(long cpu_threshold_cur)
    {
        this.cpu_threshold_cur = cpu_threshold_cur;
    }

    public long getCpuThresholdOneMinute()
    {
        return cpu_threshold_one_minute;
    }

    public void setCpuThresholdOneMinute(long cpu_threshold_one_minute)
    {
        this.cpu_threshold_one_minute = cpu_threshold_one_minute;
    }

    public int getPendingReadsThresholdCur()
    {
        return pending_reads_threshold_cur;
    }

    public void setPendingReadsThresholdCur(int pending_reads_threshold_cur)
    {
        this.pending_reads_threshold_cur = pending_reads_threshold_cur;
    }

    public int getPendingReadsThresholdOneMinute()
    {
        return pending_reads_threshold_one_minute;
    }

    public void setPendingReadsThresholdOneMinute(int pending_reads_threshold_one_minute)
    {
        this.pending_reads_threshold_one_minute = pending_reads_threshold_one_minute;
    }

    public int getPendingMutationsThresholdCur()
    {
        return pending_mutations_threshold_cur;
    }

    public void setPendingMutationsThresholdCur(int pending_mutations_threshold_cur)
    {
        this.pending_mutations_threshold_cur = pending_mutations_threshold_cur;
    }

    public int getPendingMutationsThresholdOneMinute()
    {
        return pending_mutations_threshold_one_minute;
    }

    public void setPendingMutationsThresholdOneMinute(int pending_mutations_threshold_one_minute)
    {
        this.pending_mutations_threshold_one_minute = pending_mutations_threshold_one_minute;
    }

    public int getPendingNativeTransportThresholdCur()
    {
        return pending_native_transport_threshold_cur;
    }

    public void setPendingNativeTransportThresholdCur(int pending_native_transport_threshold_cur)
    {
        this.pending_native_transport_threshold_cur = pending_native_transport_threshold_cur;
    }

    public int getPendingNativeTransportThresholdOneMinute()
    {
        return pending_native_transport_threshold_one_minute;
    }

    public void setPendingNativeTransportThresholdOneMinute(int pending_native_transport_threshold_one_minute)
    {
        this.pending_native_transport_threshold_one_minute = pending_native_transport_threshold_one_minute;
    }

    public double getPercentageOfTrafficToThrottling()
    {
        return percentage_of_traffic_to_throttling;
    }

    public void setPercentageOfTrafficToThrottling(double percentage_of_traffic_to_throttling)
    {
        this.percentage_of_traffic_to_throttling = percentage_of_traffic_to_throttling;
    }

    public int getMoreAggressiveThrottlingAfterInSec()
    {
        return more_aggressive_throttling_after_in_sec;
    }

    public void setMoreAggressiveThrottlingAfterInSec(int more_aggressive_throttling_after_in_sec)
    {
        this.more_aggressive_throttling_after_in_sec = more_aggressive_throttling_after_in_sec;
    }

    public int getResetAfterNoThrottlingSeenInSec()
    {
        return reset_after_no_throttling_seen_in_sec;
    }

    public void setResetAfterNoThrottlingSeenInSec(int reset_after_no_throttling_seen_in_sec)
    {
        this.reset_after_no_throttling_seen_in_sec = reset_after_no_throttling_seen_in_sec;
    }

    public double getAggressiveThrottlingQpsRatio()
    {
        return aggressive_throttling_qps_ratio;
    }

    public void setAggressiveThrottlingQpsRatio(double aggressive_throttling_qps_ratio)
    {
        this.aggressive_throttling_qps_ratio = aggressive_throttling_qps_ratio;
    }

    public double getAggressiveThrottlingLatencyRatio()
    {
        return aggressive_throttling_latency_ratio;
    }

    public void setAggressiveThrottlingLatencyRatio(double aggressive_throttling_latency_ratio)
    {
        this.aggressive_throttling_latency_ratio = aggressive_throttling_latency_ratio;
    }

    public String getIgnoreTablesRegex()
    {
        return ignore_tables_regex;
    }

    public void setIgnoreTablesRegex(String ignoreTablesRegex)
    {
        ignore_tables_regex = ignoreTablesRegex;
    }

    public int getHealthCheckInitDelayInSec()
    {
        return health_check_init_delay_in_sec;
    }

    public void setHealthCheckInitDelayInSec(int healthCheckInitDelayInSec)
    {
        this.health_check_init_delay_in_sec = healthCheckInitDelayInSec;
    }

    public int getHealthCheckPeriodInSec()
    {
        return health_check_freq_in_sec;
    }

    public void setHealthCheckFreqInSec(int healthCheckFreqInSec)
    {
        this.health_check_freq_in_sec = healthCheckFreqInSec;
    }

    public boolean getThrottleReadReplicaTraffic()
    {
        return throttle_read_replica_traffic;
    }

    public void setThrottleReadReplicaTraffic(boolean throttleReadReplicationTraffic)
    {
        this.throttle_read_replica_traffic = throttleReadReplicationTraffic;
    }

    public boolean getThrottleMutationReplicaTraffic()
    {
        return throttle_mutation_replica_traffic;
    }

    public void setThrottleMutationReplicaTraffic(boolean throttleMutationReplicationTraffic)
    {
        this.throttle_mutation_replica_traffic = throttleMutationReplicationTraffic;
    }

    public String getHardBlockSinglePartitionCoordReadsTablesRegex()
    {
        return hard_block_single_partition_coord_reads_tables_regex;
    }

    public void setHardBlockSinglePartitionCoordReadsTablesRegex(String hardBlockSinglePartitionCoordReadsTablesRegex)
    {
        this.hard_block_single_partition_coord_reads_tables_regex = hardBlockSinglePartitionCoordReadsTablesRegex;
    }

    public String getHardBlockSinglePartitionReplicaReadsTablesRegex()
    {
        return hard_block_single_partition_replica_reads_tables_regex;
    }

    public void setHardBlockSinglePartitionReplicaReadsTablesRegex(String hardBlockSinglePartitionReplicaReadsTablesRegex)
    {
        this.hard_block_single_partition_replica_reads_tables_regex = hardBlockSinglePartitionReplicaReadsTablesRegex;
    }

    public String getHardBlockRangeCoordReadsTablesRegex()
    {
        return hard_block_range_coord_reads_tables_regex;
    }

    public void setHardBlockRangeCoordReadsTablesRegex(String hardBlockRangeCoordReadsTablesRegex)
    {
        this.hard_block_range_coord_reads_tables_regex = hardBlockRangeCoordReadsTablesRegex;
    }

    public String getHardBlockRangeReplicaReadsTablesRegex()
    {
        return hard_block_range_replica_reads_tables_regex;
    }

    public void setHardBlockRangeReplicaReadsTablesRegex(String hardBlockRangeReplicaReadsTablesRegex)
    {
        this.hard_block_range_replica_reads_tables_regex = hardBlockRangeReplicaReadsTablesRegex;
    }

    public String getHardBlockCoordWritesTablesRegex()
    {
        return hard_block_coord_writes_tables_regex;
    }

    public void setHardBlockCoordWritesTablesRegex(String hardBlockCoordWritesTablesRegex)
    {
        this.hard_block_coord_writes_tables_regex = hardBlockCoordWritesTablesRegex;
    }

    public String getHardBlockReplicaWritesTablesRegex()
    {
        return hard_block_replica_writes_tables_regex;
    }

    public void setHardBlockReplicaWritesTablesRegex(String hardBlockReplicaWritesTablesRegex)
    {
        this.hard_block_replica_writes_tables_regex = hardBlockReplicaWritesTablesRegex;
    }

    // used in nodetool getratelimiterconfig
    public String toString()
    {
        return "enabled: " + enabled + '\n' +
               "current CPU threshold: " + cpu_threshold_cur + '\n' +
               "one minute CPU threshold: " + cpu_threshold_one_minute + '\n' +
               "current pending reads threshold: " + pending_reads_threshold_cur + '\n' +
               "one minute pending reads threshold: " + pending_reads_threshold_one_minute + '\n' +
               "current pending mutations threshold: " + pending_mutations_threshold_cur + '\n' +
               "one minute pending mutations threshold: " + pending_mutations_threshold_one_minute + '\n' +
               "current pending native transport threshold: " + pending_native_transport_threshold_cur + '\n' +
               "one minute pending native transport threshold: " + pending_native_transport_threshold_one_minute + '\n' +
               "percentage of traffic to throttle: " + percentage_of_traffic_to_throttling + '\n' +
               "more aggressive throttling after in seconds: " + more_aggressive_throttling_after_in_sec + '\n' +
               "reset after no throttling seen in seconds: " + reset_after_no_throttling_seen_in_sec + '\n' +
               "aggressive throttling qps ratio: " + aggressive_throttling_qps_ratio + '\n' +
               "aggressive throttling latency ratio: " + aggressive_throttling_latency_ratio + '\n' +
               "ignore tables regex: " + ignore_tables_regex + '\n' +
               "health initial delay in sec: " + health_check_init_delay_in_sec + '\n' +
               "health check frequency in sec: " + health_check_freq_in_sec + '\n' +
               "throttle read replica traffic: " + throttle_read_replica_traffic + '\n' +
               "throttle mutation replica traffic: " + throttle_mutation_replica_traffic + '\n' +
               "hard block single partition coordinator reads for tables regex: " + hard_block_single_partition_coord_reads_tables_regex + '\n' +
               "hard block single partition replica reads for tables regex: " + hard_block_single_partition_replica_reads_tables_regex + '\n' +
               "hard block range coordinator reads for tables regex: " + hard_block_range_coord_reads_tables_regex + '\n' +
               "hard block range replica reads for tables regex: " + hard_block_range_replica_reads_tables_regex + '\n' +
               "hard block coordinator writes for tables regex: " + hard_block_coord_writes_tables_regex + '\n' +
               "hard block replica writes for tables regex: " + hard_block_replica_writes_tables_regex + '\n' +
               "";
    }
}
