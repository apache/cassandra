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
    public boolean enabled = false;

    // for checking CPU signals
    public long cpu_threshold_one_minute = 80;
    public long pending_reads_threshold_one_minute = 10;
    public long pending_mutations_threshold_one_minute = 10;
    public long pending_native_transport_threshold_one_minute = 10;

    // for checking threadpool signals
    public long threadpool_threshold_reads = 100000; // 100K
    public long threadpool_threshold_writes = 100000; // 100K
    public long threadpool_threshold_native_transport = 100000; // 100K

    public double percentage_of_traffic_to_throttling = 0.1;
    public int more_aggressive_throttling_after_in_sec = 1 * 60; // 1 minutes
    public int reset_after_no_throttling_seen_in_sec = 15 * 60; // 15 minutes
    public double aggressive_throttling_qps_ratio = 5;
    public double aggressive_throttling_latency_ratio = 5;
    public String ignore_tables_regex = "^system.*\\..+|^pingless\\..+";
    public int health_check_init_delay_in_sec = 60;
    public int health_check_freq_in_sec = 1;
    public boolean throttle_read_replica_traffic = true;
    public boolean throttle_mutation_replica_traffic = false;
    public String hard_block_single_partition_coord_reads_tables_regex = "";
    public String hard_block_single_partition_replica_reads_tables_regex = "";
    public String hard_block_range_coord_reads_tables_regex = "";
    public String hard_block_range_replica_reads_tables_regex = "";
    public String hard_block_coord_writes_tables_regex = "";
    public String hard_block_replica_writes_tables_regex = "";

    public ThrottlingOptions()
    {
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

    public long getCpuThresholdOneMinute()
    {
        return cpu_threshold_one_minute;
    }

    public void setCpuThresholdOneMinute(long cpu_threshold_one_minute)
    {
        this.cpu_threshold_one_minute = cpu_threshold_one_minute;
    }

    public long getPendingReadsThresholdOneMinute()
    {
        return pending_reads_threshold_one_minute;
    }

    public void setPendingReadsThresholdOneMinute(int pending_reads_threshold_one_minute)
    {
        this.pending_reads_threshold_one_minute = pending_reads_threshold_one_minute;
    }

    public long getPendingMutationsThresholdOneMinute()
    {
        return pending_mutations_threshold_one_minute;
    }

    public void setPendingMutationsThresholdOneMinute(int pending_mutations_threshold_one_minute)
    {
        this.pending_mutations_threshold_one_minute = pending_mutations_threshold_one_minute;
    }

    public long getPendingNativeTransportThresholdOneMinute()
    {
        return pending_native_transport_threshold_one_minute;
    }

    public void setPendingNativeTransportThresholdOneMinute(int pending_native_transport_threshold_one_minute)
    {
        this.pending_native_transport_threshold_one_minute = pending_native_transport_threshold_one_minute;
    }

    public long getThreadpoolThresholdReads() {
        return threadpool_threshold_reads;
    }

    public void setThreadpoolThresholdReads(long threadpoolThresholdReads) {
        threadpool_threshold_reads = threadpoolThresholdReads;
    }

    public long getThreadpoolThresholdWrites() {
        return threadpool_threshold_writes;
    }

    public void setThreadpoolThresholdWrites(long threadpoolThresholdWrites) {
        threadpool_threshold_writes = threadpoolThresholdWrites;
    }

    public long getThreadpoolThresholdNativeTransport() {
        return threadpool_threshold_native_transport;
    }

    public void setThreadpoolThresholdNativeTransport(long threadpoolThresholdNativeTransport) {
        threadpool_threshold_native_transport = threadpoolThresholdNativeTransport;
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
               "one minute CPU threshold: " + cpu_threshold_one_minute + '\n' +
               "one minute pending reads threshold: " + pending_reads_threshold_one_minute + '\n' +
               "one minute pending mutations threshold: " + pending_mutations_threshold_one_minute + '\n' +
               "one minute pending native transport threshold: " + pending_native_transport_threshold_one_minute + '\n' +
               "threadpool pending task threshold for reads: " + threadpool_threshold_reads + '\n' +
               "threadpool pending task threshold for writes: " + threadpool_threshold_writes + '\n' +
               "threadpool pending task threshold for native transport: " + threadpool_threshold_native_transport + '\n' +
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
