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
package org.apache.cassandra.config;

import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class ParseAndConvertUnitsTest
{
    @BeforeClass
    public static void setupDatabaseDescriptor()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    // CASSANDRA-15234
    @Test
    public void testConfigurationLoaderParser()
    {
        Config config = DatabaseDescriptor.loadConfig();

        //Confirm duration parameters were successfully parsed with the default values in cassandra.yaml
        assertEquals(DurationSpec.inMilliseconds(10800000), config.max_hint_window);
        assertEquals(DurationSpec.inMilliseconds(0), config.native_transport_idle_timeout);
        assertEquals(DurationSpec.inMilliseconds(10000), config.request_timeout);
        assertEquals(DurationSpec.inMilliseconds(5000), config.read_request_timeout);
        assertEquals(DurationSpec.inMilliseconds(10000), config.range_request_timeout);
        assertEquals(DurationSpec.inMilliseconds(2000), config.write_request_timeout);
        assertEquals(DurationSpec.inMilliseconds(5000), config.counter_write_request_timeout);
        assertEquals(DurationSpec.inMilliseconds(1800), config.cas_contention_timeout);
        assertEquals(DurationSpec.inMilliseconds(60000), config.truncate_request_timeout);
        assertEquals(DurationSpec.inSeconds(300), config.streaming_keep_alive_period);
        assertEquals(DurationSpec.inMilliseconds(500), config.slow_query_log_timeout);
        assertEquals(DurationSpec.inMilliseconds(2000), config.internode_tcp_connect_timeout);
        assertEquals(DurationSpec.inMilliseconds(30000), config.internode_tcp_user_timeout);
        assertEquals(DurationSpec.inMilliseconds(0), config.commitlog_sync_group_window);
        assertEquals(DurationSpec.inMilliseconds(0), config.commitlog_sync_period);
        assertNull(config.periodic_commitlog_sync_lag_block);
        assertEquals(DurationSpec.inMilliseconds(250), config.cdc_free_space_check_interval);
        assertEquals(DurationSpec.inMilliseconds(100), config.dynamic_snitch_update_interval);
        assertEquals(DurationSpec.inMilliseconds(600000), config.dynamic_snitch_reset_interval);
        assertEquals(DurationSpec.inMilliseconds(200), config.gc_log_threshold);
        assertEquals(DurationSpec.inMilliseconds(10000), config.hints_flush_period);
        assertEquals(DurationSpec.inMilliseconds(1000), config.gc_warn_threshold);
        assertEquals(DurationSpec.inSeconds(86400), config.trace_type_query_ttl);
        assertEquals(DurationSpec.inSeconds(604800), config.trace_type_repair_ttl);
        assertEquals(DurationSpec.inMilliseconds(2000), config.permissions_validity);
        assertEquals(DurationSpec.inMilliseconds(0), config.permissions_update_interval);
        assertEquals(DurationSpec.inMilliseconds(2000), config.roles_validity);
        assertEquals(DurationSpec.inMilliseconds(0), config.roles_update_interval);
        assertEquals(DurationSpec.inMilliseconds(2000), config.credentials_validity);
        assertEquals(DurationSpec.inMilliseconds(0), config.credentials_update_interval);
        assertEquals(DurationSpec.inMinutes(60), config.index_summary_resize_interval);
        assertEquals(DurationSpec.inHours(4), config.key_cache_save_period);

        //Confirm space parameters were successfully parsed with the default values in cassandra.yaml
        assertNull(config.memtable_heap_space);
        assertNull(config.memtable_offheap_space);
        assertNull(config.repair_session_space); //null everywhere so should be correct, let's check whether it will bomb
        assertEquals(new DataStorageSpec("4194304B"), config.internode_application_send_queue_capacity);
        assertEquals(new DataStorageSpec("134217728B"), config.internode_application_send_queue_reserve_endpoint_capacity);
        assertEquals(new DataStorageSpec("536870912B"), config.internode_application_send_queue_reserve_global_capacity);
        assertEquals(new DataStorageSpec("4194304B"), config.internode_application_receive_queue_capacity);
        assertEquals(new DataStorageSpec("134217728B"), config.internode_application_receive_queue_reserve_endpoint_capacity);
        assertEquals(new DataStorageSpec("536870912B"), config.internode_application_receive_queue_reserve_global_capacity);
        assertEquals(new DataStorageSpec("16MiB"), config.native_transport_max_frame_size);
        assertEquals(new DataStorageSpec("256MiB"), config.max_value_size);
        assertEquals(new DataStorageSpec("4KiB"), config.column_index_size);
        assertEquals(new DataStorageSpec("2KiB"), config.column_index_cache_size);
        assertEquals(new DataStorageSpec("5KiB"), config.batch_size_warn_threshold);
        assertEquals(new DataStorageSpec("50KiB"), config.batch_size_fail_threshold);
        assertEquals(new DataStorageSpec("100MiB"), config.compaction_large_partition_warning_threshold);
        assertNull(config.commitlog_total_space);
        assertEquals(new DataStorageSpec("5MiB"), config.commitlog_segment_size);
        assertNull(config.max_mutation_size); //not set explicitly in the default yaml, check the config; not set there too
        assertEquals(new DataStorageSpec("0MiB"), config.cdc_total_space);
        assertEquals(new DataStorageSpec("1024KiB"), config.hinted_handoff_throttle);
        assertEquals(new DataStorageSpec("1024KiB"), config.batchlog_replay_throttle);
        assertEquals(new DataStorageSpec("10240KiB"), config.trickle_fsync_interval);
        assertEquals(new DataStorageSpec("50MiB"), config.sstable_preemptive_open_interval);
        assertNull(config.counter_cache_size);
        assertNull(config.file_cache_size);
        assertNull(config.index_summary_capacity);
        assertEquals(new DataStorageSpec("1MiB"), config.prepared_statements_cache_size);
        assertNull(config.key_cache_size);
        assertEquals(new DataStorageSpec("16MiB"), config.row_cache_size);

        //Confirm rate parameters were successfully parsed with the default values in cassandra.yaml
        assertEquals(DataRateSpec.inMebibytesPerSecond(0), config.compaction_throughput);
        assertEquals(DataRateSpec.inMebibytesPerSecond(23841858), config.stream_throughput_outbound);
        assertEquals(DataRateSpec.inMebibytesPerSecond(24), config.inter_dc_stream_throughput_outbound);
    }
}