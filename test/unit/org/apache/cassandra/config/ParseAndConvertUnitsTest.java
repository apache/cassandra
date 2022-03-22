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
        assertNull(config.permissions_update_interval);
        assertEquals(DurationSpec.inMilliseconds(2000), config.roles_validity);
        assertNull(config.roles_update_interval);
        assertEquals(DurationSpec.inMilliseconds(2000), config.credentials_validity);
        assertNull(config.credentials_update_interval);
        assertEquals(DurationSpec.inMinutes(60), config.index_summary_resize_interval);
        assertEquals(DurationSpec.inHours(4), config.key_cache_save_period);
        assertEquals(DurationSpec.inSeconds(30), config.cache_load_timeout);
        assertEquals(DurationSpec.inMilliseconds(1500), config.user_defined_functions_fail_timeout);
        assertEquals(DurationSpec.inMilliseconds(500), config.user_defined_functions_warn_timeout);
        assertEquals(DurationSpec.inSeconds(3600), config.validation_preview_purge_head_start);

        //Confirm space parameters were successfully parsed with the default values in cassandra.yaml
        assertNull(config.memtable_heap_space);
        assertNull(config.memtable_offheap_space);
        assertNull(config.repair_session_space); //null everywhere so should be correct, let's check whether it will bomb
        assertEquals(DataStorageSpec.inBytes(4194304), config.internode_application_send_queue_capacity);
        assertEquals(DataStorageSpec.inBytes(134217728), config.internode_application_send_queue_reserve_endpoint_capacity);
        assertEquals(DataStorageSpec.inBytes(536870912), config.internode_application_send_queue_reserve_global_capacity);
        assertEquals(DataStorageSpec.inBytes(4194304), config.internode_application_receive_queue_capacity);
        assertEquals(DataStorageSpec.inBytes(134217728), config.internode_application_receive_queue_reserve_endpoint_capacity);
        assertEquals(DataStorageSpec.inBytes(536870912), config.internode_application_receive_queue_reserve_global_capacity);
        assertEquals(DataStorageSpec.inMebibytes(16), config.native_transport_max_frame_size);
        assertEquals(DataStorageSpec.inMebibytes(256), config.max_value_size);
        assertEquals(DataStorageSpec.inKibibytes(4), config.column_index_size);
        assertEquals(DataStorageSpec.inKibibytes(2), config.column_index_cache_size);
        assertEquals(DataStorageSpec.inKibibytes(5), config.batch_size_warn_threshold);
        assertEquals(DataStorageSpec.inKibibytes(50), config.batch_size_fail_threshold);
        assertEquals(DataStorageSpec.inMebibytes(100), config.compaction_large_partition_warning_threshold);
        assertNull(config.commitlog_total_space);
        assertEquals(DataStorageSpec.inMebibytes(5), config.commitlog_segment_size);
        assertNull(config.max_mutation_size); //not set explicitly in the default yaml, check the config; not set there too
        assertEquals(DataStorageSpec.inMebibytes(0), config.cdc_total_space);
        assertEquals(DataStorageSpec.inKibibytes(1024), config.hinted_handoff_throttle);
        assertEquals(DataStorageSpec.inKibibytes(1024), config.batchlog_replay_throttle);
        assertEquals(DataStorageSpec.inKibibytes(10240), config.trickle_fsync_interval);
        assertEquals(DataStorageSpec.inMebibytes(50), config.sstable_preemptive_open_interval);
        assertNull(config.counter_cache_size);
        assertNull(config.file_cache_size);
        assertNull(config.index_summary_capacity);
        assertEquals(DataStorageSpec.inMebibytes(1), config.prepared_statements_cache_size);
        assertNull(config.key_cache_size);
        assertEquals(DataStorageSpec.inMebibytes(16), config.row_cache_size);
        assertNull(config.native_transport_max_request_data_in_flight);
        assertNull(config.native_transport_max_request_data_in_flight_per_ip);
        assertEquals(DataStorageSpec.inMebibytes(1), config.native_transport_receive_queue_capacity);

        //Confirm rate parameters were successfully parsed with the default values in cassandra.yaml
        assertEquals(DataRateSpec.inMebibytesPerSecond(0), config.compaction_throughput);
        assertEquals(DataRateSpec.inMebibytesPerSecond(23841858), config.stream_throughput_outbound);
        assertEquals(DataRateSpec.inMebibytesPerSecond(24), config.inter_dc_stream_throughput_outbound);
    }
}