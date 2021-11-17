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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class LoadOldYAMLBackwardCompatibilityTest
{
    @BeforeClass
    public static void setupDatabaseDescriptor()
    {
        System.setProperty("cassandra.config", "cassandra-old.yaml");
        DatabaseDescriptor.daemonInitialization();
    }

    // CASSANDRA-15234
    @Test
    public void testConfigurationLoaderBackwardCompatibility()
    {
        Config config = DatabaseDescriptor.loadConfig();

        //Confirm parameters were successfully read with the default values in cassandra-old.yaml
        assertEquals(CassandraDuration.inMilliseconds(10800000), config.max_hint_window);
        assertEquals(CassandraDuration.inMilliseconds(0), config.native_transport_idle_timeout);
        assertEquals(CassandraDuration.inMilliseconds(10000), config.request_timeout);
        assertEquals(CassandraDuration.inMilliseconds(5000), config.read_request_timeout);
        assertEquals(CassandraDuration.inMilliseconds(10000), config.range_request_timeout);
        assertEquals(CassandraDuration.inMilliseconds(2000), config.write_request_timeout);
        assertEquals(CassandraDuration.inMilliseconds(5000), config.counter_write_request_timeout);
        assertEquals(CassandraDuration.inMilliseconds(1000), config.cas_contention_timeout);
        assertEquals(CassandraDuration.inMilliseconds(60000), config.truncate_request_timeout);
        assertEquals(CassandraDuration.inSeconds(300), config.streaming_keep_alive_period);
        assertEquals(CassandraDuration.inMilliseconds(500), config.slow_query_log_timeout);
        assertNull(config.memtable_heap_space);
        assertNull(config.memtable_offheap_space);
        assertNull( config.repair_session_space);
        assertEquals(DataStorage.inBytes(4194304), config.internode_application_send_queue_capacity);
        assertEquals(DataStorage.inBytes(134217728), config.internode_application_send_queue_reserve_endpoint_capacity);
        assertEquals(DataStorage.inBytes(536870912), config.internode_application_send_queue_reserve_global_capacity);
        assertEquals(DataStorage.inBytes(4194304), config.internode_application_receive_queue_capacity);
        assertEquals(DataStorage.inBytes(134217728), config.internode_application_receive_queue_reserve_endpoint_capacity);
        assertEquals(DataStorage.inBytes(536870912), config.internode_application_receive_queue_reserve_global_capacity);
        assertEquals(CassandraDuration.inMilliseconds(2000), config.internode_tcp_connect_timeout);
        assertEquals(CassandraDuration.inMilliseconds(30000), config.internode_tcp_user_timeout);
        assertEquals(CassandraDuration.inMilliseconds(300000), config.internode_streaming_tcp_user_timeout);
        assertEquals(DataStorage.inMegabytes(16), config.native_transport_max_frame_size);
        assertEquals(DataStorage.inMegabytes(256), config.max_value_size);
        assertEquals(DataStorage.inKilobytes(64), config.column_index_size);
        assertEquals(DataStorage.inKilobytes(2), config.column_index_cache_size);
        assertEquals(DataStorage.inKilobytes(5), config.batch_size_warn_threshold);
        assertEquals(BitRate.inMegabitsPerSecond(64), config.compaction_throughput);
        assertEquals(DataStorage.inMegabytes(50), config.min_free_space_per_drive);
        assertEquals(BitRate.inMegabitsPerSecond(200), config.stream_throughput_outbound);
        assertEquals(BitRate.inMegabitsPerSecond(200), config.inter_dc_stream_throughput_outbound);
        assertNull(config.commitlog_total_space);
        assertEquals(CassandraDuration.inMilliseconds(0), config.commitlog_sync_group_window);
        assertEquals(CassandraDuration.inMilliseconds(0), config.commitlog_sync_period);
        assertEquals(DataStorage.inMegabytes(32), config.commitlog_segment_size);
        assertNull(config.periodic_commitlog_sync_lag_block);  //Integer
        assertNull(config.max_mutation_size);
        assertEquals(DataStorage.inMegabytes(0), config.cdc_total_space);
        assertEquals(CassandraDuration.inMilliseconds(250), config.cdc_free_space_check_interval);
        assertEquals(CassandraDuration.inMilliseconds(100), config.dynamic_snitch_update_interval);
        assertEquals(CassandraDuration.inMilliseconds(600000), config.dynamic_snitch_reset_interval);
        assertEquals(DataStorage.inKilobytes(1024), config.hinted_handoff_throttle);
        assertEquals(DataStorage.inKilobytes(1024), config.batchlog_replay_throttle);
        assertEquals(CassandraDuration.inMilliseconds(10000), config.hints_flush_period);
        assertEquals(DataStorage.inMegabytes(128), config.max_hints_file_size);
        assertEquals(DataStorage.inKilobytes(10240), config.trickle_fsync_interval);
        assertEquals(DataStorage.inMegabytes(50), config.sstable_preemptive_open_interval);
        assertNull( config.key_cache_size);
        assertEquals(DataStorage.inMegabytes(0), config.row_cache_size);
        assertNull(config.counter_cache_size);
        assertNull(config.networking_cache_size);
        assertNull(config.file_cache_size);
        assertNull(config.index_summary_capacity);
        assertEquals(CassandraDuration.inMilliseconds(200), config.gc_log_threshold);
        assertEquals(CassandraDuration.inMilliseconds(1000), config.gc_warn_threshold);
        assertEquals(CassandraDuration.inSeconds(86400), config.trace_type_query_ttl);
        assertEquals(CassandraDuration.inSeconds(604800), config.trace_type_repair_ttl);
        assertNull(config.prepared_statements_cache_size);
        assertFalse(config.user_defined_functions_enabled);
        assertFalse(config.scripted_user_defined_functions_enabled);
        assertFalse(config.materialized_views_enabled);
        assertFalse(config.transient_replication_enabled);
        assertFalse(config.sasi_indexes_enabled);
        assertFalse(config.drop_compact_storage_enabled);
        assertTrue(config.user_defined_functions_threads_enabled);
        assertEquals(CassandraDuration.inMilliseconds(2000), config.permissions_validity);
        assertEquals(CassandraDuration.inMilliseconds(0), config.permissions_update_interval);
        assertEquals(CassandraDuration.inMilliseconds(2000), config.roles_validity);
        assertEquals(CassandraDuration.inMilliseconds(0), config.roles_update_interval);
        assertEquals(CassandraDuration.inMilliseconds(2000), config.credentials_validity);
        assertEquals(CassandraDuration.inMilliseconds(0), config.credentials_update_interval);
        assertEquals(CassandraDuration.inMinutes(60), config.index_summary_resize_interval);
    }
}