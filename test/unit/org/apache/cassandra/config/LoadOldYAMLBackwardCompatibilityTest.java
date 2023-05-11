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

import java.util.concurrent.TimeUnit;

import org.junit.BeforeClass;
import org.junit.Test;

import static org.apache.cassandra.config.CassandraRelevantProperties.CASSANDRA_CONFIG;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class LoadOldYAMLBackwardCompatibilityTest
{
    @BeforeClass
    public static void setupDatabaseDescriptor()
    {
        CASSANDRA_CONFIG.setString("cassandra-old.yaml");
        DatabaseDescriptor.daemonInitialization();
    }

    // CASSANDRA-15234
    @Test
    public void testConfigurationLoaderBackwardCompatibility()
    {
        Config config = DatabaseDescriptor.loadConfig();

        assertEquals(new DurationSpec.IntMillisecondsBound(10800000), config.max_hint_window);
        assertEquals(new DurationSpec.IntMillisecondsBound("3h"), config.max_hint_window);
        assertEquals(new DurationSpec.LongMillisecondsBound(0), config.native_transport_idle_timeout);
        assertEquals(new DurationSpec.LongMillisecondsBound(10000), config.request_timeout);
        assertEquals(new DurationSpec.LongMillisecondsBound(5000), config.read_request_timeout);
        assertEquals(new DurationSpec.LongMillisecondsBound(10000), config.range_request_timeout);
        assertEquals(new DurationSpec.LongMillisecondsBound(2000), config.write_request_timeout);
        assertEquals(new DurationSpec.LongMillisecondsBound(5000), config.counter_write_request_timeout);
        assertEquals(new DurationSpec.LongMillisecondsBound(1800), config.cas_contention_timeout);
        assertEquals(new DurationSpec.LongMillisecondsBound(60000), config.truncate_request_timeout);
        assertEquals(new DurationSpec.IntSecondsBound(300), config.streaming_keep_alive_period);
        assertEquals(new DurationSpec.LongMillisecondsBound(500), config.slow_query_log_timeout);
        assertNull(config.memtable_heap_space);
        assertNull(config.memtable_offheap_space);
        assertNull( config.repair_session_space);
        assertEquals(new DataStorageSpec.IntBytesBound(4194304), config.internode_application_send_queue_capacity);
        assertEquals(new DataStorageSpec.IntBytesBound(134217728), config.internode_application_send_queue_reserve_endpoint_capacity);
        assertEquals(new DataStorageSpec.IntBytesBound(536870912), config.internode_application_send_queue_reserve_global_capacity);
        assertEquals(new DataStorageSpec.IntBytesBound(4194304), config.internode_application_receive_queue_capacity);
        assertEquals(new DataStorageSpec.IntBytesBound(134217728), config.internode_application_receive_queue_reserve_endpoint_capacity);
        assertEquals(new DataStorageSpec.IntBytesBound(536870912), config.internode_application_receive_queue_reserve_global_capacity);
        assertEquals(new DurationSpec.IntMillisecondsBound(2000), config.internode_tcp_connect_timeout);
        assertEquals(new DurationSpec.IntMillisecondsBound(30000), config.internode_tcp_user_timeout);
        assertEquals(new DurationSpec.IntMillisecondsBound(300000), config.internode_streaming_tcp_user_timeout);
        assertEquals(new DataStorageSpec.IntMebibytesBound(16), config.native_transport_max_frame_size);
        assertEquals(new DataStorageSpec.IntMebibytesBound(256), config.max_value_size);
        assertEquals(new DataStorageSpec.IntKibibytesBound(4), config.column_index_size);
        assertEquals(new DataStorageSpec.IntKibibytesBound(2), config.column_index_cache_size);
        assertEquals(new DataStorageSpec.IntKibibytesBound(5), config.batch_size_warn_threshold);
        assertEquals(new DataRateSpec.LongBytesPerSecondBound(64, DataRateSpec.DataRateUnit.MEBIBYTES_PER_SECOND), config.compaction_throughput);
        assertEquals(new DataStorageSpec.IntMebibytesBound(50), config.min_free_space_per_drive);
        assertEquals(new DataRateSpec.LongBytesPerSecondBound(25000000000000L).toString(), config.stream_throughput_outbound.toString());
        assertEquals(DataRateSpec.LongBytesPerSecondBound.megabitsPerSecondInBytesPerSecond(200000000), config.stream_throughput_outbound);
        assertEquals(new DataRateSpec.LongBytesPerSecondBound(24L  * 1024L * 1024L), config.inter_dc_stream_throughput_outbound);
        assertNull(config.commitlog_total_space);
        assertEquals(new DurationSpec.IntMillisecondsBound(0.0, TimeUnit.MILLISECONDS), config.commitlog_sync_group_window);
        assertEquals(new DurationSpec.IntMillisecondsBound(0), config.commitlog_sync_period);
        assertEquals(new DataStorageSpec.IntMebibytesBound(5), config.commitlog_segment_size);
        assertNull(config.periodic_commitlog_sync_lag_block);  //Integer
        assertNull(config.max_mutation_size);
        assertEquals(new DataStorageSpec.IntMebibytesBound(0), config.cdc_total_space);
        assertEquals(new DurationSpec.IntMillisecondsBound(250), config.cdc_free_space_check_interval);
        assertEquals(new DurationSpec.IntMillisecondsBound(100), config.dynamic_snitch_update_interval);
        assertEquals(new DurationSpec.IntMillisecondsBound(600000), config.dynamic_snitch_reset_interval);
        assertEquals(new DataStorageSpec.IntKibibytesBound(1024), config.hinted_handoff_throttle);
        assertEquals(new DataStorageSpec.IntKibibytesBound(1024), config.batchlog_replay_throttle);
        assertEquals(new DurationSpec.IntMillisecondsBound(10000), config.hints_flush_period);
        assertEquals(new DataStorageSpec.IntMebibytesBound(128), config.max_hints_file_size);
        assertEquals(new DataStorageSpec.IntKibibytesBound(10240), config.trickle_fsync_interval);
        assertEquals(new DataStorageSpec.IntMebibytesBound(50), config.sstable_preemptive_open_interval);
        assertNull( config.key_cache_size);
        assertEquals(new DataStorageSpec.LongMebibytesBound(16), config.row_cache_size);
        assertNull(config.counter_cache_size);
        assertNull(config.networking_cache_size);
        assertNull(config.file_cache_size);
        assertNull(config.index_summary_capacity);
        assertEquals(new DurationSpec.IntMillisecondsBound(200), config.gc_log_threshold);
        assertEquals(new DurationSpec.IntMillisecondsBound(1000), config.gc_warn_threshold);
        assertEquals(new DurationSpec.IntSecondsBound(86400), config.trace_type_query_ttl);
        assertEquals(new DurationSpec.IntSecondsBound(604800), config.trace_type_repair_ttl);
        assertNull(config.prepared_statements_cache_size);
        assertTrue(config.user_defined_functions_enabled);
        assertFalse(config.scripted_user_defined_functions_enabled);
        assertTrue(config.materialized_views_enabled);
        assertFalse(config.transient_replication_enabled);
        assertTrue(config.sasi_indexes_enabled);
        assertTrue(config.drop_compact_storage_enabled);
        assertTrue(config.user_defined_functions_threads_enabled);
        assertEquals(new DurationSpec.IntMillisecondsBound(2000), config.permissions_validity);
        assertNull(config.permissions_update_interval);
        assertEquals(new DurationSpec.IntMillisecondsBound(2000), config.roles_validity);
        assertNull(config.roles_update_interval);
        assertEquals(new DurationSpec.IntMillisecondsBound(2000), config.credentials_validity);
        assertNull(config.credentials_update_interval);
        assertEquals(new DurationSpec.IntMinutesBound(60), config.index_summary_resize_interval);

        //parameters which names have not changed with CASSANDRA-15234
        assertEquals(DurationSpec.IntSecondsBound.inSecondsString("14400"), config.key_cache_save_period);
        assertEquals(DurationSpec.IntSecondsBound.inSecondsString("14400s"), config.key_cache_save_period);
        assertEquals(new DurationSpec.IntSecondsBound(4, TimeUnit.HOURS), config.key_cache_save_period);
        assertEquals(DurationSpec.IntSecondsBound.inSecondsString("0"), config.row_cache_save_period);
        assertEquals(new DurationSpec.IntSecondsBound(0), config.row_cache_save_period);
        assertEquals(new DurationSpec.IntSecondsBound(2, TimeUnit.HOURS), config.counter_cache_save_period);
        assertEquals(new DurationSpec.IntSecondsBound(35), config.cache_load_timeout);
    }
}
