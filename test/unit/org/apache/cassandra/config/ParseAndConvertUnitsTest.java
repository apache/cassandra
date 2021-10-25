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
        assertEquals(Duration.inMilliseconds(10800000), config.max_hint_window);
        assertEquals(Duration.inMilliseconds(0), config.native_transport_idle_timeout);
        assertEquals(Duration.inMilliseconds(10000), config.request_timeout);
        assertEquals(Duration.inMilliseconds(5000), config.read_request_timeout);
        assertEquals(Duration.inMilliseconds(10000), config.range_request_timeout);
        assertEquals(Duration.inMilliseconds(2000), config.write_request_timeout);
        assertEquals(Duration.inMilliseconds(5000), config.counter_write_request_timeout);
        assertEquals(Duration.inMilliseconds(1000), config.cas_contention_timeout);
        assertEquals(Duration.inMilliseconds(60000), config.truncate_request_timeout);
        assertEquals(Duration.inSeconds(300), config.streaming_keep_alive_period);
        assertEquals(Duration.inMilliseconds(500), config.slow_query_log_timeout);
        assertEquals(Duration.inMilliseconds(2000), config.internode_tcp_connect_timeout);
        assertEquals(Duration.inMilliseconds(30000), config.internode_tcp_user_timeout);
        assertEquals(Duration.inMilliseconds(0), config.commitlog_sync_group_window);
        assertEquals(Duration.inMilliseconds(0), config.commitlog_sync_period);
        assertNull(config.periodic_commitlog_sync_lag_block);
        assertEquals(Duration.inMilliseconds(250), config.cdc_free_space_check_interval);
        assertEquals(Duration.inMilliseconds(100), config.dynamic_snitch_update_interval);
        assertEquals(Duration.inMilliseconds(600000), config.dynamic_snitch_reset_interval);
        assertEquals(Duration.inMilliseconds(200), config.gc_log_threshold);
        assertEquals(Duration.inMilliseconds(10000), config.hints_flush_period);
        assertEquals(Duration.inMilliseconds(1000), config.gc_warn_threshold);
        assertEquals(Duration.inSeconds(86400), config.tracetype_query_ttl);
        assertEquals(Duration.inSeconds(604800), config.tracetype_repair_ttl);
        assertEquals(Duration.inMilliseconds(2000), config.permissions_validity);
        assertEquals(Duration.inMilliseconds(0), config.permissions_update_interval);
        assertEquals(Duration.inMilliseconds(2000), config.roles_validity);
        assertEquals(Duration.inMilliseconds(0),config.roles_update_interval);
        assertEquals(Duration.inMilliseconds(2000), config.credentials_validity);
        assertEquals(Duration.inMilliseconds(0), config.credentials_update_interval);
        assertEquals(Duration.inMinutes(60), config.index_summary_resize_interval);

        //Confirm space parameters were successfully parsed with the default values in cassandra.yaml
        assertEquals(null, config.memtable_heap_space);
        assertEquals(null, config.memtable_offheap_space);
        assertEquals(null, config.repair_session_space); //null everywhere so should be correct, let's check whether it will bomb
        assertEquals(new DataStorage("4194304B"), config.internode_application_send_queue_capacity);
        assertEquals(new DataStorage("134217728B"), config.internode_application_send_queue_reserve_endpoint_capacity);
        assertEquals(new DataStorage("536870912B"), config.internode_application_send_queue_reserve_global_capacity);
        assertEquals(new DataStorage("4194304B"), config.internode_application_receive_queue_capacity);
        assertEquals(new DataStorage("134217728B"), config.internode_application_receive_queue_reserve_endpoint_capacity);
        assertEquals(new DataStorage("536870912B"), config.internode_application_receive_queue_reserve_global_capacity);
        assertEquals(new DataStorage("256MB"), config.max_native_transport_frame_size);
        //assertEquals(new DataStorage("32KB"), config.native_transport_frame_block_size);
        assertEquals(new DataStorage("256MB"), config.max_value_size);
        assertEquals(new DataStorage("4KB"), config.column_index_size);
        assertEquals(new DataStorage("2KB"), config.column_index_cache_size);
        assertEquals(new DataStorage("5KB"), config.batch_size_warn_threshold);
        assertEquals(new DataStorage("50KB"), config.batch_size_fail_threshold);
        assertEquals(new DataStorage("100MB"), config.compaction_large_partition_warning_threshold);
        assertNull(config.commitlog_total_space);
        assertEquals(new DataStorage("5MB"), config.commitlog_segment_size);
        assertNull(config.max_mutation_size); //not set explicitly in the default yaml, check the config; not set there too
        assertEquals(new DataStorage("0MB"), config.cdc_total_space);
        assertEquals(new DataStorage("1024KB"), config.hinted_handoff_throttle);
        assertEquals(new DataStorage("1024KB"), config.batchlog_replay_throttle);
        assertEquals(new DataStorage("10240KB"), config.trickle_fsync_interval);
        assertEquals(new DataStorage("50MB"), config.sstable_preemptive_open_interval);
        assertNull(config.counter_cache_size);
        assertNull(config.file_cache_size);
        assertNull(config.index_summary_capacity);
        assertEquals(new DataStorage("1MB"), config.prepared_statements_cache_size);
        assertNull(config.key_cache_size);
        assertEquals(new DataStorage("16MB"), config.row_cache_size);

        //Confirm rate parameters were successfully parsed with the default values in cassandra.yaml
        assertEquals(BitRate.inMegabitsPerSecond(0), config.compaction_throughput);
        assertEquals(BitRate.inMegabitsPerSecond(200000000), config.stream_throughput_outbound);
        assertEquals(BitRate.inMegabitsPerSecond(200), config.inter_dc_stream_throughput_outbound);
    }
};




