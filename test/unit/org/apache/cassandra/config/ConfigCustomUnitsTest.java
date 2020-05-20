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

import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.URL;
import java.util.Arrays;
import java.util.Collection;
import java.util.Enumeration;

import com.google.common.base.Throwables;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandra.OrderedJUnit4ClassRunner;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.exceptions.ConfigurationException;

import static org.junit.Assert.assertEquals;

@RunWith(OrderedJUnit4ClassRunner.class)
public class ConfigCustomUnitsTest
{
    @BeforeClass
    public static void setupDatabaseDescriptor()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    // CASSANDRA-15234
    @Test
    public void testConfigurationLoaderParser() throws Exception
    {
        Config config = DatabaseDescriptor.loadConfig();

        //Confirm duration parameters were successfully parsed with the default values in cassandra.yaml
        assertEquals(10800000, config.max_hint_window_in_ms);
        assertEquals(0, config.native_transport_idle_timeout_in_ms);
        assertEquals(10000, config.request_timeout_in_ms);
        assertEquals(5000, config.read_request_timeout_in_ms);
        assertEquals(10000, config.range_request_timeout_in_ms);
        assertEquals(2000, config.write_request_timeout_in_ms);
        assertEquals(5000, config.counter_write_request_timeout_in_ms);
        assertEquals(1000, config.cas_contention_timeout_in_ms);
        assertEquals(60000, config.truncate_request_timeout_in_ms);
        assertEquals(300, config.streaming_keep_alive_period_in_secs, 0);
        assertEquals(500, config.slow_query_log_timeout_in_ms);
        assertEquals(2000, config.internode_tcp_connect_timeout_in_ms);
        assertEquals(30000, config.internode_tcp_user_timeout_in_ms);
        assertEquals(0.0, config.commitlog_sync_batch_window_in_ms, 0);
        assertEquals(Double.NaN, config.commitlog_sync_group_window_in_ms,0);
        assertEquals(0, config.commitlog_sync_period_in_ms);
        assertEquals(1, config.periodic_commitlog_sync_lag_block_in_ms, 0);
        assertEquals(250, config.cdc_free_space_check_interval_ms);
        assertEquals(100, config.dynamic_snitch_update_interval_in_ms);
        assertEquals(600000, config.dynamic_snitch_reset_interval_in_ms);
        assertEquals(200, config.gc_log_threshold_in_ms);
        assertEquals(10000, config.hints_flush_period_in_ms);
        assertEquals(1000, config.gc_warn_threshold_in_ms);
        assertEquals(86400, config.tracetype_query_ttl_in_s);
        assertEquals(604800, config.tracetype_repair_ttl_in_s);
        assertEquals(2000, config.permissions_validity_in_ms);
        assertEquals(-1, config.permissions_update_interval_in_ms);
        assertEquals(2000, config.roles_validity_in_ms);
        assertEquals(-1, config.roles_update_interval_in_ms);
        assertEquals(2000, config.credentials_validity_in_ms);
        assertEquals(-1, config.credentials_update_interval_in_ms);
        assertEquals(60, config.index_summary_resize_interval_in_minutes);

        //Confirm space parameters were successfully parsed with the default values in cassandra.yaml
        assertEquals(null, config.memtable_heap_space_in_mb);
        assertEquals(null, config.memtable_offheap_space_in_mb );
        assertEquals(null, config.repair_session_space_in_mb); //null everywhere so should be correct, let's check whether it will bomb
        assertEquals(4194304, config.internode_application_send_queue_capacity_in_bytes);
        assertEquals(134217728, config.internode_application_send_queue_reserve_endpoint_capacity_in_bytes);
        assertEquals(536870912, config.internode_application_send_queue_reserve_global_capacity_in_bytes);
        assertEquals(4194304, config.internode_application_receive_queue_capacity_in_bytes);
        assertEquals(134217728, config.internode_application_receive_queue_reserve_endpoint_capacity_in_bytes);
        assertEquals(536870912, config.internode_application_receive_queue_reserve_global_capacity_in_bytes);
        assertEquals(256, config.max_native_transport_frame_size_in_mb);
        assertEquals(32, config.native_transport_frame_block_size_in_kb);
        assertEquals(256, config.max_value_size_in_mb);
        assertEquals(4, config.column_index_size_in_kb);
        assertEquals(2, config.column_index_cache_size_in_kb);
        assertEquals(5, config.batch_size_warn_threshold_in_kb);
        assertEquals(50, config.batch_size_fail_threshold_in_kb);
        assertEquals(100, config.compaction_large_partition_warning_threshold_mb);
        assertEquals(null, config.commitlog_total_space_in_mb);
        assertEquals(5, config.commitlog_segment_size_in_mb);
        assertEquals(null, config.max_mutation_size_in_kb); //not set explicitly in the default yaml, check the config; not set there too
        assertEquals(0, config.cdc_total_space_in_mb);
        assertEquals(1024, config.hinted_handoff_throttle_in_kb);
        assertEquals(1024, config.batchlog_replay_throttle_in_kb);
        assertEquals(10240, config.trickle_fsync_interval_in_kb);
        assertEquals(50, config.sstable_preemptive_open_interval_in_mb);
        assertEquals(null, config.counter_cache_size_in_mb);
        assertEquals(null, config.file_cache_size_in_mb);
        assertEquals(null, config.index_summary_capacity_in_mb);
        assertEquals(null, config.prepared_statements_cache_size_mb);
        assertEquals(null, config.key_cache_size_in_mb);
        assertEquals(16, config.row_cache_size_in_mb);

        //Confirm rate parameters were successfully parsed with the default values in cassandra.yaml
        assertEquals(0, config.compaction_throughput_mb_per_sec);
        assertEquals(200000000, config.stream_throughput_outbound_megabits_per_sec);
        assertEquals(200, config.inter_dc_stream_throughput_outbound_megabits_per_sec);
        Keyspace.setInitialized();

        // Now try custom loader to validate the proper conversion of units
        ConfigurationLoader testLoader = new TestLoader();
        System.setProperty("cassandra.config.loader", testLoader.getClass().getName());

        config = DatabaseDescriptor.loadConfig();
        String configUrl = System.getProperty("cassandra.config");
        URL url;
        url = new URL(configUrl);
        Config.parseUnits(config, url);

        assertEquals("6mb", config.commitlog_segment_size);
        assertEquals(6, config.commitlog_segment_size_in_mb);
        assertEquals(0, config.max_hint_window_in_ms);
        assertEquals("6m", config.streaming_keep_alive_period);
        assertEquals(360, config.streaming_keep_alive_period_in_secs, 0);
        assertEquals(1, config.memtable_offheap_space_in_mb, 0);
        assertEquals(4194304, config.internode_application_send_queue_capacity_in_bytes);
        assertEquals(1024, config.column_index_size_in_kb);
        assertEquals(1, config.compaction_throughput_mb_per_sec);
        assertEquals(1, config.stream_throughput_outbound_megabits_per_sec);
    }
    public static class TestLoader implements ConfigurationLoader
    {
        public Config loadConfig() throws ConfigurationException
        {
            Config testConfig = new Config();

            testConfig.commitlog_segment_size = "6mb";
            testConfig.max_hint_window = "1ns";
            testConfig.streaming_keep_alive_period = "6m";
            testConfig.memtable_offheap_space = "1024KB";
            testConfig.internode_application_send_queue_capacity = "4MB";
            testConfig.column_index_size = "1MB";
            testConfig.compaction_throughput = "1000Kbps";
            testConfig.stream_throughput_outbound = "1000000bps";

            return testConfig;
        }
    }
};




