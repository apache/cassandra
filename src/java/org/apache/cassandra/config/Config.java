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

import java.io.IOException;
import java.io.StringReader;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Sets;

import org.apache.cassandra.config.EncryptionOptions.ClientEncryptionOptions;
import org.apache.cassandra.config.EncryptionOptions.ServerEncryptionOptions;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.supercsv.io.CsvListReader;
import org.supercsv.prefs.CsvPreference;

/**
 * A class that contains configuration properties for the cassandra node it runs within.
 *
 * Properties declared as volatile can be mutated via JMX.
 */
public class Config
{
    /*
     * Prefix for Java properties for internal Cassandra configuration options
     */
    public static final String PROPERTY_PREFIX = "cassandra.";


    public String cluster_name = "Test Cluster";
    public String authenticator;
    public String authorizer;
    public String role_manager;
    public volatile int permissions_validity_in_ms = 2000;
    public int permissions_cache_max_entries = 1000;
    public volatile int permissions_update_interval_in_ms = -1;
    public int roles_validity_in_ms = 2000;
    public int roles_cache_max_entries = 1000;
    public int roles_update_interval_in_ms = -1;

    /* Hashing strategy Random or OPHF */
    public String partitioner;

    public Boolean auto_bootstrap = true;
    public volatile boolean hinted_handoff_enabled_global = true;
    public String hinted_handoff_enabled;
    public Set<String> hinted_handoff_enabled_by_dc = Sets.newConcurrentHashSet();
    public volatile Integer max_hint_window_in_ms = 3600 * 1000; // one hour

    public SeedProviderDef seed_provider;
    public DiskAccessMode disk_access_mode = DiskAccessMode.auto;

    public DiskFailurePolicy disk_failure_policy = DiskFailurePolicy.ignore;
    public CommitFailurePolicy commit_failure_policy = CommitFailurePolicy.stop;

    /* initial token in the ring */
    public String initial_token;
    public Integer num_tokens = 1;

    public volatile Long request_timeout_in_ms = 10000L;

    public volatile Long read_request_timeout_in_ms = 5000L;

    public volatile Long range_request_timeout_in_ms = 10000L;

    public volatile Long write_request_timeout_in_ms = 2000L;

    public volatile Long counter_write_request_timeout_in_ms = 5000L;

    public volatile Long cas_contention_timeout_in_ms = 1000L;

    public volatile Long truncate_request_timeout_in_ms = 60000L;

    public Integer streaming_socket_timeout_in_ms = 0;

    public boolean cross_node_timeout = false;

    public volatile Double phi_convict_threshold = 8.0;

    public Integer concurrent_reads = 32;
    public Integer concurrent_writes = 32;
    public Integer concurrent_counter_writes = 32;

    @Deprecated
    public Integer concurrent_replicates = null;

    public Integer memtable_flush_writers = null;
    public Integer memtable_heap_space_in_mb;
    public Integer memtable_offheap_space_in_mb;
    public Float memtable_cleanup_threshold = null;

    public Integer storage_port = 7000;
    public Integer ssl_storage_port = 7001;
    public String listen_address;
    public String listen_interface;
    public Boolean listen_interface_prefer_ipv6 = false;
    public String broadcast_address;
    public String internode_authenticator;

    public Boolean start_rpc = true;
    public String rpc_address;
    public String rpc_interface;
    public Boolean rpc_interface_prefer_ipv6 = false;
    public String broadcast_rpc_address;
    public Integer rpc_port = 9160;
    public Integer rpc_listen_backlog = 50;
    public String rpc_server_type = "sync";
    public Boolean rpc_keepalive = true;
    public Integer rpc_min_threads = 16;
    public Integer rpc_max_threads = Integer.MAX_VALUE;
    public Integer rpc_send_buff_size_in_bytes;
    public Integer rpc_recv_buff_size_in_bytes;
    public Integer internode_send_buff_size_in_bytes;
    public Integer internode_recv_buff_size_in_bytes;

    public Boolean start_native_transport = false;
    public Integer native_transport_port = 9042;
    public Integer native_transport_max_threads = 128;
    public Integer native_transport_max_frame_size_in_mb = 256;
    public volatile Long native_transport_max_concurrent_connections = -1L;
    public volatile Long native_transport_max_concurrent_connections_per_ip = -1L;

    @Deprecated
    public Integer thrift_max_message_length_in_mb = 16;

    public Integer thrift_framed_transport_size_in_mb = 15;
    public Boolean snapshot_before_compaction = false;
    public Boolean auto_snapshot = true;

    /* if the size of columns or super-columns are more than this, indexing will kick in */
    public Integer column_index_size_in_kb = 64;
    public Integer batch_size_warn_threshold_in_kb = 5;
    public volatile Integer batch_size_fail_threshold_in_kb = 50;
    public Integer concurrent_compactors;
    public volatile Integer compaction_throughput_mb_per_sec = 16;

    public Integer max_streaming_retries = 3;

    public volatile Integer stream_throughput_outbound_megabits_per_sec = 200;
    public volatile Integer inter_dc_stream_throughput_outbound_megabits_per_sec = 0;

    public String[] data_file_directories;

    public String saved_caches_directory;

    // Commit Log
    public String commitlog_directory;
    public Integer commitlog_total_space_in_mb;
    public CommitLogSync commitlog_sync;
    public Double commitlog_sync_batch_window_in_ms;
    public Integer commitlog_sync_period_in_ms;
    public int commitlog_segment_size_in_mb = 32;

    @Deprecated
    public int commitlog_periodic_queue_size = -1;

    public String endpoint_snitch;
    public Boolean dynamic_snitch = true;
    public Integer dynamic_snitch_update_interval_in_ms = 100;
    public Integer dynamic_snitch_reset_interval_in_ms = 600000;
    public Double dynamic_snitch_badness_threshold = 0.1;

    public String request_scheduler;
    public RequestSchedulerId request_scheduler_id;
    public RequestSchedulerOptions request_scheduler_options;

    public ServerEncryptionOptions server_encryption_options = new ServerEncryptionOptions();
    public ClientEncryptionOptions client_encryption_options = new ClientEncryptionOptions();
    // this encOptions is for backward compatibility (a warning is logged by DatabaseDescriptor)
    public ServerEncryptionOptions encryption_options;

    public InternodeCompression internode_compression = InternodeCompression.none;

    @Deprecated
    public Integer index_interval = null;

    public int hinted_handoff_throttle_in_kb = 1024;
    public int batchlog_replay_throttle_in_kb = 1024;
    public int max_hints_delivery_threads = 1;
    public int sstable_preemptive_open_interval_in_mb = 50;

    public volatile boolean incremental_backups = false;
    public boolean trickle_fsync = false;
    public int trickle_fsync_interval_in_kb = 10240;

    public Long key_cache_size_in_mb = null;
    public volatile int key_cache_save_period = 14400;
    public volatile int key_cache_keys_to_save = Integer.MAX_VALUE;

    public String row_cache_class_name = "org.apache.cassandra.cache.OHCProvider";
    public long row_cache_size_in_mb = 0;
    public volatile int row_cache_save_period = 0;
    public volatile int row_cache_keys_to_save = Integer.MAX_VALUE;

    public Long counter_cache_size_in_mb = null;
    public volatile int counter_cache_save_period = 7200;
    public volatile int counter_cache_keys_to_save = Integer.MAX_VALUE;

    @Deprecated
    public String memory_allocator;

    private static boolean isClientMode = false;

    public Integer file_cache_size_in_mb;

    public boolean inter_dc_tcp_nodelay = true;

    public MemtableAllocationType memtable_allocation_type = MemtableAllocationType.heap_buffers;

    private static boolean outboundBindAny = false;

    public volatile int tombstone_warn_threshold = 1000;
    public volatile int tombstone_failure_threshold = 100000;

    public volatile Long index_summary_capacity_in_mb;
    public volatile int index_summary_resize_interval_in_minutes = 60;

    private static final CsvPreference STANDARD_SURROUNDING_SPACES_NEED_QUOTES = new CsvPreference.Builder(CsvPreference.STANDARD_PREFERENCE)
                                                                                                  .surroundingSpacesNeedQuotes(true).build();

    // TTL for different types of trace events.
    public int tracetype_query_ttl = (int) TimeUnit.DAYS.toSeconds(1);
    public int tracetype_repair_ttl = (int) TimeUnit.DAYS.toSeconds(7);

    /*
     * Strategy to use for coalescing messages in OutboundTcpConnection.
     * Can be fixed, movingaverage, timehorizon, disabled. Setting is case and leading/trailing
     * whitespace insensitive. You can also specify a subclass of CoalescingStrategies.CoalescingStrategy by name.
     */
    public String otc_coalescing_strategy = "TIMEHORIZON";

    /*
     * How many microseconds to wait for coalescing. For fixed strategy this is the amount of time after the first
     * messgae is received before it will be sent with any accompanying messages. For moving average this is the
     * maximum amount of time that will be waited as well as the interval at which messages must arrive on average
     * for coalescing to be enabled.
     */
    public static final int otc_coalescing_window_us_default = 200;
    public int otc_coalescing_window_us = otc_coalescing_window_us_default;

    public static boolean getOutboundBindAny()
    {
        return outboundBindAny;
    }

    public static void setOutboundBindAny(boolean value)
    {
        outboundBindAny = value;
    }

    public static boolean isClientMode()
    {
        return isClientMode;
    }

    public static void setClientMode(boolean clientMode)
    {
        isClientMode = clientMode;
    }

    public void configHintedHandoff() throws ConfigurationException
    {
        if (hinted_handoff_enabled != null && !hinted_handoff_enabled.isEmpty())
        {
            if (hinted_handoff_enabled.equalsIgnoreCase("true"))
            {
                hinted_handoff_enabled_global = true;
            }
            else if (hinted_handoff_enabled.equalsIgnoreCase("false"))
            {
                hinted_handoff_enabled_global = false;
            }
            else
            {
                try
                {
                    hinted_handoff_enabled_by_dc.addAll(parseHintedHandoffEnabledDCs(hinted_handoff_enabled));
                }
                catch (IOException e)
                {
                    throw new ConfigurationException("Invalid hinted_handoff_enabled parameter " + hinted_handoff_enabled, e);
                }
            }
        }
    }

    public static List<String> parseHintedHandoffEnabledDCs(final String dcNames) throws IOException
    {
        try (final CsvListReader csvListReader = new CsvListReader(new StringReader(dcNames), STANDARD_SURROUNDING_SPACES_NEED_QUOTES))
        {
        	return csvListReader.read();
        }
    }

    public static enum CommitLogSync
    {
        periodic,
        batch
    }
    public static enum InternodeCompression
    {
        all, none, dc
    }

    public static enum DiskAccessMode
    {
        auto,
        mmap,
        mmap_index_only,
        standard,
    }

    public static enum MemtableAllocationType
    {
        unslabbed_heap_buffers,
        heap_buffers,
        offheap_buffers,
        offheap_objects
    }

    public static enum DiskFailurePolicy
    {
        best_effort,
        stop,
        ignore,
        stop_paranoid,
        die
    }

    public static enum CommitFailurePolicy
    {
        stop,
        stop_commit,
        ignore,
        die,
    }

    public static enum RequestSchedulerId
    {
        keyspace
    }
}
