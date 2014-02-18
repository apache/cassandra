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

import com.google.common.collect.Sets;
import org.supercsv.io.CsvListReader;
import org.supercsv.prefs.CsvPreference;

import org.apache.cassandra.config.EncryptionOptions.ClientEncryptionOptions;
import org.apache.cassandra.config.EncryptionOptions.ServerEncryptionOptions;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.util.NativeAllocator;
import org.apache.cassandra.utils.FBUtilities;

/**
 * A class that contains configuration properties for the cassandra node it runs within.
 * 
 * Properties declared as volatile can be mutated via JMX.
 */
public class Config
{
    public String cluster_name = "Test Cluster";
    public String authenticator;
    public String authorizer;
    public int permissions_validity_in_ms = 2000;

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

    // we don't want a lot of contention, but we also don't want to starve all other tables
    // if a big one flushes. OS buffering should be able to minimize contention with 2 threads.
    public int memtable_flush_writers = 2;

    public Integer memtable_total_space_in_mb;
    public float memtable_cleanup_threshold = 0.4f;

    public Integer storage_port = 7000;
    public Integer ssl_storage_port = 7001;
    public String listen_address;
    public String broadcast_address;
    public String internode_authenticator;

    public Boolean start_rpc = true;
    public String rpc_address;
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

    @Deprecated
    public Integer thrift_max_message_length_in_mb = 16;

    public Integer thrift_framed_transport_size_in_mb = 15;
    public Boolean snapshot_before_compaction = false;
    public Boolean auto_snapshot = true;

    /* if the size of columns or super-columns are more than this, indexing will kick in */
    public Integer column_index_size_in_kb = 64;
    public Integer in_memory_compaction_limit_in_mb = 64;
    public Integer concurrent_compactors = FBUtilities.getAvailableProcessors();
    public volatile Integer compaction_throughput_mb_per_sec = 16;

    public Integer max_streaming_retries = 3;

    public volatile Integer stream_throughput_outbound_megabits_per_sec = 200;
    public volatile Integer inter_dc_stream_throughput_outbound_megabits_per_sec = 0;

    public String[] data_file_directories;
    public String flush_directory;

    public String saved_caches_directory;

    // Commit Log
    public String commitlog_directory;
    public Integer commitlog_total_space_in_mb;
    public CommitLogSync commitlog_sync;
    public Double commitlog_sync_batch_window_in_ms;
    public Integer commitlog_sync_period_in_ms;
    public int commitlog_segment_size_in_mb = 32;
    public int commitlog_periodic_queue_size = 1024 * FBUtilities.getAvailableProcessors();

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
    public boolean compaction_preheat_key_cache = true;

    public volatile boolean incremental_backups = false;
    public boolean trickle_fsync = false;
    public int trickle_fsync_interval_in_kb = 10240;

    public Long key_cache_size_in_mb = null;
    public volatile int key_cache_save_period = 14400;
    public volatile int key_cache_keys_to_save = Integer.MAX_VALUE;

    public long row_cache_size_in_mb = 0;
    public volatile int row_cache_save_period = 0;
    public volatile int row_cache_keys_to_save = Integer.MAX_VALUE;

    public Long counter_cache_size_in_mb = null;
    public volatile int counter_cache_save_period = 7200;
    public volatile int counter_cache_keys_to_save = Integer.MAX_VALUE;

    public String memory_allocator = NativeAllocator.class.getSimpleName();
    public boolean populate_io_cache_on_flush = false;

    private static boolean isClientMode = false;

    public boolean preheat_kernel_page_cache = false;

    public Integer file_cache_size_in_mb;

    public boolean inter_dc_tcp_nodelay = true;

    public String memtable_allocator = "HeapSlabPool";

    private static boolean outboundBindAny = false;

    public volatile int tombstone_warn_threshold = 1000;
    public volatile int tombstone_failure_threshold = 100000;

    public volatile Long index_summary_capacity_in_mb;
    public volatile int index_summary_resize_interval_in_minutes = 60;

    private static final CsvPreference STANDARD_SURROUNDING_SPACES_NEED_QUOTES = new CsvPreference.Builder(CsvPreference.STANDARD_PREFERENCE)
                                                                                                  .surroundingSpacesNeedQuotes(true).build();

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
            if (hinted_handoff_enabled.toLowerCase().equalsIgnoreCase("true"))
            {
                hinted_handoff_enabled_global = true;
            }
            else if (hinted_handoff_enabled.toLowerCase().equalsIgnoreCase("false"))
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
        final CsvListReader csvListReader = new CsvListReader(new StringReader(dcNames), STANDARD_SURROUNDING_SPACES_NEED_QUOTES);
        return csvListReader.read();
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

    public static enum DiskFailurePolicy
    {
        best_effort,
        stop,
        ignore,
    }

    public static enum RequestSchedulerId
    {
        keyspace
    }
}
