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

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Supplier;

import com.google.common.base.Joiner;
import com.google.common.collect.Sets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.audit.AuditLogOptions;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.fql.FullQueryLoggerOptions;

/**
 * A class that contains configuration properties for the cassandra node it runs within.
 *
 * Properties declared as volatile can be mutated via JMX.
 */
public class Config
{
    private static final Logger logger = LoggerFactory.getLogger(Config.class);

    /*
     * Prefix for Java properties for internal Cassandra configuration options
     */
    public static final String PROPERTY_PREFIX = "cassandra.";

    public String cluster_name = "Test Cluster";
    public String authenticator;
    public String authorizer;
    public String role_manager;
    public String network_authorizer;

    @Replaces(oldName = "permissions_validity_in_ms", converter = Converter.MillisDurationConverter.class, deprecated = true)
    public volatile CassandraDuration permissions_validity = new CassandraDuration("2s");
    public volatile int permissions_cache_max_entries = 1000;
    @Replaces(oldName = "permissions_update_interval_in_ms", converter = Converter.MillisDurationConverterCustom.class, deprecated = true)
    public volatile CassandraDuration permissions_update_interval = new CassandraDuration("0ms");
    public volatile boolean permissions_cache_active_update = false;
    @Replaces(oldName = "roles_validity_in_ms", converter = Converter.MillisDurationConverter.class, deprecated = true)
    public volatile CassandraDuration roles_validity = new CassandraDuration("2s");
    public volatile int roles_cache_max_entries = 1000;
    @Replaces(oldName = "roles_update_interval_in_ms", converter = Converter.MillisDurationConverterCustom.class, deprecated = true)
    public volatile CassandraDuration roles_update_interval= new CassandraDuration("0ms");
    public volatile boolean roles_cache_active_update = false;
    @Replaces(oldName = "credentials_validity_in_ms", converter = Converter.MillisDurationConverter.class, deprecated = true)
    public volatile CassandraDuration credentials_validity = new CassandraDuration("2s");
    public volatile int credentials_cache_max_entries = 1000;
    @Replaces(oldName = "credentials_update_interval_in_ms", converter = Converter.MillisDurationConverterCustom.class, deprecated = true)
    public volatile CassandraDuration credentials_update_interval= new CassandraDuration("0ms");
    public volatile boolean credentials_cache_active_update = false;

    /* Hashing strategy Random or OPHF */
    public String partitioner;

    public boolean auto_bootstrap = true;
    public volatile boolean hinted_handoff_enabled = true;
    public Set<String> hinted_handoff_disabled_datacenters = Sets.newConcurrentHashSet();
    @Replaces(oldName = "max_hint_window_in_ms", converter = Converter.MillisDurationConverter.class, deprecated = true)
    public volatile CassandraDuration max_hint_window = new CassandraDuration("3h");
    public String hints_directory;
    public boolean hint_window_persistent_enabled = true;

    public ParameterizedClass seed_provider;
    public DiskAccessMode disk_access_mode = DiskAccessMode.auto;

    public DiskFailurePolicy disk_failure_policy = DiskFailurePolicy.ignore;
    public CommitFailurePolicy commit_failure_policy = CommitFailurePolicy.stop;

    public volatile boolean use_deterministic_table_id = false;

    /* initial token in the ring */
    public String initial_token;
    public Integer num_tokens;
    /** Triggers automatic allocation of tokens if set, using the replication strategy of the referenced keyspace */
    public String allocate_tokens_for_keyspace = null;
    /** Triggers automatic allocation of tokens if set, based on the provided replica count for a datacenter */
    public Integer allocate_tokens_for_local_replication_factor = null;

    @Replaces(oldName = "native_transport_idle_timeout_in_ms", converter = Converter.MillisDurationConverter.class, deprecated = true)
    public volatile CassandraDuration native_transport_idle_timeout = new CassandraDuration("0ms");

    @Replaces(oldName = "request_timeout_in_ms", converter = Converter.MillisDurationConverter.class, deprecated = true)
    public volatile CassandraDuration request_timeout = new CassandraDuration("10000ms");

    @Replaces(oldName = "read_request_timeout_in_ms", converter = Converter.MillisDurationConverter.class, deprecated = true)
    public volatile CassandraDuration read_request_timeout = new CassandraDuration("5000ms");

    @Replaces(oldName = "range_request_timeout_in_ms", converter = Converter.MillisDurationConverter.class, deprecated = true)
    public volatile CassandraDuration range_request_timeout = new CassandraDuration("10000ms");

    @Replaces(oldName = "write_request_timeout_in_ms", converter = Converter.MillisDurationConverter.class, deprecated = true)
    public volatile CassandraDuration write_request_timeout = new CassandraDuration("2000ms");

    @Replaces(oldName = "counter_write_request_timeout_in_ms", converter = Converter.MillisDurationConverter.class, deprecated = true)
    public volatile CassandraDuration counter_write_request_timeout = new CassandraDuration("5000ms");

    @Replaces(oldName = "cas_contention_timeout_in_ms", converter = Converter.MillisDurationConverter.class, deprecated = true)
    public volatile CassandraDuration cas_contention_timeout = new CassandraDuration("1000ms");

    @Replaces(oldName = "truncate_request_timeout_in_ms", converter = Converter.MillisDurationConverter.class, deprecated = true)
    public volatile CassandraDuration truncate_request_timeout = new CassandraDuration("60000ms");

    public Integer streaming_connections_per_host = 1;
    @Replaces(oldName = "streaming_keep_alive_period_in_secs", converter = Converter.SecondsDurationConverter.class, deprecated = true)
    public CassandraDuration streaming_keep_alive_period = new CassandraDuration("300s");

    @Replaces(oldName = "cross_node_timeout", deprecated = true)
    public boolean internode_timeout = true;

    @Replaces(oldName = "slow_query_log_timeout_in_ms", converter = Converter.MillisDurationConverter.class, deprecated = true)
    public volatile CassandraDuration slow_query_log_timeout = new CassandraDuration("500ms");

    public volatile double phi_convict_threshold = 8.0;

    public int concurrent_reads = 32;
    public int concurrent_writes = 32;
    public int concurrent_counter_writes = 32;
    public int concurrent_materialized_view_writes = 32;
    public int available_processors = -1;

    @Deprecated
    public Integer concurrent_replicates = null;

    public int memtable_flush_writers = 0;
    @Replaces(oldName = "memtable_heap_space_in_mb", converter = Converter.MegabytesDataStorageConverter.class, deprecated = true)
    public DataStorage memtable_heap_space;
    @Replaces(oldName = "memtable_offheap_space_in_mb", converter = Converter.MegabytesDataStorageConverter.class, deprecated = true)
    public DataStorage memtable_offheap_space;
    public Float memtable_cleanup_threshold = null;

    // Limit the maximum depth of repair session merkle trees
    @Deprecated
    public volatile Integer repair_session_max_tree_depth = null;
    @Replaces(oldName = "repair_session_space_in_mb", converter = Converter.MegabytesDataStorageConverter.class, deprecated = true)
    public volatile DataStorage repair_session_space = null;

    public volatile boolean use_offheap_merkle_trees = true;

    public int storage_port = 7000;
    public int ssl_storage_port = 7001;
    public String listen_address;
    public String listen_interface;
    public boolean listen_interface_prefer_ipv6 = false;
    public String broadcast_address;
    public boolean listen_on_broadcast_address = false;
    public String internode_authenticator;

    public boolean traverse_auth_from_root = false;

    /*
     * RPC address and interface refer to the address/interface used for the native protocol used to communicate with
     * clients. It's still called RPC in some places even though Thrift RPC is gone. If you see references to native
     * address or native port it's derived from the RPC address configuration.
     *
     * native_transport_port is the port that is paired with RPC address to bind on.
     */
    public String rpc_address;
    public String rpc_interface;
    public boolean rpc_interface_prefer_ipv6 = false;
    public String broadcast_rpc_address;
    public boolean rpc_keepalive = true;

    @Replaces(oldName = "internode_max_message_size_in_bytes", converter = Converter.BytesDataStorageConverter.class, deprecated=true)
    public DataStorage internode_max_message_size;

    @Replaces(oldName = "internode_send_buff_size_in_bytes", deprecated = true)
    public int internode_socket_send_buffer_size_in_bytes = 0;
    @Replaces(oldName = "internode_recv_buff_size_in_bytes", deprecated = true)
    public int internode_socket_receive_buffer_size_in_bytes = 0;

    // TODO: derive defaults from system memory settings?
    @Replaces(oldName = "internode_application_send_queue_capacity_in_bytes", converter = Converter.BytesDataStorageConverter.class, deprecated = true)
    public DataStorage internode_application_send_queue_capacity = new DataStorage("4MB");
    @Replaces(oldName = "internode_application_send_queue_reserve_endpoint_capacity_in_bytes", converter = Converter.BytesDataStorageConverter.class, deprecated = true)
    public DataStorage internode_application_send_queue_reserve_endpoint_capacity = new DataStorage("128MB");
    @Replaces(oldName = "internode_application_send_queue_reserve_global_capacity_in_bytes", converter = Converter.BytesDataStorageConverter.class, deprecated = true)
    public DataStorage internode_application_send_queue_reserve_global_capacity = new DataStorage("512MB");

    @Replaces(oldName = "internode_application_receive_queue_capacity_in_bytes", converter = Converter.BytesDataStorageConverter.class, deprecated = true)
    public DataStorage internode_application_receive_queue_capacity = new DataStorage("4MB");
    @Replaces(oldName = "internode_application_receive_queue_reserve_endpoint_capacity_in_bytes", converter = Converter.BytesDataStorageConverter.class, deprecated = true)
    public DataStorage internode_application_receive_queue_reserve_endpoint_capacity = new DataStorage("128MB");
    @Replaces(oldName = "internode_application_receive_queue_reserve_global_capacity_in_bytes", converter = Converter.BytesDataStorageConverter.class, deprecated = true)
    public DataStorage internode_application_receive_queue_reserve_global_capacity = new DataStorage("512MB");

    // Defensive settings for protecting Cassandra from true network partitions. See (CASSANDRA-14358) for details.
    // The amount of time to wait for internode tcp connections to establish.
    @Replaces(oldName = "internode_tcp_connect_timeout_in_ms", converter = Converter.MillisDurationConverter.class, deprecated = true)
    public volatile CassandraDuration internode_tcp_connect_timeout = new CassandraDuration("2s");
    // The amount of time unacknowledged data is allowed on a connection before we throw out the connection
    // Note this is only supported on Linux + epoll, and it appears to behave oddly above a setting of 30000
    // (it takes much longer than 30s) as of Linux 4.12. If you want something that high set this to 0
    // (which picks up the OS default) and configure the net.ipv4.tcp_retries2 sysctl to be ~8.
    @Replaces(oldName = "internode_tcp_connect_timeout_in_ms", converter = Converter.MillisDurationConverter.class, deprecated = true)
    public volatile CassandraDuration internode_tcp_user_timeout = new CassandraDuration("30s");
    // Similar to internode_tcp_user_timeout_in_ms but used specifically for streaming connection.
    // The default is 5 minutes. Increase it or set it to 0 in order to increase the timeout.
    @Replaces(oldName = "internode_streaming_tcp_user_timeout_in_ms", converter = Converter.MillisDurationConverter.class, deprecated = true)
    public volatile CassandraDuration internode_streaming_tcp_user_timeout = new CassandraDuration("300s"); // 5 minutes

    public boolean start_native_transport = true;
    public int native_transport_port = 9042;
    public Integer native_transport_port_ssl = null;
    public int native_transport_max_threads = 128;
    @Replaces(oldName = "native_transport_max_frame_size_in_mb", converter = Converter.MegabytesDataStorageConverter.class, deprecated = true)
    public DataStorage native_transport_max_frame_size = new DataStorage("16MB");
    public volatile long native_transport_max_concurrent_connections = -1L;
    public volatile long native_transport_max_concurrent_connections_per_ip = -1L;
    public boolean native_transport_flush_in_batches_legacy = false;
    public volatile boolean native_transport_allow_older_protocols = true;
    public volatile long native_transport_max_concurrent_requests_in_bytes_per_ip = -1L;
    public volatile long native_transport_max_concurrent_requests_in_bytes = -1L;
    public volatile boolean native_transport_rate_limiting_enabled = false;
    public volatile int native_transport_max_requests_per_second = 1000000;
    public int native_transport_receive_queue_capacity_in_bytes = 1 << 20; // 1MiB

    @Deprecated
    public Integer native_transport_max_negotiable_protocol_version = null;

    /**
     * Max size of values in SSTables, in MegaBytes.
     * Default is the same as the native protocol frame limit: 256Mb.
     * See AbstractType for how it is used.
     */
    @Replaces(oldName = "max_value_size_in_mb", converter = Converter.MegabytesDataStorageConverter.class, deprecated = true)
    public DataStorage max_value_size = new DataStorage("256MB");

    public boolean snapshot_before_compaction = false;
    public boolean auto_snapshot = true;
    public volatile long snapshot_links_per_second = 0;

    /* if the size of columns or super-columns are more than this, indexing will kick in */
    @Replaces(oldName = "column_index_size_in_kb", converter = Converter.KilobytesDataStorageConverter.class, deprecated = true)
    public volatile DataStorage column_index_size = new DataStorage("64KB");
    @Replaces(oldName = "column_index_cache_size_in_kb", converter = Converter.KilobytesDataStorageConverter.class, deprecated = true)
    public DataStorage column_index_cache_size = new DataStorage("2KB");
    @Replaces(oldName = "batch_size_warn_threshold_in_kb", converter = Converter.KilobytesDataStorageConverter.class, deprecated = true)
    public DataStorage batch_size_warn_threshold = new DataStorage("5KB");
    @Replaces(oldName = "batch_size_fail_threshold_in_kb", converter = Converter.KilobytesDataStorageConverter.class, deprecated = true)
    public volatile DataStorage batch_size_fail_threshold = new DataStorage("50KB");
    public Integer unlogged_batch_across_partitions_warn_threshold = 10;
    public volatile Integer concurrent_compactors;
    @Replaces(oldName = "compaction_throughput_mb_per_sec", converter = Converter.MegabitsPerSecondBitRateConverter.class, deprecated = true)
    public volatile BitRate compaction_throughput = new BitRate("16MiB/s");
    @Replaces(oldName = "compaction_large_partition_warning_threshold_mb", converter = Converter.MegabytesDataStorageConverter.class, deprecated = true)
    public volatile DataStorage compaction_large_partition_warning_threshold = new DataStorage("100MB");
    @Replaces(oldName = "min_free_space_per_drive_in_mb", converter = Converter.MegabytesDataStorageConverter.class, deprecated = true)
    public DataStorage min_free_space_per_drive = new DataStorage("50MB");
    public volatile Integer compaction_tombstone_warning_threshold = 100000;

    public volatile int concurrent_materialized_view_builders = 1;
    public volatile int reject_repair_compaction_threshold = Integer.MAX_VALUE;
    
    @Replaces(oldName = "stream_throughput_outbound_megabits_per_sec", converter = Converter.MegabitsPerSecondBitRateConverter.class, deprecated = true)
    public volatile BitRate stream_throughput_outbound = new BitRate("200MiB/s");
    @Replaces(oldName = "inter_dc_stream_throughput_outbound_megabits_per_sec", converter = Converter.MegabitsPerSecondBitRateConverter.class, deprecated = true)
    public volatile BitRate inter_dc_stream_throughput_outbound = new BitRate("200MiB/s");

    public volatile int entire_sstable_stream_throughput_outbound_megabits_per_sec = 200;
    public volatile int entire_sstable_inter_dc_stream_throughput_outbound_megabits_per_sec = 200;

    public String[] data_file_directories = new String[0];

    /**
     * The directory to use for storing the system keyspaces data.
     * If unspecified the data will be stored in the first of the data_file_directories.
     */
    public String local_system_data_file_directory;

    public String saved_caches_directory;

    // Commit Log
    public String commitlog_directory;
    @Replaces(oldName = "commitlog_total_space_in_mb", converter = Converter.MegabytesDataStorageConverter.class, deprecated = true)
    public DataStorage commitlog_total_space;
    public CommitLogSync commitlog_sync;

    /**
     * @deprecated since 4.0 This value was near useless, and we're not using it anymore
     */
    @Replaces(oldName = "commitlog_sync_group_window_in_ms", converter = Converter.MillisDurationInDoubleConverter.class, deprecated = true)
    public CassandraDuration commitlog_sync_batch_window = new CassandraDuration("0.0ms");
    @Replaces(oldName = "commitlog_sync_group_window_in_ms", converter = Converter.MillisDurationInDoubleConverter.class, deprecated = true)
    public CassandraDuration commitlog_sync_group_window = new CassandraDuration("0ms");
    @Replaces(oldName = "commitlog_sync_period_in_ms", converter = Converter.MillisDurationConverter.class, deprecated = true)
    public CassandraDuration commitlog_sync_period = new CassandraDuration("0ms");
    @Replaces(oldName = "commitlog_segment_size_in_mb", converter = Converter.MegabytesDataStorageConverter.class, deprecated = true)
    public DataStorage commitlog_segment_size = new DataStorage("32mb");

    public ParameterizedClass commitlog_compression;
    public FlushCompression flush_compression = FlushCompression.fast;
    public int commitlog_max_compression_buffers_in_pool = 3;
    @Replaces(oldName = "periodic_commitlog_sync_lag_block_in_ms", converter = Converter.MillisDurationConverter.class, deprecated = true)
    public CassandraDuration periodic_commitlog_sync_lag_block;
    public TransparentDataEncryptionOptions transparent_data_encryption_options = new TransparentDataEncryptionOptions();

    @Replaces(oldName = "max_mutation_size_in_kb", converter = Converter.KilobytesDataStorageConverter.class, deprecated = true)
    public DataStorage max_mutation_size;

    // Change-data-capture logs
    public boolean cdc_enabled = false;
    public String cdc_raw_directory;
    @Replaces(oldName = "cdc_total_space_in_mb", converter = Converter.MegabytesDataStorageConverter.class, deprecated = true)
    public DataStorage cdc_total_space = new DataStorage("0MB");
    @Replaces(oldName = "cdc_free_space_check_interval_ms", converter = Converter.MillisDurationConverter.class, deprecated = true)
    public CassandraDuration cdc_free_space_check_interval = new CassandraDuration("250ms");

    @Deprecated
    public int commitlog_periodic_queue_size = -1;

    public String endpoint_snitch;
    public boolean dynamic_snitch = true;
    @Replaces(oldName = "dynamic_snitch_update_interval_in_ms", converter = Converter.MillisDurationConverter.class, deprecated = true)
    public CassandraDuration dynamic_snitch_update_interval = new CassandraDuration("100ms");
    @Replaces(oldName = "dynamic_snitch_reset_interval_in_ms", converter = Converter.MillisDurationConverter.class, deprecated = true)
    public CassandraDuration dynamic_snitch_reset_interval = new CassandraDuration("10m");
    public double dynamic_snitch_badness_threshold = 1.0;
    
    public String failure_detector = "FailureDetector";

    public EncryptionOptions.ServerEncryptionOptions server_encryption_options = new EncryptionOptions.ServerEncryptionOptions();
    public EncryptionOptions client_encryption_options = new EncryptionOptions();

    public InternodeCompression internode_compression = InternodeCompression.none;

    @Replaces(oldName = "hinted_handoff_throttle_in_kb", converter = Converter.KilobytesDataStorageConverter.class, deprecated = true)
    public volatile DataStorage hinted_handoff_throttle = new DataStorage("1024KB");
    @Replaces(oldName = "batchlog_replay_throttle_in_kb", converter = Converter.KilobytesDataStorageConverter.class, deprecated = true)
    public volatile DataStorage batchlog_replay_throttle = new DataStorage("1024KB");
    public int max_hints_delivery_threads = 2;
    @Replaces(oldName = "hints_flush_period_in_ms", converter = Converter.MillisDurationConverter.class, deprecated = true)
    public CassandraDuration hints_flush_period = new CassandraDuration("10s");
    @Replaces(oldName = "max_hints_file_size_in_mb", converter = Converter.MegabytesDataStorageConverter.class, deprecated = true)
    public DataStorage max_hints_file_size = new DataStorage("128mb");
    public ParameterizedClass hints_compression;
    public volatile boolean auto_hints_cleanup_enabled = false;

    public volatile boolean incremental_backups = false;
    public boolean trickle_fsync = false;
    @Replaces(oldName = "trickle_fsync_interval_in_kb", converter = Converter.KilobytesDataStorageConverter.class, deprecated = true)
    public DataStorage trickle_fsync_interval = new DataStorage("10240KB");

    @Replaces(oldName = "sstable_preemptive_open_interval_in_mb", converter = Converter.MegabytesDataStorageConverter.class, deprecated = true)
    public volatile DataStorage sstable_preemptive_open_interval = new DataStorage("50MB");

    public volatile boolean key_cache_migrate_during_compaction = true;
    @Replaces(oldName = "key_cache_size_in_mb", converter = Converter.MegabytesDataStorageConverter.class, deprecated = true)
    public DataStorage key_cache_size = null;
    public volatile int key_cache_save_period = 14400;
    public volatile int key_cache_keys_to_save = Integer.MAX_VALUE;

    public String row_cache_class_name = "org.apache.cassandra.cache.OHCProvider";
    @Replaces(oldName = "row_cache_size_in_mb", converter = Converter.MegabytesDataStorageConverter.class, deprecated = true)
    public volatile DataStorage row_cache_size = new DataStorage("0MB");
    public volatile int row_cache_save_period = 0;
    public volatile int row_cache_keys_to_save = Integer.MAX_VALUE;
    
    @Replaces(oldName = "counter_cache_size_in_mb", converter = Converter.MegabytesDataStorageConverter.class, deprecated = true)
    public DataStorage counter_cache_size = null;
    public volatile int counter_cache_save_period = 7200;
    public volatile int counter_cache_keys_to_save = Integer.MAX_VALUE;

    private static boolean isClientMode = false;
    private static Supplier<Config> overrideLoadConfig = null;
    
    @Replaces(oldName = "networking_cache_size_in_mb", converter = Converter.MegabytesDataStorageConverter.class, deprecated = true)
    public DataStorage networking_cache_size;

    @Replaces(oldName = "file_cache_size_in_mb", converter = Converter.MegabytesDataStorageConverter.class, deprecated = true)
    public DataStorage file_cache_size;

    public boolean file_cache_enabled = Boolean.getBoolean("cassandra.file_cache_enabled");

    /**
     * Because of the current {@link org.apache.cassandra.utils.memory.BufferPool} slab sizes of 64 kb, we
     * store in the file cache buffers that divide 64 kb, so we need to round the buffer sizes to powers of two.
     * This boolean controls weather they are rounded up or down. Set it to true to round up to the
     * next power of two, set it to false to round down to the previous power of two. Note that buffer sizes are
     * already rounded to 4 kb and capped between 4 kb minimum and 64 kb maximum by the {@link DiskOptimizationStrategy}.
     * By default, this boolean is set to round down when {@link #disk_optimization_strategy} is {@code ssd},
     * and to round up when it is {@code spinning}.
     */
    public Boolean file_cache_round_up;

    @Deprecated
    public boolean buffer_pool_use_heap_if_exhausted;

    public DiskOptimizationStrategy disk_optimization_strategy = DiskOptimizationStrategy.ssd;

    public double disk_optimization_estimate_percentile = 0.95;

    public double disk_optimization_page_cross_chance = 0.1;

    public boolean inter_dc_tcp_nodelay = true;

    public MemtableAllocationType memtable_allocation_type = MemtableAllocationType.heap_buffers;

    public final TrackWarnings track_warnings = new TrackWarnings();

    public volatile int tombstone_warn_threshold = 1000;
    public volatile int tombstone_failure_threshold = 100000;

    public final ReplicaFilteringProtectionOptions replica_filtering_protection = new ReplicaFilteringProtectionOptions();

    @Replaces(oldName = "index_summary_capacity_in_mb", converter = Converter.MegabytesDataStorageConverter.class, deprecated = true)
    public DataStorage index_summary_capacity;
    @Replaces(oldName = "index_summary_resize_interval_in_minutes", converter = Converter.MinutesDurationConverter.class, deprecated = true)
    public volatile CassandraDuration index_summary_resize_interval = new CassandraDuration("60m");

    @Replaces(oldName = "gc_log_threshold_in_ms", converter = Converter.MillisDurationConverter.class, deprecated = true)
    public CassandraDuration gc_log_threshold = new CassandraDuration("200ms");
    @Replaces(oldName = "gc_warn_threshold_in_ms", converter = Converter.MillisDurationConverter.class, deprecated = true)
    public CassandraDuration gc_warn_threshold = new CassandraDuration("1s");

    // TTL for different types of trace events.
    @Replaces(oldName = "tracetype_query_ttl", converter = Converter.SecondsDurationConverter.class, deprecated=true)
    public CassandraDuration trace_type_query_ttl = new CassandraDuration("86400s");
    @Replaces(oldName = "tracetype_repair_ttl", converter = Converter.SecondsDurationConverter.class, deprecated=true)
    public CassandraDuration trace_type_repair_ttl = new CassandraDuration("604800s");

    /**
     * Maintain statistics on whether writes achieve the ideal consistency level
     * before expiring and becoming hints
     */
    public volatile ConsistencyLevel ideal_consistency_level = null;

    public int windows_timer_interval = 0;

    /**
     * Size of the CQL prepared statements cache in MB.
     * Defaults to 1/256th of the heap size or 10MB, whichever is greater.
     *
     */
    @Replaces(oldName = "prepared_statements_cache_size_mb", converter = Converter.MegabytesDataStorageConverter.class, deprecated = true)
    public DataStorage prepared_statements_cache_size = null;

    @Replaces(oldName = "enable_user_defined_functions", deprecated = true)
    public boolean user_defined_functions_enabled = false;

    @Replaces(oldName = "enable_scripted_user_defined_functions", deprecated = true)
    public boolean scripted_user_defined_functions_enabled = false;

    @Replaces(oldName = "enable_materialized_views", deprecated = true)
    public boolean materialized_views_enabled = false;

    @Replaces(oldName = "enable_transient_replication", deprecated = true)
    public boolean transient_replication_enabled = false;

    @Replaces(oldName = "enable_sasi_indexes", deprecated = true)
    public boolean sasi_indexes_enabled = false;

    @Replaces(oldName = "enable_drop_compact_storage", deprecated = true)
    public volatile boolean drop_compact_storage_enabled = false;

    /**
     * Optionally disable asynchronous UDF execution.
     * Disabling asynchronous UDF execution also implicitly disables the security-manager!
     * By default, async UDF execution is enabled to be able to detect UDFs that run too long / forever and be
     * able to fail fast - i.e. stop the Cassandra daemon, which is currently the only appropriate approach to
     * "tell" a user that there's something really wrong with the UDF.
     * When you disable async UDF execution, users MUST pay attention to read-timeouts since these may indicate
     * UDFs that run too long or forever - and this can destabilize the cluster.
     */
    // Below parameter is not presented in cassandra.yaml but to be on the safe side that no one was directly using it
    // I still added backward compatibility (CASSANDRA-15234)
    @Replaces(oldName = "enable_user_defined_functions_threads", deprecated = true)
    public boolean user_defined_functions_threads_enabled = true;
    /**
     * Time in milliseconds after a warning will be emitted to the log and to the client that a UDF runs too long.
     * (Only valid, if user_defined_functions_threads_enabled==true)
     */
    //No need of unit conversion as this parameter is not exposed in the yaml file
    public long user_defined_function_warn_timeout_in_ms = 500;
    /**
     * Time in milliseconds after a fatal UDF run-time situation is detected and action according to
     * user_function_timeout_policy will take place.
     * (Only valid, if user_defined_functions_threads_enabled==true)
     */
    //No need of unit conversion as this parameter is not exposed in the yaml file
    public long user_defined_function_fail_timeout_in_ms = 1500;
    /**
     * Defines what to do when a UDF ran longer than user_defined_function_fail_timeout_in_ms.
     * Possible options are:
     * - 'die' - i.e. it is able to emit a warning to the client before the Cassandra Daemon will shut down.
     * - 'die_immediate' - shut down C* daemon immediately (effectively prevent the chance that the client will receive a warning).
     * - 'ignore' - just log - the most dangerous option.
     * (Only valid, if user_defined_functions_threads_enabled==true)
     */
    public UserFunctionTimeoutPolicy user_function_timeout_policy = UserFunctionTimeoutPolicy.die;

    @Deprecated
    public volatile boolean back_pressure_enabled = false;
    @Deprecated
    public volatile ParameterizedClass back_pressure_strategy;

    public volatile int concurrent_validations;
    public RepairCommandPoolFullStrategy repair_command_pool_full_strategy = RepairCommandPoolFullStrategy.queue;
    public int repair_command_pool_size = concurrent_validations;

    /**
     * When a node first starts up it intially considers all other peers as DOWN and is disconnected from all of them.
     * To be useful as a coordinator (and not introduce latency penalties on restart) this node must have successfully
     * opened all three internode TCP connections (gossip, small, and large messages) before advertising to clients.
     * Due to this, by default, Casssandra will prime these internode TCP connections and wait for all but a single
     * node to be DOWN/disconnected in the local datacenter before offering itself as a coordinator, subject to a
     * timeout. See CASSANDRA-13993 and CASSANDRA-14297 for more details.
     *
     * We provide two tunables to control this behavior as some users may want to block until all datacenters are
     * available (global QUORUM/EACH_QUORUM), some users may not want to block at all (clients that already work
     * around the problem), and some users may want to prime the connections but not delay startup.
     *
     * block_for_peers_timeout_in_secs: controls how long this node will wait to connect to peers. To completely disable
     * any startup connectivity checks set this to -1. To trigger the internode connections but immediately continue
     * startup, set this to to 0. The default is 10 seconds.
     *
     * block_for_peers_in_remote_dcs: controls if this node will consider remote datacenters to wait for. The default
     * is to _not_ wait on remote datacenters.
     */
    //No need of unit conversion as this parameter is not exposed in the yaml file
    public int block_for_peers_timeout_in_secs = 10;
    public boolean block_for_peers_in_remote_dcs = false;

    public volatile boolean automatic_sstable_upgrade = false;
    public volatile int max_concurrent_automatic_sstable_upgrades = 1;
    public boolean stream_entire_sstables = true;

    public volatile AuditLogOptions audit_logging_options = new AuditLogOptions();
    public volatile FullQueryLoggerOptions full_query_logging_options = new FullQueryLoggerOptions();

    public CorruptedTombstoneStrategy corrupted_tombstone_strategy = CorruptedTombstoneStrategy.disabled;

    public volatile boolean diagnostic_events_enabled = false;

    // Default and minimum keyspace replication factors allow validation of newly created keyspaces
    // and good defaults if no replication factor is provided by the user
    public volatile int default_keyspace_rf = 1;
    public volatile int minimum_keyspace_rf = 0;

    /**
     * flags for enabling tracking repaired state of data during reads
     * separate flags for range & single partition reads as single partition reads are only tracked
     * when CL > 1 and a digest mismatch occurs. Currently, range queries don't use digests so if
     * enabled for range reads, all such reads will include repaired data tracking. As this adds
     * some overhead, operators may wish to disable it whilst still enabling it for partition reads
     */
    public volatile boolean repaired_data_tracking_for_range_reads_enabled = false;
    public volatile boolean repaired_data_tracking_for_partition_reads_enabled = false;
    /* If true, unconfirmed mismatches (those which cannot be considered conclusive proof of out of
     * sync repaired data due to the presence of pending repair sessions, or unrepaired partition
     * deletes) will increment a metric, distinct from confirmed mismatches. If false, unconfirmed
     * mismatches are simply ignored by the coordinator.
     * This is purely to allow operators to avoid potential signal:noise issues as these types of
     * mismatches are considerably less actionable than their confirmed counterparts. Setting this
     * to true only disables the incrementing of the counters when an unconfirmed mismatch is found
     * and has no other effect on the collection or processing of the repaired data.
     */
    public volatile boolean report_unconfirmed_repaired_data_mismatches = false;
    /*
     * If true, when a repaired data mismatch is detected at read time or during a preview repair,
     * a snapshot request will be issued to each particpating replica. These are limited at the replica level
     * so that only a single snapshot per-table per-day can be taken via this method.
     */
    public volatile boolean snapshot_on_repaired_data_mismatch = false;

    /**
     * number of seconds to set nowInSec into the future when performing validation previews against repaired data
     * this (attempts) to prevent a race where validations on different machines are started on different sides of
     * a tombstone being compacted away
     */
    public volatile int validation_preview_purge_head_start_in_sec = 60 * 60;

    // Using String instead of ConsistencyLevel here to keep static initialization from cascading and starting
    // threads during tool usage mode. See CASSANDRA-12988 and DatabaseDescriptorRefTest for details
    public volatile String auth_read_consistency_level = "LOCAL_QUORUM";
    public volatile String auth_write_consistency_level = "EACH_QUORUM";

    /** This feature allows denying access to operations on certain key partitions, intended for use by operators to
     * provide another tool to manage cluster health vs application access. See CASSANDRA-12106 and CEP-13 for more details.
     */
    public volatile boolean partition_denylist_enabled = false;

    public volatile boolean denylist_writes_enabled = true;

    public volatile boolean denylist_reads_enabled = true;

    public volatile boolean denylist_range_reads_enabled = true;

    public CassandraDuration denylist_refresh = new CassandraDuration("600S");

    public CassandraDuration denylist_initial_load_retry = new CassandraDuration("5S");

    /** We cap the number of denylisted keys allowed per table to keep things from growing unbounded. Operators will
     * receive warnings and only denylist_max_keys_per_table in natural query ordering will be processed on overflow.
     */
    public volatile int denylist_max_keys_per_table = 1000;

    /** We cap the total number of denylisted keys allowed in the cluster to keep things from growing unbounded.
     * Operators will receive warnings on initial cache load that there are too many keys and be directed to trim
     * down the entries to within the configured limits.
     */
    public volatile int denylist_max_keys_total = 10000;

    /** Since the denylist in many ways serves to protect the health of the cluster from partitions operators have identified
     * as being in a bad state, we usually want more robustness than just CL.ONE on operations to/from these tables to
     * ensure that these safeguards are in place. That said, we allow users to configure this if they're so inclined.
     */
    public ConsistencyLevel denylist_consistency_level = ConsistencyLevel.QUORUM;

    /**
     * The intial capacity for creating RangeTombstoneList.
     */
    public volatile int initial_range_tombstone_list_allocation_size = 1;
    /**
     * The growth factor to enlarge a RangeTombstoneList.
     */
    public volatile double range_tombstone_list_growth_factor = 1.5;

    /**
     * @deprecated migrate to {@link DatabaseDescriptor#isClientInitialized()}
     */
    @Deprecated
    public static boolean isClientMode()
    {
        return isClientMode;
    }

    /**
     * If true, when rows with duplicate clustering keys are detected during a read or compaction
     * a snapshot will be taken. In the read case, each a snapshot request will be issued to each
     * replica involved in the query, for compaction the snapshot will be created locally.
     * These are limited at the replica level so that only a single snapshot per-day can be taken
     * via this method.
     *
     * This requires check_for_duplicate_rows_during_reads and/or check_for_duplicate_rows_during_compaction
     * below to be enabled
     */
    public volatile boolean snapshot_on_duplicate_row_detection = false;
    /**
     * If these are enabled duplicate keys will get logged, and if snapshot_on_duplicate_row_detection
     * is enabled, the table will get snapshotted for offline investigation
     */
    public volatile boolean check_for_duplicate_rows_during_reads = true;
    public volatile boolean check_for_duplicate_rows_during_compaction = true;

    public boolean autocompaction_on_startup_enabled = Boolean.parseBoolean(System.getProperty("cassandra.autocompaction_on_startup_enabled", "true"));

    // see CASSANDRA-3200 / CASSANDRA-16274
    public volatile boolean auto_optimise_inc_repair_streams = false;
    public volatile boolean auto_optimise_full_repair_streams = false;
    public volatile boolean auto_optimise_preview_repair_streams = false;

    /**
     * Client mode means that the process is a pure client, that uses C* code base but does
     * not read or write local C* database files.
     *
     * @deprecated migrate to {@link DatabaseDescriptor#clientInitialization(boolean)}
     */
    @Deprecated
    public static void setClientMode(boolean clientMode)
    {
        isClientMode = clientMode;
    }

    public volatile int table_count_warn_threshold = 150;
    public volatile int keyspace_count_warn_threshold = 40;

    public volatile int consecutive_message_errors_threshold = 1;

    public volatile SubnetGroups client_error_reporting_exclusions = new SubnetGroups();
    public volatile SubnetGroups internode_error_reporting_exclusions = new SubnetGroups();

    public static Supplier<Config> getOverrideLoadConfig()
    {
        return overrideLoadConfig;
    }

    public static void setOverrideLoadConfig(Supplier<Config> loadConfig)
    {
        overrideLoadConfig = loadConfig;
    }

    public enum CommitLogSync
    {
        periodic,
        batch,
        group
    }

    public enum FlushCompression
    {
        none,
        fast,
        table
    }

    public enum InternodeCompression
    {
        all, none, dc
    }

    public enum DiskAccessMode
    {
        auto,
        mmap,
        mmap_index_only,
        standard,
    }

    public enum MemtableAllocationType
    {
        unslabbed_heap_buffers,
        unslabbed_heap_buffers_logged,
        heap_buffers,
        offheap_buffers,
        offheap_objects
    }

    public enum DiskFailurePolicy
    {
        best_effort,
        stop,
        ignore,
        stop_paranoid,
        die
    }

    public enum CommitFailurePolicy
    {
        stop,
        stop_commit,
        ignore,
        die,
    }

    public enum UserFunctionTimeoutPolicy
    {
        ignore,
        die,
        die_immediate
    }

    public enum DiskOptimizationStrategy
    {
        ssd,
        spinning
    }

    public enum RepairCommandPoolFullStrategy
    {
        queue,
        reject
    }

    public enum CorruptedTombstoneStrategy
    {
        disabled,
        warn,
        exception
    }

    private static final List<String> SENSITIVE_KEYS = new ArrayList<String>() {{
        add("client_encryption_options");
        add("server_encryption_options");
    }};

    public static void log(Config config)
    {
        Map<String, String> configMap = new TreeMap<>();
        for (Field field : Config.class.getFields())
        {
            // ignore the constants
            if (Modifier.isFinal(field.getModifiers()))
                continue;

            String name = field.getName();
            if (SENSITIVE_KEYS.contains(name))
            {
                configMap.put(name, "<REDACTED>");
                continue;
            }

            String value;
            try
            {
                // Field.get() can throw NPE if the value of the field is null
                value = field.get(config).toString();
            }
            catch (NullPointerException | IllegalAccessException npe)
            {
                value = "null";
            }
            configMap.put(name, value);
        }

        logger.info("Node configuration:[{}]", Joiner.on("; ").join(configMap.entrySet()));
    }
}
