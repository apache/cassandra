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
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import com.google.common.base.Joiner;
import com.google.common.collect.Sets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.audit.AuditLogOptions;
import org.apache.cassandra.fql.FullQueryLoggerOptions;
import org.apache.cassandra.db.ConsistencyLevel;

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
    public volatile int permissions_validity_in_ms = 2000;
    public volatile int permissions_cache_max_entries = 1000;
    public volatile int permissions_update_interval_in_ms = -1;
    public volatile int roles_validity_in_ms = 2000;
    public volatile int roles_cache_max_entries = 1000;
    public volatile int roles_update_interval_in_ms = -1;
    public volatile int credentials_validity_in_ms = 2000;
    public volatile int credentials_cache_max_entries = 1000;
    public volatile int credentials_update_interval_in_ms = -1;

    /* Hashing strategy Random or OPHF */
    public String partitioner;

    public boolean auto_bootstrap = true;
    public volatile boolean hinted_handoff_enabled = true;
    public Set<String> hinted_handoff_disabled_datacenters = Sets.newConcurrentHashSet();
    public volatile int max_hint_window_in_ms = 3 * 3600 * 1000; // three hours
    public String hints_directory;

    public volatile boolean force_new_prepared_statement_behaviour = false;

    public ParameterizedClass seed_provider;
    public DiskAccessMode disk_access_mode = DiskAccessMode.auto;

    public DiskFailurePolicy disk_failure_policy = DiskFailurePolicy.ignore;
    public CommitFailurePolicy commit_failure_policy = CommitFailurePolicy.stop;

    /* initial token in the ring */
    public String initial_token;
    public Integer num_tokens;
    /** Triggers automatic allocation of tokens if set, using the replication strategy of the referenced keyspace */
    public String allocate_tokens_for_keyspace = null;
    /** Triggers automatic allocation of tokens if set, based on the provided replica count for a datacenter */
    public Integer allocate_tokens_for_local_replication_factor = null;

    public long native_transport_idle_timeout_in_ms = 0L;

    public volatile long request_timeout_in_ms = 10000L;

    public volatile long read_request_timeout_in_ms = 5000L;

    public volatile long range_request_timeout_in_ms = 10000L;

    public volatile long write_request_timeout_in_ms = 2000L;

    public volatile long counter_write_request_timeout_in_ms = 5000L;

    public volatile long cas_contention_timeout_in_ms = 1000L;

    public volatile long truncate_request_timeout_in_ms = 60000L;

    public Integer streaming_connections_per_host = 1;
    public Integer streaming_keep_alive_period_in_secs = 300; //5 minutes

    public boolean cross_node_timeout = true;

    public volatile long slow_query_log_timeout_in_ms = 500L;

    public volatile double phi_convict_threshold = 8.0;

    public int concurrent_reads = 32;
    public int concurrent_writes = 32;
    public int concurrent_counter_writes = 32;
    public int concurrent_materialized_view_writes = 32;

    @Deprecated
    public Integer concurrent_replicates = null;

    public int memtable_flush_writers = 0;
    public Integer memtable_heap_space_in_mb;
    public Integer memtable_offheap_space_in_mb;
    public Float memtable_cleanup_threshold = null;
    public Map<String, String> memtable = null;

    // Limit the maximum depth of repair session merkle trees
    @Deprecated
    public volatile Integer repair_session_max_tree_depth = null;
    public volatile Integer repair_session_space_in_mb = null;

    public volatile long repair_request_timeout_in_ms = TimeUnit.MILLISECONDS.convert(1, TimeUnit.MINUTES);

    public volatile boolean use_offheap_merkle_trees = true;

    public int storage_port = 7000;
    public int ssl_storage_port = 7001;
    public String listen_address;
    public String listen_interface;
    public boolean listen_interface_prefer_ipv6 = false;
    public String broadcast_address;
    public boolean listen_on_broadcast_address = false;
    public String internode_authenticator;

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

    public Integer internode_max_message_size_in_bytes;

    @Replaces(oldName = "internode_send_buff_size_in_bytes", deprecated = true)
    public int internode_socket_send_buffer_size_in_bytes = 0;
    @Replaces(oldName = "internode_recv_buff_size_in_bytes", deprecated = true)
    public int internode_socket_receive_buffer_size_in_bytes = 0;

    // TODO: derive defaults from system memory settings?
    public int internode_application_send_queue_capacity_in_bytes = 1 << 22; // 4MiB
    public int internode_application_send_queue_reserve_endpoint_capacity_in_bytes = 1 << 27; // 128MiB
    public int internode_application_send_queue_reserve_global_capacity_in_bytes = 1 << 29; // 512MiB

    public int internode_application_receive_queue_capacity_in_bytes = 1 << 22; // 4MiB
    public int internode_application_receive_queue_reserve_endpoint_capacity_in_bytes = 1 << 27; // 128MiB
    public int internode_application_receive_queue_reserve_global_capacity_in_bytes = 1 << 29; // 512MiB

    // Defensive settings for protecting Cassandra from true network partitions. See (CASSANDRA-14358) for details.
    // The amount of time to wait for internode tcp connections to establish.
    public volatile int internode_tcp_connect_timeout_in_ms = 2000;
    // The amount of time unacknowledged data is allowed on a connection before we throw out the connection
    // Note this is only supported on Linux + epoll, and it appears to behave oddly above a setting of 30000
    // (it takes much longer than 30s) as of Linux 4.12. If you want something that high set this to 0
    // (which picks up the OS default) and configure the net.ipv4.tcp_retries2 sysctl to be ~8.
    public volatile int internode_tcp_user_timeout_in_ms = 30000;
    // Similar to internode_tcp_user_timeout_in_ms but used specifically for streaming connection.
    // The default is 5 minutes. Increase it or set it to 0 in order to increase the timeout.
    public volatile int internode_streaming_tcp_user_timeout_in_ms = 300_000; // 5 minutes

    public boolean start_native_transport = true;
    public int native_transport_port = 9042;
    public Integer native_transport_port_ssl = null;
    public int native_transport_max_threads = 128;
    public int native_transport_max_frame_size_in_mb = 256;
    public volatile long native_transport_max_concurrent_connections = -1L;
    public volatile long native_transport_max_concurrent_connections_per_ip = -1L;
    public boolean native_transport_flush_in_batches_legacy = false;
    public volatile boolean native_transport_allow_older_protocols = true;
    public volatile long native_transport_max_concurrent_requests_in_bytes_per_ip = -1L;
    public volatile long native_transport_max_concurrent_requests_in_bytes = -1L;
    public int native_transport_receive_queue_capacity_in_bytes = 1 << 20; // 1MiB

    @Deprecated
    public Integer native_transport_max_negotiable_protocol_version = null;

    /**
     * Max size of values in SSTables, in MegaBytes.
     * Default is the same as the native protocol frame limit: 256Mb.
     * See AbstractType for how it is used.
     */
    public int max_value_size_in_mb = 256;

    public boolean snapshot_before_compaction = false;
    public boolean auto_snapshot = true;
    public volatile long snapshot_links_per_second = 0;

    /* if the size of columns or super-columns are more than this, indexing will kick in */
    public int column_index_size_in_kb = 64;
    public volatile int column_index_cache_size_in_kb = 2;
    public volatile int batch_size_warn_threshold_in_kb = 5;
    public volatile int batch_size_fail_threshold_in_kb = 50;
    public Integer unlogged_batch_across_partitions_warn_threshold = 10;
    public volatile Integer concurrent_compactors;
    public volatile int compaction_throughput_mb_per_sec = 64;
    public volatile int compaction_large_partition_warning_threshold_mb = 100;
    public int min_free_space_per_drive_in_mb = 50;

    public volatile int concurrent_materialized_view_builders = 1;
    public volatile int reject_repair_compaction_threshold = Integer.MAX_VALUE;

    /**
     * @deprecated retry support removed on CASSANDRA-10992
     */
    @Deprecated
    public int max_streaming_retries = 3;

    public volatile int stream_throughput_outbound_megabits_per_sec = 200;
    public volatile int inter_dc_stream_throughput_outbound_megabits_per_sec = 200;

    public String[] data_file_directories = new String[0];

    /**
     * The directory to use for storing the system keyspaces data.
     * If unspecified the data will be stored in the first of the data_file_directories.
     */
    public String local_system_data_file_directory;

    public String saved_caches_directory;

    // Commit Log
    public String commitlog_directory;
    public Integer commitlog_total_space_in_mb;
    public CommitLogSync commitlog_sync;

    /**
     * @deprecated since 4.0 This value was near useless, and we're not using it anymore
     */
    public double commitlog_sync_batch_window_in_ms = Double.NaN;
    public double commitlog_sync_group_window_in_ms = Double.NaN;
    public int commitlog_sync_period_in_ms;
    public int commitlog_segment_size_in_mb = 32;
    public ParameterizedClass commitlog_compression;
    public FlushCompression flush_compression = FlushCompression.fast;
    public int commitlog_max_compression_buffers_in_pool = 3;
    public Integer periodic_commitlog_sync_lag_block_in_ms;
    public TransparentDataEncryptionOptions transparent_data_encryption_options = new TransparentDataEncryptionOptions();

    public Integer max_mutation_size_in_kb;

    // Change-data-capture logs
    public boolean cdc_enabled = false;
    public String cdc_raw_directory;
    public int cdc_total_space_in_mb = 0;
    public int cdc_free_space_check_interval_ms = 250;

    @Deprecated
    public int commitlog_periodic_queue_size = -1;

    public String endpoint_snitch;
    public boolean dynamic_snitch = true;
    public int dynamic_snitch_update_interval_in_ms = 100;
    public int dynamic_snitch_reset_interval_in_ms = 600000;
    public double dynamic_snitch_badness_threshold = 1.0;

    public EncryptionOptions.ServerEncryptionOptions server_encryption_options = new EncryptionOptions.ServerEncryptionOptions();
    public EncryptionOptions client_encryption_options = new EncryptionOptions();

    public InternodeCompression internode_compression = InternodeCompression.none;

    public int hinted_handoff_throttle_in_kb = 1024;
    public int batchlog_replay_throttle_in_kb = 1024;
    public int max_hints_delivery_threads = 2;
    public int hints_flush_period_in_ms = 10000;
    public int max_hints_file_size_in_mb = 128;
    public ParameterizedClass hints_compression;

    public volatile boolean incremental_backups = false;
    public boolean trickle_fsync = false;
    public int trickle_fsync_interval_in_kb = 10240;

    public volatile int sstable_preemptive_open_interval_in_mb = 50;

    public volatile boolean key_cache_migrate_during_compaction = true;
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

    public int cache_load_timeout_seconds = 30;

    private static boolean isClientMode = false;
    private static Supplier<Config> overrideLoadConfig = null;

    public Integer networking_cache_size_in_mb;

    public Integer file_cache_size_in_mb;

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

    public volatile int tombstone_warn_threshold = 1000;
    public volatile int tombstone_failure_threshold = 100000;

    public final ReplicaFilteringProtectionOptions replica_filtering_protection = new ReplicaFilteringProtectionOptions();

    public volatile Long index_summary_capacity_in_mb;
    public volatile int index_summary_resize_interval_in_minutes = 60;

    public volatile int gc_log_threshold_in_ms = 200;
    public volatile int gc_warn_threshold_in_ms = 1000;

    // TTL for different types of trace events.
    public int tracetype_query_ttl = (int) TimeUnit.DAYS.toSeconds(1);
    public int tracetype_repair_ttl = (int) TimeUnit.DAYS.toSeconds(7);

    /**
     * Maintain statistics on whether writes achieve the ideal consistency level
     * before expiring and becoming hints
     */
    public volatile ConsistencyLevel ideal_consistency_level = null;

    @Deprecated
    public String otc_coalescing_strategy = "DISABLED";

    @Deprecated
    public static final int otc_coalescing_window_us_default = 200;
    @Deprecated
    public int otc_coalescing_window_us = otc_coalescing_window_us_default;
    @Deprecated
    public int otc_coalescing_enough_coalesced_messages = 8;
    @Deprecated
    public static final int otc_backlog_expiration_interval_ms_default = 200;
    @Deprecated
    public volatile int otc_backlog_expiration_interval_ms = otc_backlog_expiration_interval_ms_default;

    public int windows_timer_interval = 0;

    /**
     * Size of the CQL prepared statements cache in MB.
     * Defaults to 1/256th of the heap size or 10MB, whichever is greater.
     */
    public Long prepared_statements_cache_size_mb = null;

    public boolean enable_user_defined_functions = false;
    public boolean enable_scripted_user_defined_functions = false;

    public boolean enable_materialized_views = false;

    public boolean enable_transient_replication = false;

    public boolean enable_sasi_indexes = false;

    public volatile boolean enable_drop_compact_storage = false;

    /**
     * Optionally disable asynchronous UDF execution.
     * Disabling asynchronous UDF execution also implicitly disables the security-manager!
     * By default, async UDF execution is enabled to be able to detect UDFs that run too long / forever and be
     * able to fail fast - i.e. stop the Cassandra daemon, which is currently the only appropriate approach to
     * "tell" a user that there's something really wrong with the UDF.
     * When you disable async UDF execution, users MUST pay attention to read-timeouts since these may indicate
     * UDFs that run too long or forever - and this can destabilize the cluster.
     *
     * This requires allow_insecure_udfs to be true
     */
    public boolean enable_user_defined_functions_threads = true;

    /**
     * Set this to true to allow running insecure UDFs.
     */
    public boolean allow_insecure_udfs = false;

    /**
     * Set this to allow UDFs accessing java.lang.System.* methods, which basically allows UDFs to execute any arbitrary code on the system.
     */
    public boolean allow_extra_insecure_udfs = false;

    /**
     * Time in milliseconds after a warning will be emitted to the log and to the client that a UDF runs too long.
     * (Only valid, if enable_user_defined_functions_threads==true)
     */
    public long user_defined_function_warn_timeout = 500;
    /**
     * Time in milliseconds after a fatal UDF run-time situation is detected and action according to
     * user_function_timeout_policy will take place.
     * (Only valid, if enable_user_defined_functions_threads==true)
     */
    public long user_defined_function_fail_timeout = 1500;
    /**
     * Defines what to do when a UDF ran longer than user_defined_function_fail_timeout.
     * Possible options are:
     * - 'die' - i.e. it is able to emit a warning to the client before the Cassandra Daemon will shut down.
     * - 'die_immediate' - shut down C* daemon immediately (effectively prevent the chance that the client will receive a warning).
     * - 'ignore' - just log - the most dangerous option.
     * (Only valid, if enable_user_defined_functions_threads==true)
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
    public int block_for_peers_timeout_in_secs = 10;
    public boolean block_for_peers_in_remote_dcs = false;

    public volatile boolean automatic_sstable_upgrade = false;
    public volatile int max_concurrent_automatic_sstable_upgrades = 1;
    public boolean stream_entire_sstables = true;

    public volatile AuditLogOptions audit_logging_options = new AuditLogOptions();
    public volatile FullQueryLoggerOptions full_query_logging_options = new FullQueryLoggerOptions();

    public CorruptedTombstoneStrategy corrupted_tombstone_strategy = CorruptedTombstoneStrategy.disabled;

    public volatile boolean diagnostic_events_enabled = false;

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

    /**
     * The intial capacity for creating RangeTombstoneList.
     */
    public volatile int initial_range_tombstone_list_allocation_size = 1;
    /**
     * The growth factor to enlarge a RangeTombstoneList.
     */
    public volatile double range_tombstone_list_growth_factor = 1.5;

    public StorageAttachedIndexOptions sai_options = new StorageAttachedIndexOptions();

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

    // see CASSANDRA-17048 and the comment in cassandra.yaml
    public boolean enable_uuid_sstable_identifiers = true;

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
