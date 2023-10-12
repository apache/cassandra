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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Supplier;
import javax.annotation.Nullable;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.audit.AuditLogOptions;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.fql.FullQueryLoggerOptions;
import org.apache.cassandra.index.internal.CassandraIndex;
import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.apache.cassandra.service.StartupChecks.StartupCheckType;
import org.apache.cassandra.utils.StorageCompatibilityMode;

import static org.apache.cassandra.config.CassandraRelevantProperties.AUTOCOMPACTION_ON_STARTUP_ENABLED;
import static org.apache.cassandra.config.CassandraRelevantProperties.FILE_CACHE_ENABLED;
import static org.apache.cassandra.config.CassandraRelevantProperties.SKIP_PAXOS_REPAIR_ON_TOPOLOGY_CHANGE;
import static org.apache.cassandra.config.CassandraRelevantProperties.SKIP_PAXOS_REPAIR_ON_TOPOLOGY_CHANGE_KEYSPACES;

/**
 * A class that contains configuration properties for the cassandra node it runs within.
 * <p>
 * Properties declared as volatile can be mutated via JMX.
 */
public class Config
{
    private static final Logger logger = LoggerFactory.getLogger(Config.class);

    public static Set<String> splitCommaDelimited(String src)
    {
        if (src == null)
            return ImmutableSet.of();
        String[] split = src.split(",\\s*");
        ImmutableSet.Builder<String> builder = ImmutableSet.builder();
        for (String s : split)
        {
            s = s.trim();
            if (!s.isEmpty())
                builder.add(s);
        }
        return builder.build();
    }
    /*
     * Prefix for Java properties for internal Cassandra configuration options
     */
    public static final String PROPERTY_PREFIX = "cassandra.";

    public String cluster_name = "Test Cluster";
    public ParameterizedClass authenticator;
    public String authorizer;
    public String role_manager;
    public ParameterizedClass crypto_provider;
    public String network_authorizer;
    public ParameterizedClass cidr_authorizer;

    @Replaces(oldName = "permissions_validity_in_ms", converter = Converters.MILLIS_DURATION_INT, deprecated = true)
    public volatile DurationSpec.IntMillisecondsBound permissions_validity = new DurationSpec.IntMillisecondsBound("2s");
    public volatile int permissions_cache_max_entries = 1000;
    @Replaces(oldName = "permissions_update_interval_in_ms", converter = Converters.MILLIS_CUSTOM_DURATION, deprecated = true)
    public volatile DurationSpec.IntMillisecondsBound permissions_update_interval = null;
    public volatile boolean permissions_cache_active_update = false;
    @Replaces(oldName = "roles_validity_in_ms", converter = Converters.MILLIS_DURATION_INT, deprecated = true)
    public volatile DurationSpec.IntMillisecondsBound roles_validity = new DurationSpec.IntMillisecondsBound("2s");
    public volatile int roles_cache_max_entries = 1000;
    @Replaces(oldName = "roles_update_interval_in_ms", converter = Converters.MILLIS_CUSTOM_DURATION, deprecated = true)
    public volatile DurationSpec.IntMillisecondsBound roles_update_interval = null;
    public volatile boolean roles_cache_active_update = false;
    @Replaces(oldName = "credentials_validity_in_ms", converter = Converters.MILLIS_DURATION_INT, deprecated = true)
    public volatile DurationSpec.IntMillisecondsBound credentials_validity = new DurationSpec.IntMillisecondsBound("2s");
    public volatile int credentials_cache_max_entries = 1000;
    @Replaces(oldName = "credentials_update_interval_in_ms", converter = Converters.MILLIS_CUSTOM_DURATION, deprecated = true)
    public volatile DurationSpec.IntMillisecondsBound credentials_update_interval = null;
    public volatile boolean credentials_cache_active_update = false;

    /* Hashing strategy Random or OPHF */
    public String partitioner;

    public boolean auto_bootstrap = true;
    public volatile boolean hinted_handoff_enabled = true;
    public Set<String> hinted_handoff_disabled_datacenters = Sets.newConcurrentHashSet();
    @Replaces(oldName = "max_hint_window_in_ms", converter = Converters.MILLIS_DURATION_INT, deprecated = true)
    public volatile DurationSpec.IntMillisecondsBound max_hint_window = new DurationSpec.IntMillisecondsBound("3h");
    public String hints_directory;
    public boolean hint_window_persistent_enabled = true;

    public volatile boolean force_new_prepared_statement_behaviour = false;

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

    @Replaces(oldName = "native_transport_idle_timeout_in_ms", converter = Converters.MILLIS_DURATION_LONG, deprecated = true)
    public DurationSpec.LongMillisecondsBound native_transport_idle_timeout = new DurationSpec.LongMillisecondsBound("0ms");

    @Replaces(oldName = "request_timeout_in_ms", converter = Converters.MILLIS_DURATION_LONG, deprecated = true)
    public volatile DurationSpec.LongMillisecondsBound request_timeout = new DurationSpec.LongMillisecondsBound("10000ms");

    @Replaces(oldName = "read_request_timeout_in_ms", converter = Converters.MILLIS_DURATION_LONG, deprecated = true)
    public volatile DurationSpec.LongMillisecondsBound read_request_timeout = new DurationSpec.LongMillisecondsBound("5000ms");

    @Replaces(oldName = "range_request_timeout_in_ms", converter = Converters.MILLIS_DURATION_LONG, deprecated = true)
    public volatile DurationSpec.LongMillisecondsBound range_request_timeout = new DurationSpec.LongMillisecondsBound("10000ms");

    @Replaces(oldName = "write_request_timeout_in_ms", converter = Converters.MILLIS_DURATION_LONG, deprecated = true)
    public volatile DurationSpec.LongMillisecondsBound write_request_timeout = new DurationSpec.LongMillisecondsBound("2000ms");

    @Replaces(oldName = "counter_write_request_timeout_in_ms", converter = Converters.MILLIS_DURATION_LONG, deprecated = true)
    public volatile DurationSpec.LongMillisecondsBound counter_write_request_timeout = new DurationSpec.LongMillisecondsBound("5000ms");

    @Replaces(oldName = "cas_contention_timeout_in_ms", converter = Converters.MILLIS_DURATION_LONG, deprecated = true)
    public volatile DurationSpec.LongMillisecondsBound cas_contention_timeout = new DurationSpec.LongMillisecondsBound("1800ms");

    @Replaces(oldName = "truncate_request_timeout_in_ms", converter = Converters.MILLIS_DURATION_LONG, deprecated = true)
    public volatile DurationSpec.LongMillisecondsBound truncate_request_timeout = new DurationSpec.LongMillisecondsBound("60000ms");

    @Replaces(oldName = "repair_request_timeout_in_ms", converter = Converters.MILLIS_DURATION_LONG, deprecated = true)
    public volatile DurationSpec.LongMillisecondsBound repair_request_timeout = new DurationSpec.LongMillisecondsBound("120000ms");

    public Integer streaming_connections_per_host = 1;
    @Replaces(oldName = "streaming_keep_alive_period_in_secs", converter = Converters.SECONDS_DURATION, deprecated = true)
    public DurationSpec.IntSecondsBound streaming_keep_alive_period = new DurationSpec.IntSecondsBound("300s");

    @Replaces(oldName = "cross_node_timeout", converter = Converters.IDENTITY, deprecated = true)
    public boolean internode_timeout = true;

    @Replaces(oldName = "slow_query_log_timeout_in_ms", converter = Converters.MILLIS_DURATION_LONG, deprecated = true)
    public volatile DurationSpec.LongMillisecondsBound slow_query_log_timeout = new DurationSpec.LongMillisecondsBound("500ms");

    public volatile DurationSpec.LongMillisecondsBound stream_transfer_task_timeout = new DurationSpec.LongMillisecondsBound("12h");

    public volatile double phi_convict_threshold = 8.0;

    public int concurrent_reads = 32;
    public int concurrent_writes = 32;
    public int concurrent_counter_writes = 32;
    public int concurrent_materialized_view_writes = 32;
    public int available_processors = -1;

    /** @deprecated See CASSANDRA-6504 */
    @Deprecated(since = "2.1")
    public Integer concurrent_replicates = null;

    public int memtable_flush_writers = 0;
    @Replaces(oldName = "memtable_heap_space_in_mb", converter = Converters.MEBIBYTES_DATA_STORAGE_INT, deprecated = true)
    public DataStorageSpec.IntMebibytesBound memtable_heap_space;
    @Replaces(oldName = "memtable_offheap_space_in_mb", converter = Converters.MEBIBYTES_DATA_STORAGE_INT, deprecated = true)
    public DataStorageSpec.IntMebibytesBound memtable_offheap_space;
    public Float memtable_cleanup_threshold = null;

    public static class MemtableOptions
    {
        public LinkedHashMap<String, InheritingClass> configurations; // order must be preserved

        public MemtableOptions()
        {
        }
    }

    public MemtableOptions memtable;

    // Limit the maximum depth of repair session merkle trees
    /** @deprecated See  */
    @Deprecated(since = "4.0")
    public volatile Integer repair_session_max_tree_depth = null;
    @Replaces(oldName = "repair_session_space_in_mb", converter = Converters.MEBIBYTES_DATA_STORAGE_INT, deprecated = true)
    public volatile DataStorageSpec.IntMebibytesBound repair_session_space = null;

    public volatile boolean use_offheap_merkle_trees = true;

    public int storage_port = 7000;
    public int ssl_storage_port = 7001;
    public String listen_address;
    public String listen_interface;
    public boolean listen_interface_prefer_ipv6 = false;
    public String broadcast_address;
    public boolean listen_on_broadcast_address = false;
    public ParameterizedClass internode_authenticator;

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

    @Replaces(oldName = "internode_max_message_size_in_bytes", converter = Converters.BYTES_DATASTORAGE, deprecated=true)
    public DataStorageSpec.IntBytesBound internode_max_message_size;

    @Replaces(oldName = "internode_socket_send_buffer_size_in_bytes", converter = Converters.BYTES_DATASTORAGE, deprecated = true)
    @Replaces(oldName = "internode_send_buff_size_in_bytes", converter = Converters.BYTES_DATASTORAGE, deprecated = true)
    public DataStorageSpec.IntBytesBound internode_socket_send_buffer_size = new DataStorageSpec.IntBytesBound("0B");
    @Replaces(oldName = "internode_socket_receive_buffer_size_in_bytes", converter = Converters.BYTES_DATASTORAGE, deprecated = true)
    @Replaces(oldName = "internode_recv_buff_size_in_bytes", converter = Converters.BYTES_DATASTORAGE, deprecated = true)
    public DataStorageSpec.IntBytesBound internode_socket_receive_buffer_size = new DataStorageSpec.IntBytesBound("0B");

    // TODO: derive defaults from system memory settings?
    @Replaces(oldName = "internode_application_send_queue_capacity_in_bytes", converter = Converters.BYTES_DATASTORAGE, deprecated = true)
    public DataStorageSpec.IntBytesBound internode_application_send_queue_capacity = new DataStorageSpec.IntBytesBound("4MiB");
    @Replaces(oldName = "internode_application_send_queue_reserve_endpoint_capacity_in_bytes", converter = Converters.BYTES_DATASTORAGE, deprecated = true)
    public DataStorageSpec.IntBytesBound internode_application_send_queue_reserve_endpoint_capacity = new DataStorageSpec.IntBytesBound("128MiB");
    @Replaces(oldName = "internode_application_send_queue_reserve_global_capacity_in_bytes", converter = Converters.BYTES_DATASTORAGE, deprecated = true)
    public DataStorageSpec.IntBytesBound internode_application_send_queue_reserve_global_capacity = new DataStorageSpec.IntBytesBound("512MiB");

    @Replaces(oldName = "internode_application_receive_queue_capacity_in_bytes", converter = Converters.BYTES_DATASTORAGE, deprecated = true)
    public DataStorageSpec.IntBytesBound internode_application_receive_queue_capacity = new DataStorageSpec.IntBytesBound("4MiB");
    @Replaces(oldName = "internode_application_receive_queue_reserve_endpoint_capacity_in_bytes", converter = Converters.BYTES_DATASTORAGE, deprecated = true)
    public DataStorageSpec.IntBytesBound internode_application_receive_queue_reserve_endpoint_capacity = new DataStorageSpec.IntBytesBound("128MiB");
    @Replaces(oldName = "internode_application_receive_queue_reserve_global_capacity_in_bytes", converter = Converters.BYTES_DATASTORAGE, deprecated = true)
    public DataStorageSpec.IntBytesBound internode_application_receive_queue_reserve_global_capacity = new DataStorageSpec.IntBytesBound("512MiB");

    // Defensive settings for protecting Cassandra from true network partitions. See (CASSANDRA-14358) for details.
    // The amount of time to wait for internode tcp connections to establish.
    @Replaces(oldName = "internode_tcp_connect_timeout_in_ms", converter = Converters.MILLIS_DURATION_INT, deprecated = true)
    public volatile DurationSpec.IntMillisecondsBound internode_tcp_connect_timeout = new DurationSpec.IntMillisecondsBound("2s");
    // The amount of time unacknowledged data is allowed on a connection before we throw out the connection
    // Note this is only supported on Linux + epoll, and it appears to behave oddly above a setting of 30000
    // (it takes much longer than 30s) as of Linux 4.12. If you want something that high set this to 0
    // (which picks up the OS default) and configure the net.ipv4.tcp_retries2 sysctl to be ~8.
    @Replaces(oldName = "internode_tcp_user_timeout_in_ms", converter = Converters.MILLIS_DURATION_INT, deprecated = true)
    public volatile DurationSpec.IntMillisecondsBound internode_tcp_user_timeout = new DurationSpec.IntMillisecondsBound("30s");
    // Similar to internode_tcp_user_timeout but used specifically for streaming connection.
    // The default is 5 minutes. Increase it or set it to 0 in order to increase the timeout.
    @Replaces(oldName = "internode_streaming_tcp_user_timeout_in_ms", converter = Converters.MILLIS_DURATION_INT, deprecated = true)
    public volatile DurationSpec.IntMillisecondsBound internode_streaming_tcp_user_timeout = new DurationSpec.IntMillisecondsBound("300s"); // 5 minutes

    public boolean start_native_transport = true;
    public int native_transport_port = 9042;
    public Integer native_transport_port_ssl = null;
    public int native_transport_max_threads = 128;
    @Replaces(oldName = "native_transport_max_frame_size_in_mb", converter = Converters.MEBIBYTES_DATA_STORAGE_INT, deprecated = true)
    public DataStorageSpec.IntMebibytesBound native_transport_max_frame_size = new DataStorageSpec.IntMebibytesBound("16MiB");
    /** do bcrypt hashing in a limited pool to prevent cpu load spikes; note: any value < 1 will be set to 1 on init **/
    public int native_transport_max_auth_threads = 4;
    public volatile long native_transport_max_concurrent_connections = -1L;
    public volatile long native_transport_max_concurrent_connections_per_ip = -1L;
    public boolean native_transport_flush_in_batches_legacy = false;
    public volatile boolean native_transport_allow_older_protocols = true;
    // Below 2 parameters were fixed in 4.0 + to get default value when ==-1 (old name and value format) or ==null(new name and value format),
    // not <=0 as it is in previous versions. Throwing config exceptions on < -1
    @Replaces(oldName = "native_transport_max_concurrent_requests_in_bytes_per_ip", converter = Converters.BYTES_CUSTOM_DATASTORAGE, deprecated = true)
    public volatile DataStorageSpec.LongBytesBound native_transport_max_request_data_in_flight_per_ip = null;
    @Replaces(oldName = "native_transport_max_concurrent_requests_in_bytes", converter = Converters.BYTES_CUSTOM_DATASTORAGE, deprecated = true)
    public volatile DataStorageSpec.LongBytesBound native_transport_max_request_data_in_flight = null;
    public volatile boolean native_transport_rate_limiting_enabled = false;
    public volatile int native_transport_max_requests_per_second = 1000000;
    @Replaces(oldName = "native_transport_receive_queue_capacity_in_bytes", converter = Converters.BYTES_DATASTORAGE, deprecated = true)
    public DataStorageSpec.IntBytesBound native_transport_receive_queue_capacity = new DataStorageSpec.IntBytesBound("1MiB");

    /** @deprecated See CASSANDRA-5529 */
    @Deprecated(since = "1.2.6")
    public Integer native_transport_max_negotiable_protocol_version = null;

    /**
     * Max size of values in SSTables, in MebiBytes.
     * Default is the same as the native protocol frame limit: 256MiB.
     * See AbstractType for how it is used.
     */
    @Replaces(oldName = "max_value_size_in_mb", converter = Converters.MEBIBYTES_DATA_STORAGE_INT, deprecated = true)
    public DataStorageSpec.IntMebibytesBound max_value_size = new DataStorageSpec.IntMebibytesBound("256MiB");

    public boolean snapshot_before_compaction = false;
    public boolean auto_snapshot = true;

    /**
     * When auto_snapshot is true and this property
     * is set, snapshots created by truncation or
     * drop use this TTL.
     */
    public String auto_snapshot_ttl;

    public volatile long snapshot_links_per_second = 0;

    /* if the size of columns or super-columns are more than this, indexing will kick in */
    @Replaces(oldName = "column_index_size_in_kb", converter = Converters.KIBIBYTES_DATASTORAGE, deprecated = true)
    public volatile DataStorageSpec.IntKibibytesBound column_index_size;
    @Replaces(oldName = "column_index_cache_size_in_kb", converter = Converters.KIBIBYTES_DATASTORAGE, deprecated = true)
    public volatile DataStorageSpec.IntKibibytesBound column_index_cache_size = new DataStorageSpec.IntKibibytesBound("2KiB");
    @Replaces(oldName = "batch_size_warn_threshold_in_kb", converter = Converters.KIBIBYTES_DATASTORAGE, deprecated = true)
    public volatile DataStorageSpec.IntKibibytesBound batch_size_warn_threshold = new DataStorageSpec.IntKibibytesBound("5KiB");
    @Replaces(oldName = "batch_size_fail_threshold_in_kb", converter = Converters.KIBIBYTES_DATASTORAGE, deprecated = true)
    public volatile DataStorageSpec.IntKibibytesBound batch_size_fail_threshold = new DataStorageSpec.IntKibibytesBound("50KiB");

    public Integer unlogged_batch_across_partitions_warn_threshold = 10;
    public volatile Integer concurrent_compactors;
    @Replaces(oldName = "compaction_throughput_mb_per_sec", converter = Converters.MEBIBYTES_PER_SECOND_DATA_RATE, deprecated = true)
    public volatile DataRateSpec.LongBytesPerSecondBound compaction_throughput = new DataRateSpec.LongBytesPerSecondBound("64MiB/s");
    @Replaces(oldName = "min_free_space_per_drive_in_mb", converter = Converters.MEBIBYTES_DATA_STORAGE_INT, deprecated = true)
    public DataStorageSpec.IntMebibytesBound min_free_space_per_drive = new DataStorageSpec.IntMebibytesBound("50MiB");

    // fraction of free disk space available for compaction after min free space is subtracted
    public volatile Double max_space_usable_for_compactions_in_percentage = .95;

    public volatile int concurrent_materialized_view_builders = 1;
    public volatile int reject_repair_compaction_threshold = Integer.MAX_VALUE;

    // The number of executors to use for building secondary indexes
    public int concurrent_index_builders = 2;

    /**
     * @deprecated retry support removed on CASSANDRA-10992
     */
    /** @deprecated See CASSANDRA-17378 */
    @Deprecated(since = "4.1")
    public int max_streaming_retries = 3;

    @Replaces(oldName = "stream_throughput_outbound_megabits_per_sec", converter = Converters.MEGABITS_TO_BYTES_PER_SECOND_DATA_RATE, deprecated = true)
    public volatile DataRateSpec.LongBytesPerSecondBound stream_throughput_outbound = new DataRateSpec.LongBytesPerSecondBound("24MiB/s");
    @Replaces(oldName = "inter_dc_stream_throughput_outbound_megabits_per_sec", converter = Converters.MEGABITS_TO_BYTES_PER_SECOND_DATA_RATE, deprecated = true)
    public volatile DataRateSpec.LongBytesPerSecondBound inter_dc_stream_throughput_outbound = new DataRateSpec.LongBytesPerSecondBound("24MiB/s");

    public volatile DataRateSpec.LongBytesPerSecondBound entire_sstable_stream_throughput_outbound = new DataRateSpec.LongBytesPerSecondBound("24MiB/s");
    public volatile DataRateSpec.LongBytesPerSecondBound entire_sstable_inter_dc_stream_throughput_outbound = new DataRateSpec.LongBytesPerSecondBound("24MiB/s");

    public String[] data_file_directories = new String[0];

    public static class SSTableConfig
    {
        public String selected_format = BigFormat.NAME;
        public Map<String, Map<String, String>> format = new HashMap<>();
    }

    public final SSTableConfig sstable = new SSTableConfig();

    /**
     * The directory to use for storing the system keyspaces data.
     * If unspecified the data will be stored in the first of the data_file_directories.
     */
    public String local_system_data_file_directory;

    public String saved_caches_directory;

    // Commit Log
    public String commitlog_directory;
    @Replaces(oldName = "commitlog_total_space_in_mb", converter = Converters.MEBIBYTES_DATA_STORAGE_INT, deprecated = true)
    public DataStorageSpec.IntMebibytesBound commitlog_total_space;
    public CommitLogSync commitlog_sync;
    @Replaces(oldName = "commitlog_sync_group_window_in_ms", converter = Converters.MILLIS_DURATION_DOUBLE, deprecated = true)
    public DurationSpec.IntMillisecondsBound commitlog_sync_group_window = new DurationSpec.IntMillisecondsBound("0ms");
    @Replaces(oldName = "commitlog_sync_period_in_ms", converter = Converters.MILLIS_DURATION_INT, deprecated = true)
    public DurationSpec.IntMillisecondsBound commitlog_sync_period = new DurationSpec.IntMillisecondsBound("0ms");
    @Replaces(oldName = "commitlog_segment_size_in_mb", converter = Converters.MEBIBYTES_DATA_STORAGE_INT, deprecated = true)
    public DataStorageSpec.IntMebibytesBound commitlog_segment_size = new DataStorageSpec.IntMebibytesBound("32MiB");
    public ParameterizedClass commitlog_compression;
    public FlushCompression flush_compression = FlushCompression.fast;
    public int commitlog_max_compression_buffers_in_pool = 3;
    @Replaces(oldName = "periodic_commitlog_sync_lag_block_in_ms", converter = Converters.MILLIS_DURATION_INT, deprecated = true)
    public DurationSpec.IntMillisecondsBound periodic_commitlog_sync_lag_block;
    public TransparentDataEncryptionOptions transparent_data_encryption_options = new TransparentDataEncryptionOptions();

    @Replaces(oldName = "max_mutation_size_in_kb", converter = Converters.KIBIBYTES_DATASTORAGE, deprecated = true)
    public DataStorageSpec.IntKibibytesBound max_mutation_size;

    // Change-data-capture logs
    public boolean cdc_enabled = false;
    // When true, new CDC mutations are rejected/blocked when reaching max CDC storage.
    // When false, new CDC mutations can always be added. But it will remove the oldest CDC commit log segment on full.
    public volatile boolean cdc_block_writes = true;
    // When true, CDC data in SSTable go through commit logs during internodes streaming, e.g. repair
    // When false, it behaves the same as normal streaming.
    public volatile boolean cdc_on_repair_enabled = true;
    public String cdc_raw_directory;
    @Replaces(oldName = "cdc_total_space_in_mb", converter = Converters.MEBIBYTES_DATA_STORAGE_INT, deprecated = true)
    public DataStorageSpec.IntMebibytesBound cdc_total_space = new DataStorageSpec.IntMebibytesBound("0MiB");
    @Replaces(oldName = "cdc_free_space_check_interval_ms", converter = Converters.MILLIS_DURATION_INT, deprecated = true)
    public DurationSpec.IntMillisecondsBound cdc_free_space_check_interval = new DurationSpec.IntMillisecondsBound("250ms");

    /** @deprecated See CASSANDRA-3578 */
    @Deprecated(since = "2.1.3")
    public int commitlog_periodic_queue_size = -1;

    public String endpoint_snitch;
    public boolean dynamic_snitch = true;
    @Replaces(oldName = "dynamic_snitch_update_interval_in_ms", converter = Converters.MILLIS_DURATION_INT, deprecated = true)
    public DurationSpec.IntMillisecondsBound dynamic_snitch_update_interval = new DurationSpec.IntMillisecondsBound("100ms");
    @Replaces(oldName = "dynamic_snitch_reset_interval_in_ms", converter = Converters.MILLIS_DURATION_INT, deprecated = true)
    public DurationSpec.IntMillisecondsBound dynamic_snitch_reset_interval = new DurationSpec.IntMillisecondsBound("10m");
    public double dynamic_snitch_badness_threshold = 1.0;

    public String failure_detector = "FailureDetector";

    public EncryptionOptions.ServerEncryptionOptions server_encryption_options = new EncryptionOptions.ServerEncryptionOptions();
    public EncryptionOptions client_encryption_options = new EncryptionOptions();

    public InternodeCompression internode_compression = InternodeCompression.none;

    @Replaces(oldName = "hinted_handoff_throttle_in_kb", converter = Converters.KIBIBYTES_DATASTORAGE, deprecated = true)
    public DataStorageSpec.IntKibibytesBound hinted_handoff_throttle = new DataStorageSpec.IntKibibytesBound("1024KiB");
    @Replaces(oldName = "batchlog_replay_throttle_in_kb", converter = Converters.KIBIBYTES_DATASTORAGE, deprecated = true)
    public DataStorageSpec.IntKibibytesBound batchlog_replay_throttle = new DataStorageSpec.IntKibibytesBound("1024KiB");
    public int max_hints_delivery_threads = 2;
    @Replaces(oldName = "hints_flush_period_in_ms", converter = Converters.MILLIS_DURATION_INT, deprecated = true)
    public DurationSpec.IntMillisecondsBound hints_flush_period = new DurationSpec.IntMillisecondsBound("10s");
    @Replaces(oldName = "max_hints_file_size_in_mb", converter = Converters.MEBIBYTES_DATA_STORAGE_INT, deprecated = true)
    public DataStorageSpec.IntMebibytesBound max_hints_file_size = new DataStorageSpec.IntMebibytesBound("128MiB");
    public volatile DataStorageSpec.LongBytesBound max_hints_size_per_host = new DataStorageSpec.LongBytesBound("0B"); // 0 means disabled

    public ParameterizedClass hints_compression;
    public volatile boolean auto_hints_cleanup_enabled = false;
    public volatile boolean transfer_hints_on_decommission = true;

    public volatile boolean incremental_backups = false;
    public boolean trickle_fsync = false;
    @Replaces(oldName = "trickle_fsync_interval_in_kb", converter = Converters.KIBIBYTES_DATASTORAGE, deprecated = true)
    public DataStorageSpec.IntKibibytesBound trickle_fsync_interval = new DataStorageSpec.IntKibibytesBound("10240KiB");

    @Nullable
    @Replaces(oldName = "sstable_preemptive_open_interval_in_mb", converter = Converters.NEGATIVE_MEBIBYTES_DATA_STORAGE_INT, deprecated = true)
    public volatile DataStorageSpec.IntMebibytesBound sstable_preemptive_open_interval = new DataStorageSpec.IntMebibytesBound("50MiB");

    public volatile boolean key_cache_migrate_during_compaction = true;
    public volatile int key_cache_keys_to_save = Integer.MAX_VALUE;
    @Replaces(oldName = "key_cache_size_in_mb", converter = Converters.MEBIBYTES_DATA_STORAGE_LONG, deprecated = true)
    public DataStorageSpec.LongMebibytesBound key_cache_size = null;
    @Replaces(oldName = "key_cache_save_period", converter = Converters.SECONDS_CUSTOM_DURATION)
    public volatile DurationSpec.IntSecondsBound key_cache_save_period = new DurationSpec.IntSecondsBound("4h");

    public String row_cache_class_name = "org.apache.cassandra.cache.OHCProvider";
    @Replaces(oldName = "row_cache_size_in_mb", converter = Converters.MEBIBYTES_DATA_STORAGE_LONG, deprecated = true)
    public DataStorageSpec.LongMebibytesBound row_cache_size = new DataStorageSpec.LongMebibytesBound("0MiB");
    @Replaces(oldName = "row_cache_save_period", converter = Converters.SECONDS_CUSTOM_DURATION)
    public volatile DurationSpec.IntSecondsBound row_cache_save_period = new DurationSpec.IntSecondsBound("0s");
    public volatile int row_cache_keys_to_save = Integer.MAX_VALUE;

    @Replaces(oldName = "counter_cache_size_in_mb", converter = Converters.MEBIBYTES_DATA_STORAGE_LONG, deprecated = true)
    public DataStorageSpec.LongMebibytesBound counter_cache_size = null;
    @Replaces(oldName = "counter_cache_save_period", converter = Converters.SECONDS_CUSTOM_DURATION)
    public volatile DurationSpec.IntSecondsBound counter_cache_save_period = new DurationSpec.IntSecondsBound("7200s");
    public volatile int counter_cache_keys_to_save = Integer.MAX_VALUE;

    public DataStorageSpec.LongMebibytesBound paxos_cache_size = null;

    @Replaces(oldName = "cache_load_timeout_seconds", converter = Converters.NEGATIVE_SECONDS_DURATION, deprecated = true)
    public DurationSpec.IntSecondsBound cache_load_timeout = new DurationSpec.IntSecondsBound("30s");

    private static boolean isClientMode = false;
    private static Supplier<Config> overrideLoadConfig = null;

    @Replaces(oldName = "networking_cache_size_in_mb", converter = Converters.MEBIBYTES_DATA_STORAGE_INT, deprecated = true)
    public DataStorageSpec.IntMebibytesBound networking_cache_size;

    @Replaces(oldName = "file_cache_size_in_mb", converter = Converters.MEBIBYTES_DATA_STORAGE_INT, deprecated = true)
    public DataStorageSpec.IntMebibytesBound file_cache_size;

    public boolean file_cache_enabled = FILE_CACHE_ENABLED.getBoolean();

    /**
     * Because of the current {@link org.apache.cassandra.utils.memory.BufferPool} slab sizes of 64 KiB, we
     * store in the file cache buffers that divide 64 KiB, so we need to round the buffer sizes to powers of two.
     * This boolean controls weather they are rounded up or down. Set it to true to round up to the
     * next power of two, set it to false to round down to the previous power of two. Note that buffer sizes are
     * already rounded to 4 KiB and capped between 4 KiB minimum and 64 kb maximum by the {@link DiskOptimizationStrategy}.
     * By default, this boolean is set to round down when {@link #disk_optimization_strategy} is {@code ssd},
     * and to round up when it is {@code spinning}.
     */
    public Boolean file_cache_round_up;

    /** @deprecated See CASSANDRA-15358 */
    @Deprecated(since = "4.0")
    public boolean buffer_pool_use_heap_if_exhausted;

    public DiskOptimizationStrategy disk_optimization_strategy = DiskOptimizationStrategy.ssd;

    public double disk_optimization_estimate_percentile = 0.95;

    public double disk_optimization_page_cross_chance = 0.1;

    public boolean inter_dc_tcp_nodelay = true;

    public MemtableAllocationType memtable_allocation_type = MemtableAllocationType.heap_buffers;

    public volatile boolean read_thresholds_enabled = false;
    public volatile DataStorageSpec.LongBytesBound coordinator_read_size_warn_threshold = null;
    public volatile DataStorageSpec.LongBytesBound coordinator_read_size_fail_threshold = null;
    public volatile DataStorageSpec.LongBytesBound local_read_size_warn_threshold = null;
    public volatile DataStorageSpec.LongBytesBound local_read_size_fail_threshold = null;
    public volatile DataStorageSpec.LongBytesBound row_index_read_size_warn_threshold = null;
    public volatile DataStorageSpec.LongBytesBound row_index_read_size_fail_threshold = null;

    public volatile int tombstone_warn_threshold = 1000;
    public volatile int tombstone_failure_threshold = 100000;

    public final ReplicaFilteringProtectionOptions replica_filtering_protection = new ReplicaFilteringProtectionOptions();

    @Replaces(oldName = "index_summary_capacity_in_mb", converter = Converters.MEBIBYTES_DATA_STORAGE_LONG, deprecated = true)
    public volatile DataStorageSpec.LongMebibytesBound index_summary_capacity;
    @Nullable
    @Replaces(oldName = "index_summary_resize_interval_in_minutes", converter = Converters.MINUTES_CUSTOM_DURATION, deprecated = true)
    public volatile DurationSpec.IntMinutesBound index_summary_resize_interval = new DurationSpec.IntMinutesBound("60m");

    @Replaces(oldName = "gc_log_threshold_in_ms", converter = Converters.MILLIS_DURATION_INT, deprecated = true)
    public volatile DurationSpec.IntMillisecondsBound gc_log_threshold = new DurationSpec.IntMillisecondsBound("200ms");
    @Replaces(oldName = "gc_warn_threshold_in_ms", converter = Converters.MILLIS_DURATION_INT, deprecated = true)
    public volatile DurationSpec.IntMillisecondsBound gc_warn_threshold = new DurationSpec.IntMillisecondsBound("1s");

    // TTL for different types of trace events.
    @Replaces(oldName = "tracetype_query_ttl", converter = Converters.SECONDS_DURATION, deprecated=true)
    public DurationSpec.IntSecondsBound trace_type_query_ttl = new DurationSpec.IntSecondsBound("1d");
    @Replaces(oldName = "tracetype_repair_ttl", converter = Converters.SECONDS_DURATION, deprecated=true)
    public DurationSpec.IntSecondsBound trace_type_repair_ttl = new DurationSpec.IntSecondsBound("7d");

    /**
     * Maintain statistics on whether writes achieve the ideal consistency level
     * before expiring and becoming hints
     */
    public volatile ConsistencyLevel ideal_consistency_level = null;

    /** @deprecated See CASSANDRA-17404 */
    @Deprecated(since = "4.1")
    public int windows_timer_interval = 0;

    @Deprecated(since = "4.0")
    public String otc_coalescing_strategy = "DISABLED";

    @Deprecated(since = "4.0")
    public static final int otc_coalescing_window_us_default = 200;
    @Deprecated(since = "4.0")
    public int otc_coalescing_window_us = otc_coalescing_window_us_default;
    @Deprecated(since = "4.0")
    public int otc_coalescing_enough_coalesced_messages = 8;
    @Deprecated(since = "4.0")
    public static final int otc_backlog_expiration_interval_ms_default = 200;
    @Deprecated(since = "4.0")
    public volatile int otc_backlog_expiration_interval_ms = otc_backlog_expiration_interval_ms_default;

    /**
     * Size of the CQL prepared statements cache in MiB.
     * Defaults to 1/256th of the heap size or 10MiB, whichever is greater.
     */
    @Replaces(oldName = "prepared_statements_cache_size_mb", converter = Converters.MEBIBYTES_DATA_STORAGE_LONG, deprecated = true)
    public DataStorageSpec.LongMebibytesBound prepared_statements_cache_size = null;

    @Replaces(oldName = "enable_user_defined_functions", converter = Converters.IDENTITY, deprecated = true)
    public boolean user_defined_functions_enabled = false;

    /** @deprecated See CASSANDRA-18252 */
    @Deprecated(since = "5.0")
    @Replaces(oldName = "enable_scripted_user_defined_functions", converter = Converters.IDENTITY, deprecated = true)
    public boolean scripted_user_defined_functions_enabled = false;

    @Replaces(oldName = "enable_materialized_views", converter = Converters.IDENTITY, deprecated = true)
    public boolean materialized_views_enabled = false;

    @Replaces(oldName = "enable_transient_replication", converter = Converters.IDENTITY, deprecated = true)
    public boolean transient_replication_enabled = false;

    @Replaces(oldName = "enable_sasi_indexes", converter = Converters.IDENTITY, deprecated = true)
    public boolean sasi_indexes_enabled = false;

    @Replaces(oldName = "enable_drop_compact_storage", converter = Converters.IDENTITY, deprecated = true)
    public volatile boolean drop_compact_storage_enabled = false;

    public volatile boolean use_statements_enabled = true;

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
    // Below parameter is not presented in cassandra.yaml but to be on the safe side that no one was directly using it
    // I still added backward compatibility (CASSANDRA-15234)
    @Replaces(oldName = "enable_user_defined_functions_threads", converter = Converters.IDENTITY, deprecated = true)
    public boolean user_defined_functions_threads_enabled = true;

    /**
     * Set this to true to allow running insecure UDFs.
     */
    public boolean allow_insecure_udfs = false;

    /**
     * Set this to allow UDFs accessing java.lang.System.* methods, which basically allows UDFs to execute any arbitrary code on the system.
     */
    public boolean allow_extra_insecure_udfs = false;

    public boolean dynamic_data_masking_enabled = false;

    /**
     * Time in milliseconds after a warning will be emitted to the log and to the client that a UDF runs too long.
     * (Only valid, if user_defined_functions_threads_enabled==true)
     */
    @Replaces(oldName = "user_defined_function_warn_timeout", converter = Converters.MILLIS_DURATION_LONG, deprecated = true)
    public DurationSpec.LongMillisecondsBound user_defined_functions_warn_timeout = new DurationSpec.LongMillisecondsBound("500ms");
    /**
     * Time in milliseconds after a fatal UDF run-time situation is detected and action according to
     * user_function_timeout_policy will take place.
     * (Only valid, if user_defined_functions_threads_enabled==true)
     */
    @Replaces(oldName = "user_defined_function_fail_timeout", converter = Converters.MILLIS_DURATION_LONG, deprecated = true)
    public DurationSpec.LongMillisecondsBound user_defined_functions_fail_timeout = new DurationSpec.LongMillisecondsBound("1500ms");
    /**
     * Defines what to do when a UDF ran longer than user_defined_functions_fail_timeout.
     * Possible options are:
     * - 'die' - i.e. it is able to emit a warning to the client before the Cassandra Daemon will shut down.
     * - 'die_immediate' - shut down C* daemon immediately (effectively prevent the chance that the client will receive a warning).
     * - 'ignore' - just log - the most dangerous option.
     * (Only valid, if user_defined_functions_threads_enabled==true)
     */
    public UserFunctionTimeoutPolicy user_function_timeout_policy = UserFunctionTimeoutPolicy.die;

    /** @deprecated See CASSANDRA-15375 */
    @Deprecated(since = "4.0")
    public volatile boolean back_pressure_enabled = false;
    /** @deprecated See CASSANDRA-15375 */
    @Deprecated(since = "4.0")
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

    public volatile boolean skip_stream_disk_space_check = false;

    public volatile AuditLogOptions audit_logging_options = new AuditLogOptions();
    public volatile FullQueryLoggerOptions full_query_logging_options = new FullQueryLoggerOptions();

    public CorruptedTombstoneStrategy corrupted_tombstone_strategy = CorruptedTombstoneStrategy.disabled;

    public volatile boolean diagnostic_events_enabled = false;

    // Default keyspace replication factors allow validation of newly created keyspaces
    // and good defaults if no replication factor is provided by the user
    public volatile int default_keyspace_rf = 1;

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
     * Number of seconds to set nowInSec into the future when performing validation previews against repaired data
     * this (attempts) to prevent a race where validations on different machines are started on different sides of
     * a tombstone being compacted away
     */

    @Replaces(oldName = "validation_preview_purge_head_start_in_sec", converter = Converters.NEGATIVE_SECONDS_DURATION, deprecated = true)
    public volatile DurationSpec.IntSecondsBound validation_preview_purge_head_start = new DurationSpec.IntSecondsBound("3600s");

    public boolean auth_cache_warming_enabled = false;

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

    public DurationSpec.IntSecondsBound denylist_refresh = new DurationSpec.IntSecondsBound("600s");

    public DurationSpec.IntSecondsBound denylist_initial_load_retry = new DurationSpec.IntSecondsBound("5s");

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

    public StorageAttachedIndexOptions sai_options = new StorageAttachedIndexOptions();

    /**
     * @deprecated migrate to {@link DatabaseDescriptor#isClientInitialized()} See CASSANDRA-12550
     */
    @Deprecated(since = "3.10")
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

    public boolean autocompaction_on_startup_enabled = AUTOCOMPACTION_ON_STARTUP_ENABLED.getBoolean();

    // see CASSANDRA-3200 / CASSANDRA-16274
    public volatile boolean auto_optimise_inc_repair_streams = false;
    public volatile boolean auto_optimise_full_repair_streams = false;
    public volatile boolean auto_optimise_preview_repair_streams = false;

    // see CASSANDRA-17048 and the comment in cassandra.yaml
    public boolean uuid_sstable_identifiers_enabled = false;

    /**
     * Client mode means that the process is a pure client, that uses C* code base but does
     * not read or write local C* database files.
     *
     * @deprecated migrate to {@link DatabaseDescriptor#clientInitialization(boolean)} See CASSANDRA-12550
     */
    @Deprecated(since = "3.10")
    public static void setClientMode(boolean clientMode)
    {
        isClientMode = clientMode;
    }

    public volatile int consecutive_message_errors_threshold = 1;

    public volatile SubnetGroups client_error_reporting_exclusions = new SubnetGroups();
    public volatile SubnetGroups internode_error_reporting_exclusions = new SubnetGroups();

    @Replaces(oldName = "keyspace_count_warn_threshold", converter = Converters.KEYSPACE_COUNT_THRESHOLD_TO_GUARDRAIL, deprecated = true)
    public volatile int keyspaces_warn_threshold = -1;
    public volatile int keyspaces_fail_threshold = -1;
    @Replaces(oldName = "table_count_warn_threshold", converter = Converters.TABLE_COUNT_THRESHOLD_TO_GUARDRAIL, deprecated = true)
    public volatile int tables_warn_threshold = -1;
    public volatile int tables_fail_threshold = -1;
    public volatile int columns_per_table_warn_threshold = -1;
    public volatile int columns_per_table_fail_threshold = -1;
    public volatile int secondary_indexes_per_table_warn_threshold = -1;
    public volatile int secondary_indexes_per_table_fail_threshold = -1;
    public volatile int materialized_views_per_table_warn_threshold = -1;
    public volatile int materialized_views_per_table_fail_threshold = -1;
    public volatile int page_size_warn_threshold = -1;
    public volatile int page_size_fail_threshold = -1;
    public volatile int partition_keys_in_select_warn_threshold = -1;
    public volatile int partition_keys_in_select_fail_threshold = -1;
    public volatile int in_select_cartesian_product_warn_threshold = -1;
    public volatile int in_select_cartesian_product_fail_threshold = -1;
    public volatile Set<String> table_properties_warned = Collections.emptySet();
    public volatile Set<String> table_properties_ignored = Collections.emptySet();
    public volatile Set<String> table_properties_disallowed = Collections.emptySet();
    public volatile Set<ConsistencyLevel> read_consistency_levels_warned = Collections.emptySet();
    public volatile Set<ConsistencyLevel> read_consistency_levels_disallowed = Collections.emptySet();
    public volatile Set<ConsistencyLevel> write_consistency_levels_warned = Collections.emptySet();
    public volatile Set<ConsistencyLevel> write_consistency_levels_disallowed = Collections.emptySet();
    public volatile boolean user_timestamps_enabled = true;
    public volatile boolean alter_table_enabled = true;
    public volatile boolean group_by_enabled = true;
    public volatile boolean bulk_load_enabled = true;
    public volatile boolean drop_truncate_table_enabled = true;
    public volatile boolean drop_keyspace_enabled = true;
    public volatile boolean secondary_indexes_enabled = true;

    public volatile String default_secondary_index = CassandraIndex.NAME;
    public volatile boolean default_secondary_index_enabled = true;

    public volatile boolean uncompressed_tables_enabled = true;
    public volatile boolean compact_tables_enabled = true;
    public volatile boolean read_before_write_list_operations_enabled = true;
    public volatile boolean allow_filtering_enabled = true;
    public volatile boolean simplestrategy_enabled = true;
    @Replaces(oldName = "compaction_large_partition_warning_threshold_mb", converter = Converters.LONG_BYTES_DATASTORAGE_MEBIBYTES_INT, deprecated = true)
    @Replaces(oldName = "compaction_large_partition_warning_threshold", converter = Converters.LONG_BYTES_DATASTORAGE_MEBIBYTES_DATASTORAGE, deprecated = true)
    public volatile DataStorageSpec.LongBytesBound partition_size_warn_threshold = null;
    public volatile DataStorageSpec.LongBytesBound partition_size_fail_threshold = null;
    @Replaces(oldName = "compaction_tombstone_warning_threshold", converter = Converters.INTEGER_PRIMITIVE_LONG, deprecated = true)
    public volatile long partition_tombstones_warn_threshold = -1;
    public volatile long partition_tombstones_fail_threshold = -1;
    public volatile DataStorageSpec.LongBytesBound column_value_size_warn_threshold = null;
    public volatile DataStorageSpec.LongBytesBound column_value_size_fail_threshold = null;
    public volatile DataStorageSpec.LongBytesBound collection_size_warn_threshold = null;
    public volatile DataStorageSpec.LongBytesBound collection_size_fail_threshold = null;
    public volatile int items_per_collection_warn_threshold = -1;
    public volatile int items_per_collection_fail_threshold = -1;
    public volatile int fields_per_udt_warn_threshold = -1;
    public volatile int fields_per_udt_fail_threshold = -1;
    public volatile int vector_dimensions_warn_threshold = -1;
    public volatile int vector_dimensions_fail_threshold = -1;
    public volatile int data_disk_usage_percentage_warn_threshold = -1;
    public volatile int data_disk_usage_percentage_fail_threshold = -1;
    public volatile DataStorageSpec.LongBytesBound data_disk_usage_max_disk_size = null;
    public volatile int minimum_replication_factor_warn_threshold = -1;
    public volatile int minimum_replication_factor_fail_threshold = -1;
    public volatile int maximum_replication_factor_warn_threshold = -1;
    public volatile int maximum_replication_factor_fail_threshold = -1;
    public volatile boolean zero_ttl_on_twcs_warned = true;
    public volatile boolean zero_ttl_on_twcs_enabled = true;

    public volatile DurationSpec.LongNanosecondsBound streaming_state_expires = new DurationSpec.LongNanosecondsBound("3d");
    public volatile DataStorageSpec.LongBytesBound streaming_state_size = new DataStorageSpec.LongBytesBound("40MiB");

    public volatile boolean streaming_stats_enabled = true;
    public volatile DurationSpec.IntSecondsBound streaming_slow_events_log_timeout = new DurationSpec.IntSecondsBound("10s");

    /** The configuration of startup checks. */
    public volatile Map<StartupCheckType, Map<String, Object>> startup_checks = new HashMap<>();

    public volatile DurationSpec.LongNanosecondsBound repair_state_expires = new DurationSpec.LongNanosecondsBound("3d");
    public volatile int repair_state_size = 100_000;

    /** The configuration of timestamp bounds */
    public volatile DurationSpec.LongMicrosecondsBound maximum_timestamp_warn_threshold = null;
    public volatile DurationSpec.LongMicrosecondsBound maximum_timestamp_fail_threshold = null;
    public volatile DurationSpec.LongMicrosecondsBound minimum_timestamp_warn_threshold = null;
    public volatile DurationSpec.LongMicrosecondsBound minimum_timestamp_fail_threshold = null;

    /**
     * The variants of paxos implementation and semantics supported by Cassandra.
     */
    public enum PaxosVariant
    {
        /**
         * v1 Paxos lacks most optimisations. Expect 4RTs for a write and 2RTs for a read.
         *
         * With legacy semantics for read/read and rejected write linearizability, i.e. not guaranteed.
         */
        v1_without_linearizable_reads_or_rejected_writes,

        /**
         * v1 Paxos lacks most optimisations. Expect 4RTs for a write and 3RTs for a read.
         */
        v1,

        /**
         * v2 Paxos. With PaxosStatePurging.repaired safe to use ANY Commit consistency.
         * Expect 2RTs for a write and 1RT for a read.
         *
         * With legacy semantics for read/read linearizability, i.e. not guaranteed.
         */
        v2_without_linearizable_reads,

        /**
         * v2 Paxos. With PaxosStatePurging.repaired safe to use ANY Commit consistency.
         * Expect 2RTs for a write and 1RT for a read.
         *
         * With legacy semantics for read/read and rejected write linearizability, i.e. not guaranteed.
         */
        v2_without_linearizable_reads_or_rejected_writes,

        /**
         * v2 Paxos. With PaxosStatePurging.repaired safe to use ANY Commit consistency.
         * Expect 2RTs for a write, and either 1RT or 2RT for a read.
         */
        v2
    }

    /**
     * Select the kind of paxos state purging to use. Migration to repaired is recommended, but requires that
     * regular paxos repairs are performed (which by default run as part of incremental repair).
     *
     * Once migrated from legacy it is unsafe to return to legacy, but gc_grace mode may be used in its place
     * and performs a very similar cleanup process.
     *
     * Should only be modified once paxos_variant = v2.
     */
    public enum PaxosStatePurging
    {
        /**
         * system.paxos records are written and garbage collected with TTLs. Unsafe to use with Commit consistency ANY.
         * Once migrated from, cannot be migrated back to safely. Must use gc_grace or repaired instead, as TTLs
         * will not have been set.
         */
        legacy,

        /**
         * Functionally similar to legacy, but the gc_grace expiry is applied at compaction and read time rather than
         * using TTLs, so may be safely enabled at any point.
         */
        gc_grace,

        /**
         * Clears system.paxos records only once they are known to be persisted to a quorum of replica's base tables
         * through the use of paxos repair. Requires that regular paxos repairs are performed on the cluster
         * (which by default are included in incremental repairs if paxos_variant = v2).
         *
         * This setting permits the use of Commit consistency ANY or LOCAL_QUORUM without any loss of durability or consistency,
         * saving 1 RT.
         */
        repaired;

        public static PaxosStatePurging fromBoolean(boolean enabled)
        {
            return enabled ? repaired : gc_grace;
        }
    }

    /**
     * See {@link PaxosVariant}. Defaults to v1, recommend upgrading to v2 at earliest opportunity.
     */
    public volatile PaxosVariant paxos_variant = PaxosVariant.v1;

    /**
     * If true, paxos topology change repair will not run on a topology change - this option should only be used in
     * rare operation circumstances e.g. where for some reason the repair is impossible to perform (e.g. too few replicas)
     * and an unsafe topology change must be made
     */
    public volatile boolean skip_paxos_repair_on_topology_change = SKIP_PAXOS_REPAIR_ON_TOPOLOGY_CHANGE.getBoolean();

    /**
     * A safety margin when purging paxos state information that has been safely replicated to a quorum.
     * Data for transactions initiated within this grace period will not be expunged.
     */
    public volatile DurationSpec.LongSecondsBound paxos_purge_grace_period = new DurationSpec.LongSecondsBound("60s");

    /**
     * A safety mechanism for detecting incorrect paxos state, that may be down either to a bug or incorrect usage of LWTs
     * (most likely due to unsafe mixing of SERIAL and LOCAL_SERIAL operations), and rejecting
     */
    public enum PaxosOnLinearizabilityViolation
    {
        // reject an operation when a linearizability violation is detected.
        // note this does not guarantee a violation has been averted,
        // as it may be a prior operation that invalidated the state.
        fail,
        // log any detected linearizability violation
        log,
        // ignore any detected linearizability violation
        ignore
    }

    /**
     * See {@link PaxosOnLinearizabilityViolation}.
     *
     * Default is to ignore, as applications may readily mix SERIAL and LOCAL_SERIAL and this is the most likely source
     * of linearizability violations. this facility should be activated only for debugging Cassandra or by power users
     * who are investigating their own application behaviour.
     */
    public volatile PaxosOnLinearizabilityViolation paxos_on_linearizability_violations = PaxosOnLinearizabilityViolation.ignore;

    /**
     * See {@link PaxosStatePurging} default is legacy.
     */
    public volatile PaxosStatePurging paxos_state_purging;

    /**
     * Enable/disable paxos repair. This is a global flag that not only determines default behaviour but overrides
     * explicit paxos repair requests, paxos repair on topology changes and paxos auto repairs.
     */
    public volatile boolean paxos_repair_enabled = true;

    /**
     * If true, paxos topology change repair only requires a global quorum of live nodes. If false,
     * it requires a global quorum as well as a local quorum for each dc (EACH_QUORUM), with the
     * exception explained in paxos_topology_repair_strict_each_quorum
     */
    public boolean paxos_topology_repair_no_dc_checks = false;

    /**
     * If true, a quorum will be required for the global and local quorum checks. If false, we will
     * accept a quorum OR n - 1 live nodes. This is to allow for topologies like 2:2:2, where paxos queries
     * always use SERIAL, and a single node down in a dc should not preclude a paxos repair
     */
    public boolean paxos_topology_repair_strict_each_quorum = false;

    /**
     * If necessary for operational purposes, permit certain keyspaces to be ignored for paxos topology repairs
     */
    public volatile Set<String> skip_paxos_repair_on_topology_change_keyspaces = splitCommaDelimited(SKIP_PAXOS_REPAIR_ON_TOPOLOGY_CHANGE_KEYSPACES.getString());

    /**
     * See {@link org.apache.cassandra.service.paxos.ContentionStrategy}
     */
    public String paxos_contention_wait_randomizer;

    /**
     * See {@link org.apache.cassandra.service.paxos.ContentionStrategy}
     */
    public String paxos_contention_min_wait;

    /**
     * See {@link org.apache.cassandra.service.paxos.ContentionStrategy}
     */
    public String paxos_contention_max_wait;

    /**
     * See {@link org.apache.cassandra.service.paxos.ContentionStrategy}
     */
    public String paxos_contention_min_delta;

    /**
     * The number of keys we may simultaneously attempt to finish incomplete paxos operations for.
     */
    public volatile int paxos_repair_parallelism = -1;

    public volatile boolean sstable_read_rate_persistence_enabled = false;

    public volatile boolean client_request_size_metrics_enabled = true;

    public volatile int max_top_size_partition_count = 10;
    public volatile int max_top_tombstone_partition_count = 10;
    public volatile DataStorageSpec.LongBytesBound min_tracked_partition_size = new DataStorageSpec.LongBytesBound("1MiB");
    public volatile long min_tracked_partition_tombstone_count = 5000;
    public volatile boolean top_partitions_enabled = true;

    public final RepairConfig repair = new RepairConfig();

    /**
     * Default compaction configuration, used if a table does not specify any.
     */
    public ParameterizedClass default_compaction = null;

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

    private static final Set<String> SENSITIVE_KEYS = new HashSet<String>() {{
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

    public volatile boolean dump_heap_on_uncaught_exception = false;
    public String heap_dump_path = "heapdump";


    public double severity_during_decommission = 0;

    public StorageCompatibilityMode storage_compatibility_mode = StorageCompatibilityMode.CASSANDRA_4;
}
