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

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

import com.google.common.primitives.Ints;

import org.apache.cassandra.db.virtual.LogMessagesTable;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.service.FileSystemOwnershipCheck;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.StorageCompatibilityMode;

// checkstyle: suppress below 'blockSystemPropertyUsage'

/** A class that extracts system properties for the cassandra node it runs within. */
public enum CassandraRelevantProperties
{
    ACQUIRE_RETRY_SECONDS("cassandra.acquire_retry_seconds", "60"),
    ACQUIRE_SLEEP_MS("cassandra.acquire_sleep_ms", "1000"),
    ALLOCATE_TOKENS_FOR_KEYSPACE("cassandra.allocate_tokens_for_keyspace"),
    ALLOW_ALTER_RF_DURING_RANGE_MOVEMENT("cassandra.allow_alter_rf_during_range_movement"),
    /** If we should allow having duplicate keys in the config file, default to true for legacy reasons */
    ALLOW_DUPLICATE_CONFIG_KEYS("cassandra.allow_duplicate_config_keys", "true"),
    /** If we should allow having both new (post CASSANDRA-15234) and old config keys for the same config item in the yaml */
    ALLOW_NEW_OLD_CONFIG_KEYS("cassandra.allow_new_old_config_keys"),
    ALLOW_UNLIMITED_CONCURRENT_VALIDATIONS("cassandra.allow_unlimited_concurrent_validations"),
    ALLOW_UNSAFE_AGGRESSIVE_SSTABLE_EXPIRATION("cassandra.allow_unsafe_aggressive_sstable_expiration"),
    ALLOW_UNSAFE_JOIN("cassandra.allow_unsafe_join"),
    ALLOW_UNSAFE_REPLACE("cassandra.allow_unsafe_replace"),
    ALLOW_UNSAFE_TRANSIENT_CHANGES("cassandra.allow_unsafe_transient_changes"),
    APPROXIMATE_TIME_PRECISION_MS("cassandra.approximate_time_precision_ms", "2"),
    /** 2 ** GENSALT_LOG2_ROUNDS rounds of hashing will be performed. */
    AUTH_BCRYPT_GENSALT_LOG2_ROUNDS("cassandra.auth_bcrypt_gensalt_log2_rounds"),
    /** We expect default values on cache retries and interval to be sufficient for everyone but have this escape hatch just in case. */
    AUTH_CACHE_WARMING_MAX_RETRIES("cassandra.auth_cache.warming.max_retries"),
    AUTH_CACHE_WARMING_RETRY_INTERVAL_MS("cassandra.auth_cache.warming.retry_interval_ms"),
    AUTOCOMPACTION_ON_STARTUP_ENABLED("cassandra.autocompaction_on_startup_enabled", "true"),
    AUTO_BOOTSTRAP("cassandra.auto_bootstrap"),
    AUTO_REPAIR_FREQUENCY_SECONDS("cassandra.auto_repair_frequency_seconds", convertToString(TimeUnit.MINUTES.toSeconds(5))),
    BATCHLOG_REPLAY_TIMEOUT_IN_MS("cassandra.batchlog.replay_timeout_in_ms"),
    BATCH_COMMIT_LOG_SYNC_INTERVAL("cassandra.batch_commitlog_sync_interval_millis", "1000"),
    /**
     * When bootstraping how long to wait for schema versions to be seen.
     */
    BOOTSTRAP_SCHEMA_DELAY_MS("cassandra.schema_delay_ms"),
    /**
     * When bootstraping we wait for all schema versions found in gossip to be seen, and if not seen in time we fail
     * the bootstrap; this property will avoid failing and allow bootstrap to continue if set to true.
     */
    BOOTSTRAP_SKIP_SCHEMA_CHECK("cassandra.skip_schema_check"),
    BROADCAST_INTERVAL_MS("cassandra.broadcast_interval_ms", "60000"),
    BTREE_BRANCH_SHIFT("cassandra.btree.branchshift", "5"),
    BTREE_FAN_FACTOR("cassandra.btree.fanfactor"),
    /** Represents the maximum size (in bytes) of a serialized mutation that can be cached **/
    CACHEABLE_MUTATION_SIZE_LIMIT("cassandra.cacheable_mutation_size_limit_bytes", convertToString(1_000_000)),
    CASSANDRA_ALLOW_SIMPLE_STRATEGY("cassandra.allow_simplestrategy"),
    CASSANDRA_AVAILABLE_PROCESSORS("cassandra.available_processors"),
    /** The classpath storage configuration file. */
    CASSANDRA_CONFIG("cassandra.config", "cassandra.yaml"),
    /**
     * The cassandra-foreground option will tell CassandraDaemon whether
     * to close stdout/stderr, but it's up to us not to background.
     * yes/null
     */
    CASSANDRA_FOREGROUND("cassandra-foreground"),
    CASSANDRA_JMX_AUTHORIZER("cassandra.jmx.authorizer"),
    CASSANDRA_JMX_LOCAL_PORT("cassandra.jmx.local.port"),
    CASSANDRA_JMX_REMOTE_LOGIN_CONFIG("cassandra.jmx.remote.login.config"),
    /** Cassandra jmx remote and local port */
    CASSANDRA_JMX_REMOTE_PORT("cassandra.jmx.remote.port"),
    CASSANDRA_MAX_HINT_TTL("cassandra.maxHintTTL", convertToString(Integer.MAX_VALUE)),
    CASSANDRA_MINIMUM_REPLICATION_FACTOR("cassandra.minimum_replication_factor"),
    CASSANDRA_NETTY_USE_HEAP_ALLOCATOR("cassandra.netty_use_heap_allocator"),
    CASSANDRA_PID_FILE("cassandra-pidfile"),
    CASSANDRA_RACKDC_PROPERTIES("cassandra-rackdc.properties"),
    CASSANDRA_SKIP_AUTOMATIC_UDT_FIX("cassandra.skipautomaticudtfix"),
    CASSANDRA_STREAMING_DEBUG_STACKTRACE_LIMIT("cassandra.streaming.debug_stacktrace_limit", "2"),
    CASSANDRA_UNSAFE_TIME_UUID_NODE("cassandra.unsafe.timeuuidnode"),
    CASSANDRA_VERSION("cassandra.version"),
    /** default heartbeating period is 1 minute */
    CHECK_DATA_RESURRECTION_HEARTBEAT_PERIOD("check_data_resurrection_heartbeat_period_milli", "60000"),
    CHRONICLE_ANNOUNCER_DISABLE("chronicle.announcer.disable"),
    CLOCK_GLOBAL("cassandra.clock"),
    CLOCK_MONOTONIC_APPROX("cassandra.monotonic_clock.approx"),
    CLOCK_MONOTONIC_PRECISE("cassandra.monotonic_clock.precise"),
    COMMITLOG_ALLOW_IGNORE_SYNC_CRC("cassandra.commitlog.allow_ignore_sync_crc"),
    COMMITLOG_IGNORE_REPLAY_ERRORS("cassandra.commitlog.ignorereplayerrors"),
    COMMITLOG_MAX_OUTSTANDING_REPLAY_BYTES("cassandra.commitlog_max_outstanding_replay_bytes", convertToString(1024 * 1024 * 64)),
    COMMITLOG_MAX_OUTSTANDING_REPLAY_COUNT("cassandra.commitlog_max_outstanding_replay_count", "1024"),
    COMMITLOG_STOP_ON_ERRORS("cassandra.commitlog.stop_on_errors"),
    /**
     * Entities to replay mutations for upon commit log replay, property is meant to contain
     * comma-separated entities which are either names of keyspaces or keyspaces and tables or their mix.
     * Examples:
     * just keyspaces
     * -Dcassandra.replayList=ks1,ks2,ks3
     * specific tables
     * -Dcassandra.replayList=ks1.tb1,ks2.tb2
     * mix of tables and keyspaces
     * -Dcassandra.replayList=ks1.tb1,ks2
     *
     * If only keyspaces are specified, mutations for all tables in such keyspace will be replayed
     * */
    COMMIT_LOG_REPLAY_LIST("cassandra.replayList"),
    /**
     * This property indicates the location for the access file. If com.sun.management.jmxremote.authenticate is false,
     * then this property and the password and access files, are ignored. Otherwise, the access file must exist and
     * be in the valid format. If the access file is empty or nonexistent, then no access is allowed.
     */
    COM_SUN_MANAGEMENT_JMXREMOTE_ACCESS_FILE("com.sun.management.jmxremote.access.file"),
    /**
     * This property indicates whether password authentication for remote monitoring is
     * enabled. By default it is disabled - com.sun.management.jmxremote.authenticate
     */
    COM_SUN_MANAGEMENT_JMXREMOTE_AUTHENTICATE("com.sun.management.jmxremote.authenticate"),
    /** This property indicates the path to the password file - com.sun.management.jmxremote.password.file */
    COM_SUN_MANAGEMENT_JMXREMOTE_PASSWORD_FILE("com.sun.management.jmxremote.password.file"),
    /** Port number to enable JMX RMI connections - com.sun.management.jmxremote.port */
    COM_SUN_MANAGEMENT_JMXREMOTE_PORT("com.sun.management.jmxremote.port"),
    /**
     * The port number to which the RMI connector will be bound - com.sun.management.jmxremote.rmi.port.
     * An Integer object that represents the value of the second argument is returned
     * if there is no port specified, if the port does not have the correct numeric format,
     * or if the specified name is empty or null.
     */
    COM_SUN_MANAGEMENT_JMXREMOTE_RMI_PORT("com.sun.management.jmxremote.rmi.port", "0"),
    /** This property  indicates whether SSL is enabled for monitoring remotely. Default is set to false. */
    COM_SUN_MANAGEMENT_JMXREMOTE_SSL("com.sun.management.jmxremote.ssl"),
    /**
     * A comma-delimited list of SSL/TLS cipher suites to enable.
     * Used in conjunction with com.sun.management.jmxremote.ssl - com.sun.management.jmxremote.ssl.enabled.cipher.suites
     */
    COM_SUN_MANAGEMENT_JMXREMOTE_SSL_ENABLED_CIPHER_SUITES("com.sun.management.jmxremote.ssl.enabled.cipher.suites"),
    /**
     * A comma-delimited list of SSL/TLS protocol versions to enable.
     * Used in conjunction with com.sun.management.jmxremote.ssl - com.sun.management.jmxremote.ssl.enabled.protocols
     */
    COM_SUN_MANAGEMENT_JMXREMOTE_SSL_ENABLED_PROTOCOLS("com.sun.management.jmxremote.ssl.enabled.protocols"),
    /**
     * This property indicates whether SSL client authentication is enabled - com.sun.management.jmxremote.ssl.need.client.auth.
     * Default is set to false.
     */
    COM_SUN_MANAGEMENT_JMXREMOTE_SSL_NEED_CLIENT_AUTH("com.sun.management.jmxremote.ssl.need.client.auth"),
    /** Defaults to false for 4.1 but plan to switch to true in a later release the thinking is that environments
     * may not work right off the bat so safer to add this feature disabled by default */
    CONFIG_ALLOW_SYSTEM_PROPERTIES("cassandra.config.allow_system_properties"),
    CONFIG_LOADER("cassandra.config.loader"),
    CONSISTENT_DIRECTORY_LISTINGS("cassandra.consistent_directory_listings"),
    CONSISTENT_RANGE_MOVEMENT("cassandra.consistent.rangemovement", "true"),
    CONSISTENT_SIMULTANEOUS_MOVES_ALLOW("cassandra.consistent.simultaneousmoves.allow"),
    CRYPTO_PROVIDER_CLASS_NAME("cassandra.crypto_provider_class_name"),
    CUSTOM_GUARDRAILS_CONFIG_PROVIDER_CLASS("cassandra.custom_guardrails_config_provider_class"),
    CUSTOM_QUERY_HANDLER_CLASS("cassandra.custom_query_handler_class"),
    CUSTOM_TRACING_CLASS("cassandra.custom_tracing_class"),
    /** Controls the type of bufffer (heap/direct) used for shared scratch buffers */
    DATA_OUTPUT_BUFFER_ALLOCATE_TYPE("cassandra.dob.allocate_type"),
    DATA_OUTPUT_STREAM_PLUS_TEMP_BUFFER_SIZE("cassandra.data_output_stream_plus_temp_buffer_size", "8192"),
    DECAYING_ESTIMATED_HISTOGRAM_RESERVOIR_STRIPE_COUNT("cassandra.dehr_stripe_count", "2"),
    DEFAULT_PROVIDE_OVERLAPPING_TOMBSTONES("default.provide.overlapping.tombstones"),
    /** determinism properties for testing */
    DETERMINISM_SSTABLE_COMPRESSION_DEFAULT("cassandra.sstable_compression_default", "true"),
    DETERMINISM_UNSAFE_UUID_NODE("cassandra.unsafe.deterministicuuidnode"),
    DIAGNOSTIC_SNAPSHOT_INTERVAL_NANOS("cassandra.diagnostic_snapshot_interval_nanos", "60000000000"),
    DISABLE_AUTH_CACHES_REMOTE_CONFIGURATION("cassandra.disable_auth_caches_remote_configuration"),
    /** properties to disable certain behaviours for testing */
    DISABLE_GOSSIP_ENDPOINT_REMOVAL("cassandra.gossip.disable_endpoint_removal"),
    DISABLE_PAXOS_AUTO_REPAIRS("cassandra.disable_paxos_auto_repairs"),
    DISABLE_PAXOS_STATE_FLUSH("cassandra.disable_paxos_state_flush"),
    DISABLE_SSTABLE_ACTIVITY_TRACKING("cassandra.sstable_activity_tracking", "true"),
    DISABLE_STCS_IN_L0("cassandra.disable_stcs_in_l0"),
    DISABLE_TCACTIVE_OPENSSL("cassandra.disable_tcactive_openssl"),
    /** property for the rate of the scheduled task that monitors disk usage */
    DISK_USAGE_MONITOR_INTERVAL_MS("cassandra.disk_usage.monitor_interval_ms", convertToString(TimeUnit.SECONDS.toMillis(30))),
    /** property for the interval on which the repeated client warnings and diagnostic events about disk usage are ignored */
    DISK_USAGE_NOTIFY_INTERVAL_MS("cassandra.disk_usage.notify_interval_ms", convertToString(TimeUnit.MINUTES.toMillis(30))),
    DOB_DOUBLING_THRESHOLD_MB("cassandra.DOB_DOUBLING_THRESHOLD_MB", "64"),
    DOB_MAX_RECYCLE_BYTES("cassandra.dob_max_recycle_bytes", convertToString(1024 * 1024)),
    /**
     * When draining, how long to wait for mutating executors to shutdown.
     */
    DRAIN_EXECUTOR_TIMEOUT_MS("cassandra.drain_executor_timeout_ms", convertToString(TimeUnit.MINUTES.toMillis(5))),
    DROP_OVERSIZED_READ_REPAIR_MUTATIONS("cassandra.drop_oversized_readrepair_mutations"),
    DTEST_API_LOG_TOPOLOGY("cassandra.dtest.api.log.topology"),
    /** This property indicates if the code is running under the in-jvm dtest framework */
    DTEST_IS_IN_JVM_DTEST("org.apache.cassandra.dtest.is_in_jvm_dtest"),
    ENABLE_DC_LOCAL_COMMIT("cassandra.enable_dc_local_commit", "true"),
    /**
     * Whether {@link org.apache.cassandra.db.ConsistencyLevel#NODE_LOCAL} should be allowed.
     */
    ENABLE_NODELOCAL_QUERIES("cassandra.enable_nodelocal_queries"),
    EXPIRATION_DATE_OVERFLOW_POLICY("cassandra.expiration_date_overflow_policy"),
    EXPIRATION_OVERFLOW_WARNING_INTERVAL_MINUTES("cassandra.expiration_overflow_warning_interval_minutes", "5"),
    FAILURE_LOGGING_INTERVAL_SECONDS("cassandra.request_failure_log_interval_seconds", "60"),
    FAIL_ON_MISSING_CRYPTO_PROVIDER("cassandra.fail_on_missing_crypto_provider", "false"),
    FD_INITIAL_VALUE_MS("cassandra.fd_initial_value_ms"),
    FD_MAX_INTERVAL_MS("cassandra.fd_max_interval_ms"),
    FILE_CACHE_ENABLED("cassandra.file_cache_enabled"),
    /** @deprecated should be removed in favor of enable flag of relevant startup check (FileSystemOwnershipCheck) */
    /** @deprecated See CASSANDRA-17797 */
    @Deprecated(since = "4.1")
    FILE_SYSTEM_CHECK_ENABLE("cassandra.enable_fs_ownership_check"),
    /** @deprecated should be removed in favor of flags in relevant startup check (FileSystemOwnershipCheck) */
    /** @deprecated See CASSANDRA-17797 */
    @Deprecated(since = "4.1")
    FILE_SYSTEM_CHECK_OWNERSHIP_FILENAME("cassandra.fs_ownership_filename", FileSystemOwnershipCheck.DEFAULT_FS_OWNERSHIP_FILENAME),
    /** @deprecated should be removed in favor of flags in relevant startup check (FileSystemOwnershipCheck) */
    /** @deprecated See CASSANDRA-17797 */
    @Deprecated(since = "4.1")
    FILE_SYSTEM_CHECK_OWNERSHIP_TOKEN(FileSystemOwnershipCheck.FILE_SYSTEM_CHECK_OWNERSHIP_TOKEN),
    FORCE_DEFAULT_INDEXING_PAGE_SIZE("cassandra.force_default_indexing_page_size"),
    /** Used when running in Client mode and the system and schema keyspaces need to be initialized outside of their normal initialization path **/
    FORCE_LOAD_LOCAL_KEYSPACES("cassandra.schema.force_load_local_keyspaces"),
    FORCE_PAXOS_STATE_REBUILD("cassandra.force_paxos_state_rebuild"),
    GIT_SHA("cassandra.gitSHA"),
    /**
     * Gossip quarantine delay is used while evaluating membership changes and should only be changed with extreme care.
     */
    GOSSIPER_QUARANTINE_DELAY("cassandra.gossip_quarantine_delay_ms"),
    GOSSIPER_SKIP_WAITING_TO_SETTLE("cassandra.skip_wait_for_gossip_to_settle", "-1"),
    GOSSIP_DISABLE_THREAD_VALIDATION("cassandra.gossip.disable_thread_validation"),

    /**
     * Delay before checking if gossip is settled.
     */
    GOSSIP_SETTLE_MIN_WAIT_MS("cassandra.gossip_settle_min_wait_ms", "5000"),

    /**
     * Interval delay between checking gossip is settled.
     */
    GOSSIP_SETTLE_POLL_INTERVAL_MS("cassandra.gossip_settle_interval_ms", "1000"),

    /**
     * Number of polls without gossip state change to consider gossip as settled.
     */
    GOSSIP_SETTLE_POLL_SUCCESSES_REQUIRED("cassandra.gossip_settle_poll_success_required", "3"),

    IGNORED_SCHEMA_CHECK_ENDPOINTS("cassandra.skip_schema_check_for_endpoints"),
    IGNORED_SCHEMA_CHECK_VERSIONS("cassandra.skip_schema_check_for_versions"),
    IGNORE_CORRUPTED_SCHEMA_TABLES("cassandra.ignore_corrupted_schema_tables"),
    /** @deprecated should be removed in favor of enable flag of relevant startup check (checkDatacenter) */
    /** @deprecated See CASSANDRA-17797 */
    @Deprecated(since = "4.1")
    IGNORE_DC("cassandra.ignore_dc"),
    IGNORE_DYNAMIC_SNITCH_SEVERITY("cassandra.ignore_dynamic_snitch_severity"),

    IGNORE_KERNEL_BUG_1057843_CHECK("cassandra.ignore_kernel_bug_1057843_check"),

    IGNORE_MISSING_NATIVE_FILE_HINTS("cassandra.require_native_file_hints"),
    /** @deprecated should be removed in favor of enable flag of relevant startup check (checkRack) */
    /** @deprecated See CASSANDRA-17797 */
    @Deprecated(since = "4.1")
    IGNORE_RACK("cassandra.ignore_rack"),
    INDEX_SUMMARY_EXPECTED_KEY_SIZE("cassandra.index_summary_expected_key_size", "64"),
    INITIAL_TOKEN("cassandra.initial_token"),
    INTERNODE_EVENT_THREADS("cassandra.internode-event-threads"),
    IO_NETTY_EVENTLOOP_THREADS("io.netty.eventLoopThreads"),
    IO_NETTY_TRANSPORT_ESTIMATE_SIZE_ON_SUBMIT("io.netty.transport.estimateSizeOnSubmit"),
    IO_NETTY_TRANSPORT_NONATIVE("io.netty.transport.noNative"),
    JAVAX_RMI_SSL_CLIENT_ENABLED_CIPHER_SUITES("javax.rmi.ssl.client.enabledCipherSuites"),
    JAVAX_RMI_SSL_CLIENT_ENABLED_PROTOCOLS("javax.rmi.ssl.client.enabledProtocols"),
    /** Java class path. */
    JAVA_CLASS_PATH("java.class.path"),
    JAVA_HOME("java.home"),
    /**
     * Indicates the temporary directory used by the Java Virtual Machine (JVM)
     * to create and store temporary files.
     */
    JAVA_IO_TMPDIR("java.io.tmpdir"),
    /**
     * Path from which to load native libraries.
     * Default is absolute path to lib directory.
     */
    JAVA_LIBRARY_PATH("java.library.path"),
    /**
     * Controls the distributed garbage collector lease time for JMX objects.
     * Should only be set by in-jvm dtests.
     */
    JAVA_RMI_DGC_LEASE_VALUE_IN_JVM_DTEST("java.rmi.dgc.leaseValue"),
    /**
     * The value of this property represents the host name string
     * that should be associated with remote stubs for locally created remote objects,
     * in order to allow clients to invoke methods on the remote object.
     */
    JAVA_RMI_SERVER_HOSTNAME("java.rmi.server.hostname"),
    /**
     * If this value is true, object identifiers for remote objects exported by this VM will be generated by using
     * a cryptographically secure random number generator. The default value is false.
     */
    JAVA_RMI_SERVER_RANDOM_ID("java.rmi.server.randomIDs"),
    JAVA_SECURITY_AUTH_LOGIN_CONFIG("java.security.auth.login.config"),
    JAVA_SECURITY_EGD("java.security.egd"),
    /** Java Runtime Environment version */
    JAVA_VERSION("java.version"),
    /** Java Virtual Machine implementation name */
    JAVA_VM_NAME("java.vm.name"),
    JOIN_RING("cassandra.join_ring", "true"),
    /** startup checks properties */
    LIBJEMALLOC("cassandra.libjemalloc"),
    /** Line separator ("\n" on UNIX). */
    LINE_SEPARATOR("line.separator"),
    /** Load persistence ring state. Default value is {@code true}. */
    LOAD_RING_STATE("cassandra.load_ring_state", "true"),
    LOG4J2_DISABLE_JMX("log4j2.disableJmx"),
    LOG4J2_DISABLE_JMX_LEGACY("log4j2.disable.jmx"),
    LOG4J_SHUTDOWN_HOOK_ENABLED("log4j.shutdownHookEnabled"),
    LOGBACK_CONFIGURATION_FILE("logback.configurationFile"),
    /** Maximum number of rows in system_views.logs table */
    LOGS_VIRTUAL_TABLE_MAX_ROWS("cassandra.virtual.logs.max.rows", convertToString(LogMessagesTable.LOGS_VIRTUAL_TABLE_DEFAULT_ROWS)),
    /**
     * Directory where Cassandra puts its logs, defaults to "." which is current directory.
     */
    LOG_DIR("cassandra.logdir", "."),
    /**
     * Directory where Cassandra persists logs from audit logging. If this property is not set, the audit log framework
     * will set it automatically to {@link CassandraRelevantProperties#LOG_DIR} + "/audit".
     */
    LOG_DIR_AUDIT("cassandra.logdir.audit"),
    /** Loosen the definition of "empty" for gossip state, for use during host replacements if things go awry */
    LOOSE_DEF_OF_EMPTY_ENABLED(Config.PROPERTY_PREFIX + "gossiper.loose_empty_enabled"),
    MAX_CONCURRENT_RANGE_REQUESTS("cassandra.max_concurrent_range_requests"),
    MAX_HINT_BUFFERS("cassandra.MAX_HINT_BUFFERS", "3"),
    MAX_LOCAL_PAUSE_IN_MS("cassandra.max_local_pause_in_ms", "5000"),
    /** what class to use for mbean registeration */
    MBEAN_REGISTRATION_CLASS("org.apache.cassandra.mbean_registration_class"),
    MEMTABLE_OVERHEAD_COMPUTE_STEPS("cassandra.memtable_row_overhead_computation_step", "100000"),
    MEMTABLE_OVERHEAD_SIZE("cassandra.memtable.row_overhead_size", "-1"),
    MEMTABLE_SHARD_COUNT("cassandra.memtable.shard.count"),
    MEMTABLE_TRIE_SIZE_LIMIT("cassandra.trie_size_limit_mb"),
    MIGRATION_DELAY("cassandra.migration_delay_ms", "60000"),
    /** Defines the maximum number of unique timed out queries that will be reported in the logs. Use a negative number to remove any limit. */
    MONITORING_MAX_OPERATIONS("cassandra.monitoring_max_operations", "50"),
    /** Defines the interval for reporting any operations that have timed out. */
    MONITORING_REPORT_INTERVAL_MS("cassandra.monitoring_report_interval_ms", "5000"),
    MV_ALLOW_FILTERING_NONKEY_COLUMNS_UNSAFE("cassandra.mv.allow_filtering_nonkey_columns_unsafe"),
    MV_ENABLE_COORDINATOR_BATCHLOG("cassandra.mv_enable_coordinator_batchlog"),
    /** mx4jaddress */
    MX4JADDRESS("mx4jaddress"),
    /** mx4jport */
    MX4JPORT("mx4jport"),
    NANOTIMETOMILLIS_TIMESTAMP_UPDATE_INTERVAL("cassandra.NANOTIMETOMILLIS_TIMESTAMP_UPDATE_INTERVAL", "10000"),
    NATIVE_EPOLL_ENABLED("cassandra.native.epoll.enabled", "true"),
    /** This is the port used with RPC address for the native protocol to communicate with clients. Now that thrift RPC is no longer in use there is no RPC port. */
    NATIVE_TRANSPORT_PORT("cassandra.native_transport_port"),
    NEVER_PURGE_TOMBSTONES("cassandra.never_purge_tombstones"),
    NIO_DATA_OUTPUT_STREAM_PLUS_BUFFER_SIZE("cassandra.nio_data_output_stream_plus_buffer_size", convertToString(32 * 1024)),
    NODETOOL_JMX_NOTIFICATION_POLL_INTERVAL_SECONDS("cassandra.nodetool.jmx_notification_poll_interval_seconds", convertToString(TimeUnit.SECONDS.convert(5, TimeUnit.MINUTES))),
    /** If set, {@link org.apache.cassandra.net.MessagingService} is shutdown abrtuptly without waiting for anything.
     * This is an optimization used in unit tests becuase we never restart a node there. The only node is stopoped
     * when the JVM terminates. Therefore, we can use such optimization and not wait unnecessarily. */
    NON_GRACEFUL_SHUTDOWN("cassandra.test.messagingService.nonGracefulShutdown"),
    /** for specific tests */
    /** This property indicates whether disable_mbean_registration is true */
    ORG_APACHE_CASSANDRA_DISABLE_MBEAN_REGISTRATION("org.apache.cassandra.disable_mbean_registration"),
    /** Operating system architecture. */
    OS_ARCH("os.arch"),
    /** Operating system name. */
    OS_NAME("os.name"),
    OTCP_LARGE_MESSAGE_THRESHOLD("cassandra.otcp_large_message_threshold", convertToString(1024 * 64)),
    /** Enabled/disable TCP_NODELAY for intradc connections. Defaults is enabled. */
    OTC_INTRADC_TCP_NODELAY("cassandra.otc_intradc_tcp_nodelay", "true"),
    OVERRIDE_DECOMMISSION("cassandra.override_decommission"),
    PARENT_REPAIR_STATUS_CACHE_SIZE("cassandra.parent_repair_status_cache_size", "100000"),
    PARENT_REPAIR_STATUS_EXPIRY_SECONDS("cassandra.parent_repair_status_expiry_seconds", convertToString(TimeUnit.SECONDS.convert(1, TimeUnit.DAYS))),
    PARTITIONER("cassandra.partitioner"),
    PAXOS_CLEANUP_SESSION_TIMEOUT_SECONDS("cassandra.paxos_cleanup_session_timeout_seconds", convertToString(TimeUnit.HOURS.toSeconds(2))),
    PAXOS_DISABLE_COORDINATOR_LOCKING("cassandra.paxos.disable_coordinator_locking"),
    PAXOS_LOG_TTL_LINEARIZABILITY_VIOLATIONS("cassandra.paxos.log_ttl_linearizability_violations", "true"),
    PAXOS_MODERN_RELEASE("cassandra.paxos.modern_release", "4.1"),
    PAXOS_REPAIR_ALLOW_MULTIPLE_PENDING_UNSAFE("cassandra.paxos_repair_allow_multiple_pending_unsafe"),
    PAXOS_REPAIR_ON_TOPOLOGY_CHANGE_RETRIES("cassandra.paxos_repair_on_topology_change_retries", "10"),
    PAXOS_REPAIR_ON_TOPOLOGY_CHANGE_RETRY_DELAY_SECONDS("cassandra.paxos_repair_on_topology_change_retry_delay_seconds", "10"),
    PAXOS_REPAIR_RETRY_TIMEOUT_IN_MS("cassandra.paxos_repair_retry_timeout_millis", "60000"),
    PAXOS_USE_SELF_EXECUTION("cassandra.paxos.use_self_execution", "true"),
    PRINT_HEAP_HISTOGRAM_ON_OUT_OF_MEMORY_ERROR("cassandra.printHeapHistogramOnOutOfMemoryError"),
    READS_THRESHOLDS_COORDINATOR_DEFENSIVE_CHECKS_ENABLED("cassandra.reads.thresholds.coordinator.defensive_checks_enabled"),
    RELEASE_VERSION("cassandra.releaseVersion"),
    REPAIR_CLEANUP_INTERVAL_SECONDS("cassandra.repair_cleanup_interval_seconds", convertToString(Ints.checkedCast(TimeUnit.MINUTES.toSeconds(10)))),
    REPAIR_DELETE_TIMEOUT_SECONDS("cassandra.repair_delete_timeout_seconds", convertToString(Ints.checkedCast(TimeUnit.DAYS.toSeconds(1)))),
    REPAIR_FAIL_TIMEOUT_SECONDS("cassandra.repair_fail_timeout_seconds", convertToString(Ints.checkedCast(TimeUnit.DAYS.toSeconds(1)))),
    REPAIR_MUTATION_REPAIR_ROWS_PER_BATCH("cassandra.repair.mutation_repair_rows_per_batch", "100"),
    REPAIR_STATUS_CHECK_TIMEOUT_SECONDS("cassandra.repair_status_check_timeout_seconds", convertToString(Ints.checkedCast(TimeUnit.HOURS.toSeconds(1)))),
    /**
     * When doing a host replacement its possible that the gossip state is "empty" meaning that the endpoint is known
     * but the current state isn't known.  If the host replacement is needed to repair this state, this property must
     * be true.
     */
    REPLACEMENT_ALLOW_EMPTY("cassandra.allow_empty_replace_address", "true"),
    REPLACE_ADDRESS("cassandra.replace_address"),
    REPLACE_ADDRESS_FIRST_BOOT("cassandra.replace_address_first_boot"),
    REPLACE_NODE("cassandra.replace_node"),
    REPLACE_TOKEN("cassandra.replace_token"),
    /**
     * Whether we reset any found data from previously run bootstraps.
     */
    RESET_BOOTSTRAP_PROGRESS("cassandra.reset_bootstrap_progress"),
    RING_DELAY("cassandra.ring_delay_ms"),

    // SAI specific properties

    /** Controls the maximum number of index query intersections that will take part in a query */
    SAI_INTERSECTION_CLAUSE_LIMIT("cassandra.sai.intersection_clause_limit", "2"),
    /** Latest version to be used for SAI index writing */
    SAI_LATEST_VERSION("cassandra.sai.latest_version", "aa"),
    SAI_MAX_FROZEN_TERM_SIZE("cassandra.sai.max_frozen_term_size", "5KiB"),
    SAI_MAX_STRING_TERM_SIZE("cassandra.sai.max_string_term_size", "1KiB"),
    SAI_MAX_VECTOR_TERM_SIZE("cassandra.sai.max_vector_term_size", "32KiB"),

    /** Minimum number of reachable leaves for a given node to be eligible for an auxiliary posting list */
    SAI_MINIMUM_POSTINGS_LEAVES("cassandra.sai.minimum_postings_leaves", "64"),

    /**
     * Skip, or the sampling interval, for selecting a balanced tree level that is eligible for an auxiliary posting list.
     * Sampling starts from 0, but balanced tree root node is at level 1. For skip = 4, eligible levels are 4, 8, 12, etc. (no
     * level 0, because there is no node at level 0).
     */
    SAI_POSTINGS_SKIP("cassandra.sai.postings_skip", "3"),

    /**
     * Used to determine the block size and block mask for the clustering sorted terms.
     */
    SAI_SORTED_TERMS_CLUSTERING_BLOCK_SHIFT("cassandra.sai.sorted_terms_clustering_block_shift", "4"),

    /**
     * Used to determine the block size and block mask for the partition sorted terms.
     */
    SAI_SORTED_TERMS_PARTITION_BLOCK_SHIFT("cassandra.sai.sorted_terms_partition_block_shift", "4"),

    SAI_TEST_BALANCED_TREE_DEBUG_ENABLED("cassandra.sai.test.balanced_tree_debug_enabled", "false"),
    SAI_TEST_DISABLE_TIMEOUT("cassandra.sai.test.timeout_disabled", "false"),

    /** Whether to allow the user to specify custom options to the hnsw index */
    SAI_VECTOR_ALLOW_CUSTOM_PARAMETERS("cassandra.sai.vector.allow_custom_parameters", "false"),

    /** Controls the maximum top-k limit for vector search */
    SAI_VECTOR_SEARCH_MAX_TOP_K("cassandra.sai.vector_search.max_top_k", "1000"),

    /**
     * Controls the maximum number of PrimaryKeys that will be read into memory at one time when ordering/limiting
     * the results of an ANN query constrained by non-ANN predicates.
     */
    SAI_VECTOR_SEARCH_ORDER_CHUNK_SIZE("cassandra.sai.vector_search.order_chunk_size", "100000"),

    SCHEMA_PULL_INTERVAL_MS("cassandra.schema_pull_interval_ms", "60000"),
    SCHEMA_UPDATE_HANDLER_FACTORY_CLASS("cassandra.schema.update_handler_factory.class"),
    SEARCH_CONCURRENCY_FACTOR("cassandra.search_concurrency_factor", "1"),

    /**
     * The maximum number of seeds returned by a seed provider before emmitting a warning.
     * A large seed list may impact effectiveness of the third gossip round.
     * The default used in SimpleSeedProvider is 20.
     */
    SEED_COUNT_WARN_THRESHOLD("cassandra.seed_count_warn_threshold"),
    SERIALIZATION_EMPTY_TYPE_NONEMPTY_BEHAVIOR("cassandra.serialization.emptytype.nonempty_behavior"),
    SET_SEP_THREAD_NAME("cassandra.set_sep_thread_name", "true"),
    SHUTDOWN_ANNOUNCE_DELAY_IN_MS("cassandra.shutdown_announce_in_ms", "2000"),
    SIZE_RECORDER_INTERVAL("cassandra.size_recorder_interval", "300"),
    SKIP_PAXOS_REPAIR_ON_TOPOLOGY_CHANGE("cassandra.skip_paxos_repair_on_topology_change"),
    /** If necessary for operational purposes, permit certain keyspaces to be ignored for paxos topology repairs. */
    SKIP_PAXOS_REPAIR_ON_TOPOLOGY_CHANGE_KEYSPACES("cassandra.skip_paxos_repair_on_topology_change_keyspaces"),
    SKIP_PAXOS_REPAIR_VERSION_VALIDATION("cassandra.skip_paxos_repair_version_validation"),
    SKIP_PAXOS_STATE_REBUILD("cassandra.skip_paxos_state_rebuild"),
    /** snapshots ttl cleanup initial delay in seconds */
    SNAPSHOT_CLEANUP_INITIAL_DELAY_SECONDS("cassandra.snapshot.ttl_cleanup_initial_delay_seconds", "5"),
    /** snapshots ttl cleanup period in seconds */
    SNAPSHOT_CLEANUP_PERIOD_SECONDS("cassandra.snapshot.ttl_cleanup_period_seconds", "60"),
    /** minimum allowed TTL for snapshots */
    SNAPSHOT_MIN_ALLOWED_TTL_SECONDS("cassandra.snapshot.min_allowed_ttl_seconds", "60"),
    SSL_ENABLE("ssl.enable"),
    SSL_STORAGE_PORT("cassandra.ssl_storage_port"),
    SSTABLE_FORMAT_DEFAULT("cassandra.sstable.format.default"),
    START_GOSSIP("cassandra.start_gossip", "true"),
    START_NATIVE_TRANSPORT("cassandra.start_native_transport"),
    STORAGE_DIR("cassandra.storagedir"),
    STORAGE_HOOK("cassandra.storage_hook"),
    STORAGE_PORT("cassandra.storage_port"),
    STREAMING_HISTOGRAM_ROUND_SECONDS("cassandra.streaminghistogram.roundseconds", "60"),
    STREAMING_SESSION_PARALLELTRANSFERS("cassandra.streaming.session.parallelTransfers"),
    STREAM_HOOK("cassandra.stream_hook"),
    /** Platform word size sun.arch.data.model. Examples: "32", "64", "unknown"*/
    SUN_ARCH_DATA_MODEL("sun.arch.data.model"),
    SUN_JAVA_COMMAND("sun.java.command", ""),
    /**
     * Controls the JMX server threadpool keap-alive time.
     * Should only be set by in-jvm dtests.
     */
    SUN_RMI_TRANSPORT_TCP_THREADKEEPALIVETIME("sun.rmi.transport.tcp.threadKeepAliveTime"),
    SUN_STDERR_ENCODING("sun.stderr.encoding"),
    SUN_STDOUT_ENCODING("sun.stdout.encoding"),
    SUPERUSER_SETUP_DELAY_MS("cassandra.superuser_setup_delay_ms", "10000"),
    SYSTEM_AUTH_DEFAULT_RF("cassandra.system_auth.default_rf", "1"),
    SYSTEM_DISTRIBUTED_DEFAULT_RF("cassandra.system_distributed.default_rf", "3"),
    SYSTEM_TRACES_DEFAULT_RF("cassandra.system_traces.default_rf", "2"),
    TEST_BBFAILHELPER_ENABLED("test.bbfailhelper.enabled"),
    TEST_BLOB_SHARED_SEED("cassandra.test.blob.shared.seed"),
    TEST_BYTEMAN_TRANSFORMATIONS_DEBUG("cassandra.test.byteman.transformations.debug"),
    TEST_CASSANDRA_KEEPBRIEFBRIEF("cassandra.keepBriefBrief"),
    TEST_CASSANDRA_RELEVANT_PROPERTIES("org.apache.cassandra.conf.CassandraRelevantPropertiesTest"),
    /** A property for various mechanisms for syncing files that makes it possible it intercept and skip syncing. */
    TEST_CASSANDRA_SKIP_SYNC("cassandra.skip_sync"),
    TEST_CASSANDRA_SUITENAME("suitename", "suitename_IS_UNDEFINED"),
    TEST_CASSANDRA_TESTTAG("cassandra.testtag", "cassandra.testtag_IS_UNDEFINED"),
    TEST_COMPRESSION("cassandra.test.compression"),
    TEST_COMPRESSION_ALGO("cassandra.test.compression.algo", "lz4"),
    TEST_DEBUG_REF_COUNT("cassandra.debugrefcount"),
    TEST_DRIVER_CONNECTION_TIMEOUT_MS("cassandra.test.driver.connection_timeout_ms", "5000"),
    TEST_DRIVER_READ_TIMEOUT_MS("cassandra.test.driver.read_timeout_ms", "12000"),
    TEST_ENCRYPTION("cassandra.test.encryption", "false"),
    TEST_FAIL_MV_LOCKS_COUNT("cassandra.test.fail_mv_locks_count", "0"),
    TEST_FAIL_WRITES_KS("cassandra.test.fail_writes_ks", ""),
    /** Flush changes of {@link org.apache.cassandra.schema.SchemaKeyspace} after each schema modification. In production,
     * we always do that. However, tests which do not restart nodes may disable this functionality in order to run
     * faster. Note that this is disabled for unit tests but if an individual test requires schema to be flushed, it
     * can be also done manually for that particular case: {@code flush(SchemaConstants.SCHEMA_KEYSPACE_NAME);}. */
    TEST_FLUSH_LOCAL_SCHEMA_CHANGES("cassandra.test.flush_local_schema_changes", "true"),
    TEST_IGNORE_SIGAR("cassandra.test.ignore_sigar"),
    TEST_INVALID_LEGACY_SSTABLE_ROOT("invalid-legacy-sstable-root"),
    TEST_JVM_DTEST_DISABLE_SSL("cassandra.test.disable_ssl"),
    TEST_LEGACY_SSTABLE_ROOT("legacy-sstable-root"),
    TEST_ORG_CAFFINITAS_OHC_SEGMENTCOUNT("org.caffinitas.ohc.segmentCount"),
    TEST_RANDOM_SEED("cassandra.test.random.seed"),
    TEST_READ_ITERATION_DELAY_MS("cassandra.test.read_iteration_delay_ms", "0"),
    TEST_REUSE_PREPARED("cassandra.test.reuse_prepared", "true"),
    TEST_ROW_CACHE_SIZE("cassandra.test.row_cache_size"),
    TEST_SERIALIZATION_WRITES("cassandra.test-serialization-writes"),
    TEST_SIMULATOR_DEBUG("cassandra.test.simulator.debug"),
    TEST_SIMULATOR_DETERMINISM_CHECK("cassandra.test.simulator.determinismcheck", "none"),
    TEST_SIMULATOR_LIVENESS_CHECK("cassandra.test.simulator.livenesscheck", "true"),
    /** properties for debugging simulator ASM output */
    TEST_SIMULATOR_PRINT_ASM("cassandra.test.simulator.print_asm", "none"),
    TEST_SIMULATOR_PRINT_ASM_CLASSES("cassandra.test.simulator.print_asm_classes", ""),
    TEST_SIMULATOR_PRINT_ASM_OPTS("cassandra.test.simulator.print_asm_opts", ""),
    TEST_SIMULATOR_PRINT_ASM_TYPES("cassandra.test.simulator.print_asm_types", ""),
    TEST_SKIP_CRYPTO_PROVIDER_INSTALLATION("cassandra.test.security.skip.provider.installation", "false"),
    TEST_SSTABLE_FORMAT_DEVELOPMENT("cassandra.test.sstableformatdevelopment"),
    /**
     * {@link StorageCompatibilityMode} mode sets how the node will behave, sstable or messaging versions to use etc.
     * according to a yaml setting. But many tests don't load the config hence we need to force it otherwise they would
     * run always under the default. Config is null for junits that don't load the config. Get from env var that
     * CI/build.xml sets.
     *
     * This is a dev/CI only property. Do not use otherwise.
     */
    TEST_STORAGE_COMPATIBILITY_MODE("cassandra.test.storage_compatibility_mode", StorageCompatibilityMode.CASSANDRA_4.toString()),
    TEST_STRICT_LCS_CHECKS("cassandra.test.strict_lcs_checks"),
    /** Turns some warnings into exceptions for testing. */
    TEST_STRICT_RUNTIME_CHECKS("cassandra.strict.runtime.checks"),
    /** Not to be used in production, this causes a Netty logging handler to be added to the pipeline, which will throttle a system under any normal load. */
    TEST_UNSAFE_VERBOSE_DEBUG_CLIENT_PROTOCOL("cassandra.unsafe_verbose_debug_client_protocol"),
    TEST_USE_PREPARED("cassandra.test.use_prepared", "true"),
    TEST_UTIL_ALLOW_TOOL_REINIT_FOR_TEST("org.apache.cassandra.tools.UtilALLOW_TOOL_REINIT_FOR_TEST"),
    /** Activate write survey mode. The node not becoming an active ring member, and you must use JMX StorageService->joinRing() to finalize the ring joining. */
    TEST_WRITE_SURVEY("cassandra.write_survey"),
    TOLERATE_SSTABLE_SIZE("cassandra.tolerate_sstable_size"),
    TRIGGERS_DIR("cassandra.triggers_dir"),
    TRUNCATE_BALLOT_METADATA("cassandra.truncate_ballot_metadata"),
    TYPE_UDT_CONFLICT_BEHAVIOR("cassandra.type.udt.conflict_behavior"),
    // See org.apache.cassandra.db.compaction.unified.Controller for the definition of the UCS parameters
    UCS_BASE_SHARD_COUNT("unified_compaction.base_shard_count", "4"),
    UCS_MIN_SSTABLE_SIZE("unified_compaction.min_sstable_size", "100MiB"),
    UCS_OVERLAP_INCLUSION_METHOD("unified_compaction.overlap_inclusion_method"),
    UCS_SCALING_PARAMETER("unified_compaction.scaling_parameters", "T4"),
    UCS_SSTABLE_GROWTH("unified_compaction.sstable_growth", "0.333"),
    UCS_SURVIVAL_FACTOR("unified_compaction.survival_factor", "1"),
    UCS_TARGET_SSTABLE_SIZE("unified_compaction.target_sstable_size", "1GiB"),
    UDF_EXECUTOR_THREAD_KEEPALIVE_MS("cassandra.udf_executor_thread_keepalive_ms", "30000"),
    UNSAFE_SYSTEM("cassandra.unsafesystem"),
    /** User's home directory. */
    USER_HOME("user.home"),
    /** When enabled, recursive directory deletion will be executed using a unix command `rm -rf` instead of traversing
     * and removing individual files. This is now used only tests, but eventually we will make it true by default.*/
    USE_NIX_RECURSIVE_DELETE("cassandra.use_nix_recursive_delete"),
    /** Gossiper compute expiration timeout. Default value 3 days. */
    VERY_LONG_TIME_MS("cassandra.very_long_time_ms", "259200000"),
    WAIT_FOR_TRACING_EVENTS_TIMEOUT_SECS("cassandra.wait_for_tracing_events_timeout_secs", "0");

    static
    {
        CassandraRelevantProperties[] values = CassandraRelevantProperties.values();
        Set<String> visited = new HashSet<>(values.length);
        CassandraRelevantProperties prev = null;
        for (CassandraRelevantProperties next : values)
        {
            if (!visited.add(next.getKey()))
                throw new IllegalStateException("System properties have duplicate key: " + next.getKey());
            if (prev != null && next.name().compareTo(prev.name()) < 0)
                throw new IllegalStateException("Enum constants are not in alphabetical order: " + prev.name() + " should come before " + next.name());
            else
                prev = next;
        }
    }

    CassandraRelevantProperties(String key, String defaultVal)
    {
        this.key = key;
        this.defaultVal = defaultVal;
    }

    CassandraRelevantProperties(String key)
    {
        this.key = key;
        this.defaultVal = null;
    }

    private final String key;
    private final String defaultVal;

    public String getKey()
    {
        return key;
    }

    /**
     * Gets the value of the indicated system property.
     * @return system property value if it exists, defaultValue otherwise.
     */
    public String getString()
    {
        String value = System.getProperty(key);

        return value == null ? defaultVal : STRING_CONVERTER.convert(value);
    }

    /**
     * Returns default value.
     *
     * @return default value, if any, otherwise null.
     */
    public String getDefaultValue()
    {
        return defaultVal;
    }

    /**
     * Sets the property to its default value if a default value was specified. Remove the property otherwise.
     */
    public void reset()
    {
        if (defaultVal != null)
            System.setProperty(key, defaultVal);
        else
            System.getProperties().remove(key);
    }

    /**
     * Gets the value of a system property as a String.
     * @return system property String value if it exists, overrideDefaultValue otherwise.
     */
    public String getString(String overrideDefaultValue)
    {
        String value = System.getProperty(key);
        if (value == null)
            return overrideDefaultValue;

        return STRING_CONVERTER.convert(value);
    }

    public <T> T convert(PropertyConverter<T> converter)
    {
        String value = System.getProperty(key);
        if (value == null)
            value = defaultVal;

        return converter.convert(value);
    }

    public static String convertToString(@Nullable Object value)
    {
        if (value == null)
            return null;
        if (value instanceof String)
            return (String) value;
        if (value instanceof Boolean)
            return Boolean.toString((Boolean) value);
        if (value instanceof Long)
            return Long.toString((Long) value);
        if (value instanceof Integer)
            return Integer.toString((Integer) value);
        if (value instanceof Double)
            return Double.toString((Double) value);
        if (value instanceof Float)
            return Float.toString((Float) value);
        throw new IllegalArgumentException("Unknown type " + value.getClass());
    }

    /**
     * Sets the value into system properties.
     * @param value to set
     */
    public String setString(String value)
    {
        return System.setProperty(key, value);
    }

    /**
     * Gets the value of a system property as a boolean.
     * @return system property boolean value if it exists, false otherwise().
     */
    public boolean getBoolean()
    {
        String value = System.getProperty(key);

        return BOOLEAN_CONVERTER.convert(value == null ? defaultVal : value);
    }

    /**
     * Gets the value of a system property as a boolean.
     * @return system property boolean value if it exists, overrideDefaultValue otherwise.
     */
    public boolean getBoolean(boolean overrideDefaultValue)
    {
        String value = System.getProperty(key);
        if (value == null)
            return overrideDefaultValue;

        return BOOLEAN_CONVERTER.convert(value);
    }

    /**
     * Sets the value into system properties.
     * @param value to set
     * @return Previous value if it exists.
     */
    public Boolean setBoolean(boolean value)
    {
        String prev = System.setProperty(key, convertToString(value));
        return prev == null ? null : BOOLEAN_CONVERTER.convert(prev);
    }

    /**
     * Clears the value set in the system property.
     */
    public void clearValue()
    {
        System.clearProperty(key);
    }

    /**
     * Gets the value of a system property as a int.
     * @return System property value if it exists, defaultValue otherwise. Throws an exception if no default value is set.
     */
    public int getInt()
    {
        String value = System.getProperty(key);
        if (value == null && defaultVal == null)
            throw new ConfigurationException("Missing property value or default value is not set: " + key);
        return INTEGER_CONVERTER.convert(value == null ? defaultVal : value);
    }

    /**
     * Gets the value of a system property as a long.
     * @return System property value if it exists, defaultValue otherwise. Throws an exception if no default value is set.
     */
    public long getLong()
    {
        String value = System.getProperty(key);
        if (value == null && defaultVal == null)
            throw new ConfigurationException("Missing property value or default value is not set: " + key);
        return LONG_CONVERTER.convert(value == null ? defaultVal : value);
    }

    /**
     * Gets the value of a system property as a long.
     * @return system property long value if it exists, defaultValue otherwise.
     */
    public long getLong(long overrideDefaultValue)
    {
        String value = System.getProperty(key);
        if (value == null)
            return overrideDefaultValue;

        return LONG_CONVERTER.convert(value);
    }

    /**
     * Gets the value of a system property as a double.
     * @return System property value if it exists, defaultValue otherwise. Throws an exception if no default value is set.
     */
    public double getDouble()
    {
        String value = System.getProperty(key);
        if (value == null && defaultVal == null)
            throw new ConfigurationException("Missing property value or default value is not set: " + key);
        return DOUBLE_CONVERTER.convert(value == null ? defaultVal : value);
    }

    /**
     * Gets the value of a system property as a double.
     * @return system property value if it exists, defaultValue otherwise.
     */
    public double getDouble(double overrideDefaultValue)
    {
        String value = System.getProperty(key);
        if (value == null)
            return overrideDefaultValue;

        return DOUBLE_CONVERTER.convert(value);
    }

    /**
     * Gets the value of a system property, given as a human-readable size in bytes (e.g. 100MiB, 10GB, 500B).
     * @return System property value if it exists, defaultValue otherwise. Throws an exception if no default value is set.
     */
    public long getSizeInBytes()
    {
        String value = System.getProperty(key);
        if (value == null && defaultVal == null)
            throw new ConfigurationException("Missing property value or default value is not set: " + key);
        return SIZE_IN_BYTES_CONVERTER.convert(value == null ? defaultVal : value);
    }

    /**
     * Gets the value of a system property, given as a human-readable size in bytes (e.g. 100MiB, 10GB, 500B).
     * @return System property value if it exists, defaultValue otherwise.
     */
    public long getSizeInBytes(long overrideDefaultValue)
    {
        String value = System.getProperty(key);
        if (value == null)
            return overrideDefaultValue;

        return SIZE_IN_BYTES_CONVERTER.convert(value);
    }

    /**
     * Gets the value of a system property as an int.
     * @return system property int value if it exists, overrideDefaultValue otherwise.
     */
    public int getInt(int overrideDefaultValue)
    {
        String value = System.getProperty(key);
        if (value == null)
            return overrideDefaultValue;

        return INTEGER_CONVERTER.convert(value);
    }

    /**
     * Sets the value into system properties.
     * @param value to set
     * @return Previous value or null if it did not have one.
     */
    public Integer setInt(int value)
    {
        String prev = System.setProperty(key, convertToString(value));
        return prev == null ? null : INTEGER_CONVERTER.convert(prev);
    }

    /**
     * Sets the value into system properties.
     * @param value to set
     * @return Previous value or null if it did not have one.
     */
    public Long setLong(long value)
    {
        String prev = System.setProperty(key, convertToString(value));
        return prev == null ? null : LONG_CONVERTER.convert(prev);
    }

    /**
     * Gets the value of a system property as a enum, calling {@link String#toUpperCase()} first.
     *
     * @param defaultValue to return when not defined
     * @param <T> type
     * @return enum value
     */
    public <T extends Enum<T>> T getEnum(T defaultValue)
    {
        return getEnum(true, defaultValue);
    }

    /**
     * Gets the value of a system property as a enum, optionally calling {@link String#toUpperCase()} first.
     *
     * @param toUppercase before converting to enum
     * @param defaultValue to return when not defined
     * @param <T> type
     * @return enum value
     */
    public <T extends Enum<T>> T getEnum(boolean toUppercase, T defaultValue)
    {
        String value = System.getProperty(key);
        if (value == null)
            return defaultValue;
        return Enum.valueOf(defaultValue.getDeclaringClass(), toUppercase ? value.toUpperCase() : value);
    }

    /**
     * Gets the value of a system property as an enum, optionally calling {@link String#toUpperCase()} first.
     * If the value is missing, the default value for this property is used
     *
     * @param toUppercase before converting to enum
     * @param enumClass enumeration class
     * @param <T> type
     * @return enum value
     */
    public <T extends Enum<T>> T getEnum(boolean toUppercase, Class<T> enumClass)
    {
        String value = System.getProperty(key, defaultVal);
        return Enum.valueOf(enumClass, toUppercase ? value.toUpperCase() : value);
    }

    /**
     * Sets the value into system properties.
     * @param value to set
     */
    public void setEnum(Enum<?> value)
    {
        System.setProperty(key, value.name());
    }

    public interface PropertyConverter<T>
    {
        T convert(String value);
    }

    private static final PropertyConverter<String> STRING_CONVERTER = value -> value;

    private static final PropertyConverter<Boolean> BOOLEAN_CONVERTER = Boolean::parseBoolean;

    private static final PropertyConverter<Integer> INTEGER_CONVERTER = value ->
    {
        try
        {
            return Integer.decode(value);
        }
        catch (NumberFormatException e)
        {
            throw new ConfigurationException(String.format("Invalid value for system property: " +
                                                           "expected integer value but got '%s'", value));
        }
    };

    private static final PropertyConverter<Long> LONG_CONVERTER = value ->
    {
        try
        {
            return Long.decode(value);
        }
        catch (NumberFormatException e)
        {
            throw new ConfigurationException(String.format("Invalid value for system property: " +
                                                           "expected long value but got '%s'", value));
        }
    };

    private static final PropertyConverter<Long> SIZE_IN_BYTES_CONVERTER = value ->
    {
        try
        {
            return FBUtilities.parseHumanReadableBytes(value);
        }
        catch (ConfigurationException e)
        {
            throw new ConfigurationException(String.format("Invalid value for system property: " +
                                                           "expected size in bytes with unit but got '%s'\n%s", value, e));
        }
    };

    private static final PropertyConverter<Double> DOUBLE_CONVERTER = value ->
    {
        try
        {
            return Double.parseDouble(value);
        }
        catch (NumberFormatException e)
        {
            throw new ConfigurationException(String.format("Invalid value for system property: " +
                                                           "expected floating point value but got '%s'", value));
        }
    };

    /**
     * @return whether a system property is present or not.
     */
    public boolean isPresent()
    {
        return System.getProperties().containsKey(key);
    }
}
