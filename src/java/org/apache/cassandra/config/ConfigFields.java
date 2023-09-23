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

/**
 * This class is created by {@link org.apache.cassandra.utils.ConstantFieldsGenerateUtil} based on provided
 * the {@link org.apache.cassandra.config.Config} class. It contains non-private non-static {@code Cofig}'s fields
 * marked with the {@link org.apache.cassandra.config.Mutable} annotation to expose them to public APIs.

 * @see org.apache.cassandra.config.Mutable
 * @see org.apache.cassandra.config.Config
 */
public class ConfigFields
{
    public static final String AUTO_HINTS_CLEANUP_ENABLED = "auto_hints_cleanup_enabled";
    public static final String CAS_CONTENTION_TIMEOUT = "cas_contention_timeout";
    public static final String CDC_BLOCK_WRITES = "cdc_block_writes";
    public static final String CDC_ON_REPAIR_ENABLED = "cdc_on_repair_enabled";
    public static final String COORDINATOR_READ_SIZE_FAIL_THRESHOLD = "coordinator_read_size_fail_threshold";
    public static final String COORDINATOR_READ_SIZE_WARN_THRESHOLD = "coordinator_read_size_warn_threshold";
    public static final String COUNTER_WRITE_REQUEST_TIMEOUT = "counter_write_request_timeout";
    public static final String CREDENTIALS_CACHE_ACTIVE_UPDATE = "credentials_cache_active_update";
    public static final String CREDENTIALS_CACHE_MAX_ENTRIES = "credentials_cache_max_entries";
    public static final String CREDENTIALS_UPDATE_INTERVAL = "credentials_update_interval";
    public static final String CREDENTIALS_VALIDITY = "credentials_validity";
    public static final String DEFAULT_KEYSPACE_RF = "default_keyspace_rf";
    public static final String FORCE_NEW_PREPARED_STATEMENT_BEHAVIOUR = "force_new_prepared_statement_behaviour";
    public static final String HINTED_HANDOFF_ENABLED = "hinted_handoff_enabled";
    public static final String IDEAL_CONSISTENCY_LEVEL = "ideal_consistency_level";
    public static final String INCREMENTAL_BACKUPS = "incremental_backups";
    public static final String INTERNODE_STREAMING_TCP_USER_TIMEOUT = "internode_streaming_tcp_user_timeout";
    public static final String INTERNODE_TCP_CONNECT_TIMEOUT = "internode_tcp_connect_timeout";
    public static final String INTERNODE_TCP_USER_TIMEOUT = "internode_tcp_user_timeout";
    public static final String LOCAL_READ_SIZE_FAIL_THRESHOLD = "local_read_size_fail_threshold";
    public static final String LOCAL_READ_SIZE_WARN_THRESHOLD = "local_read_size_warn_threshold";
    public static final String MAX_HINTS_SIZE_PER_HOST = "max_hints_size_per_host";
    public static final String MAX_HINT_WINDOW = "max_hint_window";
    public static final String NATIVE_TRANSPORT_ALLOW_OLDER_PROTOCOLS = "native_transport_allow_older_protocols";
    public static final String NATIVE_TRANSPORT_MAX_CONCURRENT_CONNECTIONS = "native_transport_max_concurrent_connections";
    public static final String NATIVE_TRANSPORT_MAX_CONCURRENT_CONNECTIONS_PER_IP = "native_transport_max_concurrent_connections_per_ip";
    public static final String NATIVE_TRANSPORT_MAX_REQUESTS_PER_SECOND = "native_transport_max_requests_per_second";
    public static final String NATIVE_TRANSPORT_MAX_REQUEST_DATA_IN_FLIGHT = "native_transport_max_request_data_in_flight";
    public static final String NATIVE_TRANSPORT_MAX_REQUEST_DATA_IN_FLIGHT_PER_IP = "native_transport_max_request_data_in_flight_per_ip";
    public static final String NATIVE_TRANSPORT_RATE_LIMITING_ENABLED = "native_transport_rate_limiting_enabled";
    public static final String PERMISSIONS_CACHE_ACTIVE_UPDATE = "permissions_cache_active_update";
    public static final String PERMISSIONS_CACHE_MAX_ENTRIES = "permissions_cache_max_entries";
    public static final String PERMISSIONS_UPDATE_INTERVAL = "permissions_update_interval";
    public static final String PERMISSIONS_VALIDITY = "permissions_validity";
    public static final String PHI_CONVICT_THRESHOLD = "phi_convict_threshold";
    public static final String RANGE_REQUEST_TIMEOUT = "range_request_timeout";
    public static final String READ_REQUEST_TIMEOUT = "read_request_timeout";
    public static final String READ_THRESHOLDS_ENABLED = "read_thresholds_enabled";
    public static final String REPAIR_REQUEST_TIMEOUT = "repair_request_timeout";
    public static final String REPAIR_SESSION_SPACE = "repair_session_space";
    public static final String REQUEST_TIMEOUT = "request_timeout";
    public static final String ROLES_CACHE_ACTIVE_UPDATE = "roles_cache_active_update";
    public static final String ROLES_CACHE_MAX_ENTRIES = "roles_cache_max_entries";
    public static final String ROLES_UPDATE_INTERVAL = "roles_update_interval";
    public static final String ROLES_VALIDITY = "roles_validity";
    public static final String ROW_INDEX_READ_SIZE_FAIL_THRESHOLD = "row_index_read_size_fail_threshold";
    public static final String ROW_INDEX_READ_SIZE_WARN_THRESHOLD = "row_index_read_size_warn_threshold";
    public static final String SLOW_QUERY_LOG_TIMEOUT = "slow_query_log_timeout";
    public static final String SNAPSHOT_LINKS_PER_SECOND = "snapshot_links_per_second";
    public static final String SSTABLE_PREEMPTIVE_OPEN_INTERVAL = "sstable_preemptive_open_interval";
    public static final String TOMBSTONE_FAILURE_THRESHOLD = "tombstone_failure_threshold";
    public static final String TOMBSTONE_WARN_THRESHOLD = "tombstone_warn_threshold";
    public static final String TRANSFER_HINTS_ON_DECOMMISSION = "transfer_hints_on_decommission";
    public static final String TRUNCATE_REQUEST_TIMEOUT = "truncate_request_timeout";
    public static final String USE_DETERMINISTIC_TABLE_ID = "use_deterministic_table_id";
    public static final String USE_OFFHEAP_MERKLE_TREES = "use_offheap_merkle_trees";
    public static final String WRITE_REQUEST_TIMEOUT = "write_request_timeout";
}
