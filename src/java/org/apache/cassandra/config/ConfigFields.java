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
    public static final String BATCH_SIZE_WARN_THRESHOLD = "batch_size_warn_threshold";
    public static final String CAS_CONTENTION_TIMEOUT = "cas_contention_timeout";
    public static final String COLUMN_INDEX_CACHE_SIZE = "column_index_cache_size";
    public static final String COLUMN_INDEX_SIZE = "column_index_size";
    public static final String CONCURRENT_COMPACTORS = "concurrent_compactors";
    public static final String CONCURRENT_VALIDATIONS = "concurrent_validations";
    public static final String COUNTER_WRITE_REQUEST_TIMEOUT = "counter_write_request_timeout";
    public static final String NATIVE_TRANSPORT_MAX_REQUESTS_PER_SECOND = "native_transport_max_requests_per_second";
    public static final String NATIVE_TRANSPORT_MAX_REQUEST_DATA_IN_FLIGHT = "native_transport_max_request_data_in_flight";
    public static final String NATIVE_TRANSPORT_MAX_REQUEST_DATA_IN_FLIGHT_PER_IP = "native_transport_max_request_data_in_flight_per_ip";
    public static final String NATIVE_TRANSPORT_RATE_LIMITING_ENABLED = "native_transport_rate_limiting_enabled";
    public static final String PHI_CONVICT_THRESHOLD = "phi_convict_threshold";
    public static final String RANGE_REQUEST_TIMEOUT = "range_request_timeout";
    public static final String READ_REQUEST_TIMEOUT = "read_request_timeout";
    public static final String REPAIR_SESSION_SPACE = "repair_session_space";
    public static final String REQUEST_TIMEOUT = "request_timeout";
    public static final String TRUNCATE_REQUEST_TIMEOUT = "truncate_request_timeout";
    public static final String WRITE_REQUEST_TIMEOUT = "write_request_timeout";
}
