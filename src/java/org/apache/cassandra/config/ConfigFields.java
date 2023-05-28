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
 * This class is created by {@link org.apache.cassandra.utils.ConfigFieldsGenerateUtil} based on provided
 * the {@link org.apache.cassandra.config.Config} class. It contains non-private non-static {@code Cofig}'s fields
 * marked with the {@link org.apache.cassandra.config.Mutable} annotation to expose them to public APIs.

 * @see org.apache.cassandra.config.Mutable
 * @see org.apache.cassandra.config.Config
 */
public class ConfigFields
{
    /** String representation of the {@link org.apache.cassandra.config.Config#compaction_throughput}. */
    public static final String COMPACTION_THROUGHPUT = "compaction_throughput";
    /** String representation of the {@link org.apache.cassandra.config.Config#concurrent_compactors}. */
    public static final String CONCURRENT_COMPACTORS = "concurrent_compactors";
    /** String representation of the {@link org.apache.cassandra.config.Config#entire_sstable_inter_dc_stream_throughput_outbound}. */
    public static final String ENTIRE_SSTABLE_INTER_DC_STREAM_THROUGHPUT_OUTBOUND = "entire_sstable_inter_dc_stream_throughput_outbound";
    /** String representation of the {@link org.apache.cassandra.config.Config#entire_sstable_stream_throughput_outbound}. */
    public static final String ENTIRE_SSTABLE_STREAM_THROUGHPUT_OUTBOUND = "entire_sstable_stream_throughput_outbound";
    /** String representation of the {@link org.apache.cassandra.config.Config#inter_dc_stream_throughput_outbound}. */
    public static final String INTER_DC_STREAM_THROUGHPUT_OUTBOUND = "inter_dc_stream_throughput_outbound";
    /** String representation of the {@link org.apache.cassandra.config.Config#repair_session_space}. */
    public static final String REPAIR_SESSION_SPACE = "repair_session_space";
    /** String representation of the {@link org.apache.cassandra.config.Config#stream_throughput_outbound}. */
    public static final String STREAM_THROUGHPUT_OUTBOUND = "stream_throughput_outbound";

    private ConfigFields() {}
}
