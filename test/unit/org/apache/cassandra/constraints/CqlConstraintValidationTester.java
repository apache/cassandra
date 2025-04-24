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

package org.apache.cassandra.constraints;

import java.util.Map;

import com.datastax.driver.core.ResultSet;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.CqlBuilder;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.transport.ProtocolVersion;

public abstract class CqlConstraintValidationTester extends CQLTester
{
    ResultSet executeDescribeNet(String cql) throws Throwable
    {
        return executeDescribeNet(null, cql);
    }

    ResultSet executeDescribeNet(String useKs, String cql) throws Throwable
    {
        return executeNetWithPaging(getProtocolVersion(useKs), cql, useKs, 3);
    }

    private ProtocolVersion getProtocolVersion(String useKs) throws Throwable
    {
        // We're using a trick here to distinguish driver sessions with a "USE keyspace" and without:
        // As different ProtocolVersions use different driver instances, we use different ProtocolVersions
        // for the with and without "USE keyspace" cases.

        ProtocolVersion v = useKs != null ? ProtocolVersion.CURRENT : ProtocolVersion.V6;

        if (useKs != null)
            executeNet(v, "USE " + useKs);
        return v;
    }

    static String tableParametersCql()
    {
        return "additional_write_policy = '99p'\n" +
               "    AND allow_auto_snapshot = true\n" +
               "    AND bloom_filter_fp_chance = 0.01\n" +
               "    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}\n" +
               "    AND cdc = false\n" +
               "    AND comment = ''\n" +
               "    AND compaction = " + cqlQuoted(CompactionParams.DEFAULT.asMap()) + "\n" +
               "    AND compression = {'chunk_length_in_kb': '16', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}\n" +
               "    AND memtable = 'default'\n" +
               "    AND crc_check_chance = 1.0\n" +
               "    AND fast_path = 'keyspace'\n" +
               "    AND default_time_to_live = 0\n" +
               "    AND extensions = {}\n" +
               "    AND gc_grace_seconds = 864000\n" +
               "    AND incremental_backups = true\n" +
               "    AND max_index_interval = 2048\n" +
               "    AND memtable_flush_period_in_ms = 0\n" +
               "    AND min_index_interval = 128\n" +
               "    AND read_repair = 'BLOCKING'\n" +
               "    AND transactional_mode = 'off'\n" +
               "    AND transactional_migration_from = 'none'\n" +
               "    AND speculative_retry = '99p';";
    }

    private static String cqlQuoted(Map<String, String> map)
    {
        return new CqlBuilder().append(map).toString();
    }
}
