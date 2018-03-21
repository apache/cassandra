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
package org.apache.cassandra.db.virtual;

import static java.lang.String.format;

import java.util.List;

import org.apache.cassandra.cql3.statements.CreateTableStatement;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.Tables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;

public class SystemInfoKeyspace
{
    private static final Logger logger = LoggerFactory.getLogger(SystemInfoKeyspace.class);

    private SystemInfoKeyspace() {}

    public static final String SETTINGS = "settings";
    public static final String TABLE_METRICS = "table_stats";
    public static final String RING_STATE = "ring_state";
    public static final String COMPACTION = "compaction_stats";

    private static final TableMetadata Settings = parse(SETTINGS,
            "Current Settings",
            "CREATE TABLE USING Settings %s");

    private static final TableMetadata TableMetrics = parse(TABLE_METRICS,
            "Table Metrics",
            "CREATE TABLE USING TableStats %s");

    private static final TableMetadata RingState = parse(RING_STATE,
            "Ring State",
            "CREATE TABLE USING RingState %s");

    private static final TableMetadata Compactions = parse(COMPACTION,
            "Compaction State",
            "CREATE TABLE USING CompactionStats %s");

    private static final List<TableMetadata> ALL_TABLE_METADATA =
            ImmutableList.of(Settings, TableMetrics, RingState, Compactions);

    private static TableMetadata parse(String table, String description, String cql)
    {
        return CreateTableStatement.parse(format(cql, table), SchemaConstants.INFO_KEYSPACE_NAME)
                .id(TableId.forSystemTable(SchemaConstants.INFO_KEYSPACE_NAME, table))
                .dcLocalReadRepairChance(0.0)
                .comment(description)
                .build();
    }

    public static KeyspaceMetadata metadata()
    {
        return KeyspaceMetadata.create(SchemaConstants.INFO_KEYSPACE_NAME, KeyspaceParams.local(), Tables.of(ALL_TABLE_METADATA));
    }

}
