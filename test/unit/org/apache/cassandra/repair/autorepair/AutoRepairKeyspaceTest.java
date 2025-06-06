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

package org.apache.cassandra.repair.autorepair;

import java.util.Optional;
import java.util.Set;

import com.google.common.collect.ImmutableSet;

import org.apache.cassandra.repair.autorepair.AutoRepairKeyspace;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.TableMetadata;

import static org.apache.cassandra.Util.setAutoRepairEnabled;

public class AutoRepairKeyspaceTest
{
    private static final Set<String> v1Tables = ImmutableSet.of(
    AutoRepairKeyspace.AUTO_REPAIR_HISTORY,
    AutoRepairKeyspace.AUTO_REPAIR_PRIORITY
    );

    private static final Set<String> v2Tables = ImmutableSet.of(
    AutoRepairKeyspace.AUTO_REPAIR_HISTORY_V2,
    AutoRepairKeyspace.AUTO_REPAIR_PRIORITY_V2
    );

    @BeforeClass
    public static void setupDatabaseDescriptor()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void testMetadataCanParseV1Schemas() throws Exception
    {
        setAutoRepairEnabled(false);
        KeyspaceMetadata keyspaceMetadata = AutoRepairKeyspace.metadata();

        assert keyspaceMetadata.tables.size() == v1Tables.size() : "Expected " + v1Tables.size() + " tables, got " + keyspaceMetadata.tables.size();

        for (String table : v1Tables)
        {
            Optional<TableMetadata> tableMetadata = keyspaceMetadata.tables.get(table);

            assert tableMetadata.isPresent() : "Table " + table + " not found in metadata";
        }
    }


    @Test
    public void testMetadataCanParseV2Schemas() throws Exception
    {
        setAutoRepairEnabled(true);
        KeyspaceMetadata keyspaceMetadata = AutoRepairKeyspace.metadata();

        assert keyspaceMetadata.tables.size() == v1Tables.size() + v2Tables.size() : "Expected " + v1Tables.size() + v2Tables.size() + " tables, got " + keyspaceMetadata.tables.size();

        for (String table : v1Tables)
        {
            Optional<TableMetadata> tableMetadata = keyspaceMetadata.tables.get(table);

            assert tableMetadata.isPresent() : "Table " + table + " not found in metadata";
        }

        for (String table : v2Tables)
        {
            Optional<TableMetadata> tableMetadata = keyspaceMetadata.tables.get(table);

            assert tableMetadata.isPresent() : "Table " + table + " not found in metadata";
        }
    }
}
